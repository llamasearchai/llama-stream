"""
llama_stream: Real-time Stream Processing with MLX Integration
================================================================

A comprehensive stream processing framework for building real-time data pipelines
with multiple sources and sinks, featuring MLX-based windowing, encryption,
anomaly detection, and more.
"""

import base64
import datetime
import hashlib
import json
import logging
import os
import pickle
import queue
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

# Third-party imports (will be properly installed via dependencies)
try:
    import cryptography.fernet
    import mlx.core as mx
    import numpy as np
    import pandas as pd
    from kafka import KafkaConsumer, KafkaProducer
    from pulsar import Client as PulsarClient
except ImportError:
    # For type checking and documentation generation
    pass

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("llama_stream")

# Type definitions
T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")
SourceID = str
SinkID = str
WindowID = str


class StreamEvent(Generic[T]):
    """A generic event in the stream processing system.

    Attributes:
        id: Unique identifier for the event
        timestamp: Event creation timestamp
        source_id: Identifier of the source that produced this event
        data: The payload of the event
        metadata: Optional metadata associated with the event
    """

    def __init__(
        self,
        data: T,
        source_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        event_id: Optional[str] = None,
        timestamp: Optional[float] = None,
    ):
        """Initialize a new stream event.

        Args:
            data: The event payload
            source_id: Identifier of the source that produced this event
            metadata: Optional metadata associated with the event
            event_id: Optional event ID, generated if not provided
            timestamp: Optional timestamp, current time if not provided
        """
        self.id = event_id or str(uuid.uuid4())
        self.timestamp = timestamp or time.time()
        self.source_id = source_id
        self.data = data
        self.metadata = metadata or {}

    def __str__(self) -> str:
        """String representation of the event."""
        return (
            f"StreamEvent(id={self.id}, source={self.source_id}, ts={self.timestamp})"
        )


class EventSource(ABC, Generic[T]):
    """Abstract base class for event sources.

    An event source produces events that can be consumed by the stream processor.
    """

    def __init__(self, source_id: str):
        """Initialize a new event source.

        Args:
            source_id: Unique identifier for this source
        """
        self.source_id = source_id
        self._running = False
        self._consumers: List[Callable[[StreamEvent[T]], None]] = []

    def add_consumer(self, consumer: Callable[[StreamEvent[T]], None]) -> None:
        """Add a consumer function that will be called for each event.

        Args:
            consumer: A callable that takes a StreamEvent and processes it
        """
        self._consumers.append(consumer)

    def emit(
        self, data: T, metadata: Optional[Dict[str, Any]] = None
    ) -> StreamEvent[T]:
        """Create and emit a new event to all registered consumers.

        Args:
            data: The event payload
            metadata: Optional metadata for the event

        Returns:
            The created StreamEvent
        """
        event = StreamEvent(data, self.source_id, metadata)
        for consumer in self._consumers:
            consumer(event)
        return event

    @abstractmethod
    async def start(self) -> None:
        """Start producing events.

        This method should be implemented by concrete sources.
        """
        self._running = True

    async def stop(self) -> None:
        """Stop producing events."""
        self._running = False


class EventSink(ABC, Generic[T]):
    """Abstract base class for event sinks.

    An event sink consumes processed events from the stream processor.
    """

    def __init__(self, sink_id: str):
        """Initialize a new event sink.

        Args:
            sink_id: Unique identifier for this sink
        """
        self.sink_id = sink_id

    @abstractmethod
    async def publish(self, event: StreamEvent[T]) -> None:
        """Publish an event to this sink.

        Args:
            event: The event to publish
        """
        pass


class WindowType(Enum):
    """Types of windows supported by the system."""

    TUMBLING = "tumbling"  # Fixed-size, non-overlapping windows
    SLIDING = "sliding"  # Fixed-size, overlapping windows
    SESSION = "session"  # Dynamic-size windows based on activity
    GLOBAL = "global"  # Single window for the entire stream


@dataclass
class WindowSpec:
    """Specification for a window in stream processing.

    Attributes:
        window_type: The type of window (tumbling, sliding, session, global)
        size: The window size in seconds (not used for session and global windows)
        slide: The slide interval in seconds (for sliding windows)
        timeout: The timeout in seconds (for session windows)
        max_size: Maximum number of events in a window (optional)
    """

    window_type: WindowType
    size: Optional[float] = None
    slide: Optional[float] = None
    timeout: Optional[float] = None
    max_size: Optional[int] = None

    def __post_init__(self):
        """Validate window specification after initialization."""
        if self.window_type == WindowType.TUMBLING and self.size is None:
            raise ValueError("Tumbling windows require a size")
        if self.window_type == WindowType.SLIDING:
            if self.size is None:
                raise ValueError("Sliding windows require a size")
            if self.slide is None:
                raise ValueError("Sliding windows require a slide interval")
        if self.window_type == WindowType.SESSION and self.timeout is None:
            raise ValueError("Session windows require a timeout")


class Window(Generic[T]):
    """A window containing a collection of events.

    Attributes:
        window_id: Unique identifier for this window
        start_time: Start timestamp of this window
        end_time: End timestamp of this window
        events: List of events in this window
        metadata: Optional metadata associated with the window
    """

    def __init__(
        self,
        window_id: str,
        start_time: float,
        end_time: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Initialize a new window.

        Args:
            window_id: Unique identifier for this window
            start_time: Start timestamp for this window
            end_time: Optional end timestamp, None for ongoing windows
            metadata: Optional metadata for the window
        """
        self.window_id = window_id
        self.start_time = start_time
        self.end_time = end_time
        self.events: List[StreamEvent[T]] = []
        self.metadata = metadata or {}

    def add_event(self, event: StreamEvent[T]) -> None:
        """Add an event to this window.

        Args:
            event: The event to add
        """
        self.events.append(event)

    def close(self, end_time: Optional[float] = None) -> None:
        """Close this window, setting its end time.

        Args:
            end_time: Optional end timestamp, current time if not provided
        """
        self.end_time = end_time or time.time()

    def is_closed(self) -> bool:
        """Check if this window is closed.

        Returns:
            True if the window is closed, False otherwise
        """
        return self.end_time is not None

    def to_mlx_array(self) -> "mx.array":
        """Convert window data to MLX array for efficient processing.

        Returns:
            An MLX array containing the window data
        """
        # Extract features from events
        features = []
        for event in self.events:
            if isinstance(event.data, dict):
                # Extract numerical values from dict
                feature_vector = [
                    float(v)
                    for k, v in event.data.items()
                    if isinstance(v, (int, float))
                ]
                features.append(feature_vector)
            elif isinstance(event.data, (list, tuple)) and all(
                isinstance(x, (int, float)) for x in event.data
            ):
                # Use data directly if it's already a list of numbers
                features.append(list(event.data))
            else:
                # Skip non-numeric data
                continue

        # Handle empty or incompatible data
        if not features or not features[0]:
            return mx.zeros((0, 0))

        # Pad features to ensure same length
        max_len = max(len(f) for f in features)
        padded_features = [f + [0.0] * (max_len - len(f)) for f in features]

        # Convert to MLX array
        return mx.array(padded_features)

    def __len__(self) -> int:
        """Get the number of events in this window.

        Returns:
            Number of events
        """
        return len(self.events)


class WindowManager:
    """Manager for creating and maintaining windows based on window specifications.

    This class is responsible for assigning events to appropriate windows
    based on the configured windowing strategy.
    """

    def __init__(self):
        """Initialize a new window manager."""
        self.windows: Dict[WindowID, Window] = {}
        self.window_specs: Dict[str, WindowSpec] = {}
        self.active_sessions: Dict[str, Window] = {}
        self.last_watermark = 0.0

    def register_window_spec(self, key: str, spec: WindowSpec) -> None:
        """Register a window specification.

        Args:
            key: Identifier for this window specification
            spec: The window specification
        """
        self.window_specs[key] = spec

    def assign_to_windows(self, event: StreamEvent, spec_key: str) -> List[Window]:
        """Assign an event to appropriate windows based on a window spec.

        Args:
            event: The event to assign
            spec_key: Key of the window specification to use

        Returns:
            List of windows the event was assigned to
        """
        if spec_key not in self.window_specs:
            raise ValueError(f"Window specification {spec_key} not found")

        spec = self.window_specs[spec_key]
        assigned_windows = []

        if spec.window_type == WindowType.TUMBLING:
            window = self._assign_to_tumbling_window(event, spec)
            assigned_windows.append(window)

        elif spec.window_type == WindowType.SLIDING:
            windows = self._assign_to_sliding_windows(event, spec)
            assigned_windows.extend(windows)

        elif spec.window_type == WindowType.SESSION:
            window = self._assign_to_session_window(event, spec)
            assigned_windows.append(window)

        elif spec.window_type == WindowType.GLOBAL:
            window = self._assign_to_global_window(event)
            assigned_windows.append(window)

        self._update_watermark(event.timestamp)

        return assigned_windows

    def _update_watermark(self, timestamp: float) -> None:
        """Update the watermark based on event timestamps.

        The watermark is the minimum timestamp that all future events
        are expected to be greater than.

        Args:
            timestamp: The timestamp to consider for watermark update
        """
        self.last_watermark = max(self.last_watermark, timestamp)

        # Close windows that are now behind the watermark
        for window_id, window in list(self.windows.items()):
            spec_key = window.metadata.get("spec_key")
            if not spec_key:
                continue

            spec = self.window_specs.get(spec_key)
            if not spec:
                continue

            if spec.window_type == WindowType.TUMBLING:
                if window.start_time + spec.size < self.last_watermark:
                    window.close()

            elif spec.window_type == WindowType.SLIDING:
                if window.start_time + spec.size < self.last_watermark:
                    window.close()

            elif spec.window_type == WindowType.SESSION:
                for session_key, session in list(self.active_sessions.items()):
                    last_event_time = (
                        max(e.timestamp for e in session.events)
                        if session.events
                        else session.start_time
                    )
                    if last_event_time + spec.timeout < self.last_watermark:
                        session.close()
                        del self.active_sessions[session_key]

    def _assign_to_tumbling_window(
        self, event: StreamEvent, spec: WindowSpec
    ) -> Window:
        """Assign an event to a tumbling window.

        Args:
            event: The event to assign
            spec: The window specification

        Returns:
            The window the event was assigned to
        """
        size = spec.size
        window_start = (event.timestamp // size) * size
        window_id = f"tumbling_{window_start}_{window_start + size}"

        if window_id not in self.windows:
            window = Window(
                window_id=window_id,
                start_time=window_start,
                end_time=window_start + size,
                metadata={"spec_key": spec, "window_type": "tumbling"},
            )
            self.windows[window_id] = window
        else:
            window = self.windows[window_id]

        window.add_event(event)
        return window

    def _assign_to_sliding_windows(
        self, event: StreamEvent, spec: WindowSpec
    ) -> List[Window]:
        """Assign an event to sliding windows.

        Args:
            event: The event to assign
            spec: The window specification

        Returns:
            List of windows the event was assigned to
        """
        size = spec.size
        slide = spec.slide

        # Find all windows that this event belongs to
        timestamp = event.timestamp
        earliest_start = timestamp - size
        latest_start = timestamp

        # Calculate all possible window start times
        window_starts = []
        current_start = (earliest_start // slide) * slide
        while current_start <= latest_start:
            window_starts.append(current_start)
            current_start += slide

        assigned_windows = []
        for start in window_starts:
            window_id = f"sliding_{start}_{start + size}"

            if window_id not in self.windows:
                window = Window(
                    window_id=window_id,
                    start_time=start,
                    end_time=start + size,
                    metadata={"spec_key": spec, "window_type": "sliding"},
                )
                self.windows[window_id] = window
            else:
                window = self.windows[window_id]

            window.add_event(event)
            assigned_windows.append(window)

        return assigned_windows

    def _assign_to_session_window(self, event: StreamEvent, spec: WindowSpec) -> Window:
        """Assign an event to a session window.

        Args:
            event: The event to assign
            spec: The window specification

        Returns:
            The window the event was assigned to
        """
        # Use source_id as session key to separate sessions by source
        session_key = event.source_id

        if session_key in self.active_sessions:
            # Check if the existing session has timed out
            session = self.active_sessions[session_key]
            last_event_time = (
                max(e.timestamp for e in session.events)
                if session.events
                else session.start_time
            )

            if event.timestamp - last_event_time > spec.timeout:
                # Session timed out, close it and create a new one
                session.close(last_event_time + spec.timeout)

                new_session = Window(
                    window_id=f"session_{session_key}_{event.timestamp}",
                    start_time=event.timestamp,
                    metadata={
                        "spec_key": spec,
                        "window_type": "session",
                        "session_key": session_key,
                    },
                )
                self.active_sessions[session_key] = new_session
                self.windows[new_session.window_id] = new_session
                new_session.add_event(event)
                return new_session
            else:
                # Add to existing session
                session.add_event(event)
                return session
        else:
            # Create a new session
            new_session = Window(
                window_id=f"session_{session_key}_{event.timestamp}",
                start_time=event.timestamp,
                metadata={
                    "spec_key": spec,
                    "window_type": "session",
                    "session_key": session_key,
                },
            )
            self.active_sessions[session_key] = new_session
            self.windows[new_session.window_id] = new_session
            new_session.add_event(event)
            return new_session

    def _assign_to_global_window(self, event: StreamEvent) -> Window:
        """Assign an event to the global window.

        Args:
            event: The event to assign

        Returns:
            The global window
        """
        window_id = "global_window"

        if window_id not in self.windows:
            window = Window(
                window_id=window_id, start_time=0, metadata={"window_type": "global"}
            )
            self.windows[window_id] = window
        else:
            window = self.windows[window_id]

        window.add_event(event)
        return window

    def get_closed_windows(self) -> List[Window]:
        """Get all closed windows.

        Returns:
            List of closed windows
        """
        return [w for w in self.windows.values() if w.is_closed()]

    def remove_window(self, window_id: str) -> None:
        """Remove a window from the manager.

        Args:
            window_id: ID of the window to remove
        """
        if window_id in self.windows:
            del self.windows[window_id]


class StreamProcessor:
    """Core stream processing engine that manages the flow of events.

    This class coordinates sources, sinks, and operations on the data stream.
    """

    def __init__(self):
        """Initialize a new stream processor."""
        self.sources: Dict[SourceID, EventSource] = {}
        self.sinks: Dict[SinkID, EventSink] = {}
        self.window_manager = WindowManager()
        self.operators: List[Tuple[Callable, Dict]] = []
        self.state: Dict[str, Any] = {}
        self.checkpoint_interval = 60  # seconds
        self.last_checkpoint = time.time()
        self.encryption_key = None
        self.privacy_filter = None
        self._buffer_queue = queue.Queue()
        self._processing_thread = None
        self._shutdown_flag = threading.Event()

    def add_source(self, source: EventSource) -> None:
        """Add an event source to the processor.

        Args:
            source: The source to add
        """
        self.sources[source.source_id] = source
        source.add_consumer(self._process_event)
        logger.info(f"Added source: {source.source_id}")

    def add_sink(self, sink: EventSink) -> None:
        """Add an event sink to the processor.

        Args:
            sink: The sink to add
        """
        self.sinks[sink.sink_id] = sink
        logger.info(f"Added sink: {sink.sink_id}")

    def register_window(self, key: str, spec: WindowSpec) -> None:
        """Register a window specification.

        Args:
            key: Identifier for this window specification
            spec: The window specification
        """
        self.window_manager.register_window_spec(key, spec)
        logger.info(f"Registered window spec: {key} ({spec.window_type.value})")

    def add_operator(
        self, operator: Callable, config: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add an operator to the processing pipeline.

        Args:
            operator: The operator function
            config: Optional configuration for the operator
        """
        self.operators.append((operator, config or {}))
        logger.info(f"Added operator: {operator.__name__}")

    def _process_event(self, event: StreamEvent) -> None:
        """Process an incoming event.

        Args:
            event: The event to process
        """
        self._buffer_queue.put(event)

    def set_encryption_key(self, key: str) -> None:
        """Set an encryption key for sensitive data.

        Args:
            key: The encryption key
        """
        # Derive a Fernet key from the provided key
        derived_key = base64.urlsafe_b64encode(hashlib.sha256(key.encode()).digest())
        self.encryption_key = derived_key
        logger.info("Encryption key set")

    def set_privacy_filter(self, filter_func: Callable[[Dict], Dict]) -> None:
        """Set a privacy filter function for GDPR compliance.

        Args:
            filter_func: Function that filters sensitive data
        """
        self.privacy_filter = filter_func
        logger.info("Privacy filter set")

    def start(self) -> None:
        """Start the stream processor."""
        if self._processing_thread and self._processing_thread.is_alive():
            logger.warning("Stream processor is already running")
            return

        self._shutdown_flag.clear()
        self._processing_thread = threading.Thread(target=self._processing_loop)
        self._processing_thread.daemon = True
        self._processing_thread.start()

        # Start all sources
        for source in self.sources.values():
            source.start()

        logger.info("Stream processor started")

    def stop(self) -> None:
        """Stop the stream processor."""
        self._shutdown_flag.set()

        # Stop all sources
        for source in self.sources.values():
            source.stop()

        if self._processing_thread:
            self._processing_thread.join(timeout=5.0)

        logger.info("Stream processor stopped")

    def _processing_loop(self) -> None:
        """Main processing loop that handles events from the buffer."""
        while not self._shutdown_flag.is_set():
            try:
                # Get event from buffer with timeout
                try:
                    event = self._buffer_queue.get(timeout=0.1)
                except queue.Empty:
                    # Check if it's time to checkpoint
                    now = time.time()
                    if now - self.last_checkpoint >= self.checkpoint_interval:
                        self._checkpoint_state()
                        self.last_checkpoint = now
                    continue

                # Apply privacy filter if set
                if self.privacy_filter and isinstance(event.data, dict):
                    event.data = self.privacy_filter(event.data)

                # Apply encryption if key is set
                if (
                    self.encryption_key
                    and "sensitive" in event.metadata
                    and event.metadata["sensitive"]
                ):
                    event.data = self._encrypt_data(event.data)

                # Process through all operators
                for operator, config in self.operators:
                    # Skip if operator has a source filter and this event doesn't match
                    if (
                        "source_filter" in config
                        and event.source_id not in config["source_filter"]
                    ):
                        continue

                    # Apply window if specified
                    if "window_spec" in config:
                        windows = self.window_manager.assign_to_windows(
                            event, config["window_spec"]
                        )

                        # Process windows that are ready
                        for window in windows:
                            if window.is_closed() or (
                                config.get("process_incomplete", False)
                                and len(window) > 0
                            ):
                                try:
                                    result = operator(window, self.state, config)
                                    if result:
                                        self._publish_result(result, window, config)
                                except Exception as e:
                                    logger.error(
                                        f"Error in operator {operator.__name__}: {str(e)}"
                                    )
                    else:
                        # Process individual event
                        try:
                            result = operator(event, self.state, config)
                            if result:
                                self._publish_result(result, None, config)
                        except Exception as e:
                            logger.error(
                                f"Error in operator {operator.__name__}: {str(e)}"
                            )

                self._buffer_queue.task_done()

            except Exception as e:
                logger.error(f"Error in processing loop: {str(e)}")

    def _publish_result(
        self, result: Any, window: Optional[Window], config: Dict
    ) -> None:
        """Publish a processing result to the appropriate sinks.

        Args:
            result: The result to publish
            window: The window that produced this result, if any
            config: Operator configuration
        """
        # Create a result event
        source_id = (
            window.events[0].source_id if window and window.events else "processor"
        )
        event = StreamEvent(
            data=result,
            source_id=source_id,
            metadata={
                "processor": True,
                "window_id": window.window_id if window else None,
                "operator": config.get("name", "unnamed"),
            },
        )

        # Publish to all sinks, or specific sinks if configured
        sink_ids = config.get("sinks", list(self.sinks.keys()))
        for sink_id in sink_ids:
            if sink_id in self.sinks:
                try:
                    self.sinks[sink_id].publish(event)
                except Exception as e:
                    logger.error(f"Error publishing to sink {sink_id}: {str(e)}")

    def _checkpoint_state(self) -> None:
        """Save the current state to a checkpoint."""
        try:
            # Prepare state for serialization
            checkpoint = {
                "state": self.state,
                "timestamp": time.time(),
                "version": "1.0",
            }

            # Save to file
            with open(f"checkpoint_{int(time.time())}.pkl", "wb") as f:
                pickle.dump(checkpoint, f)

            logger.info(f"State checkpoint created")

        except Exception as e:
            logger.error(f"Error creating checkpoint: {str(e)}")

    def load_checkpoint(self, path: str) -> bool:
        """Load state from a checkpoint file.

        Args:
            path: Path to the checkpoint file

        Returns:
            True if successful, False otherwise
        """
        try:
            with open(path, "rb") as f:
                checkpoint = pickle.load(f)

            self.state = checkpoint["state"]
            logger.info(f"Loaded checkpoint from {path}")
            return True

        except Exception as e:
            logger.error(f"Error loading checkpoint: {str(e)}")
            return False

    def _encrypt_data(self, data: Any) -> Any:
        """Encrypt sensitive data.

        Args:
            data: Data to encrypt

        Returns:
            Encrypted data
        """
        if not self.encryption_key:
            return data

        try:
            fernet = cryptography.fernet.Fernet(self.encryption_key)
            if isinstance(data, dict):
                # Encrypt values in dictionary
                return {k: self._encrypt_data(v) for k, v in data.items()}
            elif isinstance(data, (list, tuple)):
                # Encrypt values in list
                return [self._encrypt_data(v) for v in data]
            elif isinstance(data, (str, bytes)):
                # Encrypt string or bytes
                if isinstance(data, str):
                    data = data.encode()
                return {"__encrypted": fernet.encrypt(data).decode()}
            else:
                # Return as is for other types
                return data

        except Exception as e:
            logger.error(f"Encryption error: {str(e)}")
            return data

    def _decrypt_data(self, data: Any) -> Any:
        """Decrypt encrypted data.

        Args:
            data: Data to decrypt

        Returns:
            Decrypted data
        """
        if not self.encryption_key:
            return data

        try:
            fernet = cryptography.fernet.Fernet(self.encryption_key)
            if isinstance(data, dict):
                if "__encrypted" in data:
                    # Decrypt encrypted data
                    return fernet.decrypt(data["__encrypted"].encode()).decode()
                else:
                    # Decrypt values in dictionary
                    return {k: self._decrypt_data(v) for k, v in data.items()}
            elif isinstance(data, (list, tuple)):
                # Decrypt values in list
                return [self._decrypt_data(v) for v in data]
            else:
                # Return as is for other types
                return data

        except Exception as e:
            logger.error(f"Decryption error: {str(e)}")
            return data


# Concrete implementations of sources and sinks


class KafkaSource(EventSource[Dict]):
    """Event source that consumes events from a Kafka topic.

    This source connects to Kafka and emits events for each message consumed.
    """

    def __init__(
        self, source_id: str, bootstrap_servers: List[str], topic: str, **kafka_config
    ):
        """Initialize a new Kafka source.

        Args:
            source_id: Unique identifier for this source
            bootstrap_servers: List of Kafka bootstrap servers
            topic: Kafka topic to consume from
            **kafka_config: Additional configuration for KafkaConsumer
        """
        super().__init__(source_id)
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.kafka_config = kafka_config
        self.consumer = None
        self._consumer_thread = None

    async def start(self) -> None:
        """Start consuming from Kafka."""
        await super().start()

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            **self.kafka_config,
        )
