"""
Unit tests for StreamProcessor.
"""

import json
import time
import unittest
from unittest.mock import MagicMock, patch

import pytest

from llama_stream import (
    EventSink,
    EventSource,
    StreamEvent,
    StreamProcessor,
    WindowSpec,
    WindowType,
)


class MockSource(EventSource):
    """Mock source for testing."""
    
    def __init__(self, source_id):
        """Initialize a mock source."""
        super().__init__(source_id)
    
    async def start(self):
        """Start the mock source."""
        await super().start()
    
    def emit_test_event(self, data):
        """Emit a test event."""
        return self.emit(data)


class MockSink(EventSink):
    """Mock sink for testing."""
    
    def __init__(self, sink_id):
        """Initialize a mock sink."""
        super().__init__(sink_id)
        self.published_events = []
    
    async def publish(self, event):
        """Publish an event to this sink."""
        self.published_events.append(event)


class TestStreamProcessor(unittest.TestCase):
    """Test cases for StreamProcessor."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.processor = StreamProcessor()
        self.source = MockSource("test_source")
        self.sink = MockSink("test_sink")
        
        self.processor.add_source(self.source)
        self.processor.add_sink(self.sink)
        
        # Register a tumbling window
        self.processor.register_window(
            "test_window",
            WindowSpec(WindowType.TUMBLING, size=1.0)
        )
    
    def test_add_source(self):
        """Test adding a source."""
        new_source = MockSource("another_source")
        self.processor.add_source(new_source)
        
        self.assertIn("another_source", self.processor.sources)
        self.assertEqual(self.processor.sources["another_source"], new_source)
    
    def test_add_sink(self):
        """Test adding a sink."""
        new_sink = MockSink("another_sink")
        self.processor.add_sink(new_sink)
        
        self.assertIn("another_sink", self.processor.sinks)
        self.assertEqual(self.processor.sinks["another_sink"], new_sink)
    
    def test_register_window(self):
        """Test registering a window specification."""
        spec = WindowSpec(WindowType.SLIDING, size=10.0, slide=2.0)
        self.processor.register_window("sliding_window", spec)
        
        self.assertIn("sliding_window", self.processor.window_manager.window_specs)
        self.assertEqual(
            self.processor.window_manager.window_specs["sliding_window"],
            spec
        )
    
    def test_event_processing(self):
        """Test basic event processing flow."""
        # Define a simple operator that doubles numeric values
        def double_value(event, state, config):
            if isinstance(event.data, (int, float)):
                return event.data * 2
            return None
        
        # Add the operator
        self.processor.add_operator(double_value)
        
        # Start the processor
        self.processor.start()
        
        # Emit a test event
        event = self.source.emit_test_event(42)
        
        # Wait for processing
        time.sleep(0.5)
        
        # Check that the sink received the processed result
        self.processor.stop()
        time.sleep(0.5)  # Give time for shutdown
        
        self.assertGreaterEqual(len(self.sink.published_events), 1)
        self.assertEqual(self.sink.published_events[-1].data, 84)
    
    def test_window_processing(self):
        """Test windowed event processing."""
        # Define a window operator that sums values
        def sum_window(window, state, config):
            total = sum(event.data for event in window.events 
                      if isinstance(event.data, (int, float)))
            return {"sum": total, "count": len(window.events)}
        
        # Add the operator with window configuration
        self.processor.add_operator(
            sum_window,
            {"window_spec": "test_window"}
        )
        
        # Start the processor
        self.processor.start()
        
        # Emit test events
        self.source.emit_test_event(10)
        self.source.emit_test_event(20)
        self.source.emit_test_event(30)
        
        # Wait for the window to close and process
        time.sleep(1.5)
        
        # Check that the sink received the window result
        self.processor.stop()
        time.sleep(0.5)  # Give time for shutdown
        
        # Find the window result
        window_results = [e for e in self.sink.published_events 
                         if isinstance(e.data, dict) and "sum" in e.data]
        
        self.assertGreaterEqual(len(window_results), 1)
        self.assertEqual(window_results[-1].data["sum"], 60)
        self.assertEqual(window_results[-1].data["count"], 3)
    
    def test_privacy_filter(self):
        """Test