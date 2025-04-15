"""
Unit tests for source and sink implementations.
"""

import unittest
from unittest.mock import MagicMock, mock_open, patch

import pytest

from llama_stream import (
    CDCSimulationSource,
    ConsoleLogSink,
    FileSink,
    KafkaSink,
    KafkaSource,
    StreamEvent,
    VideoFrameSource,
)


class TestKafkaSource(unittest.TestCase):
    """Test cases for KafkaSource."""

    @patch("llama_stream.KafkaConsumer")
    def test_kafka_source_initialization(self, mock_kafka_consumer):
        """Test initializing a Kafka source."""
        # Create a Kafka source
        source = KafkaSource(
            "kafka_test",
            bootstrap_servers=["localhost:9092"],
            topic="test-topic",
            group_id="test-group",
        )

        # Check that the source is properly initialized
        self.assertEqual(source.source_id, "kafka_test")
        self.assertEqual(source.bootstrap_servers, ["localhost:9092"])
        self.assertEqual(source.topic, "test-topic")
        self.assertEqual(source.kafka_config["group_id"], "test-group")

    @patch("llama_stream.KafkaConsumer")
    @pytest.mark.asyncio
    async def test_kafka_source_start_stop(self, mock_kafka_consumer):
        """Test starting and stopping a Kafka source."""
        # Setup mock
        mock_instance = mock_kafka_consumer.return_value

        # Create a Kafka source
        source = KafkaSource(
            "kafka_test", bootstrap_servers=["localhost:9092"], topic="test-topic"
        )

        # Mock a consumer
        source._consumer_thread = MagicMock()

        # Start the source
        await source.start()

        # Check that consumer was created
        mock_kafka_consumer.assert_called_once_with(
            "test-topic",
            bootstrap_servers=["localhost:9092"],
            value_deserializer=unittest.mock.ANY,
        )

        # Check that consumer thread was started
        self.assertTrue(source._consumer_thread.start.called)

        # Stop the source
        await source.stop()

        # Check that consumer was closed
        self.assertTrue(mock_instance.close.called)

        # Check that thread was joined
        self.assertTrue(source._consumer_thread.join.called)


class TestKafkaSink(unittest.TestCase):
    """Test cases for KafkaSink."""

    @patch("llama_stream.KafkaProducer")
    def test_kafka_sink_initialization(self, mock_kafka_producer):
        """Test initializing a Kafka sink."""
        # Create a Kafka sink
        sink = KafkaSink(
            "kafka_test", bootstrap_servers=["localhost:9092"], topic="test-topic"
        )

        # Check that the sink is properly initialized
        self.assertEqual(sink.sink_id, "kafka_test")
        self.assertEqual(sink.bootstrap_servers, ["localhost:9092"])
        self.assertEqual(sink.topic, "test-topic")

        # Producer should not be created until publish is called
        mock_kafka_producer.assert_not_called()

    @patch("llama_stream.KafkaProducer")
    @pytest.mark.asyncio
    async def test_kafka_sink_publish(self, mock_kafka_producer):
        """Test publishing to a Kafka sink."""
        # Setup mock
        mock_instance = mock_kafka_producer.return_value

        # Create a Kafka sink
        sink = KafkaSink(
            "kafka_test", bootstrap_servers=["localhost:9092"], topic="test-topic"
        )

        # Create a test event
        event = StreamEvent({"value": 42}, "test_source")

        # Publish the event
        await sink.publish(event)

        # Check that producer was created
        mock_kafka_producer.assert_called_once_with(
            bootstrap_servers=["localhost:9092"], value_serializer=unittest.mock.ANY
        )

        # Check that send was called with correct topic
        mock_instance.send.assert_called_once()
        args, kwargs = mock_instance.send.call_args
        self.assertEqual(args[0], "test-topic")

        # Check the message format
        message = args[1]
        self.assertEqual(message["event_id"], event.id)
        self.assertEqual(message["source_id"], "test_source")
        self.assertEqual(message["data"], {"value": 42})


class TestCDCSimulationSource(unittest.TestCase):
    """Test cases for CDCSimulationSource."""

    def test_cdc_simulation_initialization(self):
        """Test initializing a CDC simulation source."""
        # Create a CDC simulation source
        source = CDCSimulationSource(
            "cdc_test", tables=["users", "orders"], rate=1.5, seed=42
        )

        # Check that the source is properly initialized
        self.assertEqual(source.source_id, "cdc_test")
        self.assertEqual(source.tables, ["users", "orders"])
        self.assertEqual(source.rate, 1.5)
        self.assertIsNotNone(source.rng)

    @patch("time.sleep", return_value=None)  # Don't actually sleep in tests
    @pytest.mark.asyncio
    async def test_cdc_event_generation(self, mock_sleep):
        """Test CDC event generation."""
        # Create a CDC simulation source with fixed seed for reproducibility
        source = CDCSimulationSource(
            "cdc_test",
            tables=["users"],
            rate=10.0,  # High rate to generate events quickly
            seed=42,
        )

        # Create a consumer to capture events
        events = []
        source.add_consumer(lambda event: events.append(event))

        # Start the source
        await source.start()

        # Wait for a short time to generate some events
        for _ in range(5):  # Simulate a few event generations
            source._generate_events()

        # Stop the source
        await source.stop()

        # Check that events were generated
        self.assertGreater(len(events), 0)

        # Check event structure
        for event in events:
            self.assertEqual(event.source_id, "cdc_test")
            self.assertIn("table", event.data)
            self.assertIn("operation", event.data)
            self.assertIn("record_id", event.data)
            self.assertIn("timestamp", event.data)

            # Check operation types
            self.assertIn(event.data["operation"], ["INSERT", "UPDATE", "DELETE"])

            # For inserts and updates, check values
            if event.data["operation"] in ["INSERT", "UPDATE"]:
                self.assertIn("values", event.data)
                self.assertIn("field1", event.data["values"])
                self.assertIn("field2", event.data["values"])
                self.assertIn("field3", event.data["values"])


class TestVideoFrameSource(unittest.TestCase):
    """Test cases for VideoFrameSource."""

    def test_video_frame_initialization(self):
        """Test initializing a video frame source."""
        # Create a video frame source
        source = VideoFrameSource("video_test", width=1280, height=720, fps=24.0)

        # Check that the source is properly initialized
        self.assertEqual(source.source_id, "video_test")
        self.assertEqual(source.width, 1280)
        self.assertEqual(source.height, 720)
        self.assertEqual(source.fps, 24.0)
        self.assertEqual(source.frame_interval, 1.0 / 24.0)

    @patch("time.sleep", return_value=None)  # Don't actually sleep in tests
    @pytest.mark.asyncio
    async def test_video_frame_generation(self, mock_sleep):
        """Test video frame generation."""
        # Create a video frame source
        source = VideoFrameSource(
            "video_test", width=640, height=480, fps=30.0, simulate_content=True
        )

        # Create a consumer to capture frames
        frames = []
        source.add_consumer(lambda frame: frames.append(frame))

        # Start the source
        await source.start()

        # Generate a few frames
        for _ in range(5):  # Simulate a few frame generations
            source._generate_frames()

        # Stop the source
        await source.stop()

        # Check that frames were generated
        self.assertEqual(len(frames), 5)

        # Check frame structure
        for i, frame in enumerate(frames):
            self.assertEqual(frame.source_id, "video_test")
            self.assertEqual(frame.data["frame_number"], i + 1)
            self.assertIn("timestamp", frame.data)
            self.assertEqual(frame.data["width"], 640)
            self.assertEqual(frame.data["height"], 480)

            # Check content if simulated
            self.assertIn("content_sample", frame.data)
            self.assertIsInstance(frame.data["content_sample"], list)
            self.assertEqual(len(frame.data["content_sample"]), 10)  # 10x10 array


class TestConsoleLogSink(unittest.TestCase):
    """Test cases for ConsoleLogSink."""

    @patch("logging.getLogger")
    def test_console_sink_initialization(self, mock_get_logger):
        """Test initializing a console log sink."""
        # Setup mock
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Create a console log sink
        import logging

        sink = ConsoleLogSink("console_test", level=logging.DEBUG)

        # Check that the sink is properly initialized
        self.assertEqual(sink.sink_id, "console_test")
        self.assertEqual(sink.level, logging.DEBUG)
        self.assertEqual(sink.logger, mock_logger)

    @patch("logging.getLogger")
    @pytest.mark.asyncio
    async def test_console_sink_publish(self, mock_get_logger):
        """Test publishing to a console log sink."""
        # Setup mock
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Create a console log sink
        import logging

        sink = ConsoleLogSink("console_test", level=logging.INFO)

        # Create a test event
        event = StreamEvent({"value": 42}, "test_source")

        # Publish the event
        await sink.publish(event)

        # Check that log was called with correct level
        mock_logger.log.assert_called_once()
        args, kwargs = mock_logger.log.call_args
        self.assertEqual(args[0], logging.INFO)

        # Check message format
        message = args[1]
        self.assertIn("Event", message)
        self.assertIn("test_source", message)
        self.assertIn("42", message)


class TestFileSink(unittest.TestCase):
    """Test cases for FileSink."""

    def test_file_sink_initialization(self):
        """Test initializing a file sink."""
        # Create a file sink
        sink = FileSink(
            "file_test", file_path="test_output.jsonl", mode="a", encoding="utf-8"
        )

        # Check that the sink is properly initialized
        self.assertEqual(sink.sink_id, "file_test")
        self.assertEqual(sink.file_path, "test_output.jsonl")
        self.assertEqual(sink.mode, "a")
        self.assertEqual(sink.encoding, "utf-8")
        self.assertIsNone(sink.file)

    @patch("builtins.open", new_callable=mock_open)
    @pytest.mark.asyncio
    async def test_file_sink_publish(self, mock_file):
        """Test publishing to a file sink."""
        # Create a file sink
        sink = FileSink("file_test", file_path="test_output.jsonl")

        # Create a test event
        event = StreamEvent({"value": 42}, "test_source")

        # Publish the event
        await sink.publish(event)

        # Check that file was opened
        mock_file.assert_called_once_with("test_output.jsonl", "a", encoding="utf-8")

        # Check that write was called
        file_handle = mock_file()
        self.assertTrue(file_handle.write.called)

        # Check the event format
        args, kwargs = file_handle.write.call_args
        event_json = args[0]
        self.assertIn("id", event_json)
        self.assertIn("test_source", event_json)
        self.assertIn("42", event_json)
        self.assertTrue(event_json.endswith("\n"))

        # Check that flush was called
        self.assertTrue(file_handle.flush.called)


if __name__ == "__main__":
    unittest.main()
