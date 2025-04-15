"""
Unit tests for stream processing operators.
"""

import unittest
from unittest.mock import MagicMock, patch

import numpy as np

from llama_stream import (
    StreamEvent,
    Window,
    mlx_window_feature_extractor,
)


class TestFeatureExtractor(unittest.TestCase):
    """Test cases for the feature extractor operator."""

    @patch("llama_stream.mx")
    def test_feature_extraction(self, mock_mx):
        """Test basic feature extraction from a window."""
        # Setup mocks for MLX functions
        mock_mx.mean.return_value = np.array([1.5, 2.5])
        mock_mx.std.return_value = np.array([0.5, 0.5])
        mock_mx.min.return_value = np.array([1.0, 2.0])
        mock_mx.max.return_value = np.array([2.0, 3.0])

        # Create a test window
        window = Window("test_window", start_time=0.0, end_time=1.0)

        # Add test events with numeric data
        window.add_event(StreamEvent([1, 2], "test_source"))
        window.add_event(StreamEvent([2, 3], "test_source"))

        # Setup mock for window.to_mlx_array
        window.to_mlx_array = MagicMock(return_value=np.array([[1, 2], [2, 3]]))

        # Call the feature extractor
        config = {"stats_features": True, "fft_features": False}
        result = mlx_window_feature_extractor(window, {}, config)

        # Check the result structure
        self.assertIsNotNone(result)
        self.assertIn("features", result)
        self.assertIn("window_id", result)
        self.assertEqual(result["window_id"], "test_window")
        self.assertEqual(result["event_count"], 2)

        # Check the extracted features
        features = result["features"]
        self.assertIn("mean", features)
        self.assertIn("std", features)
        self.assertIn("min", features)
        self.assertIn("max", features)

        # Check feature values (converted from numpy arrays)
        self.assertEqual(features["mean"], [1.5, 2.5])
        self.assertEqual(features["std"], [0.5, 0.5])
        self.assertEqual(features["min"], [1.0, 2.0])
        self.assertEqual(features["max"], [2.0, 3.0])

    @patch("llama_stream.mx")
    def test_fft_feature_extraction(self, mock_mx):
        """Test FFT feature extraction."""
        # Setup mocks for MLX functions
        mock_mx.mean.return_value = np.array([1.5, 2.5])
        mock_mx.std.return_value = np.array([0.5, 0.5])
        mock_mx.min.return_value = np.array([1.0, 2.0])
        mock_mx.max.return_value = np.array([2.0, 3.0])

        # Setup mock for mx.fft
        mock_fft = MagicMock()
        mock_fft.rfft.return_value = np.array([10.0, 0.5, 0.1, 0.05, 0.01])
        mock_mx.fft = mock_fft
        mock_mx.abs.return_value = np.array([10.0, 0.5, 0.1, 0.05, 0.01])

        # Create a test window
        window = Window("test_window", start_time=0.0, end_time=1.0)

        # Add test events with numeric data
        window.add_event(StreamEvent([1, 2], "test_source"))
        window.add_event(StreamEvent([2, 3], "test_source"))

        # Setup mock for window.to_mlx_array
        window.to_mlx_array = MagicMock(return_value=np.array([[1, 2], [2, 3]]))

        # Call
