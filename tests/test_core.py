"""Tests for core functionality"""
import pytest
from llama_stream.core import Client


def test_client_initialization():
    """Test that client initializes properly"""
    client = Client()
    assert client is not None
    assert client.config == {}
    
    # Test with config
    config = {"timeout": 60}
    client = Client(config=config)
    assert client.config == config


def test_client_process():
    """Test processing functionality"""
    client = Client()
    result = client.process("test data")
    assert isinstance(result, dict)
    assert "result" in result
    assert "input" in result
    assert result["input"] == "test data"


def test_client_status():
    """Test status functionality"""
    client = Client()
    status = client.get_status()
    assert isinstance(status, dict)
    assert "status" in status


def test_get_version():
    """Test getting version"""
    client = Client()
    version = client.get_version()
    assert isinstance(version, str)
    assert version == "0.1.0"
    
    
def test_get_info():
    """Test getting info"""
    client = Client()
    info = client.get_info()
    assert isinstance(info, dict)
    assert "name" in info
    assert "version" in info
    assert "features" in info
