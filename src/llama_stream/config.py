"""
Configuration management for LlamaStream
"""
from typing import Dict, Any, Optional


class Config:
    """Configuration handler"""
    
    DEFAULT_CONFIG = {
        "timeout": 30,
        "retries": 3,
        "log_level": "info"
        
    }
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize with optional configuration overrides"""
        self.config = self.DEFAULT_CONFIG.copy()
        if config:
            self.config.update(config)
            
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        return self.config.get(key, default)
        
    def set(self, key: str, value: Any) -> None:
        """Set configuration value"""
        self.config[key] = value
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return self.config.copy()
