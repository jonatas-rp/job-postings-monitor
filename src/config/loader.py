"""Configuration loader for job scrapper"""

import os
import logging
from pathlib import Path
from typing import Optional
import yaml

from src.models.config import AppConfig


class ConfigLoader:
    """
    Loads and validates application configuration from YAML files
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration loader
        
        Args:
            config_path: Path to configuration file (defaults to config.yaml in root)
        """
        self.logger = logging.getLogger("job_scrapper.config")
        
        if config_path is None:
            # Default to config.yaml in project root
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config.yaml"
        
        self.config_path = Path(config_path)
        self.logger.debug(f"Configuration path: {self.config_path}")
    
    def load(self) -> AppConfig:
        """
        Load configuration from file
        
        Returns:
            AppConfig instance
        
        Raises:
            FileNotFoundError: If configuration file doesn't exist
            ValueError: If configuration is invalid
            yaml.YAMLError: If YAML parsing fails
        """
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {self.config_path}\n"
                f"Please create config.yaml in the project root or specify a custom path."
            )
        
        self.logger.info(f"Loading configuration from: {self.config_path}")
        
        try:
            with open(self.config_path, 'r') as f:
                config_data = yaml.safe_load(f)
            
            if config_data is None:
                raise ValueError("Configuration file is empty")
            
            # Parse and validate configuration
            app_config = AppConfig.from_dict(config_data)
            
            self.logger.info(
                f"Configuration loaded successfully: "
                f"{len(app_config.workers)} workers defined, "
                f"{len(app_config.get_enabled_workers())} enabled"
            )
            
            return app_config
            
        except yaml.YAMLError as e:
            self.logger.error(f"Failed to parse YAML configuration: {e}")
            raise
        
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            raise
    
    @staticmethod
    def load_from_path(path: str) -> AppConfig:
        """
        Convenience method to load configuration from a specific path
        
        Args:
            path: Path to configuration file
        
        Returns:
            AppConfig instance
        """
        loader = ConfigLoader(path)
        return loader.load()
    
    @staticmethod
    def load_default() -> AppConfig:
        """
        Load configuration from default location (config.yaml in project root)
        
        Returns:
            AppConfig instance
        """
        loader = ConfigLoader()
        return loader.load()




