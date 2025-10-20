"""Configuration data models"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class WorkerConfig:
    """
    Configuration for a single worker
    
    Attributes:
        name: Unique worker identifier
        type: Worker type (e.g., 'job', 'google', 'linkedin')
        interval: Seconds between work cycles
        enabled: Whether this worker should run
        message: Custom message for the worker (optional)
        config: Additional worker-specific configuration
    """
    name: str
    type: str
    interval: int = 60
    enabled: bool = True
    message: Optional[str] = None
    config: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.name:
            raise ValueError("Worker name cannot be empty")
        
        if not self.type:
            raise ValueError("Worker type cannot be empty")
        
        if self.interval <= 0:
            raise ValueError(f"Worker interval must be positive, got {self.interval}")
    
    @classmethod
    def from_dict(cls, data: dict) -> 'WorkerConfig':
        """
        Create WorkerConfig from dictionary
        
        Args:
            data: Dictionary containing worker configuration
        
        Returns:
            WorkerConfig instance
        """
        return cls(
            name=data.get('name', ''),
            type=data.get('type', ''),
            interval=data.get('interval', 60),
            enabled=data.get('enabled', True),
            message=data.get('message'),
            config=data.get('config', {})
        )


@dataclass
class AppConfig:
    """
    Application-wide configuration
    
    Attributes:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        workers: List of worker configurations
        redis_host: Redis server host
        redis_port: Redis server port
        redis_db: Redis database number
        output_dir: Directory for output files
    """
    log_level: str = "INFO"
    workers: List[WorkerConfig] = field(default_factory=list)
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    output_dir: str = "output"
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level.upper() not in valid_log_levels:
            raise ValueError(
                f"Invalid log level '{self.log_level}'. "
                f"Must be one of: {', '.join(valid_log_levels)}"
            )
    
    def get_enabled_workers(self) -> List[WorkerConfig]:
        """
        Get only enabled workers
        
        Returns:
            List of enabled worker configurations
        """
        return [w for w in self.workers if w.enabled]
    
    def get_worker_by_name(self, name: str) -> Optional[WorkerConfig]:
        """
        Get worker configuration by name
        
        Args:
            name: Worker name to find
        
        Returns:
            WorkerConfig if found, None otherwise
        """
        for worker in self.workers:
            if worker.name == name:
                return worker
        return None
    
    @classmethod
    def from_dict(cls, data: dict) -> 'AppConfig':
        """
        Create AppConfig from dictionary
        
        Args:
            data: Dictionary containing application configuration
        
        Returns:
            AppConfig instance
        """
        # Parse worker configurations
        workers = []
        for worker_data in data.get('workers', []):
            workers.append(WorkerConfig.from_dict(worker_data))
        
        return cls(
            log_level=data.get('log_level', 'INFO'),
            workers=workers,
            redis_host=data.get('redis', {}).get('host', 'localhost'),
            redis_port=data.get('redis', {}).get('port', 6379),
            redis_db=data.get('redis', {}).get('db', 0),
            output_dir=data.get('output_dir', 'output')
        )




