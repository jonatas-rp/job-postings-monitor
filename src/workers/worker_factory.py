"""Factory for creating workers from configuration"""

import logging
from typing import Optional

from src.models.config import WorkerConfig, AppConfig
from src.workers.base_worker import BaseWorker
from src.workers.job_worker import JobWorker
from src.workers.linkedin_worker import LinkedInWorker


class WorkerFactory:
    """
    Factory class for creating worker instances from configuration
    
    This factory maps worker types to their concrete implementations
    """
    
    # Registry of worker types to their classes
    WORKER_TYPES = {
        'job': JobWorker,
        'linkedin': LinkedInWorker,
        # Future worker types can be added here:
        # 'google': GoogleWorker,
        # 'indeed': IndeedWorker,
    }
    
    def __init__(self, app_config: Optional[AppConfig] = None):
        """
        Initialize worker factory
        
        Args:
            app_config: Application configuration (used to access Redis settings)
        """
        self.logger = logging.getLogger("job_scrapper.factory")
        self.app_config = app_config
    
    def create_worker(self, config: WorkerConfig) -> Optional[BaseWorker]:
        """
        Create a worker instance from configuration
        
        Args:
            config: Worker configuration
        
        Returns:
            Worker instance or None if worker type is not registered
        
        Raises:
            ValueError: If worker type is not supported
        """
        worker_type = config.type.lower()
        
        if worker_type not in self.WORKER_TYPES:
            supported = ", ".join(self.WORKER_TYPES.keys())
            raise ValueError(
                f"Unknown worker type '{config.type}'. "
                f"Supported types: {supported}"
            )
        
        worker_class = self.WORKER_TYPES[worker_type]
        
        self.logger.debug(
            f"Creating worker '{config.name}' of type '{config.type}' "
            f"with interval {config.interval}s"
        )
        
        # Create worker instance based on type
        if worker_type == 'job':
            return worker_class(
                name=config.name,
                interval=config.interval,
                message=config.message or f"Worker {config.name} working..."
            )
        
        elif worker_type == 'linkedin':
            # Extract LinkedIn-specific config
            linkedin_config = config.config
            
            # Get Redis configuration from app_config if available
            redis_host = None
            redis_port = None
            redis_db = None
            if self.app_config:
                redis_host = self.app_config.redis_host
                redis_port = self.app_config.redis_port
                redis_db = self.app_config.redis_db
            
            return worker_class(
                name=config.name,
                interval=config.interval,
                keywords=linkedin_config.get('keywords'),
                last_time_posted=linkedin_config.get('last_time_posted'),
                location=linkedin_config.get('location'),
                job_type=linkedin_config.get('job_type'),
                workplace_type=linkedin_config.get('workplace_type'),
                experience_level=linkedin_config.get('experience_level'),
                excluded_companies=linkedin_config.get('excluded_companies'),
                max_results=linkedin_config.get('max_results', 25),
                headless=linkedin_config.get('headless', True),
                redis_host=redis_host,
                redis_port=redis_port,
                redis_db=redis_db
            )
        
        # Future worker types will have their own instantiation logic
        
        return None
    
    def create_workers_from_configs(self, configs: list) -> list:
        """
        Create multiple worker instances from configurations
        
        Args:
            configs: List of WorkerConfig instances
        
        Returns:
            List of created worker instances
        """
        workers = []
        
        for config in configs:
            if not config.enabled:
                self.logger.info(f"Skipping disabled worker: {config.name}")
                continue
            
            try:
                worker = self.create_worker(config)
                if worker:
                    workers.append(worker)
                    self.logger.info(
                        f"Created worker: {config.name} "
                        f"(type={config.type}, interval={config.interval}s)"
                    )
            except Exception as e:
                self.logger.error(
                    f"Failed to create worker '{config.name}': {e}",
                    exc_info=True
                )
        
        return workers
    
    @classmethod
    def register_worker_type(cls, type_name: str, worker_class: type):
        """
        Register a new worker type
        
        Args:
            type_name: Name of the worker type
            worker_class: Worker class (must inherit from BaseWorker)
        
        Raises:
            TypeError: If worker_class doesn't inherit from BaseWorker
        """
        if not issubclass(worker_class, BaseWorker):
            raise TypeError(
                f"Worker class must inherit from BaseWorker, "
                f"got {worker_class.__name__}"
            )
        
        cls.WORKER_TYPES[type_name.lower()] = worker_class


