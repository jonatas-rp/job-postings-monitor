"""
Job Scrapper - Main Entry Point

Background job search application that monitors the web for new job openings
"""
import sys
import argparse
import logging
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logger import setup_logger
from src.config.loader import ConfigLoader
from src.workers.worker_manager import WorkerManager
from src.workers.worker_factory import WorkerFactory


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Job Scrapper - Automated job search application"
    )
    
    parser.add_argument(
        '--config',
        '-c',
        type=str,
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    parser.add_argument(
        '--log-level',
        '-l',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Override log level from config file'
    )
    
    return parser.parse_args()


def main():
    """Main entry point for job scrapper application"""
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Setup initial logging (will be updated after config loads)
    logger = setup_logger("job_scrapper")
    
    logger.info("="*60)
    logger.info("Job Scrapper Application Starting")
    logger.info("="*60)
    
    try:
        # Load configuration
        logger.info(f"Loading configuration from: {args.config}")
        config_loader = ConfigLoader(args.config)
        config = config_loader.load()
        
        # Update log level if specified in config or CLI
        log_level = args.log_level or config.log_level
        log_level_int = getattr(logging, log_level.upper())
        logger.setLevel(log_level_int)
        for handler in logger.handlers:
            handler.setLevel(log_level_int)
        
        logger.info(f"Log level set to: {log_level}")
        logger.info(f"Configuration loaded: {len(config.workers)} workers defined")
        
        # Create worker factory with app config (for Redis settings)
        factory = WorkerFactory(app_config=config)
        
        # Create workers from configuration
        enabled_configs = config.get_enabled_workers()
        logger.info(f"Creating {len(enabled_configs)} enabled workers...")
        
        workers = factory.create_workers_from_configs(enabled_configs)
        
        if not workers:
            logger.warning("No workers were created. Check your configuration.")
            logger.info("Application exiting.")
            return 0
        
        logger.info(f"Successfully created {len(workers)} workers")
        
        # Create worker manager and register workers
        manager = WorkerManager()
        manager.register_workers(workers)
        
        # Run the application
        manager.run()
        
    except FileNotFoundError as e:
        logger.error(str(e))
        logger.error("Please create a config.yaml file or specify a valid config path.")
        logger.error("See config.example.yaml for reference.")
        return 1
    
    except Exception as e:
        logger.error(f"Application error: {e}", exc_info=True)
        return 1
    
    logger.info("Application exited successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())

