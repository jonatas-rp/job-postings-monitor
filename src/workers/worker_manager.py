"""Worker lifecycle management"""

import signal
import sys
import time
from typing import List, Dict
import logging
from .base_worker import BaseWorker


class WorkerManager:
    """
    Manages the lifecycle of multiple workers
    
    Responsibilities:
    - Register workers
    - Start all workers as separate processes
    - Stop all workers gracefully
    - Handle shutdown signals
    - Monitor worker health
    
    Note: Workers run in separate processes using multiprocessing
    """
    
    def __init__(self):
        """Initialize worker manager"""
        self.logger = logging.getLogger("job_scrapper.manager")
        self.workers: Dict[str, BaseWorker] = {}
        self._shutdown_requested = False
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def register_worker(self, worker: BaseWorker) -> None:
        """
        Register a worker with the manager
        
        Args:
            worker: Worker instance to register
        """
        if worker.name in self.workers:
            self.logger.warning(f"Worker {worker.name} already registered, replacing...")
        
        self.workers[worker.name] = worker
        self.logger.info(f"Registered worker: {worker.name}")
    
    def register_workers(self, workers: List[BaseWorker]) -> None:
        """
        Register multiple workers at once
        
        Args:
            workers: List of worker instances to register
        """
        for worker in workers:
            self.register_worker(worker)
    
    def start_all(self) -> None:
        """Start all registered workers"""
        if not self.workers:
            self.logger.warning("No workers registered to start")
            return
        
        self.logger.info(f"Starting {len(self.workers)} workers...")
        
        for name, worker in self.workers.items():
            try:
                worker.start()
            except Exception as e:
                self.logger.error(f"Failed to start worker {name}: {e}", exc_info=True)
        
        running_count = sum(1 for w in self.workers.values() if w.is_running())
        self.logger.info(f"Started {running_count}/{len(self.workers)} workers successfully")
    
    def stop_all(self, timeout: int = 10) -> None:
        """
        Stop all workers gracefully
        
        Args:
            timeout: Maximum time to wait for each worker to stop
        """
        if not self.workers:
            self.logger.info("No workers to stop")
            return
        
        self.logger.info(f"Stopping {len(self.workers)} workers...")
        
        for name, worker in self.workers.items():
            try:
                if worker.is_running():
                    worker.stop(timeout=timeout)
            except Exception as e:
                self.logger.error(f"Error stopping worker {name}: {e}", exc_info=True)
        
        self.logger.info("All workers stopped")
    
    def get_status(self) -> Dict[str, dict]:
        """
        Get status of all workers
        
        Returns:
            Dictionary mapping worker names to their status info
        """
        status = {}
        for name, worker in self.workers.items():
            status[name] = {
                "name": worker.name,
                "running": worker.is_running(),
                "interval": worker.interval
            }
        return status
    
    def print_status(self) -> None:
        """Print status of all workers to console"""
        status = self.get_status()
        
        print("\n" + "="*60)
        print("WORKER STATUS")
        print("="*60)
        
        for name, info in status.items():
            status_str = "RUNNING" if info["running"] else "STOPPED"
            print(f"  {name:20s} | {status_str:10s} | Interval: {info['interval']}s")
        
        print("="*60 + "\n")
    
    def wait_for_shutdown(self) -> None:
        """
        Block until shutdown is requested
        
        Keeps the main process alive while workers run in background processes
        """
        self.logger.info("Application running. Press Ctrl+C to stop.")
        
        try:
            # Keep main process alive
            # Use sleep loop instead of signal.pause() for better multiprocessing compatibility
            while not self._shutdown_requested:
                time.sleep(0.5)  # Check every 0.5 seconds
        except KeyboardInterrupt:
            pass
        
        self.logger.info("Shutdown signal received")
    
    def _signal_handler(self, signum, frame) -> None:
        """
        Handle shutdown signals
        
        Args:
            signum: Signal number
            frame: Current stack frame
        """
        signal_name = signal.Signals(signum).name
        self.logger.info(f"Received signal: {signal_name}")
        self._shutdown_requested = True
    
    def run(self) -> None:
        """
        Run the worker manager
        
        Starts all workers and waits for shutdown signal
        """
        self.logger.info("="*60)
        self.logger.info("Job Scrapper Worker Manager Starting")
        self.logger.info("="*60)
        
        try:
            self.start_all()
            self.print_status()
            self.wait_for_shutdown()
            
        except Exception as e:
            self.logger.error(f"Fatal error in worker manager: {e}", exc_info=True)
            
        finally:
            self.logger.info("Initiating graceful shutdown...")
            self.stop_all()
            self.logger.info("Shutdown complete")

