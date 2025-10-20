"""Base worker class for job scrapper"""

from abc import ABC, abstractmethod
from multiprocessing import Process, Event
from typing import Optional
import logging


class BaseWorker(ABC):
    """
    Abstract base class for workers
    
    Workers run background tasks in separate processes with lifecycle management
    """
    
    def __init__(self, name: str, interval: int = 60):
        """
        Initialize base worker
        
        Args:
            name: Worker name for identification
            interval: Time in seconds between work cycles
        """
        self.name = name
        self.interval = interval
        
        # Logger will be recreated in the worker process
        self.logger = None
        
        self._process: Optional[Process] = None
        self._stop_event = Event()
        self._running = False
    
    @abstractmethod
    def do_work(self) -> None:
        """
        Implement the actual work to be done by the worker
        
        This method is called repeatedly at the specified interval
        """
        pass
    
    def start(self) -> None:
        """Start the worker in a background process"""
        # Initialize logger in main process for startup messages
        if self.logger is None:
            self.logger = logging.getLogger(f"job_scrapper.worker.{self.name}")
        
        if self._running:
            self.logger.warning(f"Worker {self.name} is already running")
            return
        
        self.logger.info(f"Starting worker: {self.name}")
        self._stop_event.clear()
        self._running = True
        
        # Create and start process
        self._process = Process(target=self._run_loop, name=self.name, daemon=True)
        self._process.start()
        
        self.logger.info(f"Worker {self.name} started successfully (PID: {self._process.pid})")
    
    def stop(self, timeout: int = 10) -> None:
        """
        Stop the worker gracefully
        
        Args:
            timeout: Maximum time to wait for worker to stop (seconds)
        """
        # Initialize logger if needed
        if self.logger is None:
            self.logger = logging.getLogger(f"job_scrapper.worker.{self.name}")
        
        if not self._running:
            self.logger.warning(f"Worker {self.name} is not running")
            return
        
        self.logger.info(f"Stopping worker: {self.name}")
        self._stop_event.set()
        
        if self._process and self._process.is_alive():
            self._process.join(timeout=timeout)
            
            if self._process.is_alive():
                self.logger.error(f"Worker {self.name} did not stop within timeout")
                # Force terminate if still alive
                self.logger.warning(f"Forcing termination of worker {self.name}")
                self._process.terminate()
                self._process.join(timeout=2)
            else:
                self.logger.info(f"Worker {self.name} stopped successfully")
        
        self._running = False
    
    def is_running(self) -> bool:
        """Check if worker process is currently running"""
        return self._running and self._process is not None and self._process.is_alive()
    
    def _run_loop(self) -> None:
        """
        Main worker loop (runs in separate process)
        
        Continuously executes do_work() at specified intervals until stopped
        """
        # Recreate logger in the worker process (necessary for multiprocessing)
        import os
        self.logger = logging.getLogger(f"job_scrapper.worker.{self.name}")
        
        self.logger.debug(f"Worker {self.name} entering run loop (PID: {os.getpid()})")
        
        while not self._stop_event.is_set():
            try:
                self.logger.debug(f"Worker {self.name} executing work cycle")
                self.do_work()
                
            except Exception as e:
                self.logger.error(
                    f"Error in worker {self.name}: {type(e).__name__}: {str(e)}",
                    exc_info=True
                )
            
            # Wait for interval or until stop signal
            self.logger.debug(f"Worker {self.name} waiting {self.interval}s until next cycle")
            self._stop_event.wait(timeout=self.interval)
        
        self.logger.debug(f"Worker {self.name} exiting run loop")

