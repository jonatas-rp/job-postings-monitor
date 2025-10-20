"""Job search worker implementation"""

import time
from datetime import datetime
from .base_worker import BaseWorker


class JobWorker(BaseWorker):
    """
    Worker that performs job search operations
    
    Currently implements a simple print and sleep cycle for testing
    """
    
    def __init__(self, name: str, interval: int = 60, message: str = "Working..."):
        """
        Initialize job worker
        
        Args:
            name: Worker name
            interval: Seconds between work cycles
            message: Message to print during work
        """
        super().__init__(name, interval)
        self.message = message
        self.work_count = 0
    
    def do_work(self) -> None:
        """
        Perform job search work
        
        Currently prints a message and sleeps briefly
        """
        self.work_count += 1
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        self.logger.info(
            f"[{timestamp}] Worker '{self.name}' performing work cycle #{self.work_count}: {self.message}"
        )
        
        # Simulate some work being done
        time.sleep(2)
        
        self.logger.debug(f"Worker '{self.name}' completed work cycle #{self.work_count}")




