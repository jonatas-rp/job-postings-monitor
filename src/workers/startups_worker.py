"""Startups.gallery job search worker implementation"""

import logging
from datetime import datetime
from typing import Optional

from src.workers.base_worker import BaseWorker
from src.services.startups_scraper import StartupsScraperService
from src.utils.csv_writer import safe_write_csv


class StartupsWorker(BaseWorker):
    """
    Worker that searches for jobs on startups.gallery using Playwright
    
    This worker periodically scrapes startups.gallery for jobs matching
    specified keywords and processes the results.
    """
    
    def __init__(
        self,
        name: str,
        interval: int = 60,
        keywords: Optional[str] = None,
        max_results: int = 100,
        headless: bool = True,
        redis_host: Optional[str] = None,
        redis_port: Optional[int] = None,
        redis_db: Optional[int] = None
    ):
        """
        Initialize Startups worker
        
        Args:
            name: Worker name
            interval: Seconds between search cycles
            keywords: Search keywords (e.g., "python", "software engineer")
            max_results: Maximum results per search
            headless: Run browser in headless mode
            redis_host: Redis server host (defaults to env REDIS_HOST or 'localhost')
            redis_port: Redis server port (defaults to env REDIS_PORT or 6379)
            redis_db: Redis database number (defaults to env REDIS_DB or 0)
        """
        super().__init__(name, interval)
        
        self.keywords = keywords
        self.max_results = max_results
        self.headless = headless
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        
        self.search_count = 0
        self.total_jobs_found = 0
        
        # Scraper will be initialized in the worker process
        self._scraper: Optional[StartupsScraperService] = None
    
    def _initialize_scraper(self) -> None:
        """Initialize the scraper service (must be done in worker process)"""
        if self._scraper is None:
            self.logger.info(f"Initializing Startups scraper for worker '{self.name}'")
            self._scraper = StartupsScraperService(
                headless=self.headless,
                redis_host=self.redis_host,
                redis_port=self.redis_port,
                redis_db=self.redis_db
            )
            self._scraper.start()
    
    def _cleanup_scraper(self) -> None:
        """Cleanup the scraper service"""
        if self._scraper:
            self.logger.debug(f"Cleaning up Startups scraper for worker '{self.name}'")
            self._scraper.stop()
            self._scraper = None
    
    def do_work(self) -> None:
        """
        Perform Startups.gallery job search
        
        Searches startups.gallery for jobs matching configured keywords
        and logs the results.
        """
        # Initialize scraper if needed (in worker process)
        if self._scraper is None:
            self._initialize_scraper()
        
        self.search_count += 1
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        self.logger.info(
            f"[{timestamp}] Worker '{self.name}' starting Startups.gallery search #{self.search_count}"
        )
        self.logger.info(
            f"Search params - Keywords: '{self.keywords}', Max results: {self.max_results}"
        )

        try:
            # Perform search
            jobs = self._scraper.search_jobs(
                keywords=self.keywords,
                max_results=self.max_results
            )
            
            jobs_found = len(jobs)
            self.total_jobs_found += jobs_found
            
            self.logger.info(
                f"Worker '{self.name}' found {jobs_found} jobs "
                f"(Total: {self.total_jobs_found} across {self.search_count} searches)"
            )

            # Save jobs to CSV
            if jobs:
                try:
                    # Generate filename with timestamp
                    timestamp_str = datetime.now().strftime("%Y%m%d")
                    csv_filename = f"output/{self.name}_jobs_{timestamp_str}.csv"
                    
                    # Convert jobs to dictionaries
                    jobs_data = [job.to_dict() for job in jobs]
                    
                    # Write to CSV with file locking
                    success = safe_write_csv(
                        filename=csv_filename,
                        data=jobs_data,
                        logger=self.logger,
                        drop_columns=['description']
                    )
                    
                    if success:
                        self.logger.info(f"Saved {len(jobs)} jobs to {csv_filename}")
                    else:
                        self.logger.error(f"Failed to save jobs to {csv_filename}")
                except Exception as e:
                    self.logger.error(f"Failed to save jobs to CSV: {e}", exc_info=True)
            else:
                self.logger.debug("No jobs to save")
            
        except Exception as e:
            self.logger.error(
                f"Error during Startups.gallery search in worker '{self.name}': {e}",
                exc_info=True
            )
        
        self.logger.debug(f"Worker '{self.name}' completed search cycle #{self.search_count}")
    
    def __del__(self):
        """Cleanup when worker is destroyed"""
        self._cleanup_scraper()


