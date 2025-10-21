"""LinkedIn job search worker implementation"""

import logging
import os
import pandas as pd
from datetime import datetime
from typing import Optional, List

from src.workers.base_worker import BaseWorker
from src.services.linkedin_scraper import LinkedInScraperService


class LinkedInWorker(BaseWorker):
    """
    Worker that searches for jobs on LinkedIn using Playwright
    
    This worker periodically scrapes LinkedIn for jobs matching
    specified criteria and processes the results.
    """
    
    def __init__(
        self,
        name: str,
        interval: int = 60,
        keywords: Optional[List[str]] = None,
        location: Optional[str] = None,
        job_type: Optional[str] = None,
        workplace_type: Optional[str] = None,
        last_time_posted: int = 86400,
        excluded_companies: Optional[List[str]] = None,
        experience_level: Optional[str] = None,
        max_results: int = 25,
        headless: bool = True,
        redis_host: Optional[str] = None,
        redis_port: Optional[int] = None,
        redis_db: Optional[int] = None
    ):
        """
        Initialize LinkedIn worker
        
        Args:
            name: Worker name
            interval: Seconds between search cycles
            keywords: List of job search keywords/technologies to match (e.g., ["python", "docker"])
            location: Job location
            job_type: Job type filter (F=Full-time, P=Part-time, C=Contract, etc.)
            workplace_type: Workplace type (1=On-site, 2=Remote, 3=Hybrid)
            experience_level: Experience level (1=Internship, 2=Entry, 3=Associate, 4=Mid-Senior, etc.)
            max_results: Maximum results per search
            headless: Run browser in headless mode
            redis_host: Redis server host (defaults to env REDIS_HOST or 'localhost')
            redis_port: Redis server port (defaults to env REDIS_PORT or 6379)
            redis_db: Redis database number (defaults to env REDIS_DB or 0)
        """
        super().__init__(name, interval)
        
        self.keywords = keywords
        self.location = location
        self.job_type = job_type
        self.workplace_type = workplace_type
        self.experience_level = experience_level
        self.last_time_posted = last_time_posted
        self.excluded_companies = excluded_companies
        self.max_results = max_results
        self.headless = headless
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        
        self.search_count = 0
        self.total_jobs_found = 0
        
        # Scraper will be initialized in the worker process
        self._scraper: Optional[LinkedInScraperService] = None
    
    def _initialize_scraper(self) -> None:
        """Initialize the scraper service (must be done in worker process)"""
        if self._scraper is None:
            self.logger.info(f"Initializing LinkedIn scraper for worker '{self.name}'")
            self._scraper = LinkedInScraperService(
                headless=self.headless,
                redis_host=self.redis_host,
                redis_port=self.redis_port,
                redis_db=self.redis_db
            )
            self._scraper.start()
    
    def _cleanup_scraper(self) -> None:
        """Cleanup the scraper service"""
        if self._scraper:
            self.logger.debug(f"Cleaning up LinkedIn scraper for worker '{self.name}'")
            self._scraper.stop()
            self._scraper = None
    
    def do_work(self) -> None:
        """
        Perform LinkedIn job search
        
        Searches LinkedIn for jobs matching configured criteria
        and logs the results.
        """
        # Initialize scraper if needed (in worker process)
        if self._scraper is None:
            self._initialize_scraper()
        
        self.search_count += 1
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        self.logger.info(
            f"[{timestamp}] Worker '{self.name}' starting LinkedIn search #{self.search_count}"
        )
        self.logger.info(
            f"Search params - Keywords: '{self.keywords}', Location: '{self.location}', "
            f"Max results: {self.max_results}"
        )

        try:
            # Perform search
            jobs = self._scraper.search_jobs(
                keywords=self.keywords,
                last_time_posted=self.last_time_posted,
                location=self.location,
                job_type=self.job_type,
                workplace_type=self.workplace_type,
                experience_level=self.experience_level,
                excluded_companies=self.excluded_companies,
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
                    # Create output directory if it doesn't exist
                    output_dir = "output"
                    os.makedirs(output_dir, exist_ok=True)
                    
                    # Generate filename with timestamp
                    timestamp_str = datetime.now().strftime("%Y%m%d")
                    csv_filename = f"{output_dir}/{self.name}_jobs_{timestamp_str}.csv"
                    
                    # Convert jobs to DataFrame
                    jobs_data = [job.to_dict() for job in jobs]
                    new_df = pd.DataFrame(jobs_data)
                    
                    # Remove description column from DataFrame
                    new_df = new_df.drop(columns=['description'], errors='ignore')
                    
                    # Check if CSV file already exists
                    if os.path.exists(csv_filename):
                        # Load existing data
                        existing_df = pd.read_csv(csv_filename)
                        self.logger.info(f"Loading existing data from {csv_filename} ({len(existing_df)} existing jobs)")
                        
                        # Append new data to existing DataFrame
                        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                        
                        # Remove duplicates based on URL (keeps first occurrence)
                        if 'url' in combined_df.columns:
                            original_count = len(combined_df)
                            combined_df = combined_df.drop_duplicates(subset=['url'], keep='first')
                            duplicates_removed = original_count - len(combined_df)
                            if duplicates_removed > 0:
                                self.logger.info(f"Removed {duplicates_removed} duplicate job(s)")
                        
                        self.logger.info(f"Combined data: {len(combined_df)} total jobs")
                    else:
                        combined_df = new_df
                        self.logger.info(f"Creating new CSV file: {csv_filename}")
                    
                    # Save to CSV
                    combined_df.to_csv(csv_filename, index=False, encoding='utf-8')
                    
                    self.logger.info(f"Saved {len(combined_df)} jobs to {csv_filename}")
                except Exception as e:
                    self.logger.error(f"Failed to save jobs to CSV: {e}", exc_info=True)
            else:
                self.logger.debug("No jobs to save")
            
            # Here you would typically:
            # 1. Check cache to see if jobs are new
            # 2. Evaluate jobs against matching criteria
            # 3. Save matched jobs to output
            # 4. Send notifications for high-match jobs
            # For now, we just log them
            
        except Exception as e:
            self.logger.error(
                f"Error during LinkedIn search in worker '{self.name}': {e}",
                exc_info=True
            )
        
        self.logger.debug(f"Worker '{self.name}' completed search cycle #{self.search_count}")
    
    def __del__(self):
        """Cleanup when worker is destroyed"""
        self._cleanup_scraper()



