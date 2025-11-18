"""Startups.gallery job scraper service using Playwright"""

import logging
import re
import time
from datetime import datetime
from typing import List, Optional, Tuple
from playwright.sync_api import sync_playwright, Browser, Page, TimeoutError as PlaywrightTimeout

from src.models.job import Job
from src.utils.redis_client import RedisClient


class StartupsScraperService:
    """
    Service for scraping jobs from startups.gallery using Playwright
    
    This service uses Playwright to scrape job listings from startups.gallery
    by searching for keywords and extracting job information.
    """
    
    BASE_URL = "https://startups.gallery/jobs"
    
    def __init__(
        self, 
        headless: bool = True, 
        timeout: int = 30000,
        redis_host: Optional[str] = None,
        redis_port: Optional[int] = None,
        redis_db: Optional[int] = None
    ):
        """
        Initialize Startups scraper
        
        Args:
            headless: Run browser in headless mode
            timeout: Default timeout for page operations in milliseconds
            redis_host: Redis server host (defaults to env REDIS_HOST or 'localhost')
            redis_port: Redis server port (defaults to env REDIS_PORT or 6379)
            redis_db: Redis database number (defaults to env REDIS_DB or 0)
        """
        self.headless = headless
        self.timeout = timeout
        self.logger = logging.getLogger("job_scrapper.service.startups")
        self._browser: Optional[Browser] = None
        self._playwright = None
        
        # Initialize Redis client for caching
        try:
            self.redis_client = RedisClient(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                default_ttl=5*86400  # 5 days
            )
            self.logger.info("Redis client initialized for job caching")
        except Exception as e:
            self.logger.error(f"Failed to initialize Redis client: {e}")
            self.redis_client = None
    
    def __enter__(self):
        """Context manager entry - start browser"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - stop browser"""
        self.stop()
    
    def start(self) -> None:
        """Start the browser with anti-detection measures"""
        if self._browser is not None:
            self.logger.warning("Browser already started")
            return
        
        self.logger.info("Starting Playwright browser with stealth mode...")
        self._playwright = sync_playwright().start()
        
        # Launch browser with anti-detection arguments
        self._browser = self._playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
            ]
        )
        
        self.logger.info("Browser started successfully")
    
    def stop(self) -> None:
        """Stop the browser and cleanup"""
        if self._browser:
            self.logger.info("Closing browser...")
            self._browser.close()
            self._browser = None
        
        if self._playwright:
            self._playwright.stop()
            self._playwright = None
        
        self.logger.info("Browser closed")
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """
        Parse date from "Posted on Nov 4, 2025" format
        
        Args:
            date_str: Date string in format "Posted on Nov 4, 2025"
        
        Returns:
            datetime object or None if parsing fails
        """
        try:
            # Remove "Posted on " prefix
            date_str = date_str.replace("Posted on ", "").strip()
            
            # Parse the date
            # Format: "Nov 4, 2025" or "Nov 04, 2025"
            date_obj = datetime.strptime(date_str, "%b %d, %Y")
            return date_obj
        except ValueError:
            # Try alternative format if first fails
            try:
                date_obj = datetime.strptime(date_str, "%B %d, %Y")  # "November 4, 2025"
                return date_obj
            except ValueError:
                self.logger.warning(f"Failed to parse date: {date_str}")
                return None
    
    def _is_today(self, date_obj: datetime) -> bool:
        """
        Check if date is today
        
        Args:
            date_obj: datetime object to check
        
        Returns:
            True if date is today, False otherwise
        """
        today = datetime.now().date()
        return date_obj.date() == today
    
    def search_jobs(
        self,
        keywords: str,
        max_results: int = 100
    ) -> List[Job]:
        """
        Search for jobs on startups.gallery
        
        Args:
            keywords: Search keywords (e.g., "python", "software engineer")
            max_results: Maximum number of results to return
        
        Returns:
            List of Job objects
        """
        if not self._browser:
            self.logger.error("Browser not started. Call start() first.")
            return []
        
        if not keywords:
            self.logger.warning("No keywords provided for search")
            return []
        
        self.logger.info(f"Searching startups.gallery for: '{keywords}'")
        
        try:
            page = self._browser.new_page()
            
            # Navigate to jobs page
            self.logger.debug(f"Navigating to {self.BASE_URL}")
            page.goto(self.BASE_URL, wait_until="networkidle", timeout=self.timeout)
            
            # Wait for search input to be visible
            self.logger.debug("Waiting for search input...")
            search_input = page.locator('input[type="text"]')
            
            # If multiple inputs, use the second one (as in original script)
            if search_input.count() > 1:
                search_input = search_input.nth(0)
            
            search_input.wait_for(state="visible", timeout=self.timeout)
            
            # Type search keyword
            self.logger.debug(f"Typing '{keywords}' in search bar...")
            search_input.fill(keywords)
            
            # Submit search by pressing Enter
            search_input.press("Enter")
            
            # Wait for results to load
            time.sleep(2)
            page.wait_for_load_state("networkidle", timeout=self.timeout)
            
            all_jobs = []
            today = datetime.now().date()
            load_more_attempts = 0
            max_load_more_attempts = 50  # Prevent infinite loops
            
            # Get the main container
            data_framer = page.locator('div[data-framer-name="Main"]')
            
            while len(all_jobs) < max_results and load_more_attempts < max_load_more_attempts:
                # Get all job links
                job_links = data_framer.locator("a")
                job_count = job_links.count()
                
                self.logger.debug(f"Found {job_count} job links on current page")
                
                if job_count == 0:
                    self.logger.debug("No more jobs found")
                    break
                
                # Extract jobs from current batch
                jobs_in_batch = []
                found_old_job = False
                
                for i in range(job_count):
                    try:
                        row = job_links.nth(i)
                        
                        # Get job URL
                        job_url = row.get_attribute("href")
                        if not job_url:
                            continue
                        
                        # Make URL absolute if relative
                        if job_url.startswith("/"):
                            job_url = f"https://startups.gallery{job_url}"
                        elif not job_url.startswith("http"):
                            job_url = f"https://startups.gallery/{job_url}"
                        
                        # Get text content from paragraph element
                        element = row.locator("p")
                        if element.count() == 0:
                            continue
                        
                        text_content = element.inner_text()
                        
                        # Split by "·" to get company, location, date
                        parts = text_content.split("·")
                        
                        if len(parts) >= 3:
                            company = parts[0].strip()
                            location = parts[1].strip()
                            date_posted_str = parts[2].strip()
                            
                            # Parse date
                            posted_date_obj = self._parse_date(date_posted_str)
                            
                            if posted_date_obj is None:
                                self.logger.warning(f"Could not parse date: {date_posted_str}")
                                continue
                            
                            # Check if date is today - if not, stop processing
                            if not self._is_today(posted_date_obj):
                                self.logger.debug(
                                    f"Found job from {posted_date_obj.date()}, "
                                    f"not today ({today}). Stopping search."
                                )
                                found_old_job = True
                                break
                            
                            # Extract job title
                            # Try to get from heading elements first
                            title = None
                            title_selectors = ["h1", "h2", "h3", "h4", "h5", "h6"]
                            for selector in title_selectors:
                                title_elem = row.locator(selector)
                                if title_elem.count() > 0:
                                    title = title_elem.first.inner_text().strip()
                                    if title:
                                        break
                            
                            # If no title found, try link text
                            if not title:
                                link_text = row.inner_text().strip()
                                if link_text:
                                    # Try to extract title from link text (before company info)
                                    lines = link_text.split("\n")
                                    if lines:
                                        title = lines[0].strip()
                            
                            # Fallback to company name if still no title
                            if not title:
                                title = company
                            
                            # Create Job object
                            job = Job(
                                title=title,
                                company=company,
                                location=location,
                                url=job_url,
                                description="",  # Can be fetched later if needed
                                source="startups",
                                posted_date=posted_date_obj.strftime("%Y-%m-%d"),
                                posted_time=None,
                                scraped_at=datetime.now()
                            )
                            
                            # Check Redis cache for duplicates
                            if self.redis_client and self.redis_client.check_and_cache_job(job):
                                self.logger.debug(f"Skipping duplicate job: {job.url}")
                                continue
                            
                            jobs_in_batch.append(job)
                                
                    except Exception as e:
                        self.logger.warning(f"Error extracting job {i}: {e}")
                        continue
                
                # Add jobs from this batch (only jobs from today)
                all_jobs.extend(jobs_in_batch)
                self.logger.debug(f"Extracted {len(jobs_in_batch)} jobs from current batch")
                
                # If we found an old job, stop processing
                if found_old_job:
                    self.logger.info("Reached jobs not from today, stopping search")
                    break
                
                # If we have enough results, stop
                if len(all_jobs) >= max_results:
                    break
                
                # Try to load more jobs
                try:
                    load_more_button = data_framer.locator("div[data-framer-name='Default']")
                    if load_more_button.count() > 0:
                        self.logger.debug("Clicking 'Load more' button...")
                        load_more_button.click()
                        load_more_attempts += 1
                        
                        # Wait for new jobs to load
                        time.sleep(2)
                        page.wait_for_load_state("networkidle", timeout=self.timeout)
                    else:
                        self.logger.debug("No 'Load more' button found, stopping")
                        break
                except Exception as e:
                    self.logger.warning(f"Error clicking load more button: {e}")
                    break
            
            page.close()
            
            # Trim to max_results
            all_jobs = all_jobs[:max_results]
            
            self.logger.info(f"Successfully scraped {len(all_jobs)} jobs from startups.gallery")
            return all_jobs
            
        except Exception as e:
            self.logger.error(f"Error searching startups.gallery jobs: {e}", exc_info=True)
            return []
    
    def fetch_job_details(self, job_url: str) -> Optional[str]:
        """
        Fetch detailed job description from job URL
        
        Args:
            job_url: URL of the job posting
        
        Returns:
            Job description text or None if fetch fails
        """
        if not self._browser:
            self.logger.error("Browser not started. Call start() first.")
            return None
        
        try:
            page = self._browser.new_page()
            page.goto(job_url, wait_until="networkidle", timeout=self.timeout)
            
            # Try to extract job description
            # This is a placeholder - adjust selectors based on actual page structure
            description_selectors = [
                'div[class*="description"]',
                'div[class*="content"]',
                'article',
                'main'
            ]
            
            description = ""
            for selector in description_selectors:
                elem = page.locator(selector)
                if elem.count() > 0:
                    description = elem.first.inner_text()
                    if description:
                        break
            
            page.close()
            
            return description if description else None
            
        except Exception as e:
            self.logger.warning(f"Error fetching job details from {job_url}: {e}")
            return None

