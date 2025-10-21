"""LinkedIn job scraper service using Playwright"""

import logging
import re
import time
from typing import List, Optional, Tuple
from urllib.parse import urlencode
from playwright.sync_api import sync_playwright, Browser, Page, TimeoutError as PlaywrightTimeout

from src.models.job import Job
from src.utils.redis_client import RedisClient
from src.utils.string_matcher import match_all_keywords


class LinkedInScraperService:
    """
    Service for scraping LinkedIn jobs using Playwright
    
    This service uses Playwright to scrape job listings from LinkedIn's
    internal API endpoint without requiring authentication.
    """
    
    BASE_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    
    def __init__(
        self, 
        headless: bool = True, 
        timeout: int = 10000,
        redis_host: Optional[str] = None,
        redis_port: Optional[int] = None,
        redis_db: Optional[int] = None
    ):
        """
        Initialize LinkedIn scraper
        
        Args:
            headless: Run browser in headless mode
            timeout: Default timeout for page operations in milliseconds
            redis_host: Redis server host (defaults to env REDIS_HOST or 'localhost')
            redis_port: Redis server port (defaults to env REDIS_PORT or 6379)
            redis_db: Redis database number (defaults to env REDIS_DB or 0)
        """
        self.headless = headless
        self.timeout = timeout
        self.logger = logging.getLogger("job_scrapper.service.linkedin")
        self._browser: Optional[Browser] = None
        self._playwright = None
        
        # Initialize Redis client for caching
        try:
            self.redis_client = RedisClient(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                default_ttl=86400  # 24 hours
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
                '--disable-blink-features=AutomationControlled',  # Hide automation
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
        
        # Close Redis connection
        if self.redis_client:
            self.redis_client.close()
    
    def _create_stealth_page(self) -> Page:
        """
        Create a new page with stealth settings to avoid bot detection
        
        Returns:
            Page with anti-detection measures applied
        """
        page = self._browser.new_page()
        
        # Set realistic user agent
        page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        })
        
        # Set viewport to common resolution
        page.set_viewport_size({'width': 1920, 'height': 1080})
        
        # Remove webdriver property
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Override plugins and mimeTypes to look like real browser
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // Override languages to look realistic
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
            
            // Chrome-specific properties
            window.chrome = {
                runtime: {}
            };
            
            // Permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
        """)
        
        return page
    
    def search_jobs(
        self,
        keywords: List[str],
        last_time_posted: int = 86400,
        location: Optional[str] = None,
        job_type: Optional[str] = None,
        workplace_type: Optional[str] = None,
        experience_level: Optional[str] = None,
        excluded_companies: Optional[List[str]] = None,
        max_results: int = 100
    ) -> List[Job]:
        """
        Search for jobs on LinkedIn using the internal API endpoint
        
        Args:
            keywords: Job search keywords (required)
            last_time_posted: Time in seconds (86400=24h, 604800=week, 2592000=month) - converts to f_TPR
            location: Job location
            job_type: Job type filter (F=Full-time, P=Part-time, C=Contract, etc.) - maps to f_JT
            workplace_type: Workplace type (1=On-site, 2=Remote, 3=Hybrid) - maps to f_WT
            experience_level: Experience level (1=Internship, 2=Entry, 3=Associate, 4=Mid-Senior, etc.)
            excluded_companies: List of company names to exclude (added as NOT clause to keywords)
            max_results: Maximum number of results to fetch
        
        Returns:
            List of Job objects
            
        Example:
            If keywords="Python Developer" and excluded_companies=["Netvagas", "Valtech"],
            the search will use: "Python Developer NOT(Netvagas OR Valtech)"
        """
        if self._browser is None:
            raise RuntimeError("Browser not started. Call start() first or use context manager.")
        
        self.logger.info(f"Searching LinkedIn jobs: keywords={keywords}, location={location}")
        
        # Build keywords with NOT clause for excluded companies
        search_keywords = keywords
        if excluded_companies:
            # Create NOT clause: NOT(Company1 OR Company2 OR Company3)
            keywords_clause = " AND ".join(keywords)
            not_clause = " OR ".join(excluded_companies)
            search_keywords = f"({keywords_clause}) NOT({not_clause})"
            self.logger.info(f"Excluding companies via NOT clause: {excluded_companies}")
            self.logger.debug(f"Final search keywords: {search_keywords}")
        
        all_jobs = []
        start = 0
        jobs_per_page = 10  # LinkedIn API returns 10 jobs per page
        
        try:
            page = self._create_stealth_page()
            page.set_default_timeout(self.timeout)
            
            # Fetch jobs in batches of 10 until we reach max_results
            while len(all_jobs) < max_results:
                # Build search URL with pagination
                params = {
                    'start': start,
                    'f_SB': '1'  # Always sort by most recent
                }
                if search_keywords:
                    params['keywords'] = search_keywords
                if location:
                    params['location'] = location
                if job_type:
                    params['f_JT'] = job_type
                if workplace_type:
                    params['f_WT'] = workplace_type
                if experience_level:
                    params['f_E'] = experience_level
                if last_time_posted:
                    params['f_TPR'] = f"r{last_time_posted}"
                
                url = f"{self.BASE_URL}?{urlencode(params)}"
                
                self.logger.debug(f"Fetching page with start={start}, URL: {url}")
                
                # Navigate to search page with proper wait strategy
                try:
                    page.goto(url, wait_until='domcontentloaded', timeout=30000)
                    
                    # Wait for job listings to be present (with timeout)
                    # LinkedIn API returns jobs in <li> elements
                    page.wait_for_selector('li', timeout=10000, state='attached')
                    
                    # Small delay to ensure page is fully stable
                    time.sleep(time.time() % 3)
                    
                except PlaywrightTimeout:
                    self.logger.error(f"Timeout waiting for job listings at {url}")
                    break
                
                # Extract job listings from this page
                jobs, found = self._extract_jobs_from_page(page)
                
                if not found:
                    # No more jobs found
                    self.logger.debug("No more jobs found, stopping pagination")
                    break

                for job in jobs:
                    if self._evaluate_job_details(job.description, keywords):
                        all_jobs.append(job)
                
                self.logger.debug(f"Fetched {len(jobs)} jobs, total: {len(all_jobs)}")
                
                # Move to next page
                start += jobs_per_page
                
                # Longer delay to appear more human-like and avoid detection
                time.sleep(time.time() % 3)  # Random delay between 3-5 seconds
            
            page.close()
            
            # Trim to max_results
            all_jobs = all_jobs[:max_results]
            
            self.logger.info(f"Successfully scraped {len(all_jobs)} jobs from LinkedIn")
            return all_jobs
            
        except Exception as e:
            self.logger.error(f"Error searching LinkedIn jobs: {e}", exc_info=True)
            return []
    
    def _extract_jobs_from_page(self, page: Page) -> Tuple[List[Job], bool]:
        """
        Extract job information from the current page

        Args:
            page: Playwright page object
        
        Returns:
            Tuple of (List of Job objects, found flag)
        """
        jobs = []
        found = False
        
        try:
            # Check if page is still valid
            if page.is_closed():
                self.logger.error("Page is closed, cannot extract jobs")
                return jobs, found
            
            # Get all job list items (API endpoint returns simple li elements)
            job_elements = page.locator('li')
            
            # Try to count elements, if context is destroyed, return empty
            try:
                job_count = job_elements.count()
            except Exception as e:
                self.logger.error(f"Error counting job elements (context destroyed?): {e}")
                return jobs, found
            
            self.logger.debug(f"Found {job_count} job elements on page")
            
            if job_count == 0:
                return jobs, found
            
            # Extract all job elements at once to avoid context issues
            for i in range(job_count):
                try:
                    # Re-locate element each time to avoid stale references
                    job_elem = page.locator('li').nth(i)
                    
                    # Quick check if element still exists
                    if job_elem.count() == 0:
                        continue
                    
                    job = self._extract_job_from_element(job_elem)
                    if job:
                        found = True
                        
                        # Fetch job details (this creates a new page, shouldn't affect current page)
                        job_details = self.fetch_job_details(job.url)
                        if job_details:
                            job.description = job_details
                        
                        # Check Redis cache for duplicates
                        if self.redis_client and self.redis_client.check_and_cache_job(job):
                            self.logger.debug(f"Skipping duplicate job: {job.url}")
                            continue
                        
                        jobs.append(job)
                        
                except Exception as e:
                    self.logger.warning(f"Error extracting job {i}: {e}")
                    # Continue to next job even if one fails
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error extracting jobs from page: {e}", exc_info=True)
        
        return jobs, found
    
    def _extract_job_from_element(self, elem) -> Optional[Job]:
        """
        Extract job information from a single job list element
        
        Args:
            elem: Playwright Locator for job element
        
        Returns:
            Job object or None if extraction fails
        """
        try:
            # Extract job title (h3 element)
            title_elem = elem.locator('h3')
            title = title_elem.inner_text().strip() if title_elem.count() > 0 else "Unknown"
            
            # Extract company name (h4 element)
            company_elem = elem.locator('h4')
            company = company_elem.inner_text().strip() if company_elem.count() > 0 else "Unknown"
            
            # Extract job link (a element with specific tracking attribute)
            link_elem = elem.locator("a[data-tracking-control-name='public_jobs_jserp-result_search-card']")
            url = link_elem.get_attribute('href').split('?')[0]
            
            # Skip if we couldn't get basic info
            if title == "Unknown" or not url:
                return None
            
            # Extract location (may be in various places, try common selectors)
            location = "Not specified"
            location_selectors = ['.job-search-card__location', 'span.job-search-card__location']
            for selector in location_selectors:
                location_elem = elem.locator(selector)
                if location_elem.count() > 0:
                    location = location_elem.inner_text().strip()
                    break
            
            # Extract posted date if available
            posted_date = None
            time_elem = elem.locator('time')
            if time_elem.count() > 0:
                posted_date = time_elem.get_attribute('datetime')
                posted_time = self._parse_time_ago_to_seconds(time_elem.inner_text().strip())
                
            # Create Job object
            job = Job(
                title=title,
                company=company,
                location=location,
                url=url,
                source='linkedin',
                posted_date=posted_date,
                posted_time=posted_time
            )
            
            return job
            
        except Exception as e:
            self.logger.warning(f"Failed to extract job from element: {e}")
            return None
    
    @staticmethod
    def _parse_time_ago_to_seconds(time_string: str) -> Optional[int]:
        """
        Convert a time string like "1 hour ago" to seconds
        
        Args:
            time_string: Time string in format "{number} {unit} ago"
                        Units: minute(s), hour(s), day(s)
        
        Returns:
            Integer representing seconds, or None if parsing fails
        
        Examples:
            "1 hour ago" -> 3600
            "30 minutes ago" -> 1800
            "2 days ago" -> 172800
        """
        if not time_string:
            return None
        
        # Pattern to match: number followed by time unit
        pattern = r'(\d+)\s+(minute|minutes|hour|hours|day|days)\s+ago'
        match = re.search(pattern, time_string.lower())
        
        if not match:
            return None
        
        number = int(match.group(1))
        unit = match.group(2)
        
        # Convert to seconds based on unit
        if unit in ['minute', 'minutes']:
            return number * 60
        elif unit in ['hour', 'hours']:
            return number * 3600
        elif unit in ['day', 'days']:
            return number * 86400
        
        return None
    
    def fetch_job_details(self, job_url: str) -> Optional[dict]:
        """
        Fetch detailed job description from job URL
        
        Args:
            job_url: URL of the job posting
        
        Returns:
            Dictionary with job details or None if fetch fails
        """
        
        page = None
        try:
            page = self._create_stealth_page()
            page.set_default_timeout(self.timeout)
            
            # Random delay to appear more human-like
            time.sleep(time.time() % 3)  # Random delay between 2-3.5 seconds
            self.logger.debug(f"Fetching job details from: {job_url}")
            # Wait for network to be idle to ensure page is fully loaded
            # Navigate to job URL with retry logic
            count = 3
            success = False
            while count > 0 and not success:
                try:
                    page.goto(job_url, wait_until='domcontentloaded')
                    html = page.content()
                    if """<html><head>\n    <meta http-equiv="refresh" content="1;url=https://www.linkedin.com">\n    <script type="text/javascript">""" not in html:
                            success = True
                    else:
                        time.sleep(time.time() % 10)
                except PlaywrightTimeout:
                    self.logger.warning(f"Timeout loading page, retries left: {count - 1}")
                    time.sleep(time.time() % 10)
                count -= 1
                
            
            if not success:
                self.logger.error(f"Failed to load job details page after retries: {job_url}")
                return None
            
            # Try primary selector: section.core-section-container.my-3.description
            job_details = None
            content_elem = page.locator("section.core-section-container.my-3.description")
            
            if content_elem.count() > 0:
                job_details = content_elem.first.inner_text().strip().lower()
            else:
                # Fallback to div.mt4
                fallback_elem = page.locator('div.mt4')
                if fallback_elem.count() > 0:
                    job_details = fallback_elem.first.inner_text().strip().lower()
            #job-details > div
            if not job_details:
                self.logger.warning(f"Could not extract job details from {job_url}")
                return None
           
            return job_details

        except Exception as e:
            self.logger.error(f"Error fetching job details from {job_url}: {e}", exc_info=True)
            return None
        finally:
            if page:
                page.close()

    def _evaluate_job_details(self, job_details: str, keywords: List[str]) -> bool:
        """
        Evaluate job details to determine if the job is a match
        
        Uses Aho-Corasick algorithm for efficient multi-pattern string matching
        to check if ALL keywords are present in the job description.
        
        Args:
            job_details: Job details string (description text)
            keywords: List of keywords/technologies to search for
        
        Returns:
            True if ALL keywords are found in job description, False otherwise
        
        Example:
            keywords = ["python", "docker", "kubernetes"]
            description = "We need Python developer with Docker and Kubernetes"
            Returns: True (all keywords found)
        """
        if not job_details:
            self.logger.debug("Empty job details, evaluation failed")
            return False
        
        if not keywords:
            self.logger.debug("No keywords to evaluate, accepting job")
            return True
        
        # Use Aho-Corasick multi-pattern matching (case-insensitive)
        all_keywords_found = match_all_keywords(keywords, job_details, case_sensitive=False)
        
        if all_keywords_found:
            self.logger.debug(f"Job matches all {len(keywords)} keywords: {keywords}")
        else:
            self.logger.debug(f"Job does not match all keywords: {keywords}")
        
        return all_keywords_found