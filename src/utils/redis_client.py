"""Redis client for caching job postings"""

import os
import logging
import hashlib
from typing import Optional
from datetime import datetime
import redis

from src.models.job import Job


class RedisClient:
    """
    Redis client for caching job postings to prevent duplicate processing
    
    This client uses Redis hashsets organized by company name to store job postings.
    Each job is identified by a hash of its description to detect true duplicates,
    even when URLs differ (due to tracking parameters, etc.).
    
    Redis Structure:
        Key: job:company:{company_name}
        Type: Hash
        Fields: {description_hash: timestamp}
        
    Example:
        job:company:google -> {
            "a1b2c3...": "2024-10-19T10:30:00",
            "d4e5f6...": "2024-10-19T11:45:00"
        }
    """
    
    def __init__(
        self, 
        host: Optional[str] = None,
        port: Optional[int] = None,
        db: Optional[int] = None,
        default_ttl: int = 86400  # 24 hours in seconds
    ):
        """
        Initialize Redis client
        
        Args:
            host: Redis server host (defaults to env REDIS_HOST or 'localhost')
            port: Redis server port (defaults to env REDIS_PORT or 6379)
            db: Redis database number (defaults to env REDIS_DB or 0)
            default_ttl: Default time-to-live for cached items in seconds (default: 24 hours)
        """
        self.logger = logging.getLogger("job_scrapper.utils.redis")
        
        # Get configuration from environment variables or use provided/default values
        self.host = host or os.getenv('REDIS_HOST', 'localhost')
        self.port = int(port or os.getenv('REDIS_PORT', 6379))
        self.db = int(db or os.getenv('REDIS_DB', 0))
        self.default_ttl = default_ttl
        
        self.logger.info(f"Initializing Redis client: {self.host}:{self.port}/{self.db}")
        
        try:
            # Create Redis connection
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            
            # Test connection
            self.client.ping()
            self.logger.info("Redis connection established successfully")
            
        except redis.ConnectionError as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error initializing Redis client: {e}")
            raise
    
    def is_job_cached(self, job: Job) -> bool:
        """
        Check if a job has been cached (already processed)
        
        Uses a hash of the job description to identify duplicates,
        regardless of URL variations.
        
        Args:
            job: The Job object to check
        
        Returns:
            True if the job exists in cache, False otherwise
        """
        try:
            company_key = self._get_company_key(job.company)
            description_hash = self._hash_description(job.description)
            
            # Check if this description hash exists in the company's hashset
            exists = self.client.hexists(company_key, description_hash)
            
            if exists:
                self.logger.debug(
                    f"Cache hit for job: {job.title} at {job.company} "
                    f"(hash: {description_hash[:8]}...)"
                )
            else:
                self.logger.debug(
                    f"Cache miss for job: {job.title} at {job.company} "
                    f"(hash: {description_hash[:8]}...)"
                )
            
            return exists
            
        except Exception as e:
            self.logger.error(f"Error checking cache for job {job.title}: {e}")
            # On error, assume not cached to avoid missing jobs
            return False
    
    def cache_job(self, job: Job, ttl: Optional[int] = None) -> bool:
        """
        Add a job to the cache with expiration
        
        Stores the job in a company-specific hashset with the description hash
        as the field and current timestamp as the value.
        
        Args:
            job: The Job object to cache
            ttl: Time-to-live in seconds (defaults to default_ttl, 24 hours)
        
        Returns:
            True if successfully cached, False otherwise
        """
        try:
            company_key = self._get_company_key(job.company)
            description_hash = self._hash_description(job.description)
            timestamp = datetime.now().isoformat()
            expiration = ttl or self.default_ttl
            
            # Store job description hash with timestamp in company hashset
            self.client.hset(company_key, description_hash, timestamp)
            
            # Set expiration on the company key (resets with each new job)
            self.client.expire(company_key, expiration)
            
            self.logger.debug(
                f"Cached job: {job.title} at {job.company} "
                f"(hash: {description_hash[:8]}..., TTL={expiration}s)"
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error caching job {job.title} at {job.company}: {e}")
            return False
    
    def check_and_cache_job(self, job: Job, ttl: Optional[int] = None) -> bool:
        """
        Check if job is cached and cache it if not
        
        This is a convenience method that combines is_job_cached and cache_job.
        
        Args:
            job: The Job object to check and cache
            ttl: Time-to-live in seconds (defaults to default_ttl, 24 hours)
        
        Returns:
            True if job was already cached (duplicate), False if it's new
        """
        if self.is_job_cached(job):
            return True  # Job is a duplicate
        
        # Not cached, so cache it now
        self.cache_job(job, ttl)
        return False  # Job is new
    
    def _get_company_key(self, company_name: str) -> str:
        """
        Generate a Redis key for a company's job hashset
        
        Normalizes the company name to handle variations in casing and whitespace.
        
        Args:
            company_name: The company name
        
        Returns:
            Cache key string in format "job:company:{normalized_company_name}"
        """
        # Normalize company name: lowercase, strip whitespace, replace spaces with underscores
        normalized = company_name.lower().strip().replace(' ', '_')
        return f"job:company:{normalized}"
    
    def _hash_description(self, description: str) -> str:
        """
        Generate a hash of the job description for deduplication
        
        Uses SHA256 to create a unique identifier for the job content.
        This allows detecting duplicate jobs even when URLs differ.
        
        Args:
            description: The job description text
        
        Returns:
            Hexadecimal hash string
        """
        # Normalize description: lowercase and strip whitespace
        normalized = description.lower().strip()
        
        # Create SHA256 hash
        hash_object = hashlib.sha256(normalized.encode('utf-8'))
        return hash_object.hexdigest()
    
    def clear_cache(self) -> int:
        """
        Clear all job cache entries (all company hashsets)
        
        Returns:
            Number of company hashsets deleted
        """
        try:
            # Find all company hashset keys
            pattern = "job:company:*"
            keys = list(self.client.scan_iter(match=pattern))
            
            if keys:
                deleted = self.client.delete(*keys)
                self.logger.info(f"Cleared {deleted} company hashsets from cache")
                return deleted
            else:
                self.logger.info("No cached jobs to clear")
                return 0
                
        except Exception as e:
            self.logger.error(f"Error clearing cache: {e}")
            return 0
    
    def get_cache_stats(self) -> dict:
        """
        Get statistics about cached jobs
        
        Returns:
            Dictionary with cache statistics including total jobs and companies
        """
        try:
            pattern = "job:company:*"
            company_keys = list(self.client.scan_iter(match=pattern))
            
            # Count total jobs across all companies
            total_jobs = 0
            for company_key in company_keys:
                total_jobs += self.client.hlen(company_key)
            
            return {
                'total_companies': len(company_keys),
                'total_cached_jobs': total_jobs,
                'redis_host': self.host,
                'redis_port': self.port,
                'redis_db': self.db
            }
            
        except Exception as e:
            self.logger.error(f"Error getting cache stats: {e}")
            return {
                'total_companies': 0,
                'total_cached_jobs': 0,
                'error': str(e)
            }
    
    def close(self):
        """Close Redis connection"""
        try:
            if self.client:
                self.client.close()
                self.logger.info("Redis connection closed")
        except Exception as e:
            self.logger.error(f"Error closing Redis connection: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection"""
        self.close()

