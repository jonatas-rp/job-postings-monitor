"""Job data model"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Job:
    """
    Represents a job posting
    
    Attributes:
        title: Job title
        company: Company name
        location: Job location
        url: Link to job posting
        description: Job description/snippet
        source: Source of the job (e.g., 'linkedin', 'google')
        posted_date: When the job was posted
        scraped_at: When the job was scraped
    """
    title: str
    company: str
    location: str
    url: str
    description: str = ""
    source: str = "unknown"
    posted_date: Optional[str] = None
    posted_time: Optional[int] = None
    scraped_at: datetime = field(default_factory=datetime.now)
    
    def __str__(self) -> str:
        """String representation of job"""
        return f"{self.title} at {self.company} ({self.location})"
    
    def to_dict(self) -> dict:
        """Convert job to dictionary"""
        return {
            'title': self.title,
            'company': self.company,
            'location': self.location,
            'url': self.url,
            'description': self.description,
            'source': self.source,
            'posted_date': self.posted_date,
            'posted_time': self.posted_time,
            'scraped_at': self.scraped_at.isoformat() if self.scraped_at else None
        }



