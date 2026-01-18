"""Job data model"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List


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
        category: Job category (Frontend, Backend, Fullstack, DevOps, AI)
        tags: List of technology tags found in the job
        classification_confidence: Confidence score for the classification
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
    category: str = ""
    tags: List[str] = field(default_factory=list)
    classification_confidence: float = 0.0
    
    def __str__(self) -> str:
        """String representation of job"""
        return f"{self.title} at {self.company} ({self.location})"
    
    def classify(self) -> None:
        """Classify this job based on title and description"""
        from src.utils.job_classifier import classify_job
        
        result = classify_job(self.title, self.description)
        self.category = result.category
        self.tags = result.tags
        self.classification_confidence = result.confidence
    
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
            'scraped_at': self.scraped_at.isoformat() if self.scraped_at else None,
            'category': self.category,
            'tags': ','.join(self.tags) if self.tags else '',
            'classification_confidence': self.classification_confidence
        }



