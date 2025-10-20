"""Data models for job scrapper"""

from .config import AppConfig, WorkerConfig
from .job import Job

__all__ = ["AppConfig", "WorkerConfig", "Job"]


