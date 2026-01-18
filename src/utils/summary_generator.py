"""Summary generator utility for job market statistics"""

import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
from collections import Counter

from src.utils.job_classifier import (
    classify_job,
    get_all_categories,
    get_flat_tags,
    ClassificationResult
)


@dataclass
class CategorySummary:
    """Summary statistics for a category"""
    name: str
    count: int
    percentage: float


@dataclass
class TagSummary:
    """Summary statistics for a tag"""
    name: str
    count: int
    percentage: float


@dataclass
class JobMarketSummary:
    """Complete job market summary"""
    total_jobs: int
    categories: List[CategorySummary]
    tags: List[TagSummary]
    generated_at: datetime = field(default_factory=datetime.now)


def calculate_percentage(count: int, total: int, decimal_places: int = 2) -> float:
    """
    Calculate percentage with safe division
    
    Args:
        count: The count for the item
        total: The total count
        decimal_places: Number of decimal places to round to
    
    Returns:
        Percentage value rounded to specified decimal places
    """
    if total == 0:
        return 0.0
    return round((count / total) * 100, decimal_places)


def classify_jobs_batch(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Classify a batch of jobs, adding category and tags to each
    
    Args:
        jobs: List of job dictionaries with 'title' and 'description' keys
    
    Returns:
        List of jobs with 'category' and 'tags' fields added
    """
    classified_jobs = []
    
    for job in jobs:
        title = job.get('title', '')
        description = job.get('description', '')
        
        result = classify_job(title, description)
        
        # Create a copy of the job dict with classification added
        classified_job = job.copy()
        classified_job['category'] = result.category
        classified_job['tags'] = ','.join(result.tags)  # Store as comma-separated string
        classified_job['classification_confidence'] = result.confidence
        
        classified_jobs.append(classified_job)
    
    return classified_jobs


def generate_summary(jobs: List[Dict[str, Any]]) -> JobMarketSummary:
    """
    Generate job market summary from classified jobs
    
    Args:
        jobs: List of classified job dictionaries
    
    Returns:
        JobMarketSummary with category and tag statistics
    """
    total_jobs = len(jobs)
    
    if total_jobs == 0:
        return JobMarketSummary(
            total_jobs=0,
            categories=[],
            tags=[]
        )
    
    # Count categories
    category_counter = Counter()
    tag_counter = Counter()
    
    for job in jobs:
        # Count category
        category = job.get('category', 'Unknown')
        category_counter[category] += 1
        
        # Count tags (handle both string and list formats)
        tags = job.get('tags', '')
        if isinstance(tags, str):
            tag_list = [t.strip() for t in tags.split(',') if t.strip()]
        else:
            tag_list = tags if tags else []
        
        for tag in tag_list:
            tag_counter[tag] += 1
    
    # Build category summaries
    all_categories = get_all_categories()
    category_summaries = []
    
    for category in all_categories:
        count = category_counter.get(category, 0)
        percentage = calculate_percentage(count, total_jobs)
        category_summaries.append(CategorySummary(
            name=category,
            count=count,
            percentage=percentage
        ))
    
    # Sort categories by count (descending)
    category_summaries.sort(key=lambda x: x.count, reverse=True)
    
    # Build tag summaries (only for tags that have at least 1 occurrence)
    tag_summaries = []
    
    for tag, count in tag_counter.most_common():
        percentage = calculate_percentage(count, total_jobs)
        tag_summaries.append(TagSummary(
            name=tag,
            count=count,
            percentage=percentage
        ))
    
    return JobMarketSummary(
        total_jobs=total_jobs,
        categories=category_summaries,
        tags=tag_summaries
    )


def save_category_summary_csv(
    summary: JobMarketSummary,
    filepath: str,
    logger: Optional[logging.Logger] = None
) -> bool:
    """
    Save category summary to CSV file
    
    Args:
        summary: JobMarketSummary object
        filepath: Path to save CSV file
        logger: Optional logger instance
    
    Returns:
        True if successful, False otherwise
    """
    if logger is None:
        logger = logging.getLogger("job_scrapper.utils.summary_generator")
    
    try:
        file_path = Path(filepath).resolve()
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        data = []
        for cat in summary.categories:
            data.append({
                'category': cat.name,
                'job_count': cat.count,
                'percentage': cat.percentage,
                'total_jobs': summary.total_jobs
            })
        
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False, encoding='utf-8')
        
        logger.info(f"Saved category summary to {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to save category summary: {e}", exc_info=True)
        return False


def save_tag_summary_csv(
    summary: JobMarketSummary,
    filepath: str,
    logger: Optional[logging.Logger] = None
) -> bool:
    """
    Save tag summary to CSV file
    
    Args:
        summary: JobMarketSummary object
        filepath: Path to save CSV file
        logger: Optional logger instance
    
    Returns:
        True if successful, False otherwise
    """
    if logger is None:
        logger = logging.getLogger("job_scrapper.utils.summary_generator")
    
    try:
        file_path = Path(filepath).resolve()
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        data = []
        for tag in summary.tags:
            data.append({
                'tag': tag.name,
                'job_count': tag.count,
                'percentage': tag.percentage,
                'total_jobs': summary.total_jobs
            })
        
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False, encoding='utf-8')
        
        logger.info(f"Saved tag summary to {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to save tag summary: {e}", exc_info=True)
        return False


def save_summary(
    summary: JobMarketSummary,
    output_dir: str,
    prefix: str = "",
    logger: Optional[logging.Logger] = None
) -> bool:
    """
    Save both category and tag summaries to CSV files
    
    Args:
        summary: JobMarketSummary object
        output_dir: Directory to save CSV files
        prefix: Optional prefix for filenames
        logger: Optional logger instance
    
    Returns:
        True if both files saved successfully, False otherwise
    """
    if logger is None:
        logger = logging.getLogger("job_scrapper.utils.summary_generator")
    
    output_path = Path(output_dir)
    
    # Generate filenames with optional prefix
    if prefix:
        category_filename = f"{prefix}_categories_summary.csv"
        tag_filename = f"{prefix}_tags_summary.csv"
    else:
        category_filename = "categories_summary.csv"
        tag_filename = "tags_summary.csv"
    
    category_path = output_path / category_filename
    tag_path = output_path / tag_filename
    
    category_success = save_category_summary_csv(summary, str(category_path), logger)
    tag_success = save_tag_summary_csv(summary, str(tag_path), logger)
    
    return category_success and tag_success


def generate_and_save_summaries(
    jobs: List[Dict[str, Any]],
    worker_name: str,
    logger: Optional[logging.Logger] = None
) -> bool:
    """
    Generate summaries and save to both monthly and total folders
    
    This function:
    1. Classifies jobs if not already classified
    2. Generates summary statistics
    3. Saves to ./output/summary/{year}/{month}/
    4. Saves/updates total summary in ./output/summary/
    
    Args:
        jobs: List of job dictionaries
        worker_name: Name of the worker (used as prefix)
        logger: Optional logger instance
    
    Returns:
        True if all operations successful, False otherwise
    """
    if logger is None:
        logger = logging.getLogger("job_scrapper.utils.summary_generator")
    
    if not jobs:
        logger.debug("No jobs to generate summary for")
        return True
    
    try:
        # Classify jobs if not already classified
        if 'category' not in jobs[0]:
            jobs = classify_jobs_batch(jobs)
        
        # Generate summary
        summary = generate_summary(jobs)
        
        # Get current date for folder structure
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        
        # Save to monthly folder
        monthly_dir = f"output/summary/{year}/{month}"
        monthly_success = save_summary(
            summary, 
            monthly_dir, 
            prefix=worker_name,
            logger=logger
        )
        
        # Save to total folder (aggregate)
        total_dir = "output/summary"
        total_success = save_summary(
            summary,
            total_dir,
            prefix=f"{worker_name}_total",
            logger=logger
        )
        
        if monthly_success and total_success:
            logger.info(
                f"Generated job market summary: {summary.total_jobs} jobs, "
                f"{len(summary.categories)} categories, {len(summary.tags)} tags"
            )
        
        return monthly_success and total_success
        
    except Exception as e:
        logger.error(f"Failed to generate and save summaries: {e}", exc_info=True)
        return False


def load_and_aggregate_summaries(
    summary_dir: str = "output/summary",
    logger: Optional[logging.Logger] = None
) -> Optional[JobMarketSummary]:
    """
    Load all existing job CSVs and generate an aggregate summary
    
    Args:
        summary_dir: Base directory for summaries
        logger: Optional logger instance
    
    Returns:
        Aggregated JobMarketSummary or None if failed
    """
    if logger is None:
        logger = logging.getLogger("job_scrapper.utils.summary_generator")
    
    try:
        # Find all job CSV files in output directory
        output_path = Path("output")
        
        if not output_path.exists():
            logger.warning("Output directory does not exist")
            return None
        
        all_jobs = []
        
        # Search for job CSVs (not summary files)
        for csv_file in output_path.rglob("*_jobs_*.csv"):
            if "summary" not in str(csv_file):
                try:
                    df = pd.read_csv(csv_file)
                    jobs = df.to_dict('records')
                    all_jobs.extend(jobs)
                    logger.debug(f"Loaded {len(jobs)} jobs from {csv_file}")
                except Exception as e:
                    logger.warning(f"Failed to load {csv_file}: {e}")
        
        if not all_jobs:
            logger.info("No job data found to aggregate")
            return None
        
        # Classify and generate summary
        classified_jobs = classify_jobs_batch(all_jobs)
        summary = generate_summary(classified_jobs)
        
        logger.info(f"Aggregated summary: {summary.total_jobs} total jobs")
        return summary
        
    except Exception as e:
        logger.error(f"Failed to aggregate summaries: {e}", exc_info=True)
        return None
