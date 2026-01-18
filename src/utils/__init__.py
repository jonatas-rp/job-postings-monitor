"""Utility modules for job scrapper"""

from src.utils.redis_client import RedisClient
from src.utils.string_matcher import (
    KeywordMatcher,
    match_all_keywords,
    match_any_keywords,
    get_match_score
)
from src.utils.job_classifier import (
    classify_job,
    extract_tags,
    get_all_categories,
    get_all_tags,
    get_flat_tags,
    ClassificationResult,
    CATEGORIES,
    TAGS
)
from src.utils.summary_generator import (
    classify_jobs_batch,
    generate_summary,
    save_summary,
    generate_and_save_summaries,
    load_and_aggregate_summaries,
    calculate_percentage,
    JobMarketSummary,
    CategorySummary,
    TagSummary
)

__all__ = [
    # Redis
    'RedisClient',
    # String matching
    'KeywordMatcher',
    'match_all_keywords',
    'match_any_keywords',
    'get_match_score',
    # Job classification
    'classify_job',
    'extract_tags',
    'get_all_categories',
    'get_all_tags',
    'get_flat_tags',
    'ClassificationResult',
    'CATEGORIES',
    'TAGS',
    # Summary generation
    'classify_jobs_batch',
    'generate_summary',
    'save_summary',
    'generate_and_save_summaries',
    'load_and_aggregate_summaries',
    'calculate_percentage',
    'JobMarketSummary',
    'CategorySummary',
    'TagSummary'
]
