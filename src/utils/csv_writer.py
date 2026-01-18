"""CSV writer utility with file locking for thread-safe operations"""

import os
import logging
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional
from filelock import FileLock, Timeout


def safe_write_csv(
    filename: str,
    data: List[Dict],
    logger: Optional[logging.Logger] = None,
    drop_columns: Optional[List[str]] = None,
    lock_timeout: int = 30
) -> bool:
    """
    Safely write data to CSV file with file locking to prevent race conditions
    
    This function handles:
    - Creating output directory if it doesn't exist
    - Loading existing data if file exists
    - Merging new data with existing data
    - Removing duplicates based on 'url' column
    - File locking to prevent concurrent write conflicts
    
    Args:
        filename: Path to CSV file (can be relative or absolute)
        data: List of dictionaries to write (each dict represents a row)
        logger: Optional logger instance for logging operations
        drop_columns: Optional list of column names to drop from DataFrame
        lock_timeout: Maximum seconds to wait for file lock (default: 30)
    
    Returns:
        True if write was successful, False otherwise
    """
    if logger is None:
        logger = logging.getLogger("job_scrapper.utils.csv_writer")
    
    if not data:
        logger.debug("No data to write to CSV")
        return True
    
    try:
        # Convert to absolute path
        file_path = Path(filename).resolve()
        
        # Create output directory if it doesn't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create lock file path (same as CSV file but with .lock extension)
        lock_file = str(file_path) + ".lock"
        
        # Convert data to DataFrame
        new_df = pd.DataFrame(data)
        
        # Drop specified columns if provided
        if drop_columns:
            new_df = new_df.drop(columns=drop_columns, errors='ignore')
        
        # Acquire file lock before reading/writing
        with FileLock(lock_file, timeout=lock_timeout):
            # Check if CSV file already exists
            if file_path.exists():
                try:
                    # Load existing data
                    existing_df = pd.read_csv(file_path)
                    logger.debug(f"Loading existing data from {file_path} ({len(existing_df)} existing rows)")
                    
                    # Append new data to existing DataFrame
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    
                    # Remove duplicates based on URL (keeps first occurrence)
                    if 'url' in combined_df.columns:
                        original_count = len(combined_df)
                        combined_df = combined_df.drop_duplicates(subset=['url'], keep='first')
                        duplicates_removed = original_count - len(combined_df)
                        if duplicates_removed > 0:
                            logger.debug(f"Removed {duplicates_removed} duplicate row(s)")
                    
                    logger.debug(f"Combined data: {len(combined_df)} total rows")
                except Exception as e:
                    logger.warning(f"Error reading existing CSV file: {e}. Creating new file.")
                    combined_df = new_df
            else:
                combined_df = new_df
                logger.debug(f"Creating new CSV file: {file_path}")
            
            # Write to CSV (still within lock context)
            combined_df.to_csv(file_path, index=False, encoding='utf-8')
            logger.debug(f"Successfully wrote {len(combined_df)} rows to {file_path}")
            
            return True
            
    except Timeout:
        logger.error(
            f"Timeout waiting for file lock on {filename} "
            f"(waited {lock_timeout}s). Another process may be writing to this file."
        )
        return False
    except Exception as e:
        logger.error(f"Failed to write CSV file {filename}: {e}", exc_info=True)
        return False































