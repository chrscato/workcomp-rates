"""
Utility functions for loading column configurations from text files.
This helps maintain consistency between the column lists and avoids hardcoding.
"""

import os
import ast
import logging
from typing import List

logger = logging.getLogger(__name__)

def load_column_list(file_path: str) -> List[str]:
    """
    Load a column list from a text file containing a Python list.
    
    Args:
        file_path: Path to the text file containing the column list
        
    Returns:
        List of column names
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        ValueError: If the file doesn't contain a valid Python list
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        
        # Parse the Python list from the file
        column_list = ast.literal_eval(content)
        
        if not isinstance(column_list, list):
            raise ValueError(f"File {file_path} does not contain a valid list")
        
        # Ensure all items are strings
        if not all(isinstance(item, str) for item in column_list):
            raise ValueError(f"File {file_path} contains non-string items in the list")
        
        logger.debug(f"Loaded {len(column_list)} columns from {file_path}")
        return column_list
        
    except FileNotFoundError:
        logger.error(f"Column configuration file not found: {file_path}")
        raise
    except (ValueError, SyntaxError) as e:
        logger.error(f"Invalid column configuration file {file_path}: {e}")
        raise

def get_fact_columns_pull() -> List[str]:
    """
    Get the list of columns to pull for fact partition analysis.
    
    Returns:
        List of column names from fact_cols_pull.txt
    """
    # Get the project root directory (assuming this file is in core/utils/)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    file_path = os.path.join(project_root, 'info', 'fact_cols_pull.txt')
    
    return load_column_list(file_path)

def get_fact_columns_enriched() -> List[str]:
    """
    Get the list of enriched columns for fact partition analysis.
    
    Returns:
        List of column names from fact_enriched_cols.txt
    """
    # Get the project root directory (assuming this file is in core/utils/)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    file_path = os.path.join(project_root, 'info', 'fact_enriched_cols.txt')
    
    return load_column_list(file_path)
