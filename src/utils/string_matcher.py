"""String matching utilities for job description evaluation"""

import logging
from typing import List, Set, Dict
import ahocorasick


class KeywordMatcher:
    """
    Efficient multi-pattern string matching using Aho-Corasick algorithm
    
    This class provides approximate string matching to find keywords
    in job descriptions, with support for case-insensitive matching.
    """
    
    def __init__(self):
        """Initialize keyword matcher"""
        self.logger = logging.getLogger("job_scrapper.utils.string_matcher")
    
    def match_all_keywords(
        self, 
        keywords: List[str], 
        text: str, 
        case_sensitive: bool = False
    ) -> bool:
        """
        Check if ALL keywords are present in the text using Aho-Corasick algorithm
        
        Args:
            keywords: List of keywords to search for
            text: Text to search in (e.g., job description)
            case_sensitive: Whether matching should be case-sensitive (default: False)
        
        Returns:
            True if ALL keywords are found in text, False otherwise
        
        Example:
            >>> matcher = KeywordMatcher()
            >>> keywords = ["python", "docker", "kubernetes"]
            >>> description = "We need Python developer with Docker and Kubernetes experience"
            >>> matcher.match_all_keywords(keywords, description)
            True
        """
        if not keywords:
            self.logger.warning("No keywords provided for matching")
            return True
        
        if not text:
            self.logger.warning("Empty text provided for matching")
            return False
        
        # Normalize text for case-insensitive matching
        search_text = text if case_sensitive else text.lower()
        search_keywords = keywords if case_sensitive else [k.lower() for k in keywords]
        
        # Build Aho-Corasick automaton
        automaton = ahocorasick.Automaton()
        
        # Add keywords to automaton
        for idx, keyword in enumerate(search_keywords):
            automaton.add_word(keyword, (idx, keyword))
        
        # Finalize automaton (required before search)
        automaton.make_automaton()
        
        # Find all matches
        found_keywords: Set[str] = set()
        for end_index, (idx, keyword) in automaton.iter(search_text):
            found_keywords.add(keyword)
        
        # Check if all keywords were found
        all_found = len(found_keywords) == len(search_keywords)
        
        if all_found:
            self.logger.debug(f"All {len(keywords)} keywords found in text")
        else:
            missing = set(search_keywords) - found_keywords
            self.logger.debug(
                f"Missing keywords: {missing} "
                f"(found {len(found_keywords)}/{len(search_keywords)})"
            )
        
        return all_found
    
    def match_any_keywords(
        self, 
        keywords: List[str], 
        text: str, 
        case_sensitive: bool = False
    ) -> bool:
        """
        Check if ANY keyword is present in the text
        
        Args:
            keywords: List of keywords to search for
            text: Text to search in
            case_sensitive: Whether matching should be case-sensitive (default: False)
        
        Returns:
            True if at least one keyword is found, False otherwise
        """
        if not keywords:
            return True
        
        if not text:
            return False
        
        # Normalize for case-insensitive matching
        search_text = text if case_sensitive else text.lower()
        search_keywords = keywords if case_sensitive else [k.lower() for k in keywords]
        
        # Build automaton
        automaton = ahocorasick.Automaton()
        for idx, keyword in enumerate(search_keywords):
            automaton.add_word(keyword, (idx, keyword))
        automaton.make_automaton()
        
        # Check if any match exists (stop at first match)
        for end_index, (idx, keyword) in automaton.iter(search_text):
            self.logger.debug(f"Found keyword: {keyword}")
            return True
        
        self.logger.debug("No keywords found in text")
        return False
    
    def get_matched_keywords(
        self, 
        keywords: List[str], 
        text: str, 
        case_sensitive: bool = False
    ) -> Dict[str, int]:
        """
        Get all matched keywords with their occurrence counts
        
        Args:
            keywords: List of keywords to search for
            text: Text to search in
            case_sensitive: Whether matching should be case-sensitive (default: False)
        
        Returns:
            Dictionary mapping matched keywords to their occurrence counts
        
        Example:
            >>> matcher = KeywordMatcher()
            >>> keywords = ["python", "docker"]
            >>> text = "Python and Docker are great. I love Python!"
            >>> matcher.get_matched_keywords(keywords, text)
            {'python': 2, 'docker': 1}
        """
        if not keywords or not text:
            return {}
        
        # Normalize for case-insensitive matching
        search_text = text if case_sensitive else text.lower()
        search_keywords = keywords if case_sensitive else [k.lower() for k in keywords]
        
        # Build automaton
        automaton = ahocorasick.Automaton()
        for idx, keyword in enumerate(search_keywords):
            automaton.add_word(keyword, (idx, keyword))
        automaton.make_automaton()
        
        # Count matches
        matches: Dict[str, int] = {}
        for end_index, (idx, keyword) in automaton.iter(search_text):
            if keyword in matches:
                matches[keyword] += 1
            else:
                matches[keyword] = 1
        
        return matches
    
    def get_match_score(
        self, 
        keywords: List[str], 
        text: str, 
        case_sensitive: bool = False
    ) -> float:
        """
        Calculate match score as percentage of keywords found
        
        Args:
            keywords: List of keywords to search for
            text: Text to search in
            case_sensitive: Whether matching should be case-sensitive
        
        Returns:
            Match score between 0.0 and 1.0
        
        Example:
            >>> matcher = KeywordMatcher()
            >>> keywords = ["python", "docker", "kubernetes", "aws"]
            >>> text = "Python and Docker experience required"
            >>> matcher.get_match_score(keywords, text)
            0.5  # 2 out of 4 keywords found
        """
        if not keywords:
            return 1.0
        
        if not text:
            return 0.0
        
        # Normalize for case-insensitive matching
        search_text = text if case_sensitive else text.lower()
        search_keywords = keywords if case_sensitive else [k.lower() for k in keywords]
        
        # Build automaton
        automaton = ahocorasick.Automaton()
        for idx, keyword in enumerate(search_keywords):
            automaton.add_word(keyword, (idx, keyword))
        automaton.make_automaton()
        
        # Find unique matches
        found_keywords: Set[str] = set()
        for end_index, (idx, keyword) in automaton.iter(search_text):
            found_keywords.add(keyword)
        
        score = len(found_keywords) / len(search_keywords)
        self.logger.debug(
            f"Match score: {score:.2f} "
            f"({len(found_keywords)}/{len(search_keywords)} keywords)"
        )
        
        return score


# Convenience function for simple use cases
def match_all_keywords(keywords: List[str], text: str, case_sensitive: bool = False) -> bool:
    """
    Convenience function to check if all keywords are in text
    
    Args:
        keywords: List of keywords to search for
        text: Text to search in
        case_sensitive: Whether matching should be case-sensitive (default: False)
    
    Returns:
        True if ALL keywords are found, False otherwise
    
    Example:
        >>> from src.utils.string_matcher import match_all_keywords
        >>> keywords = ["python", "docker"]
        >>> description = "Python developer with Docker experience"
        >>> match_all_keywords(keywords, description)
        True
    """
    matcher = KeywordMatcher()
    return matcher.match_all_keywords(keywords, text, case_sensitive)


def match_any_keywords(keywords: List[str], text: str, case_sensitive: bool = False) -> bool:
    """
    Convenience function to check if any keyword is in text
    
    Args:
        keywords: List of keywords to search for
        text: Text to search in
        case_sensitive: Whether matching should be case-sensitive (default: False)
    
    Returns:
        True if at least one keyword is found, False otherwise
    """
    matcher = KeywordMatcher()
    return matcher.match_any_keywords(keywords, text, case_sensitive)


def get_match_score(keywords: List[str], text: str, case_sensitive: bool = False) -> float:
    """
    Convenience function to calculate match score
    
    Args:
        keywords: List of keywords to search for
        text: Text to search in
        case_sensitive: Whether matching should be case-sensitive
    
    Returns:
        Match score between 0.0 and 1.0
    """
    matcher = KeywordMatcher()
    return matcher.get_match_score(keywords, text, case_sensitive)

