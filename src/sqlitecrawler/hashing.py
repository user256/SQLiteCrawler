"""
Content hashing utilities for duplicate detection and crawl comparison.
"""

import hashlib
from typing import Dict, Optional
from bs4 import BeautifulSoup
from simhash import Simhash


def clean_content_for_hashing(html: str) -> str:
    """
    Clean HTML content for consistent hashing by removing dynamic elements
    and normalizing structure.
    """
    if not html:
        return ""
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove dynamic elements that change frequently
        for tag in soup.find_all(['script', 'style', 'noscript', 'iframe']):
            tag.decompose()
        
        # Remove comments
        from bs4 import Comment
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()
        
        # Remove attributes that change frequently but keep essential ones
        essential_attrs = {'href', 'src', 'alt', 'title', 'id', 'class'}
        for tag in soup.find_all():
            if tag.attrs:
                # Keep only essential attributes
                attrs_to_remove = [attr for attr in tag.attrs if attr not in essential_attrs]
                for attr in attrs_to_remove:
                    del tag.attrs[attr]
                
                # Normalize class attribute (sort classes for consistency)
                if 'class' in tag.attrs and isinstance(tag.attrs['class'], list):
                    tag.attrs['class'] = sorted(tag.attrs['class'])
        
        # Get text content with structure preserved
        text_content = soup.get_text(separator=' ', strip=True)
        
        # Normalize whitespace
        import re
        text_content = re.sub(r'\s+', ' ', text_content)
        
        return text_content.strip()
        
    except Exception as e:
        # Fallback to simple text extraction if parsing fails
        print(f"Warning: Failed to parse HTML for hashing: {e}")
        return html


def generate_content_hashes(html_content: str) -> Dict[str, str]:
    """
    Generate SHA256 and SimHash for content analysis.
    
    Args:
        html_content: Raw HTML content
        
    Returns:
        Dictionary with 'content_hash_sha256', 'content_hash_simhash', and 'content_length'
    """
    if not html_content:
        return {
            'content_hash_sha256': '',
            'content_hash_simhash': '',
            'content_length': 0
        }
    
    # Clean content for consistent hashing
    cleaned_content = clean_content_for_hashing(html_content)
    
    if not cleaned_content:
        return {
            'content_hash_sha256': '',
            'content_hash_simhash': '',
            'content_length': 0
        }
    
    # Generate SHA256 for exact duplicates
    sha256_hash = hashlib.sha256(cleaned_content.encode('utf-8')).hexdigest()
    
    # Generate SimHash for near-duplicates
    try:
        simhash_value = Simhash(cleaned_content).value
        simhash_str = str(simhash_value)
    except Exception as e:
        print(f"Warning: Failed to generate SimHash: {e}")
        simhash_str = ''
    
    return {
        'content_hash_sha256': sha256_hash,
        'content_hash_simhash': simhash_str,
        'content_length': len(cleaned_content)
    }


def calculate_similarity_score(hash1: str, hash2: str) -> float:
    """
    Calculate similarity score between two SimHash values.
    
    Args:
        hash1: First SimHash value as string
        hash2: Second SimHash value as string
        
    Returns:
        Similarity score between 0.0 and 1.0
    """
    if not hash1 or not hash2:
        return 0.0
    
    try:
        simhash1 = Simhash(int(hash1))
        simhash2 = Simhash(int(hash2))
        
        # Calculate Hamming distance and convert to similarity score
        distance = simhash1.distance(simhash2)
        max_distance = 64  # SimHash uses 64-bit hashes
        
        # Convert distance to similarity (0.0 = identical, 1.0 = completely different)
        similarity = 1.0 - (distance / max_distance)
        
        return max(0.0, min(1.0, similarity))
        
    except Exception as e:
        print(f"Warning: Failed to calculate similarity score: {e}")
        return 0.0


def is_exact_duplicate(hash1: str, hash2: str) -> bool:
    """
    Check if two SHA256 hashes represent exact duplicates.
    
    Args:
        hash1: First SHA256 hash
        hash2: Second SHA256 hash
        
    Returns:
        True if hashes are identical (exact duplicate)
    """
    return hash1 and hash2 and hash1 == hash2


def is_near_duplicate(hash1: str, hash2: str, threshold: float = 0.8) -> bool:
    """
    Check if two SimHash values represent near-duplicates.
    
    Args:
        hash1: First SimHash value as string
        hash2: Second SimHash value as string
        threshold: Similarity threshold (default 0.8 = 80% similar)
        
    Returns:
        True if similarity score is above threshold
    """
    if not hash1 or not hash2:
        return False
    
    similarity = calculate_similarity_score(hash1, hash2)
    return similarity >= threshold


def get_content_hash_summary(hashes: Dict[str, str]) -> str:
    """
    Get a human-readable summary of content hashes.
    
    Args:
        hashes: Dictionary with content hashes
        
    Returns:
        Summary string
    """
    sha256 = hashes.get('content_hash_sha256', '')
    simhash = hashes.get('content_hash_simhash', '')
    length = hashes.get('content_length', 0)
    
    if not sha256:
        return "No content hashes available"
    
    return f"SHA256: {sha256[:16]}..., SimHash: {simhash[:16]}..., Length: {length}"
