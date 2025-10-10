#!/usr/bin/env python3
"""
Test script for content hashing functionality.
"""

import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from sqlitecrawler.hashing import generate_content_hashes, clean_content_for_hashing, calculate_similarity_score, is_exact_duplicate, is_near_duplicate

def test_content_hashing():
    """Test content hashing functionality."""
    print("🧪 Testing content hashing functionality...")
    
    # Test HTML content
    html1 = """
    <html>
    <head>
        <title>Test Page 1</title>
        <meta name="description" content="This is a test page">
    </head>
    <body>
        <h1>Welcome to Test Page 1</h1>
        <p>This is some content for testing.</p>
        <script>console.log('dynamic content');</script>
    </body>
    </html>
    """
    
    html2 = """
    <html>
    <head>
        <title>Test Page 1</title>
        <meta name="description" content="This is a test page">
    </head>
    <body>
        <h1>Welcome to Test Page 1</h1>
        <p>This is some content for testing.</p>
        <script>console.log('different dynamic content');</script>
    </body>
    </html>
    """
    
    html3 = """
    <html>
    <head>
        <title>Different Page</title>
        <meta name="description" content="This is a different page">
    </head>
    <body>
        <h1>Welcome to Different Page</h1>
        <p>This is completely different content.</p>
    </body>
    </html>
    """
    
    # Test content cleaning
    print("\n📝 Testing content cleaning...")
    cleaned1 = clean_content_for_hashing(html1)
    cleaned2 = clean_content_for_hashing(html2)
    cleaned3 = clean_content_for_hashing(html3)
    
    print(f"Cleaned content 1 length: {len(cleaned1)}")
    print(f"Cleaned content 2 length: {len(cleaned2)}")
    print(f"Cleaned content 3 length: {len(cleaned3)}")
    
    # Test hash generation
    print("\n🔐 Testing hash generation...")
    hashes1 = generate_content_hashes(html1)
    hashes2 = generate_content_hashes(html2)
    hashes3 = generate_content_hashes(html3)
    
    print(f"Page 1 SHA256: {hashes1['content_hash_sha256'][:16]}...")
    print(f"Page 1 SimHash: {hashes1['content_hash_simhash'][:16]}...")
    print(f"Page 1 Length: {hashes1['content_length']}")
    
    print(f"Page 2 SHA256: {hashes2['content_hash_sha256'][:16]}...")
    print(f"Page 2 SimHash: {hashes2['content_hash_simhash'][:16]}...")
    print(f"Page 2 Length: {hashes2['content_length']}")
    
    print(f"Page 3 SHA256: {hashes3['content_hash_sha256'][:16]}...")
    print(f"Page 3 SimHash: {hashes3['content_hash_simhash'][:16]}...")
    print(f"Page 3 Length: {hashes3['content_length']}")
    
    # Test exact duplicate detection
    print("\n🔍 Testing exact duplicate detection...")
    exact_1_2 = is_exact_duplicate(hashes1['content_hash_sha256'], hashes2['content_hash_sha256'])
    exact_1_3 = is_exact_duplicate(hashes1['content_hash_sha256'], hashes3['content_hash_sha256'])
    
    print(f"Page 1 vs Page 2 (exact duplicate): {exact_1_2}")
    print(f"Page 1 vs Page 3 (exact duplicate): {exact_1_3}")
    
    # Test near-duplicate detection
    print("\n🔍 Testing near-duplicate detection...")
    near_1_2 = is_near_duplicate(hashes1['content_hash_simhash'], hashes2['content_hash_simhash'])
    near_1_3 = is_near_duplicate(hashes1['content_hash_simhash'], hashes3['content_hash_simhash'])
    
    print(f"Page 1 vs Page 2 (near duplicate): {near_1_2}")
    print(f"Page 1 vs Page 3 (near duplicate): {near_1_3}")
    
    # Test similarity scoring
    print("\n📊 Testing similarity scoring...")
    sim_1_2 = calculate_similarity_score(hashes1['content_hash_simhash'], hashes2['content_hash_simhash'])
    sim_1_3 = calculate_similarity_score(hashes1['content_hash_simhash'], hashes3['content_hash_simhash'])
    
    print(f"Page 1 vs Page 2 similarity: {sim_1_2:.3f}")
    print(f"Page 1 vs Page 3 similarity: {sim_1_3:.3f}")
    
    # Test with empty content
    print("\n🧪 Testing with empty content...")
    empty_hashes = generate_content_hashes("")
    print(f"Empty content hashes: {empty_hashes}")
    
    print("\n✅ Content hashing tests completed!")

if __name__ == "__main__":
    test_content_hashing()
