#!/usr/bin/env python3
"""
Test script to verify MAL fetcher performance and data completeness
"""
import time
from src.extractors.mal_fetcher import MALFetcher

def test_mal_performance():
    """Test MAL fetcher with a known manga ID"""
    fetcher = MALFetcher()
    
    # Test with Monster (MAL ID: 1) - known to have many reviews
    test_id = "1"
    print(f"Testing MAL fetcher with ID: {test_id}")
    
    start_time = time.time()
    result = fetcher.fetch_manga_data(test_id)
    end_time = time.time()
    
    fetch_time = end_time - start_time
    
    print(f"\n=== MAL Fetcher Performance Test ===")
    print(f"Fetch time: {fetch_time:.2f} seconds")
    print(f"Data keys collected: {list(result.keys())}")
    
    # Check reviews
    reviews = result.get('reviews', [])
    print(f"Reviews collected: {len(reviews)}")
    if reviews:
        print(f"First review preview: {reviews[0].get('text', '')[:100]}...")
    
    # Check recommendations
    recommendations = result.get('recommendations', [])
    print(f"Recommendations collected: {len(recommendations)}")
    
    # Check main manga info
    manga_info = result.get('manga_info', {})
    print(f"Main info fields: {len(manga_info)} fields")
    print(f"Title: {manga_info.get('title', 'N/A')}")
    print(f"Score: {manga_info.get('score', 'N/A')}")
    
    return result

if __name__ == "__main__":
    test_mal_performance()
