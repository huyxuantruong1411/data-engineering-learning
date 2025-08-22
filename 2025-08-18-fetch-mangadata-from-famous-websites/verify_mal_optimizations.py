#!/usr/bin/env python3
"""
Verify MAL fetcher optimizations and performance improvements
"""
import time
import logging
from src.extractors.mal_fetcher import MALFetcher

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_mal_optimization():
    """Test MAL fetcher with known manga IDs to verify optimizations"""
    fetcher = MALFetcher()
    
    # Test with multiple known manga IDs
    test_cases = [
        ("1", "Monster"),  # Known to have many reviews
        ("2", "Berserk"),  # Popular manga with reviews
    ]
    
    total_start = time.time()
    results = []
    
    for mal_id, title in test_cases:
        print(f"\n=== Testing {title} (ID: {mal_id}) ===")
        
        start_time = time.time()
        try:
            result = fetcher.fetch_manga_data(mal_id)
            fetch_time = time.time() - start_time
            
            # Analyze results
            reviews = result.get('reviews', [])
            recommendations = result.get('recommendations', [])
            manga_info = result.get('manga_info', {})
            
            test_result = {
                'id': mal_id,
                'title': title,
                'fetch_time': fetch_time,
                'reviews_count': len(reviews),
                'recommendations_count': len(recommendations),
                'data_sections': len(result),
                'success': True
            }
            
            print(f"✓ Fetch time: {fetch_time:.2f}s")
            print(f"✓ Reviews collected: {len(reviews)}")
            print(f"✓ Recommendations: {len(recommendations)}")
            print(f"✓ Data sections: {len(result)}")
            
            if reviews:
                print(f"✓ Sample review: {reviews[0].get('text', '')[:80]}...")
            
            results.append(test_result)
            
        except Exception as e:
            print(f"✗ Error: {e}")
            results.append({
                'id': mal_id,
                'title': title,
                'fetch_time': 0,
                'reviews_count': 0,
                'recommendations_count': 0,
                'data_sections': 0,
                'success': False,
                'error': str(e)
            })
    
    total_time = time.time() - total_start
    
    # Summary
    print(f"\n=== Performance Summary ===")
    print(f"Total test time: {total_time:.2f}s")
    print(f"Average time per manga: {total_time/len(test_cases):.2f}s")
    
    successful_tests = [r for r in results if r['success']]
    if successful_tests:
        avg_reviews = sum(r['reviews_count'] for r in successful_tests) / len(successful_tests)
        avg_recommendations = sum(r['recommendations_count'] for r in successful_tests) / len(successful_tests)
        print(f"Average reviews per manga: {avg_reviews:.1f}")
        print(f"Average recommendations per manga: {avg_recommendations:.1f}")
    
    # Check if optimizations are working
    print(f"\n=== Optimization Check ===")
    for result in results:
        if result['success']:
            if result['reviews_count'] > 3:
                print(f"✓ {result['title']}: Review collection improved (>{result['reviews_count']} reviews)")
            else:
                print(f"⚠ {result['title']}: Limited reviews ({result['reviews_count']})")
            
            if result['fetch_time'] < 10:
                print(f"✓ {result['title']}: Good performance ({result['fetch_time']:.2f}s)")
            else:
                print(f"⚠ {result['title']}: Slow performance ({result['fetch_time']:.2f}s)")
    
    return results

if __name__ == "__main__":
    test_mal_optimization()
