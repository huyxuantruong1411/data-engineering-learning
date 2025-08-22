# debug_animeplanet_enhanced.py
# Script để test trực tiếp anime-planet fetcher

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import logging
from pprint import pprint

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_single_manga(slug):
    """Test một manga cụ thể"""
    print(f"\n{'='*60}")
    print(f"🧪 Testing anime-planet slug: {slug}")
    print(f"{'='*60}")
    
    # Import the enhanced fetcher
    from src.extractors.animeplanet_fetcher_enhanced import get_full_data
    
    try:
        result = get_full_data(slug)
        
        print(f"\n📊 RESULTS for {slug}:")
        print(f"Status: {result.get('status')}")
        print(f"HTTP: {result.get('http')}")
        
        # Main info
        main = result.get('main', {})
        print(f"\n📖 Main Info:")
        print(f"  Title: {main.get('title', 'N/A')}")
        print(f"  Synopsis: {len(main.get('synopsis', ''))} chars")
        print(f"  Rating: {main.get('rating', 'N/A')}")
        
        # Reviews
        reviews = result.get('reviews', [])
        print(f"\n📝 Reviews: {len(reviews)} found")
        for i, review in enumerate(reviews[:3]):  # Show first 3
            print(f"  Review {i+1}:")
            print(f"    User: {review.get('user', 'Anonymous')}")
            print(f"    Text: {review.get('text', '')[:100]}...")
        
        # Recommendations  
        recs = result.get('recommendations', [])
        print(f"\n🔗 Recommendations: {len(recs)} found")
        for i, rec in enumerate(recs[:5]):  # Show first 5
            print(f"  Rec {i+1}: {rec.get('slug')} - {rec.get('title', 'No title')}")
        
        return result
        
    except Exception as e:
        print(f"❌ Error testing {slug}: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_multiple_slugs():
    """Test multiple popular manga slugs"""
    test_slugs = [
        "naruto",           # Very popular, should have reviews
        "one-piece",        # Extremely popular  
        "attack-on-titan",  # Popular, should have data
        "tower-of-god",     # Webtoon, moderate popularity
        "berserk"           # Classic, should have reviews
    ]
    
    results = {}
    
    for slug in test_slugs:
        result = test_single_manga(slug)
        if result:
            results[slug] = {
                'status': result.get('status'),
                'reviews_count': len(result.get('reviews', [])),
                'recs_count': len(result.get('recommendations', [])),
                'has_main': bool(result.get('main', {}))
            }
        
        print(f"\n⏳ Waiting before next test...")
        import time
        time.sleep(15)  # Wait between tests
    
    print(f"\n{'='*60}")
    print("📈 SUMMARY OF ALL TESTS")
    print(f"{'='*60}")
    
    for slug, data in results.items():
        print(f"{slug:20} | Status: {data['status']:12} | Reviews: {data['reviews_count']:3} | Recs: {data['recs_count']:3} | Main: {data['has_main']}")

def test_direct_requests():
    """Test direct HTTP requests để xem có bị block không"""
    import requests
    import random
    
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    
    test_urls = [
        "https://www.anime-planet.com/manga/naruto",
        "https://www.anime-planet.com/manga/naruto/reviews",
        "https://www.anime-planet.com/manga/naruto/recommendations"
    ]
    
    print(f"\n{'='*60}")
    print("🔍 TESTING DIRECT HTTP REQUESTS")
    print(f"{'='*60}")
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    })
    
    for url in test_urls:
        try:
            print(f"\n🌐 Testing: {url}")
            resp = session.get(url, timeout=30)
            
            print(f"  Status: {resp.status_code}")
            print(f"  Content-Length: {len(resp.text)}")
            print(f"  Title in HTML: {'<title>' in resp.text}")
            
            # Check for common blocking indicators
            blocked_indicators = [
                "access denied", "blocked", "captcha", "cloudflare", 
                "please enable javascript", "rate limited"
            ]
            
            content_lower = resp.text.lower()
            blocked = any(indicator in content_lower for indicator in blocked_indicators)
            
            if blocked:
                print(f"  ⚠️ Possible blocking detected")
            else:
                print(f"  ✅ Looks normal")
                
            # Show first 500 chars
            print(f"  Preview: {resp.text[:500]}...")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Test specific slug
        slug = sys.argv[1]
        test_single_manga(slug)
    else:
        print("🚀 Starting comprehensive Anime-Planet debug session")
        
        # 1. Test direct requests first
        test_direct_requests()
        
        # 2. Test with enhanced fetcher
        print(f"\n⏳ Waiting 30 seconds before enhanced tests...")
        import time
        time.sleep(30)
        
        test_multiple_slugs()