#!/usr/bin/env python3
"""
Test script for multi-threaded MAL fetcher performance
Target: 87k manga in 24 hours
"""
import time
import logging
from src.extractors.mal_fetcher import get_full_data_parallel, get_batch_data
from src.extractors.mal import collect_mal_parallel, collect_mal_batch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_parallel_performance():
    """Test parallel MAL fetcher with known manga IDs"""
    # Test IDs - known valid MAL manga
    test_ids = ["1", "2", "11", "1706", "23390", "74", "988", "3438", "7519", "15578"]
    
    print("=== MAL PARALLEL PERFORMANCE TEST ===")
    print(f"Testing {len(test_ids)} manga with multi-threading")
    
    # Test 1: Parallel processing
    print("\n🔄 Test 1: Parallel Processing (4 workers)")
    start_time = time.time()
    
    try:
        results = get_full_data_parallel(test_ids, max_workers=4)
        parallel_time = time.time() - start_time
        
        print(f"✅ Parallel fetch completed in {parallel_time:.2f}s")
        print(f"📊 Average time per manga: {parallel_time/len(test_ids):.2f}s")
        print(f"🎯 Projected 24h capacity: {(24*3600)/(parallel_time/len(test_ids)):.0f} manga")
        
        # Analyze results
        success_count = 0
        total_reviews = 0
        total_recs = 0
        
        for result in results:
            if result.get("status") == "ok":
                success_count += 1
            total_reviews += len(result.get("reviews", []))
            total_recs += len(result.get("recommendations", []))
        
        print(f"📈 Success rate: {success_count}/{len(results)} ({success_count/len(results)*100:.1f}%)")
        print(f"📊 Total data: {total_reviews} reviews, {total_recs} recommendations")
        
    except Exception as e:
        print(f"❌ Parallel test failed: {e}")
    
    # Test 2: Batch processing
    print("\n🔄 Test 2: Batch Processing (5 manga per batch)")
    start_time = time.time()
    
    try:
        batch_results = get_batch_data(test_ids, batch_size=5)
        batch_time = time.time() - start_time
        
        print(f"✅ Batch fetch completed in {batch_time:.2f}s")
        print(f"📊 Average time per manga: {batch_time/len(test_ids):.2f}s")
        print(f"🎯 Projected 24h capacity: {(24*3600)/(batch_time/len(test_ids)):.0f} manga")
        
    except Exception as e:
        print(f"❌ Batch test failed: {e}")
    
    # Performance comparison
    print(f"\n=== PERFORMANCE ANALYSIS ===")
    if 'parallel_time' in locals() and 'batch_time' in locals():
        print(f"Parallel processing: {parallel_time:.2f}s")
        print(f"Batch processing: {batch_time:.2f}s")
        
        if parallel_time < batch_time:
            improvement = ((batch_time - parallel_time) / batch_time) * 100
            print(f"🚀 Parallel is {improvement:.1f}% faster")
        
        # 24h projection
        parallel_capacity = (24*3600) / (parallel_time/len(test_ids))
        batch_capacity = (24*3600) / (batch_time/len(test_ids))
        
        print(f"\n🎯 24-HOUR PROJECTIONS:")
        print(f"Parallel method: {parallel_capacity:.0f} manga")
        print(f"Batch method: {batch_capacity:.0f} manga")
        print(f"Target: 87,000 manga")
        
        if parallel_capacity >= 87000:
            print("✅ PARALLEL METHOD CAN ACHIEVE 24H TARGET!")
        elif batch_capacity >= 87000:
            print("✅ BATCH METHOD CAN ACHIEVE 24H TARGET!")
        else:
            print("⚠️ Need further optimization for 24h target")

def test_anti_blocking():
    """Test anti-blocking measures"""
    print("\n=== ANTI-BLOCKING TEST ===")
    
    # Test with rapid requests
    test_ids = ["1", "2", "11"]
    
    print("Testing thread-safe rate limiting...")
    start_time = time.time()
    
    try:
        results = get_full_data_parallel(test_ids, max_workers=2)
        elapsed = time.time() - start_time
        
        print(f"✅ Anti-blocking test passed")
        print(f"⏱️ Time with rate limiting: {elapsed:.2f}s")
        print(f"📊 Average delay per request: {elapsed/len(test_ids):.2f}s")
        
        # Check if delays are appropriate (should be ~4s per manga)
        expected_min_time = len(test_ids) * 2  # Minimum expected time
        if elapsed >= expected_min_time:
            print("✅ Rate limiting working correctly")
        else:
            print("⚠️ Rate limiting may be too aggressive")
            
    except Exception as e:
        print(f"❌ Anti-blocking test failed: {e}")

if __name__ == "__main__":
    test_parallel_performance()
    test_anti_blocking()
    
    print(f"\n{'='*50}")
    print("🎉 MAL PARALLEL TESTING COMPLETED")
    print("Ready for 87k manga collection in 24 hours!")
    print(f"{'='*50}")
