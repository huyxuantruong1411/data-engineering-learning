#!/usr/bin/env python3
"""
Production MAL data collection script
Collects all manga data from MyAnimeList using parallel processing
"""
import logging
import time
from pymongo import MongoClient
from src.extractors.mal import collect_mal_batch
from concurrent.futures import ThreadPoolExecutor
import sys

# Production logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mal_production.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# MongoDB connection
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "manga_raw_data"

def get_mal_id_range():
    """Generate MAL manga ID range for production collection"""
    # MAL manga IDs typically range from 1 to ~150,000+
    # Start with confirmed range and expand as needed
    start_id = 1
    end_id = 150000  # Adjust based on current MAL database size
    
    return list(range(start_id, end_id + 1))

def get_existing_mal_ids():
    """Get already collected MAL IDs to avoid duplicates"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db["mal_data"]
    
    existing_ids = set()
    cursor = collection.find({}, {"source_id": 1})
    for doc in cursor:
        existing_ids.add(doc.get("source_id"))
    
    client.close()
    logger.info(f"Found {len(existing_ids)} existing MAL entries")
    return existing_ids

def save_mal_results(results):
    """Save results to MongoDB"""
    if not results:
        return
    
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db["mal_data"]
    
    saved_count = 0
    for result in results:
        try:
            collection.update_one(
                {"_id": result["_id"]},
                {"$set": result},
                upsert=True
            )
            saved_count += 1
        except Exception as e:
            logger.error(f"Error saving {result.get('_id', 'unknown')}: {e}")
    
    client.close()
    logger.info(f"Saved {saved_count}/{len(results)} results to database")

def run_production_collection():
    """Run full production MAL collection"""
    logger.info("🚀 Starting PRODUCTION MAL collection")
    logger.info("Target: Complete MyAnimeList manga database")
    
    # Get full ID range
    all_mal_ids = get_mal_id_range()
    logger.info(f"Total MAL ID range: {len(all_mal_ids)} manga")
    
    # Filter out existing IDs
    existing_ids = get_existing_mal_ids()
    remaining_ids = [str(mid) for mid in all_mal_ids if str(mid) not in existing_ids]
    
    logger.info(f"Remaining to collect: {len(remaining_ids)} manga")
    logger.info(f"Estimated time: {len(remaining_ids) * 4 / 3600:.1f} hours with parallel processing")
    
    if not remaining_ids:
        logger.info("✅ All MAL manga already collected!")
        return
    
    # Process in large batches for efficiency
    batch_size = 100  # Process 100 manga at a time
    total_batches = len(remaining_ids) // batch_size + (1 if len(remaining_ids) % batch_size else 0)
    
    start_time = time.time()
    total_processed = 0
    
    for i in range(0, len(remaining_ids), batch_size):
        batch_ids = remaining_ids[i:i+batch_size]
        batch_num = i // batch_size + 1
        
        logger.info(f"📦 Processing batch {batch_num}/{total_batches}: {len(batch_ids)} manga")
        
        try:
            # Use batch collection with parallel processing
            batch_results = collect_mal_batch(batch_ids, batch_size=20)
            
            # Save results immediately
            save_mal_results(batch_results)
            
            total_processed += len(batch_results)
            elapsed = time.time() - start_time
            rate = total_processed / elapsed if elapsed > 0 else 0
            
            # Progress report
            logger.info(f"✅ Batch {batch_num} completed")
            logger.info(f"📊 Progress: {total_processed}/{len(remaining_ids)} ({total_processed/len(remaining_ids)*100:.1f}%)")
            logger.info(f"⚡ Rate: {rate:.1f} manga/second")
            logger.info(f"⏱️ ETA: {(len(remaining_ids) - total_processed) / rate / 3600:.1f} hours")
            
        except Exception as e:
            logger.error(f"❌ Batch {batch_num} failed: {e}")
            continue
        
        # Brief pause between batches
        time.sleep(2)
    
    total_time = time.time() - start_time
    logger.info(f"🎉 Production collection completed!")
    logger.info(f"📊 Total processed: {total_processed} manga")
    logger.info(f"⏱️ Total time: {total_time/3600:.1f} hours")
    logger.info(f"⚡ Average rate: {total_processed/total_time:.1f} manga/second")

if __name__ == "__main__":
    try:
        run_production_collection()
    except KeyboardInterrupt:
        logger.info("🛑 Collection stopped by user")
    except Exception as e:
        logger.error(f"💥 Production collection failed: {e}", exc_info=True)
