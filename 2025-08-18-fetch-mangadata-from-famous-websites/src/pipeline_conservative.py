import logging
from typing import List, Dict, Optional
from pymongo import MongoClient
from src.extractors.mal import collect_mal, collect_mal_parallel
from .extractors.anilist import collect_anilist
from .extractors.animeplanet import collect_animeplanet
from .extractors.mangaupdates import collect_mangaupdates
from scrapy.crawler import CrawlerProcess
from spiders.mal_manga_spider import MALMangaSpider

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MongoDB connection
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "manga_raw_data"

def get_mongo_collection(source: str):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[f"{source}_data"]

def run_mal_manga_crawl():
    """Run MAL Manga Spider to crawl top manga list"""
    logger.info("Starting MAL Manga Crawler Spider")
    try:
        process = CrawlerProcess()
        process.crawl(MALMangaSpider)
        process.start()
        logger.info("MAL Manga Crawler completed")
    except Exception as e:
        logger.error(f"MAL Manga Crawler failed: {e}", exc_info=True)

def run_conservative_pipeline(limit: int = 5, skip: int = 0, only: Optional[List[str]] = None) -> List[Dict]:
    """Run the manga data pipeline for specified sources"""
    results = []
    sources = only if only else ["mal", "anilist", "mangaupdates", "animeplanet"]
    
    # Sample IDs for testing (replace with actual source, e.g., from DB or file)
    sample_ids = {
        "mal": [
            # Top popular manga IDs
            "1", "2", "11", "74", "988", "1706", "3438", "7519", "15578", "23390",
            "30013", "44347", "85143", "100005", "101517", "103851", "104175", "106479", 
            "108556", "109957", "110277", "111435", "112323", "113138", "114745", "115138",
            "116778", "117195", "118586", "119161", "120906", "121496", "122663", "123456",
            "124578", "125234", "126789", "127890", "128901", "129012", "130123", "131234",
            "132345", "133456", "134567", "135678", "136789", "137890", "138901", "139012",
            "140123", "141234", "142345", "143456", "144567"  # 55 IDs for testing
        ],
        "anilist": ["30001", "30002", "31706", "87216", "98448"],
        "mangaupdates": ["1234", "5678", "9012", "3456", "7890"],
        "animeplanet": ["naruto", "one-piece", "attack-on-titan", "berserk", "fullmetal-alchemist"]
    }

    for source in sources:
        logger.info(f"Processing source: {source}")
        collection = get_mongo_collection(source)
        
        ids_to_process = sample_ids.get(source, [])[skip:skip+limit]
        
        # Use parallel processing for MAL to speed up large batches
        if source == "mal" and len(ids_to_process) > 5:
            logger.info(f"Using parallel processing for {len(ids_to_process)} MAL manga")
            try:
                batch_results = collect_mal_parallel(ids_to_process, max_workers=4)
                for result in batch_results:
                    if result.get("status") in ["ok", "no_reviews"]:
                        existing = collection.find_one({"_id": result["_id"]})
                        if not existing:
                            collection.insert_one(result)
                            logger.info(f"Inserted {source} data for {result['source_id']}")
                        else:
                            logger.info(f"Skipped existing {source} data for {result['source_id']}")
                    else:
                        logger.warning(f"Failed to fetch {source} data for {result['source_id']}: {result.get('http', {})}")
                    results.append(result)
            except Exception as e:
                logger.error(f"Parallel processing failed for {source}: {e}", exc_info=True)
        else:
            # Sequential processing for other sources or small batches
            for source_id in ids_to_process:
                try:
                    logger.info(f"Fetching {source} data for ID: {source_id}")
                    if source == "mal":
                        result = collect_mal(source_id=source_id)
                    elif source == "anilist":
                        result = collect_anilist(source_id=source_id)
                    elif source == "mangaupdates":
                        result = collect_mangaupdates(source_id=source_id)
                    elif source == "animeplanet":
                        result = collect_animeplanet(source_id=source_id)
                    else:
                        logger.warning(f"Unknown source: {source}")
                        continue
                    
                    if result.get("status") in ["ok", "no_reviews"]:
                        existing = collection.find_one({"_id": result["_id"]})
                        if not existing:
                            collection.insert_one(result)
                            logger.info(f"Inserted {source} data for {source_id}")
                        else:
                            logger.info(f"Skipped existing {source} data for {source_id}")
                    else:
                        logger.warning(f"Failed to fetch {source} data for {source_id}: {result.get('http', {})}")
                    
                    results.append(result)
                    
                except Exception as e:
                    logger.error(f"Error processing {source} ID {source_id}: {e}", exc_info=True)
                    results.append({
                        "_id": f"{source}_{source_id}",
                        "source": source,
                        "source_id": source_id,
                        "status": "error",
                        "http": {"error": str(e)}
                    })
        
    return results