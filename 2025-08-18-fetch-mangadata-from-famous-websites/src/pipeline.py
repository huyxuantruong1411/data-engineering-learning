import logging
from typing import List, Dict, Optional
from pymongo import MongoClient
from extractors.mal import collect_mal, collect_mal_ranking_based
from extractors.anilist import collect_anilist
from extractors.animeplanet import collect_animeplanet
from extractors.mangaupdates import collect_mangaupdates
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
    # Đồng bộ với spider: mal -> mal_data, anilist -> anilist_data, etc.
    return db[f"{source}_data"]

def run_mal_ranking_based_crawl(start_limit: int = 0, max_pages: int = 100):
    """Run independent MAL ranking-based collection like project_dump.txt approach"""
    logger.info(f"Starting MAL ranking-based collection from limit {start_limit}")
    try:
        collection = get_mongo_collection("mal")
        results = collect_mal_ranking_based(start_limit, max_pages)
        
        inserted_count = 0
        for result in results:
            if result.get("status") in ["ok", "no_reviews"]:
                existing = collection.find_one({"_id": result["_id"]})
                if not existing:
                    collection.insert_one(result)
                    inserted_count += 1
                    logger.debug(f"Inserted MAL data for {result.get('source_id')}")
                else:
                    logger.debug(f"Skipped existing MAL data for {result.get('source_id')}")
            else:
                logger.warning(f"Failed MAL data for {result.get('source_id')}: {result.get('http', {})}")
        
        logger.info(f"MAL ranking-based collection completed: {inserted_count}/{len(results)} inserted")
        return results
    except Exception as e:
        logger.error(f"MAL ranking-based collection failed: {e}", exc_info=True)
        return []

def run_mal_manga_crawl():
    """Run MAL Manga Spider to crawl top manga list independently"""
    logger.info("Starting MAL Manga Crawler Spider")
    try:
        process = CrawlerProcess()
        process.crawl(MALMangaSpider)
        process.start()
        logger.info("MAL Manga Crawler completed")
    except Exception as e:
        logger.error(f"MAL Manga Crawler failed: {e}", exc_info=True)

def run_pipeline(limit: int = 5, skip: int = 0, only: Optional[List[str]] = None) -> List[Dict]:
    """Run the manga data pipeline for specified sources"""
    results = []
    sources = only if only else ["mal", "anilist", "mangaupdates", "animeplanet"]
    
    # Sample IDs for testing (replace with actual source of IDs, e.g., from a file or DB)
    sample_ids = {
        "mal": ["1", "2", "1706", "23390", "30013"],  # Example MAL IDs
        "anilist": ["30001", "30002", "31706", "87216", "98448"],
        "mangaupdates": ["1234", "5678", "9012", "3456", "7890"],
        "animeplanet": ["naruto", "one-piece", "attack-on-titan", "berserk", "fullmetal-alchemist"]
    }

    for source in sources:
        logger.info(f"Processing source: {source}")
        collection = get_mongo_collection(source)
        
        # Get IDs to process
        ids_to_process = sample_ids.get(source, [])[skip:skip+limit]
        
        for source_id in ids_to_process:
            try:
                logger.info(f"Fetching {source} data for ID: {source_id}")
                if source == "mal":
                    result = collect_mal(source_id=source_id)  # Không cần mangadex_id
                elif source == "anilist":
                    result = collect_anilist(source_id=source_id)
                elif source == "mangaupdates":
                    result = collect_mangaupdates(source_id=source_id)
                elif source == "animeplanet":
                    result = collect_animeplanet(source_id=source_id)
                else:
                    logger.warning(f"Unknown source: {source}")
                    continue
                
                # Insert into MongoDB
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