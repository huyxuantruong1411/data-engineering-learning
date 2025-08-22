import logging
from typing import Dict, List
from .mal_fetcher import get_full_data, get_ranking_based_data, get_full_data_parallel

logger = logging.getLogger(__name__)

def collect_mal(source_id: str):
    """
    Collector entrypoint for MAL - single manga by ID.
    - source_id: MAL numeric ID
    """
    try:
        payload = get_full_data(source_id)
        payload.setdefault("_id", f"mal_{source_id}")
        payload.setdefault("source", "mal")
        payload.setdefault("source_id", source_id)
        return payload
    except Exception as e:
        logger.error("collect_mal failed for %s: %s", source_id, e, exc_info=True)
        return {
            "_id": f"mal_{source_id}",
            "source": "mal",
            "source_id": source_id,
            "reviews": [],
            "recommendations": [],
            "manga_info": {},
            "status": "error",
            "http": {"error": str(e)},
        }

def collect_mal_ranking_based(start_limit: int = 0, max_pages: int = 100) -> List[Dict]:
    """Independent MAL collection using ranking pages like project_dump.txt"""
    logger.info(f"Starting MAL ranking-based collection from limit {start_limit}")
    
    # FALLBACK: If ranking pages fail, use known manga IDs for testing
    results = get_ranking_based_data(start_limit, max_pages)
    
    if not results:
        logger.warning("⚠️ Ranking pages failed - using fallback manga IDs for testing")
        # Popular manga IDs for testing
        test_ids = ["1", "2", "11", "13", "21", "30", "44", "74", "85", "121"]
        logger.info(f"🔄 Testing with {len(test_ids)} popular manga IDs")
        
        from .mal_fetcher import get_full_data_parallel
        results = get_full_data_parallel(test_ids, 3)
        logger.info(f"✅ Fallback collection completed: {len(results)} manga")
    
    return results

def collect_mal_parallel(source_ids: List[str], max_workers: int = 4):
    """
    Parallel collector for multiple MAL IDs with anti-blocking
    - source_ids: List of MAL numeric IDs
    - max_workers: Number of parallel workers
    """
    try:
        results = get_full_data_parallel(source_ids, max_workers)
        return results
    except Exception as e:
        logger.error(f"collect_mal_parallel failed: {e}", exc_info=True)
        return [{
            "_id": f"mal_{sid}",
            "source": "mal",
            "source_id": sid,
            "reviews": [],
            "recommendations": [],
            "manga_info": {},
            "status": "error",
            "http": {"error": str(e)},
        } for sid in source_ids]

def collect_mal_batch(source_ids: List[str], batch_size: int = 20):
    """
    Batch collector for large-scale MAL processing - optimized for 24h target
    - source_ids: List of MAL numeric IDs
    - batch_size: Size of each processing batch (increased for speed)
    """
    try:
        results = get_batch_data(source_ids, batch_size)
        return results
    except Exception as e:
        logger.error(f"collect_mal_batch failed: {e}", exc_info=True)
        return [{
            "_id": f"mal_{sid}",
            "source": "mal",
            "source_id": sid,
            "reviews": [],
            "recommendations": [],
            "manga_info": {},
            "status": "error",
            "http": {"error": str(e)},
        } for sid in source_ids]