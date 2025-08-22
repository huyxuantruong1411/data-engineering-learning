import logging
from .anilist_fetcher import get_full_data, get_full_data_parallel
from typing import List

logger = logging.getLogger(__name__)

def collect_anilist(mangadx_id: str, source_id: str):
    """
    Collector entrypoint for AniList.
    - source_id: AniList numeric id
    """
    try:
        payload = get_full_data(source_id)
        payload.setdefault("_id", f"anilist_{source_id}")
        payload.setdefault("source", "anilist")
        payload.setdefault("source_id", source_id)
        return payload
    except Exception as e:
        logger.error("collect_anilist failed for %s: %s", source_id, e, exc_info=True)
        return {
            "_id": f"anilist_{source_id}",
            "source": "anilist",
            "source_id": source_id,
            "reviews": [],
            "recommendations": [],
            "status": "error",
            "http": {"error": str(e)},
        }

def collect_anilist_batch(manga_ids: List[str], use_parallel: bool = True):
    """
    High-performance batch collector for 87k objects in 24h
    - manga_ids: List of AniList IDs
    - use_parallel: Enable parallel processing (3 workers)
    """
    try:
        if use_parallel and len(manga_ids) > 50:
            return get_full_data_parallel(manga_ids, max_workers=3)
        else:
            return get_full_data(manga_ids)
    except Exception as e:
        logger.error("collect_anilist_batch failed: %s", e, exc_info=True)
        return []