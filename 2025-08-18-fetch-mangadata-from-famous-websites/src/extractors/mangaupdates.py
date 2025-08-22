import logging
from .mangaupdates_fetcher import get_full_data

logger = logging.getLogger(__name__)

def collect_mangaupdates(mangadex_id: str, source_id: str):
    """
    Collector entrypoint for MangaUpdates.
    - source_id: MU numeric id or slug
    """
    try:
        payload = get_full_data(source_id)
        payload.setdefault("_id", f"mu_{source_id}")
        payload.setdefault("source", "mangaupdates")
        payload.setdefault("source_id", source_id)
        return payload
    except Exception as e:
        logger.error("collect_mangaupdates failed for %s: %s", source_id, e, exc_info=True)
        return {
            "_id": f"mu_{source_id}",
            "source": "mangaupdates",
            "source_id": source_id,
            "reviews": [],
            "recommendations": [],
            "status": "error",
            "http": {"error": str(e)},
        }