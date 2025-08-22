import logging
from .animeplanet_fetcher import get_full_data

logger = logging.getLogger(__name__)

def collect_animeplanet(mangadex_id: str, source_id: str):
    """
    Collector entrypoint for Anime-Planet.
    - mangadex_id: MangaDex document id
    - source_id: slug of anime-planet manga (e.g. "naruto")
    """
    try:
        payload = get_full_data(source_id)
        payload.setdefault("_id", f"ap_{source_id}")
        payload.setdefault("source", "animeplanet")
        payload.setdefault("source_id", source_id)
        return payload
    except Exception as e:
        logger.error("collect_animeplanet failed for %s: %s", source_id, e, exc_info=True)
        return {
            "_id": f"ap_{source_id}",
            "source": "animeplanet",
            "source_id": source_id,
            "reviews": [],
            "recommendations": [],
            "status": "error",
            "http": {"error": str(e)},
        }