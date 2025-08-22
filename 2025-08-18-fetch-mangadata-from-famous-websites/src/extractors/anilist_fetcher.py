import logging
from datetime import datetime
from typing import Dict, List
import requests
import random
import time
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import requests.exceptions
# from ..common.config import ANTI_BLOCKING_CONFIG  # Removed problematic import

logger = logging.getLogger(__name__)

ANILIST_API = "https://graphql.anilist.co"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Configuration cho 87k objects trong 24h
TARGET_OBJECTS_PER_HOUR = 87000 / 24  # ~3625 objects/hour
TARGET_REQUESTS_PER_HOUR = TARGET_OBJECTS_PER_HOUR / 10  # ~362 requests/hour với batch size 10
SECONDS_PER_REQUEST = 3600 / TARGET_REQUESTS_PER_HOUR  # ~10 giây/request

@retry(
    retry=retry_if_exception_type((requests.exceptions.HTTPError, requests.exceptions.RequestException)),
    wait=wait_exponential(multiplier=1.2, min=5, max=60),  # Aggressive: min 5s, max 1 phút
    stop=stop_after_attempt(3),
    reraise=True
)
def _query_anilist_batch(manga_ids: List[str]) -> List[Dict]:
    """Query AniList API with aggressive rate limiting to avoid 429 errors"""
    query = """
    query ($ids: [Int]) {
      Page {
        media(id_in: $ids, type: MANGA) {
          id
          title { romaji english native }
          recommendations { edges { node { mediaRecommendation { id title { romaji } } } } }
          reviews { nodes { summary body } }
        }
      }
    }
    """
    variables = {"ids": [int(id) for id in manga_ids]}
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    # Minimal delay cho 24h target - chỉ 2-5s
    delay = random.uniform(2, 5)
    logger.info(f"Pre-request delay: {delay:.1f}s")
    time.sleep(delay)
    
    try:
        r = requests.post(ANILIST_API, json={"query": query, "variables": variables}, headers=headers, timeout=30)
        
        # Kiểm tra rate limit headers
        remaining = int(r.headers.get("X-RateLimit-Remaining", 90))
        reset_time = int(r.headers.get("X-RateLimit-Reset", 60))
        
        logger.info(f"Rate limit remaining: {remaining}, reset in: {reset_time}s")
        
        # Minimal rate limiting cho 24h target
        if remaining < 5:  # Chỉ sleep khi rất ít quota
            delay = random.uniform(10, 20)  # Chỉ 10-20s
            logger.warning(f"Very low rate limit ({remaining}), sleeping {delay:.1f}s")
            time.sleep(delay)
        elif remaining < 15:
            time.sleep(random.uniform(2, 5))  # Delay rất nhẹ
        
        r.raise_for_status()
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            # 429 handling cho 24h target
            retry_after = int(e.response.headers.get("Retry-After", 60))  # Default 1 phút
            buffer = random.uniform(5, 15)  # Minimal buffer
            total_wait = retry_after + buffer
            logger.error(f"Rate limited! Waiting {total_wait:.1f}s before retry")
            time.sleep(total_wait)
        raise
    return r.json()["data"]["Page"]["media"]

def get_full_data_parallel(al_id: str | List[str], max_workers: int = 3) -> List[Dict]:
    """Parallel version for high-volume processing"""
    if isinstance(al_id, str):
        al_id = [al_id]
    
    # Chia thành chunks cho parallel processing
    chunk_size = len(al_id) // max_workers if len(al_id) > max_workers else len(al_id)
    chunks = [al_id[i:i+chunk_size] for i in range(0, len(al_id), chunk_size)]
    
    all_payloads = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(get_full_data, chunk) for chunk in chunks]
        for future in futures:
            all_payloads.extend(future.result())
    
    return all_payloads

def get_full_data(al_id: str | List[str]) -> List[Dict]:
    if isinstance(al_id, str):
        al_id = [al_id]  # Convert single ID to list for consistency
    
    payloads = []
    for i in range(0, len(al_id), 10):  # Tăng batch size lên 10 cho 24h target
        batch_ids = al_id[i:i+10]
        
        # Minimal batch delay cho 24h target
        if i > 0:
            batch_delay = random.uniform(5, 8)  # Chỉ 5-8s giữa các batch
            logger.info(f"Waiting {batch_delay:.1f}s before next batch...")
            time.sleep(batch_delay)
        try:
            media_list = _query_anilist_batch(batch_ids)
            for media in media_list:
                al_id_str = str(media["id"])
                payload = {
                    "_id": f"anilist_{al_id_str}",
                    "source": "anilist",
                    "source_id": al_id_str,
                    "source_url": f"https://anilist.co/manga/{al_id_str}",
                    "fetched_at": datetime.utcnow().isoformat(),
                }
                recs = [{"id": str(edge["node"]["mediaRecommendation"]["id"]),
                        "title": edge["node"]["mediaRecommendation"]["title"]["romaji"]}
                       for edge in media["recommendations"]["edges"] if edge["node"]["mediaRecommendation"]]
                reviews = [{"text": r.get("summary") or r.get("body")}
                          for r in media["reviews"]["nodes"] if (r.get("summary") or r.get("body"))]
                payload["recommendations"] = recs
                payload["reviews"] = reviews
                payload["status"] = "ok" if (reviews or recs) else "no_reviews"
                payload["http"] = {"code": 200}
                payloads.append(payload)
        except Exception as e:
            logger.error("AniList batch fetch failed for IDs %s: %s", batch_ids, e, exc_info=True)
            for bid in batch_ids:
                payloads.append({
                    "_id": f"anilist_{bid}",
                    "source": "anilist",
                    "source_id": bid,
                    "source_url": f"https://anilist.co/manga/{bid}",
                    "fetched_at": datetime.utcnow().isoformat(),
                    "recommendations": [],
                    "reviews": [],
                    "status": "error",
                    "http": {"error": str(e)}
                })
    
    return payloads