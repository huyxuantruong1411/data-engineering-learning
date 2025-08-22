# mal_crawler.py
"""
Async MAL manga crawler -> MongoDB
- Default: run without args -> auto-resume until collected TARGET_COUNT (default 78_000)
- Saves into mongodb://localhost:27017/, db=manga_raw_data, collection=mal_data
- Each doc: one manga, fields: manga_id (int), metadata, reviews (list), recommendations (list with rec_manga_id), fetched_at, status, http
- Usage:
    python mal_crawler.py               # default auto run (targets 78k)
    python mal_crawler.py --concurrency 120 --reviews-concurrency 20 --target 78000 --base-url https://api.jikan.moe/v4
"""

import asyncio
import aiohttp
import argparse
import random
import re
import sys
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from pymongo import MongoClient, errors as pymongo_errors
from loguru import logger
from typing import Optional, Dict, Tuple

# -------------------------
# Configuration defaults
# -------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "manga_raw_data"
COLLECTION_NAME = "mal_data"

DEFAULT_BASE_URL = "https://api.jikan.moe/v4"  # or self-hosted e.g. http://localhost:8080/v4
TARGET_COUNT_DEFAULT = 78000

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# -------------------------
# Logging setup (fix dupes / placeholders)
# -------------------------
logger.remove()  # remove default sink to avoid duplicates
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level="INFO")

# -------------------------
# Helpers
# -------------------------
def make_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
    }

def extract_id_from_url(url: str) -> Optional[str]:
    # Find '/manga/{id}' patterns and return the id as string
    m = re.search(r"/manga/(\d+)", url or "")
    return m.group(1) if m else None

# -------------------------
# Mongo helper
# -------------------------
def get_mongo_collection(uri=MONGO_URI, db_name=DB_NAME, coll_name=COLLECTION_NAME):
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        client.server_info()
    except Exception as e:
        logger.error(f"Cannot connect to MongoDB at {uri}: {e}")
        raise
    db = client[db_name]
    coll = db[coll_name]
    try:
        coll.create_index("manga_id", unique=True, background=True)
    except Exception:
        pass
    return coll

# -------------------------
# Async HTTP with retry/backoff
# -------------------------
async def async_get_with_retry(session: aiohttp.ClientSession, url: str, params: dict=None,
                               max_attempts: int = 12, initial_backoff: float = 1.0) -> Tuple[Optional[Dict], Optional[int], Optional[str]]:
    """
    Return (json_data_or_None, http_status, error_message_or_None)
    Handles 429 (Retry-After), 5xx with exponential backoff and jitter.
    4xx (404) returns (None, status, "client_error")
    """
    attempt = 0
    backoff = initial_backoff
    while attempt < max_attempts:
        attempt += 1
        try:
            headers = make_headers()
            async with session.get(url, params=params, headers=headers, timeout=30) as resp:
                status = resp.status
                if status == 429:
                    ra = resp.headers.get("Retry-After")
                    wait = float(ra) + random.uniform(1, 5) if ra else min(60, backoff * 2) + random.uniform(1, 5)
                    logger.warning(f"429 for {url} -> sleeping {wait:.1f}s (attempt {attempt}/{max_attempts})")
                    await asyncio.sleep(wait)
                    backoff *= 2
                    continue
                if 500 <= status < 600:
                    logger.warning(f"Server error {status} for {url} (attempt {attempt}/{max_attempts}), backoff {backoff:.1f}s")
                    await asyncio.sleep(backoff + random.uniform(0, 0.5))
                    backoff *= 2
                    continue
                if 400 <= status < 500:
                    text = await resp.text()
                    logger.debug(f"Client {status} for {url} -> {text[:200]}")
                    return None, status, f"client_error_{status}"
                # success
                try:
                    j = await resp.json()
                    return j, status, None
                except Exception as e:
                    txt = await resp.text()
                    logger.error(f"Invalid JSON from {url}: {e}; snippet: {txt[:200]}")
                    return None, status, "invalid_json"
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Network error for {url}: {e} (attempt {attempt}/{max_attempts})")
            await asyncio.sleep(backoff + random.uniform(0, 0.5))
            backoff *= 2
            continue
    return None, None, f"max_attempts_exceeded_after_{max_attempts}"

# -------------------------
# Fetchers (metadata / recommendations / reviews)
# -------------------------
async def fetch_metadata(session, base_url, manga_id) -> Dict[str, Any]:
    url = f"{base_url}/manga/{manga_id}/full"
    data, status, err = await async_get_with_retry(session, url)
    return {"http_code": status, "data": data, "error": err}

async def fetch_recommendations(session, base_url, manga_id, max_pages=5) -> Dict[str, Any]:
    recs: List[Dict[Any,Any]] = []
    for page in range(1, max_pages + 1):
        url = f"{base_url}/manga/{manga_id}/recommendations"
        params = {"page": page}
        d, status, err = await async_get_with_retry(session, url, params=params)
        if d and isinstance(d, dict) and d.get("data"):
            recs_page = d["data"]
            # Normalize recs and extract rec_manga_id
            for r in recs_page:
                # Jikan/others may include 'entry' or 'url' fields; try to robustly extract id
                rec_obj = dict(r)  # shallow copy
                # find any url field
                found_id = None
                # common places: r.get("entry") -> list of dicts with 'url' or 'mal_id'
                if "entry" in r and isinstance(r["entry"], list) and r["entry"]:
                    perhaps = r["entry"][0]
                    if isinstance(perhaps, dict):
                        # try mal_id or url
                        if "mal_id" in perhaps:
                            found_id = str(perhaps["mal_id"])
                        elif "url" in perhaps:
                            found_id = extract_id_from_url(perhaps["url"])
                # try r.get("url") or r.get("recommendation_url")
                if not found_id:
                    for key in ("url", "recommendation_url", "href"):
                        if key in r and isinstance(r[key], str):
                            found_id = extract_id_from_url(r[key])
                            if found_id:
                                break
                # try nested 'recommendation' objects
                if not found_id:
                    # try convert anything that looks like '/manga/123'
                    txt = str(r)
                    found_id = extract_id_from_url(txt)
                rec_obj["rec_manga_id"] = found_id
                recs.append(rec_obj)
            # if page returned less than typical page size (likely last), break
            if len(recs_page) < 20:
                break
        else:
            break
    return {"http_code": 200 if recs else None, "recommendations": recs}

async def fetch_reviews(session, base_url, manga_id, max_pages=9999) -> Dict[str, Any]:
    reviews: List[Dict[Any,Any]] = []
    page = 1
    while page <= max_pages:
        url = f"{base_url}/manga/{manga_id}/reviews"
        params = {"page": page}
        d, status, err = await async_get_with_retry(session, url, params=params)
        if d and isinstance(d, dict) and d.get("data"):
            block = d["data"]
            reviews.extend(block)
            page += 1
            # break early when small page
            if len(block) < 20:
                break
        else:
            # 404 or no data -> stop
            break
    return {"http_code": 200 if reviews else None, "reviews": reviews}

# -------------------------
# Build & save document
# -------------------------
def build_doc(manga_id: int, metadata_res: Dict, recs_res: Dict, reviews_res: Dict) -> Dict:
    doc = {
        "_id": f"mal_{manga_id}",
        "manga_id": int(manga_id),
        "source": "mal_jikan",
        "source_url": f"https://myanimelist.net/manga/{manga_id}",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "metadata": metadata_res.get("data"),
        "recommendations": recs_res.get("recommendations", []),
        "reviews": reviews_res.get("reviews", []),
        "status": "ok" if metadata_res.get("data") or recs_res.get("recommendations") or reviews_res.get("reviews") else "no_data",
        "http": {
            "metadata": metadata_res.get("http_code"),
            "recommendations": recs_res.get("http_code"),
            "reviews": reviews_res.get("http_code"),
        }
    }
    errors = {}
    for k, v in (("metadata", metadata_res), ("recommendations", recs_res), ("reviews", reviews_res)):
        if v and v.get("error"):
            errors[k] = v["error"]
    if errors:
        doc["errors"] = errors
        doc["status"] = "partial_error"
    return doc

def save_doc_sync(coll, doc: Dict):
    # upsert by manga_id
    try:
        res = coll.update_one({"manga_id": doc["manga_id"]}, {"$set": doc}, upsert=True)
        logger.info(f"Saved manga_id={doc['manga_id']} (matched={res.matched_count} upserted_id={res.upserted_id})")
    except Exception as e:
        logger.exception(f"Mongo save error for {doc.get('manga_id')}: {e}")
        raise

# -------------------------
# Worker per manga (async)
# -------------------------
async def process_manga(manga_id: int, session: aiohttp.ClientSession, base_url: str,
                        coll, reviews_pages_limit: int):
    logger.info(f"Start processing manga_id={manga_id}")
    meta = await fetch_metadata(session, base_url, manga_id)
    # recommendations & metadata can be fetched in parallel
    recs_task = asyncio.create_task(fetch_recommendations(session, base_url, manga_id, max_pages=5))
    # reviews we may fetch now or defer; here we fetch up to reviews_pages_limit
    reviews_task = asyncio.create_task(fetch_reviews(session, base_url, manga_id, max_pages=reviews_pages_limit))
    recs = await recs_task
    reviews = await reviews_task
    doc = build_doc(manga_id, meta, recs, reviews)
    # save synchronously via threadpool to avoid blocking event loop
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, save_doc_sync, coll, doc)
    return doc.get("status", "unknown")

# -------------------------
# Controller: automatic loop (no start/end)
# -------------------------
async def run_auto(base_url: str, target_count: int, concurrency: int, reviews_pages_limit: int,
                   resume_existing: bool, mongo_uri: str):
    coll = get_mongo_collection(mongo_uri)
    # determine where to start: find max manga_id saved, else 1
    last_doc = coll.find_one(sort=[("manga_id", -1)])
    if resume_existing and last_doc:
        start_id = int(last_doc["manga_id"]) + 1
        logger.info(f"Resuming from manga_id {start_id} (last saved {last_doc['manga_id']})")
    else:
        start_id = 1
    collected = coll.count_documents({"status": {"$in": ["ok", "partial_error"]}})
    logger.info(f"Already collected {collected} docs. Target is {target_count}. Starting at id={start_id}")

    connector = aiohttp.TCPConnector(limit_per_host=concurrency*2, limit=concurrency*3)
    timeout = aiohttp.ClientTimeout(total=60)
    sem = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # We will schedule tasks in a sliding window style
        next_id = start_id
        pending = set()
        try:
            while collected < target_count:
                # fill up to concurrency tasks
                while len(pending) < concurrency and collected + len(pending) < target_count:
                    mid = next_id
                    next_id += 1

                    async def worker_wrapper(m_id):
                        async with sem:
                            try:
                                status = await process_manga(m_id, session, base_url, coll, reviews_pages_limit)
                                return m_id, status, None
                            except Exception as e:
                                return m_id, "fatal_error", str(e)

                    task = asyncio.create_task(worker_wrapper(mid))
                    pending.add(task)

                # wait for at least one to finish
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for d in done:
                    m_id, status, err = d.result()
                    if status in ("ok", "partial_error"):
                        collected += 1
                    if err:
                        logger.warning(f"ID {m_id} finished with status={status} error={err}")
                    else:
                        logger.info(f"ID {m_id} finished {status}. Collected so far: {collected}/{target_count}")
                # small sleep to avoid tight loop
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            logger.warning("Run cancelled")
        except Exception as e:
            logger.exception(f"Fatal controller error: {e}")
        finally:
            logger.info(f"Run stopped. Collected {collected} docs. Next_id would be {next_id}")

# -------------------------
# CLI / main
# -------------------------
def main():
    parser = argparse.ArgumentParser(description="Async MAL crawler -> MongoDB (auto mode default)")
    parser.add_argument("--base-url", type=str, default=DEFAULT_BASE_URL, help="Jikan base URL (use local self-host for speed)")
    parser.add_argument("--concurrency", type=int, default=80, help="Number of concurrent manga tasks (default 80). Increase if self-hosting.")
    parser.add_argument("--reviews-concurrency", type=int, default=20, help="(Not used separately in current impl) reviews per-manga pages limit controls load")
    parser.add_argument("--reviews-pages", type=int, default=3, help="Max reviews pages to fetch per manga by default (set large for full reviews).")
    parser.add_argument("--target", type=int, default=TARGET_COUNT_DEFAULT, help="Target total mangas to collect (default 78000)")
    parser.add_argument("--resume-existing", action="store_true", help="Resume from last saved id if present (default false)")
    parser.add_argument("--mongo-uri", type=str, default=MONGO_URI, help="Mongo URI")
    parser.add_argument("--start", type=int, help="(optional) start id inclusive (overrides auto resume)")
    parser.add_argument("--end", type=int, help="(optional) end id inclusive (if given script will process range and exit)")
    args = parser.parse_args()

    logger.info("Starting MAL crawler")
    # if explicit range provided, do a simple range-run (sync wrapper for async)
    if args.start and args.end:
        # small helper to run limited range (use concurrency)
        async def run_range():
            coll = get_mongo_collection(args.mongo_uri)
            connector = aiohttp.TCPConnector(limit_per_host=args.concurrency*2, limit=args.concurrency*3)
            timeout = aiohttp.ClientTimeout(total=60)
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                sem = asyncio.Semaphore(args.concurrency)
                tasks = []
                for mid in range(args.start, args.end + 1):
                    async def worker(m_id):
                        async with sem:
                            try:
                                st = await process_manga(m_id, session, args.base_url, coll, args.reviews_pages)
                                return m_id, st, None
                            except Exception as e:
                                return m_id, "fatal_error", str(e)
                    tasks.append(asyncio.create_task(worker(mid)))
                results = await asyncio.gather(*tasks, return_exceptions=False)
                logger.info("Range run complete")
        asyncio.run(run_range())
    else:
        # run auto until target_count reached
        asyncio.run(run_auto(args.base_url, args.target, args.concurrency, args.reviews_pages, args.resume_existing, args.mongo_uri))

if __name__ == "__main__":
    main()
