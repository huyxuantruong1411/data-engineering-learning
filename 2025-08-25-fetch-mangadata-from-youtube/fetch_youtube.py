#!/usr/bin/env python3
"""
fetch_youtube.py (global threadpool + jitter + manga-level progress log + captcha backoff)

- Đọc manga titles (en/vi) từ collection mangadex_manga trong DB manga_raw_data.
- Với mỗi title -> search YouTube -> parse ytInitialData -> extract metadata
- Filter: view_count >= 1000 AND language in {en, vi}
- Upsert vào collection youtube_videos
- Có xử lý captcha / 429: sleep dài 5–15 phút rồi retry
- CLI:
    --limit   giới hạn số lượng manga cần xử lý
    --workers số thread song song (mặc định 5)
"""

import argparse
import json
import random
import time
import html
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import requests
import regex  # pip install regex
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

# -----------------------
# Config
# -----------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "manga_raw_data"
MANGA_COLL = "mangadex_manga"
OUT_COLL = "youtube_videos"

USER_AGENTS = [
    # Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.122 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.129 Safari/537.36",

    # Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.5; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",

    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.128 Safari/537.36 Edg/122.0.6261.128",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.128 Safari/537.36 Edg/122.0.6261.128",

    # Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",

    # Chrome Android
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 12; Samsung Galaxy S22) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36",
]

MIN_VIEWS = 1000

VIETNAMESE_DIACRITIC_RE = regex.compile(
    r"[ăâêôơưđĂÂÊÔƠƯĐáàảãạấầẩẫậắằẳẵặéèẻẽẹếềểễệíìỉĩịóòỏõọốồổỗộớờởỡợúùủũụứừửữựýỳỷỹỵ]",
    flags=regex.UNICODE,
)
CJK_RE = regex.compile(r"[\p{Han}\p{Hiragana}\p{Katakana}\p{Hangul}]", flags=regex.UNICODE)


# -----------------------
# Utilities
# -----------------------
def random_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.youtube.com/",
    }


class CaptchaError(Exception):
    pass


class TooManyRequestsError(Exception):
    pass


@retry(
    retry=retry_if_exception_type((requests.exceptions.RequestException, CaptchaError, TooManyRequestsError)),
    wait=wait_exponential(multiplier=1, min=2, max=300),
    stop=stop_after_attempt(10),
    reraise=True,
)
def fetch_search_html(query: str, session: requests.Session, timeout: int = 30) -> str:
    """Fetch search results HTML for a query (with retries + jitter + captcha backoff)."""
    time.sleep(random.uniform(1.0, 3.5))
    url = "https://www.youtube.com/results"
    params = {"search_query": query}
    r = session.get(url, params=params, headers=random_headers(), timeout=timeout)

    if r.status_code == 429:
        print(f"  [WARN] 429 Too Many Requests for query '{query}', sleeping 5–15 min...")
        time.sleep(random.uniform(300, 900))  # 5–15 phút
        raise TooManyRequestsError("429 Too Many Requests")

    r.raise_for_status()
    text = r.text

    if "Our systems have detected unusual traffic" in text or "verify you are human" in text:
        print(f"  [WARN] Captcha triggered for query '{query}', sleeping 5–15 min...")
        time.sleep(random.uniform(300, 900))
        raise CaptchaError("YouTube captcha triggered")

    return text


def find_json_in_html(html_text: str, marker: str = "ytInitialData") -> Optional[Dict[str, Any]]:
    idx = html_text.find(marker)
    if idx == -1:
        m = regex.search(r"ytInitialData\W*[:=]\W*", html_text)
        if not m:
            return None
        idx = m.end()
    else:
        eq = html_text.find("=", idx)
        if eq != -1:
            idx = eq + 1
    start = html_text.find("{", idx)
    if start == -1:
        return None
    depth, i, L = 0, start, len(html_text)
    in_string, escape = False, False
    while i < L:
        ch = html_text[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == in_string:
                in_string = False
        else:
            if ch in ('"', "'"):
                in_string = ch
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    json_text = html_text[start:i+1]
                    try:
                        json_text_fixed = json_text.replace(r"\x3d", "=").replace(r"\x26", "&")
                        json_text_fixed = html.unescape(json_text_fixed)
                        return json.loads(json_text_fixed)
                    except Exception:
                        return None
        i += 1
    return None


def iter_video_renderers(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "videoRenderer" and isinstance(v, dict):
                yield v
            else:
                yield from iter_video_renderers(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from iter_video_renderers(item)


def safe_get_title_from_runs(runs):
    if not runs:
        return ""
    if isinstance(runs, str):
        return runs
    if isinstance(runs, list):
        return "".join([r.get("text", "") if isinstance(r, dict) else str(r) for r in runs])
    if isinstance(runs, dict):
        return runs.get("text", "")
    return str(runs)


def parse_viewcount_text(txt: str) -> Optional[int]:
    if not txt:
        return None
    txt_low = txt.lower().replace("\xa0", " ").strip()
    txt_low = re.sub(r"lượt xem|views|view|luợt xem", "", txt_low, flags=re.IGNORECASE).strip()
    if regex.search(r"(triệu|tr|m|million|mn)", txt_low):
        num = regex.search(r"[\d\.,]+", txt_low)
        if num:
            return int(float(num.group(0).replace(",", ".")) * 1_000_000)
    if regex.search(r"(nghìn|ng|k|n\b)", txt_low):
        num = regex.search(r"[\d\.,]+", txt_low)
        if num:
            return int(float(num.group(0).replace(",", ".")) * 1_000)
    digits = regex.search(r"[\d,\.]+", txt_low)
    if digits:
        return int(float(digits.group(0).replace(",", "")))
    return None


def detect_language(title: str) -> Optional[str]:
    if not title:
        return None
    if CJK_RE.search(title):
        return None
    if VIETNAMESE_DIACRITIC_RE.search(title):
        return "vi"
    non_latin = regex.search(r"[^\p{Latin}\p{N}\p{P}\s]", title)
    if non_latin:
        return None
    return "en"


# -----------------------
# Extract video metadata
# -----------------------
def extract_videos_from_html(html_text: str) -> List[Dict[str, Any]]:
    data = find_json_in_html(html_text, marker="ytInitialData")
    if not data:
        return []
    videos = []
    for vr in iter_video_renderers(data):
        vid = vr.get("videoId")
        if not vid:
            continue
        title = safe_get_title_from_runs(vr.get("title", {}).get("runs"))
        owner = vr.get("ownerText", {}).get("runs")
        channel_title, channel_id, channel_url = None, None, None
        if owner and isinstance(owner, list):
            channel_title = owner[0].get("text")
            nav = owner[0].get("navigationEndpoint", {})
            browse = nav.get("browseEndpoint", {}) if nav else {}
            channel_id = browse.get("browseId")
            if channel_id:
                channel_url = f"https://www.youtube.com/channel/{channel_id}"
        view_text = None
        vct = vr.get("viewCountText") or vr.get("shortViewCountText")
        if isinstance(vct, dict):
            view_text = vct.get("simpleText") or safe_get_title_from_runs(vct.get("runs"))
        vc = parse_viewcount_text(view_text)
        videos.append({
            "video_id": vid,
            "title": title,
            "channel_title": channel_title,
            "channel_id": channel_id,
            "channel_url": channel_url,
            "video_url": f"https://www.youtube.com/watch?v={vid}",
            "view_count": vc,
            "raw_view_text": view_text,
        })
    return videos


# -----------------------
# Fetch manga titles
# -----------------------
def fetch_manga_titles_from_db(limit: Optional[int] = None):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    cursor = db[MANGA_COLL].find({}, {"id": 1, "attributes.title": 1, "attributes.altTitles": 1})
    if limit:
        cursor = cursor.limit(limit)
    results = []
    for doc in cursor:
        manga_id = doc["id"]
        titles = []
        title_obj = doc.get("attributes", {}).get("title", {})
        for lang in ["en", "vi"]:
            if lang in title_obj and title_obj[lang]:
                titles.append(title_obj[lang].strip())
        alt_titles = doc.get("attributes", {}).get("altTitles", []) or []
        for alt in alt_titles:
            for lang in ["en", "vi"]:
                if isinstance(alt, dict) and lang in alt and alt[lang]:
                    titles.append(alt[lang].strip())
        seen, unique_titles = set(), []
        for t in titles:
            if not t:
                continue
            t2 = " ".join(t.split())
            if t2 not in seen:
                seen.add(t2)
                unique_titles.append(t2)
        results.append({"manga_id": manga_id, "titles": unique_titles})
    client.close()
    return results


# -----------------------
# Upsert
# -----------------------
def upsert_video(db, video_doc: Dict[str, Any], manga_id: str, query: str):
    col = db[OUT_COLL]
    now = datetime.utcnow().isoformat()
    vid = video_doc["video_id"]
    update = {
        "$setOnInsert": {
            "video_id": vid,
            "video_url": video_doc.get("video_url"),
            "channel_title": video_doc.get("channel_title"),
            "channel_id": video_doc.get("channel_id"),
            "channel_url": video_doc.get("channel_url"),
            "first_seen_at": now,
            "source": "youtube_search_html",
        },
        "$set": {
            "title": video_doc.get("title"),
            "view_count": video_doc.get("view_count"),
            "language": video_doc.get("language"),
            "last_seen_at": now,
            "raw_view_text": video_doc.get("raw_view_text"),
        },
        "$addToSet": {"manga_ids": manga_id, "queries": query},
    }
    col.update_one({"video_id": vid}, update, upsert=True)


# -----------------------
# Worker (per title)
# -----------------------
def process_title(session, db, manga_id: str, title: str):
    query = f"{title} manga"
    try:
        html_text = fetch_search_html(query, session=session)
    except Exception as e:
        print(f"  [ERROR] fetch failed {query}: {e}")
        return manga_id, 0
    videos = extract_videos_from_html(html_text)
    count = 0
    seen = set()
    for v in videos:
        vid, vc = v["video_id"], v.get("view_count") or 0
        if not vid or vid in seen:
            continue
        seen.add(vid)
        if vc < MIN_VIEWS:
            continue
        lang = detect_language(v.get("title") or "")
        if lang not in ("en", "vi"):
            continue
        v["language"] = lang
        try:
            upsert_video(db, v, manga_id, query)
            count += 1
        except DuplicateKeyError:
            db[OUT_COLL].update_one(
                {"video_id": vid},
                {"$addToSet": {"manga_ids": manga_id, "queries": query}},
            )
        except Exception as e:
            print(f"  [ERROR] upsert failed {vid}: {e}")
    return manga_id, count


# -----------------------
# Runner
# -----------------------
def run(limit: Optional[int] = None, workers: int = 5):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    db[OUT_COLL].create_index("video_id", unique=True)
    mangas = fetch_manga_titles_from_db(limit=limit)
    session = requests.Session()
    total_manga = len(mangas)
    print(f"[START] Processing {total_manga} manga (limit={limit}, workers={workers})")

    pending = {m["manga_id"]: len(m["titles"]) for m in mangas}
    manga_results = {m["manga_id"]: 0 for m in mangas}
    processed_manga = 0
    lock = threading.Lock()

    tasks = []
    for m in mangas:
        for t in m["titles"]:
            tasks.append((m["manga_id"], t))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(process_title, session, db, mid, t) for mid, t in tasks]
        for f in as_completed(futures):
            try:
                manga_id, inserted = f.result()
            except Exception as e:
                print(f"    [ERROR] thread failed: {e}")
                continue
            with lock:
                manga_results[manga_id] += inserted
                pending[manga_id] -= 1
                if pending[manga_id] == 0:
                    processed_manga += 1
                    print(f"[PROGRESS] Manga {processed_manga}/{total_manga} done, upserted {manga_results[manga_id]} videos (manga_id={manga_id})")

    print("[DONE] All manga processed.")
    client.close()


# -----------------------
# CLI
# -----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch YouTube videos for manga titles.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of manga to process")
    parser.add_argument("--workers", type=int, default=5, help="Number of threads")
    args = parser.parse_args()
    run(limit=args.limit, workers=args.workers)