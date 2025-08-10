import requests
import time
from pymongo import MongoClient
from datetime import datetime

# ===== MongoDB setup =====
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "manga_raw_data"
COL_MANGA = "mangadex_manga"
COL_TAGS = "mangadex_tags"
COL_STATS = "mangadex_statistics"
COL_CHAPS = "mangadex_chapters"
COL_COVERS = "mangadex_cover_art"
COL_CREATORS = "mangadex_creators"
COL_GROUPS = "mangadex_groups"

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]

# ===== API setup =====
BASE_URL = "https://api.mangadex.org"
MIN_DELAY = 0.25
delay = 0.0
MAX_RETRIES = 5
BURST_SUCCESS_THRESHOLD = 8
success_streak = 0

# ===== Utility =====
def request_api(endpoint, params=None):
    """Gọi API với cơ chế adaptive delay"""
    global delay, success_streak
    retries = 0
    while retries < MAX_RETRIES:
        try:
            resp = requests.get(BASE_URL + endpoint, params=params, timeout=30)
            if resp.status_code == 200:
                success_streak += 1
                if delay > 0:
                    delay = max(MIN_DELAY, delay * 0.9)
                    if success_streak >= BURST_SUCCESS_THRESHOLD:
                        delay = 0.0
                        print("🚀 Quay lại SPAM MODE")
                return resp.json()
            else:
                raise Exception(f"HTTP {resp.status_code}")
        except Exception as e:
            retries += 1
            success_streak = 0
            if delay == 0.0:
                delay = MIN_DELAY
            delay *= 2
            print(f"⚠️ Lỗi: {e}. Retry {retries}/{MAX_RETRIES}, delay={delay:.2f}s")
            time.sleep(delay)
    raise Exception(f"❌ Failed after {MAX_RETRIES} retries")

# ===== Fetch Tags =====
def fetch_tags():
    if db[COL_TAGS].count_documents({}) > 0:
        print("✅ Tags đã tồn tại, bỏ qua")
        return
    data = request_api("/manga/tag")
    tags = []
    for tag in data.get("data", []):
        tags.append({
            "_id": tag["id"],
            "attributes": tag.get("attributes", {})
        })
    if tags:
        db[COL_TAGS].insert_many(tags)
        print(f"✅ Đã lưu {len(tags)} tags")

# ===== Fetch Statistics =====
def fetch_statistics(manga_ids):
    batch_size = 100
    for i in range(0, len(manga_ids), batch_size):
        ids = [str(mid) for mid in manga_ids[i:i+batch_size]]  # chuyển thành string
        params = [("manga[]", mid) for mid in ids]
        data = request_api("/statistics/manga", params=params)
        
        stats_docs = []
        for mid, stat in data.get("statistics", {}).items():
            stats_docs.append({
                "_id": mid,
                "statistics": stat
            })
        
        if stats_docs:
            db[COL_STATS].insert_many(stats_docs, ordered=False)
        
        print(f"✅ Batch statistics {i//batch_size+1} saved ({len(stats_docs)} docs)")



# ===== Fetch Chapters =====
def fetch_chapters(manga_ids):
    for idx, mid in enumerate(manga_ids, 1):
        offset = 0
        while True:
            params = {"limit": 100, "offset": offset, "translatedLanguage[]": ["en", "vi"]}
            data = request_api(f"/chapter", params={**params, "manga": mid})
            chaps = data.get("data", [])
            if not chaps:
                break
            chap_docs = [{
                "_id": chap["id"],
                "mangaId": mid,
                "attributes": chap.get("attributes", {}),
                "relationships": chap.get("relationships", [])
            } for chap in chaps]
            db[COL_CHAPS].insert_many(chap_docs, ordered=False)
            offset += 100
        print(f"✅ Chapters saved for manga {idx}/{len(manga_ids)}")

# ===== Fetch Covers =====
def fetch_covers(manga_ids):
    for idx, mid in enumerate(manga_ids, 1):
        manga_doc = db[COL_MANGA].find_one({"_id": mid}, {"relationships": 1})
        if not manga_doc:
            continue
        for rel in manga_doc.get("relationships", []):
            if rel["type"] == "cover_art":
                cover_id = rel["id"]
                data = request_api(f"/cover/{cover_id}")
                cover_data = data.get("data", {})
                if cover_data:
                    db[COL_COVERS].update_one(
                        {"_id": cover_id},
                        {"$set": {
                            "mangaId": mid,
                            "attributes": cover_data.get("attributes", {}),
                            "relationships": cover_data.get("relationships", [])
                        }},
                        upsert=True
                    )
        print(f"✅ Covers saved for manga {idx}/{len(manga_ids)}")

# ===== Fetch Creators =====
def fetch_creators(manga_ids):
    seen = set()
    for idx, mid in enumerate(manga_ids, 1):
        manga_doc = db[COL_MANGA].find_one({"_id": mid}, {"relationships": 1})
        if not manga_doc:
            continue
        for rel in manga_doc.get("relationships", []):
            if rel["type"] in ["author", "artist"] and rel["id"] not in seen:
                seen.add(rel["id"])
                data = request_api(f"/author/{rel['id']}")
                creator_data = data.get("data", {})
                if creator_data:
                    db[COL_CREATORS].update_one(
                        {"_id": rel["id"]},
                        {"$set": creator_data},
                        upsert=True
                    )
        print(f"✅ Creators saved for manga {idx}/{len(manga_ids)}")

# ===== Fetch Groups =====
def fetch_groups(manga_ids):
    seen = set()
    for idx, mid in enumerate(manga_ids, 1):
        manga_doc = db[COL_MANGA].find_one({"_id": mid}, {"relationships": 1})
        if not manga_doc:
            continue
        for rel in manga_doc.get("relationships", []):
            if rel["type"] == "scanlation_group" and rel["id"] not in seen:
                seen.add(rel["id"])
                data = request_api(f"/group/{rel['id']}")
                group_data = data.get("data", {})
                if group_data:
                    db[COL_GROUPS].update_one(
                        {"_id": rel["id"]},
                        {"$set": group_data},
                        upsert=True
                    )
        print(f"✅ Groups saved for manga {idx}/{len(manga_ids)}")

# ===== Main =====
if __name__ == "__main__":
    manga_ids = [str(doc["_id"]) for doc in db[COL_MANGA].find({}, {"_id": 1})]

    fetch_tags()
    fetch_statistics(manga_ids)
    fetch_chapters(manga_ids)
    fetch_covers(manga_ids)
    fetch_creators(manga_ids)
    fetch_groups(manga_ids)

    print("🏁 Hoàn tất tải toàn bộ dữ liệu liên quan!")