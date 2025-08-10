import time
import requests
import argparse
import json
import os
import sys
from datetime import datetime
from pymongo import MongoClient
from typing import List, Dict, Any, Optional
import logging

# ===== CONFIG =====
BASE_URL = "https://api.mangadex.org"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "manga_raw_data"
MANGA_COLLECTION = "mangadex_manga"  # Main fact table collection

# Collection names for related data
COLLECTIONS = {
    'tags': 'mangadex_tags',
    'statistics': 'mangadex_statistics', 
    'chapters': 'mangadex_chapters',
    'cover_arts': 'mangadex_cover_arts',
    'creators': 'mangadex_creators',
    'groups': 'mangadex_groups',
    'related': 'mangadex_related'
}

# API Configuration
MIN_DELAY = 0.1
MAX_DELAY = 5.0
MAX_RETRIES = 5
BURST_SUCCESS_THRESHOLD = 10
INITIAL_BATCH_SIZE = 100
MIN_BATCH_SIZE = 10

# Progress tracking
PROGRESS_FILE = "manga_progress.json"

# ===== Logging Setup =====
# Fix Unicode encoding issues for Windows
if sys.platform == "win32":
    # Use simple text format for Windows
    logging.basicConfig(
        level=logging.DEBUG,  # Changed to DEBUG for more details
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('mangadex_fetch.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
else:
    logging.basicConfig(
        level=logging.DEBUG,  # Changed to DEBUG for more details
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('mangadex_fetch.log'),
            logging.StreamHandler()
        ]
    )

logger = logging.getLogger(__name__)

# ===== MongoDB Setup =====
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]

class MangaDataFetcher:
    def __init__(self):
        self.delay = 0.0
        self.success_streak = 0
        self.batch_size = INITIAL_BATCH_SIZE
        self.progress = self.load_progress()
        self.ensure_collections()
        
    def ensure_collections(self):
        """Ensure all required collections exist."""
        for collection_name in COLLECTIONS.values():
            if collection_name not in db.list_collection_names():
                db.create_collection(collection_name)
                logger.info(f"[OK] Created collection: {collection_name}")
                
    def load_progress(self) -> Dict[str, Any]:
        """Load progress from file if exists."""
        if os.path.exists(PROGRESS_FILE):
            try:
                with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    progress_data = json.load(f)
                    # Ensure all required keys exist
                    default_progress = {
                        'tags': {'completed': False, 'last_processed': None},
                        'statistics': {'completed': False, 'last_processed': None, 'batch_size': INITIAL_BATCH_SIZE},
                        'chapters': {'completed': False, 'last_processed': None},
                        'cover_arts': {'completed': False, 'last_processed': None},
                        'creators': {'completed': False, 'last_processed': None},
                        'groups': {'completed': False, 'last_processed': None},
                        'related': {'completed': False, 'last_processed': None}
                    }
                    
                    # Merge existing progress with default structure
                    for key in default_progress:
                        if key not in progress_data:
                            progress_data[key] = default_progress[key]
                        else:
                            # Ensure all sub-keys exist
                            for sub_key in default_progress[key]:
                                if sub_key not in progress_data[key]:
                                    progress_data[key][sub_key] = default_progress[key][sub_key]
                    
                    return progress_data
            except Exception as e:
                logger.warning(f"Could not load progress file: {e}. Starting fresh.")
                
        return {
            'tags': {'completed': False, 'last_processed': None},
            'statistics': {'completed': False, 'last_processed': None, 'batch_size': INITIAL_BATCH_SIZE},
            'chapters': {'completed': False, 'last_processed': None},
            'cover_arts': {'completed': False, 'last_processed': None},
            'creators': {'completed': False, 'last_processed': None},
            'groups': {'completed': False, 'last_processed': None},
            'related': {'completed': False, 'last_processed': None}
        }
    
    def save_progress(self):
        """Save current progress to file."""
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.progress, f, indent=2, ensure_ascii=False)
    
    def request_api(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make API request with intelligent retry and adaptive delay."""
        retries = 0
        while retries < MAX_RETRIES:
            try:
                response = requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=30)
                
                if response.status_code == 200:
                    self.success_streak += 1
                    if self.delay > 0:
                        self.delay = max(MIN_DELAY, self.delay * 0.9)
                        if self.success_streak >= BURST_SUCCESS_THRESHOLD:
                            self.delay = 0.0
                            logger.info("[SPEED] Back to SPAM mode")
                    return response.json()
                    
                elif response.status_code == 400:
                    logger.warning(f"[WARN] API returned 400 for endpoint {endpoint}")
                    if "statistics" in endpoint:
                        # Reduce batch size for statistics API
                        self.batch_size = max(MIN_BATCH_SIZE, self.batch_size // 2)
                        logger.info(f"[REDUCE] Reduced batch size to {self.batch_size}")
                    return None
                    
                elif response.status_code == 429:  # Rate limit
                    self.success_streak = 0
                    self.delay = min(MAX_DELAY, self.delay * 2 if self.delay > 0 else MIN_DELAY)
                    logger.warning(f"[RATE_LIMIT] Rate limited. Increasing delay to {self.delay:.2f}s")
                    time.sleep(self.delay)
                    retries += 1
                    continue
                    
                else:
                    raise Exception(f"HTTP {response.status_code}")
                    
            except Exception as e:
                self.success_streak = 0
                if self.delay == 0.0:
                    self.delay = MIN_DELAY
                self.delay = min(MAX_DELAY, self.delay * 2)
                retries += 1
                logger.warning(f"[ERROR] {e}. Retry {retries}/{MAX_RETRIES}, delay={self.delay:.2f}s")
                time.sleep(self.delay)
        
        raise Exception(f"[FAILED] Failed after {MAX_RETRIES} retries")
    
    def get_manga_ids(self) -> List[str]:
        """Get all manga IDs from the main collection."""
        try:
            # Get all documents from the manga collection
            all_manga = list(db[MANGA_COLLECTION].find({}, {"id": 1}))
            logger.info(f"[DEBUG] Raw manga documents found: {len(all_manga)}")
            
            if not all_manga:
                raise Exception("No manga found in database")
            
            # Extract and validate manga IDs - use 'id' field instead of '_id'
            manga_ids = []
            invalid_count = 0
            
            for doc in all_manga:
                manga_id = doc.get("id")  # Changed from "_id" to "id"
                if manga_id and isinstance(manga_id, str) and manga_id.strip():
                    manga_ids.append(manga_id)
                else:
                    invalid_count += 1
                    logger.debug(f"[DEBUG] Invalid manga ID: {manga_id} (type: {type(manga_id)})")
            
            logger.info(f"[INFO] Found {len(manga_ids)} valid manga IDs in database")
            
            if invalid_count > 0:
                logger.warning(f"[WARN] {invalid_count} invalid manga IDs found and skipped")
            
            if not manga_ids:
                raise Exception("No valid manga IDs found after validation")
                
            # Show first few IDs for debugging
            logger.info(f"[DEBUG] First 5 manga IDs: {manga_ids[:5]}")
                
            return manga_ids
            
        except Exception as e:
            logger.error(f"[ERROR] Error accessing database: {e}")
            raise
    
    def fetch_tags(self):
        """Fetch all tags."""
        if self.progress['tags']['completed']:
            # Double check if data actually exists
            if db[COLLECTIONS['tags']].count_documents({}) > 0:
                logger.info("[SKIP] Tags already completed, skipping")
                return
            else:
                logger.info("[RESET] Progress says completed but no data found, resetting...")
                self.progress['tags']['completed'] = False
            
        if db[COLLECTIONS['tags']].count_documents({}) > 0:
            logger.info("[SKIP] Tags already exist in database, skipping")
            self.progress['tags']['completed'] = True
            self.save_progress()
            return
            
        logger.info("[START] Fetching all tags...")
        data = self.request_api("/manga/tag")
        if not data:
            logger.error("[ERROR] Could not fetch tags")
            return
            
        tags = [
            {
                "_id": t["id"],
                "attributes": t.get("attributes", {}),
                "fetched_at": datetime.now().isoformat()
            }
            for t in data.get("data", [])
        ]
        
        if tags:
            db[COLLECTIONS['tags']].insert_many(tags)
            logger.info(f"[SUCCESS] Saved {len(tags)} tags")
            self.progress['tags']['completed'] = True
            self.save_progress()
    
    def fetch_statistics(self, manga_ids: List[str]):
        """Fetch statistics for manga in batches with adaptive sizing."""
        if self.progress['statistics']['completed']:
            # Double check if data actually exists
            if db[COLLECTIONS['statistics']].count_documents({}) > 0:
                logger.info("[SKIP] Statistics already completed, skipping")
                return
            else:
                logger.info("[RESET] Progress says completed but no data found, resetting...")
                self.progress['statistics']['completed'] = False
            
        # Restore batch size from progress
        if self.progress['statistics']['batch_size']:
            self.batch_size = self.progress['statistics']['batch_size']
            
        logger.info(f"[START] Fetching statistics for {len(manga_ids)} manga in batches of {self.batch_size}")
        
        # Find starting point
        start_idx = 0
        if self.progress['statistics']['last_processed']:
            try:
                start_idx = manga_ids.index(self.progress['statistics']['last_processed'])
                start_idx += 1  # Start from next item
                logger.info(f"[RESUME] Resuming statistics from index {start_idx}")
            except ValueError:
                start_idx = 0
        
        for i in range(start_idx, len(manga_ids), self.batch_size):
            batch_ids = manga_ids[i:i+self.batch_size]
            batch_num = i//self.batch_size + 1
            total_batches = (len(manga_ids) + self.batch_size - 1)//self.batch_size
            
            logger.info(f"\n--- Processing batch {batch_num}/{total_batches} ---")
            logger.info(f"   Manga IDs: {batch_ids[:5]}{'...' if len(batch_ids) > 5 else ''}")
            
            # Check existing statistics
            existing_stats = db[COLLECTIONS['statistics']].find(
                {"mangaId": {"$in": batch_ids}}, 
                {"mangaId": 1}
            )
            existing_manga_ids = set(doc["mangaId"] for doc in existing_stats)
            ids_to_fetch = [mid for mid in batch_ids if mid not in existing_manga_ids]
            
            if not ids_to_fetch:
                logger.info(f"   [SKIP] All statistics already exist for this batch, skipping")
                continue
            
            logger.info(f"   [FETCH] Fetching statistics for {len(ids_to_fetch)} manga...")
            
            # Use correct parameter format for statistics API
            params = {"manga[]": ids_to_fetch}
            
            try:
                data = self.request_api("/statistics/manga", params=params)
                if not data:
                    logger.warning(f"   [WARN] Batch failed: API returned no data, skipping")
                    continue
                    
                stats_data = data.get("statistics", {})
                if not stats_data:
                    logger.warning(f"   [WARN] No statistics data in response")
                    continue
                    
                # Insert statistics
                inserted_count = 0
                for mid, stat in stats_data.items():
                    try:
                        stats_doc = {
                            "_id": f"{mid}_{int(time.time())}",
                            "mangaId": mid,
                            "source": "mangadex",
                            "statistics": stat,
                            "snapshotTime": time.time(),
                            "fetched_at": datetime.now().isoformat()
                        }
                        db[COLLECTIONS['statistics']].insert_one(stats_doc)
                        inserted_count += 1
                    except Exception as e:
                        logger.error(f"     [ERROR] Failed to insert stats for {mid}: {e}")
                        
                logger.info(f"   [SUCCESS] Saved statistics for {inserted_count}/{len(stats_data)} manga")
                
                # Update progress
                self.progress['statistics']['last_processed'] = batch_ids[-1]
                self.progress['statistics']['batch_size'] = self.batch_size
                self.save_progress()
                
            except Exception as e:
                logger.error(f"   [ERROR] Batch failed: {e}")
                # Continue with next batch instead of stopping
            
            # Adaptive delay
            if self.delay > 0:
                logger.info(f"   [WAIT] Waiting {self.delay:.2f}s...")
                time.sleep(self.delay)
            else:
                time.sleep(0.1)  # Minimum delay
        
        self.progress['statistics']['completed'] = True
        self.save_progress()
        logger.info("[SUCCESS] Statistics fetching completed")
    
    def fetch_chapters(self, manga_ids: List[str]):
        """Fetch all chapter metadata."""
        if self.progress['chapters']['completed']:
            # Double check if data actually exists
            if db[COLLECTIONS['chapters']].count_documents({}) > 0:
                logger.info("[SKIP] Chapters already completed, skipping")
                return
            else:
                logger.info("[RESET] Progress says completed but no data found, resetting...")
                self.progress['chapters']['completed'] = False
            
        logger.info(f"[START] Fetching chapters for {len(manga_ids)} manga...")
        
        # Find starting point
        start_idx = 0
        if self.progress['chapters']['last_processed']:
            try:
                start_idx = manga_ids.index(self.progress['chapters']['last_processed'])
                start_idx += 1
                logger.info(f"[RESET] Resuming chapters from index {start_idx}")
            except ValueError:
                start_idx = 0
        
        for i, mid in enumerate(manga_ids[start_idx:], start_idx + 1):
            if db[COLLECTIONS['chapters']].count_documents({"mangaId": mid}) > 0:
                continue
                
            all_chapters = []
            offset = 0
            
            while True:
                params = {
                    "manga": mid,
                    "limit": 100,
                    "offset": offset,
                    "order[chapter]": "asc"
                }
                
                data = self.request_api("/chapter", params)
                if not data:
                    logger.warning(f"[WARN] Could not fetch chapters for manga {mid}")
                    break
                    
                chapters = data.get("data", [])
                if not chapters:
                    break
                    
                for c in chapters:
                    c["_id"] = c["id"]
                    c["mangaId"] = mid
                    c["fetched_at"] = datetime.now().isoformat()
                    all_chapters.append(c)
                    
                offset += 100
                if self.delay > 0:
                    time.sleep(self.delay)
                    
            if all_chapters:
                db[COLLECTIONS['chapters']].insert_many(all_chapters)
                
            logger.info(f"[{i}/{len(manga_ids)}] [SUCCESS] {len(all_chapters)} chapters saved for {mid}")
            
            # Update progress
            self.progress['chapters']['last_processed'] = mid
            self.save_progress()
            
            if self.delay > 0:
                time.sleep(self.delay)
                
        self.progress['chapters']['completed'] = True
        self.save_progress()
        logger.info("[SUCCESS] Chapters fetching completed")
    
    def fetch_related(self, manga_ids: List[str]):
        """Fetch related manga information."""
        if self.progress['related']['completed']:
            # Double check if data actually exists
            if db[COLLECTIONS['related']].count_documents({}) > 0:
                logger.info("[SKIP] Related manga already completed, skipping")
                return
            else:
                logger.info("[RESET] Progress says completed but no data found, resetting...")
                self.progress['related']['completed'] = False
            
        logger.info(f"[START] Fetching related manga for {len(manga_ids)} manga...")
        
        # Find starting point
        start_idx = 0
        if self.progress['related']['last_processed']:
            try:
                start_idx = manga_ids.index(self.progress['related']['last_processed'])
                start_idx += 1
                logger.info(f"[RESET] Resuming related manga from index {start_idx}")
            except ValueError:
                start_idx = 0
        
        for i, mid in enumerate(manga_ids[start_idx:], start_idx + 1):
            if db[COLLECTIONS['related']].count_documents({"_id": mid}) > 0:
                continue
                
            data = self.request_api(f"/manga/{mid}")
            if not data:
                logger.warning(f"[WARN] Could not fetch manga {mid}")
                continue
                
            relationships = data.get("data", {}).get("relationships", [])
            db[COLLECTIONS['related']].insert_one({
                "_id": mid,
                "relationships": relationships,
                "fetched_at": datetime.now().isoformat()
            })
            
            logger.info(f"[{i}/{len(manga_ids)}] [SUCCESS] Related saved for {mid}")
            
            # Update progress
            self.progress['related']['last_processed'] = mid
            self.save_progress()
            
            if self.delay > 0:
                time.sleep(self.delay)
                
        self.progress['related']['completed'] = True
        self.save_progress()
        logger.info("[SUCCESS] Related manga fetching completed")
    
    def fetch_covers_creators_groups(self, manga_ids: List[str]):
        """Fetch cover art, creators, and scanlation groups using existing relationships."""
        if (self.progress['cover_arts']['completed'] and 
            self.progress['creators']['completed'] and 
            self.progress['groups']['completed']):
            # Double check if data actually exists
            covers_exist = db[COLLECTIONS['cover_arts']].count_documents({}) > 0
            creators_exist = db[COLLECTIONS['creators']].count_documents({}) > 0
            groups_exist = db[COLLECTIONS['groups']].count_documents({}) > 0
            
            if covers_exist and creators_exist and groups_exist:
                logger.info("[SKIP] Covers/Creators/Groups already completed, skipping")
                return
            else:
                logger.info("[RESET] Progress says completed but some data missing, resetting...")
                if not covers_exist:
                    self.progress['cover_arts']['completed'] = False
                if not creators_exist:
                    self.progress['creators']['completed'] = False
                if not groups_exist:
                    self.progress['groups']['completed'] = False
            
        logger.info(f"[START] Fetching covers, creators, and groups for {len(manga_ids)} manga...")
        
        # Collect all unique IDs from relationships
        all_cover_ids = set()
        all_creator_ids = set()
        all_group_ids = set()
        
        # First pass: collect all IDs from relationships
        logger.info("[COLLECT] Collecting IDs from existing relationships...")
        for mid in manga_ids:
            manga_doc = db[MANGA_COLLECTION].find_one({"_id": mid}, {"relationships": 1})
            if not manga_doc or "relationships" not in manga_doc:
                continue
                
            for rel in manga_doc["relationships"]:
                rid = rel.get("id")
                if not rid:
                    continue
                    
                if rel["type"] == "cover_art":
                    all_cover_ids.add(rid)
                elif rel["type"] in ["author", "artist"]:
                    all_creator_ids.add(rid)
                elif rel["type"] == "scanlation_group":
                    all_group_ids.add(rid)
        
        logger.info(f"[INFO] Found {len(all_cover_ids)} cover IDs, {len(all_creator_ids)} creator IDs, {len(all_group_ids)} group IDs")
        
        # Fetch cover arts
        if not self.progress['cover_arts']['completed']:
            self._fetch_covers(list(all_cover_ids))
            
        # Fetch creators
        if not self.progress['creators']['completed']:
            self._fetch_creators(list(all_creator_ids))
            
        # Fetch groups
        if not self.progress['groups']['completed']:
            self._fetch_groups(list(all_group_ids))
    
    def _fetch_covers(self, cover_ids: List[str]):
        """Fetch cover art data."""
        logger.info(f"[START] Fetching {len(cover_ids)} cover arts...")
        
        for i, rid in enumerate(cover_ids, 1):
            if db[COLLECTIONS['cover_arts']].count_documents({"_id": rid}) > 0:
                continue
                
            cover = self.request_api(f"/cover/{rid}")
            if cover:
                cover["_id"] = rid
                cover["fetched_at"] = datetime.now().isoformat()
                db[COLLECTIONS['cover_arts']].insert_one(cover)
                
            if i % 100 == 0:
                logger.info(f"[PROGRESS] {i}/{len(cover_ids)} covers processed")
                
            if self.delay > 0:
                time.sleep(self.delay)
                
        self.progress['cover_arts']['completed'] = True
        self.save_progress()
        logger.info("[SUCCESS] Cover arts fetching completed")
    
    def _fetch_creators(self, creator_ids: List[str]):
        """Fetch creator data."""
        logger.info(f"[START] Fetching {len(creator_ids)} creators...")
        
        for i, rid in enumerate(creator_ids, 1):
            if db[COLLECTIONS['creators']].count_documents({"_id": rid}) > 0:
                continue
                
            creator = self.request_api(f"/author/{rid}")
            if creator:
                creator["_id"] = rid
                creator["fetched_at"] = datetime.now().isoformat()
                db[COLLECTIONS['creators']].insert_one(creator)
                
            if i % 100 == 0:
                logger.info(f"[PROGRESS] {i}/{len(creator_ids)} creators processed")
                
            if self.delay > 0:
                time.sleep(self.delay)
                
        self.progress['creators']['completed'] = True
        self.save_progress()
        logger.info("[SUCCESS] Creators fetching completed")
    
    def _fetch_groups(self, group_ids: List[str]):
        """Fetch scanlation group data."""
        logger.info(f"[START] Fetching {len(group_ids)} scanlation groups...")
        
        for i, rid in enumerate(group_ids, 1):
            if db[COLLECTIONS['groups']].count_documents({"_id": rid}) > 0:
                continue
                
            group = self.request_api(f"/group/{rid}")
            if group:
                group["_id"] = rid
                group["fetched_at"] = datetime.now().isoformat()
                db[COLLECTIONS['groups']].insert_one(group)
                
            if i % 100 == 0:
                logger.info(f"[PROGRESS] {i}/{len(group_ids)} groups processed")
                
            if self.delay > 0:
                time.sleep(self.delay)
                
        self.progress['groups']['completed'] = True
        self.save_progress()
        logger.info("[SUCCESS] Groups fetching completed")
    
    def run(self, phase: str = "all"):
        """Run the data fetching process."""
        try:
            manga_ids = self.get_manga_ids()
            
            logger.info(f"[START] Starting {phase} phase...")
            
            if phase in ["all", "tags"]:
                self.fetch_tags()
            if phase in ["all", "statistics"]:
                self.fetch_statistics(manga_ids)
            if phase in ["all", "chapters"]:
                self.fetch_chapters(manga_ids)
            if phase in ["all", "related"]:
                self.fetch_related(manga_ids)
            if phase in ["all", "covers", "creators", "groups"]:
                self.fetch_covers_creators_groups(manga_ids)
                
            logger.info("[SUCCESS] All tasks completed successfully!")
            
        except KeyboardInterrupt:
            logger.info("[PAUSE] Process interrupted by user. Progress saved.")
        except Exception as e:
            logger.error(f"[ERROR] Fatal error: {e}")
            raise
        finally:
            self.save_progress()

def main():
    parser = argparse.ArgumentParser(description="Fetch all related manga data from MangaDex API")
    parser.add_argument("--phase", type=str, default="all", 
                       help="Phase to run: all|tags|statistics|chapters|covers|creators|groups|related")
    parser.add_argument("--reset-progress", action="store_true", 
                       help="Reset progress and start from beginning")
    
    args = parser.parse_args()
    
    if args.reset_progress and os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        logger.info("[RESET] Progress reset. Starting fresh...")
    
    fetcher = MangaDataFetcher()
    fetcher.run(args.phase)

if __name__ == "__main__":
    main() 