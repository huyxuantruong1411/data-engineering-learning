# scripts/drop_collections.py
# simple script to drop the 4 collections used for testing
# Usage:
#   python scripts/drop_collections.py
# or set MONGO_URI / MONGO_DB env vars if not default

import os
import pymongo

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "manga_raw_data")

COLLECTIONS = ["animeplanet_data", "mangaupdates_data", "mal_data", "anilist_data"]

def main():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    for c in COLLECTIONS:
        if c in db.list_collection_names():
            db.drop_collection(c)
            print(f"Dropped collection: {c}")
        else:
            print(f"Collection not found (skipped): {c}")

if __name__ == "__main__":
    main()