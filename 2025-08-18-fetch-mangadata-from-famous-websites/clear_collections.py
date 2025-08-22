from pymongo import MongoClient

def clear_test_collections():
    client = MongoClient("mongodb://localhost:27017")
    db = client["manga_raw_data"]

    collections = [
        "anilist_data",
        "mal_data",
        "mangaupdates_data",
        "animeplanet_data",
    ]

    for col in collections:
        if col in db.list_collection_names():
            db[col].drop()
            print(f"✅ Dropped collection: {col}")
        else:
            print(f"⚠️ Collection not found, skipped: {col}")

    client.close()


if __name__ == "__main__":
    clear_test_collections()