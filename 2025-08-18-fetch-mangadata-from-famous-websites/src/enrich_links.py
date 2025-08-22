from pymongo import MongoClient
from tqdm import tqdm

def enrich_links():
    client = MongoClient("mongodb://localhost:27017")
    db = client["manga_raw_data"]
    col = db["mangadex_manga"]

    cursor = col.find({}, no_cursor_timeout=True)

    for doc in tqdm(cursor, desc="Enriching"):
        links = doc.get("links") or {}
        updates = {}
        if "al" in links:
            updates["anilist_id"] = links["al"]
        if "ap" in links:
            updates["ap_slug"] = links["ap"]
        if "mu" in links:
            updates["mu_id"] = links["mu"]
        if "mal" in links:
            updates["mal_id"] = links["mal"]

        if updates:
            col.update_one({"_id": doc["_id"]}, {"$set": updates})

    cursor.close()
    client.close()

if __name__ == "__main__":
    enrich_links()
    print("✅ Done enriching links")