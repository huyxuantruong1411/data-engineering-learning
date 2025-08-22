from pymongo import MongoClient
from .config import MONGO_URI, MONGO_DB, MONGODEX_COLLECTION

def get_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]

def iter_mangadex_docs(limit=None):
    db = get_db()
    proj = {
        "_id": 1,
        "id": 1,
        "attributes.title": 1,
        "attributes.links": 1
    }
    cur = db[MONGODEX_COLLECTION].find({"attributes.links": {"$exists": True}}, proj)
    if limit:
        cur = cur.limit(int(limit))
    for doc in cur:
        yield doc