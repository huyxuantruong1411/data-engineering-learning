from pymongo import MongoClient
import os

# Mongo connection string (mặc định localhost:27017, database = manga_raw_data)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

# Tạo client global (chỉ cần 1 kết nối cho toàn project)
_client = MongoClient(MONGO_URI)


def get_db(db_name: str):
    """Trả về database object."""
    return _client[db_name]


def get_collection(db_name: str, col_name: str):
    """Trả về collection object."""
    db = get_db(db_name)
    return db[col_name]