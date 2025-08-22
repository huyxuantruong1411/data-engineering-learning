import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "manga_raw_data")

WORKERS = int(os.getenv("WORKERS", "6"))

AP_MAX_PAGES_REVIEWS = int(os.getenv("AP_MAX_PAGES_REVIEWS", "2"))
AP_MAX_PAGES_RECS = int(os.getenv("AP_MAX_PAGES_RECS", "2"))
MU_MAX_PAGES_COMMENTS = int(os.getenv("MU_MAX_PAGES_COMMENTS", "2"))

MIN_HOST_INTERVAL = float(os.getenv("MIN_HOST_INTERVAL", "0.8"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))

# User-Agent chuẩn tử tế để tránh bị chặn sớm
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)