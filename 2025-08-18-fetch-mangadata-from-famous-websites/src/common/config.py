import os
from dotenv import load_dotenv

load_dotenv()

# Database settings
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "manga_raw_data")
MONGODX_COLLECTION = os.getenv("MONGODX_COLLECTION", "mangadx_manga")

# API endpoints
ANI_API_ENDPOINT = os.getenv("ANI_API_ENDPOINT", "https://graphql.anilist.co")
MAL_CLIENT_ID = os.getenv("MAL_CLIENT_ID", "6114d00ca681b7701d1e15fe11a4987e")  # Default public client ID
MU_API_BASE = os.getenv("MU_API_BASE", "https://api.mangaupdates.com/v1")

# Proxy settings: Load from proxy_ip.txt
PROXY_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../proxy_ip.txt"))
HTTP_PROXY = []

try:
    if os.path.exists(PROXY_FILE):
        with open(PROXY_FILE, "r", encoding="utf-8") as f:
            for line in f:
                proxy = line.strip()
                if proxy:
                    # Ensure proxy has http:// prefix
                    if not proxy.startswith(("http://", "https://")):
                        proxy = f"http://{proxy}"
                    HTTP_PROXY.append(proxy)
    else:
        print(f"Warning: Proxy file {PROXY_FILE} not found, no proxies will be used.")
except Exception as e:
    print(f"Error reading proxy file {PROXY_FILE}: {e}")

# Fallback proxies if file is empty or fails
if not HTTP_PROXY:
    HTTP_PROXY = [
        "http://45.79.139.169:80",
        "http://104.236.195.251:80",
        "http://159.65.0.132:80",
    ]

HTTPS_PROXY = os.getenv("HTTPS_PROXY", None)

# Data lake path
DATA_LAKE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data-lake/raw"))

# Anti-blocking configuration
ANTI_BLOCKING_CONFIG = {
    # Base delays (seconds)
    "MIN_DELAY": float(os.getenv("MIN_DELAY", "10")),
    "MAX_DELAY": float(os.getenv("MAX_DELAY", "30")),
    "REVIEW_DELAY_MIN": float(os.getenv("REVIEW_DELAY_MIN", "8")),
    "REVIEW_DELAY_MAX": float(os.getenv("REVIEW_DELAY_MAX", "20")),
    
    # Retry configuration
    "MAX_RETRIES": int(os.getenv("MAX_RETRIES", "5")),
    "BACKOFF_MULTIPLIER": float(os.getenv("BACKOFF_MULTIPLIER", "2.0")),
    "BACKOFF_MAX": float(os.getenv("BACKOFF_MAX", "300")),  # 5 minutes max
    
    # Request timeout
    "REQUEST_TIMEOUT": float(os.getenv("REQUEST_TIMEOUT", "30")),
    
    # Use proxies (enabled since we have proxy_ip.txt)
    "USE_PROXIES": os.getenv("USE_PROXIES", "true").lower() == "true",
    
    # Conservative mode (longer delays, fewer retries)
    "CONSERVATIVE_MODE": os.getenv("CONSERVATIVE_MODE", "true").lower() == "true",
}