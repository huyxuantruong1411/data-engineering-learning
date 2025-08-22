# src/spiders/settings.py

# Cấu hình mặc định cho tất cả spiders

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/128.0.0.0 Safari/537.36"
)

DEFAULT_HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

SPIDER_SETTINGS = {
    "COOKIES_ENABLED": True,
    "DOWNLOAD_DELAY": 1.5,
    "DEFAULT_REQUEST_HEADERS": DEFAULT_HEADERS,
}

# Scrapy settings for manga scraping project

BOT_NAME = 'manga_scraper'

SPIDER_MODULES = ['spiders']
NEWSPIDER_MODULE = 'spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests
CONCURRENT_REQUESTS = 128
CONCURRENT_REQUESTS_PER_DOMAIN = 64
DOWNLOAD_DELAY = 0.1

# Enable autothrottle
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0.1
AUTOTHROTTLE_MAX_DELAY = 5.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 8.0

# Retry settings
RETRY_HTTP_CODES = [429, 500, 502, 503, 504]
RETRY_TIMES = 10

# Disable cookies
COOKIES_ENABLED = False

# Enable User-Agent rotation
DOWNLOADER_MIDDLEWARES = {
    'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 500,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 350,
}

# Proxy settings (uncomment and configure with your proxy service)
# PROXY_POOL_ENABLED = True
# PROXY_POOL = [
#     'http://proxy1:port',
#     'http://proxy2:port',
# ]

# Default headers
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# Logging
LOG_ENABLED = True
LOG_LEVEL = 'INFO'

# Export encoding
FEED_EXPORT_ENCODING = 'utf-8'