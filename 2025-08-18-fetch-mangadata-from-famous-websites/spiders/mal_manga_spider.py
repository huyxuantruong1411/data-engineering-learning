import pymongo
import logging
from pathlib import Path
import scrapy
from src.extractors.mal_fetcher import get_full_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MongoDB connection
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "manga_raw_data"
COLLECTION_NAME = "mal_data"  # Đồng bộ với pipeline

def get_mongo_collection():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION_NAME]

# MAL Constants
MAL_BASE = "https://myanimelist.net"

class MALMangaSpider(scrapy.Spider):
    name = 'mal_manga_spider'
    custom_settings = {
        'DOWNLOAD_DELAY': 3,
        'CONCURRENT_REQUESTS': 1,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 429],
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'FEED_EXPORT_ENCODING': 'utf-8',
    }

    def __init__(self):
        self.collection = get_mongo_collection()
        self.temp_folder = Path('tmp/mal_manga_data')
        self.temp_folder.mkdir(parents=True, exist_ok=True)
        self.rank_increment = 50

    def start_requests(self):
        rank_url = f'{MAL_BASE}/topmanga.php?limit=0'
        yield scrapy.Request(url=rank_url, callback=self.parse_rank)

    def parse_rank(self, response):
        works = response.css('tr.ranking-list')
        info_urls = [work.css('a.hoverinfo_trigger::attr(href)').get() for work in works]

        for info_url in info_urls:
            if info_url:
                yield scrapy.Request(url=info_url, callback=self.parse_manga)

        current_limit = int(response.url.split('=')[-1])
        if len(works) == self.rank_increment:
            next_limit = current_limit + self.rank_increment
            next_url = f'{MAL_BASE}/topmanga.php?limit={next_limit}'
            yield scrapy.Request(url=next_url, callback=self.parse_rank)

    def parse_manga(self, response):
        mal_id = response.url.split('/')[-2]
        
        existing = self.collection.find_one({'_id': f'mal_{mal_id}'})
        if existing:
            logger.info(f'Skipping existing manga {mal_id}')
            return
        
        data = get_full_data(mal_id)
        if data and data.get('status') == 'ok':
            self.collection.insert_one(data)
            logger.info(f'Inserted manga {mal_id} into MongoDB')
        else:
            logger.warning(f'Failed to fetch data for manga {mal_id}')