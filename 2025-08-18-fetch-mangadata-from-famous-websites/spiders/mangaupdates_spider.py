# spiders/mangaupdates_spider.py
# Scrapy spider => collects comments (reviews) from MangaUpdates series page
# Example usage:
#   scrapy runspider spiders/mangaupdates_spider.py -a mu_url="https://www.mangaupdates.com/series/...#comments"

import os
import random
import logging
from datetime import datetime
from urllib.parse import urlparse

import pymongo
import scrapy
from scrapy.crawler import CrawlerProcess

logger = logging.getLogger("mangaupdates_spider")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "manga_raw_data")
COLLECTION = "mangaupdates_data"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
]

def mongo_client():
    return pymongo.MongoClient(MONGO_URI)


class MangaUpdatesSpider(scrapy.Spider):
    name = "mangaupdates_spider"
    custom_settings = {
        "DOWNLOAD_DELAY": 2.0,
        "CONCURRENT_REQUESTS": 2,
        "RETRY_ENABLED": False,
        "COOKIES_ENABLED": True,
    }

    def __init__(self, mu_id=None, mu_url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not (mu_id or mu_url):
            raise ValueError("Provide mu_id or mu_url to spider")
        self.mu_id = str(mu_id) if mu_id else None
        self.mu_url = mu_url
        self.client = mongo_client()
        self.db = self.client[MONGO_DB]
        self.col = self.db[COLLECTION]
        if self.mu_id:
            self.doc_id = f"mu_{self.mu_id}"
        else:
            path = urlparse(self.mu_url).path.strip("/").replace("/", "_")
            self.doc_id = f"mu_{path}"
        if self.mu_url:
            self.start_urls = [self.mu_url]
        else:
            self.start_urls = []

    def start_requests(self):
        if not self.start_urls:
            logger.error("No start_urls for MangaUpdatesSpider. Provide mu_url.")
            return
        for url in self.start_urls:
            headers = {"User-Agent": random.choice(USER_AGENTS), "Accept-Language": "en-US,en;q=0.9"}
            parsed = url.split("?")[0]
            base_with_comments = parsed + "?perpage=100&page=1#comments"
            yield scrapy.Request(base_with_comments, headers=headers, callback=self.parse_comments, meta={"page": 1, "base": parsed}, dont_filter=True)

    def parse_comments(self, response):
        if response.status == 403:
            logger.warning("403 on MangaUpdates %s", response.url)
            doc = {
                "_id": self.doc_id,
                "fetched_at": datetime.utcnow().isoformat(),
                "source": "mangaupdates",
                "source_url": self.start_urls[0] if self.start_urls else None,
                "http": {"code": 403},
                "status": "forbidden",
                "raw_prefix": response.text[:20000] if hasattr(response, "text") else None,
            }
            self.col.replace_one({"_id": self.doc_id}, doc, upsert=True)
            return

        page = response.meta.get("page", 1)
        base = response.meta.get("base", response.url.split("?")[0])
        comments = []

        # Try multiple selectors (existing heuristics kept)
        comment_nodes = response.xpath("//div[@id='comments']//div[contains(@class,'postbody')] | //div[@id='comments']//li[contains(@class,'comment') or contains(@class,'post')]")
        if not comment_nodes:
            comment_nodes = response.xpath("//div[contains(@class,'post') and contains(@class,'postbody')] | //li[contains(@class,'comment')]")

        for node in comment_nodes:
            user = node.xpath(".//a[contains(@href,'member.php')]/text() | .//a[contains(@href,'members')]/text() | .//span[@class='username']//text()").get()
            content_parts = node.xpath(".//div[contains(@class,'postbody')]//text() | .//p//text()").getall()
            content = " ".join([c.strip() for c in content_parts]).strip()
            date = node.xpath(".//span[contains(@class,'date')]/text() | .//div[contains(@class,'postdate')]//text() | .//abbr[@class='published']/@title").get()
            comments.append({"user": user.strip() if user else None, "content": content, "date": date})

        existing = self.col.find_one({"_id": self.doc_id}) or {}
        all_comments = existing.get("comments", [])
        existing_texts = {c.get("content") for c in all_comments}
        new_added = 0
        for c in comments:
            if c.get("content") and c.get("content") not in existing_texts:
                all_comments.append(c)
                new_added += 1

        doc = {
            "_id": self.doc_id,
            "fetched_at": datetime.utcnow().isoformat(),
            "source": "mangaupdates",
            "source_url": base,
            "page_last_fetched": page,
            "comments": all_comments,
            "status": "ok" if all_comments else "no_comments",
            "raw_prefix": response.text[:20000] if hasattr(response, "text") else None,
        }
        self.col.replace_one({"_id": self.doc_id}, doc, upsert=True)
        logger.info("[SAVED] %s %s | page=%d comments_page=%d total=%d", COLLECTION, self.doc_id, page, len(comments), len(all_comments))

        # determine next page
        next_page = page + 1
        next_url = f"{base}?perpage=100&page={next_page}#comments"
        if comments and len(comments) >= 100:
            yield scrapy.Request(next_url, headers={"User-Agent": random.choice(USER_AGENTS)}, callback=self.parse_comments, meta={"page": next_page, "base": base}, dont_filter=True)
        else:
            logger.info("No more comment pages or last page detected for %s", base)


if __name__ == "__main__":
    mu_url = os.environ.get("MU_URL")
    if not mu_url:
        print("Set MU_URL environment variable to a mangaupdates series page URL to test directly.")
    else:
        process = CrawlerProcess(settings={})
        process.crawl(MangaUpdatesSpider, mu_url=mu_url)
        process.start()
