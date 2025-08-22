# spiders/animeplanet_spider.py
# Scrapy spider => collects details, reviews, recommendations from anime-planet manga page
# Usage:
#   scrapy runspider spiders/animeplanet_spider.py -a slug=tower-of-god

import os
import json
import random
import logging
from datetime import datetime
from urllib.parse import urljoin

import pymongo
import scrapy
from scrapy.crawler import CrawlerProcess

logger = logging.getLogger("animeplanet_spider")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "manga_raw_data")
COLLECTION = "animeplanet_data"

# A reasonably sized UA pool (extend as needed)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7a) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Mobile Safari/537.36",
]

def mongo_client():
    return pymongo.MongoClient(MONGO_URI)


class AnimePlanetSpider(scrapy.Spider):
    name = "animeplanet_spider"
    # Conservative settings to reduce chance of being blocked
    custom_settings = {
        "DOWNLOAD_DELAY": 3.0,
        "CONCURRENT_REQUESTS": 1,
        "RETRY_ENABLED": False,
        "COOKIES_ENABLED": True,
        # ensure default headers (can be overridden per request)
        "DEFAULT_REQUEST_HEADERS": {
            "Accept-Language": "en-US,en;q=0.9",
            "DNT": "1",
            "Referer": "https://www.google.com/",
            "Connection": "keep-alive",
        },
    }

    def __init__(self, slug=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not slug:
            raise ValueError("Missing required slug argument (e.g. tower-of-god)")
        self.slug = slug.strip().rstrip("/")
        self.start_urls = [f"https://www.anime-planet.com/manga/{self.slug}"]
        self.client = mongo_client()
        self.db = self.client[MONGO_DB]
        self.col = self.db[COLLECTION]
        self.doc_id = f"ap_{self.slug}"

        # proxy from env (optional)
        self.proxy = os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY")

    def _headers(self):
        ua = random.choice(USER_AGENTS)
        headers = {
            "User-Agent": ua,
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        return headers

    def start_requests(self):
        for url in self.start_urls:
            meta = {"retries": 0}
            if self.proxy:
                meta["proxy"] = self.proxy
            yield scrapy.Request(url, headers=self._headers(), callback=self.parse_main, errback=self.errback, dont_filter=True, meta=meta)

    def errback(self, failure):
        logger.error("Request failure: %s", failure)
        doc = {
            "_id": self.doc_id,
            "fetched_at": datetime.utcnow().isoformat(),
            "source": "animeplanet",
            "source_id": self.slug,
            "source_url": f"https://www.anime-planet.com/manga/{self.slug}",
            "http": {"error": str(failure.value) if failure.value else "request_failed"},
            "status": "error",
        }
        try:
            self.col.replace_one({"_id": self.doc_id}, doc, upsert=True)
        except Exception:
            logger.exception("Failed to save error doc to mongo", exc_info=True)

    def parse_main(self, response):
        # If blocked (403), retry a few times with different UA and optional proxy
        if response.status == 403:
            retries = response.request.meta.get("retries", 0)
            if retries < 3:
                logger.warning("Received 403 for %s, retrying attempt %d", response.url, retries + 1)
                meta = {"retries": retries + 1}
                if self.proxy:
                    meta["proxy"] = self.proxy
                yield scrapy.Request(response.url, headers=self._headers(), callback=self.parse_main, errback=self.errback, dont_filter=True, meta=meta)
                return
            else:
                logger.warning("403 after retries for %s — saving forbidden doc", response.url)
                doc = {
                    "_id": self.doc_id,
                    "fetched_at": datetime.utcnow().isoformat(),
                    "source": "animeplanet",
                    "source_id": self.slug,
                    "source_url": response.url,
                    "http": {"code": 403},
                    "raw_prefix": response.text[:20000] if hasattr(response, "text") else None,
                    "status": "forbidden",
                }
                self.col.replace_one({"_id": self.doc_id}, doc, upsert=True)
                return

        # Collect main info (title, synopsis, rating if available)
        title = response.xpath("//h1/text()").get()
        synopsis = response.xpath("//div[contains(@class,'synopsis')]/p//text()").getall()
        synopsis = " ".join([s.strip() for s in synopsis]).strip()
        if not synopsis:
            synopsis = response.xpath("//meta[@name='description']/@content").get() or ""

        rating = response.xpath("//div[contains(@class,'avgRating') or contains(@class,'rating')]/text()").get()
        if rating:
            rating = rating.strip()

        recs = []
        rec_selectors = [
            "//section[contains(. , 'Similar') or contains(. , 'recommend')]/.//a[contains(@href,'/manga/')]/@href",
            "//a[contains(@class,'similar') and contains(@href,'/manga/')]/@href",
            "//div[contains(@class,'recommendations')]//a[contains(@href,'/manga/')]/@href",
            "//a[contains(@href,'/manga/') and contains(@class,'media')]/@href",
        ]
        for sel in rec_selectors:
            items = response.xpath(sel).getall()
            if items:
                for href in items:
                    href = href.strip()
                    if href.startswith("/"):
                        href = urljoin("https://www.anime-planet.com", href)
                    if "/manga/" in href:
                        slug = href.split("/manga/")[-1].split("?")[0].split("#")[0].strip("/")
                    else:
                        slug = href
                    recs.append({"slug": slug, "url": href})
                break

        # request reviews page
        reviews_url = response.url.rstrip("/") + "/reviews"
        meta = {"main": {"title": title, "synopsis": synopsis, "rating": rating, "recs": recs}, "retries": 0}
        if self.proxy:
            meta["proxy"] = self.proxy
        yield scrapy.Request(reviews_url, headers=self._headers(), callback=self.parse_reviews, meta=meta, dont_filter=True, errback=self.errback)

    def parse_reviews(self, response):
        main = response.meta.get("main", {})
        if response.status == 403:
            retries = response.request.meta.get("retries", 0)
            if retries < 3:
                logger.warning("Reviews page 403 for %s, retry attempt %d", response.url, retries + 1)
                meta = response.request.meta.copy()
                meta["retries"] = retries + 1
                if self.proxy:
                    meta["proxy"] = self.proxy
                yield scrapy.Request(response.url, headers=self._headers(), callback=self.parse_reviews, meta=meta, dont_filter=True, errback=self.errback)
                return
            else:
                logger.warning("Reviews page forbidden after retries %s", response.url)
                doc = {
                    "_id": self.doc_id,
                    "fetched_at": datetime.utcnow().isoformat(),
                    "source": "animeplanet",
                    "source_id": self.slug,
                    "source_url": f"https://www.anime-planet.com/manga/{self.slug}",
                    "http": {"code": 403},
                    "raw_prefix": response.text[:20000] if hasattr(response, "text") else None,
                    "status": "forbidden_reviews",
                }
                self.col.replace_one({"_id": self.doc_id}, doc, upsert=True)
                return

        reviews = []
        review_nodes = response.xpath("//div[contains(@class,'user-review') or contains(@class,'review') or //article[contains(@class,'review')]]")
        if not review_nodes:
            review_nodes = response.xpath("//li[contains(@class,'review') or contains(@class,'comment')]")
        for node in review_nodes:
            user = node.xpath(".//a[contains(@href,'/user/')]/text() | .//h3//text()").get()
            score = node.xpath(".//span[contains(@class,'score')]/text() | .//div[contains(@class,'rating')]/text()").get()
            parts = node.xpath(".//div[contains(@class,'review-body')]//text() | .//p//text()").getall()
            content = " ".join([p.strip() for p in parts]).strip()
            date = node.xpath(".//time/@datetime | .//span[contains(@class,'date')]/text() | .//time/text()").get()
            reviews.append({"user": user.strip() if user else None, "score": score.strip() if score else None, "content": content, "date": date})

        # try JSON-LD fallback
        if not reviews:
            ld = response.xpath("//script[@type='application/ld+json']/text()").get()
            if ld:
                try:
                    parsed = json.loads(ld)
                    if isinstance(parsed, dict) and parsed.get("review"):
                        rv = parsed.get("review")
                        if isinstance(rv, list):
                            for r in rv:
                                reviews.append({
                                    "user": r.get("author", {}).get("name") if isinstance(r.get("author"), dict) else r.get("author"),
                                    "score": r.get("reviewRating", {}).get("ratingValue") if r.get("reviewRating") else None,
                                    "content": r.get("reviewBody"),
                                    "date": r.get("datePublished"),
                                })
                except Exception:
                    logger.debug("Failed parse ld+json on %s", response.url, exc_info=True)

        doc = {
            "_id": self.doc_id,
            "fetched_at": datetime.utcnow().isoformat(),
            "source": "animeplanet",
            "source_id": self.slug,
            "source_url": f"https://www.anime-planet.com/manga/{self.slug}",
            "status": "ok" if reviews or main.get("recs") else ("empty" if not reviews and not main.get("recs") else "partial"),
            "main": {
                "title": main.get("title"),
                "synopsis": main.get("synopsis"),
                "rating": main.get("rating"),
                "recommendations": main.get("recs"),
            },
            "reviews": reviews,
            "raw_prefix": response.text[:20000] if hasattr(response, "text") else None
        }
        self.col.replace_one({"_id": self.doc_id}, doc, upsert=True)
        logger.info("[SAVED] %s %s | reviews=%d recs=%d status=%s", COLLECTION, self.doc_id, len(reviews), len(main.get("recs", [])), doc["status"])


if __name__ == "__main__":
    slug = os.environ.get("AP_SLUG", "tower-of-god")
    process = CrawlerProcess(settings={})
    process.crawl(AnimePlanetSpider, slug=slug)
    process.start()