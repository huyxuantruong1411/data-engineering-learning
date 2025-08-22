import asyncio
import logging
import subprocess
import sys
from datetime import datetime
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from src.scrapy_runner import run_scrapy_runspider
from src.db import get_collection

logger = logging.getLogger(__name__)

ANIMEPLANET_BASE = "https://www.anime-planet.com"


# ---------------- Playwright Setup ----------------
def _ensure_playwright_browsers_installed() -> bool:
    try:
        subprocess.run([sys.executable, "-m", "playwright", "--version"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        return True
    except Exception as e:
        logger.warning("Playwright install failed: %s", e)
        return False


async def _fetch_with_playwright(url: str, timeout_ms: int = 45000) -> Optional[str]:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("Playwright not installed")
        return None

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            context = await browser.new_context(
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"),
                locale="en-US"
            )
            page = await context.new_page()
            await page.goto(url, timeout=timeout_ms)
            await page.wait_for_selector("body", timeout=8000)
            html = await page.content()
            await browser.close()
            return html
    except Exception as e:
        logger.error("Playwright fetch failed for %s: %s", url, e)
        return None


def _run_scrapy_and_read(slug: str) -> Optional[Dict]:
    rc, out, err = run_scrapy_runspider("spiders/animeplanet_spider.py", ["-a", f"slug={slug}"])
    logger.info("Scrapy fallback rc=%s", rc)
    try:
        col = get_collection("manga_raw_data", "animeplanet_data")
        return col.find_one({"_id": f"ap_{slug}"})
    except Exception as e:
        logger.error("Scrapy fallback read failed: %s", e)
        return None


# ---------------- Parsers ----------------
def _parse_main(html: str) -> Dict:
    soup = BeautifulSoup(html, "lxml")
    title = soup.select_one("h1")
    title = title.get_text(strip=True) if title else None

    synopsis = ""
    s_node = soup.select_one("div.synopsis p")
    if s_node:
        synopsis = s_node.get_text(" ", strip=True)

    rating = None
    rc = soup.select_one("div.avgRating, span[itemprop='ratingValue']")
    if rc:
        rating = rc.get_text(strip=True)

    img = None
    og_img = soup.select_one("meta[property='og:image']")
    if og_img:
        img = og_img.get("content")

    authors = [a.get_text(strip=True) for a in soup.select("a[href*='/people/']")]
    genres = [g.get_text(strip=True) for g in soup.select("a[href*='/manga/genres/']")]

    return {
        "title": title,
        "synopsis": synopsis,
        "rating": rating,
        "image": img,
        "authors": list(dict.fromkeys(authors)),
        "genres": list(dict.fromkeys(genres)),
    }


def _parse_reviews(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    reviews = []
    for div in soup.select(".reviewText, .user-review, article.review"):
        text = div.get_text(" ", strip=True)
        if text:
            reviews.append({"text": text})
    return reviews


def _parse_recommendations(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    recs = []
    for a in soup.select("a[href*='/manga/']"):
        href = a.get("href", "")
        if "/manga/" in href:
            slug = href.split("/manga/")[-1].split("?")[0].strip("/")
            recs.append({"slug": slug, "url": ANIMEPLANET_BASE + href})
    return list({r["slug"]: r for r in recs}.values())


# ---------------- Public API ----------------
def get_full_data(slug: str) -> Dict:
    payload = {
        "_id": f"ap_{slug}",
        "source": "animeplanet",
        "source_id": slug,
        "source_url": f"{ANIMEPLANET_BASE}/manga/{slug}",
        "fetched_at": datetime.utcnow().isoformat(),
    }

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        html_main = loop.run_until_complete(
            _fetch_with_playwright(f"{ANIMEPLANET_BASE}/manga/{slug}")
        )
        html_reviews = loop.run_until_complete(
            _fetch_with_playwright(f"{ANIMEPLANET_BASE}/manga/{slug}/reviews")
        )
        html_recs = loop.run_until_complete(
            _fetch_with_playwright(f"{ANIMEPLANET_BASE}/manga/{slug}/recommendations")
        )
    finally:
        loop.close()

    if html_main:
        payload["main"] = _parse_main(html_main)
    else:
        payload["main"] = {}

    payload["reviews"] = _parse_reviews(html_reviews) if html_reviews else []
    payload["recommendations"] = _parse_recommendations(html_recs) if html_recs else []

    payload["http"] = {"code": 200 if html_main else 500}
    payload["status"] = "ok" if (payload["reviews"] or payload["recommendations"]) else "no_reviews"

    if not html_main and not html_reviews and not html_recs:
        logger.info("Playwright failed, trying Scrapy fallback for %s", slug)
        doc = _run_scrapy_and_read(slug)
        if doc:
            return doc
        payload.update({"http": {"error": "could_not_fetch"}, "status": "error"})

    return payload


def get_reviews(slug: str) -> List[Dict]:
    return get_full_data(slug).get("reviews", [])