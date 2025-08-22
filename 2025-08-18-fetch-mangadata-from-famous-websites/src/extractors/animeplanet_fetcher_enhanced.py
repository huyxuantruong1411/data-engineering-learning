# src/extractors/animeplanet_fetcher_enhanced.py
import asyncio
import logging
import random
import subprocess
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

# Try to import cloudscraper if available; if not, we'll fallback to requests and Playwright.
try:
    import cloudscraper  # type: ignore
except Exception:
    cloudscraper = None  # type: ignore

# Local project fallbacks (scrapy spider, mongo read) kept for compatibility
try:
    from src.scrapy_runner import run_scrapy_runspider
    from src.db import get_collection
except Exception:
    # Allow imports to fail in editors that don't have project path; runtime will have them.
    run_scrapy_runspider = None  # type: ignore
    get_collection = None  # type: ignore

logger = logging.getLogger(__name__)

ANIMEPLANET_BASE = "https://www.anime-planet.com"
DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


USER_AGENTS = [
    # a small list, rotated per-request
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


def _make_cloudscraper_session():
    """
    Return a cloudscraper session if available, else None.
    """
    if cloudscraper is None:
        return None
    try:
        # cloudscraper.create_scraper() will try to handle CF anti-bot,
        # but may not handle Turnstile challenges.
        s = cloudscraper.create_scraper(browser={
            "browser": "chrome",
            "platform": "windows",
            "mobile": False
        })
        return s
    except Exception as e:
        logger.debug("Could not create cloudscraper session: %s", e)
        return None


def _is_challenge_html(text: str, status_code: Optional[int] = None) -> bool:
    """
    Heuristics to detect Cloudflare/Turnstile or other challenge pages.
    """
    if status_code == 403:
        return True
    low = (text or "").lower()
    challenge_signs = [
        "verifying you are human",
        "just a moment",
        "cdn-cgi/challenge-platform",
        "__cf_chl_tk",
        "turnstile",
        "cf_chl_",
        "ray id:",
        "challenge-platform",
    ]
    for s in challenge_signs:
        if s in low:
            return True
    return False


def _ensure_playwright_browsers_installed() -> bool:
    """
    Attempt to run: python -m playwright install chromium
    Returns True if the subprocess ran without crashing (idempotent).
    """
    try:
        cmd = [sys.executable, "-m", "playwright", "install", "chromium"]
        subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=120)
        return True
    except Exception as e:
        logger.debug("playwright install attempt failed: %s", e)
        return False


async def _fetch_with_playwright_url(url: str, timeout_ms: int = 45000) -> Optional[str]:
    """
    Async fetch using Playwright. Tries chromium, firefox, webkit.
    Returns HTML or None.
    """
    try:
        from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError  # type: ignore
    except Exception as e:
        logger.debug("playwright not importable: %s", e)
        return None

    browser_order = ["chromium", "firefox", "webkit"]
    # try twice: first normally, if missing executable -> try to install then retry
    for attempt in range(2):
        for engine in browser_order:
            try:
                async with async_playwright() as p:
                    browser_type = getattr(p, engine)
                    try:
                        browser = await browser_type.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
                    except Exception as le:
                        msg = str(le).lower()
                        # if missing executables detected, break to install
                        if ("executable doesn't exist" in msg or "could not find" in msg or "no such file or directory" in msg):
                            logger.warning("Playwright executable missing for %s: %s", engine, le)
                            raise le
                        logger.debug("playwright launch error for %s: %s", engine, le)
                        raise le

                    ctx = await browser.new_context(user_agent=random.choice(USER_AGENTS), locale="en-US")
                    page = await ctx.new_page()
                    logger.debug("Playwright navigating to %s (engine=%s)", url, engine)
                    await page.goto(url, timeout=timeout_ms)
                    # Wait a bit for JS and network idle; tolerant to timeout
                    try:
                        await page.wait_for_load_state("networkidle", timeout=8000)
                    except PlaywrightTimeoutError:
                        pass
                    try:
                        html = await page.content()
                    finally:
                        try:
                            await browser.close()
                        except Exception:
                            pass
                    return html
            except Exception as e:
                logger.debug("playwright engine %s failed on attempt %d: %s", engine, attempt, e)
                # if this looks like missing executable -> attempt install & retry outer loop
                if attempt == 0:
                    # try install once
                    installed = _ensure_playwright_browsers_installed()
                    if installed:
                        logger.info("Attempted playwright install; retrying playwright fetch.")
                        # small pause to allow files to settle
                        await asyncio.sleep(0.5)
                        continue
                await asyncio.sleep(0.1)
                continue
    logger.debug("All playwright attempts failed for url %s", url)
    return None


def _parse_main_and_recommendations(html: str) -> Dict:
    """
    Parse main metadata and recommendations from the overview page HTML.
    Returns dict with keys: title, synopsis, rating, image, authors, genres, recommendations (list of {slug,url})
    """
    soup = BeautifulSoup(html, "lxml")
    # title
    title = None
    h1 = soup.select_one("h1")
    if h1:
        title = h1.get_text(strip=True)
    else:
        meta = soup.select_one("meta[property='og:title'], meta[name='title']")
        if meta:
            title = meta.get("content")
    # synopsis
    synopsis = ""
    s_node = soup.select_one("div.synopsis p")
    if s_node:
        synopsis = s_node.get_text(" ", strip=True)
    else:
        meta_desc = soup.select_one("meta[name='description']")
        if meta_desc:
            synopsis = meta_desc.get("content", "")
    # rating
    rating = None
    rc = soup.select_one("div.avgRating, div.rating, span.score")
    if rc:
        rating = rc.get_text(strip=True)
    else:
        meta_rating = soup.select_one("meta[itemprop='ratingValue']")
        if meta_rating:
            rating = meta_rating.get("content")
    # image
    img = None
    og_img = soup.select_one("meta[property='og:image']")
    if og_img:
        img = og_img.get("content")
    else:
        imgnode = soup.select_one("img.media-object, img.seriesImage")
        if imgnode:
            img = imgnode.get("src")
    # authors
    authors = []
    for a in soup.select("a[href*='/people/'], a[href*='/manga/author']"):
        t = a.get_text(strip=True)
        if t:
            authors.append(t)
    authors = list(dict.fromkeys(authors))
    # genres
    genres = []
    for g in soup.select("a[href*='/genres/'], a[href*='/manga/genre']"):
        t = g.get_text(strip=True)
        if t:
            genres.append(t)
    genres = list(dict.fromkeys(genres))
    # recommendations: look first for dedicated blocks, else scan anchors
    recs = []
    for blk in soup.select("section, div"):
        snippet = " ".join(blk.get_text(" ", strip=True).split()[:30]).lower()
        if any(k in snippet for k in ("recommend", "recommendations", "you might like", "similar")):
            for a in blk.select("a[href*='/manga/']"):
                href = a.get("href", "").strip()
                if not href:
                    continue
                href_full = (ANIMEPLANET_BASE + href) if href.startswith("/") else href
                if "/manga/" in href_full:
                    slug = href_full.split("/manga/")[-1].split("?")[0].split("#")[0].strip("/")
                    if slug:
                        recs.append({"slug": slug, "url": href_full})
            if recs:
                break
    if not recs:
        seen = set()
        out = []
        for a in soup.select("a[href*='/manga/']"):
            href = a.get("href", "").strip()
            if not href:
                continue
            href_full = (ANIMEPLANET_BASE + href) if href.startswith("/") else href
            if "/manga/" in href_full:
                slug = href_full.split("/manga/")[-1].split("?")[0].split("#")[0].strip("/")
                if slug and slug not in seen:
                    seen.add(slug)
                    out.append({"slug": slug, "url": href_full})
            if len(out) >= 50:
                break
        recs = out
    return {
        "title": title,
        "synopsis": synopsis,
        "rating": rating,
        "image": img,
        "authors": authors,
        "genres": genres,
        "recommendations": recs,
    }


def _parse_reviews(html: str) -> List[Dict]:
    """
    Parse reviews from reviews page HTML.
    """
    soup = BeautifulSoup(html, "lxml")
    reviews = []
    for div in soup.select(".reviewText, .user-review, article.review, .review"):
        text = div.get_text(" ", strip=True)
        if text:
            reviews.append({"text": text})
    if not reviews:
        for li in soup.select("li.review, li.comment"):
            text = li.get_text(" ", strip=True)
            if text:
                reviews.append({"text": text})
    return reviews


def _run_scrapy_and_read(slug: str) -> Optional[Dict]:
    """
    Optional fallback: run a scrapy runspider for animeplanet spider (if available)
    and then read the result from Mongo. Works only if project includes spider and Mongo.
    """
    if run_scrapy_runspider is None or get_collection is None:
        return None
    try:
        rc, out, err = run_scrapy_runspider("spiders/animeplanet_spider.py", ["-a", f"slug={slug}"])
        logger.info("Scrapy runspider rc=%s stdout_len=%d stderr_len=%d", rc, len(out or ""), len(err or ""))
        col = get_collection("manga_raw_data", "animeplanet_data")
        doc = col.find_one({"_id": f"ap_{slug}"})
        return doc
    except Exception as e:
        logger.exception("Scrapy fallback failed: %s", e)
        return None


def get_full_data(slug: str, max_retries: int = 3, conservative_wait: bool = False) -> Dict:
    """
    Main synchronous entrypoint used by pipeline.
    Attempts (in order):
      1) cloudscraper session (if available) to fetch overview page
      2) if cloudscraper indicates Cloudflare/Turnstile or 403: fallback to Playwright
      3) if both fail: optional scrapy-runspider fallback
    Returns payload with keys:
      _id, source, source_id, source_url, fetched_at, raw_prefix, main, reviews, recommendations, http, status
    """
    payload = {
        "_id": f"ap_{slug}",
        "source": "animeplanet",
        "source_id": slug,
        "source_url": f"{ANIMEPLANET_BASE}/manga/{slug}",
        "fetched_at": datetime.utcnow().isoformat(),
    }

    session = _make_cloudscraper_session()
    html_main = None
    used_playwright = False

    # 1) Try cloudscraper (or requests if cloudscraper missing)
    if session is not None:
        headers = dict(DEFAULT_HEADERS)
        headers["User-Agent"] = random.choice(USER_AGENTS)
        url = f"{ANIMEPLANET_BASE}/manga/{slug}"
        try:
            for attempt in range(max_retries):
                try:
                    r = session.get(url, headers=headers, timeout=30)
                    status = getattr(r, "status_code", None)
                    text = getattr(r, "text", "")
                    logger.info("Request to %s: status=%s content_length=%d", url, status, len(text or ""))
                    if status == 200 and not _is_challenge_html(text, status):
                        html_main = text
                        break
                    # if challenge or 403 => retry a few times then escalate to playwright
                    if _is_challenge_html(text, status) or status == 403:
                        logger.warning("Detected challenge or 403 for %s (attempt %d)", url, attempt + 1)
                        # backoff: if conservative mode wait longer
                        wait = 10 + attempt * (20 if conservative_wait else 5) + random.random() * 3
                        time.sleep(wait)
                        continue
                    # other codes: wait a bit and retry
                    time.sleep(1.0 + random.random() * 1.5)
                except Exception as e:
                    logger.debug("Cloudscraper request error (attempt %d): %s", attempt + 1, e)
                    time.sleep(1.0 + random.random() * 0.5)
            else:
                logger.info("Cloudscraper attempts exhausted for %s", url)
        except Exception as e:
            logger.debug("Cloudscraper session fetch failed: %s", e)

    # 2) If no html_main from cloudscraper or session missing -> try Playwright
    if not html_main:
        logger.info("Falling back to Playwright for %s", slug)
        used_playwright = True
        try:
            # use a new event loop for synchronous call
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            html_main = loop.run_until_complete(_fetch_with_playwright_url(f"{ANIMEPLANET_BASE}/manga/{slug}"))
        except Exception as e:
            logger.debug("Playwright fetch main failed: %s", e)
            html_main = None
        finally:
            try:
                loop.close()
            except Exception:
                pass

    if html_main:
        payload["raw_prefix"] = html_main[:20000]
        main = _parse_main_and_recommendations(html_main)
        payload["main"] = {k: v for k, v in main.items() if k != "recommendations"}
        # recommendations from main
        payload["recommendations"] = main.get("recommendations", [])
        # 3) Fetch /recommendations endpoint (dedicated) — try cloudscraper first, else playwright
        rec_html = None
        rec_url = f"{ANIMEPLANET_BASE}/manga/{slug}/recommendations"
        # cloudscraper attempt
        if session is not None:
            try:
                headers = dict(DEFAULT_HEADERS)
                headers["User-Agent"] = random.choice(USER_AGENTS)
                r = session.get(rec_url, headers=headers, timeout=30)
                status = getattr(r, "status_code", None)
                text = getattr(r, "text", "")
                logger.info("Recommendations request %s status=%s len=%d", rec_url, status, len(text or ""))
                if status == 200 and not _is_challenge_html(text, status):
                    rec_html = text
            except Exception as e:
                logger.debug("cloudscraper rec request failed: %s", e)
        if not rec_html:
            # playwright fallback for rec endpoint
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                rec_html = loop.run_until_complete(_fetch_with_playwright_url(rec_url))
            except Exception as e:
                logger.debug("playwright rec fetch failed: %s", e)
            finally:
                try:
                    loop.close()
                except Exception:
                    pass

        if rec_html:
            recs = _parse_main_and_recommendations(rec_html).get("recommendations", [])
            # merge: add items from recs not already present (by slug)
            seen = {r["slug"] for r in payload.get("recommendations", [])}
            extras = [r for r in recs if r["slug"] not in seen]
            if extras:
                payload["recommendations"].extend(extras)

        # 4) Fetch reviews page (/reviews)
        rv_html = None
        rv_url = f"{ANIMEPLANET_BASE}/manga/{slug}/reviews"
        if session is not None:
            try:
                headers = dict(DEFAULT_HEADERS)
                headers["User-Agent"] = random.choice(USER_AGENTS)
                r = session.get(rv_url, headers=headers, timeout=30)
                status = getattr(r, "status_code", None)
                text = getattr(r, "text", "")
                logger.info("Reviews request %s status=%s len=%d", rv_url, status, len(text or ""))
                if status == 200 and not _is_challenge_html(text, status):
                    rv_html = text
            except Exception as e:
                logger.debug("cloudscraper reviews request failed: %s", e)
        if not rv_html:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                rv_html = loop.run_until_complete(_fetch_with_playwright_url(rv_url))
            except Exception as e:
                logger.debug("playwright reviews fetch failed: %s", e)
            finally:
                try:
                    loop.close()
                except Exception:
                    pass

        payload["reviews"] = _parse_reviews(rv_html) if rv_html else []
        payload["http"] = {"code": 200}
        payload["status"] = "ok" if (payload.get("reviews") or payload.get("recommendations")) else "no_reviews"
        return payload

    # If we reached here: nothing fetched — try scrapy fallback (if available)
    logger.info("All HTTP/Playwright attempts failed for ap_%s — trying scrapy spider fallback", slug)
    spider_doc = _run_scrapy_and_read(slug)
    if spider_doc:
        spider_doc.setdefault("source", "animeplanet")
        spider_doc.setdefault("source_id", slug)
        return spider_doc

    # ultimate fallback
    payload.update({"reviews": [], "recommendations": [], "http": {"error": "could_not_fetch"}, "status": "error"})
    return payload


def get_reviews(slug: str) -> List[Dict]:
    """
    Backwards-compatible function: returns reviews list (possibly empty).
    """
    doc = get_full_data(slug)
    return doc.get("reviews", [])