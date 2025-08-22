import logging
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import requests
import lxml.html
from lxml.cssselect import CSSSelector
from bs4 import BeautifulSoup
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from concurrent.futures import ThreadPoolExecutor
import threading

logger = logging.getLogger(__name__)

MAL_BASE = "https://myanimelist.net"
MAL_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Configuration for 87k manga in 24h - SPEED OPTIMIZED (2-3x faster)
TARGET_OBJECTS_PER_HOUR = 100000 / 24  # ~3625 objects/hour
MAX_WORKERS = 25  # Increased from 4 to 8 for 2x speed
REQUESTS_PER_WORKER_PER_HOUR = TARGET_OBJECTS_PER_HOUR / MAX_WORKERS  # ~453 per worker
SECONDS_PER_REQUEST = 3600 / REQUESTS_PER_WORKER_PER_HOUR  # ~8s per request per worker
JITTER_FACTOR = 0.15  # Reduced jitter for faster processing
RANK_INCREMENT = 50  # Process 50 manga per ranking page
BATCH_SIZE = 20  # Increased batch size for parallel processing

# Thread-safe rate limiting
_last_request_time = {}
_request_lock = threading.Lock()
processed_manga_cache = set()

# Speed optimization constants
MIN_DELAY = 1.0  # Reduced from 2s to 1s
MAX_DELAY = 3.0  # Reduced from 7s to 3s
RETRY_DELAYS = [1, 2, 4]  # Faster retry progression
REQUEST_TIMEOUT = 10  # Reduced from 15s to 10s

def _parse_reviews(html: str, mal_id: str) -> List[Dict]:
    if not html.strip():
        logger.warning(f"Empty reviews HTML for {mal_id}")
        return []
    
    soup = BeautifulSoup(html, "lxml")
    reviews = []
    
    selectors = ["div.review-element", "div.review-element.js-review-element", "div.borderDark"]
    for selector in selectors:
        review_elements = soup.select(selector)
        if review_elements:
            logger.debug(f"Using selector '{selector}' for {mal_id}: found {len(review_elements)} reviews")
            break
    else:
        logger.warning(f"No reviews found for {mal_id} with selectors: {selectors}")
        return []
    
    for review in review_elements:
        try:
            review_id_elem = review.select_one('div.open a') or review.select_one('a[href*="/reviews/"]')
            review_id = review_id_elem.get('href', '').split('/')[-1] if review_id_elem else ''
            if not review_id:
                continue
            
            review_text_elem = review.select_one('div.text') or review.select_one('div.review-body')
            review_text = ''
            if review_text_elem:
                review_text = ' '.join(review_text_elem.get_text(' ', strip=True).split())
            
            if not review_text or len(review_text) < 5:
                continue
            
            reactions_dict = review.get('data-reactions', '')
            reactions = {}
            if reactions_dict:
                try:
                    import json
                    reactions_data = json.loads(reactions_dict)
                    reaction_type_map = ['nice', 'loveIt', 'funny', 'confusing', 'informative', 'wellWritten', 'creative']
                    reactions = {r: c for r, c in zip(reaction_type_map, reactions_data.get('count', ['0']*7))}
                except:
                    pass
            
            author_elem = review.select_one('div.username a') or review.select_one('div.reviewer a')
            author = author_elem.get_text(strip=True) if author_elem else ''
            
            score_elem = review.select_one('div.rating span.num') or review.select_one('div.score')
            score = score_elem.get_text(strip=True) if score_elem else ''
            
            post_time = review.select_one('div.update_at') or review.select_one('div.date')
            post_time_text = post_time.get_text(strip=True) if post_time else ''
            
            episodes_seen_elem = review.select_one('.tag.preliminary span') or review.select_one('div.episodes-seen')
            episodes_seen = episodes_seen_elem.get_text(strip=True) if episodes_seen_elem else ''
            
            recommendation_elem = review.select_one('.tag.recommended') or review.select_one('.tag.recommendation')
            recommendation_status = recommendation_elem.get_text(strip=True) if recommendation_elem else ''
            
            profile_url_elem = review.select_one('div.thumb a') or review.select_one('div.reviewer a')
            profile_url = profile_url_elem.get('href') if profile_url_elem else ''
            
            profile_img_elem = review.select_one('div.thumb a img') or review.select_one('div.reviewer img')
            profile_img = profile_img_elem.get('src') if profile_img_elem else ''
            
            review_data = {
                'reviewId': review_id,
                'text': review_text[:3000],
                'author': author,
                'score': score,
                'postTime': post_time_text,
                'episodesSeen': episodes_seen,
                'recommendationStatus': recommendation_status,
                'profileUrl': profile_url,
                'profileImage': profile_img,
                **reactions
            }
            
            reviews.append(review_data)
        except Exception as e:
            logger.debug(f"Error parsing review for {mal_id}: {e}")
    
    return reviews

def _parse_recommendations(html: str) -> List[Dict]:
    if not html.strip():
        return []
    
    soup = BeautifulSoup(html, "lxml")
    recs = []
    
    selectors = [
        "div.borderClass a[href*='/manga/']",
        "table.anime_detail_related_anime a[href*='/manga/']",
        "div.spaceit_pad a[href*='/manga/']",
        "td a[href*='/manga/']",
        "a[href*='/manga/']:not([href*='/reviews']):not([href*='/userrecs'])"
    ]
    
    for selector in selectors:
        links = soup.select(selector)
        for a in links:
            href = a.get("href", "")
            title = a.get_text(strip=True)
            if "/manga/" not in href or not title or len(title) < 2:
                continue
            try:
                mid = href.split("/manga/")[1].split("/")[0]
                if mid.isdigit():
                    reason = ""
                    parent = a.find_parent('td') or a.find_parent('div')
                    if parent:
                        reason_elem = parent.find_next_sibling()
                        if reason_elem:
                            reason = reason_elem.get_text(strip=True)[:200]
                    
                    recs.append({
                        "id": mid,
                        "title": title,
                        "url": href,
                        "reason": reason
                    })
            except:
                continue
    
    seen = set()
    unique_recs = []
    for rec in recs:
        if rec["id"] not in seen:
            seen.add(rec["id"])
            unique_recs.append(rec)
    
    return unique_recs[:20]

@retry(
    retry=retry_if_exception_type((requests.exceptions.HTTPError, requests.exceptions.RequestException)),
    wait=wait_exponential(multiplier=1.2, min=1, max=15),  # Faster retry: min 1s, max 15s
    stop=stop_after_attempt(2),  # Reduced from 3 to 2 attempts
    reraise=True
)
def _fetch_page(url: str, session: requests.Session, headers: Dict, mal_id: str, page_type: str, worker_id: int = 0) -> str:
    """Thread-safe rate limiting with per-worker tracking - optimized for 24h target"""
    with _request_lock:
        current_time = time.time()
        last_time = _last_request_time.get(worker_id, 0)
        
        # Reduced delays for 24h target
        base_delay = max(2, SECONDS_PER_REQUEST * 0.7)  # 30% faster
        jitter = random.uniform(-JITTER_FACTOR * base_delay, JITTER_FACTOR * base_delay)
        required_delay = max(1, base_delay + jitter)  # Minimum 1s delay
        
        elapsed = current_time - last_time
        if elapsed < required_delay:
            sleep_time = required_delay - elapsed
            time.sleep(sleep_time)
        
        _last_request_time[worker_id] = time.time()

    try:
        response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)  # Optimized timeout
        
        # Save to temp folder for debugging
        temp_folder = Path('tmp/mal_manga_data')
        temp_folder.mkdir(parents=True, exist_ok=True)
        with open(temp_folder / f'mal_{mal_id}_{page_type}.html', 'w', encoding='utf-8') as f:
            f.write(response.text if response.ok else '')
        
        if response.status_code == 404:
            logger.warning(f"MAL ID {mal_id} not found (404)")
            return ""
        if response.status_code == 405:
            return ""
        
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return ""

def _fetch_ranking_page(limit: int, worker_id: int = 0) -> List[str]:
    """Fetch manga IDs from ranking page - independent approach like project_dump.txt"""
    headers = {
        "User-Agent": random.choice(MAL_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://myanimelist.net/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache"
    }
    
    session = requests.Session()
    rank_url = f'https://myanimelist.net/topmanga.php?limit={limit}'
    
    try:
        # Direct request instead of using _fetch_page to avoid 405 handling
        response = session.get(rank_url, headers=headers, timeout=REQUEST_TIMEOUT)
        
        # Handle different status codes
        if response.status_code == 405:
            logger.warning(f"MAL returned 405 for ranking page {limit} - trying alternative approach")
            # Try without query params first
            alt_url = 'https://myanimelist.net/topmanga.php'
            response = session.get(alt_url, headers=headers, timeout=REQUEST_TIMEOUT)
        
        if response.status_code == 403:
            logger.warning(f"MAL blocked request for ranking page {limit} - need better anti-blocking")
            return []
            
        if not response.ok:
            logger.warning(f"MAL ranking page {limit} returned {response.status_code}")
            return []
        
        html = response.text
        if not html.strip():
            return []
        
        tree = lxml.html.fromstring(html)
        
        # Try multiple selectors for manga URLs
        selectors = [
            'a.hoverinfo_trigger',
            'td.title a',
            'a[href*="/manga/"]',
            '.ranking-list a.hoverinfo_trigger'
        ]
        
        manga_urls = []
        for selector in selectors:
            try:
                css_selector = CSSSelector(selector)
                urls = [elem.get('href') for elem in css_selector(tree) if elem.get('href')]
                manga_urls.extend(urls)
                if urls:
                    logger.debug(f"Selector '{selector}' found {len(urls)} URLs")
                    break
            except:
                continue
        
        # Extract manga IDs from URLs
        manga_ids = []
        for url in manga_urls:
            if url and '/manga/' in url:
                try:
                    parts = url.split('/')
                    mal_id = None
                    for i, part in enumerate(parts):
                        if part == 'manga' and i + 1 < len(parts):
                            mal_id = parts[i + 1]
                            break
                    
                    if mal_id and mal_id.isdigit() and mal_id not in processed_manga_cache:
                        manga_ids.append(mal_id)
                        processed_manga_cache.add(mal_id)
                except:
                    continue
        
        logger.info(f"Ranking page {limit}: Found {len(manga_ids)} new manga IDs")
        return manga_ids
        
    except Exception as e:
        logger.error(f"Failed to fetch ranking page {limit}: {e}")
        return []

def _fetch_mal_comprehensive(mal_id: str, worker_id: int = 0) -> tuple[str, str, List[Dict]]:
    """Comprehensive manga data fetch - optimized for speed"""
    headers = {
        "User-Agent": random.choice(MAL_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://myanimelist.net/",
    }
    
    session = requests.Session()
    
    # Fetch main page
    main_html = _fetch_page(f"{MAL_BASE}/manga/{mal_id}", session, headers, mal_id, "main", worker_id)
    
    # Fetch recommendations page
    recs_html = _fetch_page(f"{MAL_BASE}/manga/{mal_id}/_/userrecs", session, headers, mal_id, "recs", worker_id)
    
    # Fetch reviews - limit to first page only for maximum speed
    reviews = []
    for page in range(1, 2):  # Only first page for 3x speed boost
        review_url = f"{MAL_BASE}/manga/{mal_id}/reviews?p={page}"
        try:
            review_html = _fetch_page(review_url, session, headers, mal_id, f"reviews_page_{page}", worker_id)
            if not review_html:
                break
            page_reviews = _parse_reviews(review_html, mal_id)
            reviews.extend(page_reviews)
            
            if not page_reviews or len(page_reviews) < 3:  # Stop if very few reviews (speed optimization)
                break
                
        except Exception as e:
            logger.debug(f"Review page {page} failed for {mal_id}: {e}")
            break
    
    return main_html, recs_html, reviews

def _parse_manga_info(html: str, mal_id: str) -> Dict:
    if not html.strip():
        return {}
    
    tree = lxml.html.fromstring(html)
    info = {}
    
    try:
        info['jpName'] = tree.xpath('//span[contains(text(), "Japanese:")]/following::text()')[0].strip() if tree.xpath('//span[contains(text(), "Japanese:")]/following::text()') else ''
        info['engName'] = tree.xpath('//span[contains(text(), "English:")]/following::text()')[0].strip() if tree.xpath('//span[contains(text(), "English:")]/following::text()') else ''
        info['synonyms'] = tree.xpath('//span[contains(text(), "Synonyms:")]/following::text()')[0].strip() if tree.xpath('//span[contains(text(), "Synonyms:")]/following::text()') else ''
        info['type'] = tree.xpath('//span[text()="Type:"]/following-sibling::a/text()')[0] if tree.xpath('//span[text()="Type:"]/following-sibling::a/text()') else ''
        info['volumes'] = tree.xpath('//span[text()="Volumes:"]/following::text()')[0].strip() if tree.xpath('//span[text()="Volumes:"]/following::text()') else ''
        info['chapters'] = tree.xpath('//span[text()="Chapters:"]/following::text()')[0].strip() if tree.xpath('//span[text()="Chapters:"]/following::text()') else ''
        info['status'] = tree.xpath('//span[text()="Status:"]/following::text()')[0].strip() if tree.xpath('//span[text()="Status:"]/following::text()') else ''
        info['published'] = tree.xpath('//span[text()="Published:"]/following::text()')[0].strip() if tree.xpath('//span[text()="Published:"]/following::text()') else ''
        info['genres'] = ', '.join(tree.xpath('//span[text()="Genres:"]/following-sibling::a/text()'))
        info['themes'] = ', '.join(tree.xpath('//span[text()="Themes:"]/following-sibling::a/text()'))
        info['demographic'] = tree.xpath('//span[text()="Demographic:"]/following-sibling::a/text()')[0] if tree.xpath('//span[text()="Demographic:"]/following-sibling::a/text()') else ''
        info['serialization'] = ', '.join(tree.xpath('//span[text()="Serialization:"]/following-sibling::a/text()'))
        info['authors'] = ', '.join(tree.xpath('//span[text()="Authors:"]/following-sibling::a/text()'))
        info['score'] = CSSSelector('span.score-label')(tree)[0].text if CSSSelector('span.score-label')(tree) else ''
        info['ranked'] = tree.xpath('//span[text()="Ranked:"]/following::text()')[0].strip() if tree.xpath('//span[text()="Ranked:"]/following::text()') else ''
        info['popularity'] = tree.xpath('//span[text()="Popularity:"]/following::text()')[0].strip() if tree.xpath('//span[text()="Popularity:"]/following::text()') else ''
        info['members'] = tree.xpath('//span[text()="Members:"]/following::text()')[0].strip() if tree.xpath('//span[text()="Members:"]/following::text()') else ''
        info['favorites'] = tree.xpath('//span[text()="Favorites:"]/following::text()')[0].strip() if tree.xpath('//span[text()="Favorites:"]/following::text()') else ''
        info['cover_image'] = CSSSelector('div.leftside img.lazyload')(tree)[0].get('src') or CSSSelector('div.leftside img.lazyload')(tree)[0].get('data-src') if CSSSelector('div.leftside img.lazyload')(tree) else ''
        info['synopsis'] = tree.xpath('//span[@itemprop="description"]/text()')[0].strip() if tree.xpath('//span[@itemprop="description"]/text()') else ''
    except Exception as e:
        logger.warning(f"Error parsing manga info for {mal_id}: {e}")
    
    return info

def get_full_data(mal_id: str, worker_id: int = 0) -> Dict:
    """Get comprehensive manga data for single ID"""
    payload = {
        "_id": f"mal_{mal_id}",
        "source": "mal",
        "source_id": mal_id,
        "source_url": f"{MAL_BASE}/manga/{mal_id}",
        "fetched_at": datetime.utcnow().isoformat(),
    }
    
    try:
        main_html, recs_html, reviews = _fetch_mal_comprehensive(mal_id, worker_id)
        
        payload["manga_info"] = _parse_manga_info(main_html, mal_id)
        payload["recommendations"] = _parse_recommendations(recs_html) if recs_html else []
        payload["reviews"] = reviews if reviews else []
        
        has_data = bool(payload["reviews"] or payload["recommendations"] or payload["manga_info"])
        payload["status"] = "ok" if has_data else "no_reviews"
        payload["http"] = {"code": 200} if has_data else {"error": "no_data"}
    except Exception as e:
        logger.error(f"MAL fetch failed for {mal_id}: {e}")
        payload.update({"recommendations": [], "reviews": [], "manga_info": {}, "status": "error", "http": {"error": str(e)}})

    return payload

def get_ranking_based_data(start_limit: int = 0, max_pages: int = 100) -> List[Dict]:
    """
    Collect manga data from MAL ranking pages independently.
    
    Args:
        start_limit: Starting ranking position (0, 50, 100, ...)
        max_pages: Maximum number of ranking pages to process (0 = unlimited)
        
    Returns:
        List of manga data dictionaries
    """
    logger.info(f"🚀 Starting MAL ranking-based collection from limit {start_limit}")
    
    if max_pages == 0:
        logger.info(f"🔄 UNLIMITED MODE: Will process ALL ranking pages until no more manga found")
    else:
        logger.info(f"📊 Will process up to {max_pages} ranking pages")
    
    all_results = []
    current_limit = start_limit
    page_count = 0
    consecutive_empty_pages = 0
    
    while True:
        # Check if we should stop (limited mode only)
        if max_pages > 0 and page_count >= max_pages:
            logger.info(f"📊 Reached maximum pages limit ({max_pages})")
            break
            
        # Check if we hit too many consecutive empty pages (unlimited mode)
        if consecutive_empty_pages >= 10:  # Increased from 5 to 10 for more persistence
            logger.info(f"🛑 Stopping: Found {consecutive_empty_pages} consecutive empty pages")
            break
        
        page_count += 1
        logger.info(f"📄 Processing ranking page {page_count} (limit={current_limit})")
        
        try:
            # Fetch manga IDs from current ranking page
            manga_ids = _fetch_ranking_page(current_limit)
            
            if not manga_ids:
                consecutive_empty_pages += 1
                logger.warning(f"⚠️ No manga IDs found on ranking page {current_limit} (empty #{consecutive_empty_pages})")
                current_limit += RANK_INCREMENT
                # Add delay before retrying next page
                time.sleep(random.uniform(2, 5))
                continue
            
            # Reset empty page counter
            consecutive_empty_pages = 0
            
            logger.info(f"📋 Found {len(manga_ids)} manga IDs on ranking page {current_limit}")
            
            # Process manga IDs in parallel batches
            batch_results = get_full_data_parallel(manga_ids, MAX_WORKERS)
            all_results.extend(batch_results)
            
            logger.info(f"✅ Processed {len(batch_results)} manga from ranking page {current_limit}")
            logger.info(f"📊 Total manga processed so far: {len(all_results)}")
            
            # Move to next ranking page
            current_limit += RANK_INCREMENT
            
            # Reduced delay between ranking pages for speed
            delay = random.uniform(1.5, 3.5)  # Reduced from 3-7s to 1.5-3.5s
            logger.info(f"⏳ Waiting {delay:.1f}s before next ranking page...")
            time.sleep(delay)
            
        except Exception as e:
            logger.error(f"❌ Error processing ranking page {current_limit}: {e}")
            consecutive_empty_pages += 1  # Count errors as empty pages
            current_limit += RANK_INCREMENT
            # Add delay before retrying after error
            time.sleep(random.uniform(3, 8))
            continue
    
    logger.info(f"🎉 MAL ranking-based collection completed! Total: {len(all_results)} manga")
    return all_results

def get_full_data_parallel(mal_ids: List[str], max_workers: int = MAX_WORKERS) -> List[Dict]:
    """Parallel version optimized for 24h target with anti-blocking"""
    if not mal_ids:
        return []
    
    logger.info(f"Starting parallel MAL fetch: {len(mal_ids)} manga with {max_workers} workers")
    
    def worker_task(worker_id: int, batch_ids: List[str]) -> List[Dict]:
        """Worker function with optimized rate limiting"""
        results = []
        for i, mal_id in enumerate(batch_ids):
            try:
                logger.debug(f"Worker {worker_id}: Fetching MAL {mal_id} ({i+1}/{len(batch_ids)})")
                # Thread-safe rate limiting - SPEED OPTIMIZED
                with _request_lock:
                    worker_key = f"worker_{worker_id}"
                    current_time = time.time()
                    
                    if worker_key in _last_request_time:
                        elapsed = current_time - _last_request_time[worker_key]
                        min_interval = random.uniform(MIN_DELAY, MAX_DELAY)  # 1-3s instead of 1.5-4s
                        
                        if elapsed < min_interval:
                            sleep_time = min_interval - elapsed + random.uniform(0, 0.2)  # Reduced jitter
                            logger.debug(f"Worker {worker_id}: Rate limiting, sleeping {sleep_time:.1f}s")
                            time.sleep(sleep_time)
                    
                    _last_request_time[worker_key] = time.time()
                
                result = get_full_data(mal_id, worker_id)
                results.append(result)
                
                # Minimal inter-request delay within worker - SPEED OPTIMIZED
                if i < len(batch_ids) - 1:
                    inter_delay = random.uniform(0.3, 0.8)  # Further reduced for 3x speed
                    time.sleep(inter_delay)
                    
            except Exception as e:
                logger.error(f"Worker {worker_id}: Error fetching MAL {mal_id}: {e}")
                results.append({
                    "_id": f"mal_{mal_id}",
                    "source": "mal",
                    "source_id": mal_id,
                    "reviews": [],
                    "recommendations": [],
                    "manga_info": {},
                    "status": "error",
                    "http": {"error": str(e)}
                })
        return results
    
    # Chia thành batches cho parallel processing - OPTIMIZED
    batch_size = max(1, len(mal_ids) // max_workers)
    if batch_size > BATCH_SIZE:  # Use optimized batch size
        batch_size = BATCH_SIZE
    batches = [mal_ids[i:i+batch_size] for i in range(0, len(mal_ids), batch_size)]
    
    all_results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for worker_id, chunk in enumerate(batches):
            if chunk:  # Only submit non-empty chunks
                future = executor.submit(worker_task, worker_id, chunk)
                futures.append(future)
        
        for future in futures:
            try:
                worker_results = future.result()
                all_results.extend(worker_results)
            except Exception as e:
                logger.error(f"Worker failed: {e}")
    
    logger.info(f"Parallel MAL fetch completed: {len(all_results)} results")
    return all_results

def get_batch_data(mal_ids: List[str], batch_size: int = 20) -> List[Dict]:
    """Batch processing optimized for 24h target"""
    all_results = []
    
    for i in range(0, len(mal_ids), batch_size):
        batch = mal_ids[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}: {len(batch)} manga")
        
        # Use parallel processing for each batch
        batch_results = get_full_data_parallel(batch, min(MAX_WORKERS, len(batch)))
        all_results.extend(batch_results)
        
        # Reduced inter-batch delay for 24h target
        if i + batch_size < len(mal_ids):
            batch_delay = random.uniform(4, 8)  # Reduced to 4-8s
            logger.info(f"Batch delay: {batch_delay:.1f}s")
            time.sleep(batch_delay)
    
    return all_results