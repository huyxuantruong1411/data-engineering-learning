# src/common/anti_blocking.py
"""
Advanced anti-blocking utilities for web scraping.
Provides rotating proxies, user agents, intelligent delays, and session management.
"""

import os
import time
import random
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Extended user agent pool with real browser fingerprints
USER_AGENTS = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    
    # Chrome Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    
    # Firefox Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
    
    # Safari Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    
    # Edge Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    
    # Mobile Chrome
    "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
]

# Free proxy sources (you can extend this with paid proxy services)
FREE_PROXY_APIS = [
    "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
]

class ProxyRotator:
    """Manages proxy rotation with health checking."""
    
    def __init__(self):
        self.proxies: List[str] = []
        self.working_proxies: List[str] = []
        self.failed_proxies: set = set()
        self.last_refresh = None
        self.refresh_interval = timedelta(hours=1)
        
    def refresh_proxies(self):
        """Fetch fresh proxy list from free sources."""
        if self.last_refresh and datetime.now() - self.last_refresh < self.refresh_interval:
            return
            
        logger.info("Refreshing proxy list...")
        new_proxies = []
        
        for api_url in FREE_PROXY_APIS:
            try:
                resp = requests.get(api_url, timeout=10)
                if resp.status_code == 200:
                    proxies = resp.text.strip().split('\n')
                    new_proxies.extend([p.strip() for p in proxies if ':' in p])
            except Exception as e:
                logger.warning(f"Failed to fetch proxies from {api_url}: {e}")
                
        # Remove duplicates and failed proxies
        self.proxies = list(set(new_proxies) - self.failed_proxies)
        self.working_proxies = self.proxies.copy()
        self.last_refresh = datetime.now()
        logger.info(f"Loaded {len(self.proxies)} proxies")
        
    def get_proxy(self) -> Optional[Dict[str, str]]:
        """Get a working proxy, refresh list if needed."""
        if not self.working_proxies:
            self.refresh_proxies()
            
        if not self.working_proxies:
            return None
            
        proxy_addr = random.choice(self.working_proxies)
        return {
            "http": f"http://{proxy_addr}",
            "https": f"http://{proxy_addr}"
        }
        
    def mark_proxy_failed(self, proxy_dict: Dict[str, str]):
        """Mark a proxy as failed."""
        if proxy_dict and "http" in proxy_dict:
            proxy_addr = proxy_dict["http"].replace("http://", "")
            self.failed_proxies.add(proxy_addr)
            if proxy_addr in self.working_proxies:
                self.working_proxies.remove(proxy_addr)

class RequestManager:
    """Manages intelligent request timing and session handling."""
    
    def __init__(self, base_delay: float = 5.0, max_delay: float = 60.0):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.last_request_time = 0
        self.consecutive_failures = 0
        self.session = self._create_session()
        self.proxy_rotator = ProxyRotator()
        
    def _create_session(self) -> requests.Session:
        """Create session with retry strategy."""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
        
    def _calculate_delay(self) -> float:
        """Calculate intelligent delay based on recent failures."""
        base = self.base_delay
        
        # Exponential backoff for consecutive failures
        if self.consecutive_failures > 0:
            base *= (2 ** min(self.consecutive_failures, 5))
            
        # Add random jitter (±25%)
        jitter = random.uniform(0.75, 1.25)
        delay = min(base * jitter, self.max_delay)
        
        # Add extra random delay (0-10 seconds)
        delay += random.uniform(0, 10)
        
        return delay
        
    def _get_headers(self) -> Dict[str, str]:
        """Generate realistic headers with random user agent."""
        ua = random.choice(USER_AGENTS)
        
        # Common browser headers that match the user agent
        headers = {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Cache-Control": "max-age=0",
        }
        
        # Randomly add some optional headers
        if random.random() < 0.5:
            headers["Referer"] = random.choice([
                "https://www.google.com/",
                "https://www.bing.com/",
                "https://duckduckgo.com/",
                "https://www.anime-planet.com/"
            ])
            
        return headers
        
    def make_request(self, url: str, use_proxy: bool = True, max_retries: int = 3) -> Optional[requests.Response]:
        """Make a request with intelligent timing and anti-blocking measures."""
        
        # Wait for appropriate delay
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        delay = self._calculate_delay()
        
        if time_since_last < delay:
            sleep_time = delay - time_since_last
            logger.info(f"Waiting {sleep_time:.1f}s before next request...")
            time.sleep(sleep_time)
            
        self.last_request_time = time.time()
        
        for attempt in range(max_retries):
            try:
                headers = self._get_headers()
                proxies = None
                
                if use_proxy and random.random() < 0.7:  # Use proxy 70% of the time
                    proxies = self.proxy_rotator.get_proxy()
                    if proxies:
                        logger.debug(f"Using proxy: {proxies['http']}")
                
                resp = self.session.get(
                    url, 
                    headers=headers, 
                    proxies=proxies,
                    timeout=30,
                    allow_redirects=True
                )
                
                if resp.status_code == 200:
                    self.consecutive_failures = 0
                    logger.info(f"✓ Success: {url} (status: {resp.status_code})")
                    return resp
                    
                elif resp.status_code == 403:
                    logger.warning(f"✗ Blocked (403): {url} - attempt {attempt + 1}")
                    if proxies:
                        self.proxy_rotator.mark_proxy_failed(proxies)
                    self.consecutive_failures += 1
                    
                    # Longer delay after 403
                    if attempt < max_retries - 1:
                        backoff_delay = (2 ** attempt) * 10 + random.uniform(5, 15)
                        logger.info(f"Backing off for {backoff_delay:.1f}s after 403...")
                        time.sleep(backoff_delay)
                        
                elif resp.status_code == 429:
                    logger.warning(f"✗ Rate limited (429): {url}")
                    self.consecutive_failures += 1
                    
                    # Extract retry-after header if present
                    retry_after = resp.headers.get('Retry-After')
                    if retry_after:
                        try:
                            delay = int(retry_after) + random.uniform(5, 15)
                        except ValueError:
                            delay = 60 + random.uniform(10, 30)
                    else:
                        delay = 60 + random.uniform(10, 30)
                        
                    if attempt < max_retries - 1:
                        logger.info(f"Rate limited, waiting {delay:.1f}s...")
                        time.sleep(delay)
                        
                else:
                    logger.warning(f"✗ HTTP {resp.status_code}: {url}")
                    self.consecutive_failures += 1
                    
            except requests.exceptions.ProxyError as e:
                logger.warning(f"Proxy error: {e}")
                if proxies:
                    self.proxy_rotator.mark_proxy_failed(proxies)
                    
            except requests.exceptions.Timeout as e:
                logger.warning(f"Timeout: {e}")
                
            except Exception as e:
                logger.error(f"Request error: {e}")
                
            # Wait before retry
            if attempt < max_retries - 1:
                retry_delay = (2 ** attempt) * 5 + random.uniform(2, 8)
                time.sleep(retry_delay)
                
        self.consecutive_failures += 1
        logger.error(f"✗ Failed all {max_retries} attempts for {url}")
        return None

# Global instance
request_manager = RequestManager()

def get_request_manager() -> RequestManager:
    """Get the global request manager instance."""
    return request_manager

def smart_delay(min_delay: float = 5.0, max_delay: float = 30.0):
    """Add intelligent random delay between requests."""
    delay = random.uniform(min_delay, max_delay)
    
    # Add extra delay during peak hours (assuming UTC)
    current_hour = datetime.utcnow().hour
    if 8 <= current_hour <= 22:  # Peak hours
        delay *= random.uniform(1.2, 1.8)
        
    logger.debug(f"Smart delay: {delay:.1f}s")
    time.sleep(delay)

def get_random_headers() -> Dict[str, str]:
    """Generate realistic browser headers."""
    ua = random.choice(USER_AGENTS)
    
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice([
            "en-US,en;q=0.9",
            "en-US,en;q=0.9,vi;q=0.8",
            "en-GB,en;q=0.9",
        ]),
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
    }
    
    # Randomly add referer
    if random.random() < 0.6:
        headers["Referer"] = random.choice([
            "https://www.google.com/",
            "https://www.bing.com/",
            "https://duckduckgo.com/",
            "https://www.anime-planet.com/",
            "https://myanimelist.net/",
        ])
        
    return headers
