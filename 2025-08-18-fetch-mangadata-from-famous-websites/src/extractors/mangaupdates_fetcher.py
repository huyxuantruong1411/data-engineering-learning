import logging
from datetime import datetime
from typing import Dict, List
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

MU_BASE = "https://www.mangaupdates.com"


def _parse_reviews(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    reviews = []
    for div in soup.select(".sMemberComment, .commentText"):
        text = div.get_text(" ", strip=True)
        if text:
            reviews.append({"text": text})
    return reviews


def _parse_recommendations(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    recs = []
    for a in soup.select("a[href*='/series/']"):
        href = a.get("href", "")
        if "/series/" not in href:
            continue
        sid = href.split("/series/")[-1].split("?")[0]
        recs.append({"id": sid, "url": href})
    return recs


def get_full_data(mu_id: str) -> Dict:
    payload = {
        "_id": f"mu_{mu_id}",
        "source": "mangaupdates",
        "source_id": mu_id,
        "source_url": f"{MU_BASE}/series/{mu_id}",
        "fetched_at": datetime.utcnow().isoformat(),
    }
    try:
        r1 = requests.get(f"{MU_BASE}/series.html?id={mu_id}", timeout=20)
        payload["recommendations"] = _parse_recommendations(r1.text) if r1.ok else []
        payload["reviews"] = _parse_reviews(r1.text) if r1.ok else []
        payload["status"] = "ok" if (payload["reviews"] or payload["recommendations"]) else "no_reviews"
        payload["http"] = {"code": 200}
    except Exception as e:
        logger.error("MangaUpdates fetch failed: %s", e, exc_info=True)
        payload.update({"recommendations": [], "reviews": [], "status": "error", "http": {"error": str(e)}})

    return payload