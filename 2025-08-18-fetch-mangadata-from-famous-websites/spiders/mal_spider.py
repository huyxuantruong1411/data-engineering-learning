import scrapy
from scrapy.http import Request
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class MalSpider(scrapy.Spider):
    name = "mal_spider"
    allowed_domains = ["myanimelist.net"]
    custom_settings = {
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 4,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    def __init__(self, mal_id=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mal_id = mal_id
        self.start_urls = [f"https://myanimelist.net/manga/{mal_id}"]
        self.payload = {
            "_id": f"mal_{mal_id}",
            "source": "mal",
            "source_id": mal_id,
            "source_url": f"https://myanimelist.net/manga/{mal_id}",
            "fetched_at": datetime.utcnow().isoformat(),
            "manga_info": {},
            "recommendations": [],
            "reviews": [],
            "status": "ok",
            "http": {"code": 200},
        }

    def parse(self, response):
        # Parse manga info
        info = {}
        info['jpName'] = response.xpath('//span[contains(text(), "Japanese:")]/following::text()').get(default='').strip()
        info['engName'] = response.xpath('//span[contains(text(), "English:")]/following::text()').get(default='').strip()
        info['synonyms'] = response.xpath('//span[contains(text(), "Synonyms:")]/following::text()').get(default='').strip()
        info['type'] = response.xpath('//span[text()="Type:"]/following-sibling::a/text()').get(default='')
        info['volumes'] = response.xpath('//span[text()="Volumes:"]/following::text()').get(default='').strip()
        info['chapters'] = response.xpath('//span[text()="Chapters:"]/following::text()').get(default='').strip()
        info['status'] = response.xpath('//span[text()="Status:"]/following::text()').get(default='').strip()
        info['published'] = response.xpath('//span[text()="Published:"]/following::text()').get(default='').strip()
        info['genres'] = ', '.join(response.xpath('//span[text()="Genres:"]/following-sibling::a/text()').getall())
        info['themes'] = ', '.join(response.xpath('//span[text()="Themes:"]/following-sibling::a/text()').getall())
        info['demographic'] = response.xpath('//span[text()="Demographic:"]/following-sibling::a/text()').get(default='')
        info['serialization'] = ', '.join(response.xpath('//span[text()="Serialization:"]/following-sibling::a/text()').getall())
        info['authors'] = ', '.join(response.xpath('//span[text()="Authors:"]/following-sibling::a/text()').getall())
        info['score'] = response.css('span.score-label::text').get(default='')
        info['ranked'] = response.xpath('//span[text()="Ranked:"]/following::text()').get(default='').strip()
        info['popularity'] = response.xpath('//span[text()="Popularity:"]/following::text()').get(default='').strip()
        info['members'] = response.xpath('//span[text()="Members:"]/following::text()').get(default='').strip()
        info['favorites'] = response.xpath('//span[text()="Favorites:"]/following::text()').get(default='').strip()
        info['cover_image'] = response.css('div.leftside img.lazyload::attr(src)').get(default=response.css('div.leftside img.lazyload::attr(data-src)').get(default=''))
        info['synopsis'] = response.xpath('//span[@itemprop="description"]/text()').get(default='').strip()
        
        self.payload["manga_info"] = info
        logger.info(f"Parsed manga info for {self.mal_id}: {bool(info)}")
        
        # Fetch recommendations
        yield Request(
            f"https://myanimelist.net/manga/{self.mal_id}/_/userrecs",
            callback=self.parse_recommendations,
            meta={'mal_id': self.mal_id}
        )
        
        # Fetch reviews
        yield Request(
            f"https://myanimelist.net/manga/{self.mal_id}/reviews?p=1",
            callback=self.parse_reviews,
            meta={'mal_id': self.mal_id, 'page': 1}
        )

    def parse_recommendations(self, response):
        recs = []
        for a in response.css('div.borderClass a[href*="/manga/"]'):
            href = a.attrib.get('href', '')
            title = a.css('::text').get(default='').strip()
            if "/manga/" not in href or not title or len(title) < 2:
                continue
            try:
                mid = href.split("/manga/")[1].split("/")[0]
                if mid.isdigit():
                    reason = a.xpath('following-sibling::div/text()').get(default='').strip()[:200]
                    recs.append({
                        "id": mid,
                        "title": title,
                        "url": href,
                        "reason": reason
                    })
            except Exception:
                continue
        
        seen = set()
        unique_recs = [rec for rec in recs if rec["id"] not in seen and not seen.add(rec["id"])]
        self.payload["recommendations"] = unique_recs[:20]
        logger.info(f"Parsed {len(unique_recs)} recommendations for {self.mal_id}")

    def parse_reviews(self, response):
        mal_id = response.meta['mal_id']
        page = response.meta['page']
        
        reviews = []
        for review in response.css('div.review-element, div.review-element.js-review-element, div.borderDark'):
            try:
                review_id = review.css('div.open a::attr(href), a[href*="/reviews/"]::attr(href)').get(default='').split('/')[-1]
                if not review_id:
                    logger.debug(f"No review ID found for {mal_id} on page {page}")
                    continue
                
                review_text = ' '.join(review.css('div.text, div.review-body').get(default='').strip().split())
                if not review_text or len(review_text) < 5:
                    logger.debug(f"Skipping short review for {mal_id}: {review_text[:50]}...")
                    continue
                
                reactions_dict = review.attrib.get('data-reactions', '')
                reactions = {}
                if reactions_dict:
                    try:
                        import json
                        reactions_data = json.loads(reactions_dict)
                        reaction_type_map = ['nice', 'loveIt', 'funny', 'confusing', 'informative', 'wellWritten', 'creative']
                        reactions = {r: c for r, c in zip(reaction_type_map, reactions_data.get('count', ['0']*7))}
                    except:
                        logger.debug(f"Error parsing reactions for {mal_id}: {reactions_dict}")
                
                author = review.css('div.username a::text, div.reviewer a::text').get(default='').strip()
                score = review.css('div.rating span.num::text, div.score::text').get(default='').strip()
                post_time = review.css('div.update_at::text, div.date::text').get(default='').strip()
                episodes_seen = review.css('.tag.preliminary span::text, div.episodes-seen::text').get(default='').strip()
                recommendation_status = review.css('.tag.recommended::text, .tag.recommendation::text').get(default='').strip()
                profile_url = review.css('div.thumb a::attr(href), div.reviewer a::attr(href)').get(default='')
                profile_img = review.css('div.thumb a img::attr(src), div.reviewer img::attr(src)').get(default='')
                
                reviews.append({
                    'reviewId': review_id,
                    'text': review_text[:3000],
                    'author': author,
                    'score': score,
                    'postTime': post_time,
                    'episodesSeen': episodes_seen,
                    'recommendationStatus': recommendation_status,
                    'profileUrl': profile_url,
                    'profileImage': profile_img,
                    **reactions
                })
            except Exception as e:
                logger.debug(f"Error parsing review for {mal_id}: {e}")
                continue
        
        self.payload["reviews"].extend(reviews)
        logger.info(f"Parsed {len(reviews)} reviews for {mal_id} on page {page}")
        
        # Check for next page
        next_page_url = response.css('a[href*="reviews?p="]:not([href*="p=1"])::attr(href), div.mt4 a[href*="reviews?p="]::attr(href)').get()
        if next_page_url:
            if not next_page_url.startswith('http'):
                next_page_url = f"https://myanimelist.net{next_page_url}"
            logger.debug(f"Found next page for {mal_id}: {next_page_url}")
            yield Request(
                next_page_url,
                callback=self.parse_reviews,
                meta={'mal_id': mal_id, 'page': page + 1}
            )
        else:
            logger.info(f"No next page for reviews of {mal_id} after page {page}")
            yield self.payload

    def closed(self, reason):
        logger.info(f"Spider closed for {self.mal_id}: {reason}")