import scrapy
import re


class ReviewSpider(scrapy.Spider):
    name = "review_spider"

    def __init__(self, mal_id=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not mal_id:
            raise ValueError("mal_id is required. Example: -a mal_id=1")
        self.start_urls = [f"https://myanimelist.net/manga/{mal_id}/_/reviews"]

    def parse(self, response):
        reviews = response.css("div.review-element")

        for r in reviews:
            user = r.css("div.username a::text").get() or r.css("div.username::text").get()
            user = user.strip() if user else None

            score_text = r.css("div.rating span.num::text").get()
            score = int(score_text) if score_text and score_text.isdigit() else None

            date_text = r.css("div.update_at::text").get()
            date = date_text.strip() if date_text else None

            content_parts = r.css("div.text::text, div.text *::text").getall()
            content = " ".join([c.strip() for c in content_parts if c.strip()])

            tags = r.css("div.tags span.tag::text").getall()
            tags = [t.strip() for t in tags if t.strip()]

            yield {
                "user": user,
                "score": score,
                "content": content,
                "date": date,
                "tags": tags,
            }

        next_page = response.css("div.pagination a.next::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)