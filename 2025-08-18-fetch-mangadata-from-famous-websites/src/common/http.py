import aiohttp, asyncio
from aiolimiter import AsyncLimiter
from tenacity import retry, stop_after_attempt, wait_exponential

class HttpClient:
    def __init__(self, rate_per_sec=2, headers=None, proxy=None):
        self.limiter = AsyncLimiter(rate_per_sec, 1)
        self.session = None
        self.headers = headers or {}
        self.proxy = proxy

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.headers, trust_env=True)
        return self

    async def __aexit__(self, *args):
        await self.session.close()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
    async def get_json(self, url, **kw):
        async with self.limiter:
            async with self.session.get(url, proxy=self.proxy, **kw) as r:
                txt = await r.text()
                return r.status, txt, (await r.json(content_type=None))

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
    async def post_json(self, url, json=None, **kw):
        async with self.limiter:
            async with self.session.post(url, json=json, proxy=self.proxy, **kw) as r:
                txt = await r.text()
                return r.status, txt, (await r.json(content_type=None))