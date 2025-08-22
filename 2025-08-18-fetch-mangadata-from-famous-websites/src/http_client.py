import requests
from tenacity import retry, wait_fixed, stop_after_attempt


class HttpError(Exception):
    pass


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0 Safari/537.36"
}


@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
def http_get(url, params=None, headers=None, allow_statuses=None, allow_404=False):
    hdrs = HEADERS.copy()
    if headers:
        hdrs.update(headers)

    resp = requests.get(url, params=params, headers=hdrs, timeout=15)

    # Cho phép các status code đặc biệt
    if allow_statuses and resp.status_code in allow_statuses:
        return resp
    if allow_404 and resp.status_code == 404:
        return resp

    if resp.status_code != 200:
        raise HttpError(f"GET {url} -> {resp.status_code}")

    return resp


@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
def http_post(url, json=None, data=None, headers=None, allow_statuses=None, allow_404=False):
    hdrs = HEADERS.copy()
    if headers:
        hdrs.update(headers)

    resp = requests.post(url, json=json, data=data, headers=hdrs, timeout=15)

    if allow_statuses and resp.status_code in allow_statuses:
        return resp
    if allow_404 and resp.status_code == 404:
        return resp

    if resp.status_code != 200:
        raise HttpError(f"POST {url} -> {resp.status_code}")

    return resp