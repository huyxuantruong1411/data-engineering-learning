import time
import requests
from datetime import datetime

BASE_URL = "https://api.mangadex.org/manga"
RESULTS = []

MIN_DELAY = 0.25
delay = MIN_DELAY
MAX_RETRIES = 5

def fetch_all_manga():
    global delay
    limit = 100
    created_at_since = "1900-01-01T00:00:00"  # mốc thời gian rất sớm để lấy từ đầu

    while True:
        params = {
            "limit": limit,
            "order[createdAt]": "asc",
            "createdAtSince": created_at_since,
            "includes[]": ["author", "artist", "cover_art"]
        }

        retries = 0
        success = False

        while not success and retries < MAX_RETRIES:
            try:
                resp = requests.get(BASE_URL, params=params, timeout=30)

                if resp.status_code == 200:
                    batch = resp.json()
                    data_list = batch.get("data", [])

                    if not data_list:
                        print("🏁 Hoàn tất!")
                        return RESULTS

                    for manga in data_list:
                        RESULTS.append(manga)

                    # lấy thời điểm createdAt của manga cuối
                    last_created = data_list[-1]["attributes"]["createdAt"]
                    # để tránh bị trùng, cộng thêm 1 giây
                    created_at_since = _add_one_second(last_created)

                    print(f"✅ Lấy {len(data_list)} manga, tổng cộng: {len(RESULTS)}")
                    delay = max(MIN_DELAY, delay * 0.9)
                    success = True

                else:
                    raise Exception(f"HTTP {resp.status_code}")

            except Exception as e:
                retries += 1
                delay *= 2
                print(f"⚠️ Lỗi: {e}. Retry {retries}/{MAX_RETRIES}, delay={delay:.2f}s")
                time.sleep(delay)

        if not success:
            print(f"❌ Dừng vì lỗi liên tục tại mốc {created_at_since}")
            break

        time.sleep(delay)

def _add_one_second(timestr):
    """Tăng thêm 1 giây cho mốc thời gian ISO8601"""
    dt = datetime.fromisoformat(timestr.replace("Z", "+00:00"))
    dt_plus = dt.timestamp() + 1
    return datetime.utcfromtimestamp(dt_plus).strftime("%Y-%m-%dT%H:%M:%S")

if __name__ == "__main__":
    all_manga = fetch_all_manga()
    print(f"Tổng số manga đã thu: {len(all_manga)}")
