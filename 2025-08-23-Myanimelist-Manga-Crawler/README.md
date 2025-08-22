# 📚 MyAnimeList Manga Crawler

## 1. Giới thiệu
**MyAnimeList Manga Crawler** là một công cụ thu thập dữ liệu manga từ [MyAnimeList](https://myanimelist.net/manga) thông qua API [Jikan](https://jikan.moe/).  
Dự án được xây dựng bằng Python (async/await, `aiohttp`) với mục tiêu tải về **metadata, reviews, recommendations** của toàn bộ manga trên MAL, sau đó lưu vào **MongoDB** để phục vụ nghiên cứu, phân tích hoặc xây dựng ứng dụng.

---

## 2. Tính năng chính
- ✅ Thu thập **metadata đầy đủ** cho từng manga (title, synopsis, genres, …).  
- ✅ Thu thập **recommendations** (manga liên quan) từ MAL.  
- ✅ Thu thập **reviews** với giới hạn số trang tùy chọn.  
- ✅ Cơ chế **retry/backoff** khi gặp lỗi mạng, HTTP 429 (rate limit), hoặc server 5xx.  
- ✅ **Auto-resume**: có thể chạy liên tục, tự động tiếp tục từ manga_id cuối cùng.  
- ✅ **Lưu MongoDB** với upsert theo `manga_id`.  

---

## 3. Cấu trúc dự án
```

2025-08-23-Myanimelist-Manga-Crawler/
├── mal\_crawler.py        # Script crawler chính
├── dump.py               # Xuất cấu trúc + code project ra file project\_dump.txt
└── jikan-docker/         # Thư mục chứa Jikan self-host (optional)

````

- **mal_crawler.py**: entrypoint chính, chạy crawler async.  
- **dump.py**: tiện ích dump code ra file duy nhất.  
- **jikan-docker/**: clone Jikan API (self-hosted) để tránh rate-limit khi crawl lớn.  

---

## 4. Yêu cầu hệ thống
- Python **3.9+**  
- MongoDB (local hoặc remote)  
- (Khuyến nghị) Self-host **Jikan API** để đạt hiệu suất cao, giảm giới hạn rate-limit từ API công khai.  

Cài đặt thư viện:
```bash
pip install aiohttp pymongo loguru
````

---

## 5. Cách sử dụng

### 5.1 Chạy crawler tự động (auto mode)

Mặc định crawler sẽ tự chạy từ `manga_id=1` cho đến khi đạt 78,000 manga (target mặc định):

```bash
python mal_crawler.py
```

Có thể tùy chỉnh tham số:

```bash
python mal_crawler.py \
  --concurrency 120 \
  --reviews-pages 5 \
  --target 78000 \
  --resume-existing \
  --base-url http://localhost:8080/v4
```

### 5.2 Chạy crawler trong một khoảng ID cụ thể

Ví dụ: crawl từ manga\_id **1000 → 1050**:

```bash
python mal_crawler.py --start 1000 --end 1050
```

---

## 6. Dữ liệu trong MongoDB

Kết nối mặc định:

```
mongodb://localhost:27017
```

Database: **`manga_raw_data`**
Collection: **`mal_data`**

### 6.1 Ví dụ document

```json
{
  "_id": "mal_123",
  "manga_id": 123,
  "source": "mal_jikan",
  "source_url": "https://myanimelist.net/manga/123",
  "fetched_at": "2025-08-23T01:23:45Z",
  "metadata": { ... },
  "recommendations": [
    { "rec_manga_id": "456", "title": "Related Manga", ... }
  ],
  "reviews": [
    { "user": "readerA", "rating": 8, "content": "Very good story..." }
  ],
  "status": "ok",
  "http": {
    "metadata": 200,
    "recommendations": 200,
    "reviews": 200
  }
}
```

### 6.2 Trạng thái

* `status = ok` → lấy đủ dữ liệu
* `status = partial_error` → có dữ liệu nhưng thiếu một phần (metadata/reviews/recs)
* `status = no_data` → không tìm thấy dữ liệu hợp lệ

---

## 7. Ví dụ query MongoDB

### 7.1 Đếm số manga đã crawl thành công

```js
db.mal_data.countDocuments({ status: "ok" })
```

### 7.2 Lấy top 5 manga có nhiều reviews nhất

```js
db.mal_data.aggregate([
  { $project: { title: "$metadata.title", review_count: { $size: "$reviews" } } },
  { $sort: { review_count: -1 } },
  { $limit: 5 }
])
```

### 7.3 Tìm manga có recommendation dẫn đến manga\_id=20

```js
db.mal_data.find(
  { "recommendations.rec_manga_id": "20" },
  { "metadata.title": 1, "recommendations": 1 }
).pretty()
```

---

## 8. Roadmap

* [ ] Cải thiện crawl **reviews** toàn bộ thay vì giới hạn trang.
* [ ] Parallel pipeline để vừa crawl vừa normalize dữ liệu.
* [ ] Hỗ trợ **retry thông minh** dựa trên header từ Jikan.
* [ ] Xuất dữ liệu sang định dạng Parquet/JSONL để dùng với Spark.

---

## 9. Ghi chú

* API Jikan công khai bị giới hạn tốc độ → khuyến nghị **chạy Jikan Docker self-host** để tăng concurrency.
* Khi crawl số lượng lớn (50k+), cần tăng concurrency + giới hạn trang reviews hợp lý.

---

✍️ *Maintained by Hedi Snowy – 2025*