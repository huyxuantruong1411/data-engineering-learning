# 📚 Manga Data Fetcher

## 1. Giới thiệu
**Manga Data Fetcher** là một dự án thu thập dữ liệu manga từ các website nổi tiếng.  
Mục tiêu là xây dựng một pipeline có thể lấy về thông tin chi tiết, review, recommendation và các metadata liên quan đến manga, lưu trữ vào MongoDB để phục vụ phân tích hoặc làm nguồn dữ liệu cho các ứng dụng khác.

Hiện tại, dự án chỉ **hỗ trợ ổn định** cho hai nguồn dữ liệu:
- ✅ [AniList](https://anilist.co/)  
- ✅ [MangaUpdates](https://www.mangaupdates.com/)

⚠️ Các nguồn khác như **MyAnimeList** và **Anime-Planet** đã được thử nghiệm nhưng chưa thể chạy ổn định trong pipeline này (lỗi HTTP, Cloudflare chặn).

---

## 2. Kiến trúc dự án
Cấu trúc thư mục chính:

```

project/
├── src/
│   ├── extractors/        # Bộ thu thập dữ liệu cho từng nguồn
│   ├── pipeline.py        # Pipeline chính
│   ├── pipeline\_conservative.py  # Phiên bản an toàn hơn
│   └── utils.py           # Hàm tiện ích
├── spiders/               # Scrapy spiders cho crawl dữ liệu chi tiết
├── run\_conservative.py    # Runner pipeline bảo thủ
├── clear\_collections.py   # Script dọn dữ liệu trong MongoDB
└── requirements.txt

````

**Thành phần chính:**
- **Extractors**: mỗi nguồn (anilist, mangaupdates, …) có fetcher riêng để gọi API/crawl.  
- **Pipeline**: gom dữ liệu từ nhiều nguồn, chuẩn hoá và lưu vào DB.  
- **MongoDB**: lưu dữ liệu thô từ mỗi nguồn dưới dạng collection riêng biệt.  
- **Spiders**: dùng Scrapy để lấy review/comment chi tiết (chủ yếu với MangaUpdates).  

---

## 3. Yêu cầu hệ thống
- Python **3.9+**  
- MongoDB (local hoặc remote)  
- Pipenv hoặc virtualenv để quản lý môi trường (khuyến nghị)  

---

## 4. Cài đặt
Clone repo và cài đặt dependencies:
```bash
git clone <repo-url>
cd 2025-08-18-fetch-mangadata-from-famous-websites
pip install -r requirements.txt
````

Đảm bảo MongoDB đang chạy ở địa chỉ mặc định:

```
mongodb://localhost:27017
```

---

## 5. Cách sử dụng

### 5.1 Chạy pipeline

Ví dụ: fetch dữ liệu từ **AniList** và **MangaUpdates** cho 10 manga đầu tiên:

```bash
python -m src.run --only anilist mangaupdates --limit 10
```

Hoặc chạy pipeline conservative (log chi tiết hơn, xử lý an toàn hơn):

```bash
python run_conservative.py --only anilist mangaupdates --limit 5 -v
```

### 5.2 Dọn dữ liệu test

Xoá toàn bộ collection thử nghiệm trong MongoDB:

```bash
python clear_collections.py
```

---

## 6. Dữ liệu lưu trữ trong MongoDB

Database mặc định: **`manga_raw_data`**

Các collection tương ứng với từng nguồn:

* `anilist_data`
* `mangaupdates_data`

### 6.1 Ví dụ cấu trúc document

```json
{
  "_id": "anilist_12345",
  "source": "anilist",
  "source_id": "12345",
  "source_url": "https://anilist.co/manga/12345",
  "fetched_at": "2025-08-23T01:23:45Z",
  "main": {
    "title": "Monster",
    "synopsis": "Dr. Tenma saved a boy's life...",
    "genres": ["Drama", "Thriller"]
  },
  "reviews": [
    {
      "user": "readerA",
      "rating": 9,
      "text": "One of the best thrillers I've read."
    }
  ],
  "recommendations": [
    {
      "slug": "20th-century-boys",
      "url": "https://anilist.co/manga/456"
    }
  ],
  "status": "ok",
  "http": { "code": 200 }
}
```

---

## 7. Query ví dụ trong MongoDB

### 7.1 Lấy tất cả review từ AniList

```js
db.anilist_data.find(
  { "reviews": { $exists: true, $ne: [] } },
  { "reviews": 1, "main.title": 1 }
).limit(5).pretty()
```

### 7.2 Đếm số manga có recommendation trong MangaUpdates

```js
db.mangaupdates_data.countDocuments(
  { "comments": { $exists: true, $ne: [] } }
)
```

---

## 8. Trạng thái nguồn dữ liệu

* ✅ AniList – hoạt động ổn định
* ✅ MangaUpdates – hoạt động ổn định
* ❌ MyAnimeList – chưa chạy được (HTTP error / `no_data`)
* ❌ Anime-Planet – chưa chạy được (Cloudflare 403 / block)

---

## 9. Roadmap

* [ ] Sửa MyAnimeList collector (reviews + recommendations)
* [ ] Hoàn thiện Anime-Planet fetcher (bypass Cloudflare)
* [ ] Chuẩn hoá schema dữ liệu các nguồn
* [ ] Viết unit tests cho pipeline
* [ ] Tích hợp thêm dashboard phân tích

---

## 10. Ghi chú

* Đây là phiên bản **alpha** dành cho thử nghiệm.
* Dữ liệu có thể thiếu hoặc không đồng nhất giữa các nguồn.
* Khi crawl nhiều dữ liệu, cần thêm cơ chế chống chặn (anti-blocking).

---

✍️ *Maintained by Hedi Snowy – 2025*