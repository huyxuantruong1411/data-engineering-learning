# 📘 Manga Data Pipeline (MongoDB → dbt → BigQuery)

## 📌 Giới thiệu

Project này xây dựng một **data pipeline** để trích xuất dữ liệu Manga từ **MongoDB** (database `manga_raw_data`), chuẩn hoá sang mô hình **star schema/snowflake schema**, sau đó nạp vào **Google BigQuery** thông qua **dbt**.

Mục tiêu:

* Lấy dữ liệu gốc dạng **document JSON** từ MongoDB.
* Chuyển đổi sang **các bảng dimension, fact, bridge** (dạng quan hệ).
* Dễ dàng mở rộng phân tích với dbt + BigQuery (BI, dashboard, analytics).

---

## 🗂️ Kiến trúc tổng quan

```
MongoDB (manga_raw_data)
        │
        ▼
[Extract] mongo_to_dbt_optimized.py
        │ (xuất CSV seeds)
        ▼
dbt seeds  ──> dbt models ──> BigQuery (dataset: manga_data)
```

### 1. Dữ liệu gốc trong MongoDB

Database: **`manga_raw_data`**, gồm **8 collections**:

* `mangadex_manga` → Thông tin manga
* `mangadex_creators` → Tác giả, hoạ sĩ
* `mangadex_cover_arts` → Bìa truyện
* `mangadex_related` → Quan hệ giữa các manga
* `mangadex_tags` → Thẻ (genre, theme, format…)
* `mangadex_statistics` → Thống kê (follows, rating, comments…)
* `mangadex_chapters` → Chapter
* `mangadex_groups` → Nhóm dịch

### 2. Chuẩn hoá thành bảng quan hệ

Script **`mongo_to_dbt_optimized.py`** ép phẳng dữ liệu JSON sang CSV cho dbt seeds:

* **Dimension tables**

  * `dim_manga.csv`
  * `dim_creator.csv`
  * `dim_group.csv`
  * `dim_tag.csv`

* **Bridge tables**

  * `bridge_manga_creator.csv`
  * `bridge_manga_tag.csv`
  * `bridge_manga_cover.csv`
  * `bridge_manga_related.csv`
  * `bridge_creator_biography.csv`
  * `bridge_group_altname.csv`
  * `bridge_group_language.csv`
  * `bridge_chapter_group.csv`

* **Fact tables**

  * `fact_chapters.csv`
  * `fact_statistics.csv`
  * `fact_manga_trends.csv`

---

## ⚙️ Hướng dẫn chạy

### 1. Chuẩn bị môi trường

```bash
pip install -r requirements.txt
```

Yêu cầu:

* Python 3.9+
* MongoDB đang chạy tại `mongodb://localhost:27017/`
* dbt-core + dbt-bigquery

### 2. Extract dữ liệu từ MongoDB sang CSV

```bash
cd Scripts
python mongo_to_dbt_optimized.py \
    --mongo-uri "mongodb://localhost:27017/" \
    --db "manga_raw_data" \
    --seed-dir "../mongo_to_db/seeds" \
    --max-threads 4
```

Kết quả: CSV seed files được sinh trong thư mục `mongo_to_db/seeds/`.

### 3. Load dữ liệu vào dbt + BigQuery

```bash
cd ../mongo_to_db
dbt seed
dbt run
```

Kết quả:

* Các bảng dimension/bridge/fact được nạp vào **dataset `manga_data` trên BigQuery**.
* Ví dụ model: `fact_manga_popularity` (86.9k rows, \~215 MiB processed).

---

## 📊 Mô hình dữ liệu

Mô hình star schema (đơn giản hoá):

```
             dim_creator
                 ▲
                 │
 dim_tag ◄── bridge_manga_tag ──► dim_manga ◄── bridge_manga_creator ──► dim_creator
                 │                        │
                 │                        │
             fact_statistics           fact_chapters
                 │                        │
                 ▼                        ▼
            fact_manga_trends       bridge_chapter_group ──► dim_group
```

---

## 🚀 Kế hoạch mở rộng

* Thêm snapshot dbt để theo dõi lịch sử thay đổi.
* Tạo mart models cho phân tích chuyên sâu (ví dụ: top trending manga theo thời gian).
* Kết hợp với BI tool (Looker Studio / Metabase) để trực quan hoá.