Rõ rồi 👍
Mình sẽ đặt toàn bộ nội dung **README.md** gói gọn trong một code block để bạn copy nguyên si về project.

---

````markdown
# Fetch Manga Data from YouTube

This project is part of the **Data Engineering Learning Path**, focused on building a pipeline to fetch and store YouTube video metadata related to manga titles from MangaDex.

## 📌 Features
- Connects to **MongoDB** and reads manga titles (main + alt titles, in English and Vietnamese).
- For each title, constructs a YouTube search query (`<title> manga`).
- Scrapes YouTube search results (via HTML + `ytInitialData`).
- Extracts video metadata (video id, title, channel, view count, etc.).
- Filters:
  - Minimum **1,000 views**
  - Language detection → only **English** and **Vietnamese**
- Upserts results into MongoDB (`youtube_videos` collection).
- Supports **parallel scraping** with thread pool + jitter/random sleep.
- Logs progress **per manga** (e.g., `Manga 5/50 done, upserted 3 videos`).
- Retry strategy with exponential backoff + optional long sleep (5–15 mins) when blocked by YouTube.

---

## 🛠 Requirements

### Python
- Python **3.10+** (tested with 3.13)

### Python Libraries
Install with:
```bash
pip install -r requirements.txt
````

`requirements.txt`:

```txt
pymongo==4.10.1
requests==2.32.3
regex==2024.5.15
tenacity==9.0.0
```

---

## ⚙️ Configuration

* **MongoDB connection**:
  Default URI: `mongodb://localhost:27017/`
  Database: `manga_raw_data`
  Collections:

  * Input: `mangadex_manga`
  * Output: `youtube_videos`

* **User Agents**:
  The script rotates among a list of 30+ modern browsers (Chrome, Firefox, Edge, Safari, Opera, Android, iOS).

* **Filters**:

  * Only videos with **≥ 1000 views**
  * Only languages: `en`, `vi`

---

## ▶️ Usage

Run the script:

```bash
python fetch_youtube.py --limit 50 --workers 20
```

### Options:

* `--limit`: Number of manga to process (default = all manga in collection).
* `--workers`: Number of parallel threads (default = 5).

### Example Output

```
[START] Processing 50 manga (limit=50, workers=20)
[PROGRESS] Manga 1/50 done, upserted 3 videos (manga_id=abcd1234)
[PROGRESS] Manga 2/50 done, upserted 1 video  (manga_id=efgh5678)
...
[DONE] All manga processed.
```

---

## 📂 Project Structure

```
2025-08-25-fetch-mangadata-from-youtube/
│── fetch_youtube.py       # Main script
│── requirements.txt       # Python dependencies
│── README.md              # Project documentation
```