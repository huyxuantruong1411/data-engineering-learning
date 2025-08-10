# MangaDex Data Fetcher Script

Script Python để tải toàn bộ dữ liệu liên quan đến manga từ MangaDex API, sử dụng database MongoDB có sẵn làm fact table chính.

## Tính năng chính

✅ **Sử dụng fact table có sẵn**: Đọc manga IDs từ collection `mangadex_manga`  
✅ **Tải dữ liệu bổ sung**: Tags, statistics, chapters, cover art, authors/artists, scanlation groups  
✅ **Batching thông minh**: Tự động điều chỉnh batch size dựa trên response từ API  
✅ **Retry với exponential backoff**: Xử lý lỗi mạng và rate limit một cách thông minh  
✅ **Progress tracking**: Có thể tiếp tục từ giữa nếu bị gián đoạn  
✅ **Tối ưu hóa requests**: Tận dụng relationships đã có để lấy ID liên quan  
✅ **Không duplicate**: Bỏ qua dữ liệu đã tồn tại trong database  
✅ **Adaptive speed**: Tự động điều chỉnh tốc độ dựa trên API response  

## Cấu trúc Database

### Collections được tạo tự động:
- `mangadex_tags` - Thông tin tags
- `mangadex_statistics` - Thống kê manga (với timestamp)
- `mangadex_chapters` - Metadata chapters
- `mangadex_cover_arts` - Cover art
- `mangadex_creators` - Authors và artists
- `mangadex_groups` - Scanlation groups
- `mangadex_related` - Manga liên quan

### Collection gốc (cần có sẵn):
- `mangadex_manga` - Fact table chính chứa manga IDs

## Cài đặt

1. **Cài đặt dependencies:**
```bash
pip install pymongo requests
```

2. **Đảm bảo MongoDB đang chạy** tại `localhost:27017`

3. **Kiểm tra database** `manga_raw_data` có collection `mangadex_manga`

## Cách sử dụng

### Chạy toàn bộ quá trình:
```bash
python fetch_all_related_data_v2.py
```

### Chạy từng phase riêng biệt:
```bash
# Chỉ lấy tags
python fetch_all_related_data_v2.py --phase tags

# Chỉ lấy statistics
python fetch_all_related_data_v2.py --phase statistics

# Chỉ lấy chapters
python fetch_all_related_data_v2.py --phase chapters

# Chỉ lấy covers, creators, groups
python fetch_all_related_data_v2.py --phase covers

# Chỉ lấy related manga
python fetch_all_related_data_v2.py --phase related
```

### Reset progress và chạy lại từ đầu:
```bash
python fetch_all_related_data_v2.py --reset-progress
```

## Cơ chế hoạt động

### 1. Progress Tracking
- Script tự động lưu tiến độ vào file `manga_progress.json`
- Có thể tiếp tục từ giữa nếu bị gián đoạn
- Mỗi phase có trạng thái `completed` và `last_processed`

### 2. Adaptive Batching
- **Statistics**: Bắt đầu với batch size 100, tự động giảm nếu gặp lỗi 400
- **Chapters**: Xử lý từng manga một, pagination tự động
- **Covers/Creators/Groups**: Thu thập tất cả IDs từ relationships trước, sau đó fetch

### 3. Intelligent Retry
- Exponential backoff khi gặp lỗi
- Tự động tăng delay khi rate limited (HTTP 429)
- Giảm delay khi thành công liên tục
- Tối đa 5 lần retry cho mỗi request

### 4. Speed Optimization
- **SPAM mode**: Không delay khi API response tốt
- **Adaptive mode**: Tự động tăng delay khi gặp vấn đề
- **Burst threshold**: Cần 10 request thành công liên tiếp để quay về SPAM mode

## Logging

Script tạo 2 loại log:
- **Console output**: Hiển thị tiến độ real-time
- **File log**: `mangadex_fetch.log` - Lưu tất cả thông tin chi tiết

## Xử lý lỗi

### Rate Limiting
- Tự động phát hiện HTTP 429
- Tăng delay và retry
- Không dừng script

### Network Errors
- Timeout 30 giây cho mỗi request
- Exponential backoff
- Tiếp tục với batch tiếp theo

### Invalid Data
- Validate manga IDs
- Skip manga không hợp lệ
- Log warning cho data có vấn đề

## Monitoring và Debug

### Progress File
File `manga_progress.json` chứa:
```json
{
  "tags": {"completed": false, "last_processed": null},
  "statistics": {"completed": false, "last_processed": null, "batch_size": 100},
  "chapters": {"completed": false, "last_processed": null},
  "cover_arts": {"completed": false, "last_processed": null},
  "creators": {"completed": false, "last_processed": null},
  "groups": {"completed": false, "last_processed": null},
  "related": {"completed": false, "last_processed": null}
}
```

### Log Levels
- **INFO**: Tiến độ bình thường
- **WARNING**: Vấn đề nhỏ, script vẫn tiếp tục
- **ERROR**: Lỗi nghiêm trọng, cần can thiệp

## Tùy chỉnh

### Thay đổi cấu hình:
```python
# Trong file script
MIN_DELAY = 0.1          # Delay tối thiểu (giây)
MAX_DELAY = 5.0          # Delay tối đa (giây)
MAX_RETRIES = 5          # Số lần retry tối đa
INITIAL_BATCH_SIZE = 100 # Batch size ban đầu cho statistics
```

### Thay đổi MongoDB connection:
```python
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "manga_raw_data"
MANGA_COLLECTION = "mangadex_manga"
```

## Lưu ý quan trọng

1. **Đảm bảo có đủ disk space** - Dữ liệu manga có thể rất lớn
2. **Kiểm tra MongoDB connection** trước khi chạy
3. **Backup database** nếu cần thiết
4. **Script có thể chạy rất lâu** tùy thuộc vào số lượng manga
5. **Có thể dừng và tiếp tục** bất cứ lúc nào với Ctrl+C

## Troubleshooting

### Lỗi "No manga found in database"
- Kiểm tra collection `mangadex_manga` có tồn tại không
- Kiểm tra database name có đúng không

### Lỗi MongoDB connection
- Kiểm tra MongoDB service có đang chạy không
- Kiểm tra connection string

### Lỗi rate limit liên tục
- Tăng `MIN_DELAY` và `MAX_DELAY`
- Giảm `INITIAL_BATCH_SIZE`

### Script chạy chậm
- Kiểm tra log để xem có lỗi gì không
- Giảm `MIN_DELAY` nếu API response tốt 