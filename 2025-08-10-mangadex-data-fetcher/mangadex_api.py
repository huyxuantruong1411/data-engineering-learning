import requests
import time
import random
import logging
import os
import pyodbc
import datetime
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from PIL import Image
from io import BytesIO

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("mangadex_api.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class MangaDexAPI:
    def __init__(self, base_url="https://api.mangadex.org", max_retries=5, backoff_factor=2, db_conn_str=None):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            "Referer": "https://mangadex.org/"
        })
        retries = Retry(total=max_retries, backoff_factor=backoff_factor, status_forcelist=[429, 500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.db_conn_str = db_conn_str
        self.conn = None
        self.cursor = None
        self.language_priority = ["vi", "en"]  # Ưu tiên tiếng Việt trước tiếng Anh
        self.map_languages = {
            "vi": "Vietnamese Translation",
            "en": "English Translation"
        }

    def connect_db(self):
        """Kết nối đến cơ sở dữ liệu SQL Server."""
        if not self.db_conn_str:
            logger.warning("Không có chuỗi kết nối cơ sở dữ liệu.")
            return False
        try:
            self.conn = pyodbc.connect(self.db_conn_str)
            self.cursor = self.conn.cursor()
            logger.info("Đã kết nối đến cơ sở dữ liệu.")
            return True
        except Exception as e:
            logger.error(f"Lỗi kết nối cơ sở dữ liệu: {str(e)}")
            return False

    def close_db(self):
        """Đóng kết nối cơ sở dữ liệu."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logger.info("Đã đóng kết nối cơ sở dữ liệu.")

    def make_request(self, url, params=None, use_proxy=None, verify_ssl=True):
        """Thực hiện yêu cầu GET với retry và xử lý rate limit."""
        try:
            time.sleep(random.uniform(0.2, 0.5))  # Độ trễ ngẫu nhiên
            proxies = {"https": use_proxy} if use_proxy else None
            response = self.session.get(url, params=params, timeout=10, proxies=proxies, verify=verify_ssl)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limited. Waiting for {retry_after} seconds.")
                time.sleep(retry_after)
                return self.make_request(url, params, use_proxy, verify_ssl)
            response.raise_for_status()
            data = response.json()
            if data.get("result") != "ok":
                logger.error(f"API trả về lỗi: {data.get('errors', 'Không có thông tin lỗi')}")
                return None
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Yêu cầu thất bại: {str(e)}")
            raise

    def search_manga(self, title=None, authors=None, artists=None, year=None, included_tags=None, excluded_tags=None, status=None, original_language=None, publication_demographic=None, ids=None, content_rating=None, created_at_since=None, updated_at_since=None, order=None, includes=None, has_available_chapters=None, has_unavailable_chapters=None, group=None, limit=10, offset=0, use_proxy=None, verify_ssl=True):
        """Tìm kiếm manga với các bộ lọc."""
        url = f"{self.base_url}/manga"
        params = {
            "limit": min(limit, 100),
            "offset": offset
        }
        if title:
            params["title"] = title
        if authors:
            params["authors[]"] = authors
        if artists:
            params["artists[]"] = artists
        if year:
            params["year"] = year
        if included_tags:
            params["includedTags[]"] = included_tags
        if excluded_tags:
            params["excludedTags[]"] = excluded_tags
        if status:
            params["status[]"] = status
        if original_language:
            params["originalLanguage[]"] = original_language
        if publication_demographic:
            params["publicationDemographic[]"] = publication_demographic
        if ids:
            params["ids[]"] = ids
        if content_rating:
            params["contentRating[]"] = content_rating
        if created_at_since:
            params["createdAtSince"] = created_at_since
        if updated_at_since:
            params["updatedAtSince"] = updated_at_since
        if order:
            params["order"] = order
        if includes:
            params["includes[]"] = includes
        if has_available_chapters:
            params["hasAvailableChapters"] = has_available_chapters
        if has_unavailable_chapters:
            params["hasUnavailableChapters"] = has_unavailable_chapters
        if group:
            params["group"] = group
        try:
            return self.make_request(url, params, use_proxy, verify_ssl)
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi tìm kiếm manga: {e}")
            return None

    def get_manga(self, manga_id, includes=None):
        """Lấy thông tin chi tiết của manga theo ID."""
        url = f"{self.base_url}/manga/{manga_id}"
        params = {}
        if includes:
            params["includes[]"] = includes
        try:
            return self.make_request(url, params)
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy manga {manga_id}: {e}")
            return None

    def get_manga_aggregate(self, manga_id, translated_language=None):
        """Lấy thông tin tập và chương của manga."""
        url = f"{self.base_url}/manga/aggregate/{manga_id}"
        params = {}
        if translated_language:
            params["translatedLanguage[]"] = translated_language
        try:
            return self.make_request(url, params)
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy tập và chương của manga {manga_id}: {e}")
            return None

    def get_manga_statistics(self, manga_id):
        """Lấy thống kê manga (score, số lượt theo dõi)."""
        url = f"{self.base_url}/statistics/manga/{manga_id}"
        try:
            return self.make_request(url)
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy thống kê manga {manga_id}: {e}")
            return None

    def get_chapters(self, manga_id=None, groups=None, translated_language=None, original_language=None, content_rating=None, include_future_updates="1", include_empty_pages=0, include_future_publish_at=0, include_external_url=0, include_unavailable="0", created_at_since=None, updated_at_since=None, publish_at_since=None, order=None, includes=None, limit=100, offset=0):
        """Lấy danh sách chương với các bộ lọc."""
        url = f"{self.base_url}/chapter"
        params = {
            "limit": min(limit, 100),
            "offset": offset,
            "includeFutureUpdates": include_future_updates,
            "includeEmptyPages": include_empty_pages,
            "includeFuturePublishAt": include_future_publish_at,
            "includeExternalUrl": include_external_url,
            "includeUnavailable": include_unavailable
        }
        if manga_id:
            params["manga"] = manga_id
        if groups:
            params["groups[]"] = groups
        if translated_language:
            params["translatedLanguage[]"] = translated_language
        if original_language:
            params["originalLanguage[]"] = original_language
        if content_rating:
            params["contentRating[]"] = content_rating
        if created_at_since:
            params["createdAtSince"] = created_at_since
        if updated_at_since:
            params["updatedAtSince"] = updated_at_since
        if publish_at_since:
            params["publishAtSince"] = publish_at_since
        if order:
            params["order"] = order
        if includes:
            params["includes[]"] = includes
        try:
            return self.make_request(url, params)
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy danh sách chương: {e}")
            return None

    def get_chapter(self, chapter_id, includes=None):
        """Lấy thông tin chi tiết của chương theo ID."""
        url = f"{self.base_url}/chapter/{chapter_id}"
        params = {}
        if includes:
            params["includes[]"] = includes
        try:
            return self.make_request(url, params)
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy chương {chapter_id}: {e}")
            return None

    def get_chapter_images(self, chapter_id, quality="data"):
        """Lấy danh sách URL hình ảnh của chương."""
        url = f"{self.base_url}/at-home/server/{chapter_id}"
        try:
            data = self.make_request(url)
            if data and data["result"] == "ok":
                base_url = data["baseUrl"]
                chapter_hash = data["chapter"]["hash"]
                pages = data["chapter"][quality]
                return [f"{base_url}/{quality}/{chapter_hash}/{page}" for page in pages]
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy hình ảnh chương {chapter_id}: {e}")
            return None

    def download_chapter_images(self, chapter_id, manga_title, chapter_number, save_path="DB", quality="data"):
        """Tải hình ảnh chương và lưu vào thư mục."""
        image_urls = self.get_chapter_images(chapter_id, quality)
        if not image_urls:
            logger.error(f"Không tìm thấy hình ảnh cho chương {chapter_number}.")
            return False

        chapter_folder = os.path.join(save_path, manga_title, f"Chapter {chapter_number}")
        os.makedirs(chapter_folder, exist_ok=True)
        total_pages = len(image_urls)
        downloaded_pages = 0

        for index, page_url in enumerate(image_urls, start=1):
            try:
                time.sleep(random.uniform(0.2, 0.5))
                response = self.session.get(page_url, timeout=10)
                response.raise_for_status()
                image = Image.open(BytesIO(response.content))
                image_path = os.path.join(chapter_folder, f"{index}.jpg")
                image.convert("RGB").save(image_path, "JPEG")
                downloaded_pages += 1
                logger.info(f"Đã tải trang {index}/{total_pages} cho chương {chapter_number}.")
            except Exception as e:
                logger.error(f"Lỗi khi tải trang {index} cho chương {chapter_number}: {e}")
                continue

        logger.info(f"Đã tải {downloaded_pages}/{total_pages} trang cho chương {chapter_number}.")
        return downloaded_pages > 0

    def get_authors(self, name=None, ids=None, order=None, includes=None, limit=10, offset=0):
        """Lấy danh sách tác giả với các bộ lọc."""
        url = f"{self.base_url}/author"
        params = {
            "limit": min(limit, 100),
            "offset": offset
        }
        if name:
            params["name"] = name
        if ids:
            params["ids[]"] = ids
        if order:
            params["order"] = order
        if includes:
            params["includes[]"] = includes
        try:
            return self.make_request(url, params)
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy danh sách tác giả: {e}")
            return None

    def get_author(self, author_id, includes=None):
        """Lấy thông tin chi tiết của tác giả theo ID."""
        url = f"{self.base_url}/author/{author_id}"
        params = {}
        if includes:
            params["includes[]"] = includes
        try:
            return self.make_request(url, params)
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy tác giả {author_id}: {e}")
            return None

    def get_cover_arts(self, manga=None, ids=None, uploaders=None, locales=None, order=None, includes=None, limit=10, offset=0):
        """Lấy danh sách ảnh bìa với các bộ lọc."""
        url = f"{self.base_url}/cover"
        params = {
            "limit": min(limit, 100),
            "offset": offset
        }
        if manga:
            params["manga[]"] = manga
        if ids:
            params["ids[]"] = ids
        if uploaders:
            params["uploaders[]"] = uploaders
        if locales:
            params["locales[]"] = locales
        if order:
            params["order"] = order
        if includes:
            params["includes[]"] = includes
        try:
            return self.make_request(url, params)
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy danh sách ảnh bìa: {e}")
            return None

    def get_cover_art(self, cover_id, includes=None):
        """Lấy thông tin chi tiết của ảnh bìa theo ID."""
        url = f"{self.base_url}/cover/{cover_id}"
        params = {}
        if includes:
            params["includes[]"] = includes
        try:
            return self.make_request(url, params)
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy ảnh bìa {cover_id}: {e}")
            return None

    def get_tags(self):
        """Lấy danh sách thẻ."""
        url = f"{self.base_url}/manga/tag"
        try:
            return self.make_request(url)
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy danh sách thẻ: {e}")
            return None

    def save_manga_to_db(self, manga_id, user_id):
        """Lưu thông tin manga, chương, và các dữ liệu liên quan vào cơ sở dữ liệu."""
        if not self.connect_db():
            return False

        try:
            # Lấy thông tin manga
            manga_data = self.get_manga(manga_id, includes=["author", "artist", "cover_art"])
            if not manga_data or manga_data["result"] != "ok":
                logger.error(f"Không tìm thấy manga với ID {manga_id}.")
                return False

            manga_attr = manga_data["data"]["attributes"]
            author_ids = [rel["id"] for rel in manga_data["data"]["relationships"] if rel["type"] == "author"]
            artist_ids = [rel["id"] for rel in manga_data["data"]["relationships"] if rel["type"] == "artist"]
            cover_id = next((rel["id"] for rel in manga_data["data"]["relationships"] if rel["type"] == "cover_art"), None)
            tag_ids = [tag["id"] for tag in manga_attr.get("tags", [])]

            # Lấy ảnh bìa
            cover_url = None
            if cover_id:
                cover_data = self.get_cover_art(cover_id)
                if cover_data and cover_data["result"] == "ok":
                    cover_filename = cover_data["data"]["attributes"]["fileName"]
                    cover_url = f"https://uploads.mangadex.org/covers/{manga_id}/{cover_filename}"

            # Lấy thống kê
            stats_data = self.get_manga_statistics(manga_id)
            score = stats_data["statistics"].get(manga_id, {}).get("rating", {}).get("average", None) if stats_data else None

            # Lưu manga
            created_at = datetime.datetime.fromisoformat(manga_attr["createdAt"].replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
            updated_at = datetime.datetime.fromisoformat(manga_attr["updatedAt"].replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
            title = manga_attr["title"].get("en", next(iter(manga_attr["title"].values()), ""))
            description = manga_attr["description"].get("en", next(iter(manga_attr["description"].values()), ""))

            self.cursor.execute("SELECT mangaId FROM Manga WHERE mangaId = ?", manga_id)
            if not self.cursor.fetchone():
                self.cursor.execute("""
                    INSERT INTO Manga (
                        mangaId, mangaType, title, description, coverArt, 
                        mangadexScore, originalLanguage, lastChapter, publicationDemographic, 
                        status, year, contentRating, createdAt, updatedAt
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, manga_id, manga_data["data"]["type"], title, description, cover_url, score,
                    manga_attr["originalLanguage"], manga_attr.get("lastChapter", None),
                    manga_attr.get("publicationDemographic", None), manga_attr["status"],
                    manga_attr.get("year", None), manga_attr["contentRating"], created_at, updated_at)
                logger.info(f"Đã thêm manga {title} vào cơ sở dữ liệu.")
            else:
                logger.info(f"Manga {manga_id} đã tồn tại.")

            # Lưu tiêu đề thay thế
            for alt_title in manga_attr.get("altTitles", []):
                for lang, title_text in alt_title.items():
                    self.cursor.execute("SELECT altTitleId FROM AltTitles WHERE mangaId = ? AND language = ? AND title = ?",
                                       manga_id, lang, title_text)
                    if not self.cursor.fetchone():
                        self.cursor.execute("INSERT INTO AltTitles (mangaId, language, title) VALUES (?, ?, ?)",
                                           manga_id, lang, title_text)

            # Lưu tag
            for tag_id in tag_ids:
                self.cursor.execute("SELECT tagId FROM Tags WHERE tagId = ?", tag_id)
                if self.cursor.fetchone():
                    self.cursor.execute("SELECT tagId FROM MangaByTags WHERE tagId = ? AND mangaId = ?",
                                       tag_id, manga_id)
                    if not self.cursor.fetchone():
                        self.cursor.execute("INSERT INTO MangaByTags (tagId, mangaId) VALUES (?, ?)",
                                           tag_id, manga_id)
                else:
                    logger.warning(f"Tag {tag_id} không tồn tại trong cơ sở dữ liệu.")

            # Lưu ngôn ngữ dịch
            for lang in manga_attr.get("availableTranslatedLanguages", []):
                self.cursor.execute("SELECT languageId FROM AvailableTranslatedLanguages WHERE mangaId = ? AND language = ?",
                                   manga_id, lang)
                if not self.cursor.fetchone():
                    self.cursor.execute("INSERT INTO AvailableTranslatedLanguages (mangaId, language) VALUES (?, ?)",
                                       manga_id, lang)

            # Lưu tác giả/họa sĩ
            def process_creator(creator_id, role):
                creator_data = self.get_author(creator_id)
                if not creator_data or creator_data["result"] != "ok":
                    return
                attr = creator_data["data"]["attributes"]
                self.cursor.execute("SELECT creatorId FROM Creator WHERE creatorId = ?", creator_id)
                if not self.cursor.fetchone():
                    self.cursor.execute("INSERT INTO Creator (creatorId, creatorName, biography) VALUES (?, ?, ?)",
                                       creator_id, attr.get("name", "Unknown"), attr.get("biography", {}).get("en", ""))
                    for platform, url in attr.get("social", {}).items():
                        self.cursor.execute("INSERT INTO CreatorSocialMedia (creatorId, platform, url) VALUES (?, ?, ?)",
                                           creator_id, platform, url)
                self.cursor.execute("SELECT role FROM CreatorMangaWorks WHERE creatorId = ? AND mangaId = ?",
                                   creator_id, manga_id)
                existing_role = self.cursor.fetchone()
                if existing_role:
                    if role not in existing_role[0]:
                        new_role = f"{existing_role[0]},{role}" if existing_role[0] else role
                        self.cursor.execute("UPDATE CreatorMangaWorks SET role = ? WHERE creatorId = ? AND mangaId = ?",
                                           new_role, creator_id, manga_id)
                else:
                    self.cursor.execute("INSERT INTO CreatorMangaWorks (creatorId, mangaId, role) VALUES (?, ?, ?)",
                                       creator_id, manga_id, role)

            for author_id in author_ids:
                process_creator(author_id, "author")
            for artist_id in artist_ids:
                process_creator(artist_id, "artist")

            # Lưu chương
            for lang in self.language_priority:
                chapters_data = self.get_chapters(manga_id=manga_id, translated_language=[lang], includes=["scanlation_group"])
                if not chapters_data or not chapters_data.get("data"):
                    continue
                for ch in chapters_data["data"]:
                    chapter_id = ch["id"]
                    chapter_attr = ch["attributes"]
                    chapter_number = chapter_attr.get("chapter", "")
                    chapter_title = chapter_attr.get("title", "")
                    published_at = datetime.datetime.fromisoformat(chapter_attr["publishAt"].replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
                    self.cursor.execute("SELECT chapterId FROM Chapter WHERE chapterId = ?", chapter_id)
                    if not self.cursor.fetchone():
                        self.cursor.execute("""
                            INSERT INTO Chapter (chapterId, mangaId, chapterNumber, chapterTitle, translatedLanguage, publishedAt)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, chapter_id, manga_id, chapter_number, chapter_title, lang, published_at)
                        image_urls = self.get_chapter_images(chapter_id, quality="data")
                        if image_urls:
                            total_pages = len(image_urls)
                            self.cursor.execute("UPDATE Chapter SET totalPages = ? WHERE chapterId = ?",
                                               total_pages, chapter_id)
                            for idx, page_url in enumerate(image_urls):
                                self.cursor.execute("INSERT INTO Page (chapterId, pageNumber, pageImg) VALUES (?, ?, ?)",
                                                   chapter_id, idx + 1, page_url)
                            logger.info(f"Đã thêm {total_pages} trang cho chương {chapter_number} ({lang}).")
                        else:
                            logger.warning(f"Không tìm thấy hình ảnh cho chương {chapter_number} ({lang}).")

            # Lưu lịch sử tải của người dùng
            self.cursor.execute("SELECT userId FROM UsersMangaDownloads WHERE userId = ? AND mangaId = ?",
                               user_id, manga_id)
            if not self.cursor.fetchone():
                self.cursor.execute("INSERT INTO UsersMangaDownloads (userId, mangaId) VALUES (?, ?)",
                                   user_id, manga_id)
                logger.info(f"Đã thêm manga {manga_id} vào danh sách tải của người dùng {user_id}.")

            self.conn.commit()
            logger.info(f"Đã lưu manga {title} vào cơ sở dữ liệu.")
            return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Lỗi khi lưu manga {manga_id}: {e}")
            return False
        finally:
            self.close_db()

    def download_manga(self, manga_id, manga_title, save_path="DB"):
        """Tải toàn bộ chương của manga và lưu vào thư mục."""
        list_chapters = {}
        chapters_data = self.get_chapters(manga_id=manga_id, limit=100, order={"chapter": "asc"})
        if not chapters_data or not chapters_data.get("data"):
            logger.error(f"Không tìm thấy chương cho manga {manga_id}.")
            return False

        total_chapters = len(chapters_data["data"])
        for index, chapter in enumerate(chapters_data["data"], start=1):
            chapter_number = chapter["attributes"].get("chapter", "Unknown")
            available_languages = chapter["attributes"].get("translatedLanguage", [])
            selected_lang = next((lang for lang in self.language_priority if lang in available_languages), None)
            if not selected_lang:
                continue

            if chapter_number not in list_chapters:
                list_chapters[chapter_number] = selected_lang
            elif list_chapters[chapter_number] == "en" and selected_lang == "vi":
                list_chapters[chapter_number] = "vi"
            else:
                continue

            chapter_id = chapter["id"]
            success = self.download_chapter_images(chapter_id, manga_title, chapter_number, save_path)
            if success:
                logger.info(f"Đã tải chương {chapter_number}/{total_chapters} ({self.map_languages.get(selected_lang)}).")
            else:
                logger.warning(f"Thất bại khi tải chương {chapter_number}.")

        logger.info(f"Hoàn thành tải manga {manga_title}.")
        return True

    def close(self):
        """Đóng session HTTP."""
        self.session.close()
        self.close_db()