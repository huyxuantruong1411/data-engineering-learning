# mongo_to_dbt_optimized.py
# -*- coding: utf-8 -*-
"""
Script tối ưu để extract dữ liệu từ MongoDB -> ép phẳng thành các bảng tối ưu cho dbt.
Kiến trúc được thiết kế đặc biệt cho việc phân tích xu hướng đọc truyện tranh.

Có thể chạy:
    python mongo_to_dbt_optimized.py \
        --mongo-uri "mongodb://localhost:27017/" \
        --db "manga_raw_data" \
        --seed-dir "D:\\Projects\\Học DE\\data-engineering-learning\\2025-08-16\\mongo_to_db\\seeds"
"""

import argparse
import os
import re
import csv
import logging
from typing import Any, Dict, Iterable, List, Optional
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ------------------------------
# Helpers
# ------------------------------

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def clean_text(x: Any) -> Any:
    """Làm sạch text để hợp lệ CSV/BigQuery."""
    if isinstance(x, str):
        x = x.replace("\r\n", " ").replace("\r", " ").replace("\n", " ").replace("\t", " ")
        x = x.replace('"', '""')
        x = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]", " ", x)
        x = re.sub(r'[^\x20-\x7E]', ' ', x)
        return x.strip()
    return x if x is not None else ""

def normalize_datetime(dt_str: Any) -> Optional[str]:
    if not dt_str or not isinstance(dt_str, str):
        return None
    if dt_str.endswith('+00:00'):
        dt_str = dt_str.replace('+00:00', '')
    elif dt_str.endswith('Z'):
        dt_str = dt_str.replace('Z', '')
    try:
        datetime.fromisoformat(dt_str)
        return dt_str
    except ValueError:
        return None

def normalize_year(year_val: Any) -> Optional[int]:
    if year_val is None:
        return None
    try:
        if isinstance(year_val, str) and year_val.endswith('.0'):
            year_val = year_val[:-2]
        return int(float(year_val)) if year_val else None
    except (ValueError, TypeError):
        return None

def normalize_int(x: Any) -> str:
    try:
        if x is None or x == "":
            return ""
        # Chuyển thành chuỗi để xử lý an toàn
        x_str = str(x)
        # Nếu chuỗi kết thúc bằng ".0", cắt bỏ ".0"
        if x_str.endswith(".0"):
            x_str = x_str[:-2]
        # Ép kiểu về số nguyên
        return str(int(float(x_str)))
    except (ValueError, TypeError):
        return ""

def normalize_float(x: Any) -> str:
    try:
        if x is None or x == "":
            return ""
        return str(float(str(x)))
    except (ValueError, TypeError):
        return ""

def get_attr(d: Dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return cur if cur is not None else default

def as_list(x: Any) -> List:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def write_csv(df: pd.DataFrame, seed_dir: str, filename: str):
    path = os.path.join(seed_dir, filename)
    if df is None or df.empty:
        pd.DataFrame(columns=df.columns if 'df' in locals() else []).to_csv(path, index=False, quoting=csv.QUOTE_ALL)
        logging.warning(f"{filename}: DataFrame rỗng -> ghi header trống.")
        return
    
    logging.info(f"{filename}: {len(df)} rows trước khi lọc")
    
    # Chỉ dropna trên cột chính
    key_col = next((col for col in ['manga_id', 'creator_id', 'stat_id', 'chapter_id', 'tag_id', 'group_id', 'related_group_id'] if col in df.columns), df.columns[0])
    df = df.dropna(subset=[key_col])
    
    # Xử lý đặc biệt cho trường year
    if 'year' in df.columns:
        df['year'] = df['year'].astype(str).str.replace('.0', '').replace('nan', '').replace('None', '')
        df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
    
    # Làm sạch dữ liệu
    for col in df.columns:
        df[col] = df[col].map(clean_text)
    
    logging.info(f"{filename}: {len(df)} rows sau khi lọc")
    
    # Ghi CSV với quoting all
    df.to_csv(path, index=False, quoting=csv.QUOTE_ALL)
    logging.info(f"{filename}: Ghi {len(df)} rows")

# ------------------------------
# Extractors
# ------------------------------
def extract_manga_optimized(col, seed_dir: str):
    docs = list(col.find({}))
    logging.info(f"[mangadex_manga] {len(docs)} docs")

    manga_rows = []
    creator_relations = []
    tag_relations = []
    cover_relations = []
    invalid_docs = 0

    for d in docs:
        try:
            manga_id = d.get("id")
            if not manga_id:
                invalid_docs += 1
                logging.warning(f"Document thiếu manga_id: {d.get('_id')}")
                continue

            a = d.get("attributes", {}) or {}
            manga_rows.append({
                "manga_id": manga_id,
                "title_en": get_attr(a, "title", "en"),
                "title_ja": get_attr(a, "title", "ja"),
                "year": normalize_int(a.get("year")),
                "status": a.get("status"),
                "demographic": a.get("publicationDemographic"),
                "content_rating": a.get("contentRating"),
                "original_language": a.get("originalLanguage"),
                "created_at": normalize_datetime(a.get("createdAt")),
                "updated_at": normalize_datetime(a.get("updatedAt")),
                "is_locked": a.get("isLocked"),
                "last_chapter": a.get("lastChapter"),
                "last_volume": a.get("lastVolume"),
                "latest_uploaded_chapter": a.get("latestUploadedChapter"),
                "version": a.get("version"),
                "state": a.get("state"),
                "chapter_numbers_reset_on_new_volume": a.get("chapterNumbersResetOnNewVolume"),
            })

            for r in as_list(d.get("relationships")):
                if r.get("type") in ["author", "artist"]:
                    creator_relations.append({
                        "manga_id": manga_id,
                        "creator_id": r.get("id"),
                        "role": r.get("type"),
                        "created_at": normalize_datetime(get_attr(r, "attributes", "createdAt")),
                        "updated_at": normalize_datetime(get_attr(r, "attributes", "updatedAt")),
                    })

                if r.get("type") == "cover_art":
                    cover_relations.append({
                        "manga_id": manga_id,
                        "cover_id": r.get("id"),
                        "created_at": normalize_datetime(get_attr(r, "attributes", "createdAt")),
                        "updated_at": normalize_datetime(get_attr(r, "attributes", "updatedAt")),
                    })

            for t in as_list(a.get("tags")):
                tag_id = t.get("id")
                if not tag_id:
                    logging.warning(f"Tag thiếu id trong manga_id={manga_id}")
                    continue
                tag_relations.append({
                    "manga_id": manga_id,
                    "tag_id": tag_id,
                    "tag_name_en": get_attr(t, "attributes", "name", "en"),
                    "tag_group": get_attr(t, "attributes", "group"),
                })

        except Exception as e:
            invalid_docs += 1
            logging.error(f"Lỗi xử lý document manga_id={d.get('id', d.get('_id'))}: {e}")

    logging.info(f"[mangadex_manga] {invalid_docs} documents không hợp lệ")
    write_csv(pd.DataFrame(manga_rows), seed_dir, "dim_manga.csv")
    # Đường dẫn file dim_manga.csv
    file_path = os.path.join(seed_dir, "dim_manga.csv")

    # Đọc lại file
    df = pd.read_csv(file_path)

    # Xóa 2 ký tự cuối của cột year (nếu không null và có đủ 2 ký tự)
    df["year"] = df["year"].astype(str).str[:-2].replace("n", pd.NA)

    # Ghi đè file đã chỉnh sửa
    df.to_csv(file_path, index=False)
    
    write_csv(pd.DataFrame(creator_relations), seed_dir, "bridge_manga_creator.csv")
    write_csv(pd.DataFrame(tag_relations), seed_dir, "bridge_manga_tag.csv")
    write_csv(pd.DataFrame(cover_relations), seed_dir, "bridge_manga_cover.csv")


def extract_creators_optimized(col, seed_dir: str):
    docs = list(col.find({}))
    logging.info(f"[mangadex_creators] {len(docs)} docs")

    creator_rows = []
    bio_rows = []
    invalid_docs = 0

    for d in docs:
        try:
            data = d.get("data", {}) or {}
            creator_id = data.get("id")
            if not creator_id:
                invalid_docs += 1
                logging.warning(f"Document thiếu creator_id: {d.get('_id')}")
                continue

            a = data.get("attributes", {}) or {}
            creator_rows.append({
                "creator_id": creator_id,
                "name": a.get("name"),
                "twitter": a.get("twitter"),
                "pixiv": a.get("pixiv"),
                "naver": a.get("naver"),
                "website": a.get("website"),
                "youtube": a.get("youtube"),
                "weibo": a.get("weibo"),
                "tumblr": a.get("tumblr"),
                "nicoVideo": a.get("nicoVideo"),
                "booth": a.get("booth"),
                "fanBox": a.get("fanBox"),
                "fantia": a.get("fantia"),
                "melonBook": a.get("melonBook"),
                "namicomi": a.get("namicomi"),
                "skeb": a.get("skeb"),
                "created_at": normalize_datetime(a.get("createdAt")),
                "updated_at": normalize_datetime(a.get("updatedAt")),
                "version": a.get("version"),
            })

            for lang, val in (a.get("biography") or {}).items():
                bio_rows.append({
                    "creator_id": creator_id,
                    "lang_code": lang,
                    "biography": val
                })

        except Exception as e:
            invalid_docs += 1
            logging.error(f"Lỗi xử lý document creator_id={d.get('data', {}).get('id', d.get('_id'))}: {e}")

    logging.info(f"[mangadex_creators] {invalid_docs} documents không hợp lệ")
    write_csv(pd.DataFrame(creator_rows), seed_dir, "dim_creator.csv")
    write_csv(pd.DataFrame(bio_rows), seed_dir, "bridge_creator_biography.csv")


def extract_statistics_optimized(col, seed_dir: str):
    docs = list(col.find({}))
    logging.info(f"[mangadex_statistics] {len(docs)} docs")

    stat_rows = []
    trend_rows = []
    invalid_docs = 0

    for d in docs:
        try:
            stat_id = d.get("_id")
            manga_id = d.get("mangaId")
            if not manga_id or not stat_id:
                invalid_docs += 1
                logging.warning(f"Document thiếu stat_id/manga_id: {d.get('_id')}")
                continue

            stat = d.get("statistics")
            if not isinstance(stat, dict):
                invalid_docs += 1
                logging.warning(f"Document stat_id={stat_id} thiếu hoặc không hợp lệ trường statistics")
                continue

            rating = stat.get("rating")
            if not isinstance(rating, dict):
                rating = {}  # Nếu rating là None, gán thành dict rỗng

            comments = stat.get("comments")
            if not isinstance(comments, dict):
                comments = {}  # Nếu comments là None, gán thành dict rỗng

            stat_rows.append({
                "stat_id": stat_id,
                "manga_id": manga_id,
                "snapshot_time": normalize_datetime(d.get("snapshotTime")),
                "fetched_at": normalize_datetime(d.get("fetched_at")),
                "source": d.get("source", ""),
                "follows": normalize_int(stat.get("follows", "")),
                "rating_avg": normalize_float(rating.get("average", "")),
                "rating_bayesian": normalize_float(rating.get("bayesian", "")),
                "unavailable_chapters_count": normalize_int(stat.get("unavailableChaptersCount", "")),
                "comments_thread_id": normalize_int(comments.get("threadId", "")),
                "comments_replies_count": normalize_int(comments.get("repliesCount", "")),
            })

            if d.get("snapshotTime"):
                trend_rows.append({
                    "manga_id": manga_id,
                    "snapshot_time": normalize_datetime(d.get("snapshotTime")),
                    "fetched_at": normalize_datetime(d.get("fetched_at")),
                    "follows": normalize_int(stat.get("follows", "")),
                    "rating_avg": normalize_float(rating.get("average", "")),
                    "rating_bayesian": normalize_float(rating.get("bayesian", "")),
                })

        except Exception as e:
            invalid_docs += 1
            logging.error(f"Lỗi xử lý document stat_id={d.get('_id', 'unknown')}: {e}")

    logging.info(f"[mangadex_statistics] {invalid_docs} documents không hợp lệ")
    write_csv(pd.DataFrame(stat_rows), seed_dir, "fact_statistics.csv")
    write_csv(pd.DataFrame(trend_rows), seed_dir, "fact_manga_trends.csv")


def extract_chapters_optimized(col, seed_dir: str):
    docs = list(col.find({}))
    logging.info(f"[mangadex_chapters] {len(docs)} docs")

    chapter_rows = []
    group_relations = []
    invalid_docs = 0

    for d in docs:
        try:
            a = d.get("attributes", {}) or {}
            chapter_id = d.get("id") or d.get("_id")
            manga_id = d.get("mangaId")
            if not chapter_id or not manga_id:
                invalid_docs += 1
                logging.warning(f"Document thiếu chapter_id/manga_id: {d.get('_id')}")
                continue

            chapter_rows.append({
                "chapter_id": chapter_id,
                "manga_id": manga_id,
                "volume": a.get("volume"),
                "chapter": a.get("chapter"),
                "title": a.get("title"),
                "translated_language": a.get("translatedLanguage"),
                "external_url": a.get("externalUrl"),
                "is_unavailable": a.get("isUnavailable"),
                "publish_at": normalize_datetime(a.get("publishAt")),
                "readable_at": normalize_datetime(a.get("readableAt")),
                "created_at": normalize_datetime(a.get("createdAt")),
                "updated_at": normalize_datetime(a.get("updatedAt")),
                "pages": a.get("pages"),
                "version": a.get("version"),
                "fetched_at": d.get("fetched_at"),
            })

            for r in as_list(d.get("relationships")):
                if r.get("type") == "scanlation_group":
                    group_id = r.get("id")
                    if not group_id:
                        logging.warning(f"Scanlation group thiếu id trong chapter_id={chapter_id}")
                        continue
                    group_relations.append({
                        "chapter_id": chapter_id,
                        "group_id": group_id,
                        "created_at": normalize_datetime(get_attr(r, "attributes", "createdAt")),
                        "updated_at": normalize_datetime(get_attr(r, "attributes", "updatedAt")),
                    })

        except Exception as e:
            invalid_docs += 1
            logging.error(f"Lỗi xử lý document chapter_id={d.get('id', d.get('_id'))}: {e}")

    logging.info(f"[mangadex_chapters] {invalid_docs} documents không hợp lệ")
    write_csv(pd.DataFrame(chapter_rows), seed_dir, "fact_chapters.csv")
    write_csv(pd.DataFrame(group_relations), seed_dir, "bridge_chapter_group.csv")


def extract_tags_optimized(col, seed_dir: str):
    docs = list(col.find({}))
    logging.info(f"[mangadex_tags] {len(docs)} docs")

    tag_rows = []
    name_rows = []
    invalid_docs = 0

    for d in docs:
        try:
            tag_id = d.get("_id")
            a = d.get("attributes", {}) or {}
            if not tag_id:
                invalid_docs += 1
                logging.warning(f"Document thiếu tag_id: {d.get('_id')}")
                continue

            tag_rows.append({
                "tag_id": tag_id,
                "group": a.get("group"),
                "version": a.get("version"),
                "name_en": get_attr(a, "name", "en"),
            })

            for lang, val in (a.get("name") or {}).items():
                name_rows.append({
                    "tag_id": tag_id,
                    "lang_code": lang,
                    "tag_name": val
                })

        except Exception as e:
            invalid_docs += 1
            logging.error(f"Lỗi xử lý document tag_id={d.get('_id')}: {e}")

    logging.info(f"[mangadex_tags] {invalid_docs} documents không hợp lệ")
    write_csv(pd.DataFrame(tag_rows), seed_dir, "dim_tag.csv")
    write_csv(pd.DataFrame(name_rows), seed_dir, "bridge_tag_name.csv")


def extract_groups_optimized(col, seed_dir: str):
    docs = list(col.find({}))
    logging.info(f"[mangadex_groups] {len(docs)} docs")

    group_rows = []
    alt_name_rows = []
    language_rows = []
    invalid_docs = 0

    for d in docs:
        try:
            data = d.get("data", {}) or {}
            a = data.get("attributes", {}) or {}
            group_id = data.get("id")
            if not group_id:
                invalid_docs += 1
                logging.warning(f"Document thiếu group_id: {d.get('_id')}")
                continue

            group_rows.append({
                "group_id": group_id,
                "name": a.get("name"),
                "locked": a.get("locked"),
                "website": a.get("website"),
                "irc_server": a.get("ircServer"),
                "irc_channel": a.get("ircChannel"),
                "discord": a.get("discord"),
                "contact_email": a.get("contactEmail"),
                "description": a.get("description"),
                "twitter": a.get("twitter"),
                "manga_updates": a.get("mangaUpdates"),
                "official": a.get("official"),
                "verified": a.get("verified"),
                "inactive": a.get("inactive"),
                "publish_delay": a.get("publishDelay"),
                "created_at": normalize_datetime(a.get("createdAt")),
                "updated_at": normalize_datetime(a.get("updatedAt")),
                "version": a.get("version"),
            })

            for alt in as_list(a.get("altNames")):
                if isinstance(alt, dict):
                    for lang, val in alt.items():
                        alt_name_rows.append({
                            "group_id": group_id,
                            "lang_code": lang,
                            "alt_name": val
                        })

            for lang in as_list(a.get("focusedLanguages")):
                language_rows.append({
                    "group_id": group_id,
                    "lang_code": lang
                })

        except Exception as e:
            invalid_docs += 1
            logging.error(f"Lỗi xử lý document group_id={d.get('data', {}).get('id', d.get('_id'))}: {e}")

    logging.info(f"[mangadex_groups] {invalid_docs} documents không hợp lệ")
    write_csv(pd.DataFrame(group_rows), seed_dir, "dim_group.csv")
    write_csv(pd.DataFrame(alt_name_rows), seed_dir, "bridge_group_altname.csv")
    write_csv(pd.DataFrame(language_rows), seed_dir, "bridge_group_language.csv")


def extract_related_optimized(col, seed_dir: str):
    docs = list(col.find({}))
    logging.info(f"[mangadex_related] {len(docs)} docs")

    related_rows = []
    invalid_docs = 0

    for d in docs:
        try:
            group_id = d.get("_id")
            fetched_at = d.get("fetched_at")
            if not group_id:
                invalid_docs += 1
                logging.warning(f"Document thiếu related_group_id: {d.get('_id')}")
                continue

            for r in as_list(d.get("relationships")):
                related_rows.append({
                    "related_group_id": group_id,
                    "fetched_at": fetched_at,
                    "entity_id": r.get("id"),
                    "entity_type": r.get("type"),
                    "relation_type": r.get("related")
                })

        except Exception as e:
            invalid_docs += 1
            logging.error(f"Lỗi xử lý document related_group_id={d.get('_id')}: {e}")

    logging.info(f"[mangadex_related] {invalid_docs} documents không hợp lệ")
    write_csv(pd.DataFrame(related_rows), seed_dir, "bridge_manga_related.csv")

# ------------------------------
# Main
# ------------------------------
def main():
    parser = argparse.ArgumentParser(description="Extract MongoDB collections to optimized dbt seed tables.")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017/", help="MongoDB URI")
    parser.add_argument("--db", default="manga_raw_data", help="Database name")
    parser.add_argument("--seed-dir", default="mongo_to_db/seeds", help="Output seeds directory")
    parser.add_argument("--skip", nargs="*", default=[], help="Collections to skip")
    parser.add_argument("--max-threads", type=int, default=4, help="Maximum number of threads")
    
    args = parser.parse_args()
    ensure_dir(args.seed_dir)

    jobs = [
        ("mangadex_manga", "mangadex_manga", extract_manga_optimized),
        ("mangadex_creators", "mangadex_creators", extract_creators_optimized),
        ("mangadex_statistics", "mangadex_statistics", extract_statistics_optimized),
        ("mangadex_chapters", "mangadex_chapters", extract_chapters_optimized),
        ("mangadex_tags", "mangadex_tags", extract_tags_optimized),
        ("mangadex_groups", "mangadex_groups", extract_groups_optimized),
        ("mangadex_related", "mangadex_related", extract_related_optimized),
    ]

    def run_job(alias, coll_name, fn):
        client = MongoClient(args.mongo_uri)
        db = client[args.db]
        if coll_name not in db.list_collection_names():
            logging.warning(f"{alias} -> collection '{coll_name}' không tồn tại, bỏ qua.")
            return
        logging.info(f"{alias} -> '{coll_name}'")
        col = db[coll_name]
        fn(col, args.seed_dir)
        client.close()

    with ThreadPoolExecutor(max_workers=args.max_threads) as executor:
        future_to_job = {
            executor.submit(run_job, alias, coll_name, fn): alias
            for alias, coll_name, fn in jobs
            if alias not in args.skip
        }

        for future in as_completed(future_to_job):
            alias = future_to_job[future]
            try:
                future.result()
                logging.info(f"{alias} completed")
            except Exception as e:
                logging.error(f"{alias} failed: {e}")

    logging.info("\n✅ Hoàn tất xuất CSV seeds tối ưu!")
    logging.info("📊 Bây giờ bạn có thể chạy: dbt seed")
    logging.info("🚀 Sau đó chạy: dbt run để build các models")

if __name__ == "__main__":
    main()