# mongo_to_db_seeds.py
# -*- coding: utf-8 -*-
"""
Đọc dữ liệu từ MongoDB (8 collection) -> ép phẳng về dạng bảng -> ghi CSV vào thư mục dbt seeds.
Có thể chạy:
    python mongo_to_db_seeds.py \
        --mongo-uri "mongodb://localhost:27017/" \
        --db "manga_raw_data" \
        --seed-dir "D:\\Projects\\Học DE\\data-engineering-learning\\2025-08-16\\mongo_to_db\\seeds"

Yêu cầu: pip install pymongo pandas
"""

import argparse
import os
import re
from typing import Any, Dict, Iterable, List, Optional
from datetime import datetime

import pandas as pd
from pymongo import MongoClient


# ------------------------------
# Helpers
# ------------------------------
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def clean_text(x: Any) -> Any:
    """Làm sạch text để hợp lệ CSV/BigQuery (loại bỏ \r, chuẩn hóa newline)."""
    if isinstance(x, str):
        x = x.replace("\r\n", "\n").replace("\r", "\n")
        # BigQuery CSV không thích control chars lạ:
        x = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", " ", x)
        return x
    return x

def normalize_datetime(dt_str: Any) -> Optional[str]:
    """Chuẩn hóa datetime string để tương thích với BigQuery."""
    if not dt_str or not isinstance(dt_str, str):
        return None
    
    # Loại bỏ timezone +00:00 và chuyển thành format chuẩn
    if dt_str.endswith('+00:00'):
        dt_str = dt_str.replace('+00:00', '')
    elif dt_str.endswith('Z'):
        dt_str = dt_str.replace('Z', '')
    
    # Kiểm tra format hợp lệ
    try:
        # Parse để kiểm tra format
        datetime.fromisoformat(dt_str)
        return dt_str
    except ValueError:
        # Nếu không parse được, trả về None
        return None

def normalize_year(year_val: Any) -> Optional[int]:
    """Chuẩn hóa year để tương thích với BigQuery INT64."""
    if year_val is None:
        return None
    
    try:
        # Chuyển đổi float sang int
        if isinstance(year_val, float):
            return int(year_val)
        elif isinstance(year_val, str):
            # Loại bỏ .0 nếu có
            if year_val.endswith('.0'):
                year_val = year_val[:-2]
            return int(year_val)
        elif isinstance(year_val, int):
            return year_val
        else:
            return None
    except (ValueError, TypeError):
        return None

def to_records(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for col in df.columns:
        df[col] = df[col].map(clean_text)
    return df

def post_process_csv(file_path: str):
    """Post-process CSV file to fix common issues."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Fix year column: replace .0 with empty string
        lines = content.split('\n')
        if len(lines) > 1:
            # Process data lines (skip header)
            for i in range(1, len(lines)):
                if lines[i].strip():
                    # Split by comma and fix year column (assuming it's the 5th column, index 4)
                    parts = lines[i].split(',')
                    if len(parts) > 4:
                        # Fix year column
                        if parts[4].endswith('.0'):
                            parts[4] = parts[4].replace('.0', '')
                        # Fix datetime columns (remove Z and +00:00)
                        if len(parts) > 9:  # created_at
                            parts[9] = parts[9].replace('Z', '').replace('+00:00', '')
                        if len(parts) > 10:  # updated_at
                            parts[10] = parts[10].replace('Z', '').replace('+00:00', '')
                        lines[i] = ','.join(parts)
            
            # Write back
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(lines))
            
            print(f"[POST-PROCESS] Fixed {file_path}")
    except Exception as e:
        print(f"[ERROR] Failed to post-process {file_path}: {e}")

def write_csv(df: pd.DataFrame, seed_dir: str, filename: str):
    path = os.path.join(seed_dir, filename)
    if df is None or df.empty:
        # vẫn tạo CSV với header trống để dbt seed nhận schema
        pd.DataFrame(columns=[]).to_csv(path, index=False)
        print(f"[WARN] {filename}: DataFrame rỗng -> ghi header trống.")
        return
    
    # Loại bỏ các dòng có tất cả giá trị null
    df = df.dropna(how='all')
    
    # Xử lý đặc biệt cho trường year - đảm bảo là integer
    if 'year' in df.columns:
        # Xử lý từng giá trị một cách an toàn
        def clean_year(x):
            if pd.isna(x):
                return None
            try:
                if isinstance(x, str) and x.endswith('.0'):
                    return int(float(x))
                elif isinstance(x, (int, float)):
                    return int(x)
                else:
                    return x
            except:
                return None
        
        df['year'] = df['year'].apply(clean_year)
        
        # Đảm bảo không có giá trị float nào còn sót lại
        df['year'] = df['year'].astype('Int64')  # pandas nullable integer type
    
    # Validation: Đảm bảo tất cả dòng có đủ cột
    expected_cols = len(df.columns)
    df = df.dropna(subset=df.columns[:3])  # Giữ lại dòng có ít nhất 3 cột đầu không null
    
    df = to_records(df)
    
    # Final validation: Kiểm tra dữ liệu trước khi ghi
    if not df.empty:
        # Đếm số cột trong mỗi dòng
        col_counts = df.apply(lambda x: len([v for v in x if pd.notna(v)]), axis=1)
        min_cols = col_counts.min()
        if min_cols < expected_cols:
            print(f"[WARN] {filename}: Một số dòng có ít cột ({min_cols}/{expected_cols})")
    
    df.to_csv(path, index=False)
    
    # Post-process CSV để fix các vấn đề còn lại
    if 'year' in df.columns:
        post_process_csv(path)
    
    print(f"[OK]  {filename}: {len(df)} rows")

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


# ------------------------------
# Extractors cho từng collection
# ------------------------------
def extract_mangadex_manga(col, seed_dir: str):
    docs = list(col.find({}))
    print(f"[mangadex_manga] {len(docs)} docs")

    dim_rows = []
    alt_rows = []
    desc_rows = []
    link_rows = []
    tag_rows = []
    rel_rows = []

    for d in docs:
        a = d.get("attributes", {}) or {}
        
        # Xử lý year trực tiếp
        year_val = a.get("year")
        if year_val is not None:
            try:
                if isinstance(year_val, str) and year_val.endswith('.0'):
                    year_val = int(float(year_val))
                elif isinstance(year_val, (int, float)):
                    year_val = int(year_val)
                else:
                    year_val = None
            except:
                year_val = None
        
        # Đảm bảo year_val là integer hoặc None
        if year_val is not None:
            year_val = int(year_val)
        
        dim_rows.append({
            "manga_id": d.get("id"),
            "type": d.get("type"),
            "title_en": get_attr(a, "title", "en"),
            "title_ja": get_attr(a, "title", "ja"),
            "year": year_val,
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

        for alt in as_list(a.get("altTitles")):
            if not isinstance(alt, dict):
                continue
            for lang, val in alt.items():
                alt_rows.append({
                    "manga_id": d.get("id"),
                    "lang_code": lang,
                    "alt_title": val
                })

        for lang, val in (a.get("description") or {}).items():
            desc_rows.append({
                "manga_id": d.get("id"),
                "lang_code": lang,
                "description": val
            })

        links = a.get("links")
        if isinstance(links, dict):
            for link_type, url in links.items():
                link_rows.append({
                    "manga_id": d.get("id"),
                    "link_type": link_type,
                    "url": url
                })

        for t in as_list(a.get("tags")):
            tag_rows.append({
                "manga_id": d.get("id"),
                "tag_id": t.get("id"),
                "tag_name_en": get_attr(t, "attributes", "name", "en"),
                "tag_group": get_attr(t, "attributes", "group"),
            })

        for r in as_list(d.get("relationships")):
            rel_rows.append({
                "manga_id": d.get("id"),
                "related_id": r.get("id"),
                "related_type": r.get("type"),
                "related_role": r.get("related"),
                "rel_created_at": normalize_datetime(get_attr(r, "attributes", "createdAt")),
                "rel_updated_at": normalize_datetime(get_attr(r, "attributes", "updatedAt")),
                "rel_version": get_attr(r, "attributes", "version"),
                "rel_volume": get_attr(r, "attributes", "volume"),
                "rel_name": get_attr(r, "attributes", "name"),
                "rel_file_name": get_attr(r, "attributes", "fileName"),
            })

    # Tạo DataFrame với dtype được chỉ định
    df = pd.DataFrame(dim_rows)
    
    # Xử lý year column - force string trước, sau đó clean
    if 'year' in df.columns:
        # Convert tất cả thành string trước
        df['year'] = df['year'].astype(str)
        # Clean và convert về integer
        df['year'] = df['year'].str.replace('.0', '').replace('nan', '').replace('None', '')
        # Convert về integer, với error handling
        df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
    
    # Xử lý datetime columns - đảm bảo format đúng
    datetime_cols = ['created_at', 'updated_at']
    for col in datetime_cols:
        if col in df.columns:
            # Đảm bảo format datetime đúng
            df[col] = df[col].astype(str).str.replace('Z', '').str.replace('+00:00', '')
    
    write_csv(df, seed_dir, "dim_manga.csv")
    write_csv(pd.DataFrame(alt_rows), seed_dir, "bridge_manga_alttitle.csv")
    write_csv(pd.DataFrame(desc_rows), seed_dir, "bridge_manga_description.csv")
    write_csv(pd.DataFrame(link_rows), seed_dir, "bridge_manga_links.csv")
    write_csv(pd.DataFrame(tag_rows), seed_dir, "bridge_manga_tag.csv")
    write_csv(pd.DataFrame(rel_rows), seed_dir, "bridge_manga_relationship.csv")


def extract_mangadex_creators(col, seed_dir: str):
    docs = list(col.find({}))
    print(f"[mangadex_creators] {len(docs)} docs")

    dim_rows, bio_rows, rel_rows = [], [], []

    for d in docs:
        data = d.get("data", {}) or {}
        a = data.get("attributes", {}) or {}

        dim_rows.append({
            "creator_id": data.get("id"),
            "type": data.get("type"),
            "name": a.get("name"),
            "created_at": normalize_datetime(a.get("createdAt")),
            "updated_at": normalize_datetime(a.get("updatedAt")),
            "version": a.get("version"),
            "image_url": a.get("imageUrl"),
            "booth": a.get("booth"),
            "fanBox": a.get("fanBox"),
            "fantia": a.get("fantia"),
            "melonBook": a.get("melonBook"),
            "namicomi": a.get("namicomi"),
            "naver": a.get("naver"),
            "nicoVideo": a.get("nicoVideo"),
            "pixiv": a.get("pixiv"),
            "skeb": a.get("skeb"),
            "tumblr": a.get("tumblr"),
            "twitter": a.get("twitter"),
            "website": a.get("website"),
            "weibo": a.get("weibo"),
            "youtube": a.get("youtube")
        })

        for lang, val in (a.get("biography") or {}).items():
            bio_rows.append({
                "creator_id": data.get("id"),
                "lang_code": lang,
                "biography": val
            })

        for r in as_list(data.get("relationships")):
            rel_rows.append({
                "creator_id": data.get("id"),
                "related_id": r.get("id"),
                "related_type": r.get("type")
            })

    write_csv(pd.DataFrame(dim_rows), seed_dir, "dim_creator.csv")
    write_csv(pd.DataFrame(bio_rows), seed_dir, "bridge_creator_biography.csv")
    write_csv(pd.DataFrame(rel_rows), seed_dir, "bridge_creator_relationship.csv")


def extract_mangadex_cover_arts(col, seed_dir: str):
    docs = list(col.find({}))
    print(f"[mangadex_cover_arts] {len(docs)} docs")

    dim_rows, rel_rows = [], []

    for d in docs:
        data = d.get("data", {}) or {}
        a = data.get("attributes", {}) or {}

        dim_rows.append({
            "cover_id": data.get("id"),
            "type": data.get("type"),
            "description": a.get("description"),
            "file_name": a.get("fileName"),
            "locale": a.get("locale"),
            "volume": a.get("volume"),
            "created_at": normalize_datetime(a.get("createdAt")),
            "updated_at": normalize_datetime(a.get("updatedAt")),
            "version": a.get("version"),
        })

        for r in as_list(data.get("relationships")):
            rel_rows.append({
                "cover_id": data.get("id"),
                "related_id": r.get("id"),
                "related_type": r.get("type")
            })

    write_csv(pd.DataFrame(dim_rows), seed_dir, "dim_cover_art.csv")
    write_csv(pd.DataFrame(rel_rows), seed_dir, "bridge_cover_relationship.csv")


def extract_mangadex_related(col, seed_dir: str):
    docs = list(col.find({}))
    print(f"[mangadex_related] {len(docs)} docs")

    rows = []
    for d in docs:
        group_id = d.get("_id")
        fetched_at = d.get("fetched_at")
        for r in as_list(d.get("relationships")):
            rows.append({
                "related_group_id": group_id,
                "fetched_at": fetched_at,
                "entity_id": r.get("id"),
                "entity_type": r.get("type"),
                "relation_type": r.get("related")
            })

    write_csv(pd.DataFrame(rows), seed_dir, "bridge_related.csv")


def extract_mangadex_tags(col, seed_dir: str):
    docs = list(col.find({}))
    print(f"[mangadex_tags] {len(docs)} docs")

    dim_rows, name_rows, desc_rows = [], [], []

    for d in docs:
        tag_id = d.get("_id")
        a = d.get("attributes", {}) or {}

        dim_rows.append({
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

        for lang, val in (a.get("description") or {}).items():
            desc_rows.append({
                "tag_id": tag_id,
                "lang_code": lang,
                "description": val
            })

    write_csv(pd.DataFrame(dim_rows), seed_dir, "dim_tag.csv")
    write_csv(pd.DataFrame(name_rows), seed_dir, "bridge_tag_name.csv")
    write_csv(pd.DataFrame(desc_rows), seed_dir, "bridge_tag_description.csv")


def extract_mangadex_statistics(col, seed_dir: str):
    docs = list(col.find({}))
    print(f"[mangadex_statistics] {len(docs)} docs")

    fact_rows, cm_rows = [], []

    for d in docs:
        stat = d.get("statistics", {}) or {}
        rating = stat.get("rating", {}) or {}

        fact_rows.append({
            "stat_id": d.get("_id"),
            "manga_id": d.get("mangaId"),
            "snapshot_time": d.get("snapshotTime"),
            "fetched_at": d.get("fetched_at"),
            "source": d.get("source"),
            "follows": stat.get("follows"),
            "rating_avg": rating.get("average"),
            "rating_bayesian": rating.get("bayesian"),
            "unavailable_chapters_count": stat.get("unavailableChaptersCount")
        })

        comments = stat.get("comments")
        if isinstance(comments, dict):
            cm_rows.append({
                "stat_id": d.get("_id"),
                "thread_id": comments.get("threadId"),
                "replies_count": comments.get("repliesCount")
            })

    write_csv(pd.DataFrame(fact_rows), seed_dir, "fact_statistics.csv")
    write_csv(pd.DataFrame(cm_rows), seed_dir, "fact_statistics_comments.csv")


def extract_mangadex_chapters(col, seed_dir: str):
    docs = list(col.find({}))
    print(f"[mangadex_chapters] {len(docs)} docs")

    dim_rows, rel_rows = [], []

    for d in docs:
        a = d.get("attributes", {}) or {}
        dim_rows.append({
            "chapter_id": d.get("id") or d.get("_id"),
            "type": d.get("type"),
            "manga_id": d.get("mangaId"),
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
            rel_rows.append({
                "chapter_id": d.get("id") or d.get("_id"),
                "related_id": r.get("id"),
                "related_type": r.get("type")
            })

    write_csv(pd.DataFrame(dim_rows), seed_dir, "dim_chapter.csv")
    write_csv(pd.DataFrame(rel_rows), seed_dir, "bridge_chapter_relationship.csv")


def extract_mangadex_groups(col, seed_dir: str):
    docs = list(col.find({}))
    print(f"[mangadex_groups] {len(docs)} docs")

    dim_rows, alt_rows, lang_rows, rel_rows = [], [], [], []

    for d in docs:
        data = d.get("data", {}) or {}
        a = data.get("attributes", {}) or {}

        dim_rows.append({
            "group_id": data.get("id"),
            "type": data.get("type"),
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
            if not isinstance(alt, dict):
                continue
            for lang, val in alt.items():
                alt_rows.append({
                    "group_id": data.get("id"),
                    "lang_code": lang,
                    "alt_name": val
                })

        for lang in as_list(a.get("focusedLanguages")):
            lang_rows.append({
                "group_id": data.get("id"),
                "lang_code": lang
            })

        for r in as_list(data.get("relationships")):
            rel_rows.append({
                "group_id": data.get("id"),
                "related_id": r.get("id"),
                "related_type": r.get("type")
            })

    write_csv(pd.DataFrame(dim_rows), seed_dir, "dim_group.csv")
    write_csv(pd.DataFrame(alt_rows), seed_dir, "bridge_group_altname.csv")
    write_csv(pd.DataFrame(lang_rows), seed_dir, "bridge_group_language.csv")
    write_csv(pd.DataFrame(rel_rows), seed_dir, "bridge_group_relationship.csv")


# ------------------------------
# Main
# ------------------------------
def main():
    parser = argparse.ArgumentParser(description="Export MongoDB collections to dbt seed CSVs.")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017/", help="MongoDB URI")
    parser.add_argument("--db", default="manga_raw_data", help="Database name")
    parser.add_argument("--seed-dir", default=r"D:\Projects\Học DE\data-engineering-learning\2025-08-16\mongo_to_db\seeds", help="Output seeds directory")
    # Cho phép bỏ qua collection nào đó nếu muốn
    parser.add_argument("--skip", nargs="*", default=[], help="Danh sách collection (alias) muốn bỏ qua. Ví dụ: mangadex_groups mangadex_chapters")
    args = parser.parse_args()

    ensure_dir(args.seed_dir)

    client = MongoClient(args.mongo_uri)
    db = client[args.db]

    # Map alias -> (collection_name, extractor_fn)
    jobs = [
        ("mangadex_manga", "mangadex_manga", extract_mangadex_manga),
        ("mangadex_creators", "mangadex_creators", extract_mangadex_creators),
        ("mangadex_cover_arts", "mangadex_cover_arts", extract_mangadex_cover_arts),
        ("mangadex_related", "mangadex_related", extract_mangadex_related),
        ("mangadex_tags", "mangadex_tags", extract_mangadex_tags),
        ("mangadex_statistics", "mangadex_statistics", extract_mangadex_statistics),
        ("mangadex_chapters", "mangadex_chapters", extract_mangadex_chapters),
        ("mangadex_groups", "mangadex_groups", extract_mangadex_groups),
    ]

    for alias, coll_name, fn in jobs:
        if alias in args.skip:
            print(f"[SKIP] {alias}")
            continue
        if coll_name not in db.list_collection_names():
            print(f"[MISS] {alias} -> collection '{coll_name}' không tồn tại, bỏ qua.")
            continue
        print(f"[RUN ] {alias} -> '{coll_name}'")
        col = db[coll_name]
        fn(col, args.seed_dir)

    print("\nHoàn tất xuất CSV seeds. Bạn có thể chạy:  dbt seed")


if __name__ == "__main__":
    main()
