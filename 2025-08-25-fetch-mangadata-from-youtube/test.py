from pymongo import MongoClient

def fetch_manga_titles():
    # === Kết nối MongoDB ===
    client = MongoClient("mongodb://localhost:27017/")
    db = client["manga_raw_data"]
    collection = db["mangadex_manga"]

    results = []

    # === Truy vấn toàn bộ collection ===
    for doc in collection.find({}, {"id": 1, "attributes.title": 1, "attributes.altTitles": 1}):
        manga_id = doc.get("id")
        attributes = doc.get("attributes", {})

        # --- Lấy title chính ---
        titles = []
        title_obj = attributes.get("title", {})
        for lang in ["en", "vi"]:
            if lang in title_obj:
                titles.append(title_obj[lang])

        # --- Lấy altTitles ---
        alt_titles = attributes.get("altTitles", [])
        for alt in alt_titles:
            for lang in ["en", "vi"]:
                if lang in alt:
                    titles.append(alt[lang])

        # Loại bỏ trùng lặp (nếu có)
        unique_titles = list(dict.fromkeys(titles))

        results.append({
            "manga_id": manga_id,
            "titles": unique_titles
        })

    return results


if __name__ == "__main__":
    data = fetch_manga_titles()
    for item in data[:10]:  # in thử 10 bản ghi đầu
        print(item)