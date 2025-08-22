import os, gzip, json, datetime, hashlib

from .config import DATA_LAKE_ROOT

def _date_parts(dt=None):
    if not dt:
        dt = datetime.datetime.utcnow()
    return dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d"), dt

def _safe_name(key: str):
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return digest

def write_jsonl(source: str, key: str, envelope: dict):
    y, m, d, now = _date_parts()
    out_dir = os.path.join(DATA_LAKE_ROOT, source, f"YYYY={y}", f"MM={m}", f"DD={d}")
    os.makedirs(out_dir, exist_ok=True)
    fname = f"{_safe_name(key)}.jsonl.gz"
    path = os.path.join(out_dir, fname)
    line = json.dumps(envelope, ensure_ascii=False) + "\n"
    with gzip.open(path, "at", encoding="utf-8") as f:
        f.write(line)
    return path