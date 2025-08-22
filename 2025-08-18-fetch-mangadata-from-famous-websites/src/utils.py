import re
import time
import uuid
from datetime import datetime, timezone

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def is_digits(s: str) -> bool:
    return bool(re.fullmatch(r"\d+", str(s).strip()))

def new_trace_id():
    return uuid.uuid4().hex

def ms(start_ts):
    return int((time.time() - start_ts) * 1000)