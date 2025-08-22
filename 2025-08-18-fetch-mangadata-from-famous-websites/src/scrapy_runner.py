# src/scrapy_runner.py
"""
Optional helper: run a scrapy runspider script as a subprocess.
Usage:
    from src.scrapy_runner import run_scrapy_runspider
    run_scrapy_runspider("spiders/animeplanet_spider.py", ["-a", "slug=tower-of-god"])
"""

import subprocess
import shlex
import os
from typing import List, Tuple

def run_scrapy_runspider(script_path: str, extra_args: List[str] = None, env: dict = None) -> Tuple[int, str, str]:
    """
    Run: scrapy runspider <script_path> <extra_args...>
    Returns (returncode, stdout, stderr)
    """
    if extra_args is None:
        extra_args = []
    cmd = ["scrapy", "runspider", script_path] + extra_args
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env={**os.environ, **(env or {})})
    out, err = proc.communicate()
    return proc.returncode, out.decode("utf-8", errors="replace"), err.decode("utf-8", errors="replace")