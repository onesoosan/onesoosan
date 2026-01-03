\
"""
Fetch Naver Map search results via Apify and compute rank for configured places.

Requires APIFY_TOKEN env var (GitHub Actions secret or Streamlit Cloud secret).
"""
from __future__ import annotations

import os
import re
import time
import random
import datetime as dt
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

APIFY_ACTOR = "delicious_zebu~naver-map-search-results-scraper"
RUN_SYNC_ENDPOINT = f"https://api.apify.com/v2/acts/{APIFY_ACTOR}/run-sync-get-dataset-items"

PLACE_ID_RE = re.compile(r"/place/(\d+)|place/(\d+)|placeId=(\d+)|place/(\d+)\b")

def _extract_place_id_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = PLACE_ID_RE.search(url)
    if m:
        for g in m.groups():
            if g:
                return g
    digits = re.findall(r"\b\d{7,}\b", url)
    return digits[0] if digits else None

def apify_search(keyword: str, token: str, max_results: int = 60) -> List[Dict[str, Any]]:
    payload = {"keywords": [keyword]}
    resp = requests.post(RUN_SYNC_ENDPOINT, params={"token": token}, json=payload, timeout=180)
    resp.raise_for_status()
    items = resp.json()
    if isinstance(items, list):
        return items[:max_results]
    return []

def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def find_rank(items: List[Dict[str, Any]], place_id: str, place_name: str) -> Optional[int]:
    pid = str(place_id)
    pname = normalize_text(place_name)
    # 1) id match via urls
    for idx, it in enumerate(items, start=1):
        url = str(it.get("placeUrl") or it.get("url") or it.get("link") or "")
        extracted = _extract_place_id_from_url(url)
        if extracted and str(extracted) == pid:
            return idx
    # 2) fallback: exact-ish name match
    for idx, it in enumerate(items, start=1):
        name = normalize_text(str(it.get("title") or it.get("name") or it.get("placeName") or ""))
        if name and pname and name == pname:
            return idx
    return None

def run_daily(config_dir: str = "config", data_dir: str = "data") -> pd.DataFrame:
    token = os.getenv("APIFY_TOKEN", "").strip()
    if not token:
        raise RuntimeError("APIFY_TOKEN is missing. Set it as environment variable/secret.")

    places = pd.read_csv(os.path.join(config_dir, "places.csv"), dtype=str)
    kws = pd.read_csv(os.path.join(config_dir, "keywords.csv"), dtype=str)

    now = dt.datetime.now(dt.timezone(dt.timedelta(hours=9)))  # KST
    rows = []

    grouped = kws.groupby("keyword")["place_id"].apply(list).to_dict()
    for keyword, place_ids in grouped.items():
        time.sleep(random.uniform(1.2, 2.8))
        items = apify_search(keyword, token)
        for pid in place_ids:
            pname = places.loc[places["place_id"] == pid, "place_name"].iloc[0]
            rank = find_rank(items, pid, pname)
            rows.append({
                "timestamp_kst": now.strftime("%Y-%m-%d %H:%M:%S"),
                "date_kst": now.strftime("%Y-%m-%d"),
                "place_id": pid,
                "place_name": pname,
                "keyword": keyword,
                "rank": int(rank) if rank is not None else None,
            })

    df = pd.DataFrame(rows)
    os.makedirs(data_dir, exist_ok=True)
    out_path = os.path.join(data_dir, "rank_history.csv")
    if os.path.exists(out_path):
        old = pd.read_csv(out_path)
        pd.concat([old, df], ignore_index=True).to_csv(out_path, index=False)
    else:
        df.to_csv(out_path, index=False)
    return df

if __name__ == "__main__":
    run_daily()
