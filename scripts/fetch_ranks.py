"""
Fetch Naver Map search results (FREE) and compute rank for configured places.

- 비용 0원: Apify 미사용
- 주의: 네이버가 구조/차단을 바꾸면 실패할 수 있어요(고쳐쓰는 방식)
"""
from __future__ import annotations

import os
import time
import random
import datetime as dt
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

NAVER_SEARCH_ENDPOINT = "https://map.naver.com/v5/api/search"

# 너무 빠르게 치면 차단될 수 있으니, 느리게/랜덤하게
MIN_SLEEP = 1.2
MAX_SLEEP = 2.8

def _headers() -> Dict[str, str]:
    # 네이버는 User-Agent 없으면 막는 경우가 있어요
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://map.naver.com/",
    }

def naver_search(keyword: str, max_results: int = 120) -> List[Dict[str, Any]]:
    """
    네이버 지도 검색 결과(place list)를 여러 페이지로 받아서 합칩니다.
    - displayCount=40 기준으로 page를 넘기며 max_results까지 수집
    """
    session = requests.Session()
    results: List[Dict[str, Any]] = []

    display_count = 40
    max_pages = max(1, (max_results + display_count - 1) // display_count)

    for page in range(1, max_pages + 1):
        params = {
            "caller": "pcweb",
            "query": keyword,
            "type": "all",
            "page": page,
            "displayCount": display_count,
            "lang": "ko",
        }

        # 간단 재시도(가끔 503/일시차단)
        last_err = None
        for _ in range(3):
            try:
                resp = session.get(
                    NAVER_SEARCH_ENDPOINT,
                    params=params,
                    headers=_headers(),
                    timeout=20,
                )
                resp.raise_for_status()
                data = resp.json()
                place_list = (
                    (data.get("result") or {})
                    .get("place", {})
                    .get("list", [])
                )
                # 결과 없으면 더 이상 페이지를 볼 필요 없음
                if not place_list:
                    return results

                for p in place_list:
                    pid = str(
                        p.get("id")
                        or p.get("sid")
                        or p.get("placeId")
                        or ""
                    ).strip()
                    name = str(p.get("name") or p.get("title") or "").strip()
                    if not pid:
                        continue
                    results.append(
                        {
                            "placeId": pid,
                            "name": name,
                            "url": f"https://pcmap.place.naver.com/place/{pid}",
                        }
                    )
                    if len(results) >= max_results:
                        return results

                break  # 이 페이지 성공했으니 재시도 루프 종료
            except Exception as e:
                last_err = e
                time.sleep(1.5)
        if last_err:
            # 3번 재시도 후에도 실패하면 그 시점에서 중단
            raise RuntimeError(f"Naver search failed: {keyword} (page={page})") from last_err

    return results

def normalize_text(s: str) -> str:
    return " ".join((s or "").strip().split())

def find_rank(items: List[Dict[str, Any]], place_id: str, place_name: str) -> Optional[int]:
    pid = str(place_id)
    pname = normalize_text(place_name)

    for idx, it in enumerate(items, start=1):
        if str(it.get("placeId") or "") == pid:
            return idx

    # 혹시 id가 안 들어오는 상황 대비(이름 fallback)
    for idx, it in enumerate(items, start=1):
        if normalize_text(str(it.get("name") or "")) == pname and pname:
            return idx

    return None

def run_daily(config_dir: str = "config", data_dir: str = "data") -> pd.DataFrame:
    places = pd.read_csv(os.path.join(config_dir, "places.csv"), dtype=str)
    kws = pd.read_csv(os.path.join(config_dir, "keywords.csv"), dtype=str)

    now = dt.datetime.now(dt.timezone(dt.timedelta(hours=9)))  # KST
    rows = []

    grouped = kws.groupby("keyword")["place_id"].apply(list).to_dict()
    for keyword, place_ids in grouped.items():
        time.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))
        items = naver_search(keyword, max_results=120)

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
