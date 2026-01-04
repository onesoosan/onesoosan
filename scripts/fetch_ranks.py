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

# 엔드포인트가 종종 바뀌거나 A/B가 있어서 2개를 모두 시도
NAVER_ENDPOINTS = [
    "https://map.naver.com/p/api/search",   # 요즘 로그에 잡히는 경로
    "https://map.naver.com/v5/api/search",  # 예전 경로(가끔 이게 더 잘 됨)
]

# 너무 빠르면 차단되니 천천히
MIN_SLEEP = 2.5
MAX_SLEEP = 5.0

# 재시도 설정
RETRY_STATUS = {429, 500, 502, 503, 504}
MAX_RETRIES = 7
BASE_BACKOFF = 2.0  # seconds


def _headers() -> Dict[str, str]:
    # 헤더를 조금 더 브라우저처럼
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://map.naver.com/",
        "Connection": "close",
    }


def _get_json_with_retry(session: requests.Session, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    last_err: Optional[Exception] = None

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, params=params, headers=_headers(), timeout=25)
            # 429/503 같은 경우는 대기 후 재시도
            if resp.status_code in RETRY_STATUS:
                sleep_s = BASE_BACKOFF * (2 ** attempt) + random.uniform(0.0, 1.5)
                time.sleep(min(sleep_s, 60))  # 최대 60초까지만
                continue

            resp.raise_for_status()

            # JSON이 아닌 HTML이 오면(차단 페이지 등) 예외로 처리해서 재시도
            ct = (resp.headers.get("content-type") or "").lower()
            if "json" not in ct:
                raise ValueError(f"Non-JSON response (content-type={ct})")

            return resp.json()

        except Exception as e:
            last_err = e
            # 마지막 시도 전까지는 조금 쉬었다가 재시도
            if attempt < MAX_RETRIES - 1:
                time.sleep(1.5 + random.uniform(0.0, 1.0))

    raise RuntimeError(f"Request failed after retries: {url}") from last_err


def naver_search(keyword: str, max_results: int = 120) -> List[Dict[str, Any]]:
    """
    네이버 지도 검색 결과(place list)를 여러 페이지로 받아서 합칩니다.
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

        data: Optional[Dict[str, Any]] = None
        last_endpoint_err: Optional[Exception] = None

        # 엔드포인트 2개를 순서대로 시도
        for endpoint in NAVER_ENDPOINTS:
            try:
                data = _get_json_with_retry(session, endpoint, params)
                break
            except Exception as e:
                last_endpoint_err = e
                continue

        if data is None:
            raise RuntimeError(f"Naver search failed (all endpoints): {keyword} (page={page})") from last_endpoint_err

        place_list = ((data.get("result") or {}).get("place", {}) or {}).get("list", [])

        if not place_list:
            return results

        for p in place_list:
            pid = str(p.get("id") or p.get("sid") or p.get("placeId") or "").strip()
            name = str(p.get("name") or p.get("title") or "").strip()
            if not pid:
                continue
            results.append(
                {"placeId": pid, "name": name, "url": f"https://pcmap.place.naver.com/place/{pid}"}
            )
            if len(results) >= max_results:
                return results

        # 페이지 사이도 쉬기
        time.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP) / 2)

    return results


def normalize_text(s: str) -> str:
    return " ".join((s or "").strip().split())


def find_rank(items: List[Dict[str, Any]], place_id: str, place_name: str) -> Optional[int]:
    pid = str(place_id)
    pname = normalize_text(place_name)

    for idx, it in enumerate(items, start=1):
        if str(it.get("placeId") or "") == pid:
            return idx

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

        try:
            items = naver_search(keyword, max_results=120)
        except Exception:
            # 여기서 죽지 않고, 해당 키워드만 None 처리하고 다음 키워드로 진행
            items = []

        for pid in place_ids:
            pname = places.loc[places["place_id"] == pid, "place_name"].iloc[0]
            rank = find_rank(items, pid, pname) if items else None
            rows.append(
                {
                    "timestamp_kst": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "date_kst": now.strftime("%Y-%m-%d"),
                    "place_id": pid,
                    "place_name": pname,
                    "keyword": keyword,
                    "rank": int(rank) if rank is not None else None,
                }
            )

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

