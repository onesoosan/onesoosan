\
from __future__ import annotations

import os
import pandas as pd
import streamlit as st
import subprocess
import sys

st.set_page_config(page_title="원수산/낙원 플레이스 순위", layout="wide")

st.title("📍 원수산/낙원 네이버 플레이스 키워드 순위")
st.caption("매일 오전 10시(KST) 자동 수집된 결과를 보여줍니다. (휴대폰에서 링크만 열면 확인)")

DATA_PATH = os.path.join("data", "rank_history.csv")

@st.cache_data(ttl=60)
def load_data() -> pd.DataFrame:
    if not os.path.exists(DATA_PATH):
        return pd.DataFrame(columns=["timestamp_kst","date_kst","place_id","place_name","keyword","rank"])
    df = pd.read_csv(DATA_PATH)
    if "rank" in df.columns:
        df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    return df

df = load_data()

c1, c2, c3 = st.columns([1,1,2])
with c1:
    st.metric("누적 기록", f"{len(df):,}행")
with c2:
    st.metric("최근 업데이트", df["timestamp_kst"].iloc[-1] if len(df) else "-")
with c3:
    st.info("데이터가 비어 있으면: GitHub Actions 첫 실행 전이거나 APIFY_TOKEN 설정이 필요할 수 있어요.", icon="ℹ️")

with st.expander("🔄 지금 즉시 업데이트(선택)", expanded=False):
    st.write("Streamlit Cloud Secrets에 `APIFY_TOKEN`을 넣으면 즉시 업데이트를 실행할 수 있습니다.")
    if st.button("지금 업데이트 실행"):
        try:
            subprocess.run([sys.executable, "scripts/daily_update.py"], check=True)
            st.success("업데이트 완료! 잠시 후 새로고침하면 반영됩니다.")
            st.cache_data.clear()
        except Exception as e:
            st.error(f"업데이트 실패: {e}")

df = load_data()
if df.empty:
    st.warning("아직 데이터가 없습니다. GitHub Actions 실행 후 다시 확인해주세요.")
    st.stop()

places = sorted(df["place_name"].dropna().unique().tolist())
place = st.selectbox("매장", ["전체"] + places)

view = df if place == "전체" else df[df["place_name"] == place].copy()
dates = sorted(view["date_kst"].dropna().unique().tolist())
sel_date = st.selectbox("날짜(KST)", dates, index=len(dates)-1 if dates else 0)

day = view[view["date_kst"] == sel_date].copy()

# prev day for delta
prev_date = None
if sel_date in dates:
    i = dates.index(sel_date)
    if i > 0:
        prev_date = dates[i-1]

if prev_date:
    prev = view[view["date_kst"] == prev_date][["place_name","keyword","rank"]].rename(columns={"rank":"rank_prev"})
    day = day.merge(prev, on=["place_name","keyword"], how="left")
    def arrow(r):
        if pd.isna(r["rank"]) or pd.isna(r["rank_prev"]):
            return "–"
        if r["rank"] < r["rank_prev"]:
            return f"▲ {int(r['rank_prev']-r['rank'])}"
        if r["rank"] > r["rank_prev"]:
            return f"▼ {int(r['rank']-r['rank_prev'])}"
        return "–"
    day["변동"] = day.apply(arrow, axis=1)
else:
    day["변동"] = "–"

table = day[["place_name","keyword","rank","변동"]].rename(columns={"place_name":"매장","keyword":"키워드","rank":"순위"})
table = table.sort_values(["매장","키워드"]).reset_index(drop=True)

st.subheader(f"📊 {sel_date} 순위표")
st.dataframe(table, use_container_width=True, hide_index=True)

st.subheader("📈 최근 14일 추세")
kw = st.selectbox("키워드", sorted(view["keyword"].unique().tolist()))
trend = view[view["keyword"] == kw].copy()
trend["date_kst"] = pd.to_datetime(trend["date_kst"], errors="coerce")
trend = trend.dropna(subset=["date_kst"])
trend = trend[trend["date_kst"] >= trend["date_kst"].max() - pd.Timedelta(days=13)]
pivot = trend.pivot_table(index="date_kst", columns="place_name", values="rank", aggfunc="min").sort_index()
st.line_chart(pivot)

st.caption("※ 순위는 작을수록 상위입니다. None이면 수집/매칭 실패일 수 있어요.")
