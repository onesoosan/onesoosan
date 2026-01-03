# Wonsusan / Nakwon Naver Place Rank Dashboard (Streamlit + GitHub Actions)

이 프로젝트는 **매일 오전 10시(KST)**에 네이버 지도(플레이스) 검색결과를 수집해
`data/rank_history.csv`에 누적 저장하고, Streamlit 대시보드로 **휴대폰에서 링크만 열어 확인**할 수 있게 합니다.

> ⚠️ 주의: 네이버 페이지 구조/차단 정책이 바뀌면 수집이 실패할 수 있습니다.
> 가장 안정적인 방식으로 **Apify의 Naver Map Search Results Scraper**를 기본 제공(유료/사용량 과금)합니다.

## 준비물
- GitHub 계정
- (권장) Apify 계정 + API Token
- Streamlit Community Cloud 계정(무료)

## 우리 가게/키워드
- 원수산 삼덕 (place_id: 1094913642)
- 원수산 수성못 (place_id: 1480703327)
- 낙원물회식당 (place_id: 2039347115)

키워드 수정: `config/keywords.csv`

## 로컬 실행
```bash
pip install -r requirements.txt
streamlit run app.py
```

## 자동 수집(GitHub Actions)
- 워크플로: `.github/workflows/daily_update.yml`
- GitHub Repo → Settings → Secrets and variables → Actions → New repository secret  
  - `APIFY_TOKEN` : Apify API Token

## Streamlit Cloud 배포(모바일 링크)
1) GitHub에 코드 푸시
2) Streamlit Cloud → New app → repo 선택 → `app.py` → Deploy
3) (선택) Streamlit Cloud Secrets에 토큰 추가
```toml
APIFY_TOKEN="YOUR_TOKEN"
```

### 참고
- Apify Actor: delicious_zebu/naver-map-search-results-scraper
- Apify API: https://apify.com/delicious_zebu/naver-map-search-results-scraper/api
