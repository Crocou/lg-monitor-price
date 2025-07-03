#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Amazon 독일(amazon.de)  ▸ Computer Accessories & Monitors ▸ Monitors
베스트셀러 1~100위 중 'LG'가 포함된 모니터만 추려서
Google Sheet(History·Today 탭)에 저장 + 변동률 계산
"""
import os, re, json, datetime, requests, pandas as pd, gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────────────────────────
# 1) Amazon 베스트셀러 페이지 스크랩
# ─────────────────────────────────────────────────────────────
URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
}
resp = requests.get(URL, headers=HEADERS, timeout=30)
resp.raise_for_status()

if "Enter the characters you see below" in resp.text:
    raise RuntimeError("Amazon CAPTCHA에 걸렸습니다. 잠시 후 다시 시도하세요.")

soup = BeautifulSoup(resp.text, "lxml")

# 최신(2025-07) 카드 셀렉터
cards = soup.select("div.zg-grid-general-faceout")
if not cards:  # 혹시 구조가 또 바뀌었을 경우 예비 셀렉터
    cards = soup.select("div.p13n-sc-uncoverable-faceout")

items = []
for rank, card in enumerate(cards, start=1):  # 1부터 순위 부여
    img = card.select_one("img")
    title = (img.get("alt") or "").strip() if img else ""
    link_tag = card.select_one("a.a-link-normal")
    if not link_tag:
        continue
    # 고정 URL(추가 파라미터 제거)
    link = "https://www.amazon.de" + link_tag["href"].split("?", 1)[0]
    asin_match = re.search(r"/dp/([A-Z0-9]{10})", link)
    asin = asin_match.group(1) if asin_match else None

    # === LG 모니터 필터 ===
    if "LG" in title.upper():
        items.append({"asin": asin, "title": title, "rank": rank, "url": link})

if not items:
    raise RuntimeError("LG 모니터가 목록에 없습니다! HTML 구조가 바뀌었을 가능성 ↑")

df_today = pd.DataFrame(items).sort_values("rank").reset_index(drop=True)
df_today["date"] = datetime.date.today().isoformat()

print(f"총 스크랩 상품 수   : {len(cards)}")
print(f"LG 모니터 발견 수  : {len(df_today)}")

# ─────────────────────────────────────────────────────────────
# 2) Google Sheets 연결
# ─────────────────────────────────────────────────────────────
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.environ["SHEET_ID"]  # GitHub Secret

creds = Credentials.from_service_account_info(
    json.loads(os.environ["GCP_SA_JSON"]), scopes=SCOPES
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)

def get_or_create(name: str, rows=1000, cols=20):
    try:
        return sh.worksheet(name)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(name, rows, cols)

ws_hist  = get_or_create("History")
ws_today = get_or_create("Today")

# ─────────────────────────────────────────────────────────────
# 3) 변동률(Δ) 계산
# ─────────────────────────────────────────────────────────────
try:
    prev_df = pd.DataFrame(ws_hist.get_all_records()).dropna()
except gspread.exceptions.APIError:
    prev_df = pd.DataFrame()

if not prev_df.empty:
    latest_prev = (
        prev_df.sort_values("date")
        .groupby("asin", as_index=False)
        .last()[["asin", "rank"]]
        .rename(columns={"rank": "rank_prev"})
    )
    df_today = df_today.merge(latest_prev, on="asin", how="left")
    df_today["delta"] = df_today["rank_prev"] - df_today["rank"]
else:
    df_today["delta"] = None

# ─────────────────────────────────────────────────────────────
# 4) 시트 업데이트
# ─────────────────────────────────────────────────────────────
# 4-1) History 탭: 누적 append
ws_hist.append_rows(df_today.values.tolist(), value_input_option="RAW")

# 4-2) Today 탭: 덮어쓰기
ws_today.clear()
ws_today.update(
    [df_today.columns.values.tolist()] + df_today.values.tolist(),
    value_input_option="RAW"
)

print("✓ Google Sheet 업데이트 완료")
