#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Amazon.de ▸ Monitors 베스트셀러 → LG 제품만 필터
→ Google Sheets ▸ Today, History 탭에 가격·변동 포함 기록
"""

import os, re, json, base64, datetime, requests, pandas as pd, gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# ─────── 1. Amazon Bestsellers 스크래핑 ────────
URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
}

resp = requests.get(URL, headers=HEADERS, timeout=30)
resp.raise_for_status()

if "Enter the characters you see below" in resp.text:
    raise RuntimeError("Amazon CAPTCHA에 걸렸습니다. 잠시 후 다시 시도해주세요.")

soup = BeautifulSoup(resp.text, "lxml")
cards = soup.select("div.zg-grid-general-faceout") or \
        soup.select("div.p13n-sc-uncoverable-faceout")

items = []
for rank, card in enumerate(cards, start=1):
    # 제목
    title = (card.select_one("img") or {}).get("alt", "").strip()
    # 링크
    link_tag = card.select_one("a.a-link-normal")
    if not link_tag:
        continue
    link = "https://www.amazon.de" + link_tag["href"].split("?", 1)[0]
    asin = re.search(r"/dp/([A-Z0-9]{10})", link)
    asin = asin.group(1) if asin else None
    # 가격
    price_tag = card.select_one(".p13n-sc-price")
    price_str = price_tag.text.strip().replace("€", "").replace(",", ".") if price_tag else ""
    try:
        price = float(price_str)
    except:
        price = None

    if "LG" in title.upper():
        items.append({
            "asin": asin,
            "title": title,
            "rank": rank,
            "price": price,
            "url": link
        })

if not items:
    raise RuntimeError("LG 모니터가 목록에 없습니다! HTML 구조 변경 가능성 ↑")

df_today = pd.DataFrame(items).sort_values("rank").reset_index(drop=True)

# 시간대 설정 (독일 기준)
import pytz
Seoul = pytz.timezone("Asia/Seoul")
now = datetime.datetime.now(berlin)
df_today["date"] = now.strftime("%Y-%m-%d %H:%M")

print(f"총 스크랩 상품 수   : {len(cards)}")
print(f"LG 모니터 발견 수  : {len(df_today)}")

# ─────── 2. Google Sheets 인증 ────────
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.environ["SHEET_ID"]
sa_json  = base64.b64decode(os.environ["GCP_SA_BASE64"]).decode("utf-8")
creds    = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)

def ws(name, rows=1000, cols=20):
    try:
        return sh.worksheet(name)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(name, rows, cols)

ws_hist  = ws("History")
ws_today = ws("Today")

# ─────── 3. 변동률 계산 ────────
try:
    prev = pd.DataFrame(ws_hist.get_all_records()).dropna()
except:
    prev = pd.DataFrame()

if not prev.empty:
    latest_prev = (
        prev.sort_values("date")
        .groupby("asin", as_index=False)
        .last()[["asin", "rank"]]
        .rename(columns={"rank": "rank_prev"})
    )
    df_today = df_today.merge(latest_prev, on="asin", how="left")
    df_today["delta"] = df_today["rank_prev"] - df_today["rank"]
else:
    df_today["delta"] = None

# ─────── 4. 시트 업데이트 ────────
ws_hist.append_rows(df_today.values.tolist(), value_input_option="RAW")
ws_today.clear()
ws_today.update(
    [df_today.columns.values.tolist()] + df_today.values.tolist(),
    value_input_option="RAW"
)

print("✓ Google Sheet 업데이트 완료")
