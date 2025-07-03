#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, json, base64, datetime, requests, pandas as pd, gspread, pytz
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# ────────────────────────────────
# 1. Amazon HTML 가져오기
# ────────────────────────────────
URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8",
}
resp = requests.get(URL, headers=HEADERS, timeout=30)
resp.raise_for_status()

if "Enter the characters you see below" in resp.text:
    raise RuntimeError("Amazon CAPTCHA에 걸렸습니다. 잠시 후 다시 시도하세요.")

soup = BeautifulSoup(resp.text, "lxml")

# ────────────────────────────────
# 2. 카드 파싱 (data-p13n-asin-metadata)
# ────────────────────────────────
cards = soup.select("[data-p13n-asin-metadata]")
print("스크랩된 카드 수 :", len(cards))

items = []
for rank, card in enumerate(cards, start=1):
    meta_json = card.get("data-p13n-asin-metadata")
    if not meta_json:
        continue
    meta = json.loads(meta_json)
    asin  = meta.get("asin")
    title = meta.get("title", "").strip()

    # 링크
    link_tag = card.select_one("a.a-link-normal")
    link = "https://www.amazon.de" + link_tag["href"].split("?", 1)[0] if link_tag else ""

    # 가격: 우선 p13n-sc-price, 없으면 a-price
    price_tag = card.select_one(".p13n-sc-price") or card.select_one("span.a-price > span.a-offscreen")
    price_str = price_tag.text.strip().replace("€", "").replace(",", ".") if price_tag else ""
    try:
        price = float(price_str)
    except:
        price = None

    items.append(
        {"asin": asin, "title": title, "rank": rank, "price": price, "url": link}
    )

# 디버그: 첫 5개 제목 확인
print("카드 샘플 제목 5개:", [i["title"] for i in items[:5]])

# ────────────────────────────────
# 3. LG 모니터 필터
# ────────────────────────────────
LG_KEYS = [" LG ", "LG-", "(LG", "ULTRAGEAR", "ULTRAFINE", "27GR", "32GN", "34WN", "29WP", "38WN"]
lg_items = [
    i for i in items
    if any(k in i["title"].upper() for k in LG_KEYS)
]

print("LG 모니터 발견 수 :", len(lg_items))
if not lg_items:
    print("⚠️  LG 모니터가 0개입니다. HTML 구조 변경 또는 키워드 문제일 수 있습니다.")
    # exit(0)  # 실패 처리하지 않으려면 주석 해제
    # raise RuntimeError("LG 모니터가 목록에 없습니다!")

df_today = pd.DataFrame(lg_items).sort_values("rank").reset_index(drop=True)

# 날짜-시간 (독일 베를린)
berlin = pytz.timezone("Europe/Berlin")
df_today["date"] = datetime.datetime.now(berlin).strftime("%Y-%m-%d %H:%M:%S")

# ────────────────────────────────
# 4. Google Sheets 연결
# ────────────────────────────────
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.environ["SHEET_ID"]
sa_json  = base64.b64decode(os.environ["GCP_SA_BASE64"]).decode("utf-8")
creds    = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
gc       = gspread.authorize(creds)
sh       = gc.open_by_key(SHEET_ID)

def ws(name, rows=1000, cols=20):
    try:
        return sh.worksheet(name)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(name, rows, cols)

ws_hist  = ws("History")
ws_today = ws("Today")

# ────────────────────────────────
# 5. 변동률(delta) 계산
# ────────────────────────────────
try:
    prev = pd.DataFrame(ws_hist.get_all_records()).dropna()
except Exception:
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

# ────────────────────────────────
# 6. Sheets 업데이트
# ────────────────────────────────
if not df_today.empty:
    ws_hist.append_rows(df_today.values.tolist(), value_input_option="RAW")

ws_today.clear()
ws_today.update(
    [df_today.columns.values.tolist()] + df_today.values.tolist(),
    value_input_option="RAW"
)

print("✓ Google Sheet 업데이트 완료")
