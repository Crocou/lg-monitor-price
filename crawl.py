#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Amazon.de ▸ Computer Accessories & Monitors ▸ Monitors 베스트셀러 1~100위
→ 'LG' 모니터만 수집, Google Sheet(History·Today)에 저장 + 순위 변동률 계산
"""

import os, re, json, base64, datetime, requests, pandas as pd, gspread, pytz
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# ────────────── 1) Amazon 페이지 요청 ──────────────
URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
}
COOKIES = {"lc-main": "de_DE", "i18n-prefs": "EUR"}  # 항상 독일·€ 고정

html = requests.get(URL, headers=HEADERS, cookies=COOKIES, timeout=30).text
if "Enter the characters you see below" in html:
    raise RuntimeError("Amazon CAPTCHA! 잠시 후 재시도하세요.")

soup  = BeautifulSoup(html, "lxml")
cards = soup.select("div.zg-grid-general-faceout") or \
        soup.select("div.p13n-sc-uncoverable-faceout")

# ────────────── 2) 카드 → 제목·ASIN·링크 파싱 ──────────────
def pick_title(card):
    # ① 최신 실험 UI (line-clamp 클래스)
    t = card.select_one('span[class*="p13n-sc-css-line-clamp"]')
    if t and t.get_text(strip=True):
        return t.get_text(strip=True)

    # ② title 속성
    t = card.select_one('[title]')
    if t and t['title'].strip():
        return t['title'].strip()

    # ③ 구형 데스크톱 UI
    t = card.select_one('.p13n-sc-truncate-desktop-type2')
    if t and t.get_text(strip=True):
        return t.get_text(strip=True)

    # ④ 모바일/센터 정렬
    t = card.select_one('.zg-text-center-align span.a-size-base')
    if t and t.get_text(strip=True):
        return t.get_text(strip=True)

    # ⑤ 이미지 alt (최후 수단)
    img = card.select_one('img')
    if img and img.get('alt', '').strip():
        return img['alt'].strip()
    return ""

items = []
for rank, card in enumerate(cards, start=1):
    a = card.select_one("a.a-link-normal[href*='/dp/']")
    if not a:
        continue
    title = pick_title(card) or a.get_text(" ", strip=True)
    link  = "https://www.amazon.de" + a["href"].split("?", 1)[0]
    m = re.search(r"/dp/([A-Z0-9]{10})", link)
    asin = m.group(1) if m else None

    if re.search(r"\bLG\b", title, re.I):
        items.append({"asin": asin, "title": title, "rank": rank, "url": link})

# 디버그: 카드 샘플 5줄 출력
for i, c in enumerate(cards[:5], 1):
    print(f"[DBG] card {i}: {c.get_text(' ', strip=True)[:120]}")

if not items:
    raise RuntimeError("LG 모니터가 목록에 없습니다! ↑ 위 DBG 샘플 참고")

df_today = pd.DataFrame(items).sort_values("rank").reset_index(drop=True)

# 날짜·시간(KST)
kst = pytz.timezone("Asia/Seoul")
df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

print(f"총 카드 수   : {len(cards)}")
print(f"LG 모니터 수 : {len(df_today)}")

# ────────────── 3) Google Sheets 연결 ──────────────
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.environ["SHEET_ID"]

sa_json = base64.b64decode(os.environ["GCP_SA_BASE64"]).decode("utf-8")
creds   = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
gc      = gspread.authorize(creds)
sh      = gc.open_by_key(SHEET_ID)

def ws(name, rows=1000, cols=20):
    try:
        return sh.worksheet(name)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(name, rows, cols)

ws_hist  = ws("History")
ws_today = ws("Today")

# ────────────── 4) 변동률(Δ) 계산 ──────────────
try:
    prev = pd.DataFrame(ws_hist.get_all_records()).dropna()
except gspread.exceptions.APIError:
    prev = pd.DataFrame()

if not prev.empty:
    latest = (prev.sort_values("date")
                  .groupby("asin", as_index=False)
                  .last()[["asin", "rank"]]
                  .rename(columns={"rank": "rank_prev"}))
    df_today = df_today.merge(latest, on="asin", how="left")
    df_today["delta"] = df_today["rank_prev"] - df_today["rank"]
else:
    df_today["delta"] = None

# ────────────── 5) 시트 업데이트 ──────────────
ws_hist.append_rows(df_today.values.tolist(), value_input_option="RAW")
ws_today.clear()
ws_today.update(
    [df_today.columns.values.tolist()] + df_today.values.tolist(),
    value_input_option="RAW"
)

print("✓ Google Sheet 업데이트 완료")
