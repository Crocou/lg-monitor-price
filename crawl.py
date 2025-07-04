# crawl.py (price + price_delta version)

"""
Amazon.de ▸ Computer Accessories & Monitors ▸ Monitors 베스트셀러 1~100위
→ 'LG' 모니터만 수집, (rank, Δ) + 가격, 가격 변동률까지 Google Sheet(History·Today)에 기록
"""

import os, re, json, base64, datetime, requests, pandas as pd, gspread, pytz
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# ───────────────────────────────
# 1) Amazon 페이지 요청
# ───────────────────────────────
URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
}
COOKIES = {"lc-main": "de_DE", "i18n-prefs": "EUR"}  # 독일 · € 고정

html = requests.get(URL, headers=HEADERS, cookies=COOKIES, timeout=30).text
if "Enter the characters you see below" in html:
    raise RuntimeError("Amazon CAPTCHA! 잠시 후 재시도하세요.")

soup  = BeautifulSoup(html, "lxml")
cards = soup.select("div.zg-grid-general-faceout") or soup.select("div.p13n-sc-uncoverable-faceout")

# ───────────────────────────────
# 2) 카드 파싱 Helper
# ───────────────────────────────

def pick_title(card):
    for sel in [
        'span[class*="p13n-sc-css-line-clamp"]',
        '[title]',
        '.p13n-sc-truncate-desktop-type2',
        '.zg-text-center-align span.a-size-base',
    ]:
        t = card.select_one(sel)
        if t:
            txt = t.get("title", "") if sel == '[title]' else t.get_text(strip=True)
            if txt:
                return txt
    img = card.select_one("img")
    return img.get("alt", "").strip() if img else ""

def pick_price(card):
    # 1) 베스트셀러 리스트 기본 가격 클래스
    p = card.select_one('span.p13n-sc-price')
    if p and p.get_text(strip=True):
        return p.get_text(strip=True)
    # 2) A-Price 구성 (whole + fraction)
    whole = card.select_one('span.a-price-whole')
    frac  = card.select_one('span.a-price-fraction')
    if whole:
        price_txt = whole.get_text(strip=True).replace('.', '').replace(',', '.')
        if frac:
            price_txt += frac.get_text(strip=True)
        return price_txt
    return ""

def money_to_float(price_str: str):
    # "€ 199,99" → 199.99
    digits = re.sub(r"[^0-9,\.]", "", price_str)
    digits = digits.replace('.', '').replace(',', '.')
    try:
        return float(digits)
    except ValueError:
        return None

items = []
for rank, card in enumerate(cards, start=1):
    a = card.select_one("a.a-link-normal[href*='/dp/']")
    if not a:
        continue
    title = pick_title(card) or a.get_text(" ", strip=True)
    link  = "https://www.amazon.de" + a["href"].split("?", 1)[0]
    asin_match = re.search(r"/dp/([A-Z0-9]{10})", link)
    asin = asin_match.group(1) if asin_match else None

    if re.search(r"\bLG\b", title, re.I):
        price_text = pick_price(card)
        price_val  = money_to_float(price_text)
        items.append({
            "asin": asin,
            "title": title,
            "rank": rank,
            "price": price_val,
            "url": link
        })

# 디버그 출력 3개
for i, row in enumerate(items[:3], 1):
    print(f"[DBG] #{i}: {row['title'][:60]} | rank {row['rank']} | €{row['price']}")

# 오류 핸들
if not items:
    raise RuntimeError("LG 모니터가 목록에 없습니다!")

# DataFrame + timestamp
kst = pytz.timezone("Asia/Seoul")
df_today = pd.DataFrame(items)
df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

df_today.sort_values("rank", inplace=True)

print(f"총 LG 모니터 수 : {len(df_today)}")

# ───────────────────────────────
# 3) Google Sheets 연결
# ───────────────────────────────
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.environ["SHEET_ID"]
sa_json  = base64.b64decode(os.environ["GCP_SA_BASE64"]).decode("utf-8")
creds    = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
sh       = gspread.authorize(creds).open_by_key(SHEET_ID)


def ws(name):
    try:
        return sh.worksheet(name)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(name, rows=2000, cols=20)

ws_hist  = ws("History")
ws_today = ws("Today")

# ───────────────────────────────
# 4) 변동률 계산 (rank & price)
# ───────────────────────────────
try:
    prev = pd.DataFrame(ws_hist.get_all_records()).dropna()
except:
    prev = pd.DataFrame()

if not prev.empty and set(["asin", "rank", "price", "date"]).issubset(prev.columns):
    latest_prev = (
        prev.sort_values("date")
            .groupby("asin", as_index=False)
            .last()[["asin", "rank", "price"]]
            .rename(columns={"rank": "rank_prev", "price": "price_prev"})
    )
    df_today = df_today.merge(latest_prev, on="asin", how="left")
    df_today["rank_delta"]  = df_today["rank_prev"]  - df_today["rank"]
    df_today["price_delta"] = df_today["price"] - df_today["price_prev"]
else:
    df_today["rank_delta"]  = None
    df_today["price_delta"] = None

# ───────────────────────────────
# 5) 시트 업데이트
# ───────────────────────────────
# (1) History 헤더 없으면 생성
if not ws_hist.get_all_values():
    ws_hist.append_row(df_today.columns.tolist(), value_input_option="RAW")

ws_hist.append_rows(df_today.values.tolist(), value_input_option="RAW")

# (2) Today 탭 교체
ws_today.clear()
ws_today.update([df_today.columns.tolist()] + df_today.values.tolist(),
                value_input_option="RAW")

print("✓ Google Sheet 업데이트 완료")
