# crawl.py (rank·price 변동 시각화: '-', '△', '▽')
"""
Amazon.de ▸ Computer Accessories & Monitors ▸ Monitors 베스트셀러 1~100위
→ LG 모니터만 수집, 가격·순위·변동률 계산 후 Google Sheet(History, Today)에 기록
   * 변동 표시 규칙
     - 변동 0 → '-'
     - rank/price 상승(양수) → "△값" (빨간 텍스트: 시트 조건부 서식 권장)
     - 하락(음수)          → "▽값" (파랑 텍스트)
"""

import os, re, json, base64, datetime, requests, pandas as pd, gspread, pytz
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# ───────── 1) Amazon 요청 ─────────
URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
}
COOKIES = {"lc-main": "de_DE", "i18n-prefs": "EUR"}
html = requests.get(URL, headers=HEADERS, cookies=COOKIES, timeout=30).text
if "Enter the characters you see below" in html:
    raise RuntimeError("Amazon CAPTCHA! 잠시 후 재시도하세요.")

soup = BeautifulSoup(html, "lxml")
cards = soup.select("div.zg-grid-general-faceout") or soup.select("div.p13n-sc-uncoverable-faceout")

# ───────── 2) 파싱 Helper ─────────

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
    p = card.select_one('span.p13n-sc-price')
    if p and p.get_text(strip=True):
        return p.get_text(strip=True)
    whole = card.select_one('span.a-price-whole')
    frac  = card.select_one('span.a-price-fraction')
    if whole:
        txt = whole.get_text(strip=True).replace('.', '').replace(',', '.')
        if frac:
            txt += frac.get_text(strip=True)
        return txt
    return ""

def money_to_float(txt):
    digits = re.sub(r"[^0-9,\.]", "", txt).replace('.', '').replace(',', '.')
    try:
        return float(digits)
    except ValueError:
        return None

items = []
for rank, card in enumerate(cards, 1):
    a = card.select_one("a.a-link-normal[href*='/dp/']")
    if not a:
        continue
    title = pick_title(card) or a.get_text(" ", strip=True)
    link  = "https://www.amazon.de" + a["href"].split("?", 1)[0]
    m     = re.search(r"/dp/([A-Z0-9]{10})", link)
    asin  = m.group(1) if m else None
    if re.search(r"\bLG\b", title, re.I):
        price_val = money_to_float(pick_price(card))
        items.append({"asin": asin, "title": title, "rank": rank, "price": price_val, "url": link})

if not items:
    raise RuntimeError("LG 모니터가 목록에 없습니다!")

df_today = pd.DataFrame(items).sort_values("rank").reset_index(drop=True)

# Timestamp (KST)
kst = pytz.timezone("Asia/Seoul")
df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

# ───────── 3) Sheets 연결 ─────────
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.environ["SHEET_ID"]
sa_json  = base64.b64decode(os.environ["GCP_SA_BASE64"]).decode("utf-8")
creds    = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
sh       = gspread.authorize(creds).open_by_key(SHEET_ID)

ws_hist  = sh.worksheet("History") if "History" in [w.title for w in sh.worksheets()] else sh.add_worksheet("History", 2000, 20)
ws_today = sh.worksheet("Today")   if "Today"   in [w.title for w in sh.worksheets()] else sh.add_worksheet("Today", 50, 20)

# ───────── 4) Δ 계산 & 포맷 ─────────
try:
    prev = pd.DataFrame(ws_hist.get_all_records()).dropna()
except:
    prev = pd.DataFrame()

def fmt_delta(val, is_price=False):
    if pd.isna(val):
        return "-"
    if val == 0:
        return "-"
    arrow = "△" if val > 0 else "▽"
    if is_price:
        return f"{arrow}{abs(val):.2f}"
    return f"{arrow}{abs(int(val))}"

if not prev.empty and set(["asin", "rank", "price", "date"]).issubset(prev.columns):
    latest = (prev.sort_values("date")
              .groupby("asin", as_index=False)
              .last()[["asin", "rank", "price"]]
              .rename(columns={"rank": "rank_prev", "price": "price_prev"}))
    df_today = df_today.merge(latest, on="asin", how="left")
    df_today["rank_delta_num"]  = df_today["rank_prev"]  - df_today["rank"]
    df_today["price_delta_num"] = df_today["price"] - df_today["price_prev"]
else:
    df_today["rank_delta_num"]  = None
    df_today["price_delta_num"] = None

# 포맷 컬럼 생성
df_today["rank_delta"]  = df_today["rank_delta_num"].apply(fmt_delta)
df_today["price_delta"] = df_today["price_delta_num"].apply(lambda x: fmt_delta(x, True))

# 표시용 컬럼 순서
cols = ["asin", "title", "rank", "price", "url", "date", "rank_delta", "price_delta"]
df_today = df_today[cols]

# ───────── 5) 시트 업데이트 ─────────
# History 헤더 없으면 생성
if not ws_hist.get_all_values():
    ws_hist.append_row(cols, value_input_option="RAW")
ws_hist.append_rows(df_today.values.tolist(), value_input_option="RAW")

ws_today.clear()
ws_today.update([cols] + df_today.values.tolist(), value_input_option="RAW")

print("✓ Google Sheet 업데이트 완료")
