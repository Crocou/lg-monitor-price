# crawl.py (robust pagination + accurate rank + price)
"""
Amazon.de 베스트셀러 ▸ Monitors 1~100위 (두 페이지)
- LG 모니터만 수집
- 정확한 절대 순위 (Amazon 각 페이지 1‥50 + (page-1)*50)
- 가격 (`span.a-offscreen` 등 포괄)
- △ ▽ – 변동(순위·가격)
- Google Sheet(History, Today) 기록
"""

import os, re, json, base64, datetime, requests, pandas as pd, gspread, pytz
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031"  # ?pg=1|2
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
}
COOKIES = {"lc-main": "de_DE", "i18n-prefs": "EUR"}

# ─────────────────────────────
# 1. Fetch helper (returns (card_soup, page_rank))
# ─────────────────────────────

def fetch_cards(page: int):
    url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}&ref_=zg_bs_pg_{page}"
    html = requests.get(url, headers=HEADERS, cookies=COOKIES, timeout=30).text
    if "Enter the characters you see below" in html:
        raise RuntimeError("Amazon CAPTCHA!")
    soup = BeautifulSoup(html, "lxml")
    containers = soup.select("div.zg-grid-general-faceout") or soup.select("div.p13n-sc-uncoverable-faceout")
    return containers

all_cards = []
for pg in (1, 2):
    all_cards.extend([(c, pg) for c in fetch_cards(pg)])
print(f"[INFO] total card containers: {len(all_cards)}")

# ─────────────────────────────
# 2. Parsing helpers
# ─────────────────────────────

def pick_title(card):
    selectors = [
        'span[class*="p13n-sc-css-line-clamp"]', '[title]',
        '.p13n-sc-truncate-desktop-type2', '.zg-text-center-align span.a-size-base'
    ]
    for sel in selectors:
        t = card.select_one(sel)
        if t:
            return (t.get("title") if sel == '[title]' else t.get_text(strip=True)) or ""
    img = card.select_one("img")
    return img.get("alt", "").strip() if img else ""

def pick_price(card):
    # Most reliable: span.a-offscreen
    p = card.select_one('span.a-offscreen')
    if p and p.get_text(strip=True):
        return p.get_text(strip=True)
    p = card.select_one('span.p13n-sc-price')
    if p and p.get_text(strip=True):
        return p.get_text(strip=True)
    whole = card.select_one('span.a-price-whole'); frac = card.select_one('span.a-price-fraction')
    if whole:
        txt = whole.get_text(strip=True).replace('.', '').replace(',', '.')
        if frac:
            txt += frac.get_text(strip=True)
        return txt
    return ""

def money_to_float(txt):
    clean = re.sub(r"[^0-9,\.]", "", txt).replace('.', '').replace(',', '.')
    try:
        return float(clean)
    except ValueError:
        return None

# ─────────────────────────────
# 3. Build item list with absolute ranks
# ─────────────────────────────
items = []
for idx, (card, page) in enumerate(all_cards, start=1):
    # rank text on page (1..50)
    rank_tag = card.select_one('.zg-badge-text')
    rank_on_page = int(rank_tag.get_text(strip=True).lstrip('#')) if rank_tag else ((idx - 1) % 50 + 1)
    abs_rank = (page - 1) * 50 + rank_on_page

    a = card.select_one("a.a-link-normal[href*='/dp/']")
    if not a:
        continue
    title = pick_title(card) or a.get_text(" ", strip=True)
    if not re.search(r"\bLG\b", title, re.I):
        continue

    link = "https://www.amazon.de" + a['href'].split('?', 1)[0]
    asin = re.search(r"/dp/([A-Z0-9]{10})", link)
    asin = asin.group(1) if asin else None
    price_val = money_to_float(pick_price(card))

    items.append({
        "asin": asin,
        "title": title,
        "rank": abs_rank,
        "price": price_val,
        "url": link
    })

print(f"[INFO] LG items: {len(items)}")
if not items:
    raise RuntimeError("LG 모니터를 찾지 못했습니다.")

# Sort by absolute rank
items.sort(key=lambda x: x['rank'])
df_today = pd.DataFrame(items)

# Timestamp (KST)
kst = pytz.timezone('Asia/Seoul')
df_today['date'] = datetime.datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')

# ─────────────────────────────
# 4. Google Sheets
# ─────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.environ['SHEET_ID']
sa_json = json.loads(base64.b64decode(os.environ['GCP_SA_BASE64']).decode())
creds = Credentials.from_service_account_info(sa_json, scopes=SCOPES)
sh = gspread.authorize(creds).open_by_key(SHEET_ID)

def ensure_ws(name, rows=2000, cols=20):
    try:
        return sh.worksheet(name)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(name, rows, cols)

ws_hist = ensure_ws('History')
ws_today = ensure_ws('Today', 100, 20)

# ─────────────────────────────
# 5. Δ 계산
# ─────────────────────────────
try:
    prev = pd.DataFrame(ws_hist.get_all_records())
except:
    prev = pd.DataFrame()

if not prev.empty and set(['asin', 'rank', 'price', 'date']).issubset(prev.columns):
    latest = (prev.sort_values('date')
              .groupby('asin', as_index=False)
              .last()[['asin', 'rank', 'price']]
              .rename(columns={'rank': 'rank_prev', 'price': 'price_prev'}))
    df_today = df_today.merge(latest, on='asin', how='left')
    df_today['rank_delta_num'] = df_today['rank_prev'] - df_today['rank']
    df_today['price_delta_num'] = df_today['price'] - df_today['price_prev']
else:
    df_today['rank_delta_num'] = None
    df_today['price_delta_num'] = None

fmt = lambda v, p=False: '-' if (pd.isna(v) or v == 0) else (('△' if v > 0 else '▽') + (f"{abs(v):.2f}" if p else str(abs(int(v)))))

df_today['rank_delta'] = df_today['rank_delta_num'].apply(fmt)
df_today['price_delta'] = df_today['price_delta_num'].apply(lambda x: fmt(x, True))

cols = ['asin', 'title', 'rank', 'price', 'url', 'date', 'rank_delta', 'price_delta']
df_today = df_today[cols]

# Replace NaN → "" for JSON
df_today = df_today.fillna("")

# ─────────────────────────────
# 6. Sheet update
# ─────────────────────────────
if not ws_hist.get_all_values():
    ws_hist.append_row(cols, value_input_option='RAW')
ws_hist.append_rows(df_today.values.tolist(), value_input_option='RAW')
ws_today.clear()
ws_today.update([cols] + df_today.values.tolist(), value_input_option='RAW')

print('✓ 업데이트 완료: LG 모니터', len(df_today))
