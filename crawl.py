# crawl.py (dynamic pagination + sheet retry)
"""
Amazon.de ▸ Monitors 베스트셀러 1~100위+ (페이지 수 변동 대응)
- 모든 페이지(pg=1,2,3 …) 자동 탐색 (컨테이너 없을 때까지)
- LG 모니터 필터, 가격·순위·변동(△▽–) 계산
- Google Sheet 업데이트: History 누적 / Today 최신, 3회 재시도(500 오류 대비)
"""

import os, re, json, base64, datetime, time, requests, pandas as pd, gspread, pytz
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"  # pg=N
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
}
COOKIES = {"lc-main": "de_DE", "i18n-prefs": "EUR"}

# ───── 1) 페이지 크롤링 (무한 루프, 빈 페이지 만나면 종료) ─────
all_cards = []
for pg in range(1, 10):       # Amazon 보통 1~2 페이지, 안전하게 9까지 탐색
    url = BASE_URL if pg == 1 else f"{BASE_URL}?pg={pg}"
    html = requests.get(url, headers=HEADERS, cookies=COOKIES, timeout=30).text
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("div.zg-grid-general-faceout") or soup.select("div.p13n-sc-uncoverable-faceout")
    if not cards:
        break  # 더 이상 페이지 없음
    all_cards.extend([(c, pg) for c in cards])
    if len(cards) < 50:
        break  # 마지막 페이지(보통 30개) 발견
print(f"[INFO] total containers: {len(all_cards)} on {pg} page(s)")

# ───── 2) Helper 함수 ─────

def pick_title(card):
    for sel in [
        'span[class*="p13n-sc-css-line-clamp"]', '[title]',
        '.p13n-sc-truncate-desktop-type2', '.zg-text-center-align span.a-size-base']:
        t = card.select_one(sel)
        if t:
            return (t.get("title") if sel == '[title]' else t.get_text(strip=True)) or ""
    img = card.select_one("img")
    return img.get("alt", "").strip() if img else ""

def pick_price(card):
    for sel in ['span.a-offscreen', 'span.p13n-sc-price']:
        p = card.select_one(sel)
        if p and p.get_text(strip=True):
            return p.get_text(strip=True)
    whole = card.select_one('span.a-price-whole'); frac = card.select_one('span.a-price-fraction')
    if whole:
        txt = whole.get_text(strip=True).replace('.', '').replace(',', '.')
        if frac: txt += frac.get_text(strip=True)
        return txt
    return ""

def money_to_float(txt):
    clean = re.sub(r"[^0-9,\.]", "", txt).replace('.', '').replace(',', '.')
    try:
        return float(clean)
    except ValueError:
        return None

# ───── 3) 카드 → 리스트 ─────
items = []
for idx, (card, pg) in enumerate(all_cards, start=1):
    rank_text = card.select_one('.zg-badge-text')
    rank_on_pg = int(rank_text.get_text(strip=True).lstrip('#')) if rank_text else ((idx - 1) % 50 + 1)
    abs_rank = (pg - 1) * 50 + rank_on_pg

    a = card.select_one("a.a-link-normal[href*='/dp/']")
    if not a:
        continue
    title = pick_title(card) or a.get_text(" ", strip=True)
    if 'LG' not in title.upper():
        continue
    link = "https://www.amazon.de" + a['href'].split('?', 1)[0]
    asin_m = re.search(r"/dp/([A-Z0-9]{10})", link)
    asin = asin_m.group(1) if asin_m else None
    price_val = money_to_float(pick_price(card))
    items.append({"asin": asin, "title": title, "rank": abs_rank, "price": price_val, "url": link})

if not items:
    raise RuntimeError("LG 모니터를 찾을 수 없습니다.")

items.sort(key=lambda x: x['rank'])
df_today = pd.DataFrame(items)

kst = pytz.timezone('Asia/Seoul'); df_today['date'] = datetime.datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')

# ───── 4) Sheets 연결 ─────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
sa_json = json.loads(base64.b64decode(os.environ['GCP_SA_BASE64']).decode())
creds = Credentials.from_service_account_info(sa_json, scopes=SCOPES)
sh = gspread.authorize(creds).open_by_key(os.environ['SHEET_ID'])
ws_hist = sh.worksheet('History') if 'History' in [w.title for w in sh.worksheets()] else sh.add_worksheet('History', 2000, 20)
ws_today = sh.worksheet('Today')  if 'Today'  in [w.title for w in sh.worksheets()] else sh.add_worksheet('Today',  100, 20)

# ───── 5) 변동 계산 ─────
try:
    prev = pd.DataFrame(ws_hist.get_all_records())
except: prev = pd.DataFrame()
if not prev.empty and {'asin','rank','price','date'}.issubset(prev.columns):
    latest = (prev.sort_values('date').groupby('asin').last().reset_index()[['asin','rank','price']]
              .rename(columns={'rank':'rank_prev','price':'price_prev'}))
    df_today = df_today.merge(latest, on='asin', how='left')
    df_today['rank_delta_num'] = df_today['rank_prev'] - df_today['rank']
    df_today['price_delta_num'] = df_today['price'] - df_today['price_prev']
else:
    df_today['rank_delta_num'] = None; df_today['price_delta_num'] = None

fmt = lambda v,p=False: '-' if (pd.isna(v) or v==0) else ('△' if v>0 else '▽') + (f"{abs(v):.2f}" if p else str(abs(int(v))))
df_today['rank_delta']  = df_today['rank_delta_num'].apply(fmt)
df_today['price_delta'] = df_today['price_delta_num'].apply(lambda x: fmt(x, True))

cols = ['asin','title','rank','price','url','date','rank_delta','price_delta']
df_today = df_today[cols].fillna("")

# ───── 6) 시트 업데이트 (재시도) ─────
for attempt in range(3):
    try:
        if not ws_hist.get_all_values():
            ws_hist.append_row(cols, value_input_option='RAW')
        ws_hist.append_rows(df_today.values.tolist(), value_input_option='RAW')
        ws_today.clear(); ws_today.update([cols] + df_today.values.tolist(), value_input_option='RAW')
        break
    except gspread.exceptions.APIError as e:
        if attempt == 2: raise
        print('[WARN] Sheets API error, retrying...', e)
        time.sleep(2)

print('✓ 완료, LG 모니터', len(df_today))
