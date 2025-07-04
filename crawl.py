# crawl.py — Amazon.de Monitors Bestseller Scraper (stop after 100)
"""
• 각 페이지(pg=1,2,3 …)에서
  ① 기본 HTML
  ② 스크롤 ajax 20개 (?pg=N&ajax=1)
  → 한 페이지 최대 50개 확보
• 누적 카드가 100개(1~100위) 되면 즉시 종료
• 모든 카드 로그 + LG 모니터만 Google Sheet 기록
"""

import os, re, json, base64, datetime, requests, pandas as pd, gspread, pytz, time
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

BASE = "https://www.amazon.de/gp/bestsellers/computers/429868031/"  # 반드시 슬래시 포함
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
}
COOKIES = {"lc-main": "de_DE", "i18n-prefs": "EUR"}

def money(txt: str):
    cleaned = re.sub(r"[^0-9,\.]", "", txt).replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None

def sel_cards(html: str):
    soup = BeautifulSoup(html, "lxml")
    return (
        soup.select("div.zg-grid-general-faceout") or
        soup.select("div.p13n-sc-uncoverable-faceout") or
        soup.select("div.a-section.a-spacing-none.p13n-asin") or
        soup.select("ol#zg-ordered-list > li")
    )

print("[INFO] Start scraping — stop after 100 cards")
all_cards = []
page = 1
while len(all_cards) < 100:
    url_main = BASE if page == 1 else f"{BASE}?pg={page}"
    url_ajax = f"{BASE}?pg={page}&ajax=1"

    html_main = requests.get(url_main, headers=HEADERS, cookies=COOKIES, timeout=30).text
    html_ajax = requests.get(url_ajax, headers=HEADERS, cookies=COOKIES, timeout=30).text

    cards = sel_cards(html_main) + sel_cards(html_ajax)
    if not cards:
        break  # 빈 페이지 → 종료

    print(f"[PAGE {page}] cards={len(cards):>2} | cumulative={len(all_cards)+len(cards)}")
    for i, c in enumerate(cards[:5], 1):
        snippet = c.get_text(" ", strip=True)[:80]
        price_t = c.select_one("span.a-offscreen")
        price_s = price_t.get_text(strip=True) if price_t else "—"
        print(f"   {i:>2}. {snippet} | {price_s}")

    all_cards.extend([(c, page) for c in cards])
    if len(cards) < 50:
        break  # 마지막 페이지 (예: 30개)
    page += 1

print(f"[INFO] Total containers collected: {len(all_cards)}")

# ------------- Helper functions -------------

def pick_title(card):
    for sel in [
        'span[class*="p13n-sc-css-line-clamp"]', '[title]',
        '.p13n-sc-truncate-desktop-type2', '.zg-text-center-align span.a-size-base']:
        t = card.select_one(sel)
        if t:
            return t.get("title") if sel == '[title]' else t.get_text(strip=True)
    img = card.select_one("img")
    return img.get("alt", "").strip() if img else ""

def pick_price(card):
    p = card.select_one('span.a-offscreen') or card.select_one('span.p13n-sc-price')
    if p:
        return p.get_text(strip=True)
    whole = card.select_one('span.a-price-whole'); frac = card.select_one('span.a-price-fraction')
    if whole:
        txt = whole.get_text(strip=True).replace('.', '').replace(',', '.')
        if frac:
            txt += frac.get_text(strip=True)
        return txt
    return ""

# ------------- Build item list -------------
items = []
for idx, (card, pg) in enumerate(all_cards, 1):
    badge = card.select_one('.zg-badge-text')
    rank_pg = int(badge.get_text(strip=True).lstrip('#')) if badge else ((idx - 1) % 50 + 1)
    abs_rank = (pg - 1) * 50 + rank_pg

    link_tag = card.select_one("a.a-link-normal[href*='/dp/']")
    if not link_tag:
        continue
    title = pick_title(card) or link_tag.get_text(" ", strip=True)
    link  = "https://www.amazon.de" + link_tag['href'].split('?', 1)[0]
    asin_m = re.search(r"/dp/([A-Z0-9]{10})", link)

    items.append({
        'asin': asin_m.group(1) if asin_m else None,
        'title': title,
        'rank': abs_rank,
        'price': money(pick_price(card)),
        'url': link
    })

print(f"[INFO] Items scraped: {len(items)}")

# ------------- Filter LG -------------
lg_df = pd.DataFrame([it for it in items if 'LG' in it['title'].upper()]).sort_values('rank')
if lg_df.empty:
    raise RuntimeError("LG 모니터를 찾을 수 없습니다.")

kst = pytz.timezone('Asia/Seoul')
lg_df['date'] = datetime.datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')

print("\n[LG ITEMS]")
for r in lg_df.itertuples():
    print(f" #{r.rank:>3} | {r.title[:60]} | {r.price}")

# ------------- Google Sheets upload -------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(json.loads(base64.b64decode(os.environ['GCP_SA_BASE64']).decode()), scopes=SCOPES)
sh    = gspread.authorize(creds).open_by_key(os.environ['SHEET_ID'])

for name in ('History', 'Today'):
    if name not in [w.title for w in sh.worksheets()]:
        sh.add_worksheet(name, 2000, 20)
ws_hist, ws_today = sh.worksheet('History'), sh.worksheet('Today')

cols = ['asin', 'title', 'rank', 'price', 'url', 'date']
lg_df = lg_df[cols].fillna("")

if not ws_hist.get_all_values():
    ws_hist.append_row(cols, value_input_option='RAW')
ws_hist.append_rows(lg_df.values.tolist(), value_input_option='RAW')
ws_today.clear()
ws_today.update([cols] + lg_df.values.tolist(), value_input_option='RAW')

print("\n✓ Done — LG monitors scraped:", len(lg_df))
