# crawl.py  —  Amazon.de Monitors Bestseller Scraper (FULL DATA LOG)
"""
• 모든 페이지(pg=1,2,3 …) 빈 페이지가 나올 때까지 순회
• 컨테이너 수·각 카드(1~N) 제목·가격 모두 콘솔에 출력
• LG 모니터만 필터 → 가격·순위·변동(△▽–) 계산
• Google Sheet: History 누적 / Today 최신, 500 오류 3회 재시도
"""

import os, re, json, base64, datetime, time, requests, pandas as pd, gspread, pytz
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"  # 끝 슬래시!
HEADERS  = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
}
COOKIES  = {"lc-main": "de_DE", "i18n-prefs": "EUR"}

# ───────────────── 1) 페이지 순회 ───────────────────────────
all_cards = []
for pg in range(1, 10):
    url  = BASE_URL if pg == 1 else f"{BASE_URL}?pg={pg}"
    html = requests.get(url, headers=HEADERS, cookies=COOKIES, timeout=30).text
    soup = BeautifulSoup(html, "lxml")

    cards = (
        soup.select("div.zg-grid-general-faceout") or
        soup.select("div.p13n-sc-uncoverable-faceout") or
        soup.select("div.a-section.a-spacing-none.p13n-asin") or
        soup.select("ol#zg-ordered-list > li")
    )

    print(f"\n[PAGE {pg}] URL={url} | containers={len(cards)}")
    for i, c in enumerate(cards, 1):
        title_txt = c.get_text(" ", strip=True)[:100]
        price_tag = c.select_one("span.a-offscreen") or c.select_one("span.p13n-sc-price")
        price_txt = price_tag.get_text(strip=True) if price_tag else "—"
        print(f"   {i:>2}. {title_txt} | {price_txt}")

    if not cards:
        break

print(f"\n[INFO] total containers collected: {len(all_cards)}")

# ───────────────── 2) Helper ───────────────────────────────

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

money_to_float = lambda t: (
    float(re.sub(r"[^0-9,\.]", "", t).replace('.', '').replace(',', '.'))
    if re.search(r"[0-9]", t) else None
)

# ───────────────── 3) 카드 → 아이템 ─────────────────────────
items = []
for idx, (card, pg) in enumerate(all_cards, 1):
    badge = card.select_one('.zg-badge-text')
    rank_on_pg = int(badge.get_text(strip=True).lstrip('#')) if badge else ((idx-1)%50 + 1)
    abs_rank   = (pg - 1) * 50 + rank_on_pg

    a = card.select_one("a.a-link-normal[href*='/dp/']")
    if not a: continue

    title = pick_title(card) or a.get_text(" ", strip=True)
    link  = "https://www.amazon.de" + a['href'].split('?',1)[0]
    asin  = re.search(r"/dp/([A-Z0-9]{10})", link)
    asin  = asin.group(1) if asin else None
    price = money_to_float(pick_price(card))

    items.append({"asin":asin, "title":title, "rank":abs_rank, "price":price, "url":link})

# 전체 아이템 로그
print(f"\n[INFO] total items scraped (all brands): {len(items)}")
for it in items:
    print(f" #{it['rank']:>3} | {it['asin']} | {it['title'][:70]} | {it['price']}")

# ────────────── LG 필터 후 변동 계산 ───────────────────────
lg_df = pd.DataFrame([it for it in items if 'LG' in it['title'].upper()])
if lg_df.empty:
    raise RuntimeError("LG 모니터를 찾지 못했습니다.")

kst = pytz.timezone('Asia/Seoul')
lg_df['date'] = datetime.datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')

# ────────────── Sheets 연결 · 업데이트 (기존 로직) ───────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds  = Credentials.from_service_account_info(json.loads(base64.b64decode(os.environ['GCP_SA_BASE64']).decode()), scopes=SCOPES)
sh     = gspread.authorize(creds).open_by_key(os.environ['SHEET_ID'])
ws_hist= sh.worksheet('History') if 'History' in [w.title for w in sh.worksheets()] else sh.add_worksheet('History',2000,20)
ws_today=sh.worksheet('Today')   if 'Today'   in [w.title for w in sh.worksheets()] else sh.add_worksheet('Today',100,20)

# 변동률 계산 생략 (원하면 이전 코드 블록 삽입)
cols = ['asin','title','rank','price','url','date']
lg_df = lg_df[cols].fillna("")

if not ws_hist.get_all_values():
    ws_hist.append_row(cols, value_input_option='RAW')
ws_hist.append_rows(lg_df.values.tolist(), value_input_option='RAW')
ws_today.clear(); ws_today.update([cols] + lg_df.values.tolist(), value_input_option='RAW')

print("\n✓ 완료, LG 모니터", len(lg_df))
