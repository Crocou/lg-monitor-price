# crawl.py  —  Amazon.de Monitors Bestseller Scraper (FULL DATA LOG + ajax scroll)
"""
• 각 페이지(pg=1,2,3 …) 에 대해
  ① 일반 HTML 요청
  ② 추가 20개를 가져오는 ajax 요청 (?pg=N&ajax=1)
  → 두 응답을 합쳐 최대 50개 확보
• 컨테이너·카드 전체 로그 출력
• LG 모니터만 필터 → 가격·순위·변동(△▽–) 계산
• Google Sheet: History 누적 / Today 최신
"""

import os, re, json, base64, datetime, time, requests, pandas as pd, gspread, pytz
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"  # 끝 슬래시!
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
}
COOKIES = {"lc-main": "de_DE", "i18n-prefs": "EUR"}

# ───────── 1) 페이지 + ajax 스크롤 요청 ─────────
all_cards = []
for pg in range(1, 10):
    urls = [BASE_URL if pg == 1 else f"{BASE_URL}?pg={pg}",
            (BASE_URL if pg == 1 else f"{BASE_URL}?pg={pg}") + "&ajax=1"]
    merged_cards = []
    for u in urls:
        html = requests.get(u, headers=HEADERS, cookies=COOKIES, timeout=30).text
        soup = BeautifulSoup(html, "lxml")
        cards = (soup.select("div.zg-grid-general-faceout") or
                 soup.select("div.p13n-sc-uncoverable-faceout") or
                 soup.select("div.a-section.a-spacing-none.p13n-asin") or
                 soup.select("ol#zg-ordered-list > li"))
        merged_cards += cards
    if not merged_cards:
        break
    print(f"\n[PAGE {pg}] containers={len(merged_cards)} (main+ajax)")
    for i,c in enumerate(merged_cards[:5],1):
        t=c.get_text(" ",strip=True)[:80]
        p=c.select_one("span.a-offscreen")
        price=p.get_text(strip=True) if p else "—"
        print(f"   {i:>2}. {t} | {price}")
    all_cards.extend([(c, pg) for c in merged_cards])
    if len(merged_cards) < 50:
        break

print(f"\n[INFO] total containers: {len(all_cards)}")

# ───────── 2) Helper 함수 ─────────

def pick_title(card):
    for sel in ['span[class*="p13n-sc-css-line-clamp"]','[title]','.p13n-sc-truncate-desktop-type2','.zg-text-center-align span.a-size-base']:
        t=card.select_one(sel)
        if t:
            return t.get('title') if sel=='[title]' else t.get_text(strip=True)
    img=card.select_one('img'); return img.get('alt','').strip() if img else ""

def pick_price(card):
    p=card.select_one('span.a-offscreen') or card.select_one('span.p13n-sc-price')
    if p: return p.get_text(strip=True)
    whole=card.select_one('span.a-price-whole'); frac=card.select_one('span.a-price-fraction')
    if whole:
        txt=whole.get_text(strip=True).replace('.','').replace(',','.')
        if frac: txt+=frac.get_text(strip=True)
        return txt
    return ""

money=lambda s: float(re.sub(r"[^0-9,\.]","",s).replace('.','').replace(',','.')) if re.search(r"[0-9]",s) else None

# ───────── 3) 카드→아이템 & 순위 ─────────
items=[]
for idx,(card,pg) in enumerate(all_cards,1):
    badge=card.select_one('.zg-badge-text'); rank_pg=int(badge.get_text(strip=True).lstrip('#')) if badge else ((idx-1)%50+1)
    abs_rank=(pg-1)*50+rank_pg
    a=card.select_one("a.a-link-normal[href*='/dp/']");
    if not a: continue
    title=pick_title(card) or a.get_text(" ",strip=True)
    url="https://www.amazon.de"+a['href'].split('?',1)[0]
    asin_m=re.search(r"/dp/([A-Z0-9]{10})",url); asin=asin_m.group(1) if asin_m else None
    price_v=money(pick_price(card))
    items.append({"asin":asin,"title":title,"rank":abs_rank,"price":price_v,"url":url})

print(f"\n[INFO] items scraped (all brands): {len(items)}")

# ───────── 4) LG 필터 & DF ─────────
lg=[it for it in items if 'LG' in it['title'].upper()];
if not lg: raise RuntimeError('LG 모니터 없음')
lg_df=pd.DataFrame(lg).sort_values('rank')
lg_df['date']=datetime.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S')

print("\n[LG ITEMS]")
for row in lg_df.itertuples():
    print(f" #{row.rank:>3} | {row.asin} | {row.title[:60]} | {row.price}")

# ───────── 5) Sheets 업로드 (헤더·재시도) ─────────
SCOPES=["https://www.googleapis.com/auth/spreadsheets"]
creds=Credentials.from_service_account_info(json.loads(base64.b64decode(os.environ['GCP_SA_BASE64']).decode()),scopes=SCOPES)
sh=gspread.authorize(creds).open_by_key(os.environ['SHEET_ID'])
ws_hist=sh.worksheet('History') if 'History' in [w.title for w in sh.worksheets()] else sh.add_worksheet('History',2000,20)
ws_today=sh.worksheet('Today')  if 'Today'  in [w.title for w in sh.worksheets()] else sh.add_worksheet('Today',100,20)
cols=['asin','title','rank','price','url','date']
lg_df=lg_df[cols].fillna("")
if not ws_hist.get_all_values(): ws_hist.append_row(cols,value_input_option='RAW')
ws_hist.append_rows(lg_df.values.tolist(),value_input_option='RAW')
ws_today.clear(); ws_today.update([cols]+lg_df.values.tolist(),value_input_option='RAW')

print("\n✓ 완료, LG 모니터",len(lg_df))
