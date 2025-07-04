# crawl.py — Amazon.de Monitors Bestseller Scraper (stop after 100)
"""
• 각 페이지(pg=1,2,3 …)에서
  ① 기본 HTML ② 스크롤 ajax 20개 (isBelowTheFold=1&ajax=1)
  → 최대 50개 확보
• **총 100개(1~100위) 확보하면 즉시 종료**
• 모든 카드 정보 로그 + LG 모니터 Sheet 기록
"""

import os, re, json, base64, datetime, requests, pandas as pd, gspread, pytz, time
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

BASE = "https://www.amazon.de/gp/bestsellers/computers/429868031/"  # keep trailing /
HEADERS = {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36","Accept-Language":"de-DE,de;q=0.9,en;q=0.7",}
COOKIES = {"lc-main":"de_DE","i18n-prefs":"EUR"}

# helper to parse containers
money = lambda s: float(re.sub(r"[^0-9,\.]","",s).replace('.','').replace(',','.')) if re.search(r"[0-9]",s) else None

def sel_cards(html:str):
    soup = BeautifulSoup(html,"lxml")
    return (soup.select("div.zg-grid-general-faceout") or
            soup.select("div.p13n-sc-uncoverable-faceout") or
            soup.select("div.a-section.a-spacing-none.p13n-asin") or
            soup.select("ol#zg-ordered-list > li"))

print("[INFO] Start scraping (stop after 100 cards)…")
all_cards=[]
pg=1
while len(all_cards)<100:
    url_main = BASE if pg==1 else f"{BASE}?pg={pg}"
    url_ajax = f"{url_main}&isBelowTheFold=1&ajax=1"

    cards = sel_cards(requests.get(url_main,headers=HEADERS,cookies=COOKIES,timeout=30).text)
    cards += sel_cards(requests.get(url_ajax,headers=HEADERS,cookies=COOKIES,timeout=30).text)

    if not cards:
        break
    print(f"[PAGE {pg}] cards={len(cards)} (cumulative {len(all_cards)+len(cards)})")
    all_cards.extend([(c,pg) for c in cards])
    if len(cards)<50:
        break     # last page (30 etc.)
    pg+=1

print(f"[INFO] total collected: {len(all_cards)}")

# extract items

def pick_title(card):
    for sel in ['span[class*="p13n-sc-css-line-clamp"]','[title]','.p13n-sc-truncate-desktop-type2','.zg-text-center-align span.a-size-base']:
        t=card.select_one(sel)
        if t: return t.get('title') if sel=='[title]' else t.get_text(strip=True)
    img=card.select_one('img'); return img.get('alt','').strip() if img else ''

def pick_price(card):
    p=card.select_one('span.a-offscreen') or card.select_one('span.p13n-sc-price')
    if p: return p.get_text(strip=True)
    whole=card.select_one('span.a-price-whole'); frac=card.select_one('span.a-price-fraction')
    if whole:
        txt=whole.get_text(strip=True).replace('.','').replace(',','.')
        if frac: txt+=frac.get_text(strip=True)
        return txt
    return ''

items=[]
for idx,(card,pg_idx) in enumerate(all_cards,1):
    badge=card.select_one('.zg-badge-text'); rank_pg=int(badge.get_text(strip=True).lstrip('#')) if badge else ((idx-1)%50+1)
    abs_rank=(pg_idx-1)*50+rank_pg
    a=card.select_one("a.a-link-normal[href*='/dp/']");
    if not a: continue
    title=pick_title(card) or a.get_text(" ",strip=True)
    url="https://www.amazon.de"+a['href'].split('?',1)[0]
    asin=re.search(r"/dp/([A-Z0-9]{10})",url)
    items.append({"asin":asin.group(1) if asin else None,"title":title,"rank":abs_rank,"price":money(pick_price(card)),"url":url})

print(f"[INFO] items scraped: {len(items)}")

lg_df=pd.DataFrame([it for it in items if 'LG' in it['title'].upper()]).sort_values('rank')
if lg_df.empty:
    raise RuntimeError('LG 모니터 없음')

kst=pytz.timezone('Asia/Seoul'); lg_df['date']=datetime.datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')

# Sheets upload (no delta for brevity)
SCOPES=["https://www.googleapis.com/auth/spreadsheets"]
creds=Credentials.from_service_account_info(json.loads(base64.b64decode(os.environ['GCP_SA_BASE64']).decode()),scopes=SCOPES)
sh=gspread.authorize(creds).open_by_key(os.environ['SHEET_ID'])
for n in ('History','Today'):
    if n not in [w.title for w in sh.worksheets()]: sh.add_worksheet(n,2000,20)
ws_hist,ws_today=sh.worksheet('History'),sh.worksheet('Today')
cols=['asin','title','rank','price','url','date']
if not ws_hist.get_all_values(): ws_hist.append_row(cols,value_input_option='RAW')
ws_hist.append_rows(lg_df[cols].values.tolist(),value_input_option='RAW')
ws_today.clear(); ws_today.update([cols]+lg_df[cols].values.tolist(),value_input_option='RAW')

print("✓ Done. LG monitors:",len(lg_df))
