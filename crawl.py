# crawl.py (pagination: pg=1,2 -> 100위까지)
"""
Amazon.de 베스트셀러 ▸ Monitors 1~100위 (pg=1, pg=2)
- LG 모니터 필터, 가격·순위·변동(△▽-) 계산, Google Sheet 기록
"""

import os, re, json, base64, datetime, requests, pandas as pd, gspread, pytz
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"  # pg=1|2
HEADERS  = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
}
COOKIES  = {"lc-main": "de_DE", "i18n-prefs": "EUR"}

def fetch_page(page:int):
    url = BASE_URL if page==1 else f"{BASE_URL}?pg={page}"
    html = requests.get(url, headers=HEADERS, cookies=COOKIES, timeout=30).text
    if "Enter the characters you see below" in html:
        raise RuntimeError("Amazon CAPTCHA!")
    soup = BeautifulSoup(html, "lxml")
    return soup.select("div.zg-grid-general-faceout") or soup.select("div.p13n-sc-uncoverable-faceout")

# 1. 모든 카드 모으기 (1~100위)
cards = []
for pg in (1,2):
    cards += fetch_page(pg)
print(f"[INFO] total cards fetched: {len(cards)}")

# 2. Helper

def pick_title(card):
    for sel in [
        'span[class*="p13n-sc-css-line-clamp"]', '[title]', '.p13n-sc-truncate-desktop-type2', '.zg-text-center-align span.a-size-base']:
        t = card.select_one(sel)
        if t:
            txt = t.get("title", "") if sel=='[title]' else t.get_text(strip=True)
            if txt: return txt
    img = card.select_one('img')
    return img.get('alt','').strip() if img else ""

def pick_price(card):
    p=card.select_one('span.p13n-sc-price')
    if p: return p.get_text(strip=True)
    whole=card.select_one('span.a-price-whole'); frac=card.select_one('span.a-price-fraction')
    if whole:
        txt=whole.get_text(strip=True).replace('.','').replace(',','.')
        if frac: txt+=frac.get_text(strip=True)
        return txt
    return ""

def money_to_float(txt):
    val=re.sub(r"[^0-9,\.]","",txt).replace('.','').replace(',','.')
    try: return float(val)
    except: return None

# 3. 카드 → DataFrame
items=[]
for card in cards:
    a=card.select_one("a.a-link-normal[href*='/dp/']");
    if not a: continue
    title=pick_title(card) or a.get_text(" ",strip=True)
    link="https://www.amazon.de"+a['href'].split('?',1)[0]
    m=re.search(r"/dp/([A-Z0-9]{10})",link)
    asin=m.group(1) if m else None
    if re.search(r"\bLG\b",title,re.I):
        price_val=money_to_float(pick_price(card))
        # rank is position in combined list (already ordered per page)
        items.append({"asin":asin,"title":title,"url":link,"price":price_val})

# rank assignment (1..len(cards)) respecting original order
rank_map={id(c):(idx+1) for idx,c in enumerate(cards)}
for it in items:
    # find card again? Instead use first occurrence rank by matching link
    pass
# simpler: regenerate rank by sorting by original order in cards
items_sorted=[]
for idx,c in enumerate(cards):
    link="https://www.amazon.de"+c.select_one("a.a-link-normal[href*='/dp/']")['href'].split('?',1)[0]
    for it in items:
        if it['url']==link:
            it['rank']=idx+1
            items_sorted.append(it)

df_today=pd.DataFrame(items_sorted).sort_values('rank').reset_index(drop=True)

# timestamp
kst=pytz.timezone('Asia/Seoul')
df_today['date']=datetime.datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')

# 4. Sheets
SCOPES=["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID=os.environ['SHEET_ID']
creds=Credentials.from_service_account_info(json.loads(base64.b64decode(os.environ['GCP_SA_BASE64']).decode()),scopes=SCOPES)
sh=gspread.authorize(creds).open_by_key(SHEET_ID)
ws_hist= sh.worksheet('History') if 'History' in [w.title for w in sh.worksheets()] else sh.add_worksheet('History',2000,20)
ws_today= sh.worksheet('Today')   if 'Today'   in [w.title for w in sh.worksheets()] else sh.add_worksheet('Today',50,20)

# 5. delta calc as before (reuse previous logic but using only rank/price)
try:
    prev=pd.DataFrame(ws_hist.get_all_records()).dropna()
except:
    prev=pd.DataFrame()
if not prev.empty and set(['asin','rank','price','date']).issubset(prev.columns):
    latest=(prev.sort_values('date').groupby('asin',as_index=False).last()[['asin','rank','price']]
            .rename(columns={'rank':'rank_prev','price':'price_prev'}))
    df_today=df_today.merge(latest,on='asin',how='left')
    df_today['rank_delta_num']=df_today['rank_prev']-df_today['rank']
    df_today['price_delta_num']=df_today['price']-df_today['price_prev']
else:
    df_today['rank_delta_num']=None
    df_today['price_delta_num']=None

def fmt(val,is_price=False):
    if pd.isna(val) or val==0: return '-'
    arrow='△' if val>0 else '▽'
    return f"{arrow}{abs(val):.2f}" if is_price else f"{arrow}{abs(int(val))}"

df_today['rank_delta']=df_today['rank_delta_num'].apply(fmt)
df_today['price_delta']=df_today['price_delta_num'].apply(lambda x:fmt(x,True))

cols=['asin','title','rank','price','url','date','rank_delta','price_delta']
df_today=df_today[cols]
df_today = df_today.fillna("")

# History header
if not ws_hist.get_all_values():
    ws_hist.append_row(cols,value_input_option='RAW')
ws_hist.append_rows(df_today.values.tolist(),value_input_option='RAW')
ws_today.clear(); ws_today.update([cols]+df_today.values.tolist(),value_input_option='RAW')

print('✓ 업데이트 완료: LG 모니터',len(df_today))
