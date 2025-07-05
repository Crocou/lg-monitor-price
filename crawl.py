#!/usr/bin/env python3
# crawl_selenium.py — Selenium 기반 (robust pagination + accurate rank + price)
"""
Amazon.de 베스트셀러 ▸ Monitors 1~100위 (두 페이지)
- LG 모니터만 수집
- 정확한 절대 순위
- 가격 추출(span.a-offscreen 등)
- △ ▽ – 변동(순위·가격)
- Google Sheet(History, Today) 기록
- robots.txt 및 meta[name=amazonbot] noarchive 준수
- link-level rel="nofollow" 링크 미추적
"""

import os, re, json, base64, datetime, time, random
from typing import List
import pandas as pd
from bs4 import BeautifulSoup
import pytz
import urllib.robotparser as robotparser

# ─────────────────────────────
# 0. 상수
# ─────────────────────────────
BASE_HOST = "https://www.amazon.de"
BASE_PATH = "/gp/bestsellers/computers/429868031"
BASE_URL = BASE_HOST + BASE_PATH  # ?pg=1|2
USER_AGENT = "Amazonbot/0.1"
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "de-DE,de;q=0.9,en;q=0.7"}

# ─────────────────────────────
# 1. robots.txt 파싱
# ─────────────────────────────
rp = robotparser.RobotFileParser()
rp.set_url(BASE_HOST + "/robots.txt")
rp.read()

# ─────────────────────────────
# 2. Selenium 설정
# ─────────────────────────────
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

CARD_SEL = 'div[data-p13n-asin-metadata]'
ROOT_JS = (
    "return document.querySelector('#zg-grid-view-root')"
    " || document.querySelector('div[data-testid=\"gridViewport\"]');"
)


def init_driver(headless=True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(f"--lang=de-DE,de")
    opts.add_argument(f"--window-size=1366,900")
    opts.add_argument(f'user-agent={USER_AGENT}')
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(60)
    return driver


def is_noarchive(soup: BeautifulSoup) -> bool:
    # page-level meta noarchive
    m = soup.find('meta', attrs={'name': 'amazonbot', 'content': 'noarchive'})
    return m is not None

# ─────────────────────────────
# 3. fetch helper (robots + meta + rel)
# ─────────────────────────────
def fetch_cards(page: int, driver: webdriver.Chrome) -> List[BeautifulSoup]:
    # robots.txt 허용 확인
    page_path = BASE_PATH + ("" if page == 1 else f"?pg={page}&ref_=zg_bs_pg_{page}")
    if not rp.can_fetch(USER_AGENT, BASE_HOST + page_path):
        raise RuntimeError(f"robots.txt에 의해 크롤링 차단됨: {page_path}")

    url = BASE_URL if page == 1 else BASE_HOST + page_path
    driver.get(url)

    # 컨테이너 로딩 대기
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, CARD_SEL))
        )
    except Exception:
        raise RuntimeError("Amazon CAPTCHA 차단 또는 레이아웃 변경")

    html = driver.page_source
    soup = BeautifulSoup(html, 'lxml')

    # meta noarchive 확인
    if is_noarchive(soup):
        raise RuntimeError("페이지 메타 noarchive 지시로 크롤링 생략됨")

    # 링크-level rel="nofollow" 링크는 무시 (추가 탐색 시)
    # (여기선 직접 링크를 사용하므로 패스)

    containers = soup.select(CARD_SEL)
    if len(containers) < 50:
        raise RuntimeError(f"{page}페이지 카드 수집 실패: {len(containers)}개")
    return containers

# ─────────────────────────────
# 4. 파싱 헬퍼
# ─────────────────────────────
def pick_title(card):
    selectors = [
        'span[class*="p13n-sc-css-line-clamp"]', '[title]',
        '.p13n-sc-truncate-desktop-type2',
        '.zg-text-center-align span.a-size-base'
    ]
    for sel in selectors:
        t = card.select_one(sel)
        if t:
            return (t.get('title') if sel == '[title]' else t.get_text(strip=True)) or ''
    img = card.select_one('img')
    return img.get('alt','').strip() if img else ''

def pick_price(card):
    p = card.select_one('span.a-offscreen')
    if p and p.get_text(strip=True): return p.get_text(strip=True)
    p = card.select_one('span.p13n-sc-price')
    if p and p.get_text(strip=True): return p.get_text(strip=True)
    whole = card.select_one('span.a-price-whole'); frac = card.select_one('span.a-price-fraction')
    if whole:
        txt = whole.get_text(strip=True).replace('.', '').replace(',', '.')
        if frac: txt += frac.get_text(strip=True)
        return txt
    return ''

def money_to_float(txt):
    clean = re.sub(r'[^0-9,\.]','', txt).replace('.', '').replace(',', '.')
    try: return float(clean)
    except: return None

# ─────────────────────────────
# 5. 스크래핑 및 시트 업데이트
# ─────────────────────────────
driver = init_driver(headless=True)
all_cards = []
for pg in (1,2):
    print(f"[INFO] 페이지 {pg} 수집 시작…")
    cards = fetch_cards(pg, driver)
    all_cards.extend([(c, pg) for c in cards])
driver.quit()
print(f"[INFO] total card containers: {len(all_cards)}")

items = []
for idx, (card, page) in enumerate(all_cards, start=1):
    rank_tag = card.select_one('.zg-badge-text')
    rank_on_page = rank_tag and int(rank_tag.get_text(strip=True).lstrip('#')) or ((idx-1)%50+1)
    abs_rank = (page-1)*50 + rank_on_page

    a = card.select_one("a.a-link-normal[href*='/dp/']")
    if not a: continue
    title = pick_title(card) or a.get_text(' ',strip=True)
    if not re.search(r'\bLG\b', title, re.I): continue
    link = BASE_HOST + a['href'].split('?',1)[0]
    match = re.search(r'/dp/([A-Z0-9]{10})', link)
    asin = match.group(1) if match else None
    price_val = money_to_float(pick_price(card))
    items.append({"asin":asin,"title":title,"rank":abs_rank,
                  "price":price_val,"url":link})
if not items: raise RuntimeError('LG 모니터 없음')
items.sort(key=lambda x: x['rank'])
df_today = pd.DataFrame(items)

kst = pytz.timezone('Asia/Seoul')
df_today['date'] = datetime.datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')

# Google Sheets
from google.oauth2.service_account import Credentials
import gspread
SCOPES=["https://www.googleapis.com/auth/spreadsheets"]
creds_json = json.loads(base64.b64decode(os.environ['GCP_SA_BASE64']).decode())
creds = Credentials.from_service_account_info(creds_json, scopes=SCOPES)
sh = gspread.authorize(creds).open_by_key(os.environ['SHEET_ID'])

ensure_ws=lambda name, r=2000,c=20: sh.worksheet(name) if name in [ws.title for ws in sh.worksheets()] else sh.add_worksheet(name,r,c)
ws_hist=ensure_ws('History'); ws_today=ensure_ws('Today',100,20)

try: prev=pd.DataFrame(ws_hist.get_all_records())
except: prev=pd.DataFrame()
if not prev.empty and set(['asin','rank','price','date']).issubset(prev.columns):
    latest=prev.sort_values('date').groupby('asin',as_index=False).last()[['asin','rank','price']].rename(columns={'rank':'rank_prev','price':'price_prev'})
    df_today=df_today.merge(latest,on='asin',how='left')
    df_today['rank_delta_num']=df_today['rank_prev']-df_today['rank']
    df_today['price_delta_num']=df_today['price']-df_today['price_prev']
else:
    df_today['rank_delta_num']=None; df_today['price_delta_num']=None

def fmt(v,p=False):
    return '-' if pd.isna(v) or v==0 else ('△' if v>0 else '▽') + (f"{abs(v):.2f}" if p else str(abs(int(v))))

df_today['rank_delta']=df_today['rank_delta_num'].apply(fmt)
df_today['price_delta']=df_today['price_delta_num'].apply(lambda x: fmt(x,True))
cols=['asin','title','rank','price','url','date','rank_delta','price_delta']
df_today=df_today[cols].fillna('')
if not ws_hist.get_all_values(): ws_hist.append_row(cols)
ws_hist.append_rows(df_today.values.tolist())
ws_today.clear(); ws_today.update([cols]+df_today.values.tolist())

print(f"✓ 업데이트 완료: LG 모니터 {len(df_today)}개")
