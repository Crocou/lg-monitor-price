#!/usr/bin/env python3
# crawl_selenium.py
"""
Amazon.de ▸ Monitors 베스트셀러 1~100위 (Selenium + Infinite Scroll)
- 1페이지(1~50위)는 스크롤로 50개 모두 로딩된 뒤에야 2페이지(51~100위)로 이동
- LG 모니터만 필터링, 가격·순위·변동 계산
- Google Sheet: History 누적 / Today 최신, 3회 재시도
"""

import os, re, json, base64, datetime, time, pandas as pd, gspread, pytz
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# ───── Selenium ─────
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

BASE_URL      = "https://www.amazon.de/gp/bestsellers/computers/429868031"
SCROLL_PAUSE  = 1.2          # 스크롤 후 대기 시간(초)
MAX_SCROLLS   = 25           # 페이지당 최대 스크롤 횟수
EXPECTED_PER_PAGE = 50       # 1페이지에서 기대하는 카드 수

# ────────────────────────────────────────────────────────────────────────────────
# 1) Selenium 설정 및 스크롤 로더
# ────────────────────────────────────────────────────────────────────────────────
def init_driver() -> webdriver.Chrome:
    """헤드리스 Chrome WebDriver 객체 반환"""
    opts = Options()
    opts.add_argument("--headless=new")  # Chrome 115+ 전용
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--lang=de")       # 독일어 고정
    return webdriver.Chrome(options=opts)

def load_cards_with_scroll(driver: webdriver.Chrome,
                           expected: int = EXPECTED_PER_PAGE,
                           max_scrolls: int = MAX_SCROLLS) -> None:
    """
    현재 페이지에서 expected개 카드가 보일 때까지 아래로 스크롤한다.
    결과는 driver.page_source로 확인.
    """
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(max_scrolls):
        cards = driver.find_elements(
            By.CSS_SELECTOR,
            "div.zg-grid-general-faceout, div.p13n-sc-uncoverable-faceout",
        )
        if len(cards) >= expected:
            break  # 충분히 로드됨
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:    # 더 이상 로드 불가
            break
        last_height = new_height

def scrape_all_pages() -> list[tuple[BeautifulSoup, int]]:
    """
    Selenium으로 1·2 페이지 스크랩 후 각 카드(bs4 element)와 페이지 번호 반환.
    1페이지 카드가 50개 미만이면 예외를 발생시켜 2페이지 진행을 막는다.
    """
    driver = init_driver()
    all_cards: list[tuple[BeautifulSoup, int]] = []
    try:
        for pg in (1, 2):
            url = BASE_URL if pg == 1 else f"{BASE_URL}?pg={pg}"
            driver.get(url)
            load_cards_with_scroll(driver, expected=EXPECTED_PER_PAGE if pg == 1 else 1)
            soup = BeautifulSoup(driver.page_source, "lxml")
            cards = soup.select("div.zg-grid-general-faceout") \
                 or soup.select("div.p13n-sc-uncoverable-faceout")
            if pg == 1 and len(cards) < EXPECTED_PER_PAGE:
                raise RuntimeError("1페이지에서 50개 모두 로드되지 않았습니다. 레이아웃 변동 여부 확인 요망.")
            for c in cards:
                all_cards.append((c, pg))
    finally:
        driver.quit()
    return all_cards

# ────────────────────────────────────────────────────────────────────────────────
# 2) BeautifulSoup 헬퍼 (기존 로직 유지)
# ────────────────────────────────────────────────────────────────────────────────
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
    whole = card.select_one('span.a-price-whole')
    frac  = card.select_one('span.a-price-fraction')
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

# ────────────────────────────────────────────────────────────────────────────────
# 3) 데이터 수집 & 가공
# ────────────────────────────────────────────────────────────────────────────────
def collect_items() -> pd.DataFrame:
    all_cards = scrape_all_pages()
    page_count = max((p for _, p in all_cards), default=0)
    print(f"[INFO] containers: {len(all_cards)} (pages: {page_count})")

    items = []
    for idx, (card, pg) in enumerate(all_cards, start=1):
        rank_text = card.select_one('.zg-badge-text')
        rank_on_pg = int(rank_text.get_text(strip=True).lstrip('#')) \
                     if rank_text else ((idx - 1) % 50 + 1)
        abs_rank = (pg - 1) * 50 + rank_on_pg

        a = card.select_one("a.a-link-normal[href*='/dp/']")
        if not a:
            continue
        title = pick_title(card) or a.get_text(" ", strip=True)
        if 'LG' not in title.upper():       # LG 모니터 필터
            continue
        link = "https://www.amazon.de" + a['href'].split('?', 1)[0]
        asin = re.search(r"/dp/([A-Z0-9]{10})", link).group(1)
        price_val = money_to_float(pick_price(card))
        items.append(dict(
            asin=asin, title=title, rank=abs_rank,
            price=price_val, url=link
        ))

    if not items:
        raise RuntimeError("LG 모니터를 찾을 수 없습니다.")

    items.sort(key=lambda x: x['rank'])
    df = pd.DataFrame(items)
    return df

# ────────────────────────────────────────────────────────────────────────────────
# 4) Google Sheet 갱신 (기존 로직 그대로)
# ────────────────────────────────────────────────────────────────────────────────
def update_sheets(df_today: pd.DataFrame):
    kst = pytz.timezone('Asia/Seoul')
    df_today['date'] = datetime.datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        json.loads(base64.b64decode(os.environ['GCP_SA_BASE64']).decode()),
        scopes=SCOPES,
    )
    sh = gspread.authorize(creds).open_by_key(os.environ['SHEET_ID'])
    ws_hist = sh.worksheet('History') if 'History' in [w.title for w in sh.worksheets()] \
              else sh.add_worksheet('History', 2000, 20)
    ws_today = sh.worksheet('Today')  if 'Today'  in [w.title for w in sh.worksheets()] \
              else sh.add_worksheet('Today',  100, 20)

    try:
        prev = pd.DataFrame(ws_hist.get_all_records())
    except Exception:
        prev = pd.DataFrame()

    if not prev.empty and {'asin','rank','price','date'}.issubset(prev.columns):
        latest = (prev.sort_values('date')
                       .groupby('asin').last().reset_index()[['asin','rank','price']]
                       .rename(columns={'rank':'rank_prev','price':'price_prev'}))
        df_today = df_today.merge(latest, on='asin', how='left')
        df_today['rank_delta_num']  = df_today['rank_prev']  - df_today['rank']
        df_today['price_delta_num'] = df_today['price']      - df_today['price_prev']
    else:
        df_today[['rank_delta_num', 'price_delta_num']] = None

    fmt = lambda v,p=False: '-' if (pd.isna(v) or v==0) \
          else ('△' if v>0 else '▽') + (f"{abs(v):.2f}" if p else str(abs(int(v))))
    df_today['rank_delta']  = df_today['rank_delta_num'].apply(fmt)
    df_today['price_delta'] = df_today['price_delta_num'].apply(lambda x: fmt(x, True))

    cols = ['asin','title','rank','price','url','date','rank_delta','price_delta']
    df_today = df_today[cols].fillna("")

    for attempt in range(3):
        try:
            if not ws_hist.get_all_values():
                ws_hist.append_row(cols, value_input_option='RAW')
            ws_hist.append_rows(df_today.values.tolist(), value_input_option='RAW')
            ws_today.clear()
            ws_today.update([cols] + df_today.values.tolist(), value_input_option='RAW')
            break
        except gspread.exceptions.APIError as e:
            if attempt == 2:
                raise
            print('[WARN] Sheets API error, retrying...', e)
            time.sleep(2)

    print('✓ 완료, LG 모니터', len(df_today))

# ────────────────────────────────────────────────────────────────────────────────
# 5) Main
# ────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df_today = collect_items()
    update_sheets(df_today)
