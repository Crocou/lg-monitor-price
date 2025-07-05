#!/usr/bin/env python3
# crawl.py — Selenium 버전 (2025-07)
"""
Amazon.de 베스트셀러 ▸ Monitors 1~100위 중 LG 제품을 수집
 - Selenium + Chrome headless
 - 각 페이지(1·2) 50개 카드 확보를 보장
 - CAPTCHA 감지 시 스크린샷·HTML 저장 후 재시도
 - Google Sheet(History, Today) 기록 + △ ▽ 계산
"""

import os, re, json, base64, datetime, time, random
from typing import List
import pandas as pd
from bs4 import BeautifulSoup
import pytz

# ─────────────────────────────
# 0. 상수
# ─────────────────────────────
BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031"  # ?pg=1|2
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
}
CARD_SEL = 'div[data-p13n-asin-metadata]'  # 모든 레이아웃 공통
ROOT_JS = (
    "return document.querySelector('#zg-grid-view-root') "
    "|| document.querySelector('div[data-testid=\"gridViewport\"]');"
)

# ─────────────────────────────
# 1. Selenium 준비
# ─────────────────────────────
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    WebDriverException,
    NoSuchElementException,
    TimeoutException,
)
from webdriver_manager.chrome import ChromeDriverManager


def get_driver() -> webdriver.Chrome:
    opts = Options()
    # 새 헤드리스 모드 (Chrome 115+). GitHub Actions 호환
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--window-size=1366,900")
    opts.add_argument(f'user-agent={HEADERS["User-Agent"]}')

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(60)
    return driver


def is_captcha(html: str) -> bool:
    lower = html.lower()
    return ("captcha" in lower) or ("not a robot" in lower)


def save_debug(tag: str, page_no: int, driver: webdriver.Chrome, html: str):
    """실패 상황 캡처(PNG)와 HTML 저장"""
    png = f"debug_{tag}_pg{page_no}.png"
    htm = f"debug_{tag}_pg{page_no}.html"
    try:
        driver.save_screenshot(png)
        with open(htm, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[DEBUG] {png}, {htm} 저장")
    except WebDriverException:
        pass


def fetch_cards(page_no: int, driver: webdriver.Chrome, min_cards: int = 50) -> List[str]:
    """한 페이지(1|2)에서 min_cards개 이상 카드 outerHTML 리스트 반환."""
    url = BASE_URL if page_no == 1 else f"{BASE_URL}?pg={page_no}&ref_=zg_bs_pg_{page_no}"
    driver.get(url)
    time.sleep(3)

    html0 = driver.page_source
    if is_captcha(html0):
        save_debug("captcha_init", page_no, driver, html0)
        raise RuntimeError("Amazon CAPTCHA 차단(초기)")

    # 내부 스크롤 컨테이너
    root = driver.execute_script(ROOT_JS)
    if root is None:
        save_debug("root_missing", page_no, driver, driver.page_source)
        raise RuntimeError("스크롤 컨테이너 탐지 실패")

    last_cnt, stagnate = 0, 0
    for _ in range(120):  # MAX_SCROLLS
        cards = driver.find_elements(By.CSS_SELECTOR, CARD_SEL)
        if len(cards) >= min_cards:
            break

        driver.execute_script("arguments[0].scrollBy(0, arguments[0].clientHeight);", root)
        time.sleep(0.4 + random.random() * 0.2)

        cur_cnt = len(cards)
        stagnate = stagnate + 1 if cur_cnt == last_cnt else 0
        last_cnt = cur_cnt
        if stagnate >= 15:
            break

    cards = driver.find_elements(By.CSS_SELECTOR, CARD_SEL)
    if len(cards) < min_cards:
        save_debug("too_few", page_no, driver, driver.page_source)
        raise RuntimeError(f"{page_no}페이지 카드 {len(cards)}/{min_cards}")

    return [c.get_attribute("outerHTML") for c in cards]


# ─────────────────────────────
# 2. 파싱 헬퍼
# ─────────────────────────────
def pick_title(card_soup):
    selectors = [
        'span[class*="p13n-sc-css-line-clamp"]',
        '[title]',
        ".p13n-sc-truncate-desktop-type2",
        ".zg-text-center-align span.a-size-base",
    ]
    for sel in selectors:
        t = card_soup.select_one(sel)
        if t:
            return (t.get("title") if sel == "[title]" else t.get_text(strip=True)) or ""
    img = card_soup.select_one("img")
    return img.get("alt", "").strip() if img else ""


def pick_price(card_soup):
    p = card_soup.select_one("span.a-offscreen")
    if p and p.get_text(strip=True):
        return p.get_text(strip=True)
    p = card_soup.select_one("span.p13n-sc-price")
    if p and p.get_text(strip=True):
        return p.get_text(strip=True)
    whole = card_soup.select_one("span.a-price-whole")
    frac = card_soup.select_one("span.a-price-fraction")
    if whole:
        txt = whole.get_text(strip=True).replace(".", "").replace(",", ".")
        if frac:
            txt += frac.get_text(strip=True)
        return txt
    return ""


def money_to_float(txt):
    clean = re.sub(r"[^0-9,\.]", "", txt).replace(".", "").replace(",", ".")
    try:
        return float(clean)
    except ValueError:
        return None


# ─────────────────────────────
# 3. 크롤링 실행
# ─────────────────────────────
def crawl():
    driver = get_driver()
    all_cards = []
    try:
        for pg in (1, 2):
            print(f"[INFO] 페이지 {pg} 수집 시작…")
            html_list = fetch_cards(pg, driver)
            all_cards.extend([(BeautifulSoup(h, "lxml"), pg) for h in html_list])
    finally:
        driver.quit()

    print(f"[INFO] 총 카드 수집: {len(all_cards)}")

    # ── LG 모니터 필터 + 절대 순위 계산
    items = []
    for idx, (card, page) in enumerate(all_cards, start=1):
        rank_tag = card.select_one(".zg-badge-text")
        rank_on_page = (
            int(rank_tag.get_text(strip=True).lstrip("#")) if rank_tag else ((idx - 1) % 50 + 1)
        )
        abs_rank = (page - 1) * 50 + rank_on_page

        a = card.select_one("a.a-link-normal[href*='/dp/']")
        if not a:
            continue

        title = pick_title(card) or a.get_text(" ", strip=True)
        if not re.search(r"\bLG\b", title, re.I):
            continue

        link = "https://www.amazon.de" + a["href"].split("?", 1)[0]
        asin_m = re.search(r"/dp/([A-Z0-9]{10})", link)
        asin = asin_m.group(1) if asin_m else None
        price_val = money_to_float(pick_price(card))

        items.append(
            {
                "asin": asin,
                "title": title,
                "rank": abs_rank,
                "price": price_val,
                "url": link,
            }
        )

    if not items:
        raise RuntimeError("LG 모니터를 찾지 못했습니다.")

    items.sort(key=lambda x: x["rank"])
    df_today = pd.DataFrame(items)

    # ─────────────────────────────
    # 4. 날짜 컬럼
    # ─────────────────────────────
    kst = pytz.timezone("Asia/Seoul")
    df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

    # ─────────────────────────────
    # 5. Google Sheets 기록
    # ─────────────────────────────
    from google.oauth2.service_account import Credentials
    import gspread

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    SHEET_ID = os.environ["SHEET_ID"]
    sa_json = json.loads(base64.b64decode(os.environ["GCP_SA_BASE64"]).decode())
    creds = Credentials.from_service_account_info(sa_json, scopes=SCOPES)
    sh = gspread.authorize(creds).open_by_key(SHEET_ID)

    def ensure_ws(name, rows=2000, cols=20):
        try:
            return sh.worksheet(name)
        except gspread.WorksheetNotFound:
            return sh.add_worksheet(name, rows, cols)

    ws_hist = ensure_ws("History")
    ws_today = ensure_ws("Today", 100, 20)

    # Δ 계산
    try:
        prev = pd.DataFrame(ws_hist.get_all_records())
    except Exception:
        prev = pd.DataFrame()

    if not prev.empty and {"asin", "rank", "price", "date"}.issubset(prev.columns):
        latest = (
            prev.sort_values("date")
            .groupby("asin", as_index=False)
            .last()[["asin", "rank", "price"]]
            .rename(columns={"rank": "rank_prev", "price": "price_prev"})
        )
        df_today = df_today.merge(latest, on="asin", how="left")
        df_today["rank_delta_num"] = df_today["rank_prev"] - df_today["rank"]
        df_today["price_delta_num"] = df_today["price"] - df_today["price_prev"]
    else:
        df_today["rank_delta_num"] = None
        df_today["price_delta_num"] = None

    def fmt(v, p=False):
        if pd.isna(v) or v == 0:
            return "-"
        sym = "△" if v > 0 else "▽"
        return sym + (f"{abs(v):.2f}" if p else str(abs(int(v))))

    df_today["rank_delta"] = df_today["rank_delta_num"].apply(fmt)
    df_today["price_delta"] = df_today["price_delta_num"].apply(lambda x: fmt(x, True))

    cols = ["asin", "title", "rank", "price", "url", "date", "rank_delta", "price_delta"]
    df_today = df_today[cols].fillna("")

    # 시트 업데이트
    if not ws_hist.get_all_values():
        ws_hist.append_row(cols, value_input_option="RAW")
    ws_hist.append_rows(df_today.values.tolist(), value_input_option="RAW")
    ws_today.clear()
    ws_today.update([cols] + df_today.values.tolist(), value_input_option="RAW")

    print(f"✓ 업데이트 완료: LG 모니터 {len(df_today)}개")


# ─────────────────────────────
# 6. 진입점
# ─────────────────────────────
if __name__ == "__main__":
    # 두 번까지 재시도 (CAPTCHA 등)
    for attempt in range(1, 3):
        try:
            crawl()
            break
        except Exception as e:
            print(f"[ERROR] {e} (시도 {attempt}/2)")
            if attempt == 2:
                raise
            time.sleep(5 + random.random() * 3)
