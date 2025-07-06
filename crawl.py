# crawl_scroll.py
"""
Amazon.de 베스트셀러 ▸ Monitors 1~100위
- LG 모니터 필터, 가격·순위·변동 기록 (스크롤 포함)
"""

import os, re, json, base64, datetime, time, requests, pandas as pd, gspread, pytz
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# ────────────────────────── 1. Selenium 준비 ──────────────────────────
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

def get_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,4000")
    options.add_argument("--lang=de-DE")
    options.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0 Safari/537.36"
    )

    # ✅ 서비스 객체로 경로 명시
    service = Service(executable_path=ChromeDriverManager().install())

    # ✅ service, options 모두 키워드 인자로 명확히 전달
    return webdriver.Chrome(service=service, options=options)

BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"  # pg=1|2

# ────────────────────────── 2. 페이지 가져오기 (스크롤 포함) ──────────────────────────
def fetch_page_soup(page: int, driver):
    url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}"
    driver.get(url)

    # ★ 2‑A. Amazon 쿠키(언어/통화) 강제 ‑ 선택 사항
    driver.add_cookie({"name": "lc-main", "value": "de_DE"})
    driver.add_cookie({"name": "i18n-prefs", "value": "EUR"})
    driver.refresh()

    # ★ 2‑B. 스크롤 : 새 아이템이 더 이상 증가하지 않을 때까지
    SCROLL_PAUSE = 5
    last_count = 0
    while True:
        # 스크롤 끝까지
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)

        cards_now = driver.find_elements(By.CSS_SELECTOR, "div.zg-grid-general-faceout, div.p13n-sc-uncoverable-faceout")
        if len(cards_now) == last_count:   # 변화 없음 → 종료
            break
        last_count = len(cards_now)

    # ★ 2‑C. BeautifulSoup 변환
    html = driver.page_source
    if "Enter the characters you see below" in html:
        raise RuntimeError("Amazon CAPTCHA!")
    soup = BeautifulSoup(html, "lxml")
    return soup.select("div.zg-grid-general-faceout") or soup.select("div.p13n-sc-uncoverable-faceout")

# ────────────────────────── 3. 전체 1‑100위 카드 수집 ──────────────────────────
driver = get_driver()
cards = []
for pg in (1, 2):
    cards += fetch_page_soup(pg, driver)
driver.quit()
print(f"[INFO] total cards fetched after scroll: {len(cards)}")  # 기대값 ≈ 100

# ────────────────────────── 4. 이후 로직은 기존 코드 재사용 ──────────────────────────
def pick_title(card):
    for sel in [
        'span[class*="p13n-sc-css-line-clamp"]',
        '[title]',
        '.p13n-sc-truncate-desktop-type2',
        '.zg-text-center-align span.a-size-base',
    ]:
        t = card.select_one(sel)
        if t:
            txt = t.get("title", "") if sel == "[title]" else t.get_text(strip=True)
            if txt:
                return txt
    img = card.select_one("img")
    return img.get("alt", "").strip() if img else ""

def pick_price(card):
    p = card.select_one("span.p13n-sc-price")
    if p:
        return p.get_text(strip=True)
    whole = card.select_one("span.a-price-whole")
    frac = card.select_one("span.a-price-fraction")
    if whole:
        txt = whole.get_text(strip=True).replace(".", "").replace(",", ".")
        if frac:
            txt += frac.get_text(strip=True)
        return txt
    return ""

def money_to_float(txt):
    val = re.sub(r"[^0-9,\.]", "", txt).replace(".", "").replace(",", ".")
    try:
        return float(val)
    except:
        return None

items = []
for idx, card in enumerate(cards, start=1):       # ★ idx == 실제 랭킹 (1~100)
    a = card.select_one("a.a-link-normal[href*='/dp/']")
    if not a:
        continue
    title = pick_title(card) or a.get_text(" ", strip=True)
    if not re.search(r"\bLG\b", title, re.I):
        continue                                     # LG 필터
    link = "https://www.amazon.de" + a["href"].split("?", 1)[0]
    asin = re.search(r"/dp/([A-Z0-9]{10})", link).group(1)
    price_val = money_to_float(pick_price(card))
    items.append(
        {
            "asin": asin,
            "title": title,
            "url": link,
            "price": price_val,
            "rank": idx,            # ★ 스크롤 덕분에 정확
        }
    )

df_today = pd.DataFrame(items).sort_values("rank").reset_index(drop=True)

# ────────────────────────── 5. 날짜 및 Google Sheet 기록 (기존 그대로) ──────────────────────────
kst = pytz.timezone("Asia/Seoul")
df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

# … 이하 History 비교·업데이트 부분은 기존 코드와 동일 …

print("✓ 업데이트 완료: LG 모니터", len(df_today))
