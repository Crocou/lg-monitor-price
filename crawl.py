#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Amazon.de 베스트셀러 ▸ Monitors 1~100위
- LG 모니터 필터, 가격·순위·변동 기록 (스크롤 포함)
- 배송지 PLZ 65760 적용
- Google Sheet + ▲/▼ 색상·볼드 포맷
"""

import os, re, json, base64, datetime, time
import pandas as pd, gspread, pytz
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# ────────────────────────── 1. Selenium 준비 ──────────────────────────
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait               
from selenium.webdriver.support import expected_conditions as EC       
from selenium.common.exceptions import TimeoutException                

def get_driver() -> webdriver.Chrome:
    opt = webdriver.ChromeOptions()
    opt.add_argument("--headless=new")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--window-size=1280,4000")
    opt.add_argument("--lang=de-DE")
    opt.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0 Safari/537.36"
    )
    # Selenium Manager 이용: service=None
    return webdriver.Chrome(service=None, options=opt)

# ────────────────────────── 2. 도우미 ──────────────────────────
def set_postcode(driver: webdriver.Chrome, zipcode="65760", timeout=8):
    """모달 열고 PLZ 입력 (UI 변경 대비 다중 셀렉터 + explicit wait)"""
    try:
        btn = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR,
                 "#nav-global-location-popover-link, #glow-ingress-block")
            )
        )
        btn.click()

        zip_input = WebDriverWait(driver, timeout).until(
            EC.visibility_of_element_located((By.ID, "GLUXZipUpdateInput"))
        )
        zip_input.clear()
        zip_input.send_keys(zipcode)
        driver.find_element(By.CSS_SELECTOR,
            "#GLUXZipUpdate > span > input").click()

        WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.NAME, "glowDoneButton"))
        ).click()
        time.sleep(1)
    except TimeoutException:
        print("[WARN] PLZ 버튼/필드 탐색 실패—레이아웃 변동?")
    except Exception as e:
        print("[WARN] PLZ 설정 실패:", e)

def pick_price(card) -> str:
    """가격 문자열 '123,99' 반환"""
    p = card.select_one("span.p13n-sc-price")
    if p:
        return p.get_text(strip=True)

    price_box = card.select_one("span.a-price")
    if not price_box:
        return ""

    whole = price_box.select_one("span.a-price-whole")
    frac  = price_box.select_one("span.a-price-fraction")
    if whole:
        whole_num = re.sub(r"[^\d]", "", whole.get_text())
        frac_num  = re.sub(r"[^\d]", "", frac.get_text()) if frac else "00"
        return f"{whole_num},{frac_num}"
    return ""

def money_to_float(txt: str):
    if not txt:
        return None
    num = re.sub(r"[^\d,]", "", txt).replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return None

def pick_title(card):
    for sel in [
        'span[class*="p13n-sc-css-line-clamp"]', '[title]',
        '.p13n-sc-truncate-desktop-type2',
        '.zg-text-center-align span.a-size-base',
    ]:
        t = card.select_one(sel)
        if t:
            return t.get("title", "") if sel == "[title]" else t.get_text(strip=True)
    img = card.select_one("img")
    return img.get("alt", "").strip() if img else ""

# ────────────────────────── 3. 페이지 크롤러 ──────────────────────────
BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
SELECTOR = "div.zg-grid-general-faceout, div.p13n-sc-uncoverable-faceout"

def fetch_cards(page: int, driver):
    url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}"
    driver.get(url)
    if page == 1:
        set_postcode(driver, "65760")

    # lazy-load 스크롤
    last = 0
    for _ in range(10):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(5)
        cur = len(driver.find_elements(By.CSS_SELECTOR, SELECTOR))
        if cur == last or cur >= 50:
            break
        last = cur

    soup = BeautifulSoup(driver.page_source, "lxml")
    return soup.select(SELECTOR)

# ────────────────────────── 4. 실행 ──────────────────────────
driver = get_driver()
cards = [c for pg in (1, 2) for c in fetch_cards(pg, driver)]
driver.quit()
print(f"[INFO] total cards fetched: {len(cards)}")

items = []
for idx, card in enumerate(cards, 1):
    a = card.select_one("a.a-link-normal[href*='/dp/']")
    if not a:
        continue
    title = pick_title(card) or a.get_text(" ", strip=True)
    if not re.search(r"\bLG\b", title, re.I):
        continue
    link = "https://www.amazon.de" + a["href"].split("?", 1)[0]
    asin = re.search(r"/dp/([A-Z0-9]{10})", link).group(1)
    price_val = money_to_float(pick_price(card))
    items.append(dict(asin=asin, title=title, url=link, price=price_val, rank=idx))

df_today = pd.DataFrame(items).sort_values("rank").reset_index(drop=True)
kst = pytz.timezone("Asia/Seoul")
df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

# ────────────────────────── 5. Google Sheet 기록 ──────────────────────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(
    json.loads(base64.b64decode(os.environ["GCP_SA_BASE64"]).decode()), scopes=SCOPES
)
sh = gspread.authorize(creds).open_by_key(os.environ["SHEET_ID"])
ws_hist = sh.worksheet("History") if "History" in [w.title for w in sh.worksheets()] \
          else sh.add_worksheet("History", 2000, 20)
ws_today = sh.worksheet("Today") if "Today" in [w.title for w in sh.worksheets()] \
          else sh.add_worksheet("Today", 100, 20)

# Δ 계산
prev = pd.DataFrame(ws_hist.get_all_records()).dropna() if ws_hist.get_all_values() else pd.DataFrame()
if not prev.empty and {"asin","rank","price","date"} <= set(prev.columns):
    latest = (prev.sort_values("date").groupby("asin", as_index=False).last()
              [ ["asin","rank","price"] ].rename(columns={"rank":"rank_prev","price":"price_prev"}))
    df_today = df_today.merge(latest, on="asin", how="left")
else:
    df_today[["rank_prev","price_prev"]] = None

for col in ["price","price_prev","rank_prev"]:
    df_today[col] = pd.to_numeric(df_today[col], errors="coerce")

df_today["rank_delta_num"]  = df_today["rank_prev"]  - df_today["rank"]
df_today["price_delta_num"] = df_today["price"]      - df_today["price_prev"]

def fmt(v, price=False):
    if pd.isna(v) or v == 0:
        return "-"
    arrow = "▲" if v > 0 else "▼"
    return f"{arrow} {abs(v):.2f}" if price else f"{arrow} {abs(int(v))}"

df_today["rank_delta"]  = df_today["rank_delta_num"].apply(fmt)
df_today["price_delta"] = df_today["price_delta_num"].apply(lambda x: fmt(x, True))

cols = ["asin","title","rank","price","url","date","rank_delta","price_delta"]
df_today = df_today[cols].fillna("")

# 기록
if not ws_hist.get_all_values():
    ws_hist.append_row(cols, value_input_option="USER_ENTERED")
ws_hist.append_rows(df_today.values.tolist(), value_input_option="USER_ENTERED")
ws_today.clear()
ws_today.update([cols] + df_today.values.tolist(), value_input_option="USER_ENTERED")

# ▲/▼ 서식 (gspread-formatting가 있으면 적용)
try:
    from gspread_formatting import format_cell_ranges, CellFormat, TextFormat, Color
    RED, BLUE = Color(1,0,0), Color(0,0,1)
    delta_cols = {"rank_delta": "G", "price_delta": "H"}
    fmt_ranges=[]
    for i, row in df_today.iterrows():
        r=i+2
        for k,col in delta_cols.items():
            val=row[k]
            if isinstance(val,str) and val.startswith("▲"):
                fmt_ranges.append((f"{col}{r}", CellFormat(textFormat=TextFormat(bold=True,foregroundColor=RED))))
            elif isinstance(val,str) and val.startswith("▼"):
                fmt_ranges.append((f"{col}{r}", CellFormat(textFormat=TextFormat(bold=True,foregroundColor=BLUE))))
    if fmt_ranges:
        format_cell_ranges(ws_today, fmt_ranges)
except ImportError:
    print("[INFO] gspread-formatting 미설치 → 서식 스킵")

print("✓ Google Sheet 업데이트 완료 — LG 모니터", len(df_today))
