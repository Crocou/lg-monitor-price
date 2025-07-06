#!/usr/bin/env python3
# crawl_amazon_monitors.py
"""
Amazon.de 베스트셀러 ▸ Monitors 1~100위 (두 페이지)
─────────────────────────────────────────────────────
● 지정 영역(상품 리스트 <OL>) 안에서만 Partial Scroll
● 스크롤마다 1 초 대기, 새 항목이 없으면 루프 종료
● Next 페이지 버튼 클릭 후 동일 로직 반복
● LG 모니터만 추출, 절대 순위·가격·변동(△▽) 계산
● Google Sheets(HISTORY, TODAY) 기록
"""

# ─────────────────────────────
# 0. 기본 의존성
# ─────────────────────────────
import os, re, json, base64, datetime, time, pytz, pandas as pd, gspread
from typing import List, Dict

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Google Sheets
from google.oauth2.service_account import Credentials

# ─────────────────────────────
# 1. 설정값
# ─────────────────────────────
URL_BASE = "https://www.amazon.de/gp/bestsellers/computers/429868031"  # page 1
SCROLL_AREA_XPATH = (
    "(//OL[contains(@class,"
    "'a-ordered-list a-vertical p13n-gridRow _cDEzb_grid-row_3Cywl')]/"
    "li[contains(@class,'zg-no-numbers')])/.."
)
ITEM_XPATH = SCROLL_AREA_XPATH + "/li[contains(@class,'zg-no-numbers')]"
NEXT_BTN_XPATH = "//li[@class='a-last']/a"      # Amazon 베스트셀러 '다음' 화살표

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

HEADLESS = True            # CAPTCHA 잦으면 False 로 두고 테스트

SHEET_ID = os.environ["SHEET_ID"]
SA_B64   = os.environ["GCP_SA_BASE64"]          # 서비스 계정 JSON → base64

# ─────────────────────────────
# 2. Selenium 초기화
# ─────────────────────────────
def init_driver() -> webdriver.Chrome:
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--lang=de-DE,de")
    opts.add_argument(f"user-agent={USER_AGENT}")
    opts.add_argument("--window-size=1920,1080")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

# ─────────────────────────────
# 3. Partial Scroll 구현
# ─────────────────────────────
def partial_scroll(driver: webdriver.Chrome, container) -> None:
    """컨테이너 내부를 끝까지 스크롤(1 초 대기, 새 콘텐츠 없으면 종료)"""
    while True:
        pre_count = len(container.find_elements(By.XPATH, "./li"))
        driver.execute_script(
            "arguments[0].scrollTop = arguments[0].scrollHeight;", container
        )
        time.sleep(1)
        post_count = len(container.find_elements(By.XPATH, "./li"))
        if post_count == pre_count:
            break

# ─────────────────────────────
# 4. 페이지 처리
# ─────────────────────────────
def parse_price(txt: str):
    clean = re.sub(r"[^\d,\.]", "", txt).replace(".", "").replace(",", ".")
    try:
        return float(clean)
    except ValueError:
        return None

def parse_one_page(driver) -> List[Dict]:
    """현재 페이지에서 LG 모니터 카드 정보 추출"""
    # 스크롤 영역 로드 기다림
    container = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, SCROLL_AREA_XPATH))
    )
    partial_scroll(driver, container)

    items = []
    cards = container.find_elements(By.XPATH, "./li")
    for idx, li in enumerate(cards, start=1):
        # 절대 순위 = (현재 페이지 - 1)*50 + idx
        page_num = int(driver.current_url.split("pg=")[-1].split("&")[0]) if "pg=" in driver.current_url else 1
        abs_rank = (page_num - 1) * 50 + idx

        # 링크·제목
        a = li.find_element(By.CSS_SELECTOR, "a.a-link-normal[href*='/dp/']")
        title = li.text.split("\n")[0].strip()
        if not re.search(r"\bLG\b", title, re.I):
            continue

        url  = "https://www.amazon.de" + a.get_attribute("href").split("?", 1)[0]
        asin = re.search(r"/dp/([A-Z0-9]{10})", url)
        asin = asin.group(1) if asin else None

        # 가격
        try:
            price_txt = li.find_element(By.CSS_SELECTOR, "span.a-offscreen").text
        except:
            price_txt = ""
        price_val = parse_price(price_txt)

        items.append(
            {
                "asin": asin,
                "title": title,
                "rank": abs_rank,
                "price": price_val,
                "url": url,
            }
        )

    return items

def go_next_page(driver) -> bool:
    """다음 페이지가 있으면 클릭 후 True, 없으면 False"""
    try:
        btn = driver.find_element(By.XPATH, NEXT_BTN_XPATH)
        driver.execute_script("arguments[0].scrollIntoView();", btn)
        btn.click()
        time.sleep(1)
        return True
    except:
        return False

# ─────────────────────────────
# 5. 메인 크롤링 로직
# ─────────────────────────────
def crawl() -> pd.DataFrame:
    driver = init_driver()
    driver.get(URL_BASE)
    all_items = []

    page = 1
    while True:
        print(f"[INFO] 페이지 {page} 수집…")
        all_items.extend(parse_one_page(driver))
        if not go_next_page(driver):
            break
        page += 1

    driver.quit()

    if not all_items:
        raise RuntimeError("LG 모니터를 찾지 못했습니다.")
    # 절대 순위 기준 정렬
    all_items.sort(key=lambda x: x["rank"])
    df = pd.DataFrame(all_items)
    # 타임스탬프(Asia/Seoul)
    kst = pytz.timezone("Asia/Seoul")
    df["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")
    return df

# ─────────────────────────────
# 6. Google Sheets 업데이트
# ─────────────────────────────
def setup_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds  = Credentials.from_service_account_info(
        json.loads(base64.b64decode(SA_B64).decode()), scopes=scopes
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)

    def ensure_ws(title, rows=1000, cols=20):
        try:
            return sh.worksheet(title)
        except gspread.WorksheetNotFound:
            return sh.add_worksheet(title, rows, cols)

    return ensure_ws("History"), ensure_ws("Today", 200, 20)

def compute_deltas(df_today: pd.DataFrame, ws_hist) -> pd.DataFrame:
    try:
        prev_df = pd.DataFrame(ws_hist.get_all_records())
    except:
        prev_df = pd.DataFrame()

    if not prev_df.empty and {"asin", "rank", "price", "date"} <= set(prev_df.columns):
        latest = (
            prev_df.sort_values("date")
            .groupby("asin", as_index=False)
            .last()[["asin", "rank", "price"]]
            .rename(columns={"rank": "rank_prev", "price": "price_prev"})
        )
        df_today = df_today.merge(latest, on="asin", how="left")
        df_today["rank_delta_num"]  = df_today["rank_prev"]  - df_today["rank"]
        df_today["price_delta_num"] = df_today["price"] - df_today["price_prev"]
    else:
        df_today["rank_delta_num"] = df_today["price_delta_num"] = None

    # Δ 문자열
    fmt = (
        lambda v, p=False: "-"
        if (pd.isna(v) or v == 0)
        else (("△" if v > 0 else "▽") + (f"{abs(v):.2f}" if p else str(abs(int(v)))))
    )
    df_today["rank_delta"]  = df_today["rank_delta_num"].apply(fmt)
    df_today["price_delta"] = df_today["price_delta_num"].apply(lambda x: fmt(x, True))
    return df_today

def update_sheets(df_today: pd.DataFrame, ws_hist, ws_today):
    cols = [
        "asin",
        "title",
        "rank",
        "price",
        "url",
        "date",
        "rank_delta",
        "price_delta",
    ]
    df_today = df_today[cols].fillna("")
    if not ws_hist.get_all_values():
        ws_hist.append_row(cols, value_input_option="RAW")
    ws_hist.append_rows(df_today.values.tolist(), value_input_option="RAW")
    ws_today.clear()
    ws_today.update([cols] + df_today.values.tolist(), value_input_option="RAW")
    print("✓ Google Sheets 업데이트 완료")

# ─────────────────────────────
# 7. 실행
# ─────────────────────────────
if __name__ == "__main__":
    today_df = crawl()
    ws_hist, ws_today = setup_sheet()
    today_df = compute_deltas(today_df, ws_hist)
    update_sheets(today_df, ws_hist, ws_today)
    print("✓ 전체 작업 완료 — LG 모니터", len(today_df))
