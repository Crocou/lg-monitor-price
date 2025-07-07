# crawl_scroll_zip65760.py
"""
Amazon.de 베스트셀러 ▸ Monitors 1~100위
- LG 모니터 필터, 가격·순위·변동 기록 (스크롤 포함)
- ★ 배송지(우편번호) 65760 고정 (UI 클릭 방식)
"""

import sys, os, re, json, base64, datetime, time, logging
import pandas as pd, gspread, pytz
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from gspread_formatting import format_cell_ranges, CellFormat, TextFormat, Color

# ─── 설정 상수 ──────────────────────────────────────────────────────
BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
CARD_SEL = "li.zg-no-numbers"

# ─── 0. 로깅 ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("crawl_cards.log", encoding="utf-8"),
              logging.StreamHandler(sys.stdout)],
)
logging.info("🔍 LG 모니터 크롤러 시작")

# ─── 1. Selenium ───────────────────────────────────────────────────
def get_driver():
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
    return webdriver.Chrome(options=opt)

# ★ 1-A. 우편번호를 UI로 설정 --------------------------------------
def set_zip_ui(driver, zip_code: str = "65760", timeout: int = 30):
    """UI 클릭 방식으로만 우편번호를 강제 설정한다.
       후보 id 다중 시도, 실패 시 TimeoutException 그대로 throw.
    """
    wait = WebDriverWait(driver, timeout)
    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
    logging.info("📦 페이지 로딩 완료, 우편번호 설정 시작 (%s)", zip_code)

    # 0) 쿠키 배너 닫기(있을 때만)
    try:
        logging.info("🔍 쿠키 배너 확인")
        wait.until(EC.element_to_be_clickable((By.ID, "sp-cc-accept"))).click()
        driver.execute_script("window.scrollTo(0, 0)")
        logging.info("✅ 쿠키 배너 닫힘")
    except TimeoutException:
        logging.info("ℹ️ 쿠키 배너 없음 또는 이미 닫힘")

    # 1) 위치 선택 버튼 클릭
    logging.info("📍 위치 설정 버튼 클릭 시도")
    wait.until(EC.element_to_be_clickable((By.ID, "nav-global-location-data-modal-action"))).click()
    logging.info("✅ 위치 설정 팝업 열림")

    # 2) 우편번호 입력
    logging.info("⌨️ 우편번호 입력란 찾는 중")
    input_el = wait.until(EC.presence_of_element_located((By.ID, "GLUXZipUpdateInput")))
    input_el.clear()
    input_el.send_keys(zip_code)
    logging.info("✅ 우편번호 입력 완료")

    # 3) Apply 클릭
    logging.info("🟡 'Apply' 버튼 클릭 시도")
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="GLUXZipUpdate"]/span/input'))).click()
    logging.info("✅ 'Apply' 버튼 클릭 완료")

    # 4) 닫기 버튼 클릭
    logging.info("🟡 'Confirm Close' 버튼 클릭 시도")
    wait.until(EC.element_to_be_clickable((By.ID, "GLUXConfirmClose"))).click()
    logging.info("✅ 위치 설정 팝업 닫힘")

    # 5) 최종 확인
    logging.info("🔍 헤더에 우편번호 반영 확인 중")
    wait.until(lambda d: zip_code in d.find_element(By.ID, "glow-ingress-line2").text)
    logging.info("🎯 우편번호 %s UI 방식 적용 성공", zip_code)


# ─── 2. 카드 파싱 ───────────────────────────────────────────────────
def fetch_cards_and_parse(page: int, driver):
    parsed_items = []

    url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}"
    logging.info("▶️  요청 URL (page %d): %s", page, url)
    driver.get(url)

    # 최소 한 장이라도 렌더 대기
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, CARD_SEL))
        )
    except TimeoutException:
        logging.error("⛔ page %d: 카드 0개 - 타임아웃", page)
        return []

    # 스크롤 로딩
    start = time.time()
    last, SCROLL_PAUSE, MAX_WAIT = 0, 3, 60
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)
        now = len(driver.find_elements(By.CSS_SELECTOR, CARD_SEL))
        if page == 1 and now < 50 and time.time() - start < MAX_WAIT:
            continue
        if now == last or time.time() - start >= MAX_WAIT:
            break
        last = now

    cards = driver.find_elements(By.CSS_SELECTOR, CARD_SEL)
    logging.info("✅ page %d 카드 %d개", page, len(cards))

    # 카드 루프
    for idx, card in enumerate(cards, 1):
        try:
            rank = int(re.sub(r"\D", "", card.find_element(
                By.XPATH, './/span[contains(@class,"zg-bdg-text")]').text))
        except (NoSuchElementException, ValueError, StaleElementReferenceException):
            continue

        try:
            title = card.find_element(
                By.XPATH, './/div[contains(@class,"_cDEzb_p13n-sc-css-line-clamp-2_EWgCb")]'
            ).text.strip()
        except NoSuchElementException:
            title = card.find_element(By.XPATH, './/img[@alt]').get_attribute("alt").strip()

        lg_match = bool(re.search(r"\bLG\b", title.replace("\u00a0", " "), re.I))

        # ─── 가격 ───
        price_raw = card.find_element(
            By.CSS_SELECTOR, 'span._cDEzb_p13n-sc-price_3mJ9Z'
        ).text.strip()                         # 없으면 예외 발생 → 크롤링 중단

        if not price_raw:                      # 빈 문자열이면 offers 문구
            try:
                offer_txt = card.find_element(
                    By.CSS_SELECTOR, 'span.a-color-secondary').text.strip()
                m = re.search(r'€[\d\.,]+', offer_txt)
                if m:
                    price_raw = m.group(0)
            except NoSuchElementException:
                pass

        # ─── 링크/ASIN ───
        try:
            href = card.find_element(By.XPATH, './/a[contains(@href,"/dp/")]').get_attribute("href")
            link = href.split("?", 1)[0] if href.startswith("http") else "https://www.amazon.de" + href.split("?", 1)[0]
            asin = re.search(r"/dp/([A-Z0-9]{10})", link).group(1)
        except Exception:
            continue

        if lg_match:
            parsed_items.append({
                "asin": asin,
                "title": title,
                "url": link,
                "price": price_raw,   # ★ strip 결과 그대로 저장
                "rank": rank,
            })

    return parsed_items

# ─── 3. 크롤링 실행 ────────────────────────────────────────────────
driver = get_driver()
try:
    driver.get("https://www.amazon.de/")
    set_zip_ui(driver, "65760")          # ★ UI 방식 우편번호 설정

    items = []
    for pg in (1, 2):
        items += fetch_cards_and_parse(pg, driver)
finally:
    driver.quit()

logging.info("LG 모니터 필터 후 %d개 남음", len(items))

# ─── 4. DataFrame & Google Sheet (가격은 문자열 그대로) ─────────────
cols = ["asin", "title", "url", "price", "rank"]
df_today = pd.DataFrame(items, columns=cols)

if df_today.empty:
    logging.info("LG 모니터 없음 → 시트 업데이트 생략")
    sys.exit(0)

df_today = df_today.sort_values("rank").reset_index(drop=True)
kst = pytz.timezone("Asia/Seoul")
df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

# price_delta 등 숫자 계산은 생략하거나 필요하면 별도 파싱 후 진행

# ─── 5. Google Sheet 기록 (기존 로직과 동일, price 컬럼은 문자열) ─────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(
    json.loads(base64.b64decode(os.environ["GCP_SA_BASE64"]).decode()),
    scopes=SCOPES,
)
gc = gspread.authorize(creds)
SHEET_ID = os.environ["SHEET_ID"]
sh = gc.open_by_key(SHEET_ID)

ws_hist = sh.worksheet("History") if "History" in [w.title for w in sh.worksheets()] else sh.add_worksheet("History", rows=2000, cols=20)
ws_today = sh.worksheet("Today") if "Today" in [w.title for w in sh.worksheets()] else sh.add_worksheet("Today", rows=100, cols=20)

if not ws_hist.get_all_values():
    ws_hist.append_row(cols + ["date"], value_input_option="USER_ENTERED")
ws_hist.append_rows(df_today[cols + ["date"]].values.tolist(), value_input_option="USER_ENTERED")

ws_today.clear()
ws_today.update([cols + ["date"]] + df_today[cols + ["date"]].values.tolist(), value_input_option="USER_ENTERED")

logging.info("Google Sheet 업데이트 완료 — LG 모니터 %d개", len(df_today))
print("✓ Google Sheet 업데이트 완료 — LG 모니터", len(df_today))
