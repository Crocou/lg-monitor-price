# crawl_scroll_zip65760.py
"""
Amazon.de 베스트셀러 ▸ Monitors 1~100위
- LG 모니터 필터, 가격·순위·변동 기록 (스크롤 포함)
- 배송지(우편번호) 65760 고정 (UI 클릭 방식)
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

# ─── 상수 ─────────────────────────────────────────────────────────────
BASE_URL   = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
CARD_SEL   = "li.zg-no-numbers"
ZIP_CODE   = "65760"
SCROLL_PAUSE, MAX_SCROLL_WAIT = 10, 60
WAIT_CARD  = 20
FIRST_PAGE_TARGET = 50

# ─── 로깅 ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("crawl_cards.log", "w", "utf-8"),
              logging.StreamHandler(sys.stdout)],
)
logging.info("🔍 LG 모니터 크롤러 시작")

# ─── Selenium 드라이버 ───────────────────────────────────────────────
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

# ─── 배송지 UI 강제 변경 ─────────────────────────────────────────────
def force_zip_ui(driver, zip_code: str = ZIP_CODE):
    driver.find_element(By.ID, "nav-global-location-popover-link").click()
    zip_in = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "GLUXZipUpdateInput"))
    )
    zip_in.clear()
    zip_in.send_keys(zip_code)
    driver.find_element(By.ID, "GLUXZipUpdate").click()

    # 팝업 사라질 때까지 최대 10s
    try:
        WebDriverWait(driver, 10).until(
            EC.invisibility_of_element_located((By.ID, "GLUXModalDialog"))
        )
    except TimeoutException:
        try:
            driver.find_element(By.ID, "glow-ingress-close").click()
        except Exception:
            pass

    # 헤더에 zip 코드가 뜰 때까지 새로고침·확인
    for _ in range(3):
        driver.refresh()
        try:
            WebDriverWait(driver, 5).until(
                EC.text_to_be_present_in_element(
                    (By.CSS_SELECTOR, "#glow-ingress-line2"), zip_code
                )
            )
            logging.info("✅ 배송지 %s 적용 완료", zip_code)
            return
        except TimeoutException:
            continue
    raise RuntimeError("배송지 텍스트가 %s 으로 갱신되지 않았습니다." % zip_code)

# ─── 베스트셀러 페이지 크롤링 ─────────────────────────────────────────
def fetch_cards_and_parse(page: int, driver) -> list[dict]:
    parsed_items: list[dict] = []
    url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}"
    logging.info("▶️  요청 URL (page %d): %s", page, url)
    driver.get(url)

    # 카드 한 장이라도 뜰 때까지 대기
    WebDriverWait(driver, WAIT_CARD).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, CARD_SEL))
    )

    # 무한 스크롤
    start, last = time.time(), 0
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)
        now = len(driver.find_elements(By.CSS_SELECTOR, CARD_SEL))

        if page == 1 and now < FIRST_PAGE_TARGET and time.time() - start < MAX_SCROLL_WAIT:
            continue
        if now == last or time.time() - start >= MAX_SCROLL_WAIT:
            break
        last = now

    cards = driver.find_elements(By.CSS_SELECTOR, CARD_SEL)
    logging.info("✅ page %d 카드 %d개", page, len(cards))

    for idx, card in enumerate(cards, 1):
        # 랭크
        try:
            rank = int(re.sub(r"\D", "", card.find_element(
                By.XPATH, './/span[contains(@class,"zg-bdg-text")]').text))
        except (NoSuchElementException, ValueError, StaleElementReferenceException):
            continue

        # 제목
        try:
            title = card.find_element(
                By.XPATH, './/div[contains(@class,"_cDEzb_p13n-sc-css-line-clamp-2_EWgCb")]'
            ).text.strip()
        except NoSuchElementException:
            title = card.find_element(By.XPATH, './/img[@alt]').get_attribute("alt").strip()

        lg_match = bool(re.search(r"\bLG\b", title.replace("\u00a0", " "), re.I))

        # 가격 (문자열 그대로)
        price_raw = card.find_element(
            By.CSS_SELECTOR, 'span._cDEzb_p13n-sc-price_3mJ9Z'
        ).text.strip()

        if not price_raw:  # fallback: "3 offers from €123"
            try:
                offer_txt = card.find_element(By.CSS_SELECTOR, 'span.a-color-secondary').text.strip()
                m = re.search(r'€[\d\.,]+', offer_txt)
                if m:
                    price_raw = m.group(0)
            except NoSuchElementException:
                pass

        # ASIN + 링크
        try:
            a = card.find_element(By.XPATH, './/a[contains(@href,"/dp/")]')
            href = a.get_attribute("href")
            link = href.split("?", 1)[0] if href.startswith("http") else \
                   "https://www.amazon.de" + href.split("?", 1)[0]
            asin = re.search(r"/dp/([A-Z0-9]{10})", link).group(1)
        except Exception:
            continue

        if lg_match:
            parsed_items.append({
                "asin":  asin,
                "title": title,
                "url":   link,
                "price": price_raw,   # 문자열 그대로
                "rank":  rank,
            })

    return parsed_items

# ─── 메인 실행 ───────────────────────────────────────────────────────
driver = get_driver()
try:
    driver.get("https://www.amazon.de/")
    force_zip_ui(driver, ZIP_CODE)

    items: list[dict] = []
    for pg in (1, 2):
        items += fetch_cards_and_parse(pg, driver)
finally:
    driver.quit()

logging.info("LG 모니터 필터 후 %d개", len(items))

# ─── DataFrame & 시트 기록 (price는 문자열) ───────────────────────────
cols = ["asin", "title", "url", "price", "rank"]
df_today = pd.DataFrame(items, columns=cols)

if df_today.empty:
    logging.info("LG 모니터 없음 → 시트 업데이트 생략")
    sys.exit(0)

df_today = df_today.sort_values("rank").reset_index(drop=True)
kst = pytz.timezone("Asia/Seoul")
df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

# rank_delta 계산 (price_delta는 문자열이라 생략)
if "rank" in df_today.columns:
    df_today["rank_delta"] = "-"   # 필요 시 이전 데이터와 비교 로직 추가
else:
    df_today["rank_delta"] = "-"

df_today["price_delta"] = "-"      # 더 이상 숫자 계산 안 함

cols_out = ["asin", "title", "rank", "price", "url", "date",
            "rank_delta", "price_delta"]
df_today = df_today[cols_out].fillna("")

# ─── Google Sheet 기록 (원본 로직 거의 그대로) ───────────────────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(
    json.loads(base64.b64decode(os.environ["GCP_SA_BASE64"]).decode()),
    scopes=SCOPES,
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(os.environ["SHEET_ID"])

ws_hist = sh.worksheet("History") if "History" in [w.title for w in sh.worksheets()] \
          else sh.add_worksheet("History", rows=2000, cols=20)
ws_today = sh.worksheet("Today")  if "Today"  in [w.title for w in sh.worksheets()] \
          else sh.add_worksheet("Today", rows=100, cols=20)

if not ws_hist.get_all_values():
    ws_hist.append_row(cols_out, value_input_option="USER_ENTERED")
ws_hist.append_rows(df_today.values.tolist(), value_input_option="USER_ENTERED")

ws_today.clear()
ws_today.update([cols_out] + df_today.values.tolist(), value_input_option="USER_ENTERED")

# ▲/▼ 서식 (rank_delta만 적용, price_delta는 “-”)
RED, BLUE = Color(1,0,0), Color(0,0,1)
fmt_ranges = []
for i, row in df_today.iterrows():
    r = i + 2
    if isinstance(row["rank_delta"], str) and row["rank_delta"].startswith("▲"):
        fmt_ranges.append((f"G{r}", CellFormat(textFormat=TextFormat(bold=True, foregroundColor=RED))))
    elif isinstance(row["rank_delta"], str) and row["rank_delta"].startswith("▼"):
        fmt_ranges.append((f"G{r}", CellFormat(textFormat=TextFormat(bold=True, foregroundColor=BLUE))))
if fmt_ranges:
    format_cell_ranges(ws_today, fmt_ranges)

logging.info("Google Sheet 업데이트 완료 — LG 모니터 %d개", len(df_today))
print("✓ Google Sheet 업데이트 완료 — LG 모니터", len(df_today))
