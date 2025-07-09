import sys, os, re, json, base64, datetime, time, logging, pytz
import pandas as pd, gspread
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from gspread_formatting import format_cell_ranges, CellFormat, TextFormat, Color

# ─── 0. 로깅 설정 ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("crawl_cards.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logging.info("🔍 LG 모니터 크롤러 시작")

# ────────────────────────── 1. Selenium 드라이버 준비 ──────────────────────────
def get_driver():
    service = None
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
    return webdriver.Chrome(service=service, options=opt)

# ────────────────────────── 2. 카드 크롤링 & 파싱 ──────────────────────────
def fetch_cards_and_parse(page: int, driver):
    parsed_items = []
    url = BASE_URL if page == 1 else f"{BASE_URL}?pg={page}"
    logging.info(f"▶️  페이지 {page} 크롤링 시작 – URL: {url}")
    driver.get(url)

    # 페이지 초기 로딩 대기
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((
                By.XPATH,
                "//div[starts-with(@id,'CardInstance')]/div[2]//ol/li[contains(@class,'zg-no-numbers')]"
            ))
        )
    except TimeoutException:
        logging.warning(f"    페이지 {page} 초반 로딩 타임아웃, 계속 진행합니다")

    # 스크롤하면서 추가 로딩 (최대 MAX_WAIT 초)
    SCROLL_PAUSE = 10
    MAX_WAIT = 60
    start = time.time()
    last_count = 0
    iteration = 0

    while True:
        iteration += 1
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)

        cards = driver.find_elements(
            By.XPATH,
            "//div[starts-with(@id,'CardInstance')]/div[2]//ol/li[contains(@class,'zg-no-numbers')]"
        )
        now = len(cards)
        elapsed = int(time.time() - start)
        logging.info(f"   [스크롤 {iteration}] 로딩된 카드: {now}개, 경과: {elapsed}초")

        if now == last_count or elapsed >= MAX_WAIT:
            break
        last_count = now

    total = last_count or now
    logging.info(f"✅ 페이지 {page} 카드 수집 완료: {total}개 (총 경과 {elapsed}초)")

    # 카드 파싱
    for idx, card in enumerate(cards, start=1):
        logging.info(f"  ▶ 카드 [{idx}] 파싱 시작")
        # 랭크
        try:
            rank_text = card.find_element(
                By.XPATH, './/span[contains(@class,"zg-bdg-text")]'
            ).text.strip()
            rank = int(re.sub(r"\D", "", rank_text))
            logging.info(f"    랭크: {rank_text} → {rank}")
        except (NoSuchElementException, ValueError, StaleElementReferenceException) as e:
            logging.warning(f"    [{idx}] 랭크 추출 실패: {e}")
            continue

        # 제목
        try:
            title = card.find_element(
                By.XPATH, './/div[contains(@class,"_cDEzb_p13n-sc-css-line-clamp-2_EWgCb")]'
            ).text.strip()
        except NoSuchElementException:
            title = card.find_element(By.XPATH, './/img[@alt]').get_attribute("alt").strip()
        logging.info(f"    제목: {title}")

        # LG 필터
        title_norm = title.replace("\u00a0", " ").replace("\u202f", " ")
        if not re.search(r"\bLG\b", title_norm, re.I):
            logging.info(f"    LG 모니터 아님 – 스킵: {title}")
            continue

        # 가격 (문자열 그대로)
        try:
            price = card.find_element(
                By.CSS_SELECTOR, 'span._cDEzb_p13n-sc-price_3mJ9Z'
            ).text.strip()
        except NoSuchElementException:
            price = ""
        logging.info(f"    가격: '{price}'")

        # 링크 & ASIN
        try:
            href = card.find_element(
                By.XPATH, './/a[contains(@href,"/dp/")]'
            ).get_attribute("href")
            link = href.split("?", 1)[0]
            asin = re.search(r"/dp/([A-Z0-9]{10})", link).group(1)
            logging.info(f"    링크: {link}, ASIN: {asin}")
        except Exception as e:
            logging.warning(f"    [{idx}] 링크/ASIN 추출 실패: {e}")
            continue

        parsed_items.append({
            "asin":  asin,
            "title": title,
            "url":   link,
            "price": price,
            "rank":  rank,
        })
        logging.info(f"  ✔ 카드 [{idx}] 파싱 성공 – ASIN: {asin}, 랭크: {rank}, 가격: {price}")

    return parsed_items

# ────────────────────────── 3. 크롤러 실행 & 로그인 ─────────────────────────
driver = get_driver()
try:
    # --- 1) 로그인 페이지 이동 ---
    driver.get("https://www.amazon.de/-/en/ap/signin?openid.pape.max_auth_age=0&openid.return_to=https%3A%2F%2Fwww.amazon.de%2Fref%3Dnav_signin&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.assoc_handle=deflex&openid.mode=checkid_setup&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0")
    wait = WebDriverWait(driver, 20)

    # --- 2) 아이디 입력 & 다음 ---
    amz_user = os.environ["AMZ_USER"]
    amz_pass = os.environ["AMZ_PASS"]
    wait.until(EC.presence_of_element_located((By.ID, "ap_email"))).send_keys(amz_user)
    wait.until(EC.element_to_be_clickable((By.ID, "continue"))).click()

    # --- 3) 비밀번호 입력 & 로그인 ---
    wait.until(EC.presence_of_element_located((By.ID, "ap_password"))).send_keys(amz_pass)
    wait.until(EC.element_to_be_clickable((By.ID, "signInSubmit"))).click()
    logging.info("🔐 로그인 완료 (%s)", amz_user)

    # 2) 페이지별 크롤링
    items = []
    for pg in (1, 2):
        try:
            items += fetch_cards_and_parse(pg, driver)
        except TimeoutException:
            logging.error(f"⛔ 페이지 {pg} 카드 로딩 타임아웃 발생")
finally:
    driver.quit()
    logging.info("🛑 WebDriver 종료")

logging.info(f"총 파싱된 LG 모니터 개수: {len(items)}개")

# ────────────────────────── 4. DataFrame 생성 및 후처리 ──────────────────────────
cols = ["asin", "title", "url", "price", "rank"]
df_today = pd.DataFrame(items, columns=cols)
logging.info(f"DataFrame 생성: {df_today.shape[0]}행, {df_today.shape[1]}열")

if df_today.empty:
    logging.info("LG 모니터 없음 → 시트 업데이트 생략")
    sys.exit(0)

# 정렬 및 날짜 추가
logging.info("DataFrame 정렬 및 날짜 컬럼 추가")
df_today = df_today.sort_values("rank").reset_index(drop=True)
kst = pytz.timezone("Asia/Seoul")
df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

# ────────────────────────── 5. Google Sheets 업데이트 ────────────────────────
logging.info("Google Sheets 인증 및 시트 선택")
creds = Credentials.from_service_account_info(
    json.loads(base64.b64decode(os.environ["GCP_SA_BASE64"]).decode()),
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(os.environ["SHEET_ID"])

ws_hist = (
    sh.worksheet("History") if "History" in [w.title for w in sh.worksheets()]
    else sh.add_worksheet("History", rows=2000, cols=20)
)
ws_today = (
    sh.worksheet("Today") if "Today" in [w.title for w in sh.worksheets()]
    else sh.add_worksheet("Today", rows=100, cols=20)
)

cols_out = ["asin", "title", "rank", "price", "url", "date"]
logging.info(f"시트에 기록할 컬럼: {cols_out}")

df_to_write = df_today[cols_out].fillna("")

logging.info("History 시트에 데이터 추가")
ws_hist.append_rows(df_to_write.values.tolist(), value_input_option="USER_ENTERED")
logging.info("Today 시트 초기화 및 데이터 쓰기")
ws_today.clear()
ws_today.update([cols_out] + df_to_write.values.tolist(), value_input_option="USER_ENTERED")

logging.info("Google Sheets 업데이트 완료")
print(f"✓ Google Sheet 업데이트 완료 — LG 모니터 {len(df_to_write)}개")
