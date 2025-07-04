# crawl.py  ──────────────────────────────────────────────────────────────
"""
Amazon.de 베스트셀러 ▸ Monitors 1~100위 (두 페이지, 스크롤 로딩 지원)
- LG 모니터만 수집
- Playwright로 스크롤 → JS 렌더링 완료 후 DOM 스냅샷
- 1페이지(1‥50위)를 완전히 수집해야 2페이지로 이동
- 가격(`span.a-offscreen` 등), 순위/가격 Δ 계산
- Google Sheet(History, Today) 기록
"""

import os, re, json, base64, datetime, pytz, pandas as pd
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

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
COOKIES = {"lc-main": "de_DE", "i18n-prefs": "EUR"}

SCROLL_PAUSE_MS = 800     # 스크롤 후 대기시간 (ms)
MAX_SCROLL_ITER = 20      # 안전장치: 스크롤 최대 시도 횟수

# ─────────────────────────────
# 1. Fetch helper (Playwright, 컨테이너 스크롤 완전판)
# ─────────────────────────────
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
import random, time

MIN_CARDS    = 50
SCROLL_WAIT  = 400       # ms
MAX_SCROLL   = 100
CARD_SEL     = 'div[data-index]'         # 모든 버전 공통
ROOT_SEL_JS  = 'document.querySelector("#zg-grid-view-root") || document.querySelector(\'div[data-testid="gridViewport"]\')'

def create_context(play):
    """Stealth Playwright context 생성"""
    browser = play.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--proxy-bypass-list=*",
            "--disable-dev-shm-usage",
            "--no-sandbox",
        ],
    )
    ctx = browser.new_context(
        locale="de-DE",
        user_agent=HEADERS["User-Agent"],
        extra_http_headers=HEADERS,
        viewport={"width": 1366, "height": 900},
    )
    # stealth: navigator.webdriver 제거
    ctx.add_init_script('Object.defineProperty(navigator, "webdriver", {get: () => undefined});')
    return browser, ctx

def is_captcha(html: str) -> bool:
    return (
        "Enter the characters you see below" in html
        or "not a robot" in html.lower()
        or "captcha" in html.lower()
    )

def fetch_cards(page_no: int, retries: int = 2):
    url = BASE_URL if page_no == 1 else f"{BASE_URL}?pg={page_no}&ref_=zg_bs_pg_{page_no}"

    for attempt in range(1, retries + 1):
        with sync_playwright() as p:
            browser, ctx = create_context(p)
            pg = ctx.new_page()

            try:
                pg.goto(url, wait_until="domcontentloaded", timeout=60_000)
            except PwTimeout:
                browser.close()
                if attempt == retries:
                    raise RuntimeError("Amazon 로드 타임아웃")
                continue

            # HTML 초벌 캡차 확인
            if is_captcha(pg.content()):
                browser.close()
                if attempt == retries:
                    raise RuntimeError("Amazon CAPTCHA 차단(초기)")
                time.sleep(random.uniform(2, 4))
                continue

            # 스크롤 컨테이너 확보
            try:
                pg.wait_for_selector(CARD_SEL, timeout=20_000)
            except PwTimeout:
                browser.close()
                if attempt == retries:
                    raise RuntimeError("카드 초기 로드 실패")
                continue

            last = 0
            stagnate = 0
            for _ in range(MAX_SCROLL):
                cards = pg.query_selector_all(CARD_SEL)
                if len(cards) >= MIN_CARDS:
                    break

                # 내부 컨테이너를 JS로 스크롤
                pg.evaluate(f'''
                    const root = {ROOT_SEL_JS};
                    if(root) root.scrollBy(0, root.clientHeight);
                ''')
                pg.wait_for_timeout(SCROLL_WAIT)

                cur = len(cards)
                stagnate = stagnate + 1 if cur == last else 0
                last = cur
                if stagnate >= 12:
                    break

            html = pg.content()
            browser.close()

        # CAPTCHA 재확인
        if is_captcha(html):
            if attempt == retries:
                raise RuntimeError("Amazon CAPTCHA 차단(재시도 후)")
            time.sleep(random.uniform(3, 6))
            continue

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        containers = soup.select(CARD_SEL)
        if len(containers) < MIN_CARDS:
            if attempt == retries:
                raise RuntimeError(f"{page_no}페이지 카드 수집 실패: {len(containers)} / 50")
            time.sleep(random.uniform(2, 4))
            continue

        return containers  # ✅ 성공

    # 모든 재시도 실패
    raise RuntimeError(f"{page_no}페이지 카드 확보 실패(재시도 {retries}회)")

# ─────────────────────────────
# 2. Parsing helpers
# ─────────────────────────────
def pick_title(card):
    selectors = [
        'span[class*="p13n-sc-css-line-clamp"]', '[title]',
        '.p13n-sc-truncate-desktop-type2', '.zg-text-center-align span.a-size-base'
    ]
    for sel in selectors:
        t = card.select_one(sel)
        if t:
            return (t.get("title") if sel == '[title]' else t.get_text(strip=True)) or ""
    img = card.select_one("img")
    return img.get("alt", "").strip() if img else ""

def pick_price(card):
    p = card.select_one('span.a-offscreen')
    if p and p.get_text(strip=True):
        return p.get_text(strip=True)
    p = card.select_one('span.p13n-sc-price')
    if p and p.get_text(strip=True):
        return p.get_text(strip=True)
    whole = card.select_one('span.a-price-whole')
    frac = card.select_one('span.a-price-fraction')
    if whole:
        txt = whole.get_text(strip=True).replace('.', '').replace(',', '.')
        if frac:
            txt += frac.get_text(strip=True)
        return txt
    return ""

def money_to_float(txt):
    clean = re.sub(r"[^0-9,\.]", "", txt).replace('.', '').replace(',', '.')
    try:
        return float(clean)
    except ValueError:
        return None

# ─────────────────────────────
# 3. 카드 수집 (절대 순위 계산 포함)
# ─────────────────────────────
all_cards = []
for pg in (1, 2):
    print(f"[INFO] 페이지 {pg} 수집 시작…")
    containers = fetch_cards(pg)
    all_cards.extend([(c, pg) for c in containers])
print(f"[INFO] 총 카드 수집 완료: {len(all_cards)}개")

items = []
for idx, (card, page_num) in enumerate(all_cards, start=1):
    rank_tag = card.select_one('.zg-badge-text')
    rank_on_page = int(rank_tag.get_text(strip=True).lstrip('#')) \
        if rank_tag else ((idx - 1) % 50 + 1)
    abs_rank = (page_num - 1) * 50 + rank_on_page

    a = card.select_one("a.a-link-normal[href*='/dp/']")
    if not a:
        continue
    title = pick_title(card) or a.get_text(" ", strip=True)
    if not re.search(r"\bLG\b", title, re.I):
        continue

    link = "https://www.amazon.de" + a["href"].split("?", 1)[0]
    asin_match = re.search(r"/dp/([A-Z0-9]{10})", link)
    asin = asin_match.group(1) if asin_match else None
    price_val = money_to_float(pick_price(card))

    items.append({
        "asin": asin,
        "title": title,
        "rank": abs_rank,
        "price": price_val,
        "url": link,
    })

if not items:
    raise RuntimeError("LG 모니터를 찾지 못했습니다.")

items.sort(key=lambda x: x["rank"])
df_today = pd.DataFrame(items)

# ─────────────────────────────
# 4. 타임스탬프 (KST) 및 Δ 계산
# ─────────────────────────────
kst = pytz.timezone("Asia/Seoul")
df_today["date"] = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")

# Google Sheets 설정
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.environ["SHEET_ID"]
sa_json = json.loads(base64.b64decode(os.environ["GCP_SA_BASE64"]).decode())
creds = Credentials.from_service_account_info(sa_json, scopes=SCOPES)
import gspread  # 지연 import (Playwright 충돌 방지)
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

if (not prev.empty) and set(["asin", "rank", "price", "date"]).issubset(prev.columns):
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

fmt = lambda v, p=False: "-" if (pd.isna(v) or v == 0) else (
    ("△" if v > 0 else "▽") + (f"{abs(v):.2f}" if p else str(abs(int(v))))
)
df_today["rank_delta"] = df_today["rank_delta_num"].apply(fmt)
df_today["price_delta"] = df_today["price_delta_num"].apply(lambda x: fmt(x, True))

cols = [
    "asin", "title", "rank", "price", "url",
    "date", "rank_delta", "price_delta"
]
df_today = df_today[cols].fillna("")

# ─────────────────────────────
# 5. Sheet 업데이트
# ─────────────────────────────
if not ws_hist.get_all_values():
    ws_hist.append_row(cols, value_input_option="RAW")
ws_hist.append_rows(df_today.values.tolist(), value_input_option="RAW")
ws_today.clear()
ws_today.update([cols] + df_today.values.tolist(), value_input_option="RAW")

print(f"✓ 업데이트 완료: LG 모니터 {len(df_today)}개")
