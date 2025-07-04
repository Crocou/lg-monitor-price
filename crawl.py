#!/usr/bin/env python3
"""
Amazon.de Monitors Bestseller Scraper (TRACE + Scroll/Page aware)
================================================================
• 각 페이지(pg=1,2, …):
  ① 기본 HTML (= 1~30위)
  ② 스크롤 ajax 20개 (?pg=N&ajax=1 = 31~50위)
  → 두 응답 병합 후 중복 제거, 최대 50위 확보
• 51~100위는 pg=2,3 … 동일 규칙 적용 → 누적 100개 수집하면 종료
• 단계별 상세 TRACE 로그 (요청, 응답 길이, 컨테이너 수, 카드 요약)
• 요청 실패 대비 Session+Retry, polite delay(1 s)
• LG 모니터만 필터 → Google Sheet(History append, Today replace)

필수 ENV
---------
GCP_SA_BASE64   ‑ base64 인코딩된 GCP Service Account JSON
SHEET_ID        ‑ 대상 Google Sheet ID

선택 ENV
--------
LOG_LEVEL       ‑ DEBUG/INFO (default INFO)
SCRAPER_UA      ‑ User‑Agent override
SCRAPER_TZ      ‑ Asia/Seoul 등 IANA TZ (default Asia/Seoul)
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import logging
import os
import re
import textwrap
import time
from typing import List, Tuple

import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
import gspread
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ───────── Logging ─────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger(__name__)
BAR = "—" * 80

# ───────── Constants & ENV ─────────
BASE_URL = "https://www.amazon.de/gp/bestsellers/computers/429868031/"
HEADERS = {
    "User-Agent": os.getenv(
        "SCRAPER_UA",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/126.0",
    ),
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
}
COOKIES = {"lc-main": "de_DE", "i18n-prefs": "EUR"}
TIMEZONE = pytz.timezone(os.getenv("SCRAPER_TZ", "Asia/Seoul"))

CSS_SELECTORS = [
    "div.zg-grid-general-faceout",
    "div.p13n-sc-uncoverable-faceout",
    "div.a-section.a-spacing-none.p13n-asin",
    "ol#zg-ordered-list > li",
]

# ───────── Helpers ─────────

def money(text: str) -> float | None:
    if not text:
        return None
    num = re.sub(r"[^0-9,\.]", "", text).replace(".", "").replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return None


def parse_cards(html: str) -> List[BeautifulSoup]:
    soup = BeautifulSoup(html, "lxml")
    for sel in CSS_SELECTORS:
        found = soup.select(sel)
        if found:
            return found
    return []


# ───────── Network ─────────

def init_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(HEADERS)
    return s


def fetch(url: str, session: requests.Session) -> str:
    logger.debug("GET %s", url)
    r = session.get(url, cookies=COOKIES, timeout=30)
    logger.debug("Status %s, length %d", r.status_code, len(r.text))
    if r.status_code != 200:
        logger.warning("Non‑200 response: %s", url)
    return r.text


# ───────── Scraping ─────────

def fetch_page_cards(page: int, session: requests.Session) -> List[Tuple[BeautifulSoup, int]]:
    """Return list[(card, page_idx)] for given pg."""
    cards: List[Tuple[BeautifulSoup, int]] = []
    urls = [BASE_URL if page == 1 else f"{BASE_URL}?pg={page}", f"{BASE_URL}?pg={page}&ajax=1"]
    for label, url in zip(["MAIN", "AJAX"], urls):
        print(f"\n{BAR}\n[REQUEST] {label} {url}")
        html = fetch(url, session)
        print(
            f"[RESPONSE] len={len(html)} | sample="
            f"{textwrap.shorten(html.replace('\n', ' '), width=100, placeholder='…')}\n{BAR}"
        )
        part_cards = parse_cards(html)
        print(f"[PARSE] {label} containers={len(part_cards)}")
        for i, c in enumerate(part_cards[:5], 1):
            title = c.get_text(" ", strip=True)[:80]
            print(f"   · {title}")
        cards.extend([(c, page) for c in part_cards])
    return cards


def scrape() -> pd.DataFrame:
    session = init_session()
    all_cards: List[Tuple[BeautifulSoup, int]] = []
    page = 1
    while len(all_cards) < 100:
        page_cards = fetch_page_cards(page, session)
        if not page_cards:
            logger.warning("No cards on page %d – stopping", page)
            break
        # dedup using data-asin or card html
        seen: set[str] = set()
        for card, pg in page_cards:
            asin_attr = card.get("data-asin") or ""  # some containers have data-asin
            key = asin_attr or card.encode()[:80]  # fallback hash snippet
            if key in seen:
                continue
            seen.add(key)
            all_cards.append((card, pg))
        logger.info("Accumulated cards: %d", len(all_cards))
        if len(all_cards) >= 100:
            break
        page += 1
        time.sleep(1)

    print(f"\n[TRACE] total containers={len(all_cards)}")
    # ---------------- Parse items ----------------
    items = []
    for idx, (card, pg_idx) in enumerate(all_cards, 1):
        badge = card.select_one(".zg-badge-text")
        rank_pg = (
            int(badge.get_text(strip=True).lstrip("#")) if badge else ((idx - 1) % 50 + 1)
        )
        abs_rank = (pg_idx - 1) * 50 + rank_pg
        a = card.select_one("a.a-link-normal[href*='/dp/']")
        if not a:
            continue
        url = "https://www.amazon.de" + a["href"].split("?", 1)[0]
        asin_m = re.search(r"/dp/([A-Z0-9]{10})", url)
        asin = asin_m.group(1) if asin_m else None
        # title
        title_tag = card.select_one("[title]")
        title = (
            title_tag["title"].strip() if title_tag else a.get_text(" ", strip=True)
        )
        price_tag = card.select_one("span.a-offscreen") or card.select_one(
            "span.p13n-sc-price"
        )
        price = money(price_tag.get_text(strip=True)) if price_tag else None
        items.append({
            "asin": asin,
            "title": title,
            "rank": abs_rank,
            "price": price,
            "url": url,
        })

    print(f"[TRACE] items scraped={len(items)} (showing 10)")
    for itm in items[:10]:
        print(f" #{itm['rank']:>3} | {itm['title'][:60]} | {itm['price']}")

    df = pd.DataFrame(items)
    df = df.drop_duplicates("asin").sort_values("rank")
    df_lg = df[df["title"].str.upper().str.contains("LG")].copy()
    if df_lg.empty:
        raise RuntimeError("LG 모니터 없음")

    print("\n[LG LIST]")
    for r in df_lg.itertuples():
        print(f" #{r.rank:>3} | {r.asin} | {r.title[:60]} | {r.price}")

    df_lg["date"] = dt.datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    return df_lg


# ───────── Google Sheets Upload ─────────

def upload_to_sheets(df: pd.DataFrame) -> None:
    required = ["GCP_SA_BASE64", "SHEET_ID"]
    miss = [v for v in required if v not in os.environ]
    if miss:
        raise EnvironmentError(f"Missing env vars: {', '.join(miss)}")

    creds_info = json.loads(base64.b64decode(os.environ["GCP_SA_BASE64"]).decode())
    creds = Credentials.from_service_account_info(
        creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    sh = gspread.authorize(creds).open_by_key(os.environ["SHEET_ID"])
    ws_dict = {w.title: w for w in sh.worksheets()}
    ws_hist = ws_dict.get("History") or sh.add_worksheet("History", 2000, 20)
    ws_today = ws_dict.get("Today") or sh.add_worksheet("Today", 100, 20)

    cols = ["asin", "title", "rank", "price", "url", "date"]
    df = df[cols].fillna("")

    if not ws_hist.get_all_values():
        ws_hist.append_row(cols, value_input_option="RAW")
    ws_hist.append_rows(df.values.tolist(), value_input_option="RAW")

    ws_today.clear()
    ws_today.update([cols] + df.values.tolist(), value_input_option="RAW")

    logger.info("Sheets updated: %d LG rows", len(df))


# ───────── Main ─────────

def main() -> None:
    logger.info("BEGIN scrape")
    df = scrape()
    upload_to_sheets(df)
    logger.info("✓ FINISHED — LG rows: %d", len(df))


if __name__ == "__main__":
    main()
