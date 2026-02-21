#!/usr/bin/env python3
"""
Product Hunt -> LinkedIn scraper.

Collects LinkedIn URLs of product makers from PH leaderboards (2025-2026).
No browser required — pure HTTP GET + HTML/JSON parsing.

Usage:
    python ph_linkedin_scraper.py --year 2025 --year 2026 --out ph_linkedin.csv
    python ph_linkedin_scraper.py --smoke-test
"""
from __future__ import annotations

import argparse
import base64
import csv
import json
import logging
import os
import random
import re
import sqlite3
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import httpx
from tqdm import tqdm

# ── Constants ────────────────────────────────────────────────────────────────

BASE = "https://www.producthunt.com"
YEARLY_URL = BASE + "/leaderboard/yearly/{year}"
DAILY_URL = BASE + "/leaderboard/daily/{year}/{month}/{day}"
MAKERS_URL = BASE + "/products/{slug}/makers"
PROFILE_URL = BASE + "/@{username}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

DELAY_MIN = 3.0
DELAY_MAX = 7.0
MAX_RETRIES = 5
BACKOFF_BASE = 2.0

log = logging.getLogger("ph_scraper")

# ── HTTP helpers ─────────────────────────────────────────────────────────────


def _sleep() -> None:
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


def fetch(
    client: httpx.Client,
    url: str,
    *,
    debug_dir: Optional[Path] = None,
    label: str = "",
) -> Optional[str]:
    """GET with retries + exponential backoff on 429/5xx."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = client.get(url, follow_redirects=True, timeout=30.0)
            if r.status_code == 200:
                html = r.text
                if debug_dir is not None:
                    safe = re.sub(r"[^\w\-.]", "_", label or url)[:120]
                    (debug_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                return html
            if r.status_code in (429, 500, 502, 503, 504):
                wait = BACKOFF_BASE ** attempt + random.uniform(0, 2)
                log.warning("HTTP %s %s — retry %d/%d in %.1fs",
                            r.status_code, url, attempt, MAX_RETRIES, wait)
                time.sleep(wait)
                continue
            log.warning("HTTP %s %s — skip", r.status_code, url)
            return None
        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
            wait = BACKOFF_BASE ** attempt + random.uniform(0, 2)
            log.warning("%s %s — retry %d/%d in %.1fs",
                        type(e).__name__, url, attempt, MAX_RETRIES, wait)
            time.sleep(wait)
    log.error("All retries exhausted: %s", url)
    return None


# ── Parsing: product slugs ───────────────────────────────────────────────────


def extract_product_slugs_from_leaderboard(html: str) -> set[str]:
    """Extract product slugs from a leaderboard page HTML."""
    slugs: set[str] = set()
    SKIP = {"new", "upcoming", "topics", "stories", "newsletter", "leaderboard",
            "products", "posts", "about", "terms", "privacy", "api"}

    # href="/posts/{slug}" and href="/products/{slug}"
    for pat in (
        r'href="(?:https://www\.producthunt\.com)?/posts/([a-z0-9][a-z0-9\-]*)"',
        r'href="(?:https://www\.producthunt\.com)?/products/([a-z0-9][a-z0-9\-]*?)(?:/[^"]*)?"',
    ):
        for m in re.finditer(pat, html, re.I):
            s = m.group(1).lower().rstrip("/")
            if s not in SKIP and len(s) > 1:
                slugs.add(s)

    # "slug":"value" anywhere in HTML (covers __NEXT_DATA__, RSC payloads, etc.)
    for m in re.finditer(r'"slug"\s*:\s*"([a-z0-9][a-z0-9\-]+)"', html):
        s = m.group(1)
        if s not in SKIP and len(s) > 1:
            slugs.add(s)

    return slugs


# ── Parsing: maker usernames ────────────────────────────────────────────────


def extract_usernames_from_makers_page(html: str) -> set[str]:
    """Extract maker usernames from /products/{slug}/makers page."""
    users: set[str] = set()

    # href="/@username"
    for m in re.finditer(r'href="/@([A-Za-z0-9_]+)"', html):
        users.add(m.group(1))

    # "username":"value" in JSON/RSC
    for m in re.finditer(r'"username"\s*:\s*"([A-Za-z0-9_]+)"', html):
        users.add(m.group(1))

    return users


# ── Parsing: LinkedIn URL from user profile ──────────────────────────────────
#
# KEY LOGIC: LinkedIn is NOT stored as a plain href.  It lives in a JSON-like
# structure inside the page (often in <script> / RSC payload):
#
#   { "kind": "linkedin", "encodedUrl": "aHR0cHM6Ly93d3cu\nbGlua2Vk..." }
#
# encodedUrl is **base64** and may contain embedded newlines/whitespace.
# Steps: strip whitespace → base64 decode → UTF-8 string → LinkedIn URL.
# ─────────────────────────────────────────────────────────────────────────────


def _b64decode(encoded: str) -> Optional[str]:
    """Base64-decode an encodedUrl value (strip whitespace first)."""
    try:
        cleaned = re.sub(r"\s+", "", encoded)
        # add padding if missing
        missing = len(cleaned) % 4
        if missing:
            cleaned += "=" * (4 - missing)
        return base64.b64decode(cleaned).decode("utf-8").strip()
    except Exception:
        log.debug("b64 decode failed: %.60r", encoded)
        return None


def extract_linkedin_from_user_profile(html: str) -> Optional[str]:
    """
    Extract LinkedIn URL from a PH user profile page.

    Searches for JSON fragments with kind="linkedin" + encodedUrl,
    then base64-decodes the URL.
    """
    # Pattern A: "kind":"linkedin" ... "encodedUrl":"..."
    pat_a = (
        r'"kind"\s*:\s*"linkedin"'
        r'[^}]{0,600}'
        r'"encodedUrl"\s*:\s*"([^"]*(?:\\.[^"]*)*)"'
    )
    # Pattern B: reversed key order
    pat_b = (
        r'"encodedUrl"\s*:\s*"([^"]*(?:\\.[^"]*)*)"'
        r'[^}]{0,600}'
        r'"kind"\s*:\s*"linkedin"'
    )

    for pat in (pat_a, pat_b):
        m = re.search(pat, html, re.DOTALL | re.I)
        if m:
            raw = m.group(1)
            # unescape JSON escapes (\n, \\n, etc.)
            try:
                raw = json.loads('"' + raw + '"')
            except (json.JSONDecodeError, ValueError):
                pass
            url = _b64decode(raw)
            if url and "linkedin.com" in url.lower():
                return url

    return None


# ── SQLite checkpoint ────────────────────────────────────────────────────────


def _init_db(path: str) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS done_urls (url TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS products (
            slug TEXT NOT NULL, year INTEGER NOT NULL,
            PRIMARY KEY (slug, year));
        CREATE TABLE IF NOT EXISTS makers (
            username TEXT NOT NULL, product_slug TEXT NOT NULL, year INTEGER NOT NULL,
            PRIMARY KEY (username, product_slug));
        CREATE TABLE IF NOT EXISTS linkedin (
            username TEXT PRIMARY KEY, url TEXT NOT NULL DEFAULT '');
    """)
    db.commit()
    return db


def _url_done(db: sqlite3.Connection, url: str) -> bool:
    return db.execute("SELECT 1 FROM done_urls WHERE url=?", (url,)).fetchone() is not None


def _mark_done(db: sqlite3.Connection, url: str) -> None:
    db.execute("INSERT OR IGNORE INTO done_urls(url) VALUES(?)", (url,))
    db.commit()


def _save_product(db: sqlite3.Connection, slug: str, year: int) -> None:
    db.execute("INSERT OR IGNORE INTO products VALUES(?,?)", (slug, year))
    db.commit()


def _get_products(db: sqlite3.Connection, year: int, limit: Optional[int] = None) -> list[tuple[str, int]]:
    q = "SELECT slug, year FROM products WHERE year=?"
    params: list = [year]
    if limit:
        q += " LIMIT ?"
        params.append(limit)
    return db.execute(q, params).fetchall()


def _count_products(db: sqlite3.Connection, year: int) -> int:
    return db.execute("SELECT count(*) FROM products WHERE year=?", (year,)).fetchone()[0]


def _save_maker(db: sqlite3.Connection, username: str, slug: str, year: int) -> None:
    db.execute("INSERT OR IGNORE INTO makers VALUES(?,?,?)", (username, slug, year))
    db.commit()


def _get_makers_for(db: sqlite3.Connection, slug: str) -> list[str]:
    return [r[0] for r in db.execute("SELECT username FROM makers WHERE product_slug=?", (slug,)).fetchall()]


def _save_linkedin(db: sqlite3.Connection, username: str, url: str) -> None:
    db.execute("INSERT OR REPLACE INTO linkedin VALUES(?,?)", (username, url))
    db.commit()


def _get_linkedin(db: sqlite3.Connection, username: str) -> Optional[str]:
    r = db.execute("SELECT url FROM linkedin WHERE username=?", (username,)).fetchone()
    return r[0] if r else None


# ── Pipeline steps ───────────────────────────────────────────────────────────


def _dates_for_year(year: int) -> list[date]:
    start = date(year, 1, 1)
    end = min(date(year, 12, 31), date.today())
    out = []
    d = start
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    return out


def step1_products(
    client: httpx.Client, years: list[int], db: sqlite3.Connection,
    *, limit: Optional[int] = None, debug_dir: Optional[Path] = None,
) -> list[tuple[str, int]]:
    """Collect product slugs via yearly + daily leaderboards."""
    result: list[tuple[str, int]] = []

    for year in years:
        # yearly page
        yurl = YEARLY_URL.format(year=year)
        if not _url_done(db, yurl):
            log.info("Fetching yearly leaderboard %d", year)
            html = fetch(client, yurl, debug_dir=debug_dir, label=f"yearly_{year}")
            _sleep()
            if html:
                for s in extract_product_slugs_from_leaderboard(html):
                    _save_product(db, s, year)
                _mark_done(db, yurl)
                log.info("Year %d yearly page: %d slugs", year, _count_products(db, year))

        # daily pages for completeness
        days = _dates_for_year(year)
        for d in tqdm(days, desc=f"Daily {year}", unit="day", disable=None):
            if limit and _count_products(db, year) >= limit:
                break
            durl = DAILY_URL.format(year=d.year, month=d.month, day=d.day)
            if _url_done(db, durl):
                continue
            html = fetch(client, durl, debug_dir=debug_dir, label=f"daily_{d.isoformat()}")
            _sleep()
            if html:
                for s in extract_product_slugs_from_leaderboard(html):
                    _save_product(db, s, year)
                _mark_done(db, durl)

        slugs = _get_products(db, year, limit)
        result.extend(slugs)
        log.info("Year %d total products: %d", year, len(slugs))

    return result


def step2_makers(
    client: httpx.Client, products: list[tuple[str, int]], db: sqlite3.Connection,
    *, limit_per_product: Optional[int] = None, debug_dir: Optional[Path] = None,
) -> list[tuple[str, str, int]]:
    """Fetch maker usernames for each product."""
    result: list[tuple[str, str, int]] = []

    for slug, year in tqdm(products, desc="Makers", unit="prod", disable=None):
        url = MAKERS_URL.format(slug=slug)
        if _url_done(db, url):
            for u in _get_makers_for(db, slug):
                result.append((u, slug, year))
            continue

        html = fetch(client, url, debug_dir=debug_dir, label=f"makers_{slug}")
        _sleep()
        if not html:
            _mark_done(db, url)
            continue

        users = extract_usernames_from_makers_page(html)
        if limit_per_product:
            users = set(list(users)[:limit_per_product])

        for u in users:
            _save_maker(db, u, slug, year)
            result.append((u, slug, year))
        _mark_done(db, url)

    log.info("Total maker entries: %d", len(result))
    return result


def step3_linkedin(
    client: httpx.Client, makers: list[tuple[str, str, int]], db: sqlite3.Connection,
    *, debug_dir: Optional[Path] = None,
) -> list[dict]:
    """Fetch LinkedIn URLs from user profiles."""
    results: list[dict] = []
    seen: set[str] = set()

    for username, slug, year in tqdm(makers, desc="Profiles", unit="user", disable=None):
        if username in seen:
            continue
        seen.add(username)

        cached = _get_linkedin(db, username)
        if cached is not None:
            if cached:
                results.append({
                    "linkedin_url": cached, "username": username,
                    "product_slug": slug, "year": year,
                    "source_url": PROFILE_URL.format(username=username),
                })
            continue

        url = PROFILE_URL.format(username=username)
        html = fetch(client, url, debug_dir=debug_dir, label=f"profile_{username}")
        _sleep()

        if not html:
            _save_linkedin(db, username, "")
            continue

        li = extract_linkedin_from_user_profile(html)
        _save_linkedin(db, username, li or "")
        if li:
            results.append({
                "linkedin_url": li, "username": username,
                "product_slug": slug, "year": year,
                "source_url": url,
            })

    log.info("LinkedIn found: %d / %d unique users", len(results), len(seen))
    return results


# ── CSV output ───────────────────────────────────────────────────────────────


def step4_csv(records: list[dict], out: str, db: sqlite3.Connection) -> None:
    """Write deduplicated CSV (merges DB + current run)."""
    merged: dict[str, dict] = {}

    # pull everything from DB
    for li_url, username, slug, year in db.execute("""
        SELECT l.url, l.username, m.product_slug, m.year
        FROM linkedin l JOIN makers m ON l.username = m.username
        WHERE l.url != ''
    """).fetchall():
        if li_url not in merged:
            merged[li_url] = {
                "linkedin_url": li_url, "username": username,
                "product_slug": slug, "year": year,
                "source_url": PROFILE_URL.format(username=username),
            }

    # overlay current run
    for rec in records:
        merged[rec["linkedin_url"]] = rec

    cols = ["linkedin_url", "username", "product_slug", "year", "source_url"]
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for rec in sorted(merged.values(), key=lambda r: r["linkedin_url"]):
            w.writerow(rec)

    log.info("CSV: %d unique LinkedIn URLs -> %s", len(merged), out)


# ── CLI ──────────────────────────────────────────────────────────────────────


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PH -> LinkedIn scraper")
    p.add_argument("--year", type=int, action="append", default=None,
                   help="Year(s) to scrape (repeatable). Default: 2025 2026")
    p.add_argument("--out", default="ph_linkedin.csv", help="Output CSV path")
    p.add_argument("--limit-products", type=int, default=None,
                   help="Max products per year (testing)")
    p.add_argument("--limit-makers-per-product", type=int, default=None,
                   help="Max makers per product (testing)")
    p.add_argument("--db", default="ph_scraper_progress.db",
                   help="SQLite checkpoint file")
    p.add_argument("--debug", action="store_true",
                   help="Save raw HTML to debug_samples/")
    p.add_argument("--smoke-test", action="store_true",
                   help="Quick test: 1 product, 5 makers")
    p.add_argument("-v", "--verbose", action="store_true", help="DEBUG logging")
    args = p.parse_args(argv)
    if args.year is None:
        args.year = [2025, 2026]
    return args


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S",
    )

    if args.smoke_test:
        log.info("=== SMOKE TEST ===")
        args.limit_products = 1
        args.limit_makers_per_product = 5
        args.out = "smoke_test_output.csv"

    debug_dir: Optional[Path] = None
    if args.debug:
        debug_dir = Path("debug_samples")
        debug_dir.mkdir(exist_ok=True)

    db = _init_db(args.db)
    client = httpx.Client(headers=HEADERS)

    try:
        log.info("STEP 1: Collecting products for %s", args.year)
        products = step1_products(client, args.year, db,
                                  limit=args.limit_products, debug_dir=debug_dir)
        log.info("Products: %d", len(products))
        if not products:
            log.warning("No products found — exiting")
            return

        log.info("STEP 2: Collecting makers")
        makers = step2_makers(client, products, db,
                              limit_per_product=args.limit_makers_per_product,
                              debug_dir=debug_dir)
        if not makers:
            log.warning("No makers found — exiting")
            return

        log.info("STEP 3: Fetching LinkedIn URLs")
        linkedin = step3_linkedin(client, makers, db, debug_dir=debug_dir)

        log.info("STEP 4: Writing CSV")
        step4_csv(linkedin, args.out, db)

        log.info("===== DONE =====")
        log.info("  Products : %d", len(products))
        log.info("  Makers   : %d", len(makers))
        log.info("  LinkedIn : %d", len(linkedin))
        log.info("  Output   : %s", args.out)
    finally:
        client.close()
        db.close()


# ── TODO: Extend to commenters ──────────────────────────────────────────────
#
# To also scrape commenters' LinkedIn URLs:
#
# 1. For each product, fetch the post page (BASE + "/posts/{slug}").
# 2. Comments may be in __NEXT_DATA__ JSON or loaded via PH GraphQL API:
#       POST https://www.producthunt.com/frontend/graphql
#       query { post(slug: "...") { comments { edges { node { user { username } } } } } }
# 3. Extract commenter usernames -> feed into step3 (profile -> LinkedIn).
# 4. Add --include-commenters flag and a "source_type" column (maker/commenter).
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    main()
