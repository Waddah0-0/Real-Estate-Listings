"""
PropertyFinder Egypt — Large-Scale Scraper (Crash-Resilient)
=============================================================
Scrapes Buy and Rent listings from propertyfinder.eg
for Kaggle dataset creation.

Features:
  - Auto-restarts browser on crash
  - Resumes from last completed page (progress.json)
  - Checkpoints every 50 pages

Usage:
  python scraper.py              # Full scrape (resumes if interrupted)
  python scraper.py --test       # Test: 5 pages only per category
  python scraper.py --reset      # Clear progress and start fresh
"""

import json
import time
import random
import argparse
import os
import re
from datetime import datetime
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
from playwright_stealth import Stealth

# ──────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────
BASE_URL    = "https://www.propertyfinder.eg/en/search"
SITE_ROOT   = "https://www.propertyfinder.eg"
OUTPUT_DIR  = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "propertyfinder_egypt.csv")
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "scraper_progress.json")
CHECKPOINT_INTERVAL = 50
MIN_DELAY   = 1.5
MAX_DELAY   = 3.5
MAX_RETRIES = 3          # Retries per page before skipping
BROWSER_RESTART_AFTER = 200  # Restart browser every N pages to prevent memory leaks

CATEGORIES = [
    {"name": "buy",  "c": 1},
    {"name": "rent", "c": 2},
]

# ──────────────────────────────────────────────────────────
# PROGRESS TRACKING
# ──────────────────────────────────────────────────────────

def load_progress() -> dict:
    """Load saved progress from disk."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"last_category": None, "last_page": 0, "total_rows": 0}


def save_progress(category: str, page_num: int, total_rows: int):
    """Save current progress to disk."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump({
            "last_category": category,
            "last_page": page_num,
            "total_rows": total_rows,
            "updated_at": datetime.utcnow().isoformat()
        }, f)


def clear_progress():
    """Clear progress file to start fresh."""
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        print("  Progress cleared — starting fresh.")


# ──────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────

def random_delay(min_s=None, max_s=None):
    time.sleep(random.uniform(min_s or MIN_DELAY, max_s or MAX_DELAY))


def safe_get(d, *keys, default=None):
    for key in keys:
        try:
            d = d[key]
        except (KeyError, TypeError, IndexError):
            return default
    return d


def parse_price(val):
    if val is None:
        return None
    try:
        return int(str(val).replace(",", "").replace(" ", "").split(".")[0])
    except (ValueError, AttributeError):
        return None


def clean_text(text: str) -> str:
    """Remove newlines/tabs and collapse extra spaces — keeps CSV single-row-per-listing."""
    if not text:
        return ""
    text = re.sub(r"[\r\n\t]+", " ", str(text))
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def get_contact(contacts: list, contact_type: str) -> str:
    for opt in (contacts or []):
        if opt.get("type") == contact_type:
            return opt.get("value", "")
    return ""


# ──────────────────────────────────────────────────────────
# BROWSER MANAGEMENT
# ──────────────────────────────────────────────────────────

def make_browser_and_page(p):
    """Create a fresh browser + stealth page."""
    stealth = Stealth(
        navigator_languages_override=["en-US", "en"],
        navigator_user_agent_override=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    )
    browser = p.chromium.launch(
        headless=False,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-infobars",
            "--start-maximized",
        ]
    )
    # Give the browser time to fully initialize before any interaction
    time.sleep(3)
    context = browser.new_context(
        viewport={"width": 1440, "height": 900},
        locale="en-US",
        timezone_id="Africa/Cairo",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    )
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: {} };
    """)
    page = context.new_page()
    stealth.apply_stealth_sync(page)
    return browser, page


def navigate(page, url: str):
    """Navigate and wait — all interactions wrapped in try/except."""
    page.goto(url, wait_until="load", timeout=45000)
    time.sleep(random.uniform(0.8, 1.5))
    try:
        page.keyboard.press("End")
        time.sleep(random.uniform(0.3, 0.6))
        page.keyboard.press("Home")
    except Exception:
        pass  # Non-fatal — scrolling is just a human-like hint
    time.sleep(random.uniform(0.5, 1.0))


def get_next_data(page, timeout=25000) -> dict:
    """Read __NEXT_DATA__ JSON from current page using JS polling."""
    page.wait_for_function(
        "() => !!document.getElementById('__NEXT_DATA__')",
        timeout=timeout
    )
    raw = page.evaluate("() => document.getElementById('__NEXT_DATA__').textContent")
    if not raw:
        raise ValueError("__NEXT_DATA__ is empty")
    return json.loads(raw)


# ──────────────────────────────────────────────────────────
# DATA PARSING
# ──────────────────────────────────────────────────────────

def parse_listing(listing_obj: dict, category_name: str) -> dict:
    prop = listing_obj.get("property") or {}

    loc      = prop.get("location") or {}
    coords   = loc.get("coordinates") or {}
    loc_tree = prop.get("location_tree") or []

    city        = next((l["name"] for l in loc_tree if l.get("level") == "0"), "")
    town        = next((l["name"] for l in loc_tree if l.get("level") == "1"), "")
    district    = next((l["name"] for l in loc_tree if l.get("level") == "2"), "")
    subdistrict = next((l["name"] for l in loc_tree if l.get("level") == "3"), "")

    price_info   = prop.get("price") or {}
    price_val    = parse_price(price_info.get("value"))
    price_period = price_info.get("period", "")

    size_info = prop.get("size") or {}
    area_val  = safe_get(size_info, "value")
    area_unit = safe_get(size_info, "unit") or "sqm"

    agent  = prop.get("agent")  or {}
    broker = prop.get("broker") or prop.get("client") or {}

    contacts = prop.get("contact_options") or []
    phone    = get_contact(contacts, "phone")
    whatsapp = get_contact(contacts, "whatsapp")
    email    = get_contact(contacts, "email")

    detail_url = prop.get("share_url") or (
        SITE_ROOT + prop.get("details_path", "") if prop.get("details_path") else ""
    )

    amenity_names = prop.get("amenity_names") or []
    payment_methods = prop.get("payment_method") or []

    return {
        "listing_id":           prop.get("listing_id") or prop.get("reference") or prop.get("id") or "",
        "internal_id":          prop.get("id") or "",
        "category":             category_name,
        "listing_type":         listing_obj.get("listing_type") or "",
        "detail_url":           detail_url,
        "property_type":        prop.get("property_type") or "",
        "offering_type":        prop.get("offering_type") or "",
        "completion_status":    prop.get("completion_status") or "",
        "title":                clean_text(prop.get("title") or ""),
        "price_egp":            price_val,
        "price_period":         price_period,
        "price_currency":       price_info.get("currency") or "EGP",
        "location_full":        loc.get("full_name") or "",
        "city":                 city,
        "town":                 town,
        "district":             district,
        "subdistrict":          subdistrict,
        "lat":                  coords.get("lat") or "",
        "lon":                  coords.get("lon") or "",
        "bedrooms":             prop.get("bedrooms") or "",
        "bathrooms":            prop.get("bathrooms") or "",
        "area_value":           area_val,
        "area_unit":            area_unit,
        "furnished":            prop.get("furnished") or "",
        "listing_level":        prop.get("listing_level") or "",
        "is_premium":           bool(prop.get("is_premium")),
        "is_verified":          bool(prop.get("is_verified")),
        "is_featured":          bool(prop.get("is_featured")),
        "is_new_construction":  bool(prop.get("is_new_construction")),
        "is_direct_from_dev":   bool(prop.get("is_direct_from_developer")),
        "is_exclusive":         bool(prop.get("is_exclusive")),
        "listed_date":          prop.get("listed_date") or "",
        "images_count":         prop.get("images_count") or 0,
        "has_view_360":         bool(prop.get("has_view_360")),
        "video_url":            prop.get("video_url") or "",
        "reference":            prop.get("reference") or "",
        "rera":                 prop.get("rera") or "",
        "description":          clean_text(prop.get("description") or ""),
        "amenities":            " | ".join(amenity_names),
        "payment_method":       " | ".join(payment_methods),
        "agent_id":             agent.get("id") or "",
        "agent_name":           agent.get("name") or "",
        "agent_email":          agent.get("email") or "",
        "agent_is_super":       bool(agent.get("is_super_agent")),
        "agent_languages":      " | ".join(agent.get("languages") or []),
        "broker_id":            broker.get("id") or "",
        "broker_name":          broker.get("name") or "",
        "broker_email":         broker.get("email") or "",
        "broker_phone":         broker.get("phone") or "",
        "contact_phone":        phone,
        "contact_whatsapp":     whatsapp,
        "contact_email":        email,
        "scraped_at":           datetime.utcnow().isoformat(),
    }


# ──────────────────────────────────────────────────────────
# PAGE SCRAPER
# ──────────────────────────────────────────────────────────

def get_total_pages(page, category_code: int, retries: int = 3) -> int:
    """Read total page count — retries up to 3 times, sanity-checks result >= 100."""
    url = f"{BASE_URL}?c={category_code}&fu=0&ob=mr&page=1"
    for attempt in range(1, retries + 1):
        try:
            navigate(page, url)
            data = get_next_data(page)
            meta = safe_get(data, "props", "pageProps", "searchResult", "meta") or {}
            total = int(meta.get("page_count") or meta.get("total_pages") or 0)
            if total >= 100:   # Sanity check — real categories have 1000s of pages
                return total
            print(f"  [WARN] Got suspicious page count ({total}), retrying ({attempt}/{retries})...")
            time.sleep(5)
        except Exception as e:
            print(f"  [WARN] get_total_pages attempt {attempt}/{retries}: {str(e)[:60]}")
            time.sleep(5)
    print("  [WARN] Could not read total pages, defaulting to 9999 (scrape until empty).")
    return 9999



def scrape_page_with_retry(page, category_name: str, category_code: int, page_num: int) -> list:
    """Scrape a single page with retry logic."""
    url = f"{BASE_URL}?c={category_code}&fu=0&ob=mr&page={page_num}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            navigate(page, url)
            data = get_next_data(page)
            listings = (
                safe_get(data, "props", "pageProps", "searchResult", "listings")
                or safe_get(data, "props", "pageProps", "searchResult", "properties")
                or []
            )
            results = []
            for obj in listings:
                try:
                    results.append(parse_listing(obj, category_name))
                except Exception as e:
                    print(f"    [WARN] Parse error: {e}")
            return results

        except (PlaywrightTimeoutError, PlaywrightError) as e:
            err_msg = str(e)[:80]
            if "closed" in err_msg.lower():
                raise  # Browser is dead — let the outer loop handle restart
            print(f"  [RETRY {attempt}/{MAX_RETRIES}] Page {page_num}: {err_msg}")
            if attempt < MAX_RETRIES:
                time.sleep(random.uniform(3, 6))
        except Exception as e:
            print(f"  [ERROR] Page {page_num}: {str(e)[:80]}")
            return []
    return []


# ──────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────

def run_scraper(test_mode: bool = False, reset: bool = False):
    max_pages = 5 if test_mode else 9999

    if reset:
        clear_progress()

    print("=" * 62)
    print("  PropertyFinder Egypt Scraper (Crash-Resilient)")
    print(f"  Mode    : {'TEST (5 pages/category)' if test_mode else 'FULL'}")
    print(f"  Output  : {OUTPUT_FILE}")
    print("=" * 62)

    # Load existing rows from last checkpoint if any
    progress = load_progress()
    all_rows = []

    # Load existing data from CSV if it exists
    if os.path.exists(OUTPUT_FILE) and not reset:
        try:
            existing_df = pd.read_csv(OUTPUT_FILE, encoding="utf-8-sig")
            all_rows = existing_df.to_dict("records")
            print(f"  Loaded {len(all_rows):,} existing rows from previous run.")
        except Exception:
            pass

    existing_ids = {r.get("listing_id") for r in all_rows if r.get("listing_id")}
    pages_scraped_this_session = 0

    with sync_playwright() as p:
        browser, page = make_browser_and_page(p)

        # Warm-up: visit homepage first (non-fatal if it fails)
        print("  Warming up — visiting homepage...")
        try:
            navigate(page, SITE_ROOT + "/en")
            time.sleep(random.uniform(2.0, 3.5))
            print("  Warm-up done.")
        except Exception as e:
            print(f"  [WARN] Warm-up failed ({str(e)[:60]}), continuing anyway...")

        for cat in CATEGORIES:
            cat_name = cat["name"]
            cat_code = cat["c"]

            # Determine resume point
            start_page = 1
            if (not reset
                    and progress.get("last_category") == cat_name
                    and progress.get("last_page", 0) > 0):
                start_page = progress["last_page"] + 1
                print(f"\n  Resuming {cat_name.upper()} from page {start_page}")
            elif (not reset
                    and progress.get("last_category") is not None
                    and CATEGORIES.index(cat) <
                    next((i for i, c in enumerate(CATEGORIES)
                          if c["name"] == progress["last_category"]), 0)):
                print(f"\n  Skipping {cat_name.upper()} (already completed)")
                continue

            print(f"\n{'─'*40}")
            print(f"  Category : {cat_name.upper()}")
            print(f"{'─'*40}")

            # Get total pages
            try:
                total_pages = min(get_total_pages(page, cat_code), max_pages)
            except Exception:
                total_pages = max_pages
            print(f"  Pages    : {total_pages:,}  (starting from {start_page})")
            random_delay()

            pg_num = start_page
            while pg_num <= total_pages:
                print(f"  [{cat_name}] {pg_num}/{total_pages} ...", end=" ", flush=True)

                try:
                    rows = scrape_page_with_retry(page, cat_name, cat_code, pg_num)

                    # Deduplicate on the fly
                    new_rows = [r for r in rows if r.get("listing_id") not in existing_ids]
                    for r in new_rows:
                        existing_ids.add(r.get("listing_id"))
                    all_rows.extend(new_rows)

                    print(f"{len(rows)} scraped ({len(new_rows)} new) | Total: {len(all_rows):,}")
                    save_progress(cat_name, pg_num, len(all_rows))
                    pages_scraped_this_session += 1
                    pg_num += 1
                    random_delay()

                    # Checkpoint save
                    if pages_scraped_this_session % CHECKPOINT_INTERVAL == 0:
                        df_ckpt = pd.DataFrame(all_rows)
                        df_ckpt.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
                        print(f"  [CHECKPOINT] {len(all_rows):,} rows saved → {OUTPUT_FILE}")

                    # Periodic browser restart to prevent memory leaks
                    if pages_scraped_this_session % BROWSER_RESTART_AFTER == 0:
                        print("  [RESTART] Restarting browser to free memory...")
                        try:
                            browser.close()
                        except Exception:
                            pass
                        time.sleep(2)
                        browser, page = make_browser_and_page(p)
                        navigate(page, SITE_ROOT + "/en")
                        time.sleep(random.uniform(2, 4))

                except (PlaywrightError, Exception) as e:
                    if "closed" in str(e).lower() or "target" in str(e).lower():
                        print(f"\n  [BROWSER CRASHED] Restarting browser...")
                        # Save what we have
                        pd.DataFrame(all_rows).to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
                        save_progress(cat_name, pg_num - 1, len(all_rows))
                        print(f"  [SAVED] {len(all_rows):,} rows before crash.")
                        try:
                            browser.close()
                        except Exception:
                            pass
                        time.sleep(3)
                        browser, page = make_browser_and_page(p)
                        navigate(page, SITE_ROOT + "/en")
                        time.sleep(random.uniform(2, 4))
                        # Retry same page
                    else:
                        print(f"  [ERROR] {str(e)[:80]}")
                        pg_num += 1

        try:
            browser.close()
        except Exception:
            pass

    # ── Save final CSV ────────────────────────────────────
    df = pd.DataFrame(all_rows)
    df.drop_duplicates(subset=["listing_id"], keep="first", inplace=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    clear_progress()  # Clear progress on successful completion

    print(f"\n{'='*62}")
    print(f"  ✅  Done! {len(df):,} unique properties saved.")
    print(f"  File: {OUTPUT_FILE}")
    print(f"  Cols: {len(df.columns)}")
    print(f"{'='*62}")
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PropertyFinder Egypt Scraper")
    parser.add_argument("--test",  action="store_true", help="Test mode: 5 pages per category")
    parser.add_argument("--reset", action="store_true", help="Clear progress and start fresh")
    args = parser.parse_args()
    run_scraper(test_mode=args.test, reset=args.reset)
