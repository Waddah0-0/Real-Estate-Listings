"""
Debug: Capture full property JSON to a file to find correct field names.
"""
import json, time, random
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

URL = "https://www.propertyfinder.eg/en/search?c=1&fu=0&ob=mr&page=1"
OUT = "debug_output.json"

stealth = Stealth(
    navigator_user_agent_override=(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
)

with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=False,
        args=["--disable-blink-features=AutomationControlled", "--start-maximized"]
    )
    ctx = browser.new_context(
        viewport={"width": 1440, "height": 900}, locale="en-US",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    page = ctx.new_page()
    stealth.apply_stealth_sync(page)

    page.goto("https://www.propertyfinder.eg/en", wait_until="load", timeout=45000)
    time.sleep(3)
    page.goto(URL, wait_until="load", timeout=45000)
    time.sleep(2)
    page.wait_for_function("() => !!document.getElementById('__NEXT_DATA__')", timeout=20000)
    raw = page.evaluate("() => document.getElementById('__NEXT_DATA__').textContent")
    data = json.loads(raw)

    sr = data["props"]["pageProps"]["searchResult"]
    
    # Save full first listing + pagination to JSON file
    output = {
        "searchResult_keys": list(sr.keys()),
        "pagination": sr.get("pagination") or sr.get("meta") or {},
        "first_listing": (sr.get("listings") or sr.get("properties") or [{}])[0],
    }
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"Saved to {OUT}")
    browser.close()
