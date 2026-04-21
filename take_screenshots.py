"""截取台股選股系統各頁面的螢幕截圖"""
from playwright.sync_api import sync_playwright
import os, time

OUTPUT_DIR = "/Users/roger/Documents/AI機器人/screenshots"
os.makedirs(OUTPUT_DIR, exist_ok=True)

BASE = "http://localhost:5000"

pages = [
    ("01_總表_上半部", "/index.html", {"full_page": False, "wait": 3}),
    ("02_總表_完整", "/index.html", {"full_page": True, "wait": 3}),
    ("03_個股詳細頁", "/company.html?code=2330", {"full_page": True, "wait": 4}),
    ("04_每日報告", "/daily.html", {"full_page": True, "wait": 3}),
    ("05_系統監控", "/health.html", {"full_page": True, "wait": 3}),
]

with sync_playwright() as p:
    browser = p.chromium.launch()
    context = browser.new_context(
        viewport={"width": 1440, "height": 900},
        locale="zh-TW",
    )
    page = context.new_page()

    for name, path, opts in pages:
        url = BASE + path
        print(f"截圖: {name} -> {url}")
        page.goto(url, wait_until="networkidle")
        time.sleep(opts.get("wait", 2))

        fp = os.path.join(OUTPUT_DIR, f"{name}.png")
        page.screenshot(path=fp, full_page=opts.get("full_page", True))
        print(f"  -> {fp}")

    browser.close()

print("\n截圖完成！")
