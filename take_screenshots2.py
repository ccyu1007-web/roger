"""補截個股頁上半部 + 不同 tab"""
from playwright.sync_api import sync_playwright
import os, time

OUTPUT_DIR = "/Users/roger/Documents/AI機器人/screenshots"
BASE = "http://localhost:5000"

with sync_playwright() as p:
    browser = p.chromium.launch()
    context = browser.new_context(
        viewport={"width": 1440, "height": 900},
        locale="zh-TW",
    )
    page = context.new_page()

    # 個股頁上半部（不 full_page）
    page.goto(f"{BASE}/company.html?code=2330", wait_until="networkidle")
    time.sleep(4)
    page.screenshot(path=os.path.join(OUTPUT_DIR, "03a_個股頁_上半部.png"), full_page=False)
    print("03a done")

    # 個股頁 - 季度估計表 tab
    tabs = page.query_selector_all(".tab-btn, [onclick], button")
    for tab in tabs:
        text = tab.inner_text().strip()
        if "季度" in text:
            tab.click()
            time.sleep(2)
            page.screenshot(path=os.path.join(OUTPUT_DIR, "03b_個股頁_季度估計表.png"), full_page=True)
            print("03b done")
            break

    # 個股頁 - 月營收 tab
    tabs = page.query_selector_all(".tab-btn, [onclick], button")
    for tab in tabs:
        text = tab.inner_text().strip()
        if "月營收" in text:
            tab.click()
            time.sleep(2)
            page.screenshot(path=os.path.join(OUTPUT_DIR, "03c_個股頁_月營收.png"), full_page=True)
            print("03c done")
            break

    # 總表 - 篩選設定展示（點擊篩選設定按鈕）
    page.goto(f"{BASE}/index.html", wait_until="networkidle")
    time.sleep(3)
    btns = page.query_selector_all("button")
    for btn in btns:
        text = btn.inner_text().strip()
        if "篩選設定" in text:
            btn.click()
            time.sleep(1)
            page.screenshot(path=os.path.join(OUTPUT_DIR, "01b_總表_篩選設定.png"), full_page=False)
            print("01b done")
            break

    browser.close()
    print("補截完成！")
