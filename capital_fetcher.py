"""
capital_fetcher.py — 從群益證券（嘉實系統）抓取季度損益表
免費、無額度限制、有完整歷史資料
"""
import requests
import sqlite3
import time
import random
import re
from datetime import datetime
from bs4 import BeautifulSoup

DB_PATH = "stocks.db"
BASE_URL = "https://stock.capital.com.tw/z/zc/zce/zce_{code}.djhtm"


def fetch_capital_financials(code):
    """從群益抓取個股季度損益表，存入 financial_annual + quarterly_financial"""
    try:
        s = requests.Session()
        s.headers.update({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'})
        r = s.get(BASE_URL.format(code=code), timeout=15)
        r.encoding = 'big5'
        soup = BeautifulSoup(r.text, 'html.parser')
    except:
        return 0, 0

    # 找有「季別」表頭的表格
    target_table = None
    for t in soup.find_all('table'):
        rows = t.find_all('tr')
        for row in rows[:3]:
            cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
            if '季別' in cells and '營業收入' in cells:
                target_table = t
                break
        if target_table:
            break

    if not target_table:
        return 0, 0

    rows = target_table.find_all('tr')
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    quarterly_saved = 0
    annual_data = {}  # {year: {revenue, cost, gross_profit, ...}}

    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
        if len(cells) < 10 or not re.match(r'\d+\.\d+Q', cells[0]):
            continue

        # 解析: 114.4Q, 699, 606, 92, 13.21%, 22, 3.21%, 28, 50, 45, 1.02
        q_label = cells[0]  # "114.4Q"
        m = re.match(r'(\d+)\.(\d+)Q', q_label)
        if not m:
            continue

        roc_year = int(m.group(1))
        quarter = int(m.group(2))
        west_year = roc_year + 1911
        quarter_label = f"{roc_year}Q{quarter}"

        def parse_num(s):
            s = s.replace(',', '').replace('%', '').strip()
            if s in ('', '-', '--'):
                return None
            try:
                return float(s)
            except:
                return None

        revenue = parse_num(cells[1])
        cost = parse_num(cells[2])
        gross_profit = parse_num(cells[3])
        operating_income = parse_num(cells[5])
        non_operating = parse_num(cells[7])
        pretax_income = parse_num(cells[8])
        net_income = parse_num(cells[9])
        eps = parse_num(cells[10]) if len(cells) > 10 else None

        # 單位是百萬，轉成千元（跟 FinMind 一致）
        mul = 1000000  # 百萬 → 元
        if revenue is not None: revenue *= mul
        if cost is not None: cost *= mul
        if gross_profit is not None: gross_profit *= mul
        if operating_income is not None: operating_income *= mul
        if non_operating is not None: non_operating *= mul
        if pretax_income is not None: pretax_income *= mul
        if net_income is not None: net_income *= mul

        # 存入 quarterly_financial
        c.execute("SELECT code FROM quarterly_financial WHERE code=? AND quarter=?", (code, quarter_label))
        if not c.fetchone():
            try:
                c.execute("""INSERT INTO quarterly_financial
                    (code, quarter, revenue, cost, gross_profit, operating_income,
                     non_operating, pretax_income, net_income_parent, eps, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (code, quarter_label, revenue, cost, gross_profit, operating_income,
                     non_operating, pretax_income, net_income, eps, now_str))
                quarterly_saved += 1
            except:
                pass

        # 累計到年度
        if west_year not in annual_data:
            annual_data[west_year] = {'revenue': 0, 'cost': 0, 'gross_profit': 0,
                                      'operating_income': 0, 'pretax_income': 0,
                                      'net_income': 0, 'eps': 0, 'quarters': 0}
        ad = annual_data[west_year]
        if revenue: ad['revenue'] += revenue
        if cost: ad['cost'] += cost
        if gross_profit: ad['gross_profit'] += gross_profit
        if operating_income: ad['operating_income'] += operating_income
        if pretax_income: ad['pretax_income'] += pretax_income
        if net_income: ad['net_income'] += net_income
        if eps: ad['eps'] += eps
        ad['quarters'] += 1

    # 寫入 financial_annual（只寫四季齊全的年度）
    annual_saved = 0
    for yr, ad in annual_data.items():
        if ad['quarters'] != 4:
            continue
        c.execute("SELECT net_income FROM financial_annual WHERE code=? AND year=?", (code, yr))
        existing = c.fetchone()
        if existing and existing[0] is not None:
            continue  # 已有完整資料不覆蓋

        try:
            c.execute("""INSERT INTO financial_annual
                (code, year, revenue, cost, gross_profit, operating_income,
                 pretax_income, net_income, eps, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(code, year) DO UPDATE SET
                revenue=COALESCE(excluded.revenue, revenue),
                cost=COALESCE(excluded.cost, cost),
                gross_profit=COALESCE(excluded.gross_profit, gross_profit),
                operating_income=COALESCE(excluded.operating_income, operating_income),
                pretax_income=COALESCE(excluded.pretax_income, pretax_income),
                net_income=COALESCE(excluded.net_income, net_income),
                eps=COALESCE(excluded.eps, eps),
                updated_at=excluded.updated_at""",
                (code, yr, ad['revenue'], ad['cost'], ad['gross_profit'],
                 ad['operating_income'], ad['pretax_income'], ad['net_income'],
                 ad['eps'], now_str))
            annual_saved += 1
        except:
            pass

    conn.commit()
    conn.close()
    return annual_saved, quarterly_saved


def backfill_all():
    """批次補齊所有缺資料的股票"""
    from datetime import timedelta
    cutoff = (datetime.now() - timedelta(days=3*365)).strftime('%Y%m%d')

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    need = []
    c.execute("SELECT code, name FROM stocks WHERE close IS NOT NULL AND listed_date IS NOT NULL AND listed_date <= ? ORDER BY code", (cutoff,))
    for r in c.fetchall():
        c.execute("SELECT COUNT(*) FROM financial_annual WHERE code=? AND net_income IS NOT NULL", (r[0],))
        if c.fetchone()[0] < 3:
            need.append((r[0], r[1]))
    conn.close()

    if not need:
        print("[群益] 所有股票已補齊")
        return

    print(f"[群益] 待補: {len(need)} 支（免費無限制）")

    done = 0
    fail = 0
    for code, name in need:
        a, q = fetch_capital_financials(code)
        if a > 0 or q > 0:
            done += 1
            fail = 0
        else:
            fail += 1

        if done % 50 == 0 and done > 0:
            print(f"  進度: {done}/{len(need)}")

        if fail >= 50:
            print(f"  連續失敗 {fail} 次，停止")
            break

        time.sleep(random.uniform(0.3, 0.8))

    print(f"[群益] 完成: {done}/{len(need)}")


if __name__ == "__main__":
    backfill_all()
