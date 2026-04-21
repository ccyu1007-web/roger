"""
yahoo_fetcher.py — 用 Yahoo Finance 批次補齊個股財報資料
免費、無額度限制、不依賴 FinMind
"""
import requests
import sqlite3
import time
import random
from datetime import datetime

DB_PATH = "stocks.db"

def _get_yahoo_session():
    """建立帶 crumb 的 Yahoo Finance session"""
    s = requests.Session()
    s.headers.update({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'})
    s.get('https://fc.yahoo.com', timeout=10, allow_redirects=True)
    crumb = s.get('https://query2.finance.yahoo.com/v1/test/getcrumb', timeout=10).text
    return s, crumb


def fetch_yahoo_financials(session, crumb, code, market):
    """從 Yahoo Finance 抓取個股財報（年度+季度）"""
    suffix = '.TW' if market == '上市' else '.TWO'
    symbol = f'{code}{suffix}'

    modules = 'incomeStatementHistory,balanceSheetHistory,cashflowStatementHistory,incomeStatementHistoryQuarterly'
    url = f'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules={modules}&crumb={crumb}'

    try:
        r = session.get(url, timeout=15)
        if r.status_code != 200:
            return None
        return r.json().get('quoteSummary', {}).get('result', [{}])[0]
    except:
        return None


def _safe_raw(obj, key):
    """安全取得 Yahoo 格式的數值"""
    v = obj.get(key, {})
    if isinstance(v, dict):
        return v.get('raw')
    return None


def save_yahoo_to_db(code, data):
    """把 Yahoo Finance 資料存入 financial_annual + quarterly_financial"""
    if not data:
        return 0, 0

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    annual_saved = 0
    quarterly_saved = 0

    # ── 年度損益表 ──
    is_data = data.get('incomeStatementHistory', {}).get('incomeStatementHistory', [])
    bs_data = data.get('balanceSheetHistory', {}).get('balanceSheetHistory', [])
    cf_data = data.get('cashflowStatementHistory', {}).get('cashflowStatementHistory', [])

    # 建立 BS 和 CF 的 lookup（按年份）
    bs_by_year = {}
    for bs in bs_data:
        date = bs.get('endDate', {}).get('fmt', '')
        if date:
            yr = int(date[:4])
            bs_by_year[yr] = bs

    cf_by_year = {}
    for cf in cf_data:
        date = cf.get('endDate', {}).get('fmt', '')
        if date:
            yr = int(date[:4])
            cf_by_year[yr] = cf

    for stmt in is_data:
        date = stmt.get('endDate', {}).get('fmt', '')
        if not date:
            continue
        yr = int(date[:4])

        # 已有完整資料（有 net_income）就跳過
        c.execute("SELECT net_income FROM financial_annual WHERE code=? AND year=?", (code, yr))
        existing = c.fetchone()
        if existing and existing[0] is not None:
            continue

        revenue = _safe_raw(stmt, 'totalRevenue')
        cost = _safe_raw(stmt, 'costOfRevenue')
        gross_profit = _safe_raw(stmt, 'grossProfit')
        operating_income = _safe_raw(stmt, 'operatingIncome')
        pretax_income = _safe_raw(stmt, 'incomeBeforeTax')
        tax = _safe_raw(stmt, 'incomeTaxExpense')
        net_income = _safe_raw(stmt, 'netIncome')
        ebit = _safe_raw(stmt, 'ebit')

        # 從 BS 取
        bs = bs_by_year.get(yr, {})
        total_assets = _safe_raw(bs, 'totalAssets')
        total_equity = _safe_raw(bs, 'totalStockholderEquity')
        common_stock = _safe_raw(bs, 'commonStock')

        # 從 CF 取
        cf = cf_by_year.get(yr, {})
        operating_cf = _safe_raw(cf, 'totalCashFromOperatingActivities')
        capex = _safe_raw(cf, 'capitalExpenditures')

        try:
            c.execute("""INSERT INTO financial_annual
                (code, year, revenue, cost, gross_profit, operating_income,
                 pretax_income, tax, net_income, total_assets, total_equity,
                 common_stock, operating_cf, capex, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(code, year) DO UPDATE SET
                revenue=COALESCE(excluded.revenue, revenue),
                cost=COALESCE(excluded.cost, cost),
                gross_profit=COALESCE(excluded.gross_profit, gross_profit),
                operating_income=COALESCE(excluded.operating_income, operating_income),
                pretax_income=COALESCE(excluded.pretax_income, pretax_income),
                tax=COALESCE(excluded.tax, tax),
                net_income=COALESCE(excluded.net_income, net_income),
                total_assets=COALESCE(excluded.total_assets, total_assets),
                total_equity=COALESCE(excluded.total_equity, total_equity),
                common_stock=COALESCE(excluded.common_stock, common_stock),
                operating_cf=COALESCE(excluded.operating_cf, operating_cf),
                capex=COALESCE(excluded.capex, capex),
                updated_at=excluded.updated_at""",
                (code, yr, revenue, cost, gross_profit, operating_income,
                 pretax_income, tax, net_income, total_assets, total_equity,
                 common_stock, operating_cf, capex, now_str))
            annual_saved += 1
        except:
            pass

    # ── 季度損益表 ──
    q_data = data.get('incomeStatementHistoryQuarterly', {}).get('incomeStatementHistory', [])
    for stmt in q_data:
        date = stmt.get('endDate', {}).get('fmt', '')
        if not date:
            continue
        yr = int(date[:4])
        mon = int(date[5:7])
        roc_yr = yr - 1911
        q = (mon - 1) // 3 + 1
        quarter = f'{roc_yr}Q{q}'

        c.execute("SELECT code FROM quarterly_financial WHERE code=? AND quarter=?", (code, quarter))
        if c.fetchone():
            continue

        revenue = _safe_raw(stmt, 'totalRevenue')
        cost = _safe_raw(stmt, 'costOfRevenue')
        gross_profit = _safe_raw(stmt, 'grossProfit')
        operating_income = _safe_raw(stmt, 'operatingIncome')
        pretax_income = _safe_raw(stmt, 'incomeBeforeTax')
        tax = _safe_raw(stmt, 'incomeTaxExpense')
        net_income = _safe_raw(stmt, 'netIncome')
        eps = _safe_raw(stmt, 'dilutedEPS')

        try:
            c.execute("""INSERT OR IGNORE INTO quarterly_financial
                (code, quarter, revenue, cost, gross_profit, operating_income,
                 pretax_income, tax, net_income_parent, eps, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (code, quarter, revenue, cost, gross_profit, operating_income,
                 pretax_income, tax, net_income, eps, now_str))
            quarterly_saved += 1
        except:
            pass

    conn.commit()
    conn.close()
    return annual_saved, quarterly_saved


def backfill_all():
    """用 Yahoo Finance 批次補齊所有股票的財報"""
    t0 = time.time()

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 找需要補的
    need = []
    c.execute("SELECT code, name, market FROM stocks WHERE close IS NOT NULL ORDER BY code")
    for r in c.fetchall():
        code, name, market = r
        c.execute("SELECT COUNT(*) FROM financial_annual WHERE code=? AND net_income IS NOT NULL", (code,))
        fin = c.fetchone()[0]
        if fin >= 3:
            continue
        need.append((code, name, market))

    conn.close()

    if not need:
        print("[Yahoo] 所有股票財報已補齊")
        return

    print(f"[Yahoo] 待補: {len(need)} 支")

    # 建立 session
    session, crumb = _get_yahoo_session()
    print(f"[Yahoo] Session 建立完成")

    done = 0
    fail_streak = 0

    for code, name, market in need:
        data = fetch_yahoo_financials(session, crumb, code, market)
        if data:
            a, q = save_yahoo_to_db(code, data)
            if a > 0 or q > 0:
                done += 1
                fail_streak = 0
            else:
                fail_streak += 1
        else:
            fail_streak += 1

        if done % 100 == 0 and done > 0:
            print(f"  進度: {done}/{len(need)}")

        # crumb 過期時重新取
        if fail_streak >= 30:
            print(f"  重新建立 session...")
            try:
                session, crumb = _get_yahoo_session()
                fail_streak = 0
            except:
                print(f"  session 建立失敗，停止")
                break

        time.sleep(random.uniform(0.1, 0.3))

    elapsed = time.time() - t0
    print(f"[Yahoo] 完成 {done}/{len(need)} 支，耗時 {elapsed:.0f} 秒")


if __name__ == "__main__":
    backfill_all()
