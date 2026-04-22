"""
backfill.py — 批次補齊個股詳細資料
三輪策略：Yahoo Finance → 政府 API → FinMind
每天凌晨 4:00 自動跑，跑完的不會重跑。
"""
import db as sqlite3
import time
import random
from datetime import datetime

DB_PATH = "stocks.db"


def _find_need():
    """找出需要補資料的股票（排除上市未滿 3 年的）"""
    from datetime import timedelta
    cutoff = (datetime.now() - timedelta(days=3*365)).strftime('%Y%m%d')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    need = []
    c.execute("SELECT code, name, market, listed_date FROM stocks WHERE close IS NOT NULL ORDER BY code")
    for r in c.fetchall():
        code = r[0]
        listed = r[3] if len(r) > 3 else None
        if listed and listed > cutoff:
            continue  # 上市未滿 3 年，跳過
        c.execute("SELECT COUNT(*) FROM financial_annual WHERE code=? AND net_income IS NOT NULL", (code,))
        fin = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM monthly_revenue WHERE code=?", (code,))
        rev = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM quarterly_financial WHERE code=?", (code,))
        qtr = c.fetchone()[0]
        if fin >= 3 and rev >= 12 and qtr >= 4:
            continue
        need.append((code, r[1], r[2]))
    conn.close()
    return need


def backfill():
    t0 = time.time()
    need = _find_need()

    if not need:
        print("[補齊] 所有股票資料已完整")
        return

    print(f"[補齊] 待補 {len(need)} 支")

    # ── 第一輪：Yahoo Finance（年度財報+季度財報，免費無限制）──
    print("\n[第一輪] Yahoo Finance（無限制）")
    try:
        from yahoo_fetcher import _get_yahoo_session, fetch_yahoo_financials, save_yahoo_to_db
        session, crumb = _get_yahoo_session()
        yahoo_done = 0
        yahoo_fail = 0
        for code, name, market in need:
            data = fetch_yahoo_financials(session, crumb, code, market)
            if data:
                a, q = save_yahoo_to_db(code, data)
                if a > 0 or q > 0:
                    yahoo_done += 1
                    yahoo_fail = 0
                else:
                    yahoo_fail += 1
            else:
                yahoo_fail += 1
            if yahoo_done % 100 == 0 and yahoo_done > 0:
                print(f"  進度: {yahoo_done}/{len(need)}")
            if yahoo_fail >= 30:
                try:
                    session, crumb = _get_yahoo_session()
                    yahoo_fail = 0
                except:
                    break
            time.sleep(random.uniform(0.1, 0.3))
        print(f"  Yahoo 完成: {yahoo_done} 支")
    except Exception as e:
        print(f"  Yahoo 失敗: {e}")

    # ── 第二輪：政府 API 補月營收（批次，無限制）──
    print("\n[第二輪] 政府 API 月營收（無限制）")
    try:
        from scraper import fetch_json, safe_float, init_db
        init_db()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        rev_saved = 0
        for label, url in [
            ("上市", "https://openapi.twse.com.tw/v1/openData/t187ap05_L"),
            ("上櫃", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"),
        ]:
            data = fetch_json(url)
            if not data:
                continue
            for d in data:
                code = str(d.get('公司代號', '')).strip()
                ym_str = str(d.get('資料年月', '')).strip()
                revenue = safe_float(d.get('營業收入-當月營收'))
                if not code or not ym_str or revenue is None:
                    continue
                try:
                    roc_year = int(ym_str[:-2])
                    month = int(ym_str[-2:])
                    west_year = roc_year + 1911
                except:
                    continue
                c.execute("""INSERT OR IGNORE INTO monthly_revenue
                    (code, year, month, revenue, updated_at) VALUES (?,?,?,?,?)""",
                    (code, west_year, month, revenue, now_str))
                if c.rowcount:
                    rev_saved += 1
        conn.commit()
        conn.close()
        print(f"  月營收新增: {rev_saved} 筆")
    except Exception as e:
        print(f"  月營收失敗: {e}")

    # ── 第三輪：FinMind 補充（月營收歷史 + PE 歷史）──
    print("\n[第三輪] FinMind 補充（月營收歷史+PE歷史，有額度限制）")
    try:
        from scraper import fetch_company_monthly_revenue, fetch_pe_history
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        fm_need = []
        for code, name, market in need:
            c.execute("SELECT COUNT(*) FROM monthly_revenue WHERE code=?", (code,))
            rev = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM pe_history WHERE code=?", (code,))
            pe = c.fetchone()[0]
            if rev < 12 or pe == 0:
                fm_need.append((code, name))
        conn.close()

        if not fm_need:
            print("  無需 FinMind 補充")
        else:
            fm_done = 0
            fm_fail = 0
            for code, name in fm_need[:250]:
                try:
                    r1 = fetch_company_monthly_revenue(code)
                    r2 = fetch_pe_history(code)
                    if r1 or r2:
                        fm_done += 1
                        fm_fail = 0
                    else:
                        fm_fail += 1
                    if fm_fail >= 20:
                        print(f"  FinMind 額度用完，已完成 {fm_done} 支")
                        break
                    time.sleep(random.uniform(0.1, 0.3))
                except:
                    fm_fail += 1
                    if fm_fail >= 20:
                        break
            print(f"  FinMind 完成: {fm_done} 支")
    except Exception as e:
        print(f"  FinMind 失敗: {e}")

    # ── 統計 ──
    remain = _find_need()
    elapsed = time.time() - t0
    print(f"\n[補齊] 完成，耗時 {elapsed:.0f} 秒")
    print(f"       剩餘待補: {len(remain)} 支")


if __name__ == "__main__":
    backfill()
