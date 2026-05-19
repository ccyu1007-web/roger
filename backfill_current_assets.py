#!/usr/bin/env python3
"""批次從群益 BS 補抓 current_assets（流動資產），只 UPDATE 已存在的年度記錄"""
import sqlite3, sys, os, time, re
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, os.path.dirname(__file__))
from capital_fetcher import _fetch_page, _parse_num

DB_PATH = os.path.join(os.path.dirname(__file__), 'stocks.db')

def fetch_current_assets(code):
    """從群益年度 BS 抓流動資產，回傳 {year: value} dict"""
    url = f"https://stock.capital.com.tw/z/zc/zcp/zcpb/zcpb.djhtm?a={code}"
    texts = _fetch_page(url)
    if not texts or len(texts) < 5:
        return {}

    # 找期別
    years = []
    for i, t in enumerate(texts):
        if t == '期別':
            for j in range(i + 1, min(i + 12, len(texts))):
                if re.match(r'^\d+$', texts[j]):
                    years.append(int(texts[j]))
                elif texts[j] in ('種類', '合併', '個別'):
                    break
                else:
                    break
            break
    if not years:
        return {}

    n = len(years)
    result = {}
    for i, t in enumerate(texts):
        if t == '流動資產' and i + n < len(texts):
            vals = texts[i + 1: i + 1 + n]
            for j, yr in enumerate(years):
                if j < len(vals):
                    v = _parse_num(vals[j])
                    if v is not None:
                        result[yr] = v * 1000000  # 百萬→元
            break
    return result

def main():
    conn = sqlite3.connect(DB_PATH)
    # 取所有需要補的股票（有 current_liabilities 但沒 current_assets 的）
    rows = conn.execute("""
        SELECT DISTINCT code FROM financial_annual
        WHERE current_liabilities IS NOT NULL AND current_liabilities > 0
        AND (current_assets IS NULL OR current_assets = 0)
    """).fetchall()
    codes = [r[0] for r in rows]
    conn.close()
    print(f"需要補抓 {len(codes)} 支股票的流動資產")

    updated = 0
    errors = 0

    def process(code):
        try:
            data = fetch_current_assets(code)
            if not data:
                return 0
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            cnt = 0
            for yr, val in data.items():
                c.execute("""UPDATE financial_annual SET current_assets = ?
                            WHERE code = ? AND year = ? AND (current_assets IS NULL OR current_assets = 0)""",
                         (val, code, yr))
                cnt += c.rowcount
            conn.commit()
            conn.close()
            return cnt
        except Exception as e:
            return -1

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(process, code): code for code in codes}
        for i, f in enumerate(as_completed(futures)):
            code = futures[f]
            result = f.result()
            if result > 0:
                updated += result
            elif result < 0:
                errors += 1
            if (i + 1) % 100 == 0:
                print(f"  進度: {i+1}/{len(codes)}  已更新 {updated} 筆")
            time.sleep(0.05)

    print(f"完成：{updated} 筆更新，{errors} 筆失敗")

    # 驗證
    conn = sqlite3.connect(DB_PATH)
    total = conn.execute("SELECT COUNT(*) FROM financial_annual WHERE current_assets IS NOT NULL AND current_assets > 0").fetchone()[0]
    print(f"current_assets 有值筆數: {total}")
    conn.close()

if __name__ == '__main__':
    main()
