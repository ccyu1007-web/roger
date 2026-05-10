"""一次性回填 ROIC 所需的資產負債表欄位（7個新欄位）"""
import db as sqlite3
from capital_fetcher import fetch_capital_balance_sheet
from concurrent.futures import ThreadPoolExecutor, as_completed
import time, sys

def main():
    # 找出需要回填的公司（cash_and_equivalents 為 NULL 代表沒抓過新欄位）
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        # 確保欄位存在
        try: c.execute("ALTER TABLE financial_annual ADD COLUMN cash_and_equivalents REAL")
        except: pass
        
        c.execute("""SELECT DISTINCT f.code FROM financial_annual f
                     LEFT JOIN (SELECT code FROM financial_annual WHERE cash_and_equivalents IS NOT NULL) h
                     ON f.code = h.code
                     WHERE h.code IS NULL""")
        codes = [r[0] for r in c.fetchall()]
    
    print(f"需回填 {len(codes)} 家公司的 ROIC 欄位")
    if not codes:
        print("全部已有資料，無需回填")
        return
    
    done = 0
    failed = 0
    start = time.time()
    
    def fetch_one(code):
        try:
            return code, fetch_capital_balance_sheet(code)
        except Exception as e:
            return code, -1
    
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(fetch_one, c): c for c in codes}
        for f in as_completed(futures):
            code, result = f.result()
            if result > 0:
                done += 1
            elif result < 0:
                failed += 1
            
            total = done + failed
            if total % 100 == 0:
                elapsed = time.time() - start
                rate = total / elapsed if elapsed > 0 else 0
                remain = (len(codes) - total) / rate / 60 if rate > 0 else 0
                print(f"  進度 {total}/{len(codes)} (成功:{done} 失敗:{failed}) 預估剩餘 {remain:.1f} 分鐘")
    
    elapsed = time.time() - start
    print(f"\n完成！成功 {done} / 失敗 {failed} / 總計 {len(codes)} 家，耗時 {elapsed/60:.1f} 分鐘")

if __name__ == '__main__':
    main()
