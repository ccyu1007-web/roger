"""
verify.py — 全量資料驗證腳本
1. 測試所有個股頁 API 是否能正常回傳
2. 比對本機 vs Render 的股價/EPS/營收
3. 產出報告
"""
import requests
import sqlite3
import json
import time
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = os.path.join(os.path.dirname(__file__), 'stocks.db')
RENDER_URL = "https://tock-system.onrender.com"
REPORT_PATH = os.path.join(os.path.dirname(__file__), 'verify_report.txt')

_session = requests.Session()
_session.headers.update({'User-Agent': 'Mozilla/5.0 (StockVerify/1.0)'})


def verify_all():
    t0 = time.time()
    report = []
    report.append(f"{'='*60}")
    report.append(f"逍遙投資系統 — 全量資料驗證報告")
    report.append(f"時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"{'='*60}\n")

    # ── 1. 取得本機所有股票 ──
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    local_stocks = conn.execute(
        "SELECT code, name, close, change, eps_1, eps_1q, revenue_yoy FROM stocks ORDER BY code"
    ).fetchall()
    local_map = {r['code']: dict(r) for r in local_stocks}
    conn.close()
    total = len(local_stocks)
    report.append(f"[本機] 股票總數：{total} 支\n")

    # ── 2. 取得 Render 所有股票 ──
    print("[Step 1] 取得 Render 股票清單...")
    try:
        resp = _session.get(f"{RENDER_URL}/api/stocks", timeout=90)
        render_data = resp.json().get('data', [])
        render_map = {s['code']: s for s in render_data}
        report.append(f"[Render] 股票總數：{len(render_data)} 支")
    except Exception as e:
        report.append(f"[Render] 取得股票清單失敗：{e}")
        render_map = {}

    # 筆數差異
    local_only = set(local_map.keys()) - set(render_map.keys())
    render_only = set(render_map.keys()) - set(local_map.keys())
    if local_only:
        report.append(f"[差異] 本機有但 Render 沒有：{len(local_only)} 支")
        report.append(f"  代碼：{sorted(local_only)[:20]}{'...' if len(local_only) > 20 else ''}")
    if render_only:
        report.append(f"[差異] Render 有但本機沒有：{len(render_only)} 支")
        report.append(f"  代碼：{sorted(render_only)[:20]}{'...' if len(render_only) > 20 else ''}")
    if not local_only and not render_only:
        report.append(f"[OK] 本機與 Render 股票清單完全一致")
    report.append("")

    # ── 3. 比對股價/EPS ──
    print("[Step 2] 比對股價與 EPS...")
    price_mismatch = []
    eps_mismatch = []
    common_codes = set(local_map.keys()) & set(render_map.keys())

    for code in sorted(common_codes):
        lc = local_map[code]
        rc = render_map[code]

        # 股價比對
        l_close = lc.get('close')
        r_close = rc.get('close')
        if l_close is not None and r_close is not None:
            if abs(l_close - r_close) > 0.01:
                price_mismatch.append({
                    'code': code, 'name': lc.get('name', ''),
                    'local': l_close, 'render': r_close,
                    'diff': round(l_close - r_close, 2)
                })

        # EPS 比對（最新一季）
        l_eps = lc.get('eps_1')
        r_eps = rc.get('eps_1')
        if l_eps is not None and r_eps is not None:
            if abs(l_eps - r_eps) > 0.01:
                eps_mismatch.append({
                    'code': code, 'name': lc.get('name', ''),
                    'local': l_eps, 'render': r_eps,
                    'diff': round(l_eps - r_eps, 2)
                })

    report.append(f"[股價比對] 共 {len(common_codes)} 支")
    if price_mismatch:
        report.append(f"  不一致：{len(price_mismatch)} 支")
        for p in price_mismatch[:30]:
            report.append(f"    {p['code']} {p['name']}: 本機={p['local']} Render={p['render']} 差={p['diff']}")
        if len(price_mismatch) > 30:
            report.append(f"    ...還有 {len(price_mismatch)-30} 支")
    else:
        report.append(f"  全部一致")

    report.append(f"\n[EPS比對] 共 {len(common_codes)} 支")
    if eps_mismatch:
        report.append(f"  不一致：{len(eps_mismatch)} 支")
        for p in eps_mismatch[:30]:
            report.append(f"    {p['code']} {p['name']}: 本機={p['local']} Render={p['render']} 差={p['diff']}")
        if len(eps_mismatch) > 30:
            report.append(f"    ...還有 {len(eps_mismatch)-30} 支")
    else:
        report.append(f"  全部一致")
    report.append("")

    # ── 4. 測試所有個股頁 API ──
    print("[Step 3] 測試所有個股頁 API（這會花 30-40 分鐘）...")
    codes = sorted(local_map.keys())

    api_ok = 0
    api_fail = []
    api_slow = []
    done = 0

    def _test_one(code):
        try:
            t = time.time()
            resp = _session.get(
                f"{RENDER_URL}/api/stocks/{code}/financials",
                timeout=30
            )
            elapsed = time.time() - t
            return code, resp.status_code, elapsed, None
        except Exception as e:
            return code, 0, 0, str(e)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_test_one, code): code for code in codes}
        for future in as_completed(futures):
            code, status, elapsed, err = future.result()
            done += 1
            if status == 200:
                api_ok += 1
                if elapsed > 10:
                    api_slow.append({'code': code, 'time': round(elapsed, 1)})
            else:
                api_fail.append({
                    'code': code, 'name': local_map.get(code, {}).get('name', ''),
                    'status': status, 'error': err
                })
            if done % 200 == 0:
                print(f"  進度：{done}/{total} (OK={api_ok}, Fail={len(api_fail)})")

    report.append(f"[個股頁API測試] 共測試 {total} 支")
    report.append(f"  成功：{api_ok} 支")
    report.append(f"  失敗：{len(api_fail)} 支")
    report.append(f"  回應超過10秒：{len(api_slow)} 支")

    if api_fail:
        report.append(f"\n  === 失敗清單 ===")
        for f in api_fail[:50]:
            report.append(f"    {f['code']} {f['name']}: HTTP {f['status']} {f['error'] or ''}")
        if len(api_fail) > 50:
            report.append(f"    ...還有 {len(api_fail)-50} 支")

    if api_slow:
        report.append(f"\n  === 回應慢（>10秒）===")
        for s in sorted(api_slow, key=lambda x: -x['time'])[:20]:
            report.append(f"    {s['code']}: {s['time']}秒")
    report.append("")

    # ── 5. 本機資料完整性 ──
    print("[Step 4] 檢查本機資料完整性...")
    conn = sqlite3.connect(DB_PATH)
    checks = {
        '有收盤價': conn.execute("SELECT COUNT(*) FROM stocks WHERE close IS NOT NULL").fetchone()[0],
        '有EPS': conn.execute("SELECT COUNT(*) FROM stocks WHERE eps_1 IS NOT NULL").fetchone()[0],
        '有營收': conn.execute("SELECT COUNT(*) FROM stocks WHERE revenue_yoy IS NOT NULL").fetchone()[0],
        '有股利': conn.execute("SELECT COUNT(*) FROM stocks WHERE div_c1 IS NOT NULL").fetchone()[0],
        '有財務等級': conn.execute("SELECT COUNT(*) FROM stocks WHERE fin_grade_1 IS NOT NULL").fetchone()[0],
        '年報筆數': conn.execute("SELECT COUNT(*) FROM financial_annual").fetchone()[0],
        '季報筆數': conn.execute("SELECT COUNT(*) FROM quarterly_financial").fetchone()[0],
        '月營收筆數': conn.execute("SELECT COUNT(*) FROM monthly_revenue").fetchone()[0],
    }
    conn.close()

    report.append(f"[本機資料完整性]")
    for k, v in checks.items():
        pct = f"({v/total*100:.1f}%)" if '筆數' not in k else ""
        report.append(f"  {k}: {v} {pct}")
    report.append("")

    # ── 總結 ──
    elapsed = time.time() - t0
    report.append(f"{'='*60}")
    report.append(f"驗證完成，耗時 {elapsed:.0f} 秒")

    issues = len(price_mismatch) + len(eps_mismatch) + len(api_fail) + len(local_only) + len(render_only)
    if issues == 0:
        report.append("結論：全部通過，無異常")
    else:
        report.append(f"結論：發現 {issues} 個問題需處理")
        if price_mismatch:
            report.append(f"  - 股價不一致 {len(price_mismatch)} 支")
        if eps_mismatch:
            report.append(f"  - EPS不一致 {len(eps_mismatch)} 支")
        if api_fail:
            report.append(f"  - 個股頁打不開 {len(api_fail)} 支")
        if local_only:
            report.append(f"  - 本機有但Render沒有 {len(local_only)} 支")
        if render_only:
            report.append(f"  - Render有但本機沒有 {len(render_only)} 支")
    report.append(f"{'='*60}")

    # 寫入報告
    report_text = '\n'.join(report)
    with open(REPORT_PATH, 'w') as f:
        f.write(report_text)

    print(f"\n報告已存到 {REPORT_PATH}")
    print(report_text)


if __name__ == '__main__':
    verify_all()
