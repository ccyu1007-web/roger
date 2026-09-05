"""
backtest_estimate.py — 方案A vs 方案B 季度EPS估算準確度回測
"""

import sqlite3
import statistics
from collections import defaultdict

DB_PATH = "/Users/roger/Documents/AI機器人/stock_system/stocks.db"


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def parse_quarter(q):
    """把 '114Q1' 解成 (114, 1)"""
    y, n = q.split('Q')
    return int(y), int(n)


def quarter_sort_key(q):
    y, n = parse_quarter(q)
    return y * 10 + n


def prev_quarter(q):
    y, n = parse_quarter(q)
    if n == 1:
        return f"{y-1}Q4"
    return f"{y}Q{n-1}"


def weighted_avg(values, weights=None):
    """加權平均，values 由新到舊"""
    if not values:
        return None
    if weights is None:
        weights = [0.4, 0.3, 0.2, 0.1]
    n = min(len(values), len(weights))
    vs = values[:n]
    ws = weights[:n]
    wsum = sum(ws)
    return sum(vs[i] * ws[i] for i in range(n)) / wsum


def estimate_eps_A(hist_before, ann_rows, actual_revenue):
    """
    方案A：
    - 毛利率：年70% + 季30%
    - 營業費用率：年70% + 季30%
    - 業外：年度÷4×0.7 + 季度加權×0.3
    - 稅率：年70% + 季30%
    """
    return _estimate_core(hist_before, ann_rows, actual_revenue,
                          gm_w_ann=0.7, gm_w_q=0.3,
                          opex_w_ann=0.7, opex_w_q=0.3,
                          nonop_mode='A',
                          tax_w_ann=0.7, tax_w_q=0.3)


def estimate_eps_B(hist_before, ann_rows, actual_revenue):
    """
    方案B：
    - 毛利率：季60% + 年40%
    - 營業費用率：年60% + 季40%
    - 業外：純季度加權
    - 稅率：年70% + 季30%（不變）
    """
    return _estimate_core(hist_before, ann_rows, actual_revenue,
                          gm_w_ann=0.4, gm_w_q=0.6,
                          opex_w_ann=0.6, opex_w_q=0.4,
                          nonop_mode='B',
                          tax_w_ann=0.7, tax_w_q=0.3)


def estimate_eps_variant(hist_before, ann_rows, actual_revenue, variant):
    """
    分項調整版本：只調整指定項目，其他用A方案
    variant: 'gm_only', 'opex_only', 'nonop_only'
    """
    if variant == 'gm_only':
        return _estimate_core(hist_before, ann_rows, actual_revenue,
                              gm_w_ann=0.4, gm_w_q=0.6,
                              opex_w_ann=0.7, opex_w_q=0.3,
                              nonop_mode='A',
                              tax_w_ann=0.7, tax_w_q=0.3)
    elif variant == 'opex_only':
        return _estimate_core(hist_before, ann_rows, actual_revenue,
                              gm_w_ann=0.7, gm_w_q=0.3,
                              opex_w_ann=0.6, opex_w_q=0.4,
                              nonop_mode='A',
                              tax_w_ann=0.7, tax_w_q=0.3)
    elif variant == 'nonop_only':
        return _estimate_core(hist_before, ann_rows, actual_revenue,
                              gm_w_ann=0.7, gm_w_q=0.3,
                              opex_w_ann=0.7, opex_w_q=0.3,
                              nonop_mode='B',
                              tax_w_ann=0.7, tax_w_q=0.3)
    else:
        raise ValueError(f"unknown variant: {variant}")


def _estimate_core(hist_before, ann_rows, actual_revenue,
                   gm_w_ann, gm_w_q,
                   opex_w_ann, opex_w_q,
                   nonop_mode,
                   tax_w_ann, tax_w_q):
    """
    核心估算邏輯（共用）
    hist_before: 目標季之前的季度記錄（由新到舊）
    ann_rows: 年報（由新到舊）
    actual_revenue: 目標季實際營收（測試變數，直接用）
    回傳 est_eps 或 None
    """
    est_rev = actual_revenue
    if not est_rev or est_rev <= 0:
        return None

    # --- 毛利率 ---
    recent4 = [r for r in hist_before[:4]
               if r['revenue'] and r['revenue'] > 0 and r['gross_profit'] is not None]
    gm_pool = [r['gross_profit'] / r['revenue'] for r in recent4]
    if not gm_pool:
        return None

    if len(gm_pool) >= 2:
        gm_q = weighted_avg(gm_pool)
        ann_gm = None
        for ar in ann_rows:
            if ar['revenue'] and ar['revenue'] > 0 and ar['gross_profit'] is not None:
                ann_gm = ar['gross_profit'] / ar['revenue']
                break
        if ann_gm is not None:
            est_gm = gm_q * gm_w_q + ann_gm * gm_w_ann
        else:
            est_gm = gm_q
    else:
        est_gm = gm_pool[0]

    est_gross_profit = est_rev * est_gm

    # --- 營業費用率 ---
    opex_data = [(r['operating_expense'], r['revenue'])
                 for r in hist_before[:8]
                 if r['operating_expense'] is not None
                 and r['revenue'] and r['revenue'] > 0]

    if len(opex_data) >= 2:
        q_opex_rates = [opex_data[i][0] / opex_data[i][1] for i in range(min(4, len(opex_data)))]
        q_rate = weighted_avg(q_opex_rates)

        ann_opex_rate = None
        for ar in ann_rows:
            if ar['operating_expense'] is not None and ar['revenue'] and ar['revenue'] > 0:
                ann_opex_rate = ar['operating_expense'] / ar['revenue']
                break

        if ann_opex_rate is not None:
            est_opex_rate = ann_opex_rate * opex_w_ann + q_rate * opex_w_q
        else:
            est_opex_rate = q_rate
        est_opex = est_opex_rate * est_rev
    elif opex_data:
        est_opex = opex_data[0][0]
    else:
        est_opex = est_gross_profit * 0.3

    est_oi = est_gross_profit - est_opex

    # --- 業外 ---
    nonop_list = [r['non_operating'] for r in hist_before[:8] if r['non_operating'] is not None]

    if nonop_mode == 'A':
        # 方案A：年度÷4×0.7 + 季度加權×0.3
        if nonop_list:
            recent_nonop = nonop_list[:4]
            q_nonop = weighted_avg(recent_nonop)
            ann_nonop_q = None
            for ar in ann_rows:
                if ar['non_operating'] is not None:
                    ann_nonop_q = ar['non_operating'] / 4
                    break
            if ann_nonop_q is not None:
                est_nonop = ann_nonop_q * 0.7 + q_nonop * 0.3
            else:
                est_nonop = q_nonop
        else:
            est_nonop = 0
    else:
        # 方案B：純季度加權
        if nonop_list:
            recent_nonop = nonop_list[:4]
            est_nonop = weighted_avg(recent_nonop)
        else:
            est_nonop = 0

    est_pti = est_oi + est_nonop

    # --- 稅率 ---
    tax_sum = pti_sum = 0
    for r in hist_before[:4]:
        pti = r['pretax_income']
        if pti and pti > 0:
            nip_r = r['net_income_parent']
            tax = r['tax']
            if nip_r is not None:
                calc_tax = pti - nip_r
                if tax is None or (tax == 0 and abs(calc_tax) > 100):
                    tax = calc_tax
                if abs(pti - nip_r) < 1 and pti > 1000000:
                    tax = pti * 0.20
            if tax is not None:
                pti_sum += pti
                tax_sum += tax
    q_tax_rate = tax_sum / pti_sum if pti_sum > 0 else 0.20

    ann_tax_rate = None
    for ar in ann_rows:
        ar_pti = ar['pretax_income']
        ar_ni = ar['net_income'] if ar['net_income'] else ar['net_income_parent'] if 'net_income_parent' in ar.keys() else None
        ar_tax = ar['tax'] if 'tax' in ar.keys() else None
        if ar_pti and ar_ni:
            calc = ar_pti - ar_ni
            if ar_tax is None or (ar_tax == 0 and abs(calc) > 100):
                ar_tax = calc
            if abs(ar_pti - ar_ni) < 1 and ar_pti > 1000000:
                ar_tax = ar_pti * 0.20
        if ar_pti and ar_pti > 0 and ar_tax:
            ann_tax_rate = ar_tax / ar_pti
            break

    if ann_tax_rate is not None:
        est_tax_rate = max(0.05, min(ann_tax_rate * tax_w_ann + q_tax_rate * tax_w_q, 0.40))
    else:
        est_tax_rate = max(0.05, min(q_tax_rate, 0.40))

    est_ni = est_pti * (1 - est_tax_rate) if est_pti > 0 else est_pti * 0.80

    # --- 歸屬母公司 ---
    pw_list = []
    for r in hist_before[:4]:
        ci = r['net_income_parent']  # 簡化：用 net_income_parent 本身（CI 欄未必存在）
        nip = r['net_income_parent']
        if ci and ci != 0 and nip is not None:
            pw = nip / ci
            if 0.3 <= pw <= 1.1:
                pw_list.append(pw)
    est_pw = statistics.mean(pw_list) if pw_list else 1.0
    est_nip = est_ni * est_pw

    # --- 股數 ---
    est_shares = None
    for r in hist_before[:4]:
        if r['eps'] and r['eps'] != 0 and r['net_income_parent'] is not None:
            est_shares = r['net_income_parent'] / r['eps']
            break
    if not est_shares:
        return None

    est_eps = round(est_nip / est_shares, 2)
    return est_eps


def load_all_data():
    """從 DB 讀取所有所需資料"""
    conn = get_conn()

    # 所有季度記錄（有 eps 且有 revenue）
    q_rows = conn.execute("""
        SELECT qf.code, qf.quarter, qf.eps, qf.revenue,
               qf.gross_profit, qf.operating_expense, qf.non_operating,
               qf.pretax_income, qf.net_income_parent, qf.tax
        FROM quarterly_financial qf
        WHERE qf.eps IS NOT NULL AND qf.revenue IS NOT NULL
        ORDER BY qf.code,
                 CAST(SUBSTR(qf.quarter, 1, INSTR(qf.quarter, 'Q') - 1) AS INTEGER) DESC,
                 CAST(SUBSTR(qf.quarter, INSTR(qf.quarter, 'Q') + 1) AS INTEGER) DESC
    """).fetchall()

    # 所有年報
    ann_rows = conn.execute("""
        SELECT code, year, revenue, gross_profit, operating_expense,
               non_operating, pretax_income, net_income, tax, eps
        FROM financial_annual
        WHERE revenue IS NOT NULL
        ORDER BY code, year DESC
    """).fetchall()

    conn.close()

    # 按 code 整理
    q_by_code = defaultdict(list)
    for r in q_rows:
        q_by_code[r['code']].append(dict(r))

    ann_by_code = defaultdict(list)
    for r in ann_rows:
        ann_by_code[r['code']].append(dict(r))

    return q_by_code, ann_by_code


def run_backtest():
    print("載入資料中...")
    q_by_code, ann_by_code = load_all_data()

    codes = [c for c in q_by_code if len(q_by_code[c]) >= 5]
    print(f"有足夠季度資料的股票：{len(codes)} 支")

    results_A = []
    results_B = []
    results_gm_only = []
    results_opex_only = []
    results_nonop_only = []

    skip_count = 0

    for code in codes:
        quarters = q_by_code[code]  # 已由新到舊排序
        ann_rows = ann_by_code.get(code, [])

        # 對每個季度（從第5個開始，前面需要至少4季歷史）
        for i in range(len(quarters) - 4):
            target = quarters[i]
            hist_before = quarters[i+1:]   # 目標季之前的歷史（由新到舊）
            target_q = target['quarter']
            target_eps = target['eps']
            target_rev = target['revenue']

            # 需要至少4季歷史
            if len(hist_before) < 4:
                continue

            # 找對應的年報（比目標季的西元年早的年報）
            target_y, target_n = parse_quarter(target_q)
            target_west = target_y + 1911
            # 年報要用目標季之前的，即年報年份 < 目標季西元年
            # 若目標是 113Q1(2024Q1)，要用 2023 及以前年報
            relevant_ann = [ar for ar in ann_rows if ar['year'] < target_west]
            if not relevant_ann:
                # 若無可用年報，試試同年但僅 Q3/Q4 之前（年報一般 3 月底公告）
                # 簡化：若季度 >= Q2，可以用上年年報；若 Q1 也用上年年報
                relevant_ann = [ar for ar in ann_rows if ar['year'] <= target_west - 1]

            if not relevant_ann:
                skip_count += 1
                continue

            # 估算
            eps_A = estimate_eps_A(hist_before, relevant_ann, target_rev)
            eps_B = estimate_eps_B(hist_before, relevant_ann, target_rev)
            eps_gm = estimate_eps_variant(hist_before, relevant_ann, target_rev, 'gm_only')
            eps_opex = estimate_eps_variant(hist_before, relevant_ann, target_rev, 'opex_only')
            eps_nonop = estimate_eps_variant(hist_before, relevant_ann, target_rev, 'nonop_only')

            if eps_A is None or eps_B is None:
                skip_count += 1
                continue

            # 過濾極端值（實際EPS絕對值 > 50 視為異常，如控股公司）
            if abs(target_eps) > 50:
                skip_count += 1
                continue

            results_A.append((code, target_q, target_eps, eps_A))
            results_B.append((code, target_q, target_eps, eps_B))
            if eps_gm is not None:
                results_gm_only.append((code, target_q, target_eps, eps_gm))
            if eps_opex is not None:
                results_opex_only.append((code, target_q, target_eps, eps_opex))
            if eps_nonop is not None:
                results_nonop_only.append((code, target_q, target_eps, eps_nonop))

    print(f"跳過樣本（資料不足/無年報/極端值）：{skip_count}")
    print(f"有效樣本數：{len(results_A)}")
    print()

    def calc_metrics(results, label):
        if not results:
            return
        errors = [abs(r[2] - r[3]) for r in results]
        # MAPE：只對實際EPS != 0 計算
        pct_errors = [abs(r[2] - r[3]) / abs(r[2]) * 100
                      for r in results if abs(r[2]) >= 0.1]

        mae = statistics.mean(errors)
        median_ae = statistics.median(errors)
        mape = statistics.mean(pct_errors) if pct_errors else float('nan')
        median_ape = statistics.median(pct_errors) if pct_errors else float('nan')
        rmse = (sum(e**2 for e in errors) / len(errors)) ** 0.5

        print(f"  {label}：")
        print(f"    樣本數={len(results)}  (MAPE用={len(pct_errors)})")
        print(f"    MAE={mae:.4f}  Median AE={median_ae:.4f}  RMSE={rmse:.4f}")
        print(f"    MAPE={mape:.2f}%  Median APE={median_ape:.2f}%")
        return {'mae': mae, 'mape': mape, 'rmse': rmse, 'n': len(results)}

    print("=" * 60)
    print("【整體比較：方案A vs 方案B】")
    print("=" * 60)
    m_A = calc_metrics(results_A, "方案A（現行：年70%+季30%）")
    print()
    m_B = calc_metrics(results_B, "方案B（新方案：毛利率季60%+年40%，費用年60%+季40%，業外純季度）")
    print()

    if m_A and m_B:
        mae_diff = m_A['mae'] - m_B['mae']
        mape_diff = m_A['mape'] - m_B['mape']
        print(f"  差異（A-B）：MAE {mae_diff:+.4f}（{'B較佳' if mae_diff > 0 else 'A較佳'}）  "
              f"MAPE {mape_diff:+.2f}%（{'B較佳' if mape_diff > 0 else 'A較佳'}）")

    print()
    print("=" * 60)
    print("【分項調整貢獻分析（A為基準，只改一項）】")
    print("=" * 60)
    print()
    m_gm = calc_metrics(results_gm_only, "只調整毛利率（季60%+年40%），其餘同A")
    print()
    m_opex = calc_metrics(results_opex_only, "只調整費用率（年60%+季40%），其餘同A")
    print()
    m_nonop = calc_metrics(results_nonop_only, "只調整業外（純季度加權），其餘同A")
    print()

    if m_A:
        print("=" * 60)
        print("【各項目 MAE 改善幅度（vs 方案A）】")
        print("=" * 60)
        for name, m in [("毛利率調整", m_gm), ("費用率調整", m_opex), ("業外調整", m_nonop), ("全部B方案", m_B)]:
            if m:
                diff = m_A['mae'] - m['mae']
                pct = diff / m_A['mae'] * 100
                print(f"  {name:15s}：MAE改善 {diff:+.4f}  ({pct:+.1f}%)  {'✓ 有效' if diff > 0 else '✗ 無效'}")

    print()
    print("=" * 60)
    print("【EPS 誤差分佈分析（方案A）】")
    print("=" * 60)
    bins = [0, 0.5, 1.0, 2.0, 5.0, float('inf')]
    labels = ["<0.5", "0.5~1", "1~2", "2~5", "≥5"]
    errors_A = [abs(r[2] - r[3]) for r in results_A]
    for j in range(len(bins) - 1):
        cnt = sum(1 for e in errors_A if bins[j] <= e < bins[j+1])
        pct = cnt / len(errors_A) * 100 if errors_A else 0
        print(f"  誤差{labels[j]:6s}：{cnt:5d} 筆  {pct:.1f}%")

    print()
    print("【EPS 誤差分佈分析（方案B）】")
    errors_B = [abs(r[2] - r[3]) for r in results_B]
    for j in range(len(bins) - 1):
        cnt = sum(1 for e in errors_B if bins[j] <= e < bins[j+1])
        pct = cnt / len(errors_B) * 100 if errors_B else 0
        print(f"  誤差{labels[j]:6s}：{cnt:5d} 筆  {pct:.1f}%")

    print()
    print("=" * 60)
    print("回測完成")
    print("=" * 60)


if __name__ == '__main__':
    run_backtest()
