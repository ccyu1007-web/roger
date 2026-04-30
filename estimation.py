"""
estimation.py — 系統 EPS 估算
包含季度估算、年度估算、批次更新、估算日誌
從 scraper.py 抽出，降低主檔案複雜度
"""

import logging
import os
import statistics
import db as sqlite3
from datetime import datetime
from fetcher_utils import DB_PATH
from render_sync import _push_estimates_to_render

logger = logging.getLogger(__name__)


def _batch_system_estimate():
    """批次更新所有股票的系統 EPS 估算（取第一季結果存入 stocks 表）"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    for col, typ in [('sys_est_eps','REAL'),('sys_est_quarter','TEXT'),('sys_est_confidence','TEXT')]:
        try: conn.execute(f"ALTER TABLE stocks ADD COLUMN {col} {typ}")
        except Exception: pass
    try: conn.commit()
    except Exception: pass
    rows = conn.execute("SELECT code, industry FROM stocks ORDER BY code").fetchall()
    conn.close()

    success = 0
    for r in rows:
        code = r['code']
        if (r['industry'] or '') in _EXCLUDED_INDUSTRIES:
            continue
        try:
            result = estimate_system_eps(code)
            if result.get('est_eps') is not None and 'error' not in result:
                c2 = sqlite3.connect(DB_PATH)
                c2.execute(
                    "UPDATE stocks SET sys_est_eps=?, sys_est_quarter=?, sys_est_confidence=? WHERE code=?",
                    (result['est_eps'], result['quarter'], result['confidence'], code)
                )
                c2.commit()
                c2.close()
                success += 1
        except Exception: pass
    print(f"  系統 EPS 估算：{success}/{len(rows)} 支完成")


# 不適用系統估算的產業（損益結構與一般製造/服務業不同）
_EXCLUDED_INDUSTRIES = {'金融保險業', '金融業', '保險業', '銀行業', '證券業', '票券業', '期貨業', '建材營造'}


def _batch_annual_estimate():
    """批次更新所有股票的年度 EPS 估算"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    for col, typ in [('sys_ann_eps','REAL'),('sys_ann_div','REAL'),('sys_ann_pe','REAL'),
                     ('sys_ann_yld','REAL'),('sys_ann_confidence','TEXT')]:
        try: conn.execute(f"ALTER TABLE stocks ADD COLUMN {col} {typ}")
        except Exception: pass
    try: conn.commit()
    except Exception: pass
    rows = conn.execute("SELECT code, industry FROM stocks ORDER BY code").fetchall()
    conn.close()

    success = skip = 0
    for r in rows:
        code = r['code']
        ind = r['industry'] or ''
        # 不適用產業標記 N/A
        if ind in _EXCLUDED_INDUSTRIES:
            try:
                c2 = sqlite3.connect(DB_PATH)
                c2.execute("UPDATE stocks SET sys_ann_confidence='N/A' WHERE code=?", (code,))
                c2.commit(); c2.close()
            except Exception: pass
            skip += 1
            continue
        try:
            ar = estimate_annual_eps(code)
            if ar.get('est_eps') is not None and 'error' not in ar:
                d = ar['details']
                _log_estimate(code, ar, 'annual')
                c2 = sqlite3.connect(DB_PATH)
                c2.execute("""UPDATE stocks SET sys_ann_eps=?, sys_ann_div=?, sys_ann_pe=?,
                              sys_ann_yld=?, sys_ann_confidence=? WHERE code=?""",
                           (ar['est_eps'], d.get('est_div'), d.get('est_pe'),
                            d.get('est_yld'), ar['confidence'], code))
                c2.commit(); c2.close()
                success += 1
        except Exception: pass
    print(f"  年度 EPS 估算：{success} 支完成，{skip} 支產業排除")

    # 估算結果 push 到 Render
    if not os.environ.get('DATABASE_URL'):
        _push_estimates_to_render()

    # 自動回填實際 EPS（回測用）
    _backfill_actual_eps()




def _backfill_actual_eps():
    """自動回填實際 EPS 到 system_eps_actual（回測用）"""
    try:
        _init_eps_log_db()
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        roc_year = datetime.now().year - 1911

        # 回填已公布的季度 EPS
        quarters = conn.execute("""
            SELECT DISTINCT code, quarter, eps, revenue, updated_at
            FROM quarterly_financial
            WHERE eps IS NOT NULL
            AND CAST(SUBSTR(quarter, 1, INSTR(quarter, 'Q') - 1) AS INTEGER) >= ?
        """, (roc_year - 1,)).fetchall()

        filled = 0
        for q in quarters:
            try:
                conn.execute("""INSERT OR IGNORE INTO system_eps_actual
                    (code, target_period, actual_revenue, actual_eps, report_date)
                    VALUES (?, ?, ?, ?, ?)""",
                    (q['code'], q['quarter'], q['revenue'], q['eps'], q['updated_at']))
                filled += conn.total_changes
            except Exception: pass

        # 回填已公布的年度 EPS
        annuals = conn.execute("""
            SELECT code, year, revenue, eps, updated_at
            FROM financial_annual
            WHERE eps IS NOT NULL AND year >= ?
        """, (datetime.now().year - 2,)).fetchall()

        for a in annuals:
            try:
                period = f"{a['year'] - 1911}年度"
                conn.execute("""INSERT OR IGNORE INTO system_eps_actual
                    (code, target_period, actual_revenue, actual_eps, report_date)
                    VALUES (?, ?, ?, ?, ?)""",
                    (a['code'], period, a['revenue'], a['eps'], a['updated_at']))
            except Exception: pass

        conn.commit()
        conn.close()
    except Exception: pass


# ── 系統 EPS 估算核心 ─────────────────────────────────────────
def _get_est_common_data(code):
    """取得估算所需的共用資料（季度財報 + 月營收 + 年度財報）"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    hist = [dict(r) for r in conn.execute("""
        SELECT * FROM quarterly_financial WHERE code = ?
        ORDER BY CAST(SUBSTR(quarter, 1, INSTR(quarter, 'Q') - 1) AS INTEGER) DESC,
                 CAST(SUBSTR(quarter, INSTR(quarter, 'Q') + 1) AS INTEGER) DESC
        LIMIT 16
    """, (code,)).fetchall()]
    rev_rows = conn.execute("""
        SELECT year, month, revenue FROM monthly_revenue
        WHERE code = ? ORDER BY year DESC, month DESC LIMIT 48
    """, (code,)).fetchall()
    rev_map = {(r['year'], r['month']): r['revenue'] for r in rev_rows}
    ann_rows = [dict(r) for r in conn.execute("""
        SELECT * FROM financial_annual WHERE code = ?
        ORDER BY year DESC LIMIT 5
    """, (code,)).fetchall()]
    conn.close()
    return hist, rev_map, rev_rows, ann_rows


def _estimate_quarter_core(hist, rev_map, rev_rows, roc_year, q_num, west_year, ann_rows=None):
    """
    估算單一季度 EPS 的核心邏輯。
    回傳 dict（含 quarter, est_eps, confidence, details）或含 error 的 dict。
    """
    import statistics

    Q_MONTHS = {1: [1,2,3], 2: [4,5,6], 3: [7,8,9], 4: [10,11,12]}
    months = Q_MONTHS[q_num]
    quarter_key = f"{roc_year}Q{q_num}"

    # --- Step 1: 月營收 ---
    est_rev_total = 0
    rev_actual = 0
    rev_estimated = 0
    monthly_revs = []
    for m in months:
        actual = rev_map.get((west_year, m))
        if actual:
            est_rev_total += actual
            rev_actual += 1
            monthly_revs.append({"month": m, "revenue": round(actual), "is_actual": True})
        else:
            prev = rev_map.get((west_year - 1, m))
            est_m = 0
            if prev:
                growths = []
                for rm in rev_rows:
                    ry, rmm = rm['year'], rm['month']
                    prev_rev = rev_map.get((ry - 1, rmm))
                    if prev_rev and prev_rev > 0:
                        growths.append(rm['revenue'] / prev_rev)
                        if len(growths) >= 12:
                            break
                avg_growth = statistics.mean(growths) if growths else 1.0
                est_m = prev * avg_growth
            elif hist and hist[0].get('revenue'):
                est_m = hist[0]['revenue'] / 3
            est_rev_total += est_m
            rev_estimated += 1
            monthly_revs.append({"month": m, "revenue": round(est_m), "is_actual": False})

    if est_rev_total <= 0:
        return {"error": "無營收資料"}

    # --- Step 2: 毛利率（年度×0.7 + 季度×0.3 錨定） ---
    recent = [r for r in hist[:4] if r.get('revenue') and r['revenue'] > 0
              and r.get('gross_profit') is not None]

    gm_pool = [r['gross_profit'] / r['revenue'] for r in recent]

    if not gm_pool:
        return {"error": "無毛利率資料"}

    if len(gm_pool) >= 2:
        weights = [0.4, 0.3, 0.2, 0.1][:len(gm_pool)]
        wsum = sum(weights)
        gm_q = sum(gm_pool[i] * weights[i] for i in range(len(weights))) / wsum
        # 年度錨定
        ann_gm = None
        if ann_rows:
            for ar in ann_rows:
                if ar.get('revenue') and ar['revenue'] > 0 and ar.get('gross_profit') is not None:
                    ann_gm = ar['gross_profit'] / ar['revenue']
                    break
        est_gm = gm_q * 0.3 + ann_gm * 0.7 if ann_gm is not None else gm_q
    else:
        est_gm = gm_pool[0]

    est_gross_profit = est_rev_total * est_gm

    # --- Step 3: 營業費用（年度×0.7 + 季度×0.3 錨定） ---
    opex_data = [(r['operating_expense'], r['revenue'])
                 for r in hist[:8]
                 if r.get('operating_expense') is not None
                 and r.get('revenue') and r['revenue'] > 0]

    if len(opex_data) >= 2:
        # 季度費用率（近4季加權）
        q_opex_rates = [opex_data[i][0] / opex_data[i][1] for i in range(min(4, len(opex_data)))]
        w = [0.4, 0.3, 0.2, 0.1][:len(q_opex_rates)]
        q_rate = sum(q_opex_rates[i] * w[i] for i in range(len(w))) / sum(w)

        # 年度費用率錨定
        ann_opex_rate = None
        if ann_rows:
            for ar in ann_rows:
                if ar.get('operating_expense') and ar.get('revenue') and ar['revenue'] > 0:
                    ann_opex_rate = ar['operating_expense'] / ar['revenue']
                    break
        if ann_opex_rate is not None:
            est_opex_rate = ann_opex_rate * 0.7 + q_rate * 0.3
        else:
            est_opex_rate = q_rate
        est_opex = est_opex_rate * est_rev_total
    elif opex_data:
        est_opex = opex_data[0][0]
    else:
        est_opex = est_gross_profit * 0.3

    est_oi = est_gross_profit - est_opex

    # --- Step 4: 業外（年度÷4 × 0.7 + 季度加權 × 0.3） ---
    nonop_list = [r['non_operating'] for r in hist[:8] if r.get('non_operating') is not None]
    nonop_stable = True
    nonop_level = 'stable'
    if nonop_list:
        # 季度加權
        recent = nonop_list[:4]
        weights = [0.4, 0.3, 0.2, 0.1][:len(recent)]
        wsum = sum(weights)
        q_nonop = sum(recent[i] * weights[i] for i in range(len(weights))) / wsum

        # 年度錨定（年度業外 / 4 = 季度基準）
        ann_nonop_q = None
        if ann_rows:
            for ar in ann_rows:
                if ar.get('non_operating') is not None:
                    ann_nonop_q = ar['non_operating'] / 4
                    break
        if ann_nonop_q is not None:
            est_nonop = ann_nonop_q * 0.7 + q_nonop * 0.3
        else:
            est_nonop = q_nonop

        # 穩定度判斷（用於信心等級）
        if len(nonop_list) >= 3:
            avg = statistics.mean(nonop_list)
            sd = statistics.stdev(nonop_list)
            cv = sd / abs(avg) if abs(avg) > 0 else 0
            if cv >= 1.0:
                nonop_level = 'volatile'
                nonop_stable = False
            elif cv >= 0.5:
                nonop_level = 'moderate'
    else:
        est_nonop = 0

    est_pti = est_oi + est_nonop

    # --- Step 5: 稅率（年度×0.7 + 季度累計×0.3，限 5~40%） ---
    tax_sum = pti_sum = 0
    for r in hist[:4]:
        pti = r.get('pretax_income')
        if pti and pti > 0:
            nip_r = r.get('net_income_parent')
            tax = r.get('tax')
            # 反算：tax=NULL 或 tax=0 但 pti>nip
            if nip_r is not None:
                calc_tax = pti - nip_r
                if tax is None or (tax == 0 and abs(calc_tax) > 100):
                    tax = calc_tax
                # pti==nip 異常
                if abs(pti - nip_r) < 1 and pti > 1000000:
                    tax = pti * 0.20
            if tax is not None:
                pti_sum += pti; tax_sum += tax
    q_tax_rate = tax_sum / pti_sum if pti_sum > 0 else 0.20
    # 年度稅率錨定
    ann_tax_rate = None
    if ann_rows:
        for ar in ann_rows:
            ar_pti = ar.get('pretax_income')
            ar_ni = ar.get('net_income') or ar.get('net_income_parent')
            ar_tax = ar.get('tax')
            # 反算年度稅
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
        est_tax_rate = max(0.05, min(ann_tax_rate * 0.7 + q_tax_rate * 0.3, 0.40))
    else:
        est_tax_rate = max(0.05, min(q_tax_rate, 0.40))
    est_ni = est_pti * (1 - est_tax_rate) if est_pti > 0 else est_pti * 0.80

    # --- Step 6: 歸屬母公司權重 ---
    pw_list = []
    for r in hist[:4]:
        ci = r.get('continuing_income') or r.get('net_income_parent')
        nip = r.get('net_income_parent')
        if ci and ci != 0 and nip is not None:
            pw = nip / ci
            if 0.3 <= pw <= 1.1:
                pw_list.append(pw)
    est_pw = statistics.mean(pw_list) if pw_list else 1.0
    est_nip = est_ni * est_pw

    # --- Step 7: 股數 ---
    est_shares = None
    for r in hist[:4]:
        if r.get('eps') and r['eps'] != 0 and r.get('net_income_parent') is not None:
            est_shares = r['net_income_parent'] / r['eps']
            break
    if not est_shares:
        return {"error": "無法計算股數"}

    est_eps = round(est_nip / est_shares, 2)

    # EPS-本業 = 營業利益 × (1-稅率) / 股數
    est_eps_core = round(est_oi * (1 - est_tax_rate) / est_shares, 2) if est_shares else None
    # EPS-業外 = EPS - EPS本業
    est_eps_nonop = round(est_eps - est_eps_core, 2) if est_eps_core is not None else None

    # --- Step 8: 信心等級 ---
    issues = []
    if rev_estimated >= 2: issues.append("營收多數為預估")
    if nonop_level == 'volatile': issues.append("業外高波動")
    elif nonop_level == 'moderate': issues.append("業外中波動")
    if len(gm_pool) < 2: issues.append("毛利率資料不足")
    if len(opex_data) < 4: issues.append("費用資料較少")
    confidence = "A" if not issues else ("B" if len(issues) == 1 else "C")

    # 同期比較
    yoy_q = f"{roc_year - 1}Q{q_num}"
    yoy_eps = next((r.get('eps') for r in hist if r['quarter'] == yoy_q), None)

    return {
        "quarter": quarter_key,
        "est_eps": est_eps,
        "confidence": confidence,
        "details": {
            "est_revenue": round(est_rev_total),
            "est_gm": round(est_gm * 100, 2),
            "est_gross_profit": round(est_gross_profit),
            "est_opex": round(est_opex),
            "est_oi": round(est_oi),
            "est_nonop": round(est_nonop),
            "est_pti": round(est_pti),
            "est_tax_rate": round(est_tax_rate * 100, 2),
            "est_ni": round(est_ni),
            "est_pw": round(est_pw * 100, 2),
            "est_nip": round(est_nip),
            "est_shares": round(est_shares),
            "est_eps_core": est_eps_core,
            "est_eps_nonop": est_eps_nonop,
            "rev_actual": rev_actual,
            "rev_estimated": rev_estimated,
            "yoy_eps": yoy_eps,
            "issues": issues,
            "monthly_revs": monthly_revs,
        }
    }


def estimate_system_eps(code):
    """單季估算（向下相容，用於批次 stocks 表更新）"""
    hist, rev_map, rev_rows, ann_rows = _get_est_common_data(code)
    if len(hist) < 4:
        return {"error": "季度資料不足（需至少 4 季）", "confidence": "N/A"}

    latest = hist[0]['quarter']
    ly, lq = int(latest.split('Q')[0]), int(latest.split('Q')[1])
    ny, nq = (ly + 1, 1) if lq == 4 else (ly, lq + 1)
    return _estimate_quarter_core(hist, rev_map, rev_rows, ny, nq, ny + 1911, ann_rows)


def estimate_system_eps_multi(code):
    """
    多季估算：根據當年度可用月營收數量，動態產生 1~4 欄預估。
    - 有 1~3 月 → Q1
    - 有 4~6 月 → Q1 + Q2
    - 有 7~9 月 → Q1 + Q2 + Q3
    - 有 10~12 月 → Q1 + Q2 + Q3 + Q4
    - 實際季報公布 15 天後 → 移除該欄
    """
    hist, rev_map, rev_rows, ann_rows = _get_est_common_data(code)
    if len(hist) < 4:
        return {"quarters": [], "error": "季度資料不足"}

    now = datetime.now()
    west_year = now.year
    roc_year = west_year - 1911

    available_months = sum(1 for m in range(1, 13) if rev_map.get((west_year, m)))

    estimates = []
    for qn in range(1, 5):
        # 該季需要的最小月份（Q1→月1, Q2→月4, Q3→月7, Q4→月10）
        min_month_needed = (qn - 1) * 3 + 1
        if available_months < min_month_needed:
            break  # 營收月份不夠，不估這季

        quarter_key = f"{roc_year}Q{qn}"

        # 檢查實際季報是否已公布（且超過 15 天）
        actual_q = next((r for r in hist if r['quarter'] == quarter_key), None)
        if actual_q and actual_q.get('updated_at'):
            try:
                upd = datetime.strptime(actual_q['updated_at'], '%Y-%m-%d %H:%M:%S')
                days_since = (now - upd).days
                if days_since > 15:
                    continue  # 已公布超過 15 天，跳過
            except Exception: pass

        result = _estimate_quarter_core(hist, rev_map, rev_rows, roc_year, qn, west_year, ann_rows)
        if 'error' not in result:
            # 附加實際 vs 預估對照
            if actual_q and actual_q.get('eps') is not None:
                result['has_actual'] = True
                result['actual_eps'] = actual_q.get('eps')
            else:
                result['has_actual'] = False
            estimates.append(result)

    return {"quarters": estimates}


def estimate_annual_eps(code):
    """
    年度 EPS 估算：
    營收：月份佔比法（主）+ YoY衰減法（輔）取保守值
    費用：年度財報回歸（固定+變動）
    業外：年度財報三級制
    稅率/歸屬/股數：季度資料
    """
    import statistics

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # 月營收
    rev_rows = conn.execute("""
        SELECT year, month, revenue FROM monthly_revenue
        WHERE code = ? ORDER BY year DESC, month DESC LIMIT 60
    """, (code,)).fetchall()
    rev_map = {(r['year'], r['month']): r['revenue'] for r in rev_rows}

    # 年度財報
    ann_rows = [dict(r) for r in conn.execute("""
        SELECT * FROM financial_annual WHERE code = ?
        ORDER BY year DESC LIMIT 5
    """, (code,)).fetchall()]

    # 季度財報（稅率/歸屬/股數用）
    q_rows = [dict(r) for r in conn.execute("""
        SELECT * FROM quarterly_financial WHERE code = ?
        ORDER BY CAST(SUBSTR(quarter, 1, INSTR(quarter, 'Q') - 1) AS INTEGER) DESC,
                 CAST(SUBSTR(quarter, INSTR(quarter, 'Q') + 1) AS INTEGER) DESC
        LIMIT 8
    """, (code,)).fetchall()]
    conn.close()

    if len(ann_rows) < 2:
        return {"error": "年度財報不足（需至少 2 年）"}

    west_year = datetime.now().year
    roc_year = west_year - 1911

    # === Step 1: 年度營收預估 ===
    # 當年度已公布月份
    ytd_months = []
    for m in range(1, 13):
        rev = rev_map.get((west_year, m))
        if rev:
            ytd_months.append((m, rev))
        else:
            break
    n_months = len(ytd_months)
    if n_months == 0:
        return {"error": "當年度無月營收資料"}

    ytd_revenue = sum(r for _, r in ytd_months)

    # --- 方法A：月份佔比法（近3年每月佔全年比重） ---
    weight_years = []
    for y in range(west_year - 1, west_year - 4, -1):
        year_total = sum(rev_map.get((y, m), 0) for m in range(1, 13))
        if year_total > 0:
            ytd_weight = sum(rev_map.get((y, m), 0) for m in range(1, n_months + 1)) / year_total
            if ytd_weight > 0:
                weight_years.append(ytd_weight)

    if weight_years:
        avg_weight = statistics.mean(weight_years)
        rev_method_a = ytd_revenue / avg_weight
    else:
        rev_method_a = ytd_revenue / n_months * 12  # fallback

    # --- 方法B：YoY 衰減 + 均值回歸 ---
    prev_ytd = sum(rev_map.get((west_year - 1, m), 0) for m in range(1, n_months + 1))
    prev_annual = sum(rev_map.get((west_year - 1, m), 0) for m in range(1, 13))

    if prev_ytd > 0 and prev_annual > 0:
        ytd_growth = ytd_revenue / prev_ytd - 1

        # 過去 3 年平均年成長率
        hist_growths = []
        for i in range(len(ann_rows) - 1):
            r1, r2 = ann_rows[i], ann_rows[i + 1]
            if r1.get('revenue') and r2.get('revenue') and r2['revenue'] > 0:
                hist_growths.append(r1['revenue'] / r2['revenue'] - 1)
        hist_avg_growth = statistics.mean(hist_growths) if hist_growths else 0

        # 衰減係數
        decay = {1: 0.6, 2: 0.6, 3: 0.6, 4: 0.75, 5: 0.75, 6: 0.75,
                 7: 0.85, 8: 0.85, 9: 0.85, 10: 0.95, 11: 0.95, 12: 0.95}
        d = decay.get(n_months, 0.8)

        # 均值回歸：0.7 × YTD + 0.3 × 歷史
        adj_growth = (ytd_growth * 0.7 + hist_avg_growth * 0.3) * d
        # 上下限 -30% ~ +30%
        adj_growth = max(-0.30, min(adj_growth, 0.30))

        rev_method_b = prev_annual * (1 + adj_growth)
    else:
        rev_method_b = rev_method_a  # fallback

    # --- 取保守值 + 信心 ---
    if rev_method_a > 0 and rev_method_b > 0:
        gap_pct = abs(rev_method_a - rev_method_b) / min(rev_method_a, rev_method_b) * 100
        est_revenue = min(rev_method_a, rev_method_b)
        rev_confidence = 'A' if gap_pct < 10 else ('B' if gap_pct < 25 else 'C')
    else:
        est_revenue = rev_method_a
        rev_confidence = 'C'

    # 累計營收成長率
    rev_growth_pct = (ytd_revenue / prev_ytd - 1) * 100 if prev_ytd > 0 else None
    adj_growth_pct = (est_revenue / prev_annual - 1) * 100 if prev_annual > 0 else None

    # 單月 YoY 異常檢測
    anomaly = False
    monthly_yoys = []
    for m, rev in ytd_months:
        prev_m = rev_map.get((west_year - 1, m))
        if prev_m and prev_m > 0:
            monthly_yoys.append((rev / prev_m - 1) * 100)
    if len(monthly_yoys) >= 1:
        # 近 24 月 YoY 標準差
        all_yoys = []
        for rm in rev_rows:
            prev_r = rev_map.get((rm['year'] - 1, rm['month']))
            if prev_r and prev_r > 0:
                all_yoys.append((rm['revenue'] / prev_r - 1) * 100)
                if len(all_yoys) >= 24:
                    break
        if len(all_yoys) >= 6:
            yoy_mean = statistics.mean(all_yoys)
            yoy_sd = statistics.stdev(all_yoys)
            for my in monthly_yoys:
                if abs(my - yoy_mean) > yoy_sd * 2:
                    anomaly = True
                    break

    # === Step 2: 毛利率（年度×0.7 + 季度×0.3） ===
    gm_list = []
    for r in q_rows[:4]:
        if r.get('revenue') and r['revenue'] > 0 and r.get('gross_profit') is not None:
            gm_list.append(r['gross_profit'] / r['revenue'])
    if len(gm_list) >= 2:
        weights = [0.4, 0.3, 0.2, 0.1][:len(gm_list)]
        wsum = sum(weights)
        gm_q = sum(gm_list[i] * weights[i] for i in range(len(weights))) / wsum
        ann_gm = None
        for ar in ann_rows:
            if ar.get('revenue') and ar['revenue'] > 0 and ar.get('gross_profit') is not None:
                ann_gm = ar['gross_profit'] / ar['revenue']
                break
        est_gm = gm_q * 0.3 + ann_gm * 0.7 if ann_gm is not None else gm_q
    elif gm_list:
        est_gm = gm_list[0]
    else:
        return {"error": "無毛利率資料"}

    est_gross_profit = est_revenue * est_gm

    # === Step 3: 營業費用（年度×0.7 + 季度×0.3 錨定） ===
    opex_data = [(r['operating_expense'], r['revenue'])
                 for r in ann_rows
                 if r.get('operating_expense') is not None
                 and r.get('revenue') and r['revenue'] > 0]

    if len(opex_data) >= 1:
        # 年度費用率（近年加權）
        ann_opex_rates = [opex_data[i][0] / opex_data[i][1] for i in range(min(3, len(opex_data)))]
        w_a = [0.5, 0.3, 0.2][:len(ann_opex_rates)]
        ann_rate = sum(ann_opex_rates[i] * w_a[i] for i in range(len(w_a))) / sum(w_a)

        # 季度費用率
        q_opex_data = [(r['operating_expense'], r['revenue'])
                       for r in q_rows[:4]
                       if r.get('operating_expense') is not None
                       and r.get('revenue') and r['revenue'] > 0]
        if q_opex_data:
            q_rates = [q_opex_data[i][0] / q_opex_data[i][1] for i in range(min(4, len(q_opex_data)))]
            w_q = [0.4, 0.3, 0.2, 0.1][:len(q_rates)]
            q_rate = sum(q_rates[i] * w_q[i] for i in range(len(w_q))) / sum(w_q)
        else:
            q_rate = ann_rate

        est_opex = (ann_rate * 0.7 + q_rate * 0.3) * est_revenue
    elif opex_data:
        est_opex = opex_data[0][0]
    else:
        est_opex = est_gross_profit * 0.3

    est_oi = est_gross_profit - est_opex

    # === Step 4: 業外（年度加權×0.7 + 季度加權×0.3） ===
    nonop_ann = [r['non_operating'] for r in ann_rows if r.get('non_operating') is not None]
    nonop_q = [r['non_operating'] for r in q_rows[:4] if r.get('non_operating') is not None]
    nonop_level = 'stable'
    if nonop_ann:
        # 年度加權（近年權重高）
        w_ann = [0.4, 0.3, 0.2, 0.1][:len(nonop_ann)]
        ann_avg = sum(nonop_ann[i] * w_ann[i] for i in range(len(w_ann))) / sum(w_ann)

        # 季度加權 × 4 → 年化
        if nonop_q:
            w_q = [0.4, 0.3, 0.2, 0.1][:len(nonop_q)]
            q_annual = sum(nonop_q[i] * w_q[i] for i in range(len(w_q))) / sum(w_q) * 4
        else:
            q_annual = ann_avg

        est_nonop = ann_avg * 0.7 + q_annual * 0.3

        # 穩定度判斷（信心等級用）
        if len(nonop_ann) >= 3:
            avg = statistics.mean(nonop_ann)
            sd = statistics.stdev(nonop_ann)
            cv = sd / abs(avg) if abs(avg) > 0 else 0
            if cv >= 1.0: nonop_level = 'volatile'
            elif cv >= 0.5: nonop_level = 'moderate'
    elif nonop_q:
        est_nonop = sum(nonop_q) / len(nonop_q) * 4
    else:
        est_nonop = 0

    est_pti = est_oi + est_nonop

    # === Step 5: 稅率（年度×0.7 + 季度累計×0.3，含反算） ===
    tax_sum = pti_sum = 0
    for r in q_rows[:4]:
        pti = r.get('pretax_income')
        if pti and pti > 0:
            nip_r = r.get('net_income_parent')
            tax = r.get('tax')
            if nip_r is not None:
                calc_tax = pti - nip_r
                if tax is None or (tax == 0 and abs(calc_tax) > 100):
                    tax = calc_tax
                if abs(pti - nip_r) < 1 and pti > 1000000:
                    tax = pti * 0.20
            if tax is not None:
                pti_sum += pti; tax_sum += tax
    q_tax_rate = tax_sum / pti_sum if pti_sum > 0 else 0.20
    ann_tax_rate = None
    for ar in ann_rows:
        ar_pti = ar.get('pretax_income')
        ar_ni = ar.get('net_income') or ar.get('net_income_parent')
        ar_tax = ar.get('tax')
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
        est_tax_rate = max(0.05, min(ann_tax_rate * 0.7 + q_tax_rate * 0.3, 0.40))
    else:
        est_tax_rate = max(0.05, min(q_tax_rate, 0.40))
    est_ni = est_pti * (1 - est_tax_rate) if est_pti > 0 else est_pti * 0.80

    # === Step 6: 歸屬母公司權重 ===
    pw_list = []
    for r in q_rows[:4]:
        ci = r.get('continuing_income') or r.get('net_income_parent')
        nip = r.get('net_income_parent')
        if ci and ci != 0 and nip is not None:
            pw = nip / ci
            if 0.3 <= pw <= 1.1:
                pw_list.append(pw)
    est_pw = statistics.mean(pw_list) if pw_list else 1.0
    est_nip = est_ni * est_pw

    # === Step 7: 股數 ===
    est_shares = None
    for r in q_rows[:4]:
        if r.get('eps') and r['eps'] != 0 and r.get('net_income_parent') is not None:
            est_shares = r['net_income_parent'] / r['eps']
            break
    if not est_shares:
        return {"error": "無法計算股數"}

    est_eps = round(est_nip / est_shares, 2)

    # EPS-本業 / EPS-業外
    est_eps_core = round(est_oi * (1 - est_tax_rate) / est_shares, 2) if est_shares else None
    est_eps_nonop = round(est_eps - est_eps_core, 2) if est_eps_core is not None else None

    # === 信心等級 ===
    issues = []
    if anomaly: issues.append("單月YoY異常")
    if nonop_level == 'volatile': issues.append("業外高波動")
    elif nonop_level == 'moderate': issues.append("業外中波動")
    if rev_confidence == 'C': issues.append("兩法差距大")
    if n_months <= 3: issues.append("僅" + str(n_months) + "月資料")
    # 控股型偵測：業外佔比 > 200%
    if est_oi != 0 and abs(est_nonop / est_oi) > 2:
        issues.append("業外主導型")

    confidence = rev_confidence
    if anomaly and confidence == 'A':
        confidence = 'B'
    if est_oi != 0 and abs(est_nonop / est_oi) > 2 and confidence == 'A':
        confidence = 'B'

    # 去年 EPS
    last_year_eps = ann_rows[0].get('eps') if ann_rows else None

    # === 配息率估算（近 5 年加權，排除 EPS ≤ 0） ===
    payout_data = []
    for r in ann_rows:
        r_eps = r.get('eps')
        r_div = r.get('cash_dividend') or 0
        if r_eps and r_eps > 0:
            pr = min(r_div / r_eps, 1.0)  # 上限 100%
            payout_data.append(pr)

    if payout_data:
        weights = [0.35, 0.25, 0.20, 0.12, 0.08][:len(payout_data)]
        wsum = sum(weights)
        est_payout = sum(payout_data[i] * weights[i] for i in range(len(weights))) / wsum
    else:
        est_payout = None

    # 系統預估股利 & 本益比 & 殖利率
    est_div = round(est_eps * est_payout, 2) if est_payout is not None and est_eps > 0 else None

    # 取目前股價
    try:
        conn2 = sqlite3.connect(DB_PATH)
        conn2.row_factory = sqlite3.Row
        price_row = conn2.execute("SELECT close FROM stocks WHERE code = ?", (code,)).fetchone()
        conn2.close()
        cur_price = price_row['close'] if price_row and price_row['close'] else None
    except Exception as e:
        cur_price = None

    est_pe = round(cur_price / est_eps, 2) if cur_price and est_eps and est_eps > 0 else None
    est_yld = round(est_div / cur_price * 100, 2) if cur_price and est_div and cur_price > 0 else None

    # === 系統價值評估矩陣 ===
    # 優先使用個股自訂參數，沒有才用預設值
    _user_pe_low, _user_pe_high, _user_yld_high, _user_yld_max = 10, 18, 5.5, 6.0
    try:
        ue_conn = sqlite3.connect(DB_PATH)
        ue_conn.row_factory = sqlite3.Row
        ue_row = ue_conn.execute("SELECT params FROM user_estimates WHERE code=?", (code,)).fetchone()
        ue_conn.close()
        if ue_row and ue_row['params']:
            import json as _json
            _ue = _json.loads(ue_row['params'])
            if _ue.get('peHigh'): _user_pe_high = float(_ue['peHigh'])
            if _ue.get('peLow'): _user_pe_low = float(_ue['peLow'])
            if _ue.get('yldHigh'): _user_yld_high = float(_ue['yldHigh'])
            if _ue.get('yldMax'): _user_yld_max = float(_ue['yldMax'])
    except Exception: pass

    val = {}
    if est_eps and est_eps > 0 and est_div is not None:
        pe_low = _user_pe_low
        pe_high = _user_pe_high
        pe_fair = (pe_high + pe_low) / 2
        pe_below = (pe_fair + pe_low) / 2
        yld_high, yld_max = _user_yld_high, _user_yld_max

        # 加權股利（用近 5 年加權平均股利）
        wt_div_data = []
        for r in ann_rows:
            d = (r.get('cash_dividend') or 0) + (r.get('stock_dividend') or 0)
            if d > 0:
                wt_div_data.append(d)
        if wt_div_data:
            ww = [0.35, 0.25, 0.20, 0.12, 0.08][:len(wt_div_data)]
            wt_div = sum(wt_div_data[i] * ww[i] for i in range(len(ww))) / sum(ww)
        else:
            wt_div = est_div if est_div else 0

        lt_yld = 6.0
        lt6_val = round(wt_div / lt_yld * 100 + est_div, 2) if wt_div > 0 else None

        val['aa'] = round(min(est_eps * pe_low, est_div / yld_max * 100, lt6_val or 9999), 2)
        val['a1'] = round(min(est_eps * pe_low, est_div / yld_high * 100, lt6_val or 9999), 2)
        val['a2'] = round(min(est_eps * pe_below, est_div / yld_max * 100, lt6_val or 9999), 2)
        val['a']  = round(min(est_eps * pe_below, est_div / yld_high * 100, lt6_val or 9999), 2)
        val['lt6'] = lt6_val

    return {
        "target": f"{roc_year}年度",
        "est_eps": est_eps,
        "confidence": confidence,
        "details": {
            "n_months": n_months,
            "ytd_revenue": round(ytd_revenue),
            "rev_growth_pct": round(rev_growth_pct, 2) if rev_growth_pct is not None else None,
            "adj_growth_pct": round(adj_growth_pct, 2) if adj_growth_pct is not None else None,
            "rev_method_a": round(rev_method_a),
            "rev_method_b": round(rev_method_b),
            "est_revenue": round(est_revenue),
            "est_gm": round(est_gm * 100, 2),
            "est_gross_profit": round(est_gross_profit),
            "est_opex": round(est_opex),
            "est_oi": round(est_oi),
            "est_nonop": round(est_nonop),
            "est_pti": round(est_pti),
            "est_tax_rate": round(est_tax_rate * 100, 2),
            "est_ni": round(est_ni),
            "est_pw": round(est_pw * 100, 2),
            "est_nip": round(est_nip),
            "est_shares": round(est_shares),
            "est_eps_core": est_eps_core,
            "est_eps_nonop": est_eps_nonop,
            "last_year_eps": last_year_eps,
            "anomaly": anomaly,
            "issues": issues,
            "est_payout": round(est_payout * 100, 2) if est_payout is not None else None,
            "est_div": est_div,
            "est_pe": est_pe,
            "est_yld": est_yld,
            "cur_price": cur_price,
            "val": val,
        }
    }


def _init_eps_log_db():
    """建立估算日誌表"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS system_eps_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            code            TEXT NOT NULL,
            est_date        TEXT NOT NULL,
            target_period   TEXT NOT NULL,
            months_used     INTEGER,
            rev_growth      REAL,
            est_revenue     REAL,
            est_gm          REAL,
            est_opex        REAL,
            est_nonop       REAL,
            est_tax_rate    REAL,
            est_eps         REAL,
            confidence      TEXT,
            method_version  TEXT DEFAULT 'v1'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS system_eps_actual (
            code            TEXT NOT NULL,
            target_period   TEXT NOT NULL,
            actual_revenue  REAL,
            actual_eps      REAL,
            report_date     TEXT,
            PRIMARY KEY (code, target_period)
        )
    """)
    conn.commit()
    conn.close()


def _log_estimate(code, result, est_type='annual'):
    """將估算結果寫入日誌（新增不覆蓋）"""
    _init_eps_log_db()
    if 'error' in result:
        return
    d = result['details']
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO system_eps_log
        (code, est_date, target_period, months_used, rev_growth,
         est_revenue, est_gm, est_opex, est_nonop, est_tax_rate,
         est_eps, confidence, method_version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        code,
        datetime.now().strftime('%Y-%m-%d'),
        result.get('target') or result.get('quarter', ''),
        d.get('n_months') or d.get('rev_actual', 0),
        d.get('adj_growth_pct') or d.get('rev_growth_pct'),
        d.get('est_revenue'),
        d.get('est_gm'),
        d.get('est_opex'),
        d.get('est_nonop'),
        d.get('est_tax_rate'),
        result.get('est_eps'),
        result.get('confidence'),
        'v1'
    ))
    conn.commit()
    conn.close()

