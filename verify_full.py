"""
verify_full.py — 全欄位覆蓋率驗證
掃描 stocks 表（總表）+ financial_annual（年報）+ quarterly_financial（季報）
+ pe_history + monthly_revenue 的每個欄位覆蓋率
"""
import sqlite3
import os
from datetime import date

DB_PATH = os.path.join(os.path.dirname(__file__), 'stocks.db')
REPORT_PATH = os.path.join(os.path.dirname(__file__), 'verify_full_report.txt')


def run():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    report = []

    cur_year = date.today().year
    last_year = cur_year - 1
    roc_year = cur_year - 1911
    roc_last = roc_year - 1

    report.append("=" * 70)
    report.append("逍遙投資系統 — 全欄位覆蓋率驗證報告")
    report.append(f"日期：{date.today()}")
    report.append("=" * 70)

    total = conn.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    report.append(f"\n股票總數：{total}\n")

    # ══════════════════════════════════════════════════════
    # 1. stocks 表（總表）
    # ══════════════════════════════════════════════════════
    report.append("=" * 70)
    report.append("【1】stocks 表（總表）")
    report.append("=" * 70)

    stock_groups = {
        '基本資料': ['code', 'name', 'market', 'industry'],
        '股價': ['close', 'change', 'open', 'high', 'low', 'volume'],
        '營收': ['revenue_date', 'revenue_year', 'revenue_month', 'revenue_yoy', 'revenue_mom', 'revenue_cum_yoy'],
        '季度EPS': ['eps_date', 'eps_1', 'eps_1q', 'eps_2', 'eps_2q', 'eps_3', 'eps_3q', 'eps_4', 'eps_4q', 'eps_5', 'eps_5q'],
        '年度EPS': ['eps_y1', 'eps_y1_label', 'eps_y2', 'eps_y2_label', 'eps_y3', 'eps_y3_label',
                    'eps_y4', 'eps_y4_label', 'eps_y5', 'eps_y5_label', 'eps_y6', 'eps_y6_label'],
        '累計EPS': ['eps_ytd', 'eps_ytd_label'],
        '股利': ['div_c1', 'div_s1', 'div_1_label', 'div_c2', 'div_2_label', 'div_c3', 'div_3_label',
                 'div_c4', 'div_4_label', 'div_c5', 'div_5_label', 'div_c6', 'div_6_label'],
        '法人': ['inst_foreign', 'inst_trust', 'inst_dealer'],
        '財務等級': ['fin_grade_1', 'fin_grade_1y', 'fin_grade_2', 'fin_grade_2y',
                    'fin_grade_3', 'fin_grade_3y'],
        '合約負債': ['contract_1', 'contract_1q', 'contract_2', 'contract_2q'],
        '240日漲跌': ['change_240d'],
        '系統估算': ['sys_ann_eps', 'sys_ann_pe', 'sys_ann_yld', 'sys_ann_div', 'sys_ann_confidence'],
    }

    # 先取得所有實際欄位
    all_cols = [r[1] for r in conn.execute("PRAGMA table_info(stocks)").fetchall()]

    for group, cols in stock_groups.items():
        report.append(f"\n  --- {group} ---")
        for col in cols:
            if col not in all_cols:
                report.append(f"    {col}: [欄位不存在]")
                continue
            cnt = conn.execute(f"SELECT COUNT(*) FROM stocks WHERE [{col}] IS NOT NULL").fetchone()[0]
            pct = cnt / total * 100
            status = "OK" if pct >= 95 else ("注意" if pct >= 80 else "缺漏")
            missing = total - cnt
            line = f"    {col}: {cnt}/{total} ({pct:.1f}%)"
            if missing > 0 and missing <= 20:
                codes = [r[0] for r in conn.execute(f"SELECT code FROM stocks WHERE [{col}] IS NULL ORDER BY code").fetchall()]
                line += f"  缺: {','.join(codes)}"
            elif missing > 20:
                line += f"  缺 {missing} 支"
            if status != "OK":
                line += f"  [{status}]"
            report.append(line)

    # ══════════════════════════════════════════════════════
    # 2. financial_annual（年報）
    # ══════════════════════════════════════════════════════
    report.append(f"\n{'='*70}")
    report.append("【2】financial_annual 年報（最近3年）")
    report.append("=" * 70)

    fa_cols = [r[1] for r in conn.execute("PRAGMA table_info(financial_annual)").fetchall()]
    check_years = [cur_year - 1, cur_year - 2, cur_year - 3]  # 最近3個完整年度

    for yr in check_years:
        fa_total = conn.execute("SELECT COUNT(*) FROM financial_annual WHERE year=?", (yr,)).fetchone()[0]
        report.append(f"\n  --- {yr}年（民國{yr-1911}年）共 {fa_total} 支 ---")

        key_cols = ['revenue', 'cost', 'gross_profit', 'operating_expense', 'operating_income',
                    'non_operating', 'pretax_income', 'net_income', 'eps',
                    'total_assets', 'total_equity', 'common_stock',
                    'operating_cf', 'capex', 'cash_dividend', 'stock_dividend']
        for col in key_cols:
            if col not in fa_cols:
                continue
            cnt = conn.execute(f"SELECT COUNT(*) FROM financial_annual WHERE year=? AND [{col}] IS NOT NULL", (yr,)).fetchone()[0]
            pct = cnt / fa_total * 100 if fa_total > 0 else 0
            status = "OK" if pct >= 95 else ("注意" if pct >= 80 else "缺漏")
            line = f"    {col}: {cnt}/{fa_total} ({pct:.1f}%)"
            if status != "OK":
                line += f"  [{status}]"
            report.append(line)

    # ══════════════════════════════════════════════════════
    # 3. quarterly_financial（季報，最近4季）
    # ══════════════════════════════════════════════════════
    report.append(f"\n{'='*70}")
    report.append("【3】quarterly_financial 季報（最近4季）")
    report.append("=" * 70)

    # 找最近4個季度
    latest_q = conn.execute("""
        SELECT DISTINCT quarter FROM quarterly_financial
        ORDER BY CAST(SUBSTR(quarter,1,INSTR(quarter,'Q')-1) AS INTEGER) DESC,
                 CAST(SUBSTR(quarter,INSTR(quarter,'Q')+1) AS INTEGER) DESC
        LIMIT 4
    """).fetchall()
    latest_quarters = [r[0] for r in latest_q]

    qf_cols = [r[1] for r in conn.execute("PRAGMA table_info(quarterly_financial)").fetchall()]
    key_qcols = ['revenue', 'cost', 'gross_profit', 'operating_expense', 'operating_income',
                 'non_operating', 'pretax_income', 'net_income_parent', 'eps',
                 'contract_liability', 'inventory']

    for q in latest_quarters:
        q_total = conn.execute("SELECT COUNT(*) FROM quarterly_financial WHERE quarter=?", (q,)).fetchone()[0]
        report.append(f"\n  --- {q} 共 {q_total} 支 ---")
        for col in key_qcols:
            if col not in qf_cols:
                report.append(f"    {col}: [欄位不存在]")
                continue
            cnt = conn.execute(f"SELECT COUNT(*) FROM quarterly_financial WHERE quarter=? AND [{col}] IS NOT NULL", (q,)).fetchone()[0]
            pct = cnt / q_total * 100 if q_total > 0 else 0
            status = "OK" if pct >= 95 else ("注意" if pct >= 80 else "缺漏")
            line = f"    {col}: {cnt}/{q_total} ({pct:.1f}%)"
            if status != "OK":
                line += f"  [{status}]"
            report.append(line)

    # ══════════════════════════════════════════════════════
    # 4. pe_history（本益比歷史）
    # ══════════════════════════════════════════════════════
    report.append(f"\n{'='*70}")
    report.append("【4】pe_history 歷史本益比")
    report.append("=" * 70)

    pe_stocks = conn.execute("SELECT COUNT(DISTINCT code) FROM pe_history").fetchone()[0]
    pe_total = conn.execute("SELECT COUNT(*) FROM pe_history").fetchone()[0]
    pe_null_high = conn.execute("SELECT COUNT(*) FROM pe_history WHERE pe_high IS NULL").fetchone()[0]
    pe_null_low = conn.execute("SELECT COUNT(*) FROM pe_history WHERE pe_low IS NULL").fetchone()[0]
    report.append(f"  股票數: {pe_stocks}")
    report.append(f"  總筆數: {pe_total}")
    report.append(f"  pe_high 為空: {pe_null_high} 筆")
    report.append(f"  pe_low 為空: {pe_null_low} 筆")

    # 沒有 PE 歷史的公司
    no_pe = conn.execute("""
        SELECT COUNT(*) FROM stocks
        WHERE code NOT IN (SELECT DISTINCT code FROM pe_history)
        AND close IS NOT NULL
    """).fetchone()[0]
    report.append(f"  有股票但無PE歷史: {no_pe} 支")

    # ══════════════════════════════════════════════════════
    # 5. monthly_revenue（月營收）
    # ══════════════════════════════════════════════════════
    report.append(f"\n{'='*70}")
    report.append("【5】monthly_revenue 月營收")
    report.append("=" * 70)

    mr_total = conn.execute("SELECT COUNT(*) FROM monthly_revenue").fetchone()[0]
    report.append(f"  總筆數: {mr_total}")

    # 當年度每月覆蓋
    for m in range(1, 13):
        cnt = conn.execute("SELECT COUNT(*) FROM monthly_revenue WHERE year=? AND month=?",
                          (cur_year, m)).fetchone()[0]
        if cnt > 0:
            report.append(f"  {cur_year}/{m}月: {cnt} 支")

    # ══════════════════════════════════════════════════════
    # 6. 本機 vs Render 比對（抽樣）
    # ══════════════════════════════════════════════════════
    report.append(f"\n{'='*70}")
    report.append("【6】跨表一致性檢查")
    report.append("=" * 70)

    # stocks.eps_1 vs quarterly_financial 最新一季 EPS
    mismatch_eps = conn.execute("""
        SELECT s.code, s.eps_1, s.eps_1q, q.eps as q_eps
        FROM stocks s
        JOIN quarterly_financial q ON s.code = q.code AND s.eps_1q = q.quarter
        WHERE s.eps_1 IS NOT NULL AND q.eps IS NOT NULL
        AND ABS(s.eps_1 - q.eps) > 0.01
        LIMIT 20
    """).fetchall()
    report.append(f"\n  stocks.eps_1 vs 季報 EPS 不一致: {len(mismatch_eps)} 支（前20）")
    for r in mismatch_eps:
        report.append(f"    {r[0]} {r[2]}: stocks={r[1]} 季報={r[3]}")

    # ══════════════════════════════════════════════════════
    # 總結
    # ══════════════════════════════════════════════════════
    report.append(f"\n{'='*70}")
    report.append("驗證完成")
    report.append("=" * 70)

    conn.close()

    report_text = '\n'.join(report)
    with open(REPORT_PATH, 'w') as f:
        f.write(report_text)
    print(report_text)
    print(f"\n報告已存到 {REPORT_PATH}")


if __name__ == '__main__':
    run()
