#!/usr/bin/env python3
"""
逍遙投資系統 — 投資報告書資料簡報產生器
產出完整的資料簡報 + 報告格式指令，複製貼給 Claude 即可寫出報告。

用法：
  python3 investment_report_engine.py 1580        # 產出資料簡報到 stdout
  python3 investment_report_engine.py 1580 --copy  # 同時複製到剪貼簿
"""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import urllib.request
from datetime import date

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'stocks.db')
RENDER_NOTES_URL = 'https://tock-system.onrender.com/api/user-notes/'
RENDER_REPORT_URL = 'https://tock-system.onrender.com/api/investment-report/'

# g 對照表：累積營收 YoY → 前瞻成長率 g
G_LOOKUP = [
    (-999, 0, 0), (0, 5, 3), (5, 10, 5), (10, 15, 8),
    (15, 20, 10), (20, 30, 15), (30, 999, 20),
]

# 檢核表項目定義
CHECKLIST_PROFIT = [
    ('roic_avg5', 'ROIC 近5年平均 >= 15%'),
    ('roic_latest', 'ROIC 最近一年 >= 15%'),
    ('roic_min5', 'ROIC 近5年最低值 >= 10%'),
    ('opm_avg5', '營益率近5年平均 >= 10%'),
    ('opm_min5', '營益率近5年最低值 >= 5%'),
    ('gm_median', '毛利率 >= 近5年中位數'),
    ('gm_q_median', '最近一季毛利率 >= 近4季中位數'),
]

CHECKLIST_SAFETY = [
    ('debt_ratio_ok', '負債比 <= 50%'),
    ('fin_debt_ok', '金融負債比 < 30%'),
    ('icr_ok', '利息保障倍數 > 5'),
    ('fcf_freq', 'FCF近5年至少3年為正'),
    ('fcf_no_consec', 'FCF近2年不得連續為負'),
    ('fcf_sum_pos', 'FCF近5年加總為正'),
    ('inv_level', '存貨水準 <= 近5年平均x1.2'),
    ('inv_trend', '存貨方向：最近一季 <= 近4季中位數x1.15'),
    ('ar_level', '應收水準 <= 近5年平均x1.2'),
    ('ar_trend', '應收方向：最近一季 <= 近4季中位數x1.15'),
]

CHECKLIST_VALUE = [
    ('grade_a_ok', '預估(沈董)等級為A級以上'),
    ('eps_vs_median5', '預估(沈董)EPS >= 近5年EPS中位數'),
    ('core_ratio', '累計營業利益/累計稅前淨利 > 70%'),
    ('price_val_ok', '現價 <= A級評價；<= AA更佳'),
    ('ge_neff_ratio', '前瞻Neff比率 >= 1.0'),
]

CHECKLIST_GROWTH = [
    ('cum_rev_pos', '累積營收年增率 >= 0%'),
    ('rev_12m_pos', '12M營收年增率 >= 0%'),
    ('rev_3m_pos', '3M營收年增率 >= 0%'),
    ('rev_3m_gt_12m', '短期3M >= 長期12M'),
]


def _cum_yoy_to_g(cum_yoy):
    """累積營收 YoY 轉前瞻 g（對照表）"""
    if cum_yoy is None:
        return 0
    for lo, hi, g in G_LOOKUP:
        if lo <= cum_yoy < hi:
            return g
    return 0


def generate_briefing(code):
    # ── 從 Render 拉最新 user_estimates + recalc（確保前台設定即時反映）──
    try:
        from render_sync import _pull_user_estimates_from_render
        _pull_user_estimates_from_render()
        from app import recalc_all_derived
        recalc_all_derived(codes=[code])
    except Exception as e:
        print(f"[報告引擎] 同步 user_estimates 失敗（使用本機現有值）: {e}", file=sys.stderr)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # ── 基本資料 ──
    r = conn.execute("SELECT * FROM stocks WHERE code=?", (code,)).fetchone()
    if not r:
        print(f"找不到 {code}")
        return None
    r = dict(r)

    # ── 檢核表 ──
    sc_row = conn.execute("SELECT * FROM stock_checklist WHERE code=?", (code,)).fetchone()
    sc = dict(sc_row) if sc_row else {}
    detail = json.loads(sc.get('detail', '{}') or '{}')

    # ── 年度財報 6 年 ──
    fa_rows = conn.execute("""
        SELECT year, revenue, cost, gross_profit, operating_expense, operating_income,
               net_income, eps, cash_dividend, roic, debt_ratio, fin_debt_ratio,
               operating_cf, capex, fcf, earnings_quality, interest_expense, interest_coverage,
               total_equity, inventory, inventory_days, accounts_receivable, ar_days
        FROM financial_annual WHERE code=? ORDER BY year DESC LIMIT 6
    """, (code,)).fetchall()
    fa = [dict(x) for x in fa_rows]

    # ── 季度損益 ──
    qf_rows = conn.execute("""
        SELECT quarter, eps, revenue, gross_profit, operating_expense, operating_income,
               eps_core, eps_nonop
        FROM quarterly_financial WHERE code=?
        ORDER BY CAST(REPLACE(REPLACE(quarter,'Q','.'),'q','.') AS REAL) DESC LIMIT 10
    """, (code,)).fetchall()

    # ── PE 歷史 ──
    pe_rows = conn.execute("""
        SELECT year, pe_high, pe_low FROM pe_history
        WHERE code=? ORDER BY year DESC LIMIT 6
    """, (code,)).fetchall()

    # ── 使用者預估 ──
    ue_row = conn.execute("SELECT * FROM user_estimates WHERE code=?", (code,)).fetchone()
    ue = dict(ue_row) if ue_row else {}

    # ── 質性筆記（從 Render）──
    notes = ''
    try:
        req = urllib.request.Request(RENDER_NOTES_URL + code)
        resp = urllib.request.urlopen(req, timeout=10)
        notes = json.loads(resp.read()).get('content', '')
    except Exception:
        pass

    conn.close()

    # ── 跑選股引擎取分類與判斷 ──
    engine_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'stock_picks_engine.py')
    try:
        result = subprocess.run(['python3', engine_path, '--check', code],
                                capture_output=True, text=True, timeout=30)
        engine_output = result.stdout
    except Exception:
        engine_output = '（引擎執行失敗）'

    # ══════════════════════════════════════════
    # 組裝資料簡報
    # ══════════════════════════════════════════
    lines = []
    lines.append(f"請根據以下資料簡報，為 {code} {r.get('name','')} 產出投資報告書。\n")
    lines.append("=" * 60)
    lines.append(f"資料簡報：{code} {r.get('name','')}　分析日期：{date.today().isoformat()}")
    lines.append("=" * 60)

    # ── 總覽 ──
    lines.append(f"\n【總覽】")
    lines.append(f"股價：{r.get('close')}　產業：{r.get('industry','')}　日均量：{r.get('volume',0):,.0f}")
    lines.append(f"便宜天數：{r.get('val_cheap_days',0)}")

    # ── 選股引擎判斷 ──
    lines.append(f"\n【選股引擎判斷】")
    lines.append(engine_output.strip())

    # ── 財務等級 6 年 ──
    lines.append(f"\n【財務等級（6年）】")
    grade_line = "  "
    for i in range(6, 0, -1):
        y = r.get(f'fin_grade_{i}y', '')
        g = r.get(f'fin_grade_{i}', '')
        grade_line += f"{y}:{g}　"
    lines.append(grade_line.strip())

    # ── 評價門檻 ──
    lines.append(f"\n【評價門檻】")
    lines.append(f"  AA：{r.get('val_aa')}　A1：{r.get('val_a1')}　A2：{r.get('val_a2')}　A：{r.get('val_a')}")

    # ══════════════════════════════════════════
    # 一、獲利面
    # ══════════════════════════════════════════
    lines.append(f"\n{'=' * 60}")
    lines.append("【一、獲利面 — 數據表】")
    lines.append("=" * 60)

    # 獲利面表格：營收 / 毛利率 / 營業費用率 / 營益率 / ROIC / EPS
    lines.append(f"  {'年度':>6} {'營收(百萬)':>10} {'YoY':>7} {'毛利率':>7} {'費用率':>7} {'營益率':>7} {'ROIC':>7} {'EPS':>7}")
    fa_sorted = list(reversed(fa))  # 由舊到新
    for i, row in enumerate(fa_sorted):
        rev = row.get('revenue')
        gp = row.get('gross_profit')
        oe = row.get('operating_expense')
        oi = row.get('operating_income')
        gm = f"{gp/rev*100:.1f}%" if gp and rev and rev > 0 else '—'
        # 營業費用率 = 營業費用 / 營收
        if oe and rev and rev > 0:
            exp_rate = f"{oe/rev*100:.1f}%"
        elif gp is not None and oi is not None and rev and rev > 0:
            exp_rate = f"{(gp-oi)/rev*100:.1f}%"
        else:
            exp_rate = '—'
        opm = f"{oi/rev*100:.1f}%" if oi and rev and rev > 0 else '—'
        roic = f"{row['roic']:.1f}%" if row.get('roic') else '—'
        rev_s = f"{rev/1e6:>10.0f}" if rev else '         —'
        eps_s = f"{row.get('eps','—'):>7}"
        # YoY
        if i > 0 and rev and fa_sorted[i-1].get('revenue') and fa_sorted[i-1]['revenue'] > 0:
            yoy = (rev - fa_sorted[i-1]['revenue']) / fa_sorted[i-1]['revenue'] * 100
            yoy_s = f"{yoy:>+6.1f}%"
        else:
            yoy_s = '      —'
        lines.append(f"  {row['year']:>6} {rev_s} {yoy_s} {gm:>7} {exp_rate:>7} {opm:>7} {roic:>7} {eps_s}")

    # 獲利面表格二：季度毛利率 / 營益率
    lines.append(f"\n  --- 季度毛利率 / 營益率 ---")
    lines.append(f"  {'季度':>8} {'營收(百萬)':>10} {'毛利率':>7} {'費用率':>7} {'營益率':>7}")
    for row in reversed(list(qf_rows)):
        rev = row['revenue']
        gp = row['gross_profit']
        oe = row['operating_expense']
        oi = row['operating_income']
        rev_s = f"{rev/1e6:>10.0f}" if rev else '         —'
        gm = f"{gp/rev*100:.1f}%" if gp and rev and rev > 0 else '     —'
        if oe and rev and rev > 0:
            exp_r = f"{oe/rev*100:.1f}%"
        elif gp is not None and oi is not None and rev and rev > 0:
            exp_r = f"{(gp-oi)/rev*100:.1f}%"
        else:
            exp_r = '     —'
        opm = f"{oi/rev*100:.1f}%" if oi and rev and rev > 0 else '     —'
        lines.append(f"  {row['quarter']:>8} {rev_s} {gm:>7} {exp_r:>7} {opm:>7}")

    # ══════════════════════════════════════════
    # 二、安全面
    # ══════════════════════════════════════════
    lines.append(f"\n{'=' * 60}")
    lines.append("【二、安全面 — 數據表】")
    lines.append("=" * 60)

    lines.append(f"  {'年度':>6} {'負債比':>7} {'金融負債':>8} {'ICR':>7} {'FCF(百萬)':>10} {'盈餘品質':>8} {'存貨天數':>8} {'應收天數':>8}")
    for row in fa_sorted:
        debt = f"{row['debt_ratio']:.1f}%" if row.get('debt_ratio') else '—'
        fin_debt = f"{row['fin_debt_ratio']:.1f}%" if row.get('fin_debt_ratio') else '—'
        icr_val = row.get('interest_coverage')
        icr = f"{icr_val:.1f}x" if icr_val else '—'
        fcf_val = row.get('fcf')
        if fcf_val is None:
            ocf = row.get('operating_cf') or 0
            cap = row.get('capex') or 0
            fcf_val = ocf + cap
        fcf_s = f"{fcf_val/1e6:>10.0f}" if fcf_val else '         —'
        eq = f"{row['earnings_quality']:.1f}%" if row.get('earnings_quality') else '—'
        inv_d = f"{row['inventory_days']:.0f}" if row.get('inventory_days') else '—'
        ar_d = f"{row['ar_days']:.0f}" if row.get('ar_days') else '—'
        lines.append(f"  {row['year']:>6} {debt:>7} {fin_debt:>8} {icr:>7} {fcf_s} {eq:>8} {inv_d:>8} {ar_d:>8}")

    # ══════════════════════════════════════════
    # 三、價值面
    # ══════════════════════════════════════════
    lines.append(f"\n{'=' * 60}")
    lines.append("【三、價值面 — 預估EPS與前瞻Neff】")
    lines.append("=" * 60)

    # 價值面表格一：歷年EPS（由舊到新）
    lines.append(f"\n  --- 歷年EPS ---")
    lines.append(f"  {'年度':>6} {'EPS':>7}")
    for row in fa_sorted:
        eps_s = f"{row.get('eps','—'):>7}"
        lines.append(f"  {row['year']:>6} {eps_s}")

    # 預估 EPS 設定
    lines.append(f"\n  --- 預估EPS設定 ---")
    est_eps = r.get('est_eps')
    shen_eps = r.get('shen_eps')
    blend_eps = r.get('blend_eps')
    sys_ann_eps = r.get('sys_ann_eps')
    lines.append(f"  使用者設定預估EPS：{est_eps}")
    lines.append(f"  近4季EPS合計：{r.get('eps_4q_sum')}")
    lines.append(f"  （參考）沈董EPS：{shen_eps}　綜合EPS：{blend_eps}　系統估算：{sys_ann_eps}（信心{r.get('sys_ann_confidence','')}）")

    # 價值面表格二：季度EPS明細（預估EPS計算依據）
    lines.append(f"\n  --- 季度EPS明細 ---")
    lines.append(f"  {'季度':>8} {'EPS':>7} {'本業EPS':>8} {'業外EPS':>8}")
    for row in reversed(list(qf_rows)):
        ec = row['eps_core']
        en = row['eps_nonop']
        ec_s = f"{ec:>8.2f}" if ec is not None else '       —'
        en_s = f"{en:>8.2f}" if en is not None else '       —'
        lines.append(f"  {row['quarter']:>8} {row['eps']:>7} {ec_s} {en_s}")

    # 股利
    lines.append(f"\n  --- 股利設定 ---")
    div_line = "  股利（6年）："
    for i in range(6, 0, -1):
        y = r.get(f'div_{i}_label', '')
        d = r.get(f'div_c{i}', '')
        if d is not None:
            try: d = round(float(d), 2)
            except Exception: pass
        div_line += f"{y}:{d}　"
    lines.append(div_line.strip())
    payouts = [r.get(f'payout_{i}') for i in range(1, 6) if r.get(f'payout_{i}') is not None]
    if payouts:
        lines.append(f"  配息率（5年）：{' / '.join(f'{p:.1f}%' for p in payouts)}")

    # 前瞻 Neff
    lines.append(f"\n  --- 前瞻Neff比率 ---")
    fwd_neff = r.get('fwd_neff')
    fwd_g = r.get('fwd_neff_g')
    fwd_pe = r.get('fwd_neff_pe')
    fwd_yld = r.get('fwd_neff_yld')
    cum_yoy = r.get('revenue_cum_yoy')
    lines.append(f"  累積營收YoY：{cum_yoy}%　→ 對照表 g = {fwd_g}%")
    lines.append(f"  預估本益比：{fwd_pe}　預估殖利率：{fwd_yld}%")
    lines.append(f"  前瞻Neff = ({fwd_g}% + {fwd_yld}%) / {fwd_pe} = {fwd_neff}")
    lines.append(f"  g 對照表：<0%->0 | 0~5%->3 | 5~10%->5 | 10~15%->8 | 15~20%->10 | 20~30%->15 | >30%->20")

    # PE 歷史
    lines.append(f"\n  --- PE歷史區間 ---")
    pe_highs, pe_lows = [], []
    for row in pe_rows:
        h = min(float(row['pe_high']), 20) if row['pe_high'] else None
        l = float(row['pe_low']) if row['pe_low'] else None
        lines.append(f"  {row['year']}: 高={row['pe_high']} 低={row['pe_low']}（高點以20封頂：{h}）")
        if h: pe_highs.append(h)
        if l: pe_lows.append(l)
    if pe_highs and pe_lows:
        avg_h = sum(pe_highs) / len(pe_highs)
        avg_l = sum(pe_lows) / len(pe_lows)
        mid = (avg_h + avg_l) / 2
        lines.append(f"  → 5年平均：低={avg_l:.1f} 中={mid:.1f} 高={avg_h:.1f}")

    # ══════════════════════════════════════════
    # 四、成長面
    # ══════════════════════════════════════════
    lines.append(f"\n{'=' * 60}")
    lines.append("【四、成長面】")
    lines.append("=" * 60)

    lines.append(f"  營收CAGR 3年：{sc.get('gi_rev_cagr_3y')}%　5年：{sc.get('gi_rev_cagr_5y')}%")
    lines.append(f"  累積營收YoY：{cum_yoy}%　3M：{sc.get('gi_rev_3m_yoy')}%　12M：{sc.get('gi_rev_12m_yoy')}%")
    lines.append(f"  趨勢燈號：{sc.get('growth_signal')}　紅旗：{sc.get('red_flags')}")
    lines.append(f"  PEG：{sc.get('gi_lynch_d')}{'（灰）' if sc.get('gi_lynch_gray') else ''}　Neff（舊）：{sc.get('gi_neff_d')}{'（灰）' if sc.get('gi_neff_gray') else ''}")
    lines.append(f"  存貨風險：{'有' if sc.get('growth_inv_risk') == 1 else '無'}　股本變動：{sc.get('gi_shares_change')}%")

    # 席勒指標
    lines.append(f"\n  --- 席勒PE ---")
    lines.append(f"  席勒均值EPS：{sc.get('gi_shiller_avg_eps')}　席勒PE：{sc.get('gi_shiller_pe')}　Alert：{sc.get('gi_shiller_alert')}")

    # ══════════════════════════════════════════
    # 檢核表（分類列出，帶實際值）
    # ══════════════════════════════════════════
    def _fmt_checklist(items, category_name):
        lines_out = []
        passed = sum(1 for key, _ in items if sc.get(f'chk_{key}') == 1)
        lines_out.append(f"\n【檢核表 — {category_name}（{passed}/{len(items)}）】")
        for key, label in items:
            chk = sc.get(f'chk_{key}')
            mark = 'V' if chk == 1 else 'X' if chk == 0 else '?'
            actual = detail.get(key, '')
            lines_out.append(f"  {mark} {label}")
            if actual:
                lines_out.append(f"    -> {actual}")
        return lines_out

    lines += _fmt_checklist(CHECKLIST_PROFIT, '獲利性')
    lines += _fmt_checklist(CHECKLIST_SAFETY, '安全性')
    lines += _fmt_checklist(CHECKLIST_VALUE, '價值評估')
    lines += _fmt_checklist(CHECKLIST_GROWTH, '成長性')

    # ── 質性研究筆記 ──
    lines.append(f"\n{'=' * 60}")
    lines.append("【質性研究筆記】")
    lines.append("=" * 60)
    if notes.strip():
        lines.append(notes.strip())
    else:
        lines.append("（尚未完成質性研究）")

    # ══════════════════════════════════════════
    # 報告格式指令
    # ══════════════════════════════════════════
    lines.append(f"\n{'=' * 60}")
    lines.append("【報告格式指令】")
    lines.append("=" * 60)
    lines.append("""
請根據以上資料簡報，撰寫投資報告書。嚴格遵守以下規則：

═══════════════════════════════
撰寫規則（最高優先級）
═══════════════════════════════
1. 報告中的數據與檢核結果以本次資料簡報為唯一準據。
2. 不得自行重新計算、修改或推翻檢核表的既有判定。
3. 不得補充資料簡報中沒有出現的數據；若必要資料不存在，寫「待查證」。
4. 質性筆記僅作為質性分析依據；若與本次資料簡報中的較新資料衝突，以較新的資料為準。
5. 嚴格區分「事實」與「判斷」，不得把管理層展望、產業預測或模型推估當成已發生事實。
6. 所有重要判斷都必須能回溯至本次資料簡報中的數據、檢核結果或質性筆記。
7. 不得因為報告格式要求而強行建立正面或負面投資論點。
8. 若資料不足，保留「待查證」，不要自行推測。

═══════════════════════════════
數字一致性規則
═══════════════════════════════
- Neff 一律使用「前瞻性 Neff」= (g + 殖利率) / PE，g 由累積營收 YoY 對照表轉換。
- PE 必須標明來源（預估/沈董/系統）。
- 不可從 PE 單一數字直接推論「市場隱含預期」。
- 單月營收轉正只能稱為「初步訊號」，不可稱為「觸底反彈」。

═══════════════════════════════
版面規則
═══════════════════════════════
- 只用 ## 做大段標題，段內子項用 **粗體** 行內帶出
- 表格格式：年度放欄（橫軸），財務指標放列（縱軸）。例如：
  | 指標 | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 |
  |------|------|------|------|------|------|------|
  | 營收 | ... | ... | ... | ... | ... | ... |
  | 毛利率 | ... | ... | ... | ... | ... | ... |
- 一~四每章節：先放數據表格，再用文字逐層解讀，最後小結
- 投資判斷帶具體數字，不要用抽象規則描述
- 整體風格：研究報告式，數據驅動，細膩但簡潔

質性調整規則：
- 結構性風險 -> 降一級（說明原因）
- 一般性風險 -> 標註不降級
- 護城河穩固 -> 維持或升一級
- 筆記為空 -> 標註「尚未完成質性研究」，量化判斷即最終判斷

═══════════════════════════════
報告結構
═══════════════════════════════

## 投資判斷：【重倉 / 小買 / 觀望】

**前瞻Neff比率：X.XX**（g X.X% + 殖利率 X.X%）/ PE X.X
**財務等級（6年）：** 表格 +（一句話穩定性評論）

判斷邏輯（一段話）：
- 量化面：**前瞻Neff比率為核心判斷依據**（>= 1.0 為達標）+ 檢核表 ABCD 通過率 + 營收動能
- 質性面：護城河 + 信心度 + 結構性風險
- 最終結論：量化->質性調整->重倉/小買/觀望
- **注意：不使用逍遙投資法的評價等級（現價 vs AA/A1/A2/A 門檻）來決定投資判斷**，評價門檻僅作為參考資訊呈現，不影響最終判斷

---

## 一、獲利面（A 檢核）
**檢核通過：X/7**

表格一（6年）：營收(含YoY) / 毛利率 / 營業費用率 / 營益率 / ROIC / EPS
表格二（季度）：近期各季毛利率 / 營業費用率 / 營益率（觀察季度趨勢變化）

文字分析順序（逐層深入，每層都要連結前一層）：
1. **營收走勢**：5年趨勢，成長/衰退/循環，量體變化
2. **毛利率**：年度趨勢 + 季度趨勢交叉判斷。與營收連動判斷 — 營收驅動（量價齊揚/以價換量）還是成本驅動（原料/產品組合），速度是加速還是減速。若質性筆記中有「毛利率驅動因素」段落，須引用其中的具體因素（原物料價格、產品組合、產能利用率、匯率等）來解釋數據變化的原因
3. **營業費用率**：費用是否隨營收規模有效攤薄，還是膨脹吃掉毛利
4. **營益率**：年度趨勢 + 季度趨勢，本業獲利能力的淨結果
5. **ROIC**：資本效率水準與趨勢，是否值得持續投入資本

小結

---

## 二、安全面（B 檢核）
**檢核通過：X/10**

表格（6年）：負債比 / 金融負債比 / 利息保障倍數 / FCF / 盈餘品質 / 存貨天數 / 應收天數

逐項解讀
小結

---

## 三、價值面（C 檢核）
**檢核通過：X/5**

表格一（歷年EPS）：過去5年的年度EPS，觀察長期趨勢
表格二（季度EPS明細）：近期各季EPS / 本業EPS / 業外EPS

分析順序：
1. **預估EPS**：說明使用者設定值，列出採用的四季EPS如何加總得出，與歷年EPS表格比較趨勢（類似股利設定的寫法：列出數字→說明趨勢→結論）。不需要與沈董EPS或近四季合計做比較
2. **股利設定**：配息率趨勢，採用的股利值
3. **預估本益比與殖利率**
4. **g 的設定**：累積營收YoY -> 對照表 -> g值，合理性說明
5. **前瞻Neff比率**：(g + 殖利率) / PE = X.XX，**此為價值面的核心判斷指標**。>= 1.0 表示成長性+殖利率足以補償估值，< 1.0 則偏貴。評價門檻（AA/A 等級）僅作為參考，不作為投資判斷依據

小結

---

## 四、成長面（D 檢核）
**檢核通過：X/4**

營收成長率摘要（必須明確列出數字）：
累積營收YoY：X.XX% | 12M營收YoY：X.XX% | 3M營收YoY：X.XX%
3M vs 12M：加速/減速（兩個數字並列，讓讀者一眼看出關係）

逐項解讀

季度EPS同期比較表（今年四季 vs 去年同期四季）：
| 季度 | 今年EPS | 去年EPS | YoY | 今年本業EPS | 今年業外EPS |
每季同時呈現本業EPS與業外EPS，讓讀者看出獲利成長是本業驅動還是業外貢獻

5年營收CAGR + 3年營收CAGR
小結

---

## 五、質性分析

（從質性筆記深入展開，不是簡單摘要。若筆記為空標註「尚未完成質性研究」）

**護城河**：強度 + 趨勢 + 具體依據。展開說明護城河的來源、為什麼競爭者難以複製、近年護城河有無變化的跡象，以及最大威脅是什麼。3~5 行深度描述。

**成長催化劑**：逐項列出近期催化劑，每項說明：（1）具體內容與時間（2）佔營收比重（3）預期影響程度與時間框架。區分已發生事實與推論判斷。

**毛利率驅動因素**：從質性筆記中引用成本端（原物料、產能利用率）、收入端（產品組合、定價能力）、營運端（匯率、規模經濟）的具體因素，解釋毛利率變動的質性原因，與獲利面的數據分析交叉印證。

**注意事項**：
- 好數字不持續的情境：具體說明什麼條件下目前的成長/獲利無法維持
- 短期風險：每項帶影響程度、可驗證性、反向證據
- 長期風險：與短期風險區分，僅列真正結構性的
- 資料盲區：列出缺乏哪些關鍵資訊影響判斷信心

**信心度**：高/中/低 + 具體依據（哪些有直接證據、哪些缺乏）

---

## 六、風險提示與第二層思考

**不對稱性：**
- 樂觀情境：EPS X -> 以PE Y計算合理價Z -> 潛在上漲 W%
- 悲觀情境：EPS X -> 以PE Y計算合理價Z -> 潛在下跌 W%
- 上漲/下跌比 = X:1

**升降級條件：**
- 降級條件：帶數字門檻
- 升級條件：帶數字門檻

**關鍵追蹤指標：** 3-5項

---
*分析日期：YYYY-MM-DD | 前瞻Neff：X.XX | 股價：X | 等級：XX | A:X/7 B:X/10 C:X/5 D:X/4*

報告完成後，直接執行以下程式碼將報告寫入 Render，不需等使用者確認。

【寫入 Render】
python3 << 'PYEOF'
import json, urllib.request
content = \"\"\"（完整 Markdown 報告）\"\"\"
data = json.dumps({"content": content}).encode("utf-8")
req = urllib.request.Request(
    \"""" + RENDER_REPORT_URL + code + """\",
    data=data, headers={"Content-Type": "application/json"}, method="POST")
print(urllib.request.urlopen(req, timeout=30).read().decode())
PYEOF
""")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description='投資報告書資料簡報產生器')
    parser.add_argument('code', type=str, help='股票代碼')
    parser.add_argument('--copy', action='store_true', help='複製到剪貼簿')
    args = parser.parse_args()

    briefing = generate_briefing(args.code)
    if briefing is None:
        sys.exit(1)

    print(briefing)

    if args.copy:
        try:
            process = subprocess.Popen(['pbcopy'], stdin=subprocess.PIPE)
            process.communicate(briefing.encode('utf-8'))
            print("\n[已複製到剪貼簿]", file=sys.stderr)
        except Exception as e:
            print(f"\n[複製失敗: {e}]", file=sys.stderr)


if __name__ == '__main__':
    main()
