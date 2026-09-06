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

# 檢核表項目定義（對應 export_checklist.py）
CHECKLIST_PROFIT = [
    ('roic_avg5', 'ROIC 近5年平均 ≥ 15%'),
    ('roic_latest', 'ROIC 最近一年 ≥ 15%'),
    ('roic_min5', 'ROIC 近5年最低值 ≥ 10%'),
    ('opm_avg5', '營益率近5年平均 ≥ 10%'),
    ('opm_min5', '營益率近5年最低值 ≥ 5%'),
    ('gm_median', '毛利率 ≥ 近5年中位數'),
    ('gm_q_median', '最近一季毛利率 ≥ 近4季中位數'),
]

CHECKLIST_SAFETY = [
    ('debt_ratio_ok', '負債比 ≤ 50%'),
    ('fin_debt_ok', '金融負債比 < 30%'),
    ('icr_ok', '利息保障倍數 > 5'),
    ('fcf_freq', 'FCF近5年至少3年為正'),
    ('fcf_no_consec', 'FCF近2年不得連續為負'),
    ('fcf_sum_pos', 'FCF近5年加總為正'),
    ('inv_level', '存貨水準 ≤ 近5年平均×1.2'),
    ('inv_trend', '存貨方向：最近一季 ≤ 近4季中位數×1.15'),
    ('ar_level', '應收水準 ≤ 近5年平均×1.2'),
    ('ar_trend', '應收方向：最近一季 ≤ 近4季中位數×1.15'),
]

CHECKLIST_VALUE = [
    ('grade_a_ok', '預估(沈董)等級為A級以上'),
    ('eps_vs_median5', '預估(沈董)EPS >= 近5年EPS中位數'),
    ('core_ratio', '累計營業利益/累計稅前淨利 > 70%'),
    ('price_val_ok', '現價 <= A級評價；<= AA更佳'),
    ('val_ddm_return', '股利折現現價潛在年報酬 >= 10%'),
    ('dcf_safe_ok', '現價 <= DCF安全邊際價'),
    ('ge_neff_ratio', 'Neff 比率 >= 1.0'),
]

CHECKLIST_GROWTH = [
    ('cum_rev_pos', '累積營收年增率 ≥ 0%'),
    ('rev_12m_pos', '12M營收年增率 ≥ 0%'),
    ('rev_3m_pos', '3M營收年增率 ≥ 0%'),
    ('rev_3m_gt_12m', '短期3M ≥ 長期12M'),
]


def generate_briefing(code):
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
        SELECT year, revenue, gross_profit, operating_income, net_income, eps,
               cash_dividend, roic, debt_ratio, operating_cf, capex, earnings_quality,
               interest_expense, total_equity, inventory, accounts_receivable
        FROM financial_annual WHERE code=? ORDER BY year DESC LIMIT 6
    """, (code,)).fetchall()
    fa = [dict(x) for x in fa_rows]

    # ── 季度損益 ──
    qf_rows = conn.execute("""
        SELECT quarter, eps, revenue, operating_income, eps_core, eps_nonop
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
    lines.append(f"近4季EPS：{r.get('eps_4q_sum')}　沈董EPS：{r.get('shen_eps')}　綜合EPS：{r.get('blend_eps')}")
    lines.append(f"沈董PE：{r.get('shen_pe')}　綜合PE：{r.get('blend_pe')}　綜合殖利率：{r.get('blend_yld')}%")
    lines.append(f"系統估算EPS：{r.get('sys_ann_eps')}（信心{r.get('sys_ann_confidence','')}）")
    lines.append(f"累積營收年增率：{r.get('revenue_cum_yoy')}%")
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

    # ── EPS 6 年 ──
    lines.append(f"\n【EPS 6年】")
    eps_line = "  "
    for i in range(6, 0, -1):
        y = r.get(f'eps_y{i}_label', '')
        e = r.get(f'eps_y{i}', '')
        eps_line += f"{y}:{e}　"
    lines.append(eps_line.strip())

    # ── 股利 6 年 ──
    lines.append(f"\n【股利 6年】")
    div_line = "  "
    for i in range(6, 0, -1):
        y = r.get(f'div_{i}_label', '')
        d = r.get(f'div_c{i}', '')
        if d is not None:
            d = round(float(d), 2)
        div_line += f"{y}:{d}　"
    lines.append(div_line.strip())

    # ── 配息率 ──
    lines.append(f"\n【配息率 5年】")
    payouts = [r.get(f'payout_{i}') for i in range(1, 6) if r.get(f'payout_{i}') is not None]
    if payouts:
        lines.append(f"  {' / '.join(f'{p:.1f}%' for p in payouts)}")

    # ── 年度財務數據 ──
    lines.append(f"\n【年度財務數據（6年）】")
    lines.append(f"  {'年度':>6} {'營收(百萬)':>10} {'毛利率':>7} {'營益率':>7} {'EPS':>7} {'股利':>6} {'ROIC':>7} {'負債比':>7} {'FCF(百萬)':>10} {'盈餘品質':>8} {'利息保障':>8}")
    for row in reversed(fa):
        rev = row['revenue']
        gp = row.get('gross_profit')
        oi = row.get('operating_income')
        gm = f"{gp/rev*100:.1f}%" if gp and rev and rev > 0 else '—'
        opm = f"{oi/rev*100:.1f}%" if oi and rev and rev > 0 else '—'
        fcf = (row.get('operating_cf') or 0) + (row.get('capex') or 0)
        eq = f"{row['earnings_quality']:.1f}%" if row.get('earnings_quality') else '—'
        ie = row.get('interest_expense')
        icr = f"{oi/ie:.1f}x" if oi and ie and ie > 0 else '—'
        div = row.get('cash_dividend')
        div_s = f"{div:.2f}" if div is not None else '—'
        roic = f"{row['roic']:.1f}%" if row.get('roic') else '—'
        debt = f"{row['debt_ratio']:.1f}%" if row.get('debt_ratio') else '—'
        rev_s = f"{rev/1e6:>10.0f}" if rev else '         —'
        fcf_s = f"{fcf/1e6:>10.0f}" if fcf else '         —'
        lines.append(f"  {row['year']:>6} {rev_s} {gm:>7} {opm:>7} {row.get('eps','—'):>7} {div_s:>6} {roic:>7} {debt:>7} {fcf_s} {eq:>8} {icr:>8}")

    # ── 季度 EPS ──
    lines.append(f"\n【季度EPS（近10季）】")
    lines.append(f"  {'季度':>8} {'EPS':>7} {'本業EPS':>8} {'業外EPS':>8} {'營收(百萬)':>10} {'營業利益(百萬)':>14}")
    for row in reversed(list(qf_rows)):
        rev = row['revenue']
        oi = row['operating_income']
        ec = row['eps_core']
        en = row['eps_nonop']
        rev_s = f"{rev/1e6:>10.0f}" if rev else '         —'
        oi_s = f"{oi/1e6:>14.0f}" if oi else '             —'
        ec_s = f"{ec:>8.2f}" if ec is not None else '       —'
        en_s = f"{en:>8.2f}" if en is not None else '       —'
        lines.append(f"  {row['quarter']:>8} {row['eps']:>7} {ec_s} {en_s} {rev_s} {oi_s}")

    # ── PE 歷史 ──
    lines.append(f"\n【PE歷史區間】")
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

    # ── 席勒指標 ──
    lines.append(f"\n【席勒PE指標】")
    lines.append(f"  席勒均值EPS：{sc.get('gi_shiller_avg_eps')}　席勒PE：{sc.get('gi_shiller_pe')}　Alert：{sc.get('gi_shiller_alert')}")

    # ── 成長指標 ──
    lines.append(f"\n【成長指標】")
    lines.append(f"  營收CAGR 3年：{sc.get('gi_rev_cagr_3y')}%　5年：{sc.get('gi_rev_cagr_5y')}%")
    lines.append(f"  3M營收年增：{sc.get('gi_rev_3m_yoy')}%　12M營收年增：{sc.get('gi_rev_12m_yoy')}%")
    lines.append(f"  趨勢燈號：{sc.get('growth_signal')}　紅旗：{sc.get('red_flags')}")
    lines.append(f"  PEG：{sc.get('gi_lynch_d')}{'（灰）' if sc.get('gi_lynch_gray') else ''}　Neff：{sc.get('gi_neff_d')}{'（灰）' if sc.get('gi_neff_gray') else ''}")
    lines.append(f"  ROIC均：{sc.get('gi_roic_avg')}%　ROE均：{sc.get('gi_roe_avg')}%　營益率均：{sc.get('gi_opm_avg')}%　FCF/營收均：{sc.get('gi_fcf_rev_avg')}%")
    lines.append(f"  存貨風險：{'有' if sc.get('growth_inv_risk') == 1 else '無'}　股本變動：{sc.get('gi_shares_change')}%")

    # ── 檢核表 45 項（分類列出，帶實際值）──
    def _fmt_checklist(items, category_name):
        lines_out = []
        passed = sum(1 for key, _ in items if sc.get(f'chk_{key}') == 1)
        lines_out.append(f"\n【檢核表 — {category_name}（{passed}/{len(items)}）】")
        for key, label in items:
            chk = sc.get(f'chk_{key}')
            mark = '✓' if chk == 1 else '✗' if chk == 0 else '?'
            actual = detail.get(key, '')
            lines_out.append(f"  {mark} {label}")
            if actual:
                lines_out.append(f"    → {actual}")
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

    # ── 報告格式指令 ──
    lines.append(f"\n{'=' * 60}")
    lines.append("【報告格式指令】")
    lines.append("=" * 60)
    lines.append("""
請根據以上資料簡報，撰寫投資報告書。嚴格遵守以下版面：

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
- PE 必須標明來源（預估/沈董/系統），與 Neff 比率使用的 PE 來源一致。
- Neff 比率 = (5年營收CAGR + 殖利率) / PE，是核心評價指標。
- DDM 與 DCF 為估值輔助；Neff ≥ 1.0 為價值篩選門檻。
- 不可從 PE 單一數字直接推論「市場隱含預期」。
- 單月營收轉正只能稱為「初步訊號」，不可稱為「觸底反彈」。

═══════════════════════════════
版面規則
═══════════════════════════════
- 只用 ## 做大段標題，段內子項用 **粗體** 行內帶出
- 表格一律橫式（年度為欄）
- 一~四每類都要：趨勢表（帶6年實際數據）+ 逐項解讀檢核項（帶實際值與門檻比較）+ 小結
- 投資判斷帶具體數字，不要用抽象規則描述
- 整體風格：研究報告式，數據驅動，細膩但簡潔

質性調整規則：
- 結構性風險 → 降一級（說明原因）
- 一般性風險 → 標註不降級
- 護城河穩固 → 維持或升一級
- 筆記為空 → 標註「尚未完成質性研究」，量化判斷即最終判斷

═══════════════════════════════
報告結構
═══════════════════════════════

## 投資判斷：【重倉 / 小買 / 觀望】

**Neff 群組：XX**（精選/價值型/動能型/僅Neff≥1.0）
**Neff 比率：X.XX**（5年營收CAGR X.X% + 殖利率 X.X%）/ PE X.X（來源：預估/沈董/系統）
**財務等級（6年）：** 表格 +（一句話穩定性評論）

判斷邏輯（一段話）：
- 量化面：Neff 群組 + 檢核表 ABCD 通過率 + 營收動能（累積/3M/12M）
- 質性面：護城河（具體依據）+ 信心度（具體依據）+ 結構性風險
- 質性 g vs 5年營收CAGR 差異說明
- 最終結論：量化→質性調整→重倉/小買/觀望

---

## 一、獲利面（A 檢核）
**檢核通過：X/7**
（6年趨勢表：營收/毛利率/營益率/ROIC/EPS）
逐項解讀：ROIC 均值/最新/底部、營益率 均值/底部、毛利率 vs 中位數/季趨勢
小結

---

## 二、安全面（B 檢核）
**檢核通過：X/10**
（趨勢表：負債比/金融負債比/利息保障/FCF）
逐項解讀：負債比/金融負債比/ICR、FCF三層（頻率/近期/總量）、存貨（水準/方向）、應收（水準/方向）
小結

---

## 三、價值面（C 檢核）
**檢核通過：X/7**
評價門檻表（AA/A1/A2/A vs 股價，折溢價%）
逐項解讀：等級、EPS vs 5年中位數、本業比率、股價 vs A級、Neff 比率、DDM年報酬、DCF安全邊際
質性 g（X%）vs 5年營收CAGR（X%）：差異原因說明
小結

---

## 四、成長面（D 檢核）
**檢核通過：X/4**
逐項解讀：累積營收YoY、12M、3M、3M vs 12M（加速/減速）
季度EPS趨勢表（近8季，含YoY）
5年營收CAGR + 3年營收CAGR
小結

---

## 五、質性分析

（從質性筆記摘要，若筆記為空標註「尚未完成質性研究」）

**護城河**：強度 + 趨勢 + 具體依據（moat_desc）
**成長催化劑**：近期催化劑（附營收佔比）
**注意事項**：好數字不持續的情境 + 短期風險 + 長期風險
**信心度**：高/中/低 + 具體依據（confidence_desc）

---

## 六、風險提示與第二層思考

**不對稱性：**
- 樂觀情境：EPS X → 以PE Y計算合理價Z → 潛在上漲 W%
- 悲觀情境：EPS X → 以PE Y計算合理價Z → 潛在下跌 W%
- 上漲/下跌比 = X:1

**升降級條件：**
- 降級條件：帶數字門檻（如「12M營收YoY連續3月負」）
- 升級條件：帶數字門檻（如「Neff突破1.5且3M>12M>0」）

**關鍵追蹤指標：** 3–5項（每項一句話說明為什麼重要）

---
*分析日期：YYYY-MM-DD ｜ Neff：X.XX ｜ 股價：X ｜ 等級：XX ｜ A:X/7 B:X/10 C:X/7 D:X/4*

報告完成後，先輸出完整報告供使用者檢查，不要立即寫入 Render。
待使用者確認後再寫入 Render。

【確認後存入 Render】
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
