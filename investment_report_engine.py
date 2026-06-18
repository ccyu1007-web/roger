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
    ('roic_avg5', 'ROIC 近5年平均 > 15%'),
    ('roic_latest', 'ROIC 最近一年 > 15%'),
    ('roic_trend', 'ROIC 趨勢：近3年均 > 近5年均'),
    ('roic_min5', 'ROIC 近5年最低值 > 10%'),
    ('gm_avg5', '毛利率近5年平均 > 30%'),
    ('gm_latest', '毛利率最近一年 > 30%'),
    ('gm_3v5', '毛利率趨勢：近3年均 > 近5年均'),
    ('gm_min5', '毛利率近5年最低值 > 25%'),
    ('opm_avg5', '營益率近5年平均 > 10%'),
    ('opm_latest', '營益率最近一年 > 10%'),
    ('opm_3v5', '營益率趨勢：近3年均 > 近5年均'),
    ('opm_min5', '營益率近5年最低值 > 5%'),
]

CHECKLIST_SAFETY = [
    ('debt_ratio_ok', '負債比 <= 50%'),
    ('fin_debt_ok', '長短期金融負債比 < 30%'),
    ('icr_ok', '利息保障倍數 > 5'),
    ('icr_min5', '利息保障倍數近5年最低值 > 3'),
    ('fcf_5y_pos', '自由現金流連續5年為正'),
    ('fcf_latest_pos', '最近一年自由現金流 > 0'),
    ('eq_ok', '盈餘品質率 >= 70%'),
    ('eq_min5', '盈餘品質率近5年最低值 > 60%'),
    ('inv_days_avg', '存貨週轉天數 <= 近5年平均'),
    ('inv_days_high', '存貨週轉天數未創5年新高'),
    ('ar_days_avg', '應收帳款週轉天數 <= 近5年平均'),
    ('ar_days_high', '應收帳款週轉天數未創5年新高'),
    ('qinv_4v20', '近四季平均存貨天數 < 近20季平均'),
]

CHECKLIST_VALUE = [
    ('shen_pe_ok', '沈董本益比 <= 15'),
    ('shen_vs_avg5', '沈董EPS >= 近5年平均EPS'),
    ('shen_vs_avg3', '沈董EPS >= 近3年平均EPS'),
    ('eps_5y_pos', '近5年EPS皆大於0'),
    ('eps_5y_stable', '近5年最高EPS/最低EPS < 3'),
    ('core_ratio', '累計營業利益/累計稅前淨利 > 80%'),
    ('wt_yld_ok', '綜合殖利率 >= 5%'),
    ('wt_payout_ok', '加權配息率 > 50%'),
    ('eps_vs_10y', '沈董EPS / 10年平均EPS >= 1'),
    ('grade_a_ok', '沈董等級為A級以上'),
    ('price_in_a', 'AA級評價 <= 股價 <= A級評價'),
    ('price_below_aa_v', '股價 <= AA級評價'),
    ('val_ddm_return', '股利折現模式年報酬 >= 10%'),
    ('dcf_safe_ok', '現價 <= 現金流量折現安全邊際價'),
    ('ge_neff_ratio', 'Neff 比率 >= 0.7'),
    ('ge_lynch_peg', 'PEG <= 1.0'),
    ('ge_lynch_consist', '林區成長一致性 >= 0.5'),
]

CHECKLIST_GROWTH = [
    ('rev_cagr5_ok', '近5年營收CAGR > 5%'),
    ('eps_cagr5_ok', '近5年EPS保守成長率 > 5%'),
    ('rev_accel', '近3年營收CAGR > 近5年營收CAGR'),
    ('eps_accel', '近3年EPS CAGR > 近5年EPS CAGR'),
    ('cum_rev_pos', '累積營收年增率 >= 0%'),
    ('rev_3m_pos', '短期3M營收年增率 >= 0%'),
    ('rev_12m_pos', '長期12M營收年增率 >= 0%'),
    ('rev_3m_gt_12m', '短期3M >= 長期12M'),
    ('ge_growth_green', '趨勢燈號為多頭'),
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
        SELECT quarter, eps, revenue, operating_income
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
    lines.append(f"  {'季度':>8} {'EPS':>7} {'營收(百萬)':>10} {'營業利益(百萬)':>14}")
    for row in reversed(list(qf_rows)):
        rev = row['revenue']
        oi = row['operating_income']
        rev_s = f"{rev/1e6:>10.0f}" if rev else '         —'
        oi_s = f"{oi/1e6:>14.0f}" if oi else '             —'
        lines.append(f"  {row['quarter']:>8} {row['eps']:>7} {rev_s} {oi_s}")

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

版面規則：
- 只用 ## 做大段標題，段內子項用 **粗體** 行內帶出
- 表格一律橫式（年度為欄）
- 一~四每類都要：趨勢表（帶6年實際數據）+ 以林區分類角度逐項解讀檢核項（帶實際值與門檻比較）+ 小結
- 投資判斷那段話要帶具體數字，不要用抽象規則描述
- 整體風格：研究報告式，數據驅動，細膩但簡潔

林區分類影響分析角度：
- 價值股（緩慢成長/穩健）：重殖利率、評價門檻、配息穩定性。PEG/Neff僅參考。
- 成長股（快速成長）：重PEG/Neff、營收加速度、ROIC趨勢。殖利率和逍遙評價門檻僅參考。
- 循環股（景氣循環/轉機）：重正常化PE、復甦進度、復甦訊號。一般PE和CAGR可能誤導。

質性調整規則：
- 結構性風險 → 降一級（說明原因）
- 一般性風險 → 標註不降級
- 護城河穩固 → 維持或升一級
- 筆記為空 → 標註「尚未完成質性研究」

## 投資判斷：【重倉 / 小買 / 觀望 / 避開】

**林區分類：XX股**（分類依據帶實際數字）
**財務等級（6年）：** 表格 +（一句話穩定性評論）

（一段話：因為是X類型用X框架，帶出關鍵數字，量化結論→質性調整→最終判斷）

---
## 一、獲利面
**檢核通過：X/12**
（6年趨勢表）→ ROIC/毛利率/營益率逐項解讀（帶實際值vs門檻）→ 小結（以分類角度）

---
## 二、安全性
**檢核通過：X/13**
（趨勢表）→ 負債/利息保障/FCF/盈餘品質/存貨應收逐項解讀 → 小結

---
## 三、價值評估
**檢核通過：X/15**
評價門檻表 + PE/殖利率 + EPS穩定性/配息率/DDM/DCF → 以分類角度解讀 → 小結

---
## 四、成長性
**檢核通過：X/9**
營收趨勢 + 季度EPS表 + PEG/Neff + 燈號 → 解讀動能方向 → 小結

---
## 五、質性說明
護城河強度/核心優勢/最大威脅/成長催化劑（從質性筆記摘要）

---
## 六、風險提示（第二層思考）
**市場隱含預期：** PE隱含什麼預期，我跟市場哪裡不同
**不對稱性：** 樂觀/悲觀情境（EPS→合理價→漲跌幅），上漲/下跌比
**升降級條件：** 帶數字門檻
**催化劑與時間框架：** 什麼事件、多久可驗證
**資訊完整度：** 缺失資訊、判斷信心度 高/中/低
**關鍵追蹤指標：** 3~5項

---
*分析日期：YYYY-MM-DD ｜ EPS來源：XX X.XX ｜ 股價：X ｜ 評價等級：XX ｜ 便宜天數：X*

【完成後自動存入 Render】
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
