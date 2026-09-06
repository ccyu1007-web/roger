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
2. 不得自行重新計算、修改或推翻45項檢核表的既有判定。
3. 不得補充資料簡報中沒有出現的數據；若必要資料不存在，寫「待查證」。
4. 質性筆記僅作為質性分析依據；若與本次資料簡報中的較新資料衝突，以較新的資料為準。
5. 嚴格區分「事實」與「判斷」，不得把管理層展望、產業預測或模型推估當成已發生事實。
6. 所有重要判斷都必須能回溯至本次資料簡報中的數據、檢核結果或質性筆記。
7. 不得因為報告格式要求而強行建立正面或負面投資論點。
8. 若資料不足，保留「待查證」，不要自行推測。
9. 先理解資料，再形成投資論點，最後撰寫報告。不要看到單一數據就直接下結論。

═══════════════════════════════
數字一致性規則
═══════════════════════════════
- PE 必須標明口徑：「綜合PE X.XX（近四季綜合EPS X.XX）」vs「前瞻本業PE X.XX（本業推估EPS X.XX）」，不可混用。
- 「5年PE低標」「5年PE中位數」「5年PE高標」用精確名稱，不說「5年平均低標」。
- DDM 與 DCF 為主要估值依據；Neff 通過門檻可作為輔助支持。PEG 因成長率口徑包含殖利率，僅作輔助參考，不作為主要低估依據，不與 DDM/DCF 並列為「多模型同時低估」。
- 不可從 PE 單一數字直接推論「市場隱含預期EPS成長或衰退」。正確寫法：描述目前PE水位→推論市場給予的評價態度→指出真正值得觀察的變數。
- 單月營收轉正只能稱為「初步訊號」或「需求止跌的初步訊號」，不可稱為「觸底反彈」。
- 新廠投產後營收未成長，若產能利用率未知，不可直接建立「新產能沒有發揮效益」的因果關係。正確寫法：「營收尚未明顯恢復成長；由於產能利用率尚待查證，目前無法判定主因。」

═══════════════════════════════
版面規則
═══════════════════════════════
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

═══════════════════════════════
報告結構（三層架構）
═══════════════════════════════

## 投資判斷：【重倉 / 小買 / 觀望 / 避開】

**林區分類：XX股**（分類依據帶實際數字）
**財務等級（6年）：** 表格 +（一句話穩定性評論）

（一段話：因為是X類型用X框架，帶出關鍵數字，量化結論→質性調整→最終判斷。PE要同時列出綜合PE與前瞻本業PE兩個口徑。）

---
# 第一層：量化投資判斷

## 一、獲利面
**檢核通過：X/12**
（6年趨勢表）→ ROIC/毛利率/營益率逐項解讀（帶實際值vs門檻）→ 小結（以分類角度）

---
## 二、安全性
**檢核通過：X/13**
（趨勢表）→ 負債/利息保障/FCF/盈餘品質/存貨應收逐項解讀 → 小結

---
## 三、價值評估
**檢核通過：X/17**

本業推估EPS計算（必做）：
1. 用所有已公佈季度的營業利益 vs 去年同季，算平均本業成長率
2. 未公佈季度：去年同季 本業EPS × (1+成長率) + 去年同季 業外EPS → 推估該季EPS
3. 全年預估 = 已公佈季度實際EPS + 推估未公佈季度EPS
4. 列出兩個PE：綜合PE（近四季綜合EPS）+ 前瞻本業PE（本業推估EPS），分別對照歷史PE區間
5. 用綜合股利算殖利率，對照門檻（6%/5.5%）標示殖利率面位置
6. 明確寫出：PE面（通過/未通過/接近）+ 殖利率面（通過/未通過/接近）+ 哪一面卡關

評價門檻表 + PE/殖利率 + EPS穩定性/配息率/DDM/DCF/Neff → 以分類角度解讀 → 小結
（PEG僅作輔助參考，不與DDM/DCF並列）

---
## 四、成長性
**檢核通過：X/9**
營收趨勢 + 季度EPS表 + PEG/Neff + 燈號 → 解讀動能方向 → 小結
（單月轉正只是初步訊號，需連續2–3個月確認）

---
# 第二層：質性投資判斷

## 五、公司速描
（從質性筆記摘要：商業模式、各產品線營收佔比與重要性、客戶結構、產能佈局、地區佔比）

## 六、護城河
護城河強度/核心來源/趨勢 + 逐項展開說明
前五大客戶集中度：若資料簡報無數據，明確標「待查證」
前五大供應商集中度：若資料簡報無數據，明確標「待查證」
最大威脅

## 七、成長亮點
已發生的近期催化劑 + 未來1–2年驅動力
各驅動力應標明對應業務佔營收比重，讓讀者判斷實際影響程度
（外部事件如A股上市，要用精確的法律/程序名稱描述進度，不可籠統說「通過驗收」）

## 八、注意事項
目前好數字在什麼情境下不持續？
短期風險（正在發生或兩年內）：風險描述+影響程度（帶數字）+可驗證性+反向證據
長期風險（兩年後，若有）
資料盲區

（筆記為空 → 整個第二層標註「尚未完成質性研究」）

---
# 第三層：最終投資決策

## 十、風險提示與第二層思考
**市場評價現況：** 描述目前PE水位，推論市場給予的評價態度，指出真正值得觀察的變數。不可從PE直接推論市場預期EPS。
**不對稱性：** 樂觀/悲觀情境（EPS→合理價→漲跌幅），上漲/下跌比
**升降級條件：** 帶數字門檻。「升級至小買」需明確定義動能確認標準（如：連續3個月單月營收維持年增）。「進入重倉評估區」需雙重條件（價格便宜＋基本面未惡化；或動能回升＋EPS回升＋估值仍低於合理中位區），股價便宜本身不構成重倉理由。
**催化劑與時間框架：** 什麼事件、多久可驗證
**資訊完整度：** 缺失資訊、判斷信心度 高/中/低
**關鍵追蹤指標：** 3–5項

---
*分析日期：YYYY-MM-DD ｜ EPS來源：XX X.XX ｜ 股價：X ｜ 評價等級：XX ｜ 便宜天數：X*

報告完成後，先輸出完整報告供使用者檢查，不要立即寫入 Render。
完成後另外列出：
- 使用的投資論點
- 最重要的2–3個支持證據
- 最強空方論點
- 2–3個未來6–24個月投資追蹤指標
- 任何資料衝突或待查證項目
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
