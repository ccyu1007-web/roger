#!/usr/bin/env python3
"""
逍遙投資系統 — 選股推薦引擎
唯一真相：所有選股邏輯集中在此檔案，不在其他地方重複。

用法：
  python3 stock_picks_engine.py              # 完整篩選 + 推送 Render
  python3 stock_picks_engine.py --check 2330  # 驗證單支，印出完整判斷路徑
  python3 stock_picks_engine.py --excluded    # 列出所有被排除的股票及原因
  python3 stock_picks_engine.py --dry-run     # 完整篩選但不推送
"""

import argparse
import json
import os
import re
import sqlite3
import urllib.request
from datetime import date

# ══════════════════════════════════════════════════════════════
# 常數定義（修改門檻只改這裡）
# ══════════════════════════════════════════════════════════════

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'stocks.db')
RENDER_PICKS_URL = 'https://tock-system.onrender.com/api/stock-picks'

# ── 信任門檻 ──
EXCLUDED_INDUSTRIES = ('金融保險業', '金融業', '保險業', '建材營造')
MIN_LISTED_YEARS = 5
MIN_DIV_YEARS = 3            # 連續配息年數
MIN_GRADE_DATA_YEARS = 3     # 至少要有幾年有等級資料
MAX_BAD_GRADE_YEARS = 3      # 近5年差等級超過此數排除
BAD_GRADE_BASES = {'B1', 'B2', 'C', 'D', 'X'}

# ── 林區分類 ──
CYCLICAL_AMP_THRESHOLD = 2.5    # EPS振幅 = 6年正值EPS最高/最低，>此值歸景氣循環
FAST_GROWTH_CAGR = 15.0
STEADY_GROWTH_CAGR = 5.0
SLOW_GROWTH_AMP_MAX = 1.5      # 緩慢成長要求EPS振幅<此值（穩定性條件）

# ── 財務等級排序（排序第15以內 = A級） ──
A_LEVEL_BASES = {'AA', 'A1', 'A2', 'A', 'B1A'}
CDX_BASES = {'C', 'D', 'X'}
B2A_PLUS_BASES = {'AA', 'A1', 'A2', 'A', 'B1A', 'B2A'}

# ── 價值精選 ──
# （門檻用 val_aa / val_a 欄位，無需額外常數）

# ── 成長精選 ──
GROWTH_PE_MAX = 20.0
GROWTH_PEG_HEAVY = 0.8
GROWTH_PEG_SMALL = 1.2
GROWTH_NEFF_HEAVY = 1.2
GROWTH_NEFF_SMALL = 0.8

# ── 循環反轉 ──
CYCLE_NORM_PE_HEAVY = 10.0
CYCLE_NORM_PE_SMALL = 12.0
CYCLE_NORM_PE_WATCH = 15.0
CYCLE_EXTREME_RATIO = 1.8    # max/均值 > 此值用中位數
CYCLE_RECOVERY_EXCLUDE = 2.0
CYCLE_RECOVERY_DOWNGRADE = 1.5
CYCLE_RECOVERY_MIN = 0.5
CYCLE_GM_STABLE_RATIO = 0.95  # 毛利率 >= 5年均值 * 此比例

# ── 防呆 ──
SHILLER_ALERT_THRESHOLD = 0.7
EPS_EXTREME_MIN_RATIO = 0.15  # min/max < 此值視為極端
SHARE_CHANGE_EXCLUDE = 50.0
SHARE_CHANGE_WARN = 20.0

# ── 輸出 ──
LOW_LIQUIDITY_THRESHOLD = 100000
PE_HIGH_CAP = 20.0  # pe_high 超過此值以此計


# ══════════════════════════════════════════════════════════════
# Helper functions
# ══════════════════════════════════════════════════════════════

def _grade_base(g):
    """取等級的 base（去掉 +/-）"""
    if not g or g in ('-', 'X', ''):
        return 'X'
    return g.replace('+', '').replace('-', '')


def _is_a_level(g):
    return _grade_base(g) in A_LEVEL_BASES


def _is_cdx(g):
    b = _grade_base(g)
    return b in CDX_BASES or b == 'X'


def _is_b2a_or_above(g):
    return _grade_base(g) in B2A_PLUS_BASES


def _is_bad_grade(g):
    return _grade_base(g) in BAD_GRADE_BASES


def _median(vals):
    sv = sorted(vals)
    n = len(sv)
    if n % 2 == 0:
        return (sv[n // 2 - 1] + sv[n // 2]) / 2
    return sv[n // 2]


# ══════════════════════════════════════════════════════════════
# 資料載入
# ══════════════════════════════════════════════════════════════

def load_all_data():
    """一次載入所有需要的資料"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # stocks + checklist
    stocks = {}
    for r in conn.execute("""
        SELECT s.*,
               sc.profit_count, sc.safety_count, sc.value_count, sc.growth_eval_count,
               sc.gi_neff_d, sc.gi_lynch_d, sc.gi_neff_gray, sc.gi_lynch_gray,
               sc.gi_roic_avg, sc.gi_roe_avg, sc.gi_opm_avg,
               sc.gi_shiller_avg_eps, sc.gi_shiller_pe, sc.gi_shiller_alert,
               sc.growth_signal, sc.red_flags,
               sc.gi_rev_cagr_3y, sc.gi_rev_cagr_5y,
               sc.gi_rev_3m_yoy, sc.gi_rev_12m_yoy,
               sc.growth_inv_risk, sc.gi_shares_change, sc.gi_pe, sc.gi_fcf_rev_avg
        FROM stocks s
        LEFT JOIN stock_checklist sc ON s.code = sc.code
        WHERE s.close IS NOT NULL AND s.close > 0
    """).fetchall():
        stocks[r['code']] = dict(r)

    codes = list(stocks.keys())
    if not codes:
        conn.close()
        return stocks, {}, {}, {}, set(), set()

    ph = ','.join('?' * len(codes))

    # PE history (5年)
    pe_data = {}
    for r in conn.execute(
        f"SELECT code, year, pe_high, pe_low FROM pe_history WHERE code IN ({ph}) ORDER BY code, year DESC",
        codes
    ).fetchall():
        c = r['code']
        if c not in pe_data:
            pe_data[c] = []
        if len(pe_data[c]) < 5:
            pe_data[c].append({
                'high': min(float(r['pe_high']), PE_HIGH_CAP) if r['pe_high'] else None,
                'low': float(r['pe_low']) if r['pe_low'] else None
            })

    for c, pl in pe_data.items():
        h = [p['high'] for p in pl if p['high'] and p['high'] > 0]
        l = [p['low'] for p in pl if p['low'] and p['low'] > 0]
        if h and l and c in stocks:
            stocks[c]['_pe_low'] = round(sum(l) / len(l), 1)
            stocks[c]['_pe_mid'] = round((sum(h) / len(h) + sum(l) / len(l)) / 2, 1)
            stocks[c]['_pe_high'] = round(sum(h) / len(h), 1)
            # 五分法用中位數（高點封頂，低點不封頂）
            hs = sorted(h)
            ls = sorted(l)
            stocks[c]['_xpe_hi'] = hs[len(hs) // 2]
            stocks[c]['_xpe_lo'] = ls[len(ls) // 2]

    # 毛利率：從 financial_annual 取近5年實際值
    gm_data = {}
    for r in conn.execute(f"""
        SELECT code, year, gross_profit, revenue FROM financial_annual
        WHERE code IN ({ph}) AND gross_profit IS NOT NULL AND revenue IS NOT NULL AND revenue > 0
        ORDER BY code, year DESC
    """, codes).fetchall():
        c = r['code']
        if c not in gm_data:
            gm_data[c] = []
        if len(gm_data[c]) < 5:
            gm_data[c].append(round(r['gross_profit'] / r['revenue'] * 100, 2))

    for c, gms in gm_data.items():
        if c in stocks and gms:
            stocks[c]['_gm_latest'] = gms[0]
            stocks[c]['_gm_avg5'] = round(sum(gms) / len(gms), 2)

    # 使用者清單（track / watch / quality）
    track_set = set()
    watch_set = set()
    quality_set = set()
    for row in conn.execute("SELECT list_type, code FROM user_lists WHERE list_type IN ('track','watch','quality')"):
        lt = row['list_type']
        c = row['code']
        if lt == 'track':
            track_set.add(c)
        elif lt == 'watch':
            watch_set.add(c)
        elif lt == 'quality':
            quality_set.add(c)

    conn.close()
    return stocks, pe_data, gm_data, track_set, watch_set, quality_set


def load_previous_picks():
    """從 Render 讀取上次選股推薦，提取所有代碼"""
    try:
        req = urllib.request.Request(RENDER_PICKS_URL)
        resp = urllib.request.urlopen(req, timeout=15)
        content = json.loads(resp.read()).get('content', '')
    except Exception:
        content = ''

    # 提取所有表格中的股票代碼
    all_codes = set(re.findall(r'\|\s*(\d{4,6})\s*\|', content))
    return all_codes


# ══════════════════════════════════════════════════════════════
# Step 1: 信任門檻
# ══════════════════════════════════════════════════════════════

def check_trust(r):
    """回傳 (通過, 排除原因)"""
    code = r['code']
    name = r.get('name') or ''
    ind = r.get('industry') or ''

    # 1. KY/DR
    if '-KY' in code or '-KY' in name or code.startswith('910'):
        return False, 'KY/DR'

    # 2. 排除產業
    for ex in EXCLUDED_INDUSTRIES:
        if ex in ind:
            return False, f'產業排除({ind})'

    # 3. 上市不足5年
    ld = r.get('listed_date')
    if not ld:
        return False, '無上市日期'
    try:
        cutoff = (date.today().year - MIN_LISTED_YEARS) * 10000 + 101
        if int(str(ld).replace('-', '').replace('/', '')[:8]) > cutoff:
            return False, f'上市不足{MIN_LISTED_YEARS}年'
    except (ValueError, TypeError):
        return False, '上市日期格式錯誤'

    # 4. 配息不足3年
    for i in range(1, MIN_DIV_YEARS + 1):
        d = r.get(f'div_c{i}')
        if not d or d == 0:
            return False, f'配息不足{MIN_DIV_YEARS}年(div_c{i}缺)'

    # 5. 財務等級：至少3年有資料，且差等級不超過門檻
    grade_count = 0
    bad_count = 0
    for i in range(1, 6):
        g = r.get(f'fin_grade_{i}')
        if g and g not in ('-', ''):
            grade_count += 1
            if _is_bad_grade(g):
                bad_count += 1
    if grade_count < MIN_GRADE_DATA_YEARS:
        return False, f'等級資料不足({grade_count}年<{MIN_GRADE_DATA_YEARS}年)'
    if bad_count >= MAX_BAD_GRADE_YEARS:
        return False, f'差等級{bad_count}年>={MAX_BAD_GRADE_YEARS}年'

    return True, ''


# ══════════════════════════════════════════════════════════════
# Step 2: 林區分類
# ══════════════════════════════════════════════════════════════

def classify_lynch(r):
    """回傳 (分類, EPS振幅, EPS正值列表, 判斷說明)"""
    eps_vals = []
    for i in range(1, 7):
        v = r.get(f'eps_y{i}')
        if v is not None:
            eps_vals.append(float(v))
    r['_eps_vals'] = eps_vals

    pos_vals = [v for v in eps_vals if v > 0]
    has_neg = any(v <= 0 for v in eps_vals)

    # 振幅只看正值（決議#5）
    if len(pos_vals) >= 2:
        amp = max(pos_vals) / min(pos_vals)
    else:
        amp = None
    r['_amp'] = amp

    cagr3 = r.get('gi_rev_cagr_3y')
    fg1, fg2 = r.get('fin_grade_1'), r.get('fin_grade_2')

    # 決議#3: 景氣循環 > 轉機 > 快速成長 > 穩健 > 緩慢
    if amp is not None and amp > CYCLICAL_AMP_THRESHOLD:
        return '景氣循環', amp, f'振幅{amp:.1f}x>{CYCLICAL_AMP_THRESHOLD}'

    if _is_cdx(fg2) and _is_b2a_or_above(fg1):
        return '轉機股', amp, f'等級{fg2}→{fg1}'

    # 決議#4: CAGR 為 NULL 排除
    if cagr3 is None:
        return None, amp, 'CAGR為NULL，無法分類'

    if cagr3 > FAST_GROWTH_CAGR:
        return '快速成長', amp, f'CAGR3={cagr3:.1f}%>{FAST_GROWTH_CAGR}%'

    if cagr3 >= STEADY_GROWTH_CAGR:
        return '穩健股', amp, f'CAGR3={cagr3:.1f}% in {STEADY_GROWTH_CAGR}~{FAST_GROWTH_CAGR}%'

    if amp is not None and amp < SLOW_GROWTH_AMP_MAX:
        return '緩慢成長', amp, f'CAGR3={cagr3:.1f}%<{STEADY_GROWTH_CAGR}% & 振幅{amp:.1f}x<{SLOW_GROWTH_AMP_MAX}'

    # CAGR < 5% 但振幅 >= 1.5，不屬於任何分類
    return '緩慢成長', amp, f'CAGR3={cagr3:.1f}%<{STEADY_GROWTH_CAGR}%（振幅{amp:.1f}x偏高但未達循環）'


# ══════════════════════════════════════════════════════════════
# Step 3: 防呆檢查
# ══════════════════════════════════════════════════════════════

def check_defenses(r):
    """回傳 (flags列表, max_level)"""
    flags = []
    eps_vals = r.get('_eps_vals', [])
    pos_eps = [v for v in eps_vals if v > 0]
    has_neg = any(v <= 0 for v in eps_vals)

    # EPS 極端值
    eps_extreme = has_neg
    if not eps_extreme and len(pos_eps) >= 2:
        eps_extreme = min(pos_eps) / max(pos_eps) < EPS_EXTREME_MIN_RATIO
    if eps_extreme:
        flags.append('EPS含極端值')

    # 席勒 alert
    alert = r.get('gi_shiller_alert')
    if alert is not None and alert < SHILLER_ALERT_THRESHOLD:
        if eps_extreme:
            flags.append('席勒PE含極端值')
        else:
            flags.append('循環高點')

    # 股利可持續性
    ey1, ey2, ey3 = r.get('eps_y1'), r.get('eps_y2'), r.get('eps_y3')
    dc1, dc2, dc3 = r.get('div_c1'), r.get('div_c2'), r.get('div_c3')
    if ey1 and ey2 and ey3 and ey2 > 0 and ey3 > 0:
        if (ey3 - ey2) / ey3 < -0.2 and (ey2 - ey1) / ey2 < -0.2:
            if dc1 and dc2 and dc3 and dc1 >= dc3 and dc2 >= dc3:
                flags.append('股利可持續性疑慮')

    # 存貨風險
    if r.get('growth_inv_risk') == 1:
        flags.append('存貨風險')

    # 股本變動
    sc = r.get('gi_shares_change')
    if sc is not None:
        if abs(sc) > SHARE_CHANGE_EXCLUDE:
            flags.append('股本變動>50%')
        elif abs(sc) > SHARE_CHANGE_WARN:
            flags.append('股本變動20-50%')

    # 決定 max_level
    max_level = 'heavy'
    if 'EPS含極端值' in flags or '股本變動20-50%' in flags:
        max_level = 'small'
    if '循環高點' in flags:
        max_level = 'watch'  # 決議#9: 改為觀望而非完全排除

    r['_defense_flags'] = flags
    r['_max_level'] = max_level
    return flags, max_level


# ══════════════════════════════════════════════════════════════
# Step 4: 估值判斷
# ══════════════════════════════════════════════════════════════

def evaluate_value(r):
    """價值精選判斷，回傳 (level, reason)"""
    cl = r['close']
    val_aa, val_a = r.get('val_aa'), r.get('val_a')
    fg1, fg2 = r.get('fin_grade_1'), r.get('fin_grade_2')
    m3 = r.get('gi_rev_3m_yoy')
    m12 = r.get('gi_rev_12m_yoy')
    rc = r.get('revenue_cum_yoy')
    ml = r['_max_level']
    flags = r['_defense_flags']
    passed_def = '循環高點' not in flags and '股本變動>50%' not in flags

    # 重倉
    if (val_aa and cl <= val_aa
            and m3 is not None and m3 >= 0
            and m12 is not None and m3 >= m12
            and rc is not None and rc > 0
            and passed_def and ml == 'heavy'
            and _is_a_level(fg1) and _is_a_level(fg2)):
        return 'heavy', f'評價AA(股價{cl}<=門檻{val_aa})+3M>=0+3M>=12M+累積>0+防呆通過+近2年等級A以上'

    # 小買（max_level 必須 heavy 或 small，watch 不能進小買）
    if (val_a and cl <= val_a
            and m3 is not None and m3 >= 0
            and rc is not None and rc > 0
            and ml in ('heavy', 'small')):
        vl = 'AA' if val_aa and cl <= val_aa else ('A2' if r.get('val_a2') and cl <= r['val_a2'] else 'A')
        return 'small', f'評價{vl}(股價{cl}<=門檻{val_a})+3M>=0+累積>0'

    # 觀望（決議#6: 3M和累積都NULL不列入）
    if _is_a_level(fg1) and val_a and cl <= val_a:
        has_neg_m3 = m3 is not None and m3 < 0
        has_neg_rc = rc is not None and rc < 0
        if has_neg_m3 or has_neg_rc:
            reasons = []
            if has_neg_m3:
                reasons.append(f'等3M轉正（目前{m3:.1f}%）')
            if has_neg_rc:
                reasons.append(f'等累積轉正（目前{rc:.1f}%）')
            return 'watch', '; '.join(reasons)

    return None, ''


def evaluate_growth(r):
    """成長精選判斷，回傳 (level, reason)"""
    spe = r.get('shen_pe')
    if spe is None or spe <= 0 or spe > GROWTH_PE_MAX:
        return None, ''

    peg = r.get('gi_lynch_d')
    neff = r.get('gi_neff_d')
    m3 = r.get('gi_rev_3m_yoy')
    m12 = r.get('gi_rev_12m_yoy')
    rc = r.get('revenue_cum_yoy')
    ml = r['_max_level']
    flags = r['_defense_flags']
    passed_def = '循環高點' not in flags and '股本變動>50%' not in flags

    peg_h = peg is not None and peg <= GROWTH_PEG_HEAVY
    neff_h = neff is not None and neff >= GROWTH_NEFF_HEAVY
    peg_s = peg is not None and peg <= GROWTH_PEG_SMALL
    neff_s = neff is not None and neff >= GROWTH_NEFF_SMALL

    # 重倉
    if ((peg_h or neff_h)
            and m3 is not None and m3 > 0
            and rc is not None and rc > 0
            and m12 is not None and m3 > m12
            and passed_def and ml == 'heavy'):
        indicator = f'PEG={peg:.2f}' if peg_h else f'Neff={neff:.2f}'
        return 'heavy', f'沈董PE={spe:.1f}+{indicator}+3M>0+累積>0+3M>12M+防呆通過'

    # 小買（max_level 必須 heavy 或 small）
    if ((peg_s or neff_s)
            and m3 is not None and m3 > 0
            and rc is not None and rc > 0
            and passed_def and ml in ('heavy', 'small')):
        indicator = f'PEG={peg:.2f}' if peg_s else f'Neff={neff:.2f}'
        return 'small', f'沈董PE={spe:.1f}+{indicator}+3M>0+累積>0+防呆通過'

    # 觀望（決議#6）
    if (peg_s or neff_s) and passed_def:
        has_neg_m3 = m3 is not None and m3 < 0
        has_neg_rc = rc is not None and rc < 0
        if has_neg_m3 or has_neg_rc:
            reasons = []
            if has_neg_m3:
                reasons.append(f'等3M轉正（目前{m3:.1f}%）')
            if has_neg_rc:
                reasons.append(f'等累積轉正（目前{rc:.1f}%）')
            return 'watch', '; '.join(reasons)

    return None, ''


def evaluate_cycle(r):
    """循環反轉判斷，回傳 (level, reason)"""
    cl = r['close']
    e4q = r.get('eps_4q_sum')
    if not e4q or e4q <= 0:
        return None, '近4季EPS<=0'

    eps_vals = r.get('_eps_vals', [])
    if len(eps_vals) < 2:
        return None, 'EPS資料不足'
    pos_vals = [v for v in eps_vals if v > 0]
    if not pos_vals:
        return None, '無正值EPS'

    # 正常化 EPS
    mean_eps = sum(eps_vals) / len(eps_vals)
    if mean_eps > 0 and max(eps_vals) / mean_eps > CYCLE_EXTREME_RATIO:
        norm_eps = _median(eps_vals)
        norm_method = '中位數'
    else:
        norm_eps = mean_eps
        norm_method = '平均'

    if norm_eps <= 0:
        return None, '正常化EPS<=0'

    r['_norm_eps'] = round(norm_eps, 2)
    r['_norm_method'] = norm_method
    r['_norm_pe'] = round(cl / norm_eps, 1)

    # 復甦進度
    rp = round(e4q / norm_eps, 2)
    r['_rp'] = rp

    if rp > CYCLE_RECOVERY_EXCLUDE:
        return None, f'復甦進度{rp}>={CYCLE_RECOVERY_EXCLUDE}（高峰排除）'

    npe = r['_norm_pe']
    m3 = r.get('gi_rev_3m_yoy')
    m12 = r.get('gi_rev_12m_yoy')
    rc = r.get('revenue_cum_yoy')
    ml = r['_max_level']
    flags = r['_defense_flags']

    # 復甦訊號（決議#8: 用實際毛利率）
    sig = 0
    sd = []
    if m3 is not None and m3 > 0:
        sig += 1
        sd.append('3M+')
    gm_latest = r.get('_gm_latest')
    gm_avg5 = r.get('_gm_avg5')
    if gm_latest is not None and gm_avg5 is not None and gm_avg5 > 0:
        if gm_latest >= gm_avg5 * CYCLE_GM_STABLE_RATIO:
            sig += 1
            sd.append('毛利穩')
    if rc is not None and rc > 0:
        sig += 1
        sd.append('累積+')
    r['_sig'] = sig
    r['_sd'] = '/'.join(sd) if sd else '無'

    # 循環位置
    alert = r.get('gi_shiller_alert')
    if alert is not None:
        if alert < 0.7:
            cp = '高峰'
        elif alert < 1.0:
            cp = '回落'
        elif alert >= 1.2:
            cp = '谷底'
        elif m3 is not None and m3 > 0:
            cp = '復甦初期'
        else:
            cp = '回落'
    else:
        cp = '未知'
    r['_cp'] = cp

    passed_def = '股本變動>50%' not in flags  # 循環高點改觀望不排除

    # 判斷基本層級
    base_level = None
    reason = ''

    if (npe <= CYCLE_NORM_PE_HEAVY and CYCLE_RECOVERY_MIN <= rp <= CYCLE_RECOVERY_DOWNGRADE
            and sig >= 3 and m12 is not None and m3 is not None and m3 > m12
            and passed_def and ml == 'heavy'):
        base_level = 'heavy'
        reason = f'正常化PE={npe}<=10+進度{rp}+訊號{sig}/3+3M>12M+防呆通過'
    elif (passed_def and ml not in ('none',)
          and ((npe <= CYCLE_NORM_PE_HEAVY and CYCLE_RECOVERY_MIN <= rp <= CYCLE_RECOVERY_DOWNGRADE and sig >= 2)
               or (npe <= CYCLE_NORM_PE_SMALL and CYCLE_RECOVERY_MIN <= rp <= CYCLE_RECOVERY_DOWNGRADE and sig >= 2))):
        base_level = 'small'
        reason = f'正常化PE={npe}+進度{rp}+訊號{sig}/3+防呆通過'
    elif npe <= CYCLE_NORM_PE_WATCH and (rp < CYCLE_RECOVERY_MIN or sig < 2):
        base_level = 'watch'
        reasons = []
        if rp < CYCLE_RECOVERY_MIN:
            reasons.append(f'EPS復甦不足（{rp}）')
        if sig < 2:
            reasons.append(f'訊號不足（{sig}/3）')
        reason = '; '.join(reasons)

    # 決議#9: 循環高點列觀望
    if '循環高點' in flags and base_level is None and npe <= CYCLE_NORM_PE_WATCH:
        base_level = 'watch'
        reason = '循環高點，不宜追價'

    # 復甦進度降級
    if rp > CYCLE_RECOVERY_DOWNGRADE:
        if base_level == 'heavy':
            base_level = 'small'
            r['_dg'] = '復甦進度1.5~2.0降級'
            reason += '（復甦進度降級）'
        elif base_level == 'small':
            base_level = 'watch'
            r['_dg'] = '復甦進度1.5~2.0降級'
            reason = '復甦進度1.5~2.0降級'
        elif base_level is None and npe <= CYCLE_NORM_PE_WATCH:
            base_level = 'watch'
            reason = '復甦進度1.5~2.0降級'

    return base_level, reason


# ══════════════════════════════════════════════════════════════
# 格式化輸出
# ══════════════════════════════════════════════════════════════

def _liq(r):
    return '[低流動性]' if (r.get('volume') or 0) < LOW_LIQUIDITY_THRESHOLD else ''


def _fl(r):
    fl = r.get('_defense_flags', [])
    dg = r.get('_dg', '')
    parts = list(fl)
    if dg:
        parts.append(dg)
    return ','.join(parts) or '-'


# ── 共用 V/X 格式化 ──────────────────────────────────────────
_V = '<span style="color:#2563eb;font-weight:700">V</span>'
_X = '<span style="color:#ef4444;font-weight:700">X</span>'
_PASS = '<span style="color:#2563eb">通過</span>'
_FAIL = '<span style="color:#ef4444">未達</span>'
CROSS_YLD_THRESHOLD = 5.5


def _pe_position(pe, pe_lo, pe_hi):
    """PE 五分法位置判斷"""
    if pe_lo is None or pe_hi is None or pe_hi <= pe_lo:
        return None, '無區間'
    step = (pe_hi - pe_lo) / 5
    b1 = pe_lo + step
    b2 = pe_lo + step * 2
    b3 = pe_lo + step * 3
    b4 = pe_lo + step * 4
    if pe < pe_lo:
        return True, f'{pe:.1f} 極低'
    elif pe <= b1:
        return True, f'{pe:.1f} 很便宜'
    elif pe <= b2:
        return True, f'{pe:.1f} 偏低'
    elif pe <= b3:
        return False, f'{pe:.1f} 合理'
    elif pe <= b4:
        return False, f'{pe:.1f} 偏高'
    elif pe <= pe_hi:
        return False, f'{pe:.1f} 偏貴'
    else:
        return False, f'{pe:.1f} 極高'


def _fmt_pe_range(r):
    """PE區間（中位數低~高）"""
    lo = r.get('_xpe_lo')
    hi = r.get('_xpe_hi')
    if lo and hi:
        return f'{lo:.1f}~{hi:.1f}'
    return '—'


def _fmt_pe(r):
    """格式化 PE 面（V/X + 五分法位置），EPS用min(沈董,綜合)與逍遙評價法一致"""
    cl = r.get('close')
    val_eps = r.get('val_eps_used')
    if not cl or not val_eps or val_eps <= 0:
        return f'{_X} —'
    pe = cl / val_eps
    pe_lo = r.get('_xpe_lo')
    pe_hi = r.get('_xpe_hi')
    passed, desc = _pe_position(pe, pe_lo, pe_hi)
    return f'{_V if passed else _X} {desc}'


def _fmt_yld(r):
    """格式化殖利率面（V/X + 數字）"""
    cl = r.get('close')
    bdiv = r.get('blend_div')
    if not cl or cl <= 0 or not bdiv or bdiv <= 0:
        return f'{_X} —'
    yld = bdiv / cl * 100
    passed = yld >= CROSS_YLD_THRESHOLD
    return f'{_V if passed else _X} {yld:.1f}% {_PASS if passed else _FAIL}'


def _fmt_m3(r):
    v = r.get('gi_rev_3m_yoy')
    if v is None: return '—'
    mark = _V if v >= 0 else _X
    return f'{mark} {v:.1f}%'


def _fmt_m12(r):
    v = r.get('gi_rev_12m_yoy')
    if v is None: return '—'
    mark = _V if v >= 0 else _X
    return f'{mark} {v:.1f}%'


def _fmt_rc(r):
    v = r.get('revenue_cum_yoy')
    if v is None: return '—'
    mark = _V if v > 0 else _X
    return f'{mark} {v:.1f}%'


def fmt_value(r):
    c, n, cl = r['code'], (r.get('name') or '')[:8], r['close']
    vl = ''
    if r.get('val_aa') and cl <= r['val_aa']: vl = 'AA'
    elif r.get('val_a1') and cl <= r['val_a1']: vl = 'A1'
    elif r.get('val_a2') and cl <= r['val_a2']: vl = 'A2'
    elif r.get('val_a') and cl <= r['val_a']: vl = 'A'
    ch = r.get('val_cheap_days') or 0
    return f"| {c} | {n}{_liq(r)} | {r.get('fin_grade_1', '')} | {vl} | {cl} | {_fmt_m3(r)} | {_fmt_m12(r)} | {_fmt_rc(r)} | {_fmt_pe(r)} | {_fmt_pe_range(r)} | {_fmt_yld(r)} | {ch} | {_fl(r)} |"


def fmt_growth(r):
    c, n, cl = r['code'], (r.get('name') or '')[:8], r['close']
    pg = f"{r['gi_lynch_d']:.2f}" if r.get('gi_lynch_d') else '-'
    nf = f"{r['gi_neff_d']:.2f}" if r.get('gi_neff_d') else '-'
    if r.get('gi_lynch_gray') and r.get('gi_lynch_d'): pg += '(灰)'
    if r.get('gi_neff_gray') and r.get('gi_neff_d'): nf += '(灰)'
    cg = f"{r['gi_rev_cagr_3y']:.1f}" if r.get('gi_rev_cagr_3y') else '-'
    sp = f"{r['shen_pe']:.1f}" if r.get('shen_pe') else '-'
    return f"| {c} | {n}{_liq(r)} | {r.get('fin_grade_1', '')} | {cl} | {_fmt_m3(r)} | {_fmt_m12(r)} | {_fmt_rc(r)} | {_fmt_pe(r)} | {_fmt_pe_range(r)} | {_fmt_yld(r)} | {cg} | {pg} | {nf} | {sp} | {_fl(r)} |"


def fmt_cycle(r):
    c, n, cl = r['code'], (r.get('name') or '')[:8], r['close']
    ne = r.get('_norm_eps', '')
    nm = r.get('_norm_method', '')
    rp = r.get('_rp', '')
    sd = r.get('_sd', '')
    e4 = f"{r['eps_4q_sum']:.2f}" if r.get('eps_4q_sum') else '—'
    ne_s = f"{ne}({nm})" if ne and nm else '—'
    return f"| {c} | {n}{_liq(r)} | {r.get('fin_grade_1', '')} | {cl} | {_fmt_m3(r)} | {_fmt_m12(r)} | {_fmt_rc(r)} | {_fmt_pe(r)} | {_fmt_pe_range(r)} | {_fmt_yld(r)} | {e4} | {ne_s} | {rp} | {sd} | {_fl(r)} |"


# ── 統一格式（新版四區塊用）──────────────────────────────────

def _fmt_key_indicator(r):
    """依林區分類產出關鍵指標欄"""
    lt = r.get('_lt', '')
    if lt in ('緩慢成長', '穩健股'):
        cl = r['close']
        vl = ''
        if r.get('val_aa') and cl <= r['val_aa']: vl = 'AA'
        elif r.get('val_a1') and cl <= r['val_a1']: vl = 'A1'
        elif r.get('val_a2') and cl <= r['val_a2']: vl = 'A2'
        elif r.get('val_a') and cl <= r['val_a']: vl = 'A'
        ch = r.get('val_cheap_days') or 0
        return f'評價{vl} {ch}天' if vl else f'Above {ch}天'
    elif lt == '快速成長':
        sp = f"沈董PE={r['shen_pe']:.1f}" if r.get('shen_pe') else 'PE—'
        pg = f"PEG={r['gi_lynch_d']:.2f}" if r.get('gi_lynch_d') else ''
        nf = f"Neff={r['gi_neff_d']:.2f}" if r.get('gi_neff_d') else ''
        parts = [sp]
        if pg: parts.append(pg)
        if nf: parts.append(nf)
        return ' '.join(parts)
    elif lt in ('景氣循環', '轉機股'):
        npe = f"正常化PE={r['_norm_pe']:.1f}" if r.get('_norm_pe') else 'PE—'
        rp = f"進度{r['_rp']:.2f}" if r.get('_rp') is not None else ''
        sd = r.get('_sd', '')
        parts = [npe]
        if rp: parts.append(rp)
        if sd: parts.append(f'訊號{sd}')
        return ' '.join(parts)
    return '—'


def _type_label(r):
    """林區分類的簡短標籤"""
    lt = r.get('_lt', '')
    labels = {'緩慢成長': '價值', '穩健股': '價值', '快速成長': '成長', '景氣循環': '循環', '轉機股': '循環'}
    return labels.get(lt, '—')


def fmt_unified(r):
    """統一格式行"""
    c, n, cl = r['code'], (r.get('name') or '')[:8], r['close']
    return (f"| {c} | {n}{_liq(r)} | {_type_label(r)} | {r.get('fin_grade_1', '')} | {cl} "
            f"| {_fmt_m3(r)} | {_fmt_m12(r)} | {_fmt_rc(r)} | {_fmt_pe(r)} | {_fmt_pe_range(r)} "
            f"| {_fmt_yld(r)} | {_fmt_key_indicator(r)} | {_fl(r)} |")


# ══════════════════════════════════════════════════════════════
# 排序（決議#12）
# ══════════════════════════════════════════════════════════════

def sort_value(codes, stocks):
    """按折價%排（離val_aa越近越前）"""
    def _key(c):
        r = stocks[c]
        val_aa = r.get('val_aa')
        if val_aa and val_aa > 0:
            return (r['close'] - val_aa) / val_aa
        return 999
    return sorted(codes, key=_key)


def sort_growth(codes, stocks):
    """按 PEG 由低到高"""
    def _key(c):
        peg = stocks[c].get('gi_lynch_d')
        return peg if peg is not None else 999
    return sorted(codes, key=_key)


def sort_cycle(codes, stocks):
    """按正常化PE由低到高"""
    def _key(c):
        return stocks[c].get('_norm_pe', 999)
    return sorted(codes, key=_key)


# ══════════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════════

def _evaluate_stock(r):
    """對單支股票執行林區分類 + 防呆 + 估值，回傳 (level, reason, eval_type)
    level: 'heavy'/'small'/'watch'/None
    eval_type: '價值'/'成長'/'循環'/None
    """
    lt, amp, cls_reason = classify_lynch(r)
    if lt is None:
        return None, f'分類失敗: {cls_reason}', None
    r['_lt'] = lt

    flags, ml = check_defenses(r)

    if '股本變動>50%' in flags and lt == '景氣循環':
        return None, '股本變動>50%排除', '循環'

    if lt in ('緩慢成長', '穩健股'):
        level, reason = evaluate_value(r)
        return level, reason, '價值'
    elif lt == '快速成長':
        level, reason = evaluate_growth(r)
        return level, reason, '成長'
    elif lt in ('景氣循環', '轉機股'):
        level, reason = evaluate_cycle(r)
        return level, reason, '循環'
    return None, '', None


def _sort_section(codes, stocks_dict):
    """區塊內排序：重倉/小買/觀望各自按估值類型排"""
    def _key(c):
        r = stocks_dict[c]
        lt = r.get('_lt', '')
        if lt in ('緩慢成長', '穩健股'):
            val_aa = r.get('val_aa')
            if val_aa and val_aa > 0:
                return (r['close'] - val_aa) / val_aa
            return 999
        elif lt == '快速成長':
            peg = r.get('gi_lynch_d')
            return peg if peg is not None else 999
        elif lt in ('景氣循環', '轉機股'):
            return r.get('_norm_pe', 999)
        return 999
    return sorted(codes, key=_key)


def _render_section(L, title, desc, heavy, small, watch, stocks_dict, eval_log):
    """渲染單一區塊（追蹤/觀察/體質/全市場）的 markdown"""
    total = len(heavy) + len(small) + len(watch)
    L.append(f"---\n## {title}\n")
    if desc:
        L.append(f"> {desc}\n")
    L.append(f"**小計：** {len(heavy)}重/{len(small)}小/{len(watch)}望，合計 {total}\n")

    TH = "| 代碼 | 名稱 | 類型 | 等級 | 股價 | 3M | 12M | 累積 | PE面 | PE區間 | 殖利率面 | 關鍵指標 | 防呆 |\n|------|------|------|------|------|-----|------|------|------|--------|----------|----------|------|"
    TH_W = "| 代碼 | 名稱 | 類型 | 等級 | 股價 | 3M | 12M | 累積 | PE面 | PE區間 | 殖利率面 | 關鍵指標 | 等什麼 |\n|------|------|------|------|------|-----|------|------|------|--------|----------|----------|--------|"

    if heavy:
        L.append("### 重倉候選\n")
        L.append(TH)
        for c in heavy:
            L.append(fmt_unified(stocks_dict[c]))
        L.append("")

    if small:
        L.append("### 小買候選\n")
        L.append(TH)
        for c in small:
            L.append(fmt_unified(stocks_dict[c]))
        L.append("")

    if watch:
        L.append("### 觀望\n")
        L.append(TH_W)
        for c in watch:
            row = fmt_unified(stocks_dict[c])
            reason = eval_log.get(c, '').split('→')[-1].split(':')[-1].strip() if c in eval_log else ''
            # 替換最後的防呆欄為等什麼
            parts = row.rsplit('|', 2)
            L.append(f"{parts[0]}| {reason} |")
        L.append("")

    if not heavy and not small and not watch:
        L.append("無符合條件的標的。\n")


def run_full(dry_run=False):
    """完整篩選 — 四區塊架構（追蹤→觀察→體質→全市場）"""
    stocks, _, _, track_set, watch_set, quality_set = load_all_data()
    today = date.today().isoformat()

    # 去重分組（優先順序：追蹤 > 觀察 > 體質 > 未勾選）
    # 只取有股價資料的
    track_codes = sorted(c for c in track_set if c in stocks)
    watch_codes = sorted(c for c in watch_set - track_set if c in stocks)
    quality_codes = sorted(c for c in quality_set - track_set - watch_set if c in stocks)
    remaining_codes = sorted(c for c in stocks if c not in track_set and c not in watch_set and c not in quality_set)

    # 對各組執行估值
    eval_log = {}
    all_results = {}  # code -> (level, eval_type)

    def _process_group(codes, skip_trust=False):
        """處理一組股票，回傳 (heavy, small, watch) 的 code 列表"""
        h, s, w = [], [], []
        for code in codes:
            r = stocks[code]

            # 信任門檻（追蹤/觀察/體質跳過）
            if not skip_trust:
                ok, reason = check_trust(r)
                if not ok:
                    eval_log[code] = f'信任門檻排除: {reason}'
                    continue

            level, reason, eval_type = _evaluate_stock(r)
            if eval_type:
                eval_log[code] = f'{r.get("_lt", "")}/{eval_type} → {level or "未達標"}: {reason}'
            else:
                eval_log[code] = f'分類失敗: {reason}'

            if level == 'heavy':
                h.append(code)
            elif level == 'small':
                s.append(code)
            elif level == 'watch':
                w.append(code)

            if level:
                all_results[code] = (level, eval_type)

        h = _sort_section(h, stocks)
        s = _sort_section(s, stocks)
        w = _sort_section(w, stocks)
        return h, s, w

    t_h, t_s, t_w = _process_group(track_codes, skip_trust=True)
    w_h, w_s, w_w = _process_group(watch_codes, skip_trust=True)
    q_h, q_s, q_w = _process_group(quality_codes, skip_trust=True)
    r_h, r_s, r_w = _process_group(remaining_codes, skip_trust=False)

    # 統計
    t_total = len(t_h) + len(t_s) + len(t_w)
    w_total = len(w_h) + len(w_s) + len(w_w)
    q_total = len(q_h) + len(q_s) + len(q_w)
    r_total = len(r_h) + len(r_s) + len(r_w)
    grand_total = t_total + w_total + q_total + r_total

    # 產出 Markdown
    L = []
    L.append(f"# 選股推薦 {today}\n")
    L.append(f"**統計：** 追蹤 {t_total} | 觀察 {w_total} | 體質 {q_total} | 全市場 {r_total} | 合計 {grand_total}\n")
    L.append("> **估值框架**：依林區分類自動套用 — 價值（逍遙評價法）/ 成長（PEG/Neff）/ 循環（正常化PE+復甦進度）")
    L.append("> **類型欄**：價值=緩慢成長+穩健股 / 成長=快速成長 / 循環=景氣循環+轉機股")
    L.append("> **追蹤/觀察/體質**：跳過信任門檻 / **全市場**：套用信任門檻（排除KY、金融、上市<5年等）")
    L.append("> **同一支股票只出現在最高優先區塊**（追蹤 > 觀察 > 體質 > 全市場）\n")

    _render_section(L, f'一、追蹤清單（{len(track_codes)}支）',
                    '你最關注的標的，優先處理',
                    t_h, t_s, t_w, stocks, eval_log)

    _render_section(L, f'二、觀察清單（{len(watch_codes)}支）',
                    '已進入觀察但還沒追蹤的標的',
                    w_h, w_s, w_w, stocks, eval_log)

    _render_section(L, f'三、體質清單（{len(quality_codes)}支）',
                    '機器篩出體質好的標的（扣除追蹤+觀察）',
                    q_h, q_s, q_w, stocks, eval_log)

    _render_section(L, f'四、全市場掃描（達標 {r_total} 支）',
                    '不在任何清單中但估值達標的標的（套用信任門檻）',
                    r_h, r_s, r_w, stocks, eval_log)

    L.append(f"\n分析日期：{today}")

    content = "\n".join(L)

    # 輸出統計
    print(f"追蹤: {len(track_codes)}支 → {t_total}達標 ({len(t_h)}H/{len(t_s)}S/{len(t_w)}W)")
    print(f"觀察: {len(watch_codes)}支 → {w_total}達標 ({len(w_h)}H/{len(w_s)}S/{len(w_w)}W)")
    print(f"體質: {len(quality_codes)}支 → {q_total}達標 ({len(q_h)}H/{len(q_s)}S/{len(q_w)}W)")
    print(f"全市場: {len(remaining_codes)}支 → {r_total}達標 ({len(r_h)}H/{len(r_s)}S/{len(r_w)}W)")
    print(f"合計: {grand_total}")

    if not dry_run:
        try:
            data = json.dumps({'content': content}).encode('utf-8')
            req = urllib.request.Request(RENDER_PICKS_URL, data=data,
                                         headers={'Content-Type': 'application/json'}, method='POST')
            resp = urllib.request.urlopen(req, timeout=15)
            result = json.loads(resp.read()).get('status', 'error')
            print(f"Render push: {result}")
        except Exception as e:
            print(f"Render push failed: {e}")
    else:
        print("[dry-run] 不推送 Render")
        print(content)


def run_check(code):
    """驗證單支股票的完整判斷路徑"""
    stocks, _, _, track_set, watch_set, quality_set = load_all_data()
    if code not in stocks:
        print(f"找不到 {code}")
        return

    r = stocks[code]
    in_lists = []
    if code in track_set: in_lists.append('追蹤')
    if code in watch_set: in_lists.append('觀察')
    if code in quality_set: in_lists.append('體質')
    list_info = '、'.join(in_lists) if in_lists else '未勾選'
    print(f"═══ {code} {r.get('name', '')} ═══")
    print(f"股價: {r['close']}  產業: {r.get('industry', '')}  日均量: {r.get('volume', 0)}")
    print(f"清單: {list_info}（{'跳過信任門檻' if in_lists else '需通過信任門檻'}）")
    print()

    # 信任門檻（追蹤/觀察/體質跳過）
    if not in_lists:
        ok, reason = check_trust(r)
        print(f"【信任門檻】{'通過' if ok else f'排除 — {reason}'}")
        if not ok:
            return
    else:
        print(f"【信任門檻】跳過（在{list_info}清單中）")

    # 林區分類
    lt, amp, cls_reason = classify_lynch(r)
    print(f"【林區分類】{lt or '無法分類'} — {cls_reason}")
    if lt is None:
        return
    r['_lt'] = lt

    # EPS 資料
    print(f"  EPS 6年: {r.get('_eps_vals', [])}")
    print(f"  振幅: {amp:.2f}x" if amp else "  振幅: 無法計算")
    print()

    # 防呆
    flags, ml = check_defenses(r)
    print(f"【防呆】flags={flags}, max_level={ml}")
    print()

    # 估值
    if lt in ('緩慢成長', '穩健股'):
        level, reason = evaluate_value(r)
        print(f"【價值精選】{level or '未達標'}")
        print(f"  {reason}")
        print(f"  股價={r['close']} val_aa={r.get('val_aa')} val_a={r.get('val_a')}")
        print(f"  3M={r.get('gi_rev_3m_yoy')} 12M={r.get('gi_rev_12m_yoy')} 累積={r.get('revenue_cum_yoy')}")
        print(f"  等級: fg1={r.get('fin_grade_1')} fg2={r.get('fin_grade_2')}")
        print(f"  blend_pe={r.get('blend_pe')} blend_yld={r.get('blend_yld')}")
        print(f"  PE區間: {r.get('_pe_low')}/{r.get('_pe_mid')}/{r.get('_pe_high')}")

    elif lt == '快速成長':
        level, reason = evaluate_growth(r)
        print(f"【成長精選】{level or '未達標'}")
        print(f"  {reason}")
        print(f"  沈董PE={r.get('shen_pe')} PEG={r.get('gi_lynch_d')}{'(灰)' if r.get('gi_lynch_gray') else ''} Neff={r.get('gi_neff_d')}{'(灰)' if r.get('gi_neff_gray') else ''}")
        print(f"  ROIC={r.get('gi_roic_avg')} CAGR3={r.get('gi_rev_cagr_3y')}")
        print(f"  3M={r.get('gi_rev_3m_yoy')} 累積={r.get('revenue_cum_yoy')}")

    elif lt in ('景氣循環', '轉機股'):
        level, reason = evaluate_cycle(r)
        print(f"【循環反轉】{level or '未達標'}")
        print(f"  {reason}")
        print(f"  近4季EPS={r.get('eps_4q_sum')} 正常化EPS={r.get('_norm_eps')}({r.get('_norm_method', '')})")
        print(f"  正常化PE={r.get('_norm_pe')} 復甦進度={r.get('_rp')}")
        print(f"  復甦訊號={r.get('_sig')}/3 ({r.get('_sd', '')})")
        print(f"  循環位置={r.get('_cp')}")
        print(f"  毛利率: 最新={r.get('_gm_latest')}% 5年均={r.get('_gm_avg5')}%")


def run_excluded():
    """列出所有被排除的股票及原因"""
    stocks, _, _, _, _, _ = load_all_data()

    reasons = {}
    for code, r in stocks.items():
        ok, reason = check_trust(r)
        if not ok:
            reasons.setdefault(reason, []).append(f"{code} {r.get('name', '')}")

    for reason, items in sorted(reasons.items(), key=lambda x: -len(x[1])):
        print(f"\n【{reason}】({len(items)}支)")
        for item in sorted(items)[:20]:
            print(f"  {item}")
        if len(items) > 20:
            print(f"  ... 及其他 {len(items) - 20} 支")


# ══════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='逍遙投資系統選股推薦引擎')
    parser.add_argument('--check', type=str, help='驗證單支股票（輸入代碼）')
    parser.add_argument('--excluded', action='store_true', help='列出所有被排除的股票')
    parser.add_argument('--dry-run', action='store_true', help='完整篩選但不推送 Render')
    args = parser.parse_args()

    if args.check:
        run_check(args.check)
    elif args.excluded:
        run_excluded()
    else:
        run_full(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
