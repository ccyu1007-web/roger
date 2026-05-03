"""
後端 API：Flask
提供股票資料給前端網頁
"""

import logging
from flask import Flask, jsonify, request
import os
import db as sqlite3
import threading

logger = logging.getLogger(__name__)
from guardian import (generate_health_report, get_provider_status, PROVIDER_TIERS,
                      get_all_breakers, get_breaker,
                      get_quarantine_list, resolve_quarantine,
                      get_fingerprint_stats, get_coverage_map,
                      get_audit_log, get_daily_briefing,
                      get_recent_news,
                      cross_validate, get_latest_validation)
from scraper import (run as scraper_run, refresh_prices, init_db, init_financial_db,
                     init_monthly_revenue_db, init_quarterly_db,
                     init_pe_history_db, fetch_company_financials,
                     fetch_company_monthly_revenue, fetch_company_quarterly,
                     fetch_pe_history, _calc_fin_grade, fetch_institutional,
                     quick_update, estimate_system_eps, estimate_system_eps_multi,
                     estimate_annual_eps, _log_estimate, _fix_tax_data,
                     cross_validate_financial)
from etf_fetcher import (init_etf_db, get_stock_etf_membership,
                         get_etf_holdings_list, get_etf_changes)
from capital_fetcher import fetch_financial_detail

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

app = Flask(__name__, static_folder=".", static_url_path="")
app.config['COMPRESS_MIMETYPES'] = ['application/json']
DB_PATH = "stocks.db"

def _bg_push_table(table, columns, pk, create_sql=None, where=None, clear_first=False):
    """背景 push 單一表到 Render（僅本機執行）"""
    if os.environ.get('DATABASE_URL'):
        return
    def _do():
        try:
            from render_sync import _push_table_to_render
            _push_table_to_render(table=table, columns=columns, pk=pk,
                                  create_sql=create_sql, where=where, clear_first=clear_first)
        except Exception as e:
            print(f"[bg_push] {table} 失敗: {e}")
    threading.Thread(target=_do, daemon=True).start()

# ── Sync API Token 驗證 ─────────────────────────────────────
SYNC_TOKEN = os.environ.get('SYNC_TOKEN', 'stock-sync-2026')

def check_sync_token():
    """驗證 sync/refresh API 的 token"""
    token = request.headers.get('X-Sync-Token') or request.args.get('token')
    if token != SYNC_TOKEN:
        return False
    return True


# ── 快取控制 ──────────────────────────────────────────────
# HTML 不快取（確保載入最新版），API JSON 短快取（減少重複請求）
@app.after_request
def add_cache_headers(response):
    if response.content_type and 'text/html' in response.content_type:
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        # 移除 ETag 避免瀏覽器 304 快取
        response.headers.pop('ETag', None)
        response.headers.pop('Last-Modified', None)
    return response

# ── 回應壓縮 ──────────────────────────────────────────────
try:
    from flask_compress import Compress
    Compress(app)
except ImportError:
    pass  # Render 上會安裝，本機沒有就不壓縮

# ── 股票資料快取（避免每次都查 DB）──────────────────────────
_stocks_cache = None
_stocks_cache_time = 0

# ── 爬蟲狀態鎖（避免同時跑兩次）──────────────────────────
_refresh_lock   = threading.Lock()
_is_refreshing  = False

def query_db(sql, args=()):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(sql, args)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

# ── 沈董EPS/股利/綜合股利 後端計算 ────────────────────────────
DEFAULT_WEIGHTS = [30, 30, 20, 10, 10]

def _calc_shen_fields(r, cur_roc):
    """計算沈董EPS、沈董股利、綜合股利，寫入 row dict"""
    # 沈董EPS
    all_eps = []
    for i in range(1, 6):
        q = r.get(f'eps_{i}q')
        v = r.get(f'eps_{i}')
        if q and v is not None:
            all_eps.append((q, v))
    cur_year = [(q, v) for q, v in all_eps if q and int(q.split('Q')[0]) == cur_roc]
    n = len(cur_year)
    is_fallback = False
    if n >= 4:
        r['shen_eps'] = round(sum(v for _, v in cur_year), 2)
    elif n > 0:
        s = sum(v for _, v in cur_year)
        r['shen_eps'] = round(s / n * 4, 2)
    else:
        eps4 = [r.get(f'eps_{i}') for i in range(1, 5)]
        eps4 = [v for v in eps4 if v is not None]
        r['shen_eps'] = round(sum(eps4), 2) if len(eps4) == 4 else (r.get('eps_y1') or r.get('eps_ytd'))
        is_fallback = True

    shen_eps = r.get('shen_eps')

    # 配息率（同年度 EPS × 股利配對）
    eps_map = {}
    div_map = {}
    for i in range(1, 7):
        el = r.get(f'eps_y{i}_label')
        ev = r.get(f'eps_y{i}')
        if el and ev: eps_map[str(el)] = ev
        dl = r.get(f'div_{i}_label')
        dc = r.get(f'div_c{i}') or 0
        ds = r.get(f'div_s{i}') or 0
        if dl and (dc + ds) > 0: div_map[str(dl)] = dc + ds

    # 加權股利
    ws = DEFAULT_WEIGHTS
    wdiv = wsum = 0
    for i in range(1, 7):
        dc = r.get(f'div_c{i}') or 0
        ds = r.get(f'div_s{i}') or 0
        w = ws[i - 1] if i <= 5 else 0
        if (dc + ds) > 0 and w > 0:
            wdiv += (dc + ds) * w / 100
            wsum += w
    r['weighted_div'] = round(wdiv * 100) / 100 if wsum > 0 else None

    # 加權配息率
    sorted_years = sorted(eps_map.keys(), key=lambda x: int(x), reverse=True)
    payout_pairs = []
    for yr in sorted_years:
        if yr in div_map and eps_map[yr] > 0:
            payout_pairs.append(min(div_map[yr] / eps_map[yr], 1.0))
        if len(payout_pairs) >= 5:
            break
    wpS = wpW = 0
    for i, p in enumerate(payout_pairs):
        w = ws[i] if i < 5 else 0
        if w > 0:
            wpS += p * w
            wpW += w
    weighted_payout = wpS / wpW if wpW > 0 else None

    # 沈董股利
    r['shen_div'] = None
    r['_shen_div_formula'] = None
    if shen_eps and shen_eps > 0:
        if is_fallback:
            lbl = str(r.get('eps_y1_label') or '')
            if lbl in div_map:
                r['shen_div'] = round(div_map[lbl] * 100) / 100
                r['_shen_div_formula'] = f'{lbl}年實際股利 = {r["shen_div"]}'
        if r['shen_div'] is None and weighted_payout is not None:
            r['shen_div'] = round(shen_eps * weighted_payout * 100) / 100
            r['_shen_div_formula'] = f'沈董EPS{shen_eps} × 加權配息率{round(weighted_payout*100,1)}% = {r["shen_div"]}'
        if r['shen_div'] is None:
            for i in range(1, 7):
                dc = r.get(f'div_c{i}')
                if dc and dc > 0:
                    r['shen_div'] = dc
                    r['_shen_div_formula'] = f'最近現金股利 = {dc}'
                    break

    # 綜合股利 = 沈董股利 × 60% + 加權股利 × 40%（預設，前端可覆蓋）
    sd = r.get('shen_div')
    wd = r.get('weighted_div')
    if sd is not None and wd is not None:
        r['blend_div'] = round((sd * 0.6 + wd * 0.4) * 100) / 100
        r['_blend_div_formula'] = f'沈董股利{sd}×60% + 加權股利{wd}×40% = {r["blend_div"]}'
    elif sd is not None:
        r['blend_div'] = round(sd * 100) / 100
        r['_blend_div_formula'] = f'沈董股利{sd}（無加權股利）'
    elif wd is not None:
        r['blend_div'] = round(wd * 100) / 100
        r['_blend_div_formula'] = f'加權股利{wd}（無沈董股利）'
    else:
        r['blend_div'] = None


# ── 檢核表計算 ─────────────────────────────────────────────

def _init_checklist_db():
    """建立 stock_checklist 資料表"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS stock_checklist (
        code TEXT PRIMARY KEY,
        chk_1 INTEGER, chk_2 INTEGER, chk_3 INTEGER, chk_4 INTEGER,
        chk_5 INTEGER, chk_6 INTEGER, chk_7 INTEGER, chk_8 INTEGER,
        chk_9 INTEGER, chk_10 INTEGER, chk_11 INTEGER, chk_12 INTEGER,
        chk_13 INTEGER,
        pass_count INTEGER, total_count INTEGER DEFAULT 13,
        detail TEXT,
        eps_setting REAL, div_setting REAL,
        yld_high REAL, yld_max REAL, pe_high REAL, pe_low REAL,
        lt_div REAL, lt_yld REAL,
        val_a REAL, val_a1 REAL, val_a2 REAL, val_aa REAL,
        lt5 REAL, lt6 REAL, lt7 REAL,
        updated_at TEXT
    )""")
    # 既有表加欄位
    for col, typ in [('chk_13','INTEGER'),
                     ('eps_setting','REAL'),('div_setting','REAL'),
                     ('yld_high','REAL'),('yld_max','REAL'),('pe_high','REAL'),('pe_low','REAL'),
                     ('lt_div','REAL'),('lt_yld','REAL'),
                     ('val_a','REAL'),('val_a1','REAL'),('val_a2','REAL'),('val_aa','REAL'),
                     ('lt5','REAL'),('lt6','REAL'),('lt7','REAL'),
                     ('base_count','INTEGER'),('bonus_count','INTEGER')]:
        try: conn.execute(f"ALTER TABLE stock_checklist ADD COLUMN {col} {typ}")
        except Exception: pass
    conn.commit()
    conn.close()

def _is_grade_above_b(g):
    """財務等級是否 B 級以上（不含帶 - 的，不含 C/D/X）"""
    if not g or g == '-':
        return False
    if g.endswith('-'):
        return False
    base = g.replace('+', '').replace('-', '')
    return base in ('AA','A1','A2','A','B2A','B1A','B1','B2','BA1','BA2')

def _is_grade_a(g):
    """等級是否 A 級以上（AA/A1/A2/A，含 +）"""
    if not g or g in ('-', 'X'):
        return False
    base = g.replace('+', '').replace('-', '')
    return base in ('AA','A1','A2','A')

def _is_grade_aa(g):
    """等級是否 AA 級"""
    if not g or g in ('-', 'X'):
        return False
    base = g.replace('+', '').replace('-', '')
    return base == 'AA'

def _calc_matrix_grade(pe, yld, pe_hi=18, pe_lo=10, y_high=5.5, y_max=6, y_floor=5):
    """PE×殖利率矩陣等級"""
    if pe is None or pe <= 0 or yld is None or yld <= 0:
        return None
    pe_fair = (pe_hi + pe_lo) / 2
    pe_above = (pe_hi + pe_fair) / 2
    pe_below = (pe_fair + pe_lo) / 2
    pe_cols = [(-999999, pe_lo), (pe_lo, pe_below), (pe_below, pe_fair),
               (pe_fair, pe_above), (pe_above, pe_hi), (pe_hi, 999999)]
    y_rows = [(y_max, 999999), (y_high, y_max), (y_floor, y_high), (-999999, y_floor)]
    grades = [
        ['AA','A2','BA2','觀察','臨界點','X'],
        ['A1','A','B2','臨界點','X','X'],
        ['BA1','B1','臨界點','X','X','X'],
        ['觀察','臨界點','X','X','X','X'],
    ]
    col = next((i for i, c in enumerate(pe_cols) if pe >= c[0] and pe < c[1]), -1)
    row = next((i for i, y in enumerate(y_rows) if yld >= y[0] and yld < y[1]), -1)
    if col >= 0 and row >= 0:
        return grades[row][col]
    return None

def _calc_checklist_for_stock(r, user_params=None):
    """計算單支股票的 12 項檢核，r 為 stocks 表的 row dict（已含 shen 欄位）"""
    import json
    checks = {}
    detail = {}

    # 讀取個股參數
    pe_hi, pe_lo, y_high, y_max = 18, 10, 5.5, 6
    est_eps_user, est_div_user = None, None
    if user_params:
        if user_params.get('peHigh'): pe_hi = float(user_params['peHigh'])
        if user_params.get('peLow'): pe_lo = float(user_params['peLow'])
        if user_params.get('yldHigh'): y_high = float(user_params['yldHigh'])
        if user_params.get('yldMax'): y_max = float(user_params['yldMax'])
        qs = [user_params.get(f'q{i}') for i in range(1, 5)]
        qs = [float(v) for v in qs if v]
        if qs:
            est_eps_user = round(sum(qs), 2)
        if user_params.get('div'):
            est_div_user = float(user_params['div'])

    close = r.get('close')
    shen_eps = r.get('shen_eps')
    shen_div = r.get('shen_div')
    blend_div = r.get('blend_div')
    weighted_div = r.get('weighted_div')

    # 沈董EPS 計算過程（用於 chk_4 detail）
    from datetime import date as _date
    _cur_roc = _date.today().year - 1911
    _all_eps = []
    for i in range(1, 6):
        q = r.get(f'eps_{i}q')
        v = r.get(f'eps_{i}')
        if q and v is not None:
            _all_eps.append((q, v))
    _cur_year = [(q, v) for q, v in _all_eps if q and int(q.split('Q')[0]) == _cur_roc]
    _n = len(_cur_year)
    if _n >= 4:
        _eps_parts = ' + '.join(f'{q}:{v}' for q, v in _cur_year)
        _shen_formula = f'({_eps_parts}) = {shen_eps}'
    elif _n > 0:
        _eps_parts = ' + '.join(f'{q}:{v}' for q, v in _cur_year)
        _s = round(sum(v for _, v in _cur_year), 2)
        _shen_formula = f'({_eps_parts}) / {_n} × 4 = {_s}/{_n}×4 = {shen_eps}'
    else:
        _eps4 = [(f'Q{i}', r.get(f'eps_{i}')) for i in range(1, 5) if r.get(f'eps_{i}') is not None]
        if len(_eps4) == 4:
            _eps_parts = ' + '.join(f'{q}:{v}' for q, v in _eps4)
            _shen_formula = f'近四季({_eps_parts}) = {shen_eps}'
        else:
            _shen_formula = f'年度EPS = {shen_eps}'

    # 沈董 PE / 殖利率
    shen_pe = round(close / shen_eps, 2) if shen_eps and shen_eps > 0 and close else None
    shen_yld = round(shen_div / close * 100, 2) if shen_div and shen_div > 0 and close and close > 0 else None

    # 沈董等級
    if shen_eps is not None and shen_eps <= 0:
        shen_grade = 'X'
    elif shen_pe and shen_yld:
        shen_grade = _calc_matrix_grade(shen_pe, shen_yld, pe_hi, pe_lo, y_high, y_max)
    else:
        shen_grade = None

    # 加權 EPS
    weighted_eps = None
    dw = [30, 25, 20, 15, 10]
    we = ws2 = 0
    for i in range(1, 6):
        e = r.get(f'eps_y{i}')
        w = dw[i - 1]
        if e is not None and w > 0:
            we += e * w / 100
            ws2 += w
    if ws2 > 0:
        weighted_eps = round(we, 2)
    weighted_pe = round(close / weighted_eps, 2) if weighted_eps and weighted_eps > 0 and close else None
    weighted_yld = round(weighted_div / close * 100, 2) if weighted_div and weighted_div > 0 and close and close > 0 else None

    # 加權等級
    if weighted_eps is not None and weighted_eps <= 0:
        weighted_grade = 'X'
    elif weighted_pe and weighted_yld:
        weighted_grade = _calc_matrix_grade(weighted_pe, weighted_yld, pe_hi, pe_lo, y_high, y_max)
    else:
        weighted_grade = None

    # 綜合 EPS
    bs, bw = 0.5, 0.5
    blend_eps = None
    if shen_eps is not None and weighted_eps is not None:
        blend_eps = round(shen_eps * bs + weighted_eps * bw, 2)
    elif shen_eps is not None:
        blend_eps = shen_eps
    elif weighted_eps is not None:
        blend_eps = weighted_eps
    blend_pe = round(close / blend_eps, 2) if blend_eps and blend_eps > 0 and close else None
    blend_yld = round(blend_div / close * 100, 2) if blend_div and blend_div > 0 and close and close > 0 else None

    # 綜合等級
    if blend_eps is not None and blend_eps <= 0:
        blend_grade = 'X'
    elif blend_pe and blend_yld:
        blend_grade = _calc_matrix_grade(blend_pe, blend_yld, pe_hi, pe_lo, y_high, y_max)
    else:
        blend_grade = None

    # 預估 EPS/股利（預估 > 沈董）
    est_eps = est_eps_user if est_eps_user is not None else None
    est_div = est_div_user if est_div_user is not None else None
    est_pe = round(close / est_eps, 2) if est_eps and est_eps > 0 and close else None
    est_yld = round(est_div / close * 100, 2) if est_div and est_div > 0 and close and close > 0 else None
    if est_eps is not None and est_eps <= 0:
        est_grade = 'X'
    elif est_pe and est_yld:
        est_grade = _calc_matrix_grade(est_pe, est_yld, pe_hi, pe_lo, y_high, y_max)
    else:
        est_grade = None

    # 評價門檻（預估 > 沈董）
    val_eps = est_eps if est_eps is not None else shen_eps
    val_div = est_div if est_div is not None else shen_div
    def _calc_val(pe_val, yld_val):
        if val_eps is None or val_eps <= 0 or val_div is None or val_div <= 0:
            return None
        v1 = val_eps * pe_val
        v2 = val_div / (yld_val / 100)
        candidates = [v1, v2]
        if blend_div and blend_div > 0:
            candidates.append(blend_div / 0.06 + val_div)
        return round(min(candidates), 2)

    pe_mid = (pe_hi + pe_lo) / 2
    pe_lo_bias = (pe_mid + pe_lo) / 2
    val_aa = _calc_val(pe_lo, y_max)
    val_a1 = _calc_val(pe_lo, y_high)
    val_a2 = _calc_val(pe_lo_bias, y_max)
    val_a = _calc_val(pe_lo_bias, y_high)
    lt_div = blend_div
    lt_yld = 6
    lt5 = round(lt_div / 0.05, 2) if lt_div and lt_div > 0 else None
    lt6 = round(lt_div / 0.06, 2) if lt_div and lt_div > 0 else None
    lt7 = round(lt_div / 0.07, 2) if lt_div and lt_div > 0 else None

    # === 基本門檻 6 項 + 成長加分 4 項 ===

    # ── 基本門檻 ──

    # 1. 近五年財務等級：有3年以上B級以上，且近兩年都B級以上
    grades5 = [r.get(f'fin_grade_{i}') for i in range(1, 6)]
    grades5y = [r.get(f'fin_grade_{i}y') for i in range(1, 6)]
    above_b = sum(1 for g in grades5 if _is_grade_above_b(g))
    recent2 = _is_grade_above_b(grades5[0]) and _is_grade_above_b(grades5[1]) if len(grades5) >= 2 else False
    checks[1] = 1 if above_b >= 3 and recent2 else 0
    detail['chk_1'] = ' / '.join(
        f'{grades5y[i] or ""}:{grades5[i]}' if grades5[i] else '--'
        for i in range(5)
    )

    # 2. 累積營收年增率 >= 0%
    cum_yoy = r.get('revenue_cum_yoy')
    checks[2] = 1 if cum_yoy is not None and cum_yoy >= 0 else 0
    detail['chk_2'] = f'{cum_yoy}%' if cum_yoy is not None else None

    # 3. 最佳等級 AA 級（預估>系統>沈董>綜合>加權>近四季）
    # 系統等級
    sys_grade = None
    sys_eps = r.get('sys_ann_eps')
    sys_div = r.get('sys_ann_div')
    if sys_eps is not None and sys_eps > 0 and close and close > 0 and sys_div is not None and sys_div > 0:
        sys_pe = round(close / sys_eps, 2)
        sys_yld = round(sys_div / close * 100, 2)
        sys_grade = _calc_matrix_grade(sys_pe, sys_yld, pe_hi, pe_lo, y_high, y_max)
    elif sys_eps is not None and sys_eps <= 0:
        sys_grade = 'X'

    # 近四季等級
    trail_eps = r.get('eps_4q_sum')
    trail_div = shen_div  # 近四季用沈董股利
    trail_grade = None
    if trail_eps and trail_eps > 0 and close and close > 0 and trail_div and trail_div > 0:
        trail_pe = round(close / trail_eps, 2)
        trail_yld = round(trail_div / close * 100, 2)
        trail_grade = _calc_matrix_grade(trail_pe, trail_yld, pe_hi, pe_lo, y_high, y_max)
    elif trail_eps is not None and trail_eps <= 0:
        trail_grade = 'X'

    best_grade_order = [
        ('預估', est_grade),
        ('系統', sys_grade),
        ('沈董', shen_grade),
        ('綜合', blend_grade),
        ('加權', weighted_grade),
        ('近四季', trail_grade),
    ]
    best_grade = None
    best_grade_src = ''
    for src, g in best_grade_order:
        if g and g not in ('-', 'X', None):
            best_grade = g
            best_grade_src = src
            break
    checks[3] = 1 if _is_grade_aa(best_grade) else 0
    grade_summary = ' / '.join(f'{src}:{g or "-"}' for src, g in best_grade_order)
    detail['chk_3'] = f'最佳={best_grade_src}:{best_grade}　({grade_summary})'

    # 4. 目前股價低於總表評價 AA
    _eps_src = '預估EPS' if est_eps is not None else '沈董EPS'
    _div_src = '預估股利' if est_div is not None else '沈董股利'
    _val_param = f'EPS={val_eps}({_eps_src}) 股利={val_div}({_div_src})'
    checks[4] = 1 if close is not None and val_aa is not None and close <= val_aa + 0.005 else 0
    detail['chk_4'] = f'股價:{close} 評價AA:{val_aa}　{_val_param}' if val_aa is not None else None

    # 5. 綜合殖利率 >= 6%
    checks[5] = 1 if blend_yld is not None and blend_yld >= 6 else 0
    if blend_yld is not None:
        _blend_formula = r.get('_blend_div_formula') or ''
        detail['chk_5'] = f'殖利率={blend_yld}%　綜合股利{blend_div} / 股價{close} × 100　股利算法：{_blend_formula}'
    else:
        detail['chk_5'] = None

    # 6. 股利折現模式現價潛在年報酬 >= 10%
    ddm_eps = est_eps if est_eps is not None else shen_eps
    ddm_pe = float(user_params.get('ddmPE', 14)) if user_params and user_params.get('ddmPE') else 14
    ddm_rate = float(user_params.get('ddmRate', 0.10)) if user_params and user_params.get('ddmRate') else 0.10
    ddm_div = blend_div or shen_div
    ddm_ann_ret = None
    if ddm_eps and ddm_eps > 0 and close and close > 0 and ddm_div and ddm_div > 0:
        sell_price = ddm_eps * ddm_pe
        total_div = ddm_div * 3
        target_price = sell_price + total_div
        total_ret = (target_price - close) / close
        ddm_ann_ret = round((pow(1 + total_ret, 1/3) - 1) * 100, 2)
    checks[6] = 1 if ddm_ann_ret is not None and ddm_ann_ret >= 10 else 0
    if ddm_ann_ret is not None:
        detail['chk_6'] = f'年報酬={ddm_ann_ret}%　EPS={ddm_eps} PE={ddm_pe} 股利={ddm_div} 折現率={ddm_rate}'
    else:
        detail['chk_6'] = None

    # ── 成長加分 ──

    # 7. 聶夫保守成長率 >= 7%
    gi = r.get('_gi') or {}
    neff_a = gi.get('neff_a')
    neff_b = gi.get('neff_b')
    neff_c = gi.get('neff_c')
    _discount = gi.get('discount')
    checks[7] = 1 if neff_c is not None and neff_c >= 7 and not gi.get('neff_gray') else 0
    if neff_c is not None:
        _gray_note = '（灰色標記：不計入）' if gi.get('neff_gray') else ''
        detail['chk_7'] = f'保守成長率={neff_c}%　min(5年CAGR {neff_a}%, 3年CAGR {neff_b}%) × 折扣{_discount} = {neff_c}%{_gray_note}'
    else:
        detail['chk_7'] = None

    # 8. 聶夫Neff比率 >= 0.7
    neff_d = gi.get('neff_d')
    _gi_yld = gi.get('yield', 0)
    _gi_pe = gi.get('pe', 0)
    checks[8] = 1 if neff_d is not None and neff_d >= 0.7 and not gi.get('neff_gray') else 0
    if neff_d is not None:
        _total_ret = round(neff_c + _gi_yld, 2) if neff_c is not None else None
        _gray_note = '（灰色標記：不計入）' if gi.get('neff_gray') else ''
        detail['chk_8'] = f'Neff比率={neff_d}　(保守成長率{neff_c}% + 殖利率{_gi_yld}%) / PE{_gi_pe} = {_total_ret}/{_gi_pe} = {neff_d}{_gray_note}'
    else:
        detail['chk_8'] = None

    # 9. 林區PEG <= 1.0
    lynch_d = gi.get('lynch_d')
    checks[9] = 1 if lynch_d is not None and lynch_d <= 1.0 else 0
    if lynch_d is not None:
        _total_ret = round(neff_c + _gi_yld, 2) if neff_c is not None else None
        _lynch_gray_note = '（景氣循環股：PEG參考用）' if gi.get('lynch_gray') else ''
        detail['chk_9'] = f'PEG={lynch_d}　PE{_gi_pe} / (保守成長率{neff_c}% + 殖利率{_gi_yld}%) = {_gi_pe}/{_total_ret} = {lynch_d}{_lynch_gray_note}'
    else:
        detail['chk_9'] = None

    # 10. 林區成長一致性 >= 0.5
    lynch_b = gi.get('lynch_b')
    lynch_c = gi.get('lynch_c')
    checks[10] = 1 if lynch_c is not None and lynch_c >= 0.5 else 0
    if lynch_c is not None:
        detail['chk_10'] = f'一致性={lynch_c}　1 - |算術平均{lynch_b}% - CAGR{neff_a}%| / CAGR{neff_a}% = {lynch_c}'
    else:
        detail['chk_10'] = None

    base_count = sum(checks[i] for i in range(1, 7))
    bonus_count = sum(checks[i] for i in range(7, 11))
    pass_count = base_count + bonus_count

    return {
        'code': r['code'],
        **{f'chk_{i}': checks.get(i, 0) for i in range(1, 11)},
        'pass_count': pass_count,
        'total_count': 10,
        'base_count': base_count,
        'bonus_count': bonus_count,
        'detail': json.dumps(detail, ensure_ascii=False),
        'eps_setting': val_eps,
        'div_setting': val_div,
        'yld_high': y_high,
        'yld_max': y_max,
        'pe_high': pe_hi,
        'pe_low': pe_lo,
        'lt_div': lt_div,
        'lt_yld': lt_yld,
        'val_a': val_a,
        'val_a1': val_a1,
        'val_a2': val_a2,
        'val_aa': val_aa,
        'lt5': lt5,
        'lt6': lt6,
        'lt7': lt7,
    }

def calc_all_checklists():
    """批次計算所有股票的檢核表並存入 DB"""
    import json
    from datetime import datetime
    _init_checklist_db()

    cur_roc = datetime.now().year - 1911
    rows = query_db("""SELECT code, name, close, change, revenue_cum_yoy,
                       eps_1, eps_1q, eps_2, eps_2q, eps_3, eps_3q, eps_4, eps_4q, eps_5, eps_5q,
                       eps_y1, eps_y1_label, eps_y2, eps_y2_label, eps_y3, eps_y3_label,
                       eps_y4, eps_y4_label, eps_y5, eps_y5_label, eps_y6, eps_y6_label,
                       eps_ytd, eps_ytd_label,
                       div_c1, div_s1, div_1_label, div_c2, div_s2, div_2_label,
                       div_c3, div_s3, div_3_label, div_c4, div_s4, div_4_label,
                       div_c5, div_s5, div_5_label, div_c6, div_s6, div_6_label,
                       fin_grade_1, fin_grade_1y, fin_grade_2, fin_grade_2y,
                       fin_grade_3, fin_grade_3y, fin_grade_4, fin_grade_4y, fin_grade_5, fin_grade_5y,
                       sys_ann_eps, sys_ann_div
                    FROM stocks""")

    # 批次讀取 user_estimates
    ue_map = {}
    try:
        ue_rows = query_db("SELECT code, params FROM user_estimates")
        for ue in ue_rows:
            if ue['params']:
                ue_map[ue['code']] = json.loads(ue['params'])
    except Exception:
        pass

    # 批次計算成長率指標（聶夫/林區）
    gi_map = {}
    try:
        with app.test_request_context():
            gi_resp = growth_indicators()
            gi_map = json.loads(gi_resp.data)
    except Exception as e:
        print(f"[Checklist] 成長率指標計算失敗: {e}")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    count = 0

    for r in rows:
        r = dict(r)
        _calc_shen_fields(r, cur_roc)
        r['_gi'] = gi_map.get(r['code'])
        r['eps_4q_sum'] = sum(r.get(f'eps_{i}') or 0 for i in range(1, 5)) if r.get('eps_1') is not None else None
        user_params = ue_map.get(r['code'])
        result = _calc_checklist_for_stock(r, user_params)

        c.execute("""INSERT INTO stock_checklist
                     (code, chk_1, chk_2, chk_3, chk_4, chk_5, chk_6,
                      chk_7, chk_8, chk_9, chk_10,
                      pass_count, total_count, base_count, bonus_count, detail,
                      eps_setting, div_setting, yld_high, yld_max, pe_high, pe_low,
                      lt_div, lt_yld, val_a, val_a1, val_a2, val_aa, lt5, lt6, lt7,
                      updated_at)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                     ON CONFLICT(code) DO UPDATE SET
                      chk_1=excluded.chk_1, chk_2=excluded.chk_2, chk_3=excluded.chk_3,
                      chk_4=excluded.chk_4, chk_5=excluded.chk_5, chk_6=excluded.chk_6,
                      chk_7=excluded.chk_7, chk_8=excluded.chk_8, chk_9=excluded.chk_9,
                      chk_10=excluded.chk_10,
                      pass_count=excluded.pass_count, total_count=excluded.total_count,
                      base_count=excluded.base_count, bonus_count=excluded.bonus_count,
                      detail=excluded.detail,
                      eps_setting=excluded.eps_setting, div_setting=excluded.div_setting,
                      yld_high=excluded.yld_high, yld_max=excluded.yld_max,
                      pe_high=excluded.pe_high, pe_low=excluded.pe_low,
                      lt_div=excluded.lt_div, lt_yld=excluded.lt_yld,
                      val_a=excluded.val_a, val_a1=excluded.val_a1,
                      val_a2=excluded.val_a2, val_aa=excluded.val_aa,
                      lt5=excluded.lt5, lt6=excluded.lt6, lt7=excluded.lt7,
                      updated_at=excluded.updated_at""",
                  (result['code'], result['chk_1'], result['chk_2'], result['chk_3'],
                   result['chk_4'], result['chk_5'], result['chk_6'],
                   result['chk_7'], result['chk_8'], result['chk_9'],
                   result['chk_10'],
                   result['pass_count'], result['total_count'],
                   result['base_count'], result['bonus_count'], result['detail'],
                   result['eps_setting'], result['div_setting'],
                   result['yld_high'], result['yld_max'], result['pe_high'], result['pe_low'],
                   result['lt_div'], result['lt_yld'],
                   result['val_a'], result['val_a1'], result['val_a2'], result['val_aa'],
                   result['lt5'], result['lt6'], result['lt7'], now))
        count += 1

    conn.commit()
    conn.close()
    global _stocks_cache_time
    _stocks_cache_time = 0  # 清快取，讓下次 API 重新查詢
    print(f"[Checklist] 已計算 {count} 支股票檢核表")
    return count


def _recalc_checklist_single(code):
    """重算單支股票的檢核表（儲存預估參數或股價更新後呼叫）"""
    import json
    from datetime import datetime
    _init_checklist_db()

    cur_roc = datetime.now().year - 1911
    rows = query_db("""SELECT code, name, close, change, revenue_cum_yoy,
                       eps_1, eps_1q, eps_2, eps_2q, eps_3, eps_3q, eps_4, eps_4q, eps_5, eps_5q,
                       eps_y1, eps_y1_label, eps_y2, eps_y2_label, eps_y3, eps_y3_label,
                       eps_y4, eps_y4_label, eps_y5, eps_y5_label, eps_y6, eps_y6_label,
                       eps_ytd, eps_ytd_label,
                       div_c1, div_s1, div_1_label, div_c2, div_s2, div_2_label,
                       div_c3, div_s3, div_3_label, div_c4, div_s4, div_4_label,
                       div_c5, div_s5, div_5_label, div_c6, div_s6, div_6_label,
                       fin_grade_1, fin_grade_1y, fin_grade_2, fin_grade_2y,
                       fin_grade_3, fin_grade_3y, fin_grade_4, fin_grade_4y, fin_grade_5, fin_grade_5y,
                       sys_ann_eps, sys_ann_div
                    FROM stocks WHERE code=?""", (code,))
    if not rows:
        return

    r = dict(rows[0])
    _calc_shen_fields(r, cur_roc)

    user_params = None
    try:
        ue = query_db("SELECT params FROM user_estimates WHERE code=?", (code,))
        if ue and ue[0]['params']:
            user_params = json.loads(ue[0]['params'])
    except Exception: pass

    result = _calc_checklist_for_stock(r, user_params)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""INSERT INTO stock_checklist
                 (code, chk_1, chk_2, chk_3, chk_4, chk_5, chk_6,
                  chk_7, chk_8, chk_9, chk_10, chk_11, chk_12, chk_13,
                  pass_count, total_count, detail,
                  eps_setting, div_setting, yld_high, yld_max, pe_high, pe_low,
                  lt_div, lt_yld, val_a, val_a1, val_a2, val_aa, lt5, lt6, lt7,
                  updated_at)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                 ON CONFLICT(code) DO UPDATE SET
                  chk_1=excluded.chk_1, chk_2=excluded.chk_2, chk_3=excluded.chk_3,
                  chk_4=excluded.chk_4, chk_5=excluded.chk_5, chk_6=excluded.chk_6,
                  chk_7=excluded.chk_7, chk_8=excluded.chk_8, chk_9=excluded.chk_9,
                  chk_10=excluded.chk_10, chk_11=excluded.chk_11, chk_12=excluded.chk_12,
                  chk_13=excluded.chk_13,
                  pass_count=excluded.pass_count, total_count=excluded.total_count,
                  detail=excluded.detail,
                  eps_setting=excluded.eps_setting, div_setting=excluded.div_setting,
                  yld_high=excluded.yld_high, yld_max=excluded.yld_max,
                  pe_high=excluded.pe_high, pe_low=excluded.pe_low,
                  lt_div=excluded.lt_div, lt_yld=excluded.lt_yld,
                  val_a=excluded.val_a, val_a1=excluded.val_a1,
                  val_a2=excluded.val_a2, val_aa=excluded.val_aa,
                  lt5=excluded.lt5, lt6=excluded.lt6, lt7=excluded.lt7,
                  updated_at=excluded.updated_at""",
              (result['code'], result['chk_1'], result['chk_2'], result['chk_3'],
               result['chk_4'], result['chk_5'], result['chk_6'],
               result['chk_7'], result['chk_8'], result['chk_9'],
               result['chk_10'], result['chk_11'], result['chk_12'], result['chk_13'],
               result['pass_count'], result['total_count'], result['detail'],
               result['eps_setting'], result['div_setting'],
               result['yld_high'], result['yld_max'], result['pe_high'], result['pe_low'],
               result['lt_div'], result['lt_yld'],
               result['val_a'], result['val_a1'], result['val_a2'], result['val_aa'],
               result['lt5'], result['lt6'], result['lt7'], now))
    conn.commit()
    conn.close()
    global _stocks_cache_time
    _stocks_cache_time = 0  # 清快取


# ── 取得全部股票 ────────────────────────────────────────────
@app.route("/api/stocks")
def get_stocks():
    import time as _time
    global _stocks_cache, _stocks_cache_time

    q      = request.args.get("q", "").strip()
    market = request.args.get("market", "")

    # 確保新欄位存在（Render PostgreSQL 可能還沒有）
    try:
        conn_init = sqlite3.connect(DB_PATH)
        for col, typ in [('revenue_year','INTEGER'),('revenue_month','INTEGER'),
                        ('revenue_note','TEXT'),('deepest_val_level','TEXT'),('val_cheap_days','INTEGER'),
                        ('sys_est_eps','REAL'),('sys_est_quarter','TEXT'),('sys_est_confidence','TEXT'),
                        ('sys_ann_eps','REAL'),('sys_ann_div','REAL'),('sys_ann_pe','REAL'),
                        ('sys_ann_yld','REAL'),('sys_ann_confidence','TEXT'),
                        ('priority_grade','TEXT'),('grade_source','TEXT')]:
            try: conn_init.execute(f"ALTER TABLE stocks ADD COLUMN {col} {typ}")
            except Exception: pass
        try: conn_init.commit()
        except Exception: pass
        conn_init.close()
    except Exception: pass

    sql    = """SELECT code, name, market, industry, close, change, change_240d, volume,
                       revenue_date, revenue_year, revenue_month,
                       revenue_yoy, revenue_mom, revenue_cum_yoy,
                       eps_date, eps_1, eps_1q, eps_2, eps_2q,
                       eps_3, eps_3q, eps_4, eps_4q, eps_5, eps_5q,
                       eps_y1, eps_y1_label, eps_y2, eps_y2_label,
                       eps_y3, eps_y3_label, eps_y4, eps_y4_label,
                       eps_y5, eps_y5_label, eps_y6, eps_y6_label,
                       eps_ytd, eps_ytd_label,
                       div_c1, div_s1, div_1_label, div_c2, div_s2, div_2_label,
                       div_c3, div_s3, div_3_label, div_c4, div_s4, div_4_label,
                       div_c5, div_s5, div_5_label, div_c6, div_s6, div_6_label,
                       contract_1, contract_1q, contract_2, contract_2q,
                       contract_3, contract_3q,
                       fin_grade_1, fin_grade_1y, fin_grade_2, fin_grade_2y,
                       fin_grade_3, fin_grade_3y, fin_grade_4, fin_grade_4y,
                       fin_grade_5, fin_grade_5y,
                       fin_grade_6, fin_grade_6y,
                       price_pos, fair_low, fair_high,
                       inst_foreign, inst_trust, inst_dealer,
                       revenue_note,
                       sys_est_eps, sys_est_quarter, sys_est_confidence,
                       sys_ann_eps, sys_ann_div, sys_ann_pe, sys_ann_yld, sys_ann_confidence
                FROM stocks WHERE 1=1"""
    params = []
    exact = request.args.get("exact", "")
    if exact:
        sql += " AND code = ?"
        params.append(exact)
    elif q:
        sql += " AND (code LIKE ? OR name LIKE ?)"
        params += [f"%{q}%", f"%{q}%"]
    if market in ("上市", "上櫃"):
        sql += " AND market = ?"
        params.append(market)
    sql += " ORDER BY code ASC"

    # 無篩選時用記憶體快取（30秒）
    use_cache = not q and not market and not exact
    if use_cache and _stocks_cache and (_time.time() - _stocks_cache_time < 30):
        return jsonify(_stocks_cache)

    rows = query_db(sql, params)

    # 附加 ETF 持股資訊（批次查詢，避免 N+1）
    etf_map = {}
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("""
            SELECT h.stock_code,
                   GROUP_CONCAT(h.etf_code || ':' || COALESCE(i.name,''), ',') as etf_list
            FROM etf_holdings h
            LEFT JOIN etf_info i ON h.etf_code = i.code
            GROUP BY h.stock_code
        """)
        for r in c.fetchall():
            etf_map[r["stock_code"]] = r["etf_list"]
        conn.close()
    except Exception: pass

    # 批次查詢月營收（當年度各月）
    rev_map = {}  # code -> [{month, revenue, yoy}, ...]
    try:
        from datetime import date
        cur_west = date.today().year
        conn_rev = sqlite3.connect(DB_PATH)
        conn_rev.row_factory = sqlite3.Row
        rev_rows = conn_rev.execute(
            """SELECT r.code, r.month, r.revenue, r2.revenue as prev_revenue
               FROM monthly_revenue r
               LEFT JOIN monthly_revenue r2 ON r.code = r2.code AND r2.year = r.year - 1 AND r2.month = r.month
               WHERE r.year = ?
               ORDER BY r.code, r.month""", (cur_west,)).fetchall()
        conn_rev.close()
        for r in rev_rows:
            code = r['code']
            if code not in rev_map:
                rev_map[code] = []
            yoy = None
            if r['revenue'] and r['prev_revenue'] and r['prev_revenue'] > 0:
                yoy = round((r['revenue'] - r['prev_revenue']) / r['prev_revenue'] * 100, 2)
            rev_map[code].append({'month': r['month'], 'revenue': r['revenue'], 'yoy': yoy})
    except Exception: pass

    # 後端統一計算沈董EPS/股利/綜合股利，避免前後端不一致
    cur_roc = __import__('datetime').date.today().year - 1911

    # 批次查詢 checklist pass_count
    chk_map = {}
    try:
        _init_checklist_db()
        chk_rows = query_db("SELECT code, pass_count, total_count FROM stock_checklist")
        for cr in chk_rows:
            chk_map[cr['code']] = (cr['pass_count'], cr['total_count'])
    except Exception: pass

    for row in rows:
        row["etf_tags"] = etf_map.get(row["code"], "")
        row["monthly_rev"] = rev_map.get(row["code"], [])
        _calc_shen_fields(row, cur_roc)
        chk = chk_map.get(row["code"])
        row["_chk_pass"] = chk[0] if chk else None
        row["_chk_total"] = chk[1] if chk else None

    result_data = {"count": len(rows), "data": rows}
    if use_cache:
        _stocks_cache = result_data
        _stocks_cache_time = _time.time()
    return jsonify(result_data)

# ── 狀態（資料筆數 + 最後更新時間）────────────────────────
@app.route("/api/status")
def status():
    rows    = query_db("SELECT updated_at FROM stocks ORDER BY updated_at DESC LIMIT 1")
    updated = rows[0]["updated_at"] if rows else None
    total   = query_db("SELECT COUNT(*) as n FROM stocks")[0]["n"]
    # API 健康狀態
    health_rows = query_db("SELECT source, description, status, last_success, fail_count FROM api_health ORDER BY status DESC, source")
    alerts = [dict(r) for r in health_rows if r['status'] != 'ok']

    return jsonify({
        "updated_at":   updated,
        "api_alerts":   alerts,
        "total":        total,
        "is_refreshing": _is_refreshing
    })

# ── 手動觸發更新（背景執行，立即回應）─────────────────────
@app.route("/api/refresh", methods=["POST"])
def refresh():
    global _is_refreshing
    if _is_refreshing:
        return jsonify({"status": "already_running", "msg": "更新中，請稍候"}), 200

    def do_refresh():
        global _is_refreshing
        with _refresh_lock:
            _is_refreshing = True
            try:
                refresh_prices()
                # 存入每日價量
                from scraper import _save_daily_price
                try: _save_daily_price()
                except Exception: pass
                # 同步 EPS + 快照 + 新聞
                from scraper import _sync_eps_from_quarterly
                from guardian import snapshot_stock_states, fetch_material_news, fetch_moneydj_news
                try: _sync_eps_from_quarterly()
                except Exception: pass
                try: snapshot_stock_states()
                except Exception: pass
                try: fetch_material_news()
                except Exception: pass
                try: fetch_moneydj_news()
                except Exception: pass
                # 股價更新後重算檢核表
                try: calc_all_checklists()
                except Exception: pass
            finally:
                _is_refreshing = False

    threading.Thread(target=do_refresh, daemon=True).start()
    return jsonify({"status": "started", "msg": "開始更新資料"})

# ── 更新進度查詢 ────────────────────────────────────────────
@app.route("/api/refresh/status")
def refresh_status():
    return jsonify({"is_refreshing": _is_refreshing})

# ── 本機同步評價快照到 Render ────────────────────────────────
@app.route("/api/sync/snapshot", methods=["POST"])
def sync_snapshot():
    """接收本機 push 過來的 stock_state 評價資料"""
    if not check_sync_token():
        return jsonify({"status": "error", "msg": "unauthorized"}), 403
    from datetime import datetime
    data = request.json
    if not data or 'rows' not in data:
        return jsonify({"error": "missing rows"}), 400

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # 確保欄位存在
    for col, typ in [('val_level','TEXT'),('val_aa','REAL'),('val_a1','REAL'),
                     ('val_a2','REAL'),('val_a','REAL'),('val_lt6','REAL'),('discount_pct','REAL')]:
        try: c.execute(f"ALTER TABLE stock_state ADD COLUMN {col} {typ}")
        except Exception: pass
    try: c.execute("ALTER TABLE stocks ADD COLUMN deepest_val_level TEXT")
    except Exception: pass
    try: c.execute("ALTER TABLE stocks ADD COLUMN val_cheap_days INTEGER DEFAULT 0")
    except Exception: pass
    try: conn.commit()
    except Exception: pass

    updated = 0
    for r in data['rows']:
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            c.execute("""INSERT INTO stock_state
                         (stock_id, date, price, price_pos, fair_low, fair_mid, fair_high,
                          shen_eps, shen_pe, shen_yld, fin_grade,
                          val_level, val_aa, val_a1, val_a2, val_a, val_lt6, discount_pct, updated_at)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                         ON CONFLICT(stock_id, date) DO UPDATE SET
                         price=excluded.price, price_pos=excluded.price_pos,
                         fair_low=excluded.fair_low, fair_mid=excluded.fair_mid, fair_high=excluded.fair_high,
                         shen_eps=excluded.shen_eps, shen_pe=excluded.shen_pe, shen_yld=excluded.shen_yld,
                         fin_grade=excluded.fin_grade,
                         val_level=excluded.val_level, val_aa=excluded.val_aa, val_a1=excluded.val_a1,
                         val_a2=excluded.val_a2, val_a=excluded.val_a, val_lt6=excluded.val_lt6,
                         discount_pct=excluded.discount_pct, updated_at=excluded.updated_at""",
                      (r['code'], r['date'], r.get('price'), r.get('pp'),
                       r.get('fl'), r.get('fm'), r.get('fh'),
                       r.get('se'), r.get('sp'), r.get('sy'), r.get('fg'),
                       r.get('vl'), r.get('aa'), r.get('a1'), r.get('a2'),
                       r.get('a'), r.get('lt6'), r.get('dp'), now))
            updated += 1
            # 更新 stocks 表
            c.execute("UPDATE stocks SET deepest_val_level=?, val_cheap_days=? WHERE code=?",
                      (r.get('deepest'), r.get('cheap_days', 0), r['code']))
        except Exception: pass

    conn.commit()
    conn.close()
    return jsonify({"status": "ok", "updated": updated})

# ── 本機同步估算到 Render ────────────────────────────────────
@app.route("/api/sync/estimates", methods=["POST"])
def sync_estimates():
    """接收本機 push 過來的系統估算結果"""
    if not check_sync_token():
        return jsonify({"status": "error", "msg": "unauthorized"}), 403
    data = request.json
    if not data or 'data' not in data:
        return jsonify({"error": "missing data"}), 400

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for col, typ in [('sys_ann_eps','REAL'),('sys_ann_div','REAL'),('sys_ann_pe','REAL'),
                     ('sys_ann_yld','REAL'),('sys_ann_confidence','TEXT')]:
        try: c.execute(f"ALTER TABLE stocks ADD COLUMN {col} {typ}")
        except Exception: pass

    updated = 0
    for row in data['data']:
        try:
            c.execute("""UPDATE stocks SET sys_ann_eps=?, sys_ann_div=?, sys_ann_pe=?,
                         sys_ann_yld=?, sys_ann_confidence=? WHERE code=?""",
                      (row.get('sys_ann_eps'), row.get('sys_ann_div'), row.get('sys_ann_pe'),
                       row.get('sys_ann_yld'), row.get('sys_ann_confidence'), row['code']))
            updated += c.rowcount
        except Exception: pass

    conn.commit()
    conn.close()
    return jsonify({"status": "ok", "updated": updated})


# ── 本機同步新聞到 Render ────────────────────────────────────
@app.route("/api/sync/news", methods=["POST"])
def sync_news():
    """接收本機 push 過來的新聞"""
    if not check_sync_token():
        return jsonify({"status": "error", "msg": "unauthorized"}), 403
    from datetime import datetime
    data = request.json
    if not data or 'rows' not in data:
        return jsonify({"error": "missing rows"}), 400
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    inserted = 0
    for r in data['rows']:
        try:
            # 用 subject+code+date 去重
            c.execute("SELECT id FROM material_news WHERE code=? AND subject=? AND date=?",
                      (r.get('code'), r.get('subject'), r.get('date')))
            if not c.fetchone():
                c.execute("""INSERT INTO material_news
                             (code, name, date, subject, link, tier, matched_rule, direction, created_at)
                             VALUES (?,?,?,?,?,?,?,?,?)""",
                          (r.get('code'), r.get('name'), r.get('date'), r.get('subject'),
                           r.get('link'), r.get('tier'),
                           r.get('matched_rule'), r.get('direction'), r.get('created_at')))
            inserted += c.rowcount
        except Exception: pass
    conn.commit()
    conn.close()
    return jsonify({"status": "ok", "inserted": inserted})

# ── 更新三大法人 ────────────────────────────────────────────
@app.route("/api/refresh/institutional", methods=["POST"])
def refresh_institutional():
    # 如果 POST body 有 data，直接批次寫入（從本機同步用）
    if request.is_json and request.json.get('data'):
        rows = request.json['data']
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        updated = 0
        for r in rows:
            c.execute("UPDATE stocks SET inst_foreign=?, inst_trust=?, inst_dealer=? WHERE code=?",
                      (r.get('f'), r.get('t'), r.get('d'), r['code']))
            if c.rowcount:
                updated += 1
        conn.commit()
        conn.close()
        return jsonify({"status": "ok", "updated": updated})

    # 否則觸發群益爬蟲
    def do_inst():
        try:
            fetch_institutional()
        except Exception as e:
            print(f"[法人更新] 錯誤: {e}")
    threading.Thread(target=do_inst, daemon=True).start()
    return jsonify({"status": "started", "msg": "開始更新三大法人資料"})

@app.route("/api/sync/financial-detail", methods=["POST"])
def sync_financial_detail():
    """本機 push 完整財報明細到 Render"""
    if not check_sync_token():
        return jsonify({"status": "error", "msg": "unauthorized"}), 403
    if not request.is_json or not request.json.get('data'):
        return jsonify({"status": "error", "msg": "no data"}), 400
    rows = request.json['data']
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS financial_detail (
            code TEXT NOT NULL, period TEXT NOT NULL, period_type TEXT NOT NULL,
            report_type TEXT NOT NULL, item TEXT NOT NULL, value REAL, updated_at TEXT,
            PRIMARY KEY (code, period, report_type, item))""")
        conn.commit()
    except Exception: pass
    updated = 0
    for r in rows:
        try:
            c.execute("""INSERT INTO financial_detail (code, period, period_type, report_type, item, value, updated_at)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(code, period, report_type, item) DO UPDATE SET
                value=excluded.value, period_type=excluded.period_type, updated_at=excluded.updated_at""",
                (r['code'], r['period'], r['period_type'], r['report_type'], r['item'], r['value'], r.get('updated_at', '')))
            updated += 1
        except Exception: pass
    conn.commit()
    conn.close()
    return jsonify({"status": "ok", "updated": updated})


@app.route("/api/sync/quarterly", methods=["POST"])
def sync_quarterly():
    """本機 push 季報資料到 Render"""
    if not check_sync_token():
        return jsonify({"status": "error", "msg": "unauthorized"}), 403
    if not request.is_json or not request.json.get('data'):
        return jsonify({"status": "error", "msg": "no data"}), 400
    rows = request.json['data']
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # 確保欄位存在（Render PostgreSQL 可能缺少新欄位）
    for col, typ in [('inventory', 'REAL'), ('continuing_income', 'REAL')]:
        try: c.execute(f"ALTER TABLE quarterly_financial ADD COLUMN {col} {typ}")
        except Exception: pass
    try: conn.commit()
    except Exception: pass
    updated = 0
    errors = 0
    # 查詢實際存在的欄位，只用存在的欄位做 UPDATE/INSERT
    try:
        c.execute("SELECT * FROM quarterly_financial LIMIT 0")
        existing_cols = set(desc[0] for desc in c.description)
    except Exception:
        existing_cols = set()
    qf_cols = [col for col in ['revenue','cost','gross_profit','operating_expense','operating_income',
               'non_operating','pretax_income','tax','continuing_income',
               'net_income_parent','eps','contract_liability','inventory'] if col in existing_cols]
    for r in rows:
        code = r.get('code')
        quarter = r.get('quarter')
        if not code or not quarter:
            continue
        fields = []
        vals = []
        for col in qf_cols:
            if col in r and r[col] is not None:
                fields.append(f'{col}=?')
                vals.append(r[col])
        if fields:
            fields.append('updated_at=?')
            vals.append(r.get('updated_at', ''))
            vals.extend([code, quarter])
            try:
                c.execute(f"UPDATE quarterly_financial SET {', '.join(fields)} WHERE code=? AND quarter=?", vals)
                if c.rowcount > 0:
                    updated += 1
                else:
                    ins_cols = ['code', 'quarter'] + [col for col in qf_cols if col in r and r[col] is not None] + ['updated_at']
                    ins_vals = [code, quarter] + [r[col] for col in qf_cols if col in r and r[col] is not None] + [r.get('updated_at', '')]
                    placeholders = ','.join(['?'] * len(ins_cols))
                    c.execute(f"INSERT INTO quarterly_financial ({','.join(ins_cols)}) VALUES ({placeholders})", ins_vals)
                    updated += 1
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"[sync_quarterly] {code} {quarter} 失敗: {e}")
    conn.commit()
    conn.close()
    return jsonify({"status": "ok", "updated": updated})


@app.route("/api/sync/table", methods=["POST"])
def sync_table():
    """
    通用全表同步 API — 本機 push 任意資料表到 Render
    POST body: { "table": "pe_history", "columns": [...], "pk": ["code","year"], "data": [...] }
    """
    if not check_sync_token():
        return jsonify({"status": "error", "msg": "unauthorized"}), 403
    if not request.is_json:
        return jsonify({"status": "error", "msg": "not json"}), 400

    table = request.json.get('table', '').strip()
    columns = request.json.get('columns', [])
    pk = request.json.get('pk', [])
    rows = request.json.get('data', [])
    create_sql = request.json.get('create_sql', '')
    clear_first = request.json.get('clear_first', False)

    # 安全檢查：只允許白名單內的表
    ALLOWED_TABLES = {
        'pe_history', 'monthly_revenue', 'stock_state', 'material_news',
        'etf_holdings', 'etf_changes', 'etf_info',
        'user_lists', 'user_notes', 'user_estimates', 'user_settings',
        'system_eps_actual', 'system_eps_log',
        'quarterly_financial', 'financial_annual', 'financial_detail',
        'stocks', 'stock_checklist',
        'daily_price', 'focus_tracking', 'focus_signals',
    }
    if table not in ALLOWED_TABLES:
        return jsonify({"status": "error", "msg": f"table '{table}' not allowed"}), 400
    if not columns or not rows:
        return jsonify({"status": "ok", "updated": 0, "msg": "no data"})

    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
    except Exception as e:
        return jsonify({"status": "error", "msg": f"db connect: {e}"}), 500

    # 自動建表（如果有提供 CREATE SQL）
    if create_sql:
        try:
            c.execute(create_sql)
            conn.commit()
        except Exception:
            try: conn.rollback()
            except Exception: pass

    updated = 0
    errors = []

    # clear_first：在同一個 transaction 裡清空 + 寫入，避免空窗期
    if clear_first:
        try:
            c.execute(f"DELETE FROM {table}")
        except Exception as e:
            errors.append(f"clear: {e}")
            try: conn.rollback()
            except Exception: pass

    if pk:
        # UPSERT: INSERT ON CONFLICT UPDATE
        non_pk = [col for col in columns if col not in pk]
        placeholders = ','.join(['?'] * len(columns))
        if non_pk:
            update_clause = ','.join(f'{col}=excluded.{col}' for col in non_pk)
            conflict_clause = ','.join(pk)
            sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders}) ON CONFLICT({conflict_clause}) DO UPDATE SET {update_clause}"
        else:
            sql = f"INSERT OR IGNORE INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
        for r in rows:
            try:
                vals = [r.get(col) for col in columns]
                c.execute(sql, vals)
                updated += 1
            except Exception as e:
                if len(errors) < 3:
                    errors.append(str(e))
                try: conn.rollback()
                except Exception: pass
    else:
        # 無主鍵：先清空再插入（整表替換）
        try:
            c.execute(f"DELETE FROM {table}")
            conn.commit()
        except Exception:
            try: conn.rollback()
            except Exception: pass
        placeholders = ','.join(['?'] * len(columns))
        for r in rows:
            try:
                vals = [r.get(col) for col in columns]
                c.execute(f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})", vals)
                updated += 1
            except Exception as e:
                if len(errors) < 3:
                    errors.append(str(e))
                try: conn.rollback()
                except Exception: pass

    try:
        conn.commit()
    except Exception as e:
        errors.append(f"commit: {e}")
        try: conn.rollback()
        except Exception: pass
    conn.close()
    result = {"status": "ok", "updated": updated}
    if errors:
        result["errors"] = errors
    return jsonify(result)


@app.route("/api/sync/clear-table", methods=["POST"])
def sync_clear_table():
    """清空指定資料表（同步前用，避免殘留已刪除的資料）"""
    if not check_sync_token():
        return jsonify({"status": "error", "msg": "unauthorized"}), 403
    table = request.json.get('table', '').strip()
    ALLOWED_TABLES = {
        'material_news', 'etf_holdings', 'etf_changes', 'user_lists',
        'user_notes', 'user_estimates', 'focus_tracking', 'focus_signals',
    }
    if table not in ALLOWED_TABLES:
        return jsonify({"status": "error", "msg": f"table '{table}' not allowed"}), 400
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(f"DELETE FROM {table}")
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)}), 500


@app.route("/api/sync/pe-history", methods=["POST"])
def sync_pe_history():
    """本機 push 歷史本益比到 Render"""
    if not check_sync_token():
        return jsonify({"status": "error", "msg": "unauthorized"}), 403
    if not request.is_json or not request.json.get('data'):
        return jsonify({"status": "error", "msg": "no data"}), 400
    rows = request.json['data']
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # 確保表存在
    c.execute("""CREATE TABLE IF NOT EXISTS pe_history (
        code TEXT NOT NULL, year INTEGER NOT NULL,
        pe_high REAL, pe_low REAL, updated_at TEXT,
        PRIMARY KEY (code, year))""")
    updated = 0
    for r in rows:
        code = r.get('code')
        year = r.get('year')
        if not code or not year:
            continue
        try:
            c.execute("""INSERT INTO pe_history (code, year, pe_high, pe_low, updated_at)
                VALUES (?,?,?,?,?)
                ON CONFLICT(code, year) DO UPDATE SET
                pe_high=excluded.pe_high, pe_low=excluded.pe_low,
                updated_at=excluded.updated_at""",
                (code, year, r.get('pe_high'), r.get('pe_low'), r.get('updated_at')))
            updated += 1
        except Exception: pass
    conn.commit()
    conn.close()
    return jsonify({"status": "ok", "updated": updated})


@app.route("/api/sync/prices", methods=["POST"])
def sync_prices():
    """本機 push 股價到 Render"""
    if not check_sync_token():
        return jsonify({"status": "error", "msg": "unauthorized"}), 403
    if not request.is_json or not request.json.get('data'):
        return jsonify({"status": "error", "msg": "no data"}), 400
    rows = request.json['data']
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    updated = 0
    for r in rows:
        code = r.get('code')
        if not code:
            continue
        fields = []
        vals = []
        for col in ['close', 'change', 'open', 'high', 'low', 'volume',
                    'revenue_date', 'revenue_year', 'revenue_month',
                    'revenue_yoy', 'revenue_mom', 'revenue_cum_yoy']:
            if col in r and r[col] is not None:
                fields.append(f'{col}=?')
                vals.append(r[col])
        if fields:
            vals.append(code)
            c.execute(f"UPDATE stocks SET {', '.join(fields)} WHERE code=?", vals)
            if c.rowcount:
                updated += 1
    conn.commit()
    conn.close()
    return jsonify({"status": "ok", "updated": updated})


@app.route("/api/sync/annual", methods=["POST"])
def sync_annual():
    """本機 push 年度 EPS + 股利 + 財務等級到 Render"""
    if not check_sync_token():
        return jsonify({"status": "error", "msg": "unauthorized"}), 403
    if not request.is_json or not request.json.get('data'):
        return jsonify({"status": "error", "msg": "no data"}), 400
    rows = request.json['data']
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    updated = 0
    for r in rows:
        fields = []
        vals = []
        if 'eps_date' in r:
            fields.append('eps_date=?')
            vals.append(r['eps_date'])
        # 季度 EPS（eps_1~eps_5, eps_1q~eps_5q）
        for i in range(1, 6):
            for prefix in [f'eps_{i}', f'eps_{i}q']:
                if prefix in r:
                    fields.append(f'{prefix}=?')
                    vals.append(r[prefix])
        for i in range(1, 7):
            for prefix in [f'eps_y{i}', f'eps_y{i}_label',
                           f'div_c{i}', f'div_s{i}', f'div_{i}_label',
                           f'fin_grade_{i}', f'fin_grade_{i}y']:
                if prefix in r:
                    fields.append(f'{prefix}=?')
                    vals.append(r[prefix])
        # eps_ytd, deepest_val_level, val_cheap_days
        for extra in ['eps_ytd', 'eps_ytd_label', 'deepest_val_level', 'val_cheap_days']:
            if extra in r:
                fields.append(f'{extra}=?')
                vals.append(r[extra])
        if fields:
            vals.append(r['code'])
            c.execute(f"UPDATE stocks SET {', '.join(fields)} WHERE code=?", vals)
            if c.rowcount:
                updated += 1
    conn.commit()
    conn.close()
    return jsonify({"status": "ok", "updated": updated})


@app.route("/api/sync/financial-annual", methods=["POST"])
def sync_financial_annual():
    """本機 push financial_annual 整表資料到 Render"""
    if not check_sync_token():
        return jsonify({"status": "error", "msg": "unauthorized"}), 403
    if not request.is_json or not request.json.get('data'):
        return jsonify({"status": "error", "msg": "no data"}), 400
    rows = request.json['data']
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    updated = 0
    errors = 0
    # 查詢實際存在的欄位
    try:
        c.execute("SELECT * FROM financial_annual LIMIT 0")
        existing_cols = set(desc[0] for desc in c.description)
    except Exception:
        existing_cols = set()
    fa_cols = [col for col in ['revenue','cost','gross_profit','operating_expense','operating_income',
               'non_operating','pretax_income','tax','net_income','net_income_parent',
               'total_assets','total_equity','common_stock','inventory','contract_liability',
               'operating_cf','capex','eps','weighted_shares','cash_dividend','stock_dividend'] if col in existing_cols]
    for r in rows:
        code = r.get('code')
        year = r.get('year')
        if not code or not year:
            continue
        fields = []
        vals = []
        for col in fa_cols:
            if col in r and r[col] is not None:
                fields.append(f'{col}=?')
                vals.append(r[col])
        if fields:
            fields.append('updated_at=?')
            vals.append(r.get('updated_at', ''))
            vals.extend([code, year])
            try:
                c.execute(f"UPDATE financial_annual SET {', '.join(fields)} WHERE code=? AND year=?", vals)
                if c.rowcount == 0:
                    ins_fields = {col: r[col] for col in fa_cols if col in r and r[col] is not None}
                    ins_fields['code'] = code
                    ins_fields['year'] = year
                    ins_fields['updated_at'] = r.get('updated_at', '')
                    col_names = ','.join(ins_fields.keys())
                    placeholders = ','.join('?' * len(ins_fields))
                    c.execute(f"INSERT INTO financial_annual ({col_names}) VALUES ({placeholders})",
                              list(ins_fields.values()))
                updated += 1
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"[sync_financial_annual] {code} {year} 失敗: {e}")
    conn.commit()
    conn.close()
    return jsonify({"status": "ok", "updated": updated})


# ── 背景更新佇列 ─────────────────────────────────────────
_bg_updating = set()  # 正在背景更新的股票代碼
_bg_updating_lock = threading.Lock()

def _bg_update_financials(code):
    """背景更新個股全部資料"""
    with _bg_updating_lock:
        if code in _bg_updating:
            return
        _bg_updating.add(code)
    def _do():
        try:
            fetch_company_financials(code)
        except Exception: pass
        finally:
            with _bg_updating_lock:
                _bg_updating.discard(code)
    threading.Thread(target=_do, daemon=True).start()

# ── 個股年度財報 ────────────────────────────────────────────
@app.route("/api/stocks/<code>/financials")
def get_financials(code):
    from datetime import datetime, timedelta

    rows = query_db(
        "SELECT * FROM financial_annual WHERE code = ? ORDER BY year DESC LIMIT 6",
        (code,)
    )

    # 快取過期 → 背景更新，先回傳現有資料
    cache_valid = False
    if rows:
        try:
            updated = datetime.strptime(rows[0]['updated_at'], '%Y-%m-%d %H:%M:%S')
            if datetime.now() - updated < timedelta(hours=24):
                cache_valid = True
        except Exception: pass

    is_cloud = os.environ.get('DATABASE_URL') is not None
    if not cache_valid:
        if rows:
            # 有舊資料：背景更新，先回傳舊的（秒開）
            if not is_cloud:
                _bg_update_financials(code)
        elif not is_cloud:
            # 本機：完全沒資料時同步抓（第一次必須等）
            try:
                fetch_company_financials(code)
            except Exception: pass
            rows = query_db(
                "SELECT * FROM financial_annual WHERE code = ? ORDER BY year DESC LIMIT 6",
                (code,)
            )

    # 計算衍生指標
    data = []
    for r in rows:
        d = dict(r)
        rev = d.get('revenue')
        ni  = d.get('net_income')
        ocf = d.get('operating_cf')
        capex = d.get('capex')
        ta  = d.get('total_assets')
        te  = d.get('total_equity')
        cs  = d.get('common_stock')
        eps_val = d.get('eps')
        cd  = d.get('cash_dividend')
        sd  = d.get('stock_dividend')

        oi  = d.get('operating_income')
        pti = d.get('pretax_income')
        nip = d.get('net_income_parent')
        opex = d.get('operating_expense')

        # 反算 net_income_parent（NULL 時用 net_income，pti==ni 時用 80%）
        if nip is None and ni is not None:
            nip = ni
            d['net_income_parent'] = nip
        if pti is not None and ni is not None and abs(pti - ni) < 1 and pti > 1000000:
            ni = round(pti * 0.80, 2)
            nip = ni
            d['net_income'] = ni
            d['net_income_parent'] = nip

        # 毛利率
        d['gross_margin'] = round(d['gross_profit'] / rev * 100, 2) if rev and d.get('gross_profit') is not None else None
        # 營業費用占營收比率
        d['opex_ratio'] = round(opex / rev * 100, 2) if rev and opex is not None else None
        # 營業利益率
        d['operating_margin'] = round(oi / rev * 100, 2) if rev and oi is not None else None
        # 稅前淨利率
        d['pretax_margin'] = round(pti / rev * 100, 2) if rev and pti is not None else None
        # 反算稅額（稅為 NULL 或 0 但 pti>ni 時反算）
        tax_val = d.get('tax')
        if pti is not None and ni is not None:
            calc_tax = round(pti - ni, 2)
            if tax_val is None or (tax_val == 0 and abs(calc_tax) > 100):
                tax_val = calc_tax
                d['tax'] = tax_val
        # 稅率（虧損不算，限 0~100%）
        if pti and pti > 0 and tax_val is not None:
            raw_rate = tax_val / pti * 100
            d['tax_rate'] = round(min(max(raw_rate, 0), 100), 2)
        else:
            d['tax_rate'] = None
        # 稅後淨利率
        d['net_margin'] = round(ni / rev * 100, 2) if rev and ni is not None else None
        # 繼續營業單位損益（近似 = 稅後淨利）
        d['continuing_income'] = ni
        # 歸屬母公司權重
        d['parent_weight'] = round(nip / ni * 100, 2) if ni and ni != 0 and nip is not None else None
        # ROA
        d['roa'] = round(ni / ta * 100, 2) if ta and ni is not None else None
        # ROE
        d['roe'] = round(ni / te * 100, 2) if te and ni is not None else None
        # 盈餘品質率
        # 稅後淨利為負時不計算盈餘品質率（無意義）
        d['earnings_quality'] = round(ocf / ni * 100, 2) if ni and ni > 0 and ocf is not None else None
        # 自由現金流（capex 為負值）
        d['fcf'] = round(ocf + capex, 2) if ocf is not None and capex is not None else None
        # 加權平均股數（千股，從 EPS 反算）
        if eps_val and eps_val != 0 and nip is not None:
            shares_raw = nip / eps_val
            d['weighted_shares'] = round(shares_raw / 1000, 0)
        else:
            shares_raw = None
            d['weighted_shares'] = None
        # 每股自由現金流
        shares = cs / 10 if cs and cs > 0 else None
        d['fcf_per_share'] = round(d['fcf'] / shares, 2) if d.get('fcf') is not None and shares else None
        # 每股盈餘-本業
        eff_tax = tax_val / pti if pti and pti != 0 and tax_val is not None else None
        if oi is not None and shares_raw and eff_tax is not None:
            d['eps_core'] = round(oi * (1 - eff_tax) / shares_raw, 2)
        else:
            d['eps_core'] = None
        # 每股盈餘-業外
        if d.get('eps_core') is not None and eps_val is not None:
            d['eps_nonop'] = round(eps_val - d['eps_core'], 2)
        else:
            d['eps_nonop'] = None
        # 配息率（EPS <= 0 但有配息 → 100%）
        total_div = ((cd or 0) + (sd or 0))
        if total_div > 0 and eps_val is not None and eps_val > 0:
            d['payout_ratio'] = round(total_div / eps_val * 100, 2)
        elif total_div > 0 and (eps_val is None or eps_val <= 0):
            d['payout_ratio'] = 100.0
        else:
            d['payout_ratio'] = None
        # 年度標籤（民國年）
        d['year_label'] = str(d['year'] - 1911)

        data.append(d)

    # 計算財務體質等級並寫入 stocks 表
    if data:
        try:
            conn2 = sqlite3.connect(DB_PATH, timeout=10)
            c2 = conn2.cursor()
            updates = {}
            for i, d in enumerate(data[:5], 1):
                grade = _calc_fin_grade(d.get('roe'), d.get('operating_margin'), d.get('fcf'), d.get('revenue'))
                updates[f'fin_grade_{i}'] = grade
                updates[f'fin_grade_{i}y'] = d.get('year_label')
            for i in range(len(data[:5]) + 1, 6):
                updates[f'fin_grade_{i}'] = None
                updates[f'fin_grade_{i}y'] = None
            set_clause = ', '.join(f'{k}=?' for k in updates.keys())
            c2.execute(f'UPDATE stocks SET {set_clause} WHERE code=?',
                       list(updates.values()) + [code])
            conn2.commit()
            conn2.close()
        except Exception:
            pass  # DB locked 時不影響資料回傳

    # 取得公司名稱
    stock_info = query_db("SELECT name, market FROM stocks WHERE code = ?", (code,))
    name = stock_info[0]['name'] if stock_info else code

    return jsonify({"code": code, "name": name, "data": data})


# ── 個股季度估計表 ──────────────────────────────────────────
@app.route("/api/stocks/<code>/quarterly")
def get_quarterly(code):
    from datetime import datetime, timedelta
    is_cloud = os.environ.get('DATABASE_URL') is not None

    q_order = """ORDER BY CAST(SUBSTR(quarter, 1, INSTR(quarter, 'Q') - 1) AS INTEGER) DESC,
                    CAST(SUBSTR(quarter, INSTR(quarter, 'Q') + 1) AS INTEGER) DESC"""
    rows = query_db(
        f"SELECT * FROM quarterly_financial WHERE code = ? {q_order} LIMIT 8",
        (code,)
    )
    cache_valid = False
    if rows:
        try:
            updated = datetime.strptime(rows[0]['updated_at'], '%Y-%m-%d %H:%M:%S')
            if datetime.now() - updated < timedelta(hours=12):
                cache_valid = True
        except Exception: pass

    if not cache_valid and not is_cloud:
        if rows:
            def _bg_q(c=code):
                try: fetch_company_quarterly(c)
                except Exception: pass
            threading.Thread(target=_bg_q, daemon=True).start()
        else:
            try: fetch_company_quarterly(code)
            except Exception: pass
            rows = query_db(
                f"SELECT * FROM quarterly_financial WHERE code = ? {q_order} LIMIT 8",
                (code,)
            )

    # 批次查詢所有年度的股數（避免逐筆查詢）
    _shares_map = {}
    try:
        fa_rows = query_db(
            "SELECT year, weighted_shares FROM financial_annual WHERE code=?", (code,))
        for fr in fa_rows:
            if fr.get('weighted_shares'):
                _shares_map[fr['year']] = fr['weighted_shares']
    except Exception: pass

    # fallback 股數（找一季能反算的）
    _fallback_shares = None
    for r in rows:
        e = r.get('eps')
        n = r.get('net_income_parent')
        if e and e != 0 and n is not None:
            _fallback_shares = n / e
            break

    data = []
    for r in rows:
        d = dict(r)
        rev = d.get('revenue')
        pti = d.get('pretax_income')
        tax = d.get('tax')
        oi  = d.get('operating_income')
        ci  = d.get('continuing_income')
        nip = d.get('net_income_parent')
        eps_val = d.get('eps')
        opex = d.get('operating_expense')

        # 修正 pti==nip 異常（應有稅但沒扣，用 20% 預設稅率）
        if pti is not None and nip is not None and abs(pti - nip) < 1 and pti > 1000000:
            nip = round(pti * 0.80, 2)
            d['net_income_parent'] = nip

        # 反算稅額（群益季表無稅欄位或為0，用 稅前淨利 - 稅後淨利 推算）
        if pti is not None and nip is not None:
            calc_tax = round(pti - nip, 2)
            if tax is None or (tax == 0 and abs(calc_tax) > 100):
                tax = calc_tax
                d['tax'] = tax

        # 繼續營業單位損益 = 合併稅後淨利（群益 net_income_parent 存的就是這個）
        if ci is None and nip is not None:
            ci = nip
            d['continuing_income'] = ci

        # 加權平均股數（千股）— 從群益 zcqa 年度資料取得
        d['weighted_shares'] = None
        shares_raw = None  # 原始股數（股），用於本業/業外EPS計算
        quarter = d.get('quarter', '')
        if quarter:
            try:
                roc_yr = int(quarter.split('Q')[0])
                west_yr = roc_yr + 1911
                ann_shares = _shares_map.get(west_yr)
                if ann_shares:
                    d['weighted_shares'] = round(ann_shares, 0)
                    shares_raw = ann_shares * 1000  # 轉為股
            except Exception: pass
        # fallback：EPS 反算（年度股數尚未入庫時）
        if shares_raw is None and eps_val is not None and eps_val != 0 and nip is not None:
            shares_raw = nip / eps_val
            d['weighted_shares'] = round(shares_raw / 1000, 0)
        # fallback2：EPS=0 但有其他季可反算
        if shares_raw is None and _fallback_shares:
            shares_raw = _fallback_shares
            d['weighted_shares'] = round(shares_raw / 1000, 0)

        # 毛利率
        d['gross_margin'] = round(d['gross_profit'] / rev * 100, 2) if rev and d.get('gross_profit') is not None else None
        # 營業費用占營收比率
        d['opex_ratio'] = round(opex / rev * 100, 2) if rev and opex is not None else None
        # 稅率 = 稅 / 稅前淨利
        if pti and pti > 0 and tax is not None:
            raw_rate = tax / pti * 100
            d['tax_rate'] = round(min(max(raw_rate, 0), 100), 2)
        else:
            d['tax_rate'] = None
        # 歸屬母公司權重 = 歸屬母公司淨利 / 繼續營業單位損益
        if nip is not None and ci and ci != 0:
            d['parent_weight'] = round(nip / ci * 100, 2)
        else:
            d['parent_weight'] = None

        # 每股盈餘-本業
        eff_tax = tax / pti if pti and pti != 0 and tax is not None else None
        if oi is not None and shares_raw and eff_tax is not None:
            d['eps_core'] = round(oi * (1 - eff_tax) / shares_raw, 2)
        else:
            d['eps_core'] = None
        # 每股盈餘-業外
        if d.get('eps_core') is not None and eps_val is not None:
            d['eps_nonop'] = round(eps_val - d['eps_core'], 2)
        else:
            d['eps_nonop'] = None

        data.append(d)

    stock_info = query_db("SELECT name FROM stocks WHERE code = ?", (code,))
    name = stock_info[0]['name'] if stock_info else code
    return jsonify({"code": code, "name": name, "data": data})


# ── 系統 EPS 估算 ─────────────────────────────────────────────
@app.route("/api/stocks/<code>/system-estimate")
def get_system_estimate(code):
    try:
        result = estimate_system_eps(code)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e), "confidence": "N/A"})


@app.route("/api/stocks/<code>/system-estimate-multi")
def get_system_estimate_multi(code):
    try:
        result = estimate_system_eps_multi(code)
        return jsonify(result)
    except Exception as e:
        return jsonify({"quarters": [], "error": str(e)})


@app.route("/api/stocks/<code>/system-estimate-annual")
def get_system_estimate_annual(code):
    try:
        result = estimate_annual_eps(code)
        _log_estimate(code, result, 'annual')
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)})


# ── 歷史本益比 ──────────────────────────────────────────────
@app.route("/api/stocks/<code>/pe-history")
def get_pe_history(code):
    from datetime import datetime, timedelta
    import statistics

    rows = query_db(
        "SELECT * FROM pe_history WHERE code = ? ORDER BY year ASC",
        (code,)
    )
    cache_valid = False
    if rows:
        try:
            updated = datetime.strptime(rows[-1]['updated_at'], '%Y-%m-%d %H:%M:%S')
            if datetime.now() - updated < timedelta(days=7):
                cache_valid = True
        except Exception: pass

    is_cloud = os.environ.get('DATABASE_URL') is not None
    if not cache_valid and not is_cloud:
        if rows:
            def _bg_pe(c=code):
                try: fetch_pe_history(c)
                except Exception: pass
            threading.Thread(target=_bg_pe, daemon=True).start()
        else:
            try: fetch_pe_history(code)
            except Exception: pass
            rows = query_db(
                "SELECT * FROM pe_history WHERE code = ? ORDER BY year ASC",
                (code,)
            )

    data = [dict(r) for r in rows]
    # 取最近 8 年
    data = data[-8:] if len(data) > 8 else data

    # 統計推估
    est = {}
    if len(data) >= 3:
        highs = [d['pe_high'] for d in data if d.get('pe_high') is not None]
        lows  = [d['pe_low'] for d in data if d.get('pe_low') is not None]
        if highs:
            est['avg_high'] = round(sum(highs) / len(highs), 2)
            est['median_high'] = round(statistics.median(highs), 2)
        if lows:
            est['avg_low']  = round(sum(lows) / len(lows), 2)
            est['median_low']  = round(statistics.median(lows), 2)
        # 去極值平均（去掉最高和最低各一個）
        if len(highs) >= 5:
            trimmed_h = sorted(highs)[1:-1]
            est['trimmed_avg_high'] = round(sum(trimmed_h) / len(trimmed_h), 2)
        if len(lows) >= 5:
            trimmed_l = sorted(lows)[1:-1]
            est['trimmed_avg_low']  = round(sum(trimmed_l) / len(trimmed_l), 2)

    stock_info = query_db("SELECT name FROM stocks WHERE code = ?", (code,))
    name = stock_info[0]['name'] if stock_info else code
    return jsonify({"code": code, "name": name, "data": data, "estimate": est})


# ── 個股完整財報（損益表+資產負債表）─────────────────────────
@app.route("/api/stocks/<code>/financial-detail")
def get_financial_detail(code):
    from datetime import datetime, timedelta
    is_cloud = os.environ.get('DATABASE_URL') is not None

    # 確保表存在
    try:
        conn_init = sqlite3.connect(DB_PATH)
        conn_init.execute("""CREATE TABLE IF NOT EXISTS financial_detail (
            code TEXT NOT NULL, period TEXT NOT NULL, period_type TEXT NOT NULL,
            report_type TEXT NOT NULL, item TEXT NOT NULL, value REAL, updated_at TEXT,
            PRIMARY KEY (code, period, report_type, item))""")
        conn_init.commit()
        conn_init.close()
    except Exception: pass

    rows = query_db(
        "SELECT period, period_type, report_type, item, value, updated_at FROM financial_detail WHERE code = ? ORDER BY period DESC",
        (code,)
    )

    # 快取：有資料且 24 小時內更新過就不重抓
    cache_valid = False
    if rows:
        try:
            latest = max(r['updated_at'] for r in rows if r.get('updated_at'))
            updated = datetime.strptime(latest, '%Y-%m-%d %H:%M:%S')
            if datetime.now() - updated < timedelta(hours=24):
                cache_valid = True
        except Exception: pass

    if not cache_valid and not is_cloud:
        if rows:
            def _bg(c=code):
                try: fetch_financial_detail(c)
                except Exception: pass
            threading.Thread(target=_bg, daemon=True).start()
        else:
            try: fetch_financial_detail(code)
            except Exception: pass
            rows = query_db(
                "SELECT period, period_type, report_type, item, value, updated_at FROM financial_detail WHERE code = ? ORDER BY period DESC",
                (code,)
            )

    # 整理成前端友好格式
    result = {'income_statement': {'annual': {}, 'quarterly': {}},
              'balance_sheet': {'annual': {}, 'quarterly': {}}}
    for r in rows:
        rt = r['report_type']
        pt = r['period_type']
        period = r['period']
        if rt in result and pt in result[rt]:
            if period not in result[rt][pt]:
                result[rt][pt][period] = {}
            result[rt][pt][period][r['item']] = r['value']

    return jsonify(result)


# ── 個股月營收 ──────────────────────────────────────────────
@app.route("/api/stocks/<code>/monthly-revenue")
def get_monthly_revenue(code):
    from datetime import datetime, timedelta
    import math

    # 檢查快取
    rows = query_db(
        "SELECT * FROM monthly_revenue WHERE code = ? ORDER BY year DESC, month ASC",
        (code,)
    )
    cache_valid = False
    if rows:
        try:
            updated = datetime.strptime(rows[0]['updated_at'], '%Y-%m-%d %H:%M:%S')
            if datetime.now() - updated < timedelta(hours=24):
                cache_valid = True
        except Exception: pass

    is_cloud = os.environ.get('DATABASE_URL') is not None
    if not cache_valid and not is_cloud:
        if rows:
            def _bg_rev(c=code):
                try:
                    from capital_fetcher import fetch_capital_monthly_revenue
                    fetch_capital_monthly_revenue(c)
                except Exception: pass
            threading.Thread(target=_bg_rev, daemon=True).start()
        else:
            try:
                from capital_fetcher import fetch_capital_monthly_revenue
                fetch_capital_monthly_revenue(code)
            except Exception: pass
            rows = query_db(
                "SELECT * FROM monthly_revenue WHERE code = ? ORDER BY year DESC, month ASC",
                (code,)
            )

    # 建立 {(year, month): revenue} 查找表
    rev_map = {}
    for r in rows:
        rev_map[(r['year'], r['month'])] = r['revenue']

    # 找出最近 3 個有資料的年度（加上前一年用來算年增率）
    all_years = sorted(set(r['year'] for r in rows), reverse=True)
    display_years = all_years[:3]  # 最近 3 年顯示
    if not display_years:
        stock_info = query_db("SELECT name FROM stocks WHERE code = ?", (code,))
        name = stock_info[0]['name'] if stock_info else code
        return jsonify({"code": code, "name": name, "years": [], "data": []})

    # 組合每月資料，計算增率
    data = []
    for m in range(1, 13):
        row = {"month": m}
        for yr in display_years:
            cur = rev_map.get((yr, m))
            # 上月營收（上月或去年12月）
            if m == 1:
                prev_m = rev_map.get((yr - 1, 12))
            else:
                prev_m = rev_map.get((yr, m - 1))
            # 去年同月
            prev_y = rev_map.get((yr - 1, m))
            # 當月沒有營收 → 所有衍生指標都不算
            if cur is None:
                row[str(yr)] = {"revenue": None, "mom": None, "yoy": None, "cum_yoy": None}
                continue

            # 月增率
            mom = round((cur / prev_m - 1) * 100, 2) if prev_m else None
            # 年增率
            yoy = round((cur / prev_y - 1) * 100, 2) if prev_y else None
            # 累積營收年增率（只累計有資料的月份）
            cum_cur = sum(rev_map.get((yr, i), 0) for i in range(1, m + 1) if rev_map.get((yr, i)))
            cum_prev = sum(rev_map.get((yr - 1, i), 0) for i in range(1, m + 1) if rev_map.get((yr - 1, i)))
            cum_yoy = round((cum_cur / cum_prev - 1) * 100, 2) if cum_prev and cum_cur else None

            row[str(yr)] = {
                "revenue": cur,
                "mom": mom,
                "yoy": yoy,
                "cum_yoy": cum_yoy,
            }
        data.append(row)

    stock_info = query_db("SELECT name FROM stocks WHERE code = ?", (code,))
    name = stock_info[0]['name'] if stock_info else code

    return jsonify({
        "code": code,
        "name": name,
        "years": sorted(display_years),
        "data": data,
    })


# ── 系統健康報告 ──────────────────────────────────────────
@app.route("/api/sync-status")
def sync_status():
    """同步狀態：比對本機 vs Render 的資料筆數和股價"""
    import requests as req
    is_cloud = os.environ.get('DATABASE_URL') is not None
    RENDER_URL = "https://tock-system.onrender.com"

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 各表筆數
    tables = ['stocks', 'quarterly_financial', 'financial_annual', 'pe_history',
              'monthly_revenue', 'stock_state', 'material_news', 'etf_holdings', 'user_lists']
    counts = []
    for t in tables:
        try:
            local_cnt = c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except Exception:
            local_cnt = 0
        counts.append({'table': t, 'local': local_cnt, 'render': 0})

    # Render 筆數
    if not is_cloud:
        try:
            r = req.get(f"{RENDER_URL}/api/sync-status?counts_only=1", timeout=15)
            rd = r.json()
            render_counts = {rc['table']: rc['local'] for rc in rd.get('counts', [])}
            for ct in counts:
                ct['render'] = render_counts.get(ct['table'], 0)
        except Exception: pass

    # 股價抽樣比對
    prices = []
    sample_codes = ['2330', '2317', '1101', '2454', '2881', '2618', '3008', '1301']
    local_prices = {}
    for code in sample_codes:
        r = c.execute("SELECT code, name, close FROM stocks WHERE code=?", (code,)).fetchone()
        if r:
            local_prices[r[0]] = {'code': r[0], 'name': r[1], 'local': r[2], 'render': None}

    if not is_cloud:
        try:
            r = req.get(f"{RENDER_URL}/api/stocks", timeout=30)
            rd = r.json()
            for s in rd.get('data', []):
                if s['code'] in local_prices:
                    local_prices[s['code']]['render'] = s.get('close')
        except Exception: pass
    prices = list(local_prices.values())

    # 最近更新時間
    times = {}
    try:
        r = c.execute("SELECT MAX(updated_at) FROM stocks").fetchone()
        times['stocks 最後更新'] = r[0] if r else None
    except Exception: pass
    try:
        r = c.execute("SELECT MAX(updated_at) FROM quarterly_financial").fetchone()
        times['季報最後更新'] = r[0] if r else None
    except Exception: pass
    try:
        r = c.execute("SELECT MAX(updated_at) FROM financial_annual").fetchone()
        times['年報最後更新'] = r[0] if r else None
    except Exception: pass
    try:
        r = c.execute("SELECT MAX(date) FROM stock_state").fetchone()
        times['快照最新日期'] = r[0] if r else None
    except Exception: pass

    conn.close()
    return jsonify({'counts': counts, 'prices': prices, 'times': times})


@app.route("/api/health")
def health():
    report = generate_health_report()
    # 附加 Render 同步狀態
    try:
        from render_sync import get_last_sync_result
        report['render_sync'] = get_last_sync_result()
    except Exception:
        pass
    return jsonify(report)

@app.route("/api/cross-validate", methods=["POST"])
def run_cross_validate():
    """手動觸發交叉校驗"""
    result = cross_validate(sample_size=30)
    return jsonify(result)

@app.route("/api/cross-validate")
def get_cross_validate():
    """取得最近一次校驗結果"""
    result = get_latest_validation()
    return jsonify(result or {"checked": 0, "ok": 0, "mismatches": []})

@app.route("/api/financial-validation")
def get_financial_validation():
    """取得財報交叉驗證結果"""
    rows = query_db("""SELECT v.*, s.name FROM data_validation_log v
        LEFT JOIN stocks s ON v.code = s.code
        WHERE v.resolved = 0
        ORDER BY v.diff_pct DESC LIMIT 100""")
    if not rows:
        return jsonify({"issues": [], "count": 0})
    return jsonify({
        "issues": [dict(r) for r in rows],
        "count": len(rows)
    })


@app.route("/api/providers")
def providers():
    return jsonify(get_provider_status())

@app.route("/api/breakers")
def breakers():
    return jsonify(get_all_breakers())

@app.route("/api/breakers/<source>/reset", methods=["POST"])
def reset_breaker(source):
    b = get_breaker(source)
    b.reset()
    return jsonify({"status": "ok", "source": source, "state": "CLOSED"})

@app.route("/api/quarantine")
def quarantine():
    return jsonify(get_quarantine_list(100))

@app.route("/api/quarantine/<int:qid>/<action>", methods=["POST"])
def handle_quarantine(qid, action):
    if action not in ('accept', 'reject'):
        return jsonify({"error": "action must be accept or reject"}), 400
    ok = resolve_quarantine(qid, action)
    return jsonify({"status": "ok" if ok else "error"})

@app.route("/api/fingerprints")
def fingerprints():
    return jsonify(get_fingerprint_stats())

@app.route("/api/coverage")
def coverage():
    return jsonify(get_coverage_map())

@app.route("/api/db-status")
def db_status():
    import os
    alert_file = "logs/DB_ALERT"
    db_file = "stocks.db"
    size = os.path.getsize(db_file) if os.path.exists(db_file) else 0
    alert = os.path.exists(alert_file)
    icloud_alert = os.path.exists("logs/ICLOUD_ALERT")
    icloud_ok = os.path.exists(os.path.expanduser(
        "~/Library/Mobile Documents/com~apple~CloudDocs/Documents/"))
    # 讀最近一筆 guard log
    last_check = None
    try:
        with open("logs/db_guard.log") as f:
            lines = f.readlines()
            if lines:
                last_check = lines[-1].strip()
    except Exception: pass
    return jsonify({
        "size_bytes": size,
        "size_mb": round(size / 1024 / 1024, 2),
        "alert": alert,
        "icloud": icloud_ok and not icloud_alert,
        "last_check": last_check,
    })

@app.route("/api/bulk/revenue")
def bulk_revenue():
    """批次月營收+季營收（供沈董系統雲端使用）"""
    from datetime import date as dt
    current_year = dt.today().year
    last_year = current_year - 1
    cur_roc = current_year - 1911
    last_roc = last_year - 1911

    # 當年度月營收
    monthly = query_db(
        "SELECT code, month, revenue FROM monthly_revenue WHERE year = ?",
        (current_year,)
    )
    monthly_map = {}
    for r in monthly:
        monthly_map.setdefault(r['code'], {})[str(r['month'])] = r['revenue']
    months = sorted(set(r['month'] for r in monthly))

    # 季營收+EPS（去年+今年，用民國年 quarter 格式）
    quarterly = query_db(
        "SELECT code, quarter, revenue, eps FROM quarterly_financial WHERE quarter LIKE ? OR quarter LIKE ?",
        (f"{last_roc}Q%", f"{cur_roc}Q%")
    )
    qrev_map = {}
    qeps_map = {}
    all_q = set()
    for r in quarterly:
        q = r['quarter']
        roc_yr = int(q.split('Q')[0])
        west_yr = roc_yr + 1911
        qn = q.split('Q')[1]
        key = f"{west_yr}Q{qn}"
        all_q.add((west_yr, int(qn), key))
        if r['revenue'] is not None:
            qrev_map.setdefault(r['code'], {})[key] = r['revenue']
        if r['eps'] is not None:
            qeps_map.setdefault(r['code'], {})[key] = r['eps']

    sorted_q = sorted(all_q, key=lambda x: (x[0], x[1]))
    q_cols = [k[2] for k in sorted_q]

    return jsonify({
        'months': months,
        'monthly': monthly_map,
        'quarterly_cols': q_cols,
        'quarterly_revenue': qrev_map,
        'quarterly_eps': qeps_map,
        'current_year': current_year,
        'last_year': last_year,
    })

# ── 聶夫 & 林區 成長率指標 API ─────────────────────────────────
_gi_cache = None
_gi_cache_time = 0

@app.route("/api/growth-indicators")
def growth_indicators():
    """計算聶夫總報酬率法 + 林區PEG法所需欄位（30秒快取）"""
    import json as _json
    import traceback
    from datetime import date as _dt
    import time as _time
    global _gi_cache, _gi_cache_time
    now = _time.time()
    if _gi_cache is not None and now - _gi_cache_time < 30:
        return jsonify(_gi_cache)
    try:
        result = _calc_growth_indicators(_json, _dt)
        _gi_cache = result.get_json()
        _gi_cache_time = now
        return result
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

def _calc_growth_indicators(_json, _dt):

    current_year = _dt.today().year

    # ── 1. 抓最近7年 financial_annual（需要6年算5年CAGR）
    min_year = current_year - 6
    try:
        fa_rows = query_db(
            "SELECT code, year, net_income, eps, weighted_shares, common_stock, "
            "revenue, total_equity, cash_dividend "
            "FROM financial_annual WHERE year >= ? ORDER BY code, year",
            (min_year,)
        )
    except Exception:
        try:
            fa_rows = query_db(
                "SELECT code, year, net_income, eps, common_stock, "
                "revenue, total_equity, cash_dividend "
                "FROM financial_annual WHERE year >= ? ORDER BY code, year",
                (min_year,)
            )
            for r in fa_rows:
                r['weighted_shares'] = None
        except Exception:
            fa_rows = query_db(
                "SELECT code, year, net_income, eps, common_stock "
                "FROM financial_annual WHERE year >= ? ORDER BY code, year",
                (min_year,)
            )
            for r in fa_rows:
                r['weighted_shares'] = None
                r['revenue'] = None
                r['total_equity'] = None
                r['cash_dividend'] = None
    # 按 code 分組
    fa_map = {}
    for r in fa_rows:
        fa_map.setdefault(r['code'], []).append(r)

    # ── 2. 抓 stocks 表（股價、沈董估算）
    st_rows = query_db(
        "SELECT code, close, sys_ann_eps, sys_ann_div, sys_ann_pe, sys_ann_yld FROM stocks"
    )
    st_map = {r['code']: r for r in st_rows}

    # ── 3. 抓 user_estimates（個股自訂參數）
    ue_rows = query_db("SELECT code, params FROM user_estimates WHERE params IS NOT NULL")
    ue_map = {}
    for r in ue_rows:
        try:
            ue_map[r['code']] = _json.loads(r['params'])
        except Exception:
            pass

    # ── 4. 逐支計算
    result = {}
    for code, rows in fa_map.items():
        # 按年份排序
        rows.sort(key=lambda x: x['year'])
        st = st_map.get(code)
        if not st or not st.get('close'):
            continue
        close = st['close']

        # 取有 net_income 且 > 0 的年份清單
        valid = [(r['year'], r['net_income'], r['eps']) for r in rows
                 if r.get('net_income') and r['net_income'] > 0
                 and r.get('eps') and r['eps'] > 0]

        if len(valid) < 4:
            continue  # 至少需要4年（算3年CAGR）

        years = [v[0] for v in valid]
        nis = [v[1] for v in valid]
        epss = [v[2] for v in valid]
        latest_yr = years[-1]

        # ── 5年淨利CAGR（A）
        neff_a = None
        if len(valid) >= 6:
            ni_start = nis[-6]
            ni_end = nis[-1]
            if ni_start > 0 and ni_end > 0:
                neff_a = (ni_end / ni_start) ** (1.0 / 5) - 1
        elif len(valid) >= 5:
            ni_start = nis[-5]
            ni_end = nis[-1]
            n = len(valid) - 1
            if ni_start > 0 and ni_end > 0 and n >= 4:
                neff_a = (ni_end / ni_start) ** (1.0 / (years[-1] - years[-5])) - 1

        # ── 3年淨利CAGR（B）
        neff_b = None
        if len(valid) >= 4:
            ni_start3 = nis[-4]
            ni_end3 = nis[-1]
            if ni_start3 > 0 and ni_end3 > 0:
                neff_b = (ni_end3 / ni_start3) ** (1.0 / 3) - 1

        # 如果只能算3年，A也用3年
        if neff_a is None and neff_b is not None:
            neff_a = neff_b

        if neff_a is None:
            continue

        # ── 保守成長率（C）：聶夫三重驗證
        a_pct = neff_a * 100
        b_pct = (neff_b * 100) if neff_b is not None else a_pct
        min_cagr = min(a_pct, b_pct)

        # 折扣係數：差距大打更重折
        gap = abs(a_pct - b_pct)
        if gap > 5:
            discount = 0.7
        elif gap > 3:
            discount = 0.75
        else:
            discount = 0.8

        neff_c = min_cagr * discount

        # ── 警示判斷
        warnings = []
        if neff_c < 0:
            warnings.append('負成長不適用')
        if a_pct > 20:
            warnings.append('CAGR>20%不可信')
        if b_pct < a_pct - 3:
            warnings.append('近期成長減速')
        # 中間有虧損年被跳過
        all_years = [r['year'] for r in rows]
        gap_years = len(all_years) - len(valid)
        if gap_years > 0:
            warnings.append(f'有{gap_years}年虧損被排除')

        # ── 原始EPS CAGR（輔助對照）
        eps_cagr_5y = None
        if len(valid) >= 6:
            e_start = epss[-6]
            e_end = epss[-1]
            if e_start > 0 and e_end > 0:
                eps_cagr_5y = ((e_end / e_start) ** (1.0 / 5) - 1) * 100

        # 股本變動率
        shares_change = None
        shares_list = [(r['year'], r.get('weighted_shares') or r.get('common_stock'))
                       for r in rows if r.get('weighted_shares') or r.get('common_stock')]
        if len(shares_list) >= 2:
            s_start = shares_list[0][1]
            s_end = shares_list[-1][1]
            if s_start and s_start > 0:
                shares_change = (s_end - s_start) / s_start * 100

        # EPS CAGR 與淨利 CAGR 差距警示
        if eps_cagr_5y is not None and abs(eps_cagr_5y - a_pct) > 3:
            warnings.append('EPS與淨利成長率差距大')

        # ── 殖利率：預估 > 沈董
        ue = ue_map.get(code, {})
        div_val = None
        # 優先用 user_estimates 的預估股利
        vm_div = ue.get('vmDiv')
        if vm_div and str(vm_div).strip():
            try:
                div_val = float(vm_div)
            except Exception:
                pass
        # fallback: 沈董股利
        if not div_val and st.get('sys_ann_div'):
            div_val = st['sys_ann_div']

        yld = (div_val / close * 100) if div_val and close > 0 else 0

        # ── PE：預估 > 沈董
        pe = None
        # 優先用 user_estimates 的預估EPS算PE
        vm_eps = ue.get('vmEps')
        if vm_eps and str(vm_eps).strip():
            try:
                est_eps = float(vm_eps)
                if est_eps > 0:
                    pe = close / est_eps
            except Exception:
                pass
        # fallback: 沈董EPS
        if pe is None and st.get('sys_ann_eps') and st['sys_ann_eps'] > 0:
            pe = close / st['sys_ann_eps']

        if pe is None or pe <= 0:
            continue

        # ── 林區：算術平均成長率（B）
        yoy_list = []
        for i in range(1, len(valid)):
            prev_eps = epss[i - 1]
            curr_eps = epss[i]
            if prev_eps > 0:
                yoy_list.append((curr_eps - prev_eps) / prev_eps * 100)

        # 取最近5個YoY
        recent_yoy = yoy_list[-5:] if len(yoy_list) >= 5 else yoy_list
        lynch_b = sum(recent_yoy) / len(recent_yoy) if recent_yoy else None

        # ── 林區：成長一致性（C）
        lynch_c = None
        if lynch_b is not None and a_pct > 0:
            lynch_c = 1 - abs(lynch_b - a_pct) / a_pct
            if lynch_c < 0:
                lynch_c = 0

        # 一致性太低警示
        if lynch_c is not None and lynch_c < 0.5:
            warnings.append('景氣循環股不適用PEG')

        # ── PEG（D）= PE / (保守成長率 + 殖利率)
        # 注意：PEG 用原始保守成長率（neff_c 會在品質系統裡被重算）
        _peg_return = neff_c + yld
        lynch_d = pe / _peg_return if _peg_return > 0 else None

        # ── 灰色標記判斷（分開判斷）
        neff_gray = neff_c < 0 or a_pct > 20 or gap_years >= 2
        lynch_gray = (lynch_c is not None and lynch_c < 0.5)
        gray = neff_gray or lynch_gray

        # ── 五層金字塔品質評級 + 決策矩陣 ──
        # 從 rows 取 ROE / 配息率 / 營收CAGR / 股息CAGR
        roe_list = []
        payout_list = []
        rev_list = []
        div_list = []
        for rr in rows:
            ni = rr.get('net_income')
            eq = rr.get('total_equity')
            e = rr.get('eps')
            cd = rr.get('cash_dividend')
            rv = rr.get('revenue')
            if ni and eq and eq > 0:
                roe_list.append(ni / eq)
            if e and e > 0 and cd is not None:
                payout_list.append(cd / e)
            if rv and rv > 0:
                rev_list.append(rv)
            if cd and cd > 0:
                div_list.append(cd)

        # 內生成長率 = 平均ROE × (1 - 平均配息率)
        intrinsic_growth = None
        avg_roe = None
        avg_roe_pct = None
        avg_payout = None
        if len(roe_list) >= 3 and len(payout_list) >= 3:
            avg_roe = sum(roe_list) / len(roe_list)
            avg_roe_pct = round(avg_roe * 100, 2)
            avg_payout = sum(payout_list) / len(payout_list)
            if avg_payout < 1:
                intrinsic_growth = round(avg_roe * (1 - avg_payout) * 100, 2)

        # 股息 CAGR
        div_cagr = None
        if len(div_list) >= 4:
            if div_list[0] > 0 and div_list[-1] > 0:
                div_cagr = ((div_list[-1] / div_list[0]) ** (1.0 / (len(div_list) - 1)) - 1) * 100

        # ── 五層金字塔品質評級 ──
        quality_details = []
        layer_results = [None, None, None, None, None]  # 5層結果

        # 第1層：護城河測試（平均ROE >= 12% 才通過）
        if avg_roe_pct is not None:
            if avg_roe_pct >= 18:
                layer_results[0] = 'strong'
                quality_details.append(f'L1:護城河強(ROE {avg_roe_pct}%)')
            elif avg_roe_pct >= 12:
                layer_results[0] = 'mid'
                quality_details.append(f'L1:護城河中(ROE {avg_roe_pct}%)')
            else:
                layer_results[0] = 'fail'
                # 例外：殖利率 >= 7% 視為純配息族
                if yld >= 7:
                    quality_details.append(f'L1:無護城河但高殖利率{round(yld,1)}%(ROE {avg_roe_pct}%)')
                    layer_results[0] = 'dividend'
                else:
                    quality_details.append(f'L1:無護城河(ROE {avg_roe_pct}%)')

        # 第2層：真實性測試（內生成長率 vs 實際CAGR，差距 < 5% 通過）
        if intrinsic_growth is not None:
            ig_gap = abs(intrinsic_growth - a_pct)
            if ig_gap < 5:
                layer_results[1] = 'pass'
                quality_details.append(f'L2:真實成長(差距{round(ig_gap,1)}%)')
            else:
                layer_results[1] = 'fail'
                quality_details.append(f'L2:會計可疑(差距{round(ig_gap,1)}%)')

        # 第3層：動能測試（3年CAGR vs 5年CAGR）
        cagr_gap = b_pct - a_pct
        if cagr_gap >= 0:
            layer_results[2] = 'pass'
            quality_details.append(f'L3:成長加速')
        elif cagr_gap > -3:
            layer_results[2] = 'ok'
            quality_details.append(f'L3:成長穩定(差距{round(cagr_gap,1)}%)')
        else:
            layer_results[2] = 'fail'
            quality_details.append(f'L3:成長減速(差距{round(cagr_gap,1)}%)')

        # 第4層：兌現測試（股息CAGR >= 淨利CAGR × 0.7 才通過）
        if div_cagr is not None and a_pct > 0:
            div_ratio = div_cagr / a_pct if a_pct != 0 else 0
            if div_ratio >= 0.7:
                layer_results[3] = 'pass'
                quality_details.append(f'L4:股息兌現(股息CAGR {round(div_cagr,1)}% / 淨利CAGR {round(a_pct,1)}% = {round(div_ratio*100)}%)')
            else:
                layer_results[3] = 'fail'
                quality_details.append(f'L4:兌現不足(股息CAGR {round(div_cagr,1)}% / 淨利CAGR {round(a_pct,1)}% = {round(div_ratio*100)}%)')

        # 第5層：智慧測試（管理層資本配置）
        if avg_roe_pct is not None and avg_payout is not None:
            pr = avg_payout * 100
            if avg_roe_pct > 15 and pr < 50:
                layer_results[4] = 'pass'
                quality_details.append(f'L5:聰明配置(高ROE低配息{round(pr)}%)')
            elif avg_roe_pct > 15 and pr > 60:
                layer_results[4] = 'fail'
                quality_details.append(f'L5:應留更多(高ROE但配息{round(pr)}%)')
            elif avg_roe_pct < 12 and pr > 50:
                layer_results[4] = 'pass'
                quality_details.append(f'L5:聰明配置(低ROE高配息{round(pr)}%)')
            elif avg_roe_pct < 12 and pr < 30:
                layer_results[4] = 'fail'
                quality_details.append(f'L5:毀滅價值(低ROE低配息{round(pr)}%)')
            else:
                layer_results[4] = 'ok'
                quality_details.append(f'L5:中性(ROE{avg_roe_pct}% 配息{round(pr)}%)')

        # ── 綜合評級（層層過濾）──
        # 第1層失敗 → D（除非高殖利率純配息族）
        # 第2層失敗 → C
        # 第3層失敗 → C
        # 第4層失敗 → B
        # 第5層失敗 → B
        # 多項嚴重失敗 → E
        fail_count = sum(1 for r in layer_results if r == 'fail')
        none_count = sum(1 for r in layer_results if r is None)
        passed = sum(1 for r in layer_results if r in ('pass', 'strong', 'mid'))

        # 綜合評級：嚴格按層次判定
        if fail_count >= 3 or (layer_results[0] == 'fail' and fail_count >= 2):
            quality_grade = 'E'  # 多項嚴重失敗
        elif layer_results[0] == 'fail':
            quality_grade = 'D'  # L1 失敗（無護城河）
        elif layer_results[1] == 'fail' or layer_results[2] == 'fail':
            quality_grade = 'C'  # L2 或 L3 失敗（會計可疑/減速）
        elif layer_results[3] == 'fail' or layer_results[4] == 'fail':
            quality_grade = 'B'  # L4 或 L5 失敗（兌現/配置偏弱）
        elif none_count > 0:
            quality_grade = 'B'  # 有資料不足，最高 B
        elif passed == 5:
            quality_grade = 'A'  # 五層全部通過
        else:
            quality_grade = 'B'

        quality_score = passed

        # ── 保守成長率（新版）：加入內生成長率和12%上限 ──
        candidates = [a_pct, b_pct]
        if intrinsic_growth is not None:
            candidates.append(intrinsic_growth)
        conservative_growth = min(candidates) * 0.75
        conservative_growth = min(conservative_growth, 12)  # 上限12%
        neff_c = round(conservative_growth, 2)

        # 重算 Neff 比率（用新的保守成長率）
        total_return = neff_c + yld
        neff_d = total_return / pe if pe > 0 else None

        # ── 四級 Neff 分級 + 5×4 決策矩陣 ──
        if neff_d is not None:
            if neff_d >= 1.3:
                neff_tier = '特價'
            elif neff_d >= 0.9:
                neff_tier = '合理'
            elif neff_d >= 0.7:
                neff_tier = '邊緣'
            else:
                neff_tier = '不買'
        else:
            neff_tier = None

        # 決策矩陣
        decision = None
        if neff_gray:
            decision = '不適用'
        elif neff_d is not None:
            if quality_grade == 'A':
                if neff_d >= 1.3: decision = '立刻研究'
                elif neff_d >= 0.9: decision = '優先研究'
                elif neff_d >= 0.7: decision = '觀察'
                else: decision = '不買'
            elif quality_grade == 'B':
                if neff_d >= 1.3: decision = '研究'
                elif neff_d >= 0.9: decision = '觀察'
                else: decision = '不買'
            elif quality_grade == 'D':
                # D級例外：殖利率>=7% 且 Neff>=1.3 的純配息族
                if yld >= 7 and neff_d >= 1.3 and layer_results[0] == 'dividend':
                    decision = '觀察(配息族)'
                else:
                    decision = '不買'
            else:  # C, E
                decision = '不買'

        entry = {
            'neff_a': round(a_pct, 2),
            'neff_b': round(b_pct, 2),
            'neff_c': round(neff_c, 2),
            'neff_d': round(neff_d, 2) if neff_d is not None else None,
            'lynch_a': round(a_pct, 2),
            'lynch_b': round(lynch_b, 2) if lynch_b is not None else None,
            'lynch_c': round(lynch_c, 2) if lynch_c is not None else None,
            'lynch_d': round(lynch_d, 2) if lynch_d is not None else None,
            'eps_cagr_5y': round(eps_cagr_5y, 2) if eps_cagr_5y is not None else None,
            'shares_change': round(shares_change, 2) if shares_change is not None else None,
            'yield': round(yld, 2),
            'pe': round(pe, 2),
            'discount': discount,
            'gray': gray,
            'neff_gray': neff_gray,
            'lynch_gray': lynch_gray,
            'intrinsic_growth': intrinsic_growth,
            'quality_grade': quality_grade,
            'quality_score': quality_score,
            'quality_details': quality_details,
            'layer_pass': [
                layer_results[1] in ('pass', 'strong', 'mid') if layer_results[1] is not None else None,  # (1)真實性
                layer_results[2] in ('pass', 'strong', 'mid', 'ok') if layer_results[2] is not None else None,  # (2)動能
                layer_results[3] in ('pass', 'strong', 'mid') if layer_results[3] is not None else None,  # (3)兌現
            ],
            'decision': decision,
            'warnings': warnings,
        }
        result[code] = entry

    return jsonify(result)


# ── 沈董系統用的儲存 API ─────────────────────────────────────
@app.route("/api/shendong/estimates/<code>", methods=["GET"])
def shendong_get_estimate(code):
    rows = query_db("SELECT data FROM shendong_estimates WHERE code=?", (code,))
    import json
    return jsonify(json.loads(rows[0]['data']) if rows else {})

@app.route("/api/shendong/estimates/<code>", methods=["POST"])
def shendong_save_estimate(code):
    import json
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS shendong_estimates (code TEXT PRIMARY KEY, data TEXT, updated_at TEXT)")
    conn.execute("INSERT OR REPLACE INTO shendong_estimates (code, data, updated_at) VALUES (?, ?, datetime('now'))", (code, json.dumps(request.json)))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})

@app.route("/api/shendong/estimates", methods=["GET"])
def shendong_get_all_estimates():
    import json
    try:
        rows = query_db("SELECT code, data FROM shendong_estimates")
        return jsonify({r['code']: json.loads(r['data']) for r in rows})
    except Exception:
        return jsonify({})

@app.route("/api/shendong/watchlist", methods=["GET"])
def shendong_get_watchlist():
    try:
        rows = query_db("SELECT code FROM shendong_watchlist ORDER BY added_at")
        return jsonify([r['code'] for r in rows])
    except Exception:
        return jsonify([])

@app.route("/api/shendong/watchlist", methods=["POST"])
def shendong_save_watchlist():
    codes = request.json.get('codes', [])
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS shendong_watchlist (code TEXT PRIMARY KEY, added_at TEXT)")
    conn.execute("DELETE FROM shendong_watchlist")
    for code in codes:
        conn.execute("INSERT OR IGNORE INTO shendong_watchlist (code, added_at) VALUES (?, datetime('now'))", (code,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route("/api/daily-briefing")
def daily_briefing():
    return jsonify(get_daily_briefing())

@app.route("/api/realtime")
def realtime():
    """盤中即時報價（前端傳入代碼清單）"""
    import requests as req
    codes_param = request.args.get("codes", "")
    if not codes_param:
        return jsonify([])

    code_list = [c.strip() for c in codes_param.split(",") if c.strip()]
    if not code_list:
        return jsonify([])

    # 查市場別
    rows = query_db("SELECT code, market FROM stocks WHERE code IN ({})".format(
        ",".join("?" for _ in code_list)), code_list)
    market_map = {r['code']: r['market'] for r in rows}

    # 組 TWSE 即時 API 參數（每批最多 50 檔）
    all_results = []
    ex_codes = []
    for code in code_list:
        mkt = market_map.get(code, '上市')
        prefix = 'tse' if mkt == '上市' else 'otc'
        ex_codes.append(f"{prefix}_{code}.tw")

    for i in range(0, len(ex_codes), 50):
        batch = ex_codes[i:i+50]
        try:
            url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={'|'.join(batch)}"
            r = req.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            data = r.json()
            for s in data.get("msgArray", []):
                price = s.get("z")
                if price == "-" or not price:
                    # z 沒值時取最佳買價（五檔第一筆）
                    bid = s.get("b", "")
                    if bid and "_" in bid:
                        price = bid.split("_")[0]
                if price == "-" or not price:
                    price = s.get("y")  # 最後 fallback 昨收
                all_results.append({
                    "code": s.get("c"),
                    "name": s.get("n"),
                    "price": float(price) if price else None,
                    "open": float(s["o"]) if s.get("o") else None,
                    "high": float(s["h"]) if s.get("h") else None,
                    "low": float(s["l"]) if s.get("l") else None,
                    "volume": int(s["v"]) if s.get("v") else None,
                    "time": s.get("t"),
                    "yesterday": float(s["y"]) if s.get("y") else None,
                })
        except Exception: pass

    return jsonify(all_results)

@app.route("/api/news")
def news():
    code = request.args.get("code")
    tier = int(request.args.get("tier", 1))
    limit = int(request.args.get("limit", 50))
    if request.args.get("important") == "1" and code:
        try:
            rows = query_db("""SELECT * FROM material_news
                              WHERE code=? AND status='important' AND created_at > datetime('now', '-30 days')
                              ORDER BY created_at DESC LIMIT ?""", (code, limit))
        except Exception:
            rows = []
        return jsonify(rows)
    return jsonify(get_recent_news(code, tier, limit))

@app.route("/api/news/<int:nid>/upgrade", methods=["POST"])
def upgrade_news(nid):
    """把 Tier 0 升級到 Tier 1（使用者認為被誤過濾）"""

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE material_news SET tier=1, matched_rule='使用者升級' WHERE id=? AND tier=0", (nid,))
    conn.commit()
    conn.close()
    _bg_push_table('material_news',
        ['id','code','name','date','time','subject','description','tier',
         'matched_rule','created_at','direction','link','status'],
        ['id'], clear_first=True,
        create_sql="""CREATE TABLE IF NOT EXISTS material_news (
            id INTEGER PRIMARY KEY, code TEXT, name TEXT, date TEXT, time TEXT,
            subject TEXT, description TEXT, tier INTEGER, matched_rule TEXT,
            created_at TEXT, direction TEXT, link TEXT, status TEXT)""")
    return jsonify({"status": "ok"})

@app.route("/api/news/<int:nid>/status", methods=["POST"])
def update_news_status(nid):

    status = request.json.get("status") if request.is_json else request.args.get("status")
    if status not in ('important', 'dismissed', None):
        return jsonify({"error": "status must be important, dismissed, or null"}), 400
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE material_news SET status=? WHERE id=?", (status, nid))
    conn.commit()
    conn.close()
    _bg_push_table('material_news',
        ['id','code','name','date','time','subject','description','tier',
         'matched_rule','created_at','direction','link','status'],
        ['id'], clear_first=True,
        create_sql="""CREATE TABLE IF NOT EXISTS material_news (
            id INTEGER PRIMARY KEY, code TEXT, name TEXT, date TEXT, time TEXT,
            subject TEXT, description TEXT, tier INTEGER, matched_rule TEXT,
            created_at TEXT, direction TEXT, link TEXT, status TEXT)""")
    return jsonify({"status": "ok"})

@app.route("/api/news-flags")
def news_flags():
    """回傳有重要新聞的股票代碼清單（給總表標記用）"""
    rows = query_db("""SELECT code, COUNT(*) as cnt FROM material_news
                       WHERE status='important' AND created_at > datetime('now', '-30 days')
                       GROUP BY code""")
    return jsonify({r['code']: r['cnt'] for r in rows})

@app.route("/api/audit")
def audit():
    code = request.args.get("code")
    limit = int(request.args.get("limit", 100))
    return jsonify(get_audit_log(limit, code))

# ── 同業比較 ────────────────────────────────────────────────
@app.route("/api/industry-compare/<code>")
def industry_compare(code):
    """回傳同產業所有股票的關鍵指標，以及目標股票在同業中的排名"""
    # 1. 取得目標股票的產業
    target = query_db("SELECT code, name, industry FROM stocks WHERE code = ?", (code,))
    if not target or not target[0].get("industry"):
        return jsonify({"error": "找不到股票或無產業分類"}), 404
    industry = target[0]["industry"]

    # 2. 撈同產業全部股票的關鍵欄位
    peers = query_db("""
        SELECT code, name, close, eps_y1, eps_y2, eps_y1_label,
               revenue_yoy, revenue_cum_yoy, div_c1, div_1_label,
               price_pos, change_240d, market
        FROM stocks
        WHERE industry = ? AND close IS NOT NULL AND close > 0
        ORDER BY code
    """, (industry,))

    # 3. 計算衍生指標
    for p in peers:
        eps = p.get("eps_y1")
        close = p.get("close")
        # 本益比
        if eps and eps > 0 and close:
            p["pe"] = round(close / eps, 2)
        else:
            p["pe"] = None
        # 殖利率 (%)
        div = p.get("div_c1") or 0
        if close and close > 0:
            p["yield_pct"] = round(div / close * 100, 2)
        else:
            p["yield_pct"] = None
        # EPS 成長率 (%)
        eps1 = p.get("eps_y1")
        eps2 = p.get("eps_y2")
        if eps1 is not None and eps2 is not None and eps2 != 0:
            p["eps_growth"] = round((eps1 - eps2) / abs(eps2) * 100, 2)
        else:
            p["eps_growth"] = None

    # 4. 排名函式（數值越大排名越前）
    def rank_desc(lst, key):
        vals = [(i, x.get(key)) for i, x in enumerate(lst)]
        valid = [(i, v) for i, v in vals if v is not None]
        valid.sort(key=lambda t: t[1], reverse=True)
        ranks = {}
        for rank, (i, _) in enumerate(valid, 1):
            ranks[i] = rank
        total = len(valid)
        return ranks, total

    # 數值越小排名越前（本益比低 = 好）
    def rank_asc(lst, key):
        vals = [(i, x.get(key)) for i, x in enumerate(lst)]
        valid = [(i, v) for i, v in vals if v is not None]
        valid.sort(key=lambda t: t[1])
        ranks = {}
        for rank, (i, _) in enumerate(valid, 1):
            ranks[i] = rank
        total = len(valid)
        return ranks, total

    metrics = [
        ("pe",              "asc"),    # 本益比越低越好
        ("eps_y1",          "desc"),   # EPS 越高越好
        ("eps_growth",      "desc"),   # EPS 成長越高越好
        ("revenue_yoy",     "desc"),   # 營收年增越高越好
        ("revenue_cum_yoy", "desc"),   # 累計營收年增越高越好
        ("yield_pct",       "desc"),   # 殖利率越高越好
        ("change_240d",     "desc"),   # 240日漲幅越高越好
    ]

    # 計算每個指標的排名
    ranking_data = {}
    for key, direction in metrics:
        if direction == "desc":
            ranks, total = rank_desc(peers, key)
        else:
            ranks, total = rank_asc(peers, key)
        ranking_data[key] = {"ranks": ranks, "total": total}

    # 5. 找出目標股票的 index
    target_idx = None
    for i, p in enumerate(peers):
        if p["code"] == code:
            target_idx = i
            break

    # 6. 組裝目標股票的排名摘要
    summary = {}
    if target_idx is not None:
        for key, _ in metrics:
            rd = ranking_data[key]
            rank = rd["ranks"].get(target_idx)
            total = rd["total"]
            if rank and total:
                summary[key] = {
                    "rank": rank,
                    "total": total,
                    "percentile": round((1 - (rank - 1) / total) * 100, 1)
                }
            else:
                summary[key] = None

    # 7. 把排名塞進每筆 peer 資料
    for i, p in enumerate(peers):
        p["rankings"] = {}
        for key, _ in metrics:
            rd = ranking_data[key]
            rank = rd["ranks"].get(i)
            total = rd["total"]
            if rank:
                p["rankings"][key] = {"rank": rank, "total": total}
            else:
                p["rankings"][key] = None

    # 8. 計算同業中位數
    import statistics
    medians = {}
    for key, _ in metrics:
        vals = [p.get(key) for p in peers if p.get(key) is not None]
        if vals:
            medians[key] = round(statistics.median(vals), 2)
        else:
            medians[key] = None

    return jsonify({
        "code": code,
        "name": target[0]["name"],
        "industry": industry,
        "peer_count": len(peers),
        "summary": summary,
        "medians": medians,
        "peers": peers
    })

# ── ETF 成分股 API ─────────────────────────────────────────
@app.route("/api/etf/stock/<code>")
def etf_membership(code):
    """查詢某股票被哪些 ETF 持有"""
    return jsonify(get_stock_etf_membership(code))

@app.route("/api/etf/<etf_code>/holdings")
def etf_holdings(etf_code):
    """查詢某 ETF 的所有持股"""
    return jsonify(get_etf_holdings_list(etf_code))

@app.route("/api/etf/changes")
def etf_changes():
    """查詢 ETF 成分股異動紀錄"""
    etf_code = request.args.get("etf")
    limit = int(request.args.get("limit", 50))
    return jsonify(get_etf_changes(etf_code, limit))

@app.route("/api/etf/changes-report")
def etf_changes_report():
    """ETF 成分股異動報告（按 ETF+日期分組，納入/剔除分開）"""
    limit = int(request.args.get("limit", 500))
    rows = query_db("""
        SELECT c.etf_code, COALESCE(i.name, c.etf_code) as etf_name,
               i.category as etf_category,
               c.stock_code, c.stock_name, c.action, c.change_date
        FROM etf_changes c
        INNER JOIN etf_info i ON c.etf_code = i.code
        ORDER BY c.change_date DESC, c.etf_code, c.action
        LIMIT ?
    """, [limit])
    # 各 ETF 目前完整成分股
    holdings_map = {}
    for h in query_db("SELECT etf_code, stock_code, stock_name FROM etf_holdings ORDER BY etf_code, weight DESC"):
        holdings_map.setdefault(h['etf_code'], []).append(
            {'code': h['stock_code'], 'name': h['stock_name'] or h['stock_code']})

    # 分組：{etf_code + change_date} → {etf_code, etf_name, change_date, holdings, add:[], remove:[]}
    # 取得所有 ETF 的 category 對照
    category_map = {}
    for ei in query_db("SELECT code, category FROM etf_info"):
        category_map[ei['code']] = ei.get('category') or ''

    groups = {}
    for r in rows:
        key = f"{r['etf_code']}_{r['change_date']}"
        if key not in groups:
            groups[key] = {
                'etf_code': r['etf_code'],
                'etf_name': r['etf_name'],
                'category': category_map.get(r['etf_code'], ''),
                'change_date': r['change_date'],
                'holdings': holdings_map.get(r['etf_code'], []),
                'add': [], 'remove': []
            }
        item = {'code': r['stock_code'], 'name': r['stock_name'] or r['stock_code']}
        groups[key][r['action']].append(item)

    # 所有追蹤的 ETF（含無異動的）
    all_etfs = query_db("SELECT code, name, category FROM etf_info ORDER BY code")
    tracked_codes = {g['etf_code'] for g in groups.values()}
    for etf in all_etfs:
        if etf['code'] not in tracked_codes:
            groups[f"{etf['code']}_none"] = {
                'etf_code': etf['code'],
                'etf_name': etf['name'] or etf['code'],
                'category': etf.get('category') or '',
                'change_date': None,
                'holdings': holdings_map.get(etf['code'], []),
                'add': [], 'remove': []
            }

    return jsonify(sorted(groups.values(), key=lambda g: (g['change_date'] or '', g['etf_code']), reverse=True))

@app.route("/api/etf/list")
def etf_list():
    """取得所有追蹤的 ETF 清單及其持股數"""
    rows = query_db("""
        SELECT i.code, i.name, i.issuer, i.category, i.last_fetch,
               COUNT(h.stock_code) as holding_count
        FROM etf_info i
        LEFT JOIN etf_holdings h ON i.code = h.etf_code
        GROUP BY i.code
        ORDER BY i.code
    """)
    return jsonify(rows)

# ── 連線測試（除錯用）────────────────────────────────────────
@app.route("/api/test-db")
def test_db():
    import os
    db_url = os.environ.get('DATABASE_URL', 'NOT SET')
    # 遮蔽密碼
    safe_url = db_url[:30] + '***' + db_url[-30:] if len(db_url) > 60 else db_url
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT 1")
        result = c.fetchone()
        conn.close()
        return jsonify({"status": "ok", "db_type": sqlite3.DB_TYPE, "url": safe_url, "test": str(result)})
    except Exception as e:
        return jsonify({"status": "error", "db_type": sqlite3.DB_TYPE, "url": safe_url, "error": str(e)})

# ── 前端首頁 ────────────────────────────────────────────────
@app.route("/")
def index():
    return app.send_static_file("index.html")

# ── 初始化資料庫 ────────────────────────────────────────────
def _init_all_db():
    try:
        init_db()
        init_financial_db()
        init_monthly_revenue_db()
        init_quarterly_db()
        init_pe_history_db()
        init_etf_db()
        # PostgreSQL 需要額外建立 api_health 表
        if sqlite3.DB_TYPE == 'postgresql':
            conn = sqlite3.connect()
            c = conn.cursor()
            c.execute("""CREATE TABLE IF NOT EXISTS api_health (
                source TEXT PRIMARY KEY,
                description TEXT,
                last_success TEXT,
                last_fail TEXT,
                fail_count INTEGER DEFAULT 0,
                last_record_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'ok'
            )""")
            conn.commit()
            conn.close()
        print("[DB] 初始化完成")
    except Exception as e:
        print(f"[DB] 初始化失敗（表格可能已存在）: {e}")

_init_all_db()

# ── 雲端排程（APScheduler，取代 LaunchAgent）────────────────
# 只在主 worker 啟動（避免 gunicorn 多 worker 重複執行）
if os.environ.get('DATABASE_URL') and os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        scheduler = BackgroundScheduler(timezone='Asia/Taipei')
        # 每 30 分鐘快速更新（股價 + 最新營收 + EPS）
        scheduler.add_job(quick_update, 'interval', minutes=30,
                          id='quick_update', replace_existing=True)
        # 每天 07:00 完整爬蟲（本機 06:00 跑 28~41 分鐘，錯開 1 小時避免撞車）
        scheduler.add_job(scraper_run, 'cron', hour=7, minute=0,
                          id='daily_scrape', replace_existing=True)
        # 週一到週五 15:30 盤後更新（本機 14:30 跑完+push 約需 40 分鐘）
        scheduler.add_job(scraper_run, 'cron', day_of_week='mon-fri',
                          hour=15, minute=30,
                          id='afternoon_scrape', replace_existing=True)
        # 三大法人：Render 上群益會被擋，不排程
        # 法人資料由本機 17:10 抓完後 push 到 Render（/api/refresh/institutional POST with data）
        scheduler.start()
        print("[排程] APScheduler 已啟動")
    except Exception as e:
        print(f"[排程] APScheduler 啟動失敗: {e}")

# ── 使用者清單（觀察/持股/重點/體質）─────────────────────
def _init_user_lists():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS user_lists (
        list_type TEXT NOT NULL,
        code TEXT NOT NULL,
        added_at TEXT,
        price_at REAL,
        PRIMARY KEY (list_type, code)
    )""")
    # 個股筆記也存 DB
    c.execute("""CREATE TABLE IF NOT EXISTS user_notes (
        code TEXT PRIMARY KEY,
        content TEXT,
        updated_at TEXT
    )""")
    # 個股估值參數也存 DB
    c.execute("""CREATE TABLE IF NOT EXISTS user_estimates (
        code TEXT PRIMARY KEY,
        params TEXT,
        updated_at TEXT
    )""")
    # 確保 est_year 欄位存在
    try: c.execute("ALTER TABLE user_estimates ADD COLUMN est_year INTEGER")
    except Exception: pass
    try: conn.commit()
    except Exception: pass
    conn.close()

_init_user_lists()

# 自動清除過期預估（隔年 3/31 後清除）
def _cleanup_expired_estimates():
    from datetime import datetime
    now = datetime.now()
    roc_year = now.year - 1911
    if now.month > 3:
        cutoff_year = roc_year - 1
    else:
        cutoff_year = roc_year - 2
    if cutoff_year > 0:
        try:
            conn = sqlite3.connect(DB_PATH)
            deleted = conn.execute("DELETE FROM user_estimates WHERE est_year IS NOT NULL AND est_year <= ?",
                                   (cutoff_year,)).rowcount
            conn.commit()
            conn.close()
            if deleted > 0:
                print(f"[自動清除] 已清除 {deleted} 筆過期預估（est_year <= {cutoff_year}）")
        except Exception: pass

_cleanup_expired_estimates()

# 啟動時修正稅務資料（本機+Render 通用）
try:
    from scraper import _fix_tax_data
    _fix_tax_data()
except Exception: pass

@app.route("/api/user-lists")
def get_user_lists():
    rows = query_db("SELECT list_type, code, added_at, price_at FROM user_lists ORDER BY list_type, code")
    result = {}
    for r in rows:
        lt = r['list_type']
        if lt not in result:
            result[lt] = []
        result[lt].append({'code': r['code'], 'added_at': r['added_at'], 'price_at': r['price_at']})
    return jsonify(result)

@app.route("/api/user-lists/<list_type>", methods=["POST"])
def update_user_list(list_type):

    from datetime import datetime
    if list_type not in ('watch', 'hold', 'focus', 'quality', 'skip', 'track'):
        return jsonify({"error": "invalid list_type"}), 400
    data = request.json
    action = data.get('action')  # 'add' or 'remove' or 'sync'
    code = data.get('code')

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    if action == 'add' and code:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        price = data.get('price')
        c.execute("INSERT OR REPLACE INTO user_lists (list_type, code, added_at, price_at) VALUES (?,?,?,?)",
                  (list_type, code, now, price))
    elif action == 'remove' and code:
        c.execute("DELETE FROM user_lists WHERE list_type=? AND code=?", (list_type, code))
    elif action == 'sync':
        # 整批同步（從 localStorage 遷移用）
        codes = data.get('codes', [])
        c.execute("DELETE FROM user_lists WHERE list_type=?", (list_type,))
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for item in codes:
            if isinstance(item, str):
                c.execute("INSERT OR IGNORE INTO user_lists (list_type, code, added_at) VALUES (?,?,?)",
                          (list_type, item, now))
            elif isinstance(item, dict):
                c.execute("INSERT OR IGNORE INTO user_lists (list_type, code, added_at, price_at) VALUES (?,?,?,?)",
                          (list_type, item.get('code',''), now, item.get('price')))

    conn.commit()
    conn.close()
    _bg_push_table('user_lists', ['list_type','code','added_at','price_at'],
                   ['list_type','code'], clear_first=True,
                   create_sql="""CREATE TABLE IF NOT EXISTS user_lists (
                       list_type TEXT NOT NULL, code TEXT NOT NULL, added_at TEXT, price_at REAL,
                       PRIMARY KEY (list_type, code))""")
    return jsonify({"status": "ok"})

# ── 重點追蹤 ──────────────────────────────────────────────
@app.route("/api/focus-tracking")
def get_focus_tracking():
    """取得所有重點追蹤股票 + 最近訊號"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # 追蹤清單
    tracked = [dict(r) for r in conn.execute(
        "SELECT * FROM focus_tracking ORDER BY focus_date DESC").fetchall()]
    # 近 7 天訊號
    signals = [dict(r) for r in conn.execute(
        "SELECT * FROM focus_signals WHERE date >= date('now', '-7 days') ORDER BY date DESC").fetchall()]
    conn.close()
    return jsonify({"tracked": tracked, "signals": signals})

@app.route("/api/focus-tracking", methods=["POST"])
def update_focus_tracking():
    """勾選/取消重點追蹤"""

    from datetime import datetime as dt
    data = request.json
    action = data.get('action')  # 'add' or 'remove'
    code = data.get('code')
    if not code:
        return jsonify({"error": "missing code"}), 400

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # 確保表存在
    c.execute("""CREATE TABLE IF NOT EXISTS focus_tracking (
        code TEXT PRIMARY KEY, focus_date TEXT, focus_price REAL,
        signal_mode TEXT DEFAULT 'initial', mode_switch_date TEXT,
        last_signal_date TEXT, last_signal_type TEXT, note TEXT)""")

    if action == 'add':
        price = data.get('price')
        note = data.get('note', '')
        now = dt.now().strftime('%Y-%m-%d')
        c.execute("""INSERT OR REPLACE INTO focus_tracking
                     (code, focus_date, focus_price, signal_mode, note)
                     VALUES (?,?,?,'initial',?)""",
                  (code, now, price, note))
    elif action == 'remove':
        c.execute("DELETE FROM focus_tracking WHERE code=?", (code,))
        c.execute("DELETE FROM focus_signals WHERE code=?", (code,))
    conn.commit()
    conn.close()
    _bg_push_table('focus_tracking',
        ['code','focus_date','focus_price','signal_mode','mode_switch_date',
         'last_signal_date','last_signal_type','note'],
        ['code'], clear_first=True,
        create_sql="""CREATE TABLE IF NOT EXISTS focus_tracking (
            code TEXT PRIMARY KEY, focus_date TEXT, focus_price REAL,
            signal_mode TEXT DEFAULT 'initial', mode_switch_date TEXT,
            last_signal_date TEXT, last_signal_type TEXT, note TEXT)""")
    _bg_push_table('focus_signals',
        ['code','date','signal_type','detail'],
        ['code','date','signal_type'],
        where="WHERE date >= date('now', '-30 days')",
        create_sql="""CREATE TABLE IF NOT EXISTS focus_signals (
            code TEXT NOT NULL, date TEXT NOT NULL, signal_type TEXT NOT NULL,
            detail TEXT, PRIMARY KEY (code, date, signal_type))""")
    return jsonify({"status": "ok"})

@app.route("/api/focus-signals/<code>")
def get_focus_signals(code):
    """取得單一股票的訊號歷史"""
    rows = query_db(
        "SELECT * FROM focus_signals WHERE code=? ORDER BY date DESC LIMIT 30", (code,))
    return jsonify(rows)

# ── 使用者設定（跨裝置同步）────────────────────────────────
@app.route("/api/user-settings")
def get_user_settings():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS user_settings (
            key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)""")
        conn.commit()
    except Exception: pass
    rows = conn.execute("SELECT key, value, updated_at FROM user_settings").fetchall()
    conn.close()
    result = {r[0]: r[1] for r in rows}
    # 回傳最後更新時間
    times = [r[2] for r in rows if r[2]]
    if times:
        result['_updated_at'] = max(times)
    return jsonify(result)

@app.route("/api/user-settings", methods=["POST"])
def save_user_settings():

    from datetime import datetime
    data = request.json
    if not data:
        return jsonify({"error": "no data"}), 400
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS user_settings (
            key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)""")
    except Exception: pass
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for key, value in data.items():
        c.execute("INSERT OR REPLACE INTO user_settings (key, value, updated_at) VALUES (?,?,?)",
                  (key, value, now))
    conn.commit()
    conn.close()
    _bg_push_table('user_settings', ['key','value','updated_at'], ['key'],
                   create_sql="""CREATE TABLE IF NOT EXISTS user_settings (
                       key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)""")
    return jsonify({"status": "ok"})


@app.route("/api/user-notes/<code>", methods=["GET"])
def get_user_note(code):
    rows = query_db("SELECT content, updated_at FROM user_notes WHERE code=?", (code,))
    if rows:
        return jsonify(rows[0])
    return jsonify({"content": "", "updated_at": None})

@app.route("/api/user-notes/<code>", methods=["POST"])
def save_user_note(code):

    from datetime import datetime
    content = request.json.get('content', '')
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if content.strip():
        c.execute("INSERT OR REPLACE INTO user_notes (code, content, updated_at) VALUES (?,?,?)",
                  (code, content, now))
    else:
        c.execute("DELETE FROM user_notes WHERE code=?", (code,))
    conn.commit()
    conn.close()
    _bg_push_table('user_notes', ['code','content','updated_at'], ['code'],
                   create_sql="""CREATE TABLE IF NOT EXISTS user_notes (
                       code TEXT PRIMARY KEY, content TEXT, updated_at TEXT)""")
    return jsonify({"status": "ok"})

# ── 檢核表 API ─────────────────────────────────────────────
@app.route("/api/stocks/<code>/checklist")
def get_checklist(code):
    _init_checklist_db()
    rows = query_db("SELECT * FROM stock_checklist WHERE code=?", (code,))
    if rows:
        import json
        r = dict(rows[0])
        if r.get('detail'):
            try: r['detail'] = json.loads(r['detail'])
            except Exception: pass
        return jsonify(r)
    return jsonify({})

@app.route("/api/checklist/refresh", methods=["POST"])
def refresh_checklist():
    """手動觸發重算所有檢核表"""
    count = calc_all_checklists()
    return jsonify({"status": "ok", "count": count})

@app.route("/api/user-estimates-all")
def get_all_user_estimates():
    """批次取得所有個股估值參數"""
    import json as _json
    rows = query_db("SELECT code, params FROM user_estimates WHERE params IS NOT NULL")
    result = {}
    for r in rows:
        try:
            result[r['code']] = _json.loads(r['params'])
        except Exception:
            pass
    return jsonify(result)

@app.route("/api/user-estimates/<code>", methods=["GET"])
def get_user_estimate(code):
    rows = query_db("SELECT params, updated_at FROM user_estimates WHERE code=?", (code,))
    if rows and rows[0]['params']:
        import json
        return jsonify(json.loads(rows[0]['params']))
    return jsonify({})

@app.route("/api/user-estimates/<code>", methods=["POST"])
def save_user_estimate(code):

    from datetime import datetime
    import json
    params = request.json
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    est_year = datetime.now().year - 1911  # 民國年
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO user_estimates (code, params, updated_at, est_year) VALUES (?,?,?,?)",
              (code, json.dumps(params, ensure_ascii=False), now, est_year))
    conn.commit()
    conn.close()
    # 即時重算該股檢核表
    try: _recalc_checklist_single(code)
    except Exception: pass
    # 本機才推 Render（背景執行避免阻塞）
    if not os.environ.get('DATABASE_URL'):
        def _push_single():
            try:
                from render_sync import _push_table_to_render
                _push_table_to_render(
                    table='stock_checklist',
                    columns=['code','chk_1','chk_2','chk_3','chk_4','chk_5','chk_6',
                             'chk_7','chk_8','chk_9','chk_10','chk_11','chk_12','chk_13',
                             'pass_count','total_count','detail',
                             'eps_setting','div_setting','yld_high','yld_max','pe_high','pe_low',
                             'lt_div','lt_yld','val_a','val_a1','val_a2','val_aa','lt5','lt6','lt7',
                             'updated_at'],
                    pk=['code'],
                    where=f"WHERE code='{code}'",
                )
            except Exception: pass
        threading.Thread(target=_push_single, daemon=True).start()
    return jsonify({"status": "ok"})

# ── 啟動 ────────────────────────────────────────────────────
if __name__ == "__main__":
    is_local = not os.environ.get('DATABASE_URL')
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=is_local)
