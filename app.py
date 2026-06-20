"""
後端 API：Flask
提供股票資料給前端網頁
"""

import logging
from flask import Flask, jsonify, request, make_response
import os
import db as sqlite3
import threading

IS_CLOUD = os.environ.get('DATABASE_URL') is not None

logger = logging.getLogger(__name__)
from guardian import (generate_health_report, get_provider_status, PROVIDER_TIERS,
                      get_all_breakers, get_breaker,
                      get_quarantine_list, resolve_quarantine,
                      get_fingerprint_stats, get_coverage_map,
                      get_audit_log, get_daily_briefing,
                      get_recent_news,
                      cross_validate, get_latest_validation)
from scraper import (run as scraper_run, run_prices, run_maintenance,
                     refresh_prices, init_db, init_financial_db,
                     init_monthly_revenue_db, init_quarterly_db,
                     init_pe_history_db, fetch_company_financials,
                     fetch_company_monthly_revenue, fetch_company_quarterly,
                     fetch_pe_history, _calc_fin_grade, fetch_institutional,
                     quick_update, estimate_system_eps, estimate_system_eps_multi,
                     estimate_annual_eps, _log_estimate, _fix_tax_data,
                     cross_validate_financial)
from etf_fetcher import (init_etf_db, get_stock_etf_membership,
                         get_etf_holdings_list, get_etf_changes)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

app = Flask(__name__, static_folder=".", static_url_path="")
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0  # static file 不快取
app.config['COMPRESS_MIMETYPES'] = ['application/json']
DB_PATH = "stocks.db"

# ── 表結構常數（push 和 API 共用）────────────────────────────
_USER_NOTES_COLS = ['code','content','news_archive','updated_at',
                    'moat_strength','moat_source','structural_risk',
                    'structural_risk_desc','growth_catalyst','confidence','lynch_override']
_USER_NOTES_CREATE = """CREATE TABLE IF NOT EXISTS user_notes (
    code TEXT PRIMARY KEY, content TEXT, news_archive TEXT, updated_at TEXT,
    moat_strength TEXT, moat_source TEXT, structural_risk TEXT,
    structural_risk_desc TEXT, growth_catalyst TEXT, confidence TEXT, lynch_override TEXT)"""

_REPORT_COLS = ['code','content','updated_at','snapshot_price','snapshot_grade','snapshot_eps','snapshot_judgment']
_REPORT_CREATE = """CREATE TABLE IF NOT EXISTS investment_reports (
    code TEXT PRIMARY KEY, content TEXT, updated_at TEXT,
    snapshot_price REAL, snapshot_grade TEXT, snapshot_eps REAL, snapshot_judgment TEXT)"""

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
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        # 移除所有可能觸發 304 的標頭
        response.headers.pop('ETag', None)
        response.headers.pop('Last-Modified', None)
        # 加 Vary 確保不同請求不共用快取
        response.headers['Vary'] = '*'
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
_cache_lock = threading.Lock()

# ── 爬蟲狀態鎖（避免同時跑兩次）──────────────────────────
_refresh_lock   = threading.Lock()
_is_refreshing  = False
_bg_done_at     = None  # 背景更新完成時間

def query_db(sql, args=()):
    with sqlite3.get_conn(row_factory=True) as conn:
        c = conn.cursor()
        c.execute(sql, args)
        rows = [dict(r) for r in c.fetchall()]
    return rows

# ── 全域設定讀取（user_settings 表）────────────────────────────
_global_settings_cache = None
_global_settings_time = 0

def _get_global_settings():
    """從 DB 讀取全域設定，30 秒快取"""
    global _global_settings_cache, _global_settings_time
    import time as _t, json as _j
    now = _t.time()
    if _global_settings_cache and now - _global_settings_time < 30:
        return _global_settings_cache

    defaults = {
        'div_weights': [30, 30, 20, 10, 10],
        'blend_ratio': {'shen': 50, 'wt': 50},
        'pe_high': 18, 'pe_low': 10,
        'yld_floor': 5, 'yld_high': 5.5, 'yld_max': 6, 'lt_yld': 6,
    }
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT key, value FROM user_settings WHERE key IN ('global_val_params','blend_ratio','global_div_weights')").fetchall()
        conn.close()
        for key, val in rows:
            try:
                d = _j.loads(val)
                if key == 'global_val_params':
                    if d.get('peHigh') is not None: defaults['pe_high'] = float(d['peHigh'])
                    if d.get('peLow') is not None: defaults['pe_low'] = float(d['peLow'])
                    if d.get('yldFloor') is not None: defaults['yld_floor'] = float(d['yldFloor'])
                    if d.get('yldHigh') is not None: defaults['yld_high'] = float(d['yldHigh'])
                    if d.get('yldMax') is not None: defaults['yld_max'] = float(d['yldMax'])
                    if d.get('ltYld') is not None: defaults['lt_yld'] = float(d['ltYld'])
                elif key == 'blend_ratio':
                    defaults['blend_ratio'] = d
                elif key == 'global_div_weights':
                    if isinstance(d, list): defaults['div_weights'] = d
            except Exception:
                pass
    except Exception:
        pass

    _global_settings_cache = defaults
    _global_settings_time = now
    return defaults

def _get_stock_params(user_params, global_settings):
    """取得個股參數（個股覆蓋 > 全域預設）"""
    gs = global_settings
    pe_hi = gs['pe_high']
    pe_lo = gs['pe_low']
    y_high = gs['yld_high']
    y_max = gs['yld_max']
    if user_params:
        if user_params.get('peHigh'): pe_hi = float(user_params['peHigh'])
        if user_params.get('peLow'): pe_lo = float(user_params['peLow'])
        if user_params.get('yldHigh'): y_high = float(user_params['yldHigh'])
        if user_params.get('yldMax'): y_max = float(user_params['yldMax'])
    return pe_hi, pe_lo, y_high, y_max

# ── 沈董EPS/股利/綜合股利 後端計算 ────────────────────────────
DEFAULT_WEIGHTS = [30, 30, 20, 10, 10]

def _calc_shen_fields(r, cur_roc, global_settings=None, qf_data=None):
    """計算沈董EPS、沈董股利、綜合股利，寫入 row dict"""
    if global_settings is None:
        global_settings = _get_global_settings()
    # 沈董EPS — 當年度已公佈季度年化（Q1×4）
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

    r['_shen_is_fallback'] = is_fallback
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

    # 加權股利（用全域設定的權重）
    ws = global_settings.get('div_weights', DEFAULT_WEIGHTS)
    wdiv = wsum = 0
    for i in range(1, 7):
        dc = r.get(f'div_c{i}') or 0
        ds = r.get(f'div_s{i}') or 0
        w = ws[i - 1] if i <= len(ws) else 0
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
    r['weighted_payout'] = round(weighted_payout * 100 * 100) / 100 if weighted_payout is not None else None  # 存百分比值（如 52.57 代表 52.57%）

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

    # 綜合股利 = 沈董股利 × A% + 加權股利 × B%（從全域設定讀取比例）
    br = global_settings.get('blend_ratio', {'shen': 50, 'wt': 50})
    bS = (br.get('shen') or 50) / 100
    bW = (br.get('wt') or 50) / 100
    sd = r.get('shen_div')
    wd = r.get('weighted_div')
    if sd is not None and wd is not None:
        r['blend_div'] = round((sd * bS + wd * bW) * 100) / 100
        r['_blend_div_formula'] = f'沈董股利{sd}×{bS*100:.0f}% + 加權股利{wd}×{bW*100:.0f}% = {r["blend_div"]}'
    elif sd is not None:
        r['blend_div'] = round(sd * 100) / 100
        r['_blend_div_formula'] = f'沈董股利{sd}（無加權股利）'
    elif wd is not None:
        r['blend_div'] = round(wd * 100) / 100
        r['_blend_div_formula'] = f'加權股利{wd}（無沈董股利）'
    else:
        r['blend_div'] = None


# ── 矩陣等級計算（與 guardian.py 一致）─────────────────────────
def _calc_matrix_grade(pe, yld, pe_high=18, pe_low=10, yld_max=6.0, yld_high=5.5, yld_floor=5.0):
    """矩陣等級：AA/A1/A2/A/B1A/B2A/B1/B2/觀察/臨界點/X"""
    if pe is None or pe <= 0 or yld is None or yld <= 0:
        return 'X'
    pe_fair = (pe_high + pe_low) / 2
    pe_above = (pe_high + pe_fair) / 2
    pe_below = (pe_fair + pe_low) / 2
    pe_cols = [(-9999, pe_low), (pe_low, pe_below), (pe_below, pe_fair),
               (pe_fair, pe_above), (pe_above, pe_high), (pe_high, 9999)]
    y_rows = [(yld_max, 9999), (yld_high, yld_max), (yld_floor, yld_high), (-9999, yld_floor)]
    grades = [
        ['AA', 'A2', 'B2A', '觀察', '臨界點', 'X'],
        ['A1', 'A', 'B2', '臨界點', 'X', 'X'],
        ['B1A', 'B1', '臨界點', 'X', 'X', 'X'],
        ['觀察', '臨界點', 'X', 'X', 'X', 'X'],
    ]
    col = next((i for i, (lo, hi) in enumerate(pe_cols) if pe >= lo and pe < hi), -1)
    row = next((i for i, (lo, hi) in enumerate(y_rows) if yld >= lo and yld < hi), -1)
    return grades[row][col] if col >= 0 and row >= 0 else 'X'


# ── 衍生欄位計算（統一由後端算完存 DB）───────────────────────────
DERIVED_COLS = [
    'shen_eps','shen_div','shen_pe','shen_yld','shen_grade',
    'weighted_eps','weighted_div','weighted_pe','weighted_yld','weighted_grade','weighted_payout',
    'blend_eps','blend_div','blend_pe','blend_yld','blend_grade',
    'eps_4q_sum','trailing_div','trailing_pe','trailing_yld','trailing_grade',
    'contract_chg',
    'payout_1','payout_2','payout_3','payout_4','payout_5','payout_6',
    'val_aa','val_a1','val_a2','val_a','val_lt6','val_eps_used','val_div_used','val_source',
    'val_pe','val_yld',
    'est_eps','est_div','est_pe','est_yld','est_grade',
    'sys_pe','sys_yld','sys_grade',
    'gb_roic','gb_ey','gb_roic_rank','gb_ey_rank','gb_total_rank'
]

def _calc_derived_fields(r, global_settings=None, user_params=None):
    """
    根據 row dict 裡已有的基礎欄位，計算所有衍生欄位並寫回 row。
    僅由 recalc_all_derived() 呼叫，計算結果存入 DB。
    API 端直接從 DB SELECT 讀取，不呼叫此函式。
    """
    if global_settings is None:
        global_settings = _get_global_settings()

    close = r.get('close')

    # ── 個股PE/殖利率參數 ──
    gs = global_settings
    pe_hi, pe_lo, y_high, y_max = gs['pe_high'], gs['pe_low'], gs['yld_high'], gs['yld_max']
    y_floor = gs.get('yld_floor', 5.0)
    if user_params:
        if user_params.get('peHigh'): pe_hi = float(user_params['peHigh'])
        if user_params.get('peLow'): pe_lo = float(user_params['peLow'])
        if user_params.get('yldHigh'): y_high = float(user_params['yldHigh'])
        if user_params.get('yldMax'): y_max = float(user_params['yldMax'])

    # ── 沈董 PE / 殖利率（即時算，等級讀 DB）──
    shen_eps = r.get('shen_eps')
    shen_div = r.get('shen_div')
    r['shen_pe'] = round(close / shen_eps, 2) if close and shen_eps and shen_eps > 0 else None
    r['shen_yld'] = round(shen_div / close * 100, 2) if close and close > 0 and shen_div and shen_div > 0 else None
    r['shen_grade'] = _calc_matrix_grade(r['shen_pe'], r['shen_yld'], pe_hi, pe_lo, y_max, y_high, y_floor) if r['shen_pe'] and r['shen_yld'] else 'X'

    # ── 加權 EPS ──
    ws = gs.get('div_weights', DEFAULT_WEIGHTS)
    weps_sum = weps_w = 0
    for i in range(1, 6):
        v = r.get(f'eps_y{i}')
        w = ws[i - 1] if i - 1 < len(ws) else 0
        if v is not None and w > 0:
            weps_sum += v * w / 100
            weps_w += w
    r['weighted_eps'] = round(weps_sum * 100) / 100 if weps_w > 0 else None
    # weighted_div 和 weighted_payout 已在 _calc_shen_fields 算好

    # ── 加權 PE / 殖利率 / 等級 ──
    weps = r.get('weighted_eps')
    wdiv = r.get('weighted_div')
    r['weighted_pe'] = round(close / weps, 2) if close and weps and weps > 0 else None
    r['weighted_yld'] = round(wdiv / close * 100, 2) if close and close > 0 and wdiv and wdiv > 0 else None
    r['weighted_grade'] = _calc_matrix_grade(r['weighted_pe'], r['weighted_yld'], pe_hi, pe_lo, y_max, y_high, y_floor) if r['weighted_pe'] and r['weighted_yld'] else 'X'

    # ── 綜合 EPS / PE / 殖利率 / 等級 ──
    br = gs.get('blend_ratio', {'shen': 50, 'wt': 50})
    bS = (br.get('shen') or 50)
    bW = (br.get('wt') or 50)
    total = bS + bW
    nS = bS / total if total > 0 else 0.5
    nW = bW / total if total > 0 else 0.5
    if shen_eps is not None and weps is not None:
        r['blend_eps'] = round(shen_eps * nS + weps * nW, 2)
    elif shen_eps is not None:
        r['blend_eps'] = round(shen_eps, 2)
    elif weps is not None:
        r['blend_eps'] = round(weps, 2)
    else:
        r['blend_eps'] = None
    # blend_div 已在 _calc_shen_fields 算好
    beps = r.get('blend_eps')
    bdiv = r.get('blend_div')
    r['blend_pe'] = round(close / beps, 2) if close and beps and beps > 0 else None
    r['blend_yld'] = round(bdiv / close * 100, 2) if close and close > 0 and bdiv and bdiv > 0 else None
    r['blend_grade'] = _calc_matrix_grade(r['blend_pe'], r['blend_yld'], pe_hi, pe_lo, y_max, y_high, y_floor) if r['blend_pe'] and r['blend_yld'] else 'X'

    # ── 近四季 EPS / 股利 / PE / 殖利率 / 等級 ──
    eps4 = [r.get(f'eps_{i}') for i in range(1, 5)]
    if all(v is not None for v in eps4):
        r['eps_4q_sum'] = round(sum(eps4), 2)
    else:
        r['eps_4q_sum'] = None

    # trailing_div：跟前端邏輯一致
    r['trailing_div'] = None
    wp = r.get('weighted_payout')
    e4 = r.get('eps_4q_sum')
    is_fallback = r.get('_shen_is_fallback', False)
    if is_fallback:
        # fallback 模式：找對應年度實際股利
        lbl = r.get('eps_y1_label')
        if lbl:
            for i in range(1, 7):
                dl = r.get(f'div_{i}_label')
                if dl and str(dl) == str(lbl):
                    dc = r.get(f'div_c{i}') or 0
                    ds = r.get(f'div_s{i}') or 0
                    if dc + ds > 0:
                        r['trailing_div'] = round(dc + ds, 2)
                    break
        if r['trailing_div'] is None and e4 and e4 > 0 and wp:
            r['trailing_div'] = round(e4 * wp) / 100
    else:
        if e4 and e4 > 0 and wp:
            r['trailing_div'] = round(e4 * wp) / 100

    r['trailing_pe'] = round(close / e4, 2) if close and e4 and e4 > 0 else None
    tdiv = r.get('trailing_div')
    r['trailing_yld'] = round(tdiv / close * 100, 2) if close and close > 0 and tdiv and tdiv > 0 else None
    r['trailing_grade'] = _calc_matrix_grade(r['trailing_pe'], r['trailing_yld'], pe_hi, pe_lo, y_max, y_high, y_floor) if r['trailing_pe'] and r['trailing_yld'] else 'X'

    # ── 合約負債變動率 ──
    c1 = r.get('contract_1')
    c2 = r.get('contract_2')
    if c1 is not None and c2 is not None and c2 != 0:
        r['contract_chg'] = round((c1 - c2) / abs(c2) * 100, 2)
    else:
        r['contract_chg'] = None

    # ── 歷年配息率 payout_1~6 ──
    # 建立年度 EPS 查找表
    _eps_map = {}
    for i in range(1, 7):
        _lbl = r.get(f'eps_y{i}_label')
        _val = r.get(f'eps_y{i}')
        if _lbl and _val is not None:
            _eps_map[_lbl] = _val
    for i in range(1, 7):
        _lbl = r.get(f'div_{i}_label')
        _cash = r.get(f'div_c{i}') or 0
        _stock = r.get(f'div_s{i}') or 0
        _total_d = _cash + _stock
        if _total_d > 0:
            _ep = _eps_map.get(_lbl)
            if _ep is not None and _ep > 0:
                r[f'payout_{i}'] = min(100, round(_total_d / _ep * 100, 2))
            else:
                r[f'payout_{i}'] = 100  # EPS <= 0 但有配息 → 100%
        else:
            r[f'payout_{i}'] = None

    # ── 評價門檻（統一計算，存 DB）──
    # EPS/股利取用順序：使用者手動設定 > min(沈董EPS, 綜合EPS)，股利跟隨EPS來源
    # 沈董EPS已改用本業推估法，blend含50%沈董，min()提供極端季節性保護同時保留50%成長
    est_eps = None
    est_div = None
    if user_params:
        # 手動設定的 vmEps（有 _vmManual flag）優先
        if user_params.get('_vmManual'):
            est_eps = user_params.get('vmEps')
            est_div = user_params.get('vmDiv')
        # 再看舊格式的 eps/div key
        if not est_eps:
            est_eps = user_params.get('eps')
        if not est_div:
            est_div = user_params.get('div')
        if est_eps: est_eps = float(est_eps)
        if est_div: est_div = float(est_div)

    val_eps = None
    val_div = None
    if est_eps and est_eps > 0:
        val_eps = est_eps
        val_div = est_div
    else:
        # min(沈董EPS, 綜合EPS)：沈董已改用本業推估法，綜合含50%沈董+50%加權
        # min 在成長股仍保留50%成長（透過blend），在極端季節性股票提供安全帽
        _blend_eps = r.get('blend_eps')
        _shen_eps = r.get('shen_eps')
        _blend_pos = _blend_eps if _blend_eps and _blend_eps > 0 else None
        _shen_pos = _shen_eps if _shen_eps and _shen_eps > 0 else None
        if _blend_pos is not None and _shen_pos is not None:
            if _blend_pos <= _shen_pos:
                val_eps = _blend_pos
                val_div = r.get('blend_div')
            else:
                val_eps = _shen_pos
                val_div = r.get('shen_div')
        elif _shen_pos is not None:
            val_eps = _shen_pos
            val_div = r.get('shen_div')
        elif _blend_pos is not None:
            val_eps = _blend_pos
            val_div = r.get('blend_div')
    val_bdiv = r.get('blend_div')
    r['val_eps_used'] = val_eps
    r['val_div_used'] = val_div
    r['val_pe'] = round(close / val_eps, 2) if close and val_eps and val_eps > 0 else None
    r['val_yld'] = round(val_div / close * 100, 2) if close and close > 0 and val_div and val_div > 0 else None
    # 標記來源
    if est_eps and est_eps > 0:
        r['val_source'] = '預估'
    elif val_eps == r.get('blend_eps'):
        r['val_source'] = '綜合'
    elif val_eps == r.get('shen_eps'):
        r['val_source'] = '沈董'
    else:
        r['val_source'] = None

    pe_mid_v = (pe_hi + pe_lo) / 2
    pe_lo_bias_v = (pe_mid_v + pe_lo) / 2

    def _calc_val_threshold(pe_val, yld_val):
        if val_eps is None or val_eps <= 0 or val_div is None or val_div <= 0:
            return None
        v1 = val_eps * pe_val
        v2 = val_div / (yld_val / 100)
        candidates = [v1, v2]
        lt_yld_r = gs.get('lt_yld', 6) / 100
        if val_bdiv and val_bdiv > 0 and lt_yld_r > 0:
            candidates.append(val_bdiv / lt_yld_r + val_div)
        return round(min(candidates), 2)

    r['val_aa'] = _calc_val_threshold(pe_lo, y_max)
    r['val_a1'] = _calc_val_threshold(pe_lo, y_high)
    r['val_a2'] = _calc_val_threshold(pe_lo_bias_v, y_max)
    r['val_a']  = _calc_val_threshold(pe_lo_bias_v, y_high)
    _lt_yld_r = gs.get('lt_yld', 6) / 100
    r['val_lt6'] = round(val_bdiv / _lt_yld_r, 2) if val_bdiv and val_bdiv > 0 and _lt_yld_r > 0 else None

    # ── 預估 EPS/股利/PE/殖利率/等級 ──
    import json as _json_est
    _est_eps = None
    _est_div = None
    if user_params:
        # vmEps 優先 > q1~q4 加總
        if user_params.get('vmEps') and float(user_params.get('vmEps', 0) or 0):
            _est_eps = round(float(user_params['vmEps']), 2)
        else:
            qs = [user_params.get(f'q{i}') for i in range(1, 5)]
            qs_vals = [float(v) for v in qs if v]
            if qs_vals:
                _est_eps = round(sum(qs_vals), 2)
        # vmDiv 優先 > div
        if user_params.get('vmDiv') and float(user_params.get('vmDiv', 0) or 0):
            _est_div = round(float(user_params['vmDiv']), 2)
        elif user_params.get('div'):
            _est_div = round(float(user_params['div']), 2)
    r['est_eps'] = _est_eps
    r['est_div'] = _est_div
    r['est_pe'] = round(close / _est_eps, 2) if _est_eps and _est_eps > 0 and close else None
    r['est_yld'] = round(_est_div / close * 100, 2) if _est_div and _est_div > 0 and close and close > 0 else None
    if _est_eps is not None and _est_eps <= 0:
        r['est_grade'] = 'X'
    elif r['est_pe'] and r['est_yld']:
        r['est_grade'] = _calc_matrix_grade(r['est_pe'], r['est_yld'], pe_hi, pe_lo, y_max, y_high, y_floor)
    else:
        r['est_grade'] = None

    # ── 系統估算等級 ──
    _sys_eps = r.get('sys_ann_eps')
    _sys_div = r.get('sys_ann_div')
    _sys_pe = round(close / _sys_eps, 2) if _sys_eps and _sys_eps > 0 and close else None
    _sys_yld = round(_sys_div / close * 100, 2) if _sys_div and _sys_div > 0 and close and close > 0 else None
    r['sys_pe'] = _sys_pe
    r['sys_yld'] = _sys_yld
    if _sys_eps is not None and _sys_eps <= 0:
        r['sys_grade'] = 'X'
    elif _sys_pe and _sys_yld:
        r['sys_grade'] = _calc_matrix_grade(_sys_pe, _sys_yld, pe_hi, pe_lo, y_max, y_high, y_floor)
    else:
        r['sys_grade'] = None


def _save_derived_to_db(code, r):
    """將衍生欄位寫回 stocks 表"""
    sets = ', '.join(f'{col}=?' for col in DERIVED_COLS)
    vals = [r.get(col) for col in DERIVED_COLS] + [code]
    conn = sqlite3.connect(DB_PATH)
    conn.execute(f"UPDATE stocks SET {sets} WHERE code=?", vals)
    conn.commit()
    conn.close()


def recalc_all_derived(codes=None):
    """批次重算所有（或指定）股票的衍生欄位並存DB。
    供排程、API、權重變更時呼叫。
    """
    import json as _json
    gs = _get_global_settings()
    cur_roc = __import__('datetime').date.today().year - 1911

    where = ""
    params = []
    if codes:
        placeholders = ','.join('?' * len(codes))
        where = f" WHERE code IN ({placeholders})"
        params = list(codes)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(f"""SELECT code, name, close, industry,
        eps_1, eps_1q, eps_2, eps_2q, eps_3, eps_3q, eps_4, eps_4q, eps_5, eps_5q,
        eps_y1, eps_y1_label, eps_y2, eps_y2_label, eps_y3, eps_y3_label,
        eps_y4, eps_y4_label, eps_y5, eps_y5_label, eps_y6, eps_y6_label,
        div_c1, div_s1, div_1_label, div_c2, div_s2, div_2_label,
        div_c3, div_s3, div_3_label, div_c4, div_s4, div_4_label,
        div_c5, div_s5, div_5_label, div_c6, div_s6, div_6_label,
        contract_1, contract_2,
        sys_ann_eps, sys_ann_div
    FROM stocks{where}""", params).fetchall()

    # 讀取 user_estimates
    ue_map = {}
    try:
        ue_rows = conn.execute("SELECT code, params FROM user_estimates WHERE params IS NOT NULL").fetchall()
        for ue in ue_rows:
            if ue['params']:
                ue_map[ue['code']] = _json.loads(ue['params'])
    except Exception:
        pass

    # 讀取季度資料（沈董EPS本業推估用）
    qf_map = {}  # code → [{'quarter','eps','eps_core','eps_nonop','oi'}, ...]
    try:
        qf_rows = conn.execute(
            "SELECT code, quarter, eps, eps_core, eps_nonop, operating_income "
            "FROM quarterly_financial WHERE eps IS NOT NULL"
        ).fetchall()
        for qf in qf_rows:
            c = qf['code']
            if c not in qf_map:
                qf_map[c] = []
            qf_map[c].append({
                'quarter': qf['quarter'], 'eps': qf['eps'],
                'eps_core': qf['eps_core'], 'eps_nonop': qf['eps_nonop'],
                'oi': qf['operating_income']
            })
    except Exception:
        pass

    # 讀取最新年度 financial_annual（葛林布萊盈餘殖利率用）
    fa_map = {}
    try:
        fa_rows = conn.execute("""
            SELECT f.code, f.operating_income, f.common_stock,
                   f.cash_and_equivalents, f.short_term_debt, f.short_term_notes,
                   f.current_long_term_debt, f.long_term_bank_debt, f.bonds_payable,
                   f.roic
            FROM financial_annual f
            INNER JOIN (SELECT code, MAX(year) as max_year FROM financial_annual
                        WHERE operating_income IS NOT NULL GROUP BY code) latest
            ON f.code = latest.code AND f.year = latest.max_year
        """).fetchall()
        for fa in fa_rows:
            fa_map[fa['code']] = dict(fa)
    except Exception:
        pass

    updated = 0
    all_results = []  # 收集所有結果，排名用
    sets_sql = ', '.join(f'{col}=?' for col in DERIVED_COLS)
    for row in rows:
        r = dict(row)
        _calc_shen_fields(r, cur_roc, gs, qf_data=qf_map.get(r['code']))
        up = ue_map.get(r['code'])
        _calc_derived_fields(r, gs, up)

        # 葛林布萊：計算 ROIC 和盈餘殖利率
        fa = fa_map.get(r['code'])
        is_financial = r.get('industry') in ('金融保險業', '金融業')
        is_ky = r['code'].endswith('-KY') or (r.get('name') or '').endswith('-KY')
        if fa and not is_financial and not is_ky and r.get('close'):
            # ROIC 直接取 financial_annual 已算好的值
            r['gb_roic'] = round(fa['roic'], 2) if fa.get('roic') else None
            # 盈餘殖利率 = operating_income / EV，同時算市值供門檻過濾
            oi = fa.get('operating_income')
            cs = fa.get('common_stock')
            if oi and cs and cs > 0:
                shares = cs / 10  # 股本(元) / 面額10 = 股數
                market_cap = r['close'] * shares
                r['_gb_market_cap'] = market_cap  # 暫存，排名過濾用
                cash = fa.get('cash_and_equivalents') or 0
                debt = (fa.get('short_term_debt') or 0) + (fa.get('short_term_notes') or 0) + \
                       (fa.get('current_long_term_debt') or 0) + (fa.get('long_term_bank_debt') or 0) + \
                       (fa.get('bonds_payable') or 0)
                ev = market_cap + debt - cash
                if ev > 0:
                    r['gb_ey'] = round(oi / ev * 100, 2)
                else:
                    r['gb_ey'] = None
            else:
                r['gb_ey'] = None
                r['_gb_market_cap'] = None
        else:
            r['gb_roic'] = None
            r['gb_ey'] = None
            r['_gb_market_cap'] = None
        r['gb_roic_rank'] = None
        r['gb_ey_rank'] = None
        r['gb_total_rank'] = None
        all_results.append(r)

    # 葛林布萊排名（排除金融股、KY股、市值<30億）
    GB_MIN_MARKET_CAP = 3_000_000_000  # 30億
    rankable = [r for r in all_results
                if r.get('gb_roic') is not None and r.get('gb_ey') is not None
                and r.get('_gb_market_cap') and r['_gb_market_cap'] >= GB_MIN_MARKET_CAP]
    rankable.sort(key=lambda x: x['gb_roic'], reverse=True)
    for i, r in enumerate(rankable):
        r['gb_roic_rank'] = i + 1
    rankable.sort(key=lambda x: x['gb_ey'], reverse=True)
    for i, r in enumerate(rankable):
        r['gb_ey_rank'] = i + 1
    for r in rankable:
        r['gb_total_rank'] = r['gb_roic_rank'] + r['gb_ey_rank']

    # 寫入 DB
    for r in all_results:
        vals = [r.get(col) for col in DERIVED_COLS] + [r['code']]
        conn.execute(f"UPDATE stocks SET {sets_sql} WHERE code=?", vals)
        updated += 1

    conn.commit()
    conn.close()
    logger.info(f"[Derived] 重算完成：{updated} 支股票")
    return updated


# ── 檢核表計算 ─────────────────────────────────────────────

# 檢核項目定義（順序即顯示順序，插入/調序只改這裡）
CHECKLIST_ITEMS = [
    # ── A 獲利性檢核（10項）──
    {'key': 'roic_avg5',   'category': 'profit', 'label': 'ROIC 近5年平均', 'threshold': '≥ 15%', 'weight': '核心', 'hint': '衡量公司長期投入資本的回報效率，排除財務槓桿影響'},
    {'key': 'roic_latest', 'category': 'profit', 'label': 'ROIC 最近一年', 'threshold': '≥ 15%', 'weight': '核心', 'hint': '確認目前獲利能力仍維持在高水準'},
    {'key': 'roic_trend',  'category': 'profit', 'label': 'ROIC 趨勢：最近一年 ≥ 近3年平均', 'threshold': '是', 'weight': '重要', 'hint': '確認獲利效率沒有走下坡'},
    {'key': 'roic_min5',   'category': 'profit', 'label': 'ROIC 近5年最低值', 'threshold': '≥ 10%', 'weight': '重要', 'hint': '即使在最差年度仍有基本獲利能力，代表護城河穩固'},
    {'key': 'opm_avg5',    'category': 'profit', 'label': '營益率近5年平均', 'threshold': '≥ 10%', 'weight': '核心', 'hint': '本業獲利能力，排除業外收支干擾'},
    {'key': 'opm_trend',   'category': 'profit', 'label': '營益率趨勢：最近一年 ≥ 近3年平均', 'threshold': '是', 'weight': '重要', 'hint': '確認本業獲利沒有衰退'},
    {'key': 'opm_min5',    'category': 'profit', 'label': '營益率近5年最低值', 'threshold': '≥ 5%', 'weight': '輔助', 'hint': '景氣谷底仍能維持正營益率，不至於虧損'},
    {'key': 'gm_trend',    'category': 'profit', 'label': '毛利率趨勢：最近一年 ≥ 近3年平均', 'threshold': '是', 'weight': '輔助', 'hint': '毛利率上升代表產品競爭力或定價權改善'},
    {'key': 'gm_median',   'category': 'profit', 'label': '毛利率位置：最近一年 ≥ 近5年中位數', 'threshold': '是', 'weight': '重要', 'hint': '確認毛利率在歷史水準之上，未被壓縮'},
    {'key': 'gm_q_trend',  'category': 'profit', 'label': '毛利率季趨勢：近4季平均 ≥ 近12季平均', 'threshold': '是', 'weight': '輔助', 'hint': '用季度資料捕捉更即時的毛利率變化方向'},
    # ── B 安全性檢核（15項）──
    {'key': 'debt_ratio_ok',  'category': 'safety', 'label': '負債比', 'threshold': '≤ 50%', 'weight': '核心', 'hint': '負債比過高代表財務風險大，景氣反轉時容易出問題'},
    {'key': 'fin_debt_ok',    'category': 'safety', 'label': '長短期金融負債比', 'threshold': '< 30%', 'weight': '核心', 'hint': '金融負債（銀行借款）佔比過高代表依賴借貸經營'},
    {'key': 'current_ratio',  'category': 'safety', 'label': '流動比率', 'threshold': '≥ 150%', 'weight': '重要', 'hint': '短期償債能力，流動資產能否覆蓋流動負債'},
    {'key': 'quick_ratio',    'category': 'safety', 'label': '速動比率', 'threshold': '≥ 100%', 'weight': '重要', 'hint': '扣除存貨後的短期償債能力，比流動比率更嚴格'},
    {'key': 'icr_ok',         'category': 'safety', 'label': '利息保障倍數', 'threshold': '> 5', 'weight': '重要', 'hint': '營業利益能否輕鬆支付利息費用'},
    {'key': 'icr_min5',       'category': 'safety', 'label': '利息保障倍數近5年最低值', 'threshold': '> 3', 'weight': '重要', 'hint': '即使在最差年度也不至於付不出利息'},
    {'key': 'fcf_5y_pos',     'category': 'safety', 'label': '自由現金流連續5年為正', 'threshold': '是', 'weight': '核心', 'hint': '公司能持續產生現金，不需靠借貸或增資維持營運'},
    {'key': 'fcf_latest_pos', 'category': 'safety', 'label': '最近一年自由現金流 > 0', 'threshold': '是', 'weight': '重要', 'hint': '確認目前仍有正現金流，非靠吃老本'},
    {'key': 'eq_ok',          'category': 'safety', 'label': '盈餘品質率', 'threshold': '≥ 70%', 'weight': '重要', 'hint': '營業現金流 / 稅後淨利，確認獲利有實際現金支撐而非紙上富貴'},
    {'key': 'eq_min5',        'category': 'safety', 'label': '盈餘品質率近5年最低值', 'threshold': '> 60%', 'weight': '重要', 'hint': '長期盈餘品質穩定，非一次性灌水'},
    {'key': 'inv_days_avg',   'category': 'safety', 'label': '存貨週轉天數 ≤ 近5年平均', 'threshold': '是', 'weight': '重要', 'hint': '存貨消化速度正常，沒有庫存堆積風險'},
    {'key': 'inv_days_high',  'category': 'safety', 'label': '存貨週轉天數未創5年新高', 'threshold': '是', 'weight': '輔助', 'hint': '存貨天數創新高可能代表產品滯銷或需求下滑'},
    {'key': 'qinv_4v20',      'category': 'safety', 'label': '近4季存貨週轉天數 < 近20季平均', 'threshold': '是', 'weight': '輔助', 'hint': '用季度資料捕捉更即時的存貨變化趨勢'},
    {'key': 'ar_days_avg',    'category': 'safety', 'label': '應收帳款週轉天數 ≤ 近5年平均', 'threshold': '是', 'weight': '重要', 'hint': '收款速度正常，沒有客戶賴帳風險'},
    {'key': 'ar_days_high',   'category': 'safety', 'label': '應收帳款週轉天數未創5年新高', 'threshold': '是', 'weight': '輔助', 'hint': '應收天數創新高可能代表客戶還款能力變差'},
    # ── C 價值評估檢核（13項）──
    {'key': 'grade_a_ok',     'category': 'value', 'label': '預估(沈董)等級為A級以上', 'threshold': '是', 'weight': '核心', 'group': '沈董法', 'hint': '矩陣等級A以上代表PE和殖利率都在合理範圍'},
    {'key': 'blend_grade_ok', 'category': 'value', 'label': '綜合等級為A級以上', 'threshold': '是', 'weight': '核心', 'group': '沈董法', 'hint': '綜合EPS加權後的矩陣等級，A以上代表整體評價合理'},
    {'key': 'eps_vs_multi',   'category': 'value', 'label': '預估(沈董)EPS ≥ 近5年/近3年/十年均EPS 中至少2個', 'threshold': '是', 'weight': '重要', 'group': '沈董法', 'hint': '確認EPS不是異常偏低，估值基礎可靠'},
    {'key': 'eps_vs_10y',     'category': 'value', 'label': '預估(沈董)EPS / 十年平均EPS', 'threshold': '≥ 1', 'weight': '重要', 'group': '沈董法', 'hint': '長期視角確認EPS水準，排除短期高低波動'},
    {'key': 'core_ratio',     'category': 'value', 'label': '累計營業利益 / 累計稅前淨利', 'threshold': '> 80%', 'weight': '重要', 'group': '沈董法', 'hint': '獲利主要來自本業，非靠業外收入撐場'},
    {'key': 'price_val_ok',   'category': 'value', 'label': '現價 ≤ A級評價；≤ AA更佳', 'threshold': '是', 'weight': '重要', 'group': '沈董法', 'hint': '股價低於評價門檻，有安全邊際'},
    {'key': 'eps_5y_pos',     'category': 'value', 'label': '近5年EPS逐年皆 > 0', 'threshold': '是', 'weight': '核心', 'group': 'EPS 品質', 'hint': '穩定獲利是估值的前提，有虧損年度代表風險高'},
    {'key': 'eps_5y_stable',  'category': 'value', 'label': '近5年最高EPS / 最低EPS', 'threshold': '< 3', 'weight': '重要', 'group': 'EPS 品質', 'hint': 'EPS波動太大代表獲利不穩定，估值可靠性低'},
    {'key': 'wt_yld_ok',      'category': 'value', 'label': '綜合殖利率', 'threshold': '≥ 5%', 'weight': '核心', 'group': '殖利率法', 'hint': '股利報酬率夠高，提供持有期間的現金回報'},
    {'key': 'wt_payout_ok',   'category': 'value', 'label': '加權配息率', 'threshold': '40%~80%', 'weight': '重要', 'group': '殖利率法', 'hint': '配息率太低代表股利少，太高代表可能超發不可持續'},
    {'key': 'val_ddm_return', 'category': 'value', 'label': '股利折現現價潛在年報酬', 'threshold': '≥ 10%', 'weight': '重要', 'group': 'DDM', 'hint': '以股利折現模型估算，現價買入的預期年化報酬'},
    {'key': 'dcf_safe_ok',    'category': 'value', 'label': '現價 ≤ DCF安全邊際價', 'threshold': '是', 'weight': '重要', 'group': 'DCF', 'hint': '自由現金流折現後，現價低於內在價值打折後的安全價'},
    {'key': 'ge_neff_ratio',  'category': 'value', 'label': '聶夫 Neff 比率', 'threshold': '≥ 0.7', 'weight': '輔助', 'group': '林區／聶夫法', 'hint': '(EPS成長率+殖利率)/PE，越高代表成長性相對股價越被低估'},
    {'key': 'ge_lynch_peg',   'category': 'value', 'label': '林區 PEG', 'threshold': '≤ 1.0', 'weight': '輔助', 'group': '林區／聶夫法', 'hint': 'PE/EPS成長率，越低代表股價相對成長越便宜'},
    # ── D 成長性檢核（6項）──
    {'key': 'cum_rev_pos',    'category': 'growth_eval', 'label': '累積營收年增率', 'threshold': '≥ 0%', 'weight': '重要', 'hint': '今年以來累積營收是否成長，反映整體趨勢'},
    {'key': 'rev_12m_pos',    'category': 'growth_eval', 'label': '長期12M營收年增率', 'threshold': '≥ 0%', 'weight': '重要', 'hint': '近12個月累計營收年增率，過濾短期波動看長期趨勢'},
    {'key': 'rev_3m_pos',     'category': 'growth_eval', 'label': '短期3M營收年增率', 'threshold': '≥ 0%', 'weight': '重要', 'hint': '近3個月累計營收年增率，捕捉最近的營收動能'},
    {'key': 'rev_both_pos',   'category': 'growth_eval', 'label': '短期3M ≥ 0% 且 長期12M ≥ 0%（一致向上）', 'threshold': '是', 'weight': '輔助', 'hint': '短期和長期營收同時正成長，趨勢一致性高'},
    {'key': 'rev_3m_gt_12m',  'category': 'growth_eval', 'label': '短期3M ≥ 長期12M', 'threshold': '是', 'weight': '重要', 'hint': '短期成長加速，營收動能正在增強而非減弱'},
    {'key': 'growth_green',   'category': 'growth_eval', 'label': '趨勢燈號為多頭', 'threshold': 'green', 'weight': '輔助', 'hint': '綜合營收和EPS趨勢的多空判斷'},
]
CHECKLIST_PROFIT_KEYS = [item['key'] for item in CHECKLIST_ITEMS if item['category'] == 'profit']
CHECKLIST_SAFETY_KEYS = [item['key'] for item in CHECKLIST_ITEMS if item['category'] == 'safety']
CHECKLIST_VALUE_KEYS = [item['key'] for item in CHECKLIST_ITEMS if item['category'] == 'value']
CHECKLIST_GROWTH_EVAL_KEYS = [item['key'] for item in CHECKLIST_ITEMS if item['category'] == 'growth_eval']
CHECKLIST_ALL_KEYS = [item['key'] for item in CHECKLIST_ITEMS]

def _init_checklist_db():
    """建立 stock_checklist 資料表"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS stock_checklist (
        code TEXT PRIMARY KEY,
        pass_count INTEGER, total_count INTEGER,
        base_count INTEGER, bonus_count INTEGER,
        detail TEXT,
        eps_setting REAL, div_setting REAL,
        yld_high REAL, yld_max REAL, pe_high REAL, pe_low REAL,
        lt_div REAL, lt_yld REAL,
        val_a REAL, val_a1 REAL, val_a2 REAL, val_aa REAL,
        lt5 REAL, lt6 REAL, lt7 REAL,
        updated_at TEXT
    )""")
    # 動態加 chk_{key} 欄位（名稱制）
    add_cols = [(f'chk_{item["key"]}', 'INTEGER') for item in CHECKLIST_ITEMS]
    # 舊欄位相容（保留但不再寫入）
    add_cols += [('chk_1','INTEGER'),('chk_2','INTEGER'),('chk_3','INTEGER'),
                 ('chk_4','INTEGER'),('chk_5','INTEGER'),('chk_6','INTEGER'),
                 ('chk_7','INTEGER'),('chk_8','INTEGER'),('chk_9','INTEGER'),
                 ('chk_10','INTEGER'),('chk_11','INTEGER'),('chk_12','INTEGER'),('chk_13','INTEGER')]
    # 其他欄位
    add_cols += [('eps_setting','REAL'),('div_setting','REAL'),
                 ('yld_high','REAL'),('yld_max','REAL'),('pe_high','REAL'),('pe_low','REAL'),
                 ('lt_div','REAL'),('lt_yld','REAL'),
                 ('val_a','REAL'),('val_a1','REAL'),('val_a2','REAL'),('val_aa','REAL'),
                 ('lt5','REAL'),('lt6','REAL'),('lt7','REAL'),
                 ('profit_count','INTEGER'),('safety_count','INTEGER'),('value_count','INTEGER'),('growth_eval_count','INTEGER'),('base_count','INTEGER'),('bonus_count','INTEGER'),
                 ('gi_neff_a','REAL'),('gi_neff_b','REAL'),
                 ('gi_neff_3a','REAL'),('gi_neff_3b','REAL'),
                 ('gi_neff_c','REAL'),('gi_neff_d','REAL'),
                 ('gi_intrinsic_growth','REAL'),
                 ('gi_lynch_a','REAL'),('gi_lynch_b','REAL'),
                 ('gi_lynch_c','REAL'),('gi_lynch_d','REAL'),
                 ('gi_rev_cagr_3y','REAL'),('gi_rev_cagr_5y','REAL'),('gi_shares_change','REAL'),
                 ('gi_yield','REAL'),('gi_pe','REAL'),
                 ('gi_gray','INTEGER'),('gi_neff_gray','INTEGER'),('gi_lynch_gray','INTEGER'),
                 ('gi_warnings','TEXT'),
                 ('gi_shiller_avg_eps','REAL'),('gi_shiller_pe','REAL'),('gi_shiller_alert','REAL'),
                 ('gi_roic_avg','REAL'),('gi_roe_avg','REAL'),('gi_opm_avg','REAL'),('gi_fcf_rev_avg','REAL'),
                 ('growth_signal','TEXT'),('growth_rev_momentum','REAL'),
                 ('growth_eps_trend','REAL'),('growth_inv_risk','INTEGER'),
                 ('growth_detail','TEXT'),
                 ('gi_rev_3m_yoy','REAL'),('gi_rev_12m_yoy','REAL'),
                 ('borderline','TEXT'),('red_flags','TEXT')]
    for col, typ in add_cols:
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
    return base in ('AA','A1','A2','A','B1A','B2A','B1','B2')

def _is_grade_a_level(g):
    """財務等級是否 A級（AA/A1/A2/B1A/B2A），即值得深入研究的等級"""
    if not g or g in ('-', 'X'):
        return False
    base = g.replace('+', '').replace('-', '')
    return base in ('AA', 'A1', 'A2', 'B1A', 'B2A')

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
        ['AA','A2','B2A','觀察','臨界點','X'],
        ['A1','A','B2','臨界點','X','X'],
        ['B1A','B1','臨界點','X','X','X'],
        ['觀察','臨界點','X','X','X','X'],
    ]
    col = next((i for i, c in enumerate(pe_cols) if pe >= c[0] and pe < c[1]), -1)
    row = next((i for i, y in enumerate(y_rows) if yld >= y[0] and yld < y[1]), -1)
    if col >= 0 and row >= 0:
        return grades[row][col]
    return None

def _calc_checklist_for_stock(r, user_params=None, global_settings=None, growth_map=None):
    """計算單支股票的檢核表（名稱制），r 為 stocks 表的 row dict（含所有衍生欄位）
    所有 EPS/PE/殖利率/等級/評價門檻均直接讀取 stocks 表已存值，不重算。
    """
    import json
    checks = {}
    detail = {}

    # 讀取 PE/殖利率參數（個股覆蓋 > 全域預設，用於 DDM）
    if global_settings is None:
        global_settings = _get_global_settings()
    pe_hi, pe_lo, y_high, y_max = _get_stock_params(user_params, global_settings)

    # ── 直接從 stocks 表讀取已算好的值（recalc_all_derived 統一計算）──
    close = r.get('close')
    shen_eps = r.get('shen_eps')
    shen_div = r.get('shen_div')
    shen_pe = r.get('shen_pe')
    shen_yld = r.get('shen_yld')
    blend_eps = r.get('blend_eps')
    blend_div = r.get('blend_div')
    blend_pe = r.get('blend_pe')
    blend_yld = r.get('blend_yld')
    weighted_eps = r.get('weighted_eps')
    weighted_div = r.get('weighted_div')
    weighted_pe = r.get('weighted_pe')
    weighted_yld = r.get('weighted_yld')
    est_eps = r.get('est_eps')
    est_div = r.get('est_div')
    val_aa = r.get('val_aa')
    val_a1 = r.get('val_a1')
    val_a2 = r.get('val_a2')
    val_a = r.get('val_a')
    val_lt6 = r.get('val_lt6')
    lt_div = blend_div
    _lt_yld_pct = global_settings.get('lt_yld', 6)
    lt_yld = _lt_yld_pct
    _lt_r = _lt_yld_pct / 100
    lt5 = round(lt_div / 0.05, 2) if lt_div and lt_div > 0 else None
    lt6 = round(lt_div / _lt_r, 2) if lt_div and lt_div > 0 and _lt_r > 0 else None
    lt7 = round(lt_div / 0.07, 2) if lt_div and lt_div > 0 else None

    # === 獲利性檢核（12項） ===
    _roic_5y = r.get('_roic_5y') or []  # [(year, roic%), ...] 最近→最遠
    _roic_vals_5 = [v for _, v in _roic_5y if v is not None]
    _roic_vals_3 = [v for _, v in _roic_5y[:3] if v is not None]
    _roic_avg5 = sum(_roic_vals_5) / len(_roic_vals_5) if _roic_vals_5 else None
    _roic_avg3 = sum(_roic_vals_3) / len(_roic_vals_3) if _roic_vals_3 else None
    _roic_min5 = min(_roic_vals_5) if _roic_vals_5 else None
    _roic_latest = _roic_vals_5[0] if _roic_vals_5 else None

    checks['roic_avg5'] = 1 if _roic_avg5 is not None and _roic_avg5 > 15 else 0
    detail['roic_avg5'] = f'5年平均={_roic_avg5:.2f}%' if _roic_avg5 is not None else '無資料'
    checks['roic_latest'] = 1 if _roic_latest is not None and _roic_latest > 15 else 0
    detail['roic_latest'] = f'最近一年={_roic_latest:.2f}%' if _roic_latest is not None else '無資料'
    checks['roic_trend'] = 1 if _roic_latest is not None and _roic_avg3 is not None and _roic_latest >= _roic_avg3 else 0
    detail['roic_trend'] = f'最近一年={_roic_latest:.2f}% vs 近3年平均={_roic_avg3:.2f}%' if _roic_latest is not None and _roic_avg3 is not None else '無資料'
    checks['roic_min5'] = 1 if _roic_min5 is not None and _roic_min5 > 10 else 0
    detail['roic_min5'] = f'5年最低={_roic_min5:.2f}%' if _roic_min5 is not None else '無資料'

    _gm_5y = r.get('_gm_5y') or []  # [(year, gm%), ...]
    _gm_vals_5 = [v for _, v in _gm_5y if v is not None]
    _gm_vals_3 = [v for _, v in _gm_5y[:3] if v is not None]
    _gm_avg5 = sum(_gm_vals_5) / len(_gm_vals_5) if _gm_vals_5 else None
    _gm_avg3 = sum(_gm_vals_3) / len(_gm_vals_3) if _gm_vals_3 else None
    _gm_min5 = min(_gm_vals_5) if _gm_vals_5 else None
    _gm_latest = _gm_vals_5[0] if _gm_vals_5 else None

    checks['gm_trend'] = 1 if _gm_latest is not None and _gm_avg3 is not None and _gm_latest >= _gm_avg3 else 0
    detail['gm_trend'] = f'最近一年={_gm_latest:.2f}% vs 近3年平均={_gm_avg3:.2f}%' if _gm_latest is not None and _gm_avg3 is not None else '無資料'

    # 毛利率位置：最近一年 >= 近5年中位數
    _gm_median5 = None
    if len(_gm_vals_5) >= 5:
        _sorted_gm = sorted(_gm_vals_5)
        _gm_median5 = _sorted_gm[len(_sorted_gm) // 2]
    checks['gm_median'] = 1 if _gm_latest is not None and _gm_median5 is not None and _gm_latest >= _gm_median5 else 0
    detail['gm_median'] = f'最近一年={_gm_latest:.2f}% vs 5年中位數={_gm_median5:.2f}%' if _gm_latest is not None and _gm_median5 is not None else '無資料'

    # 毛利率季趨勢：近4季平均 >= 近12季平均
    _qgm = r.get('_qgm')
    if _qgm and _qgm.get('q_avg4') is not None and _qgm.get('q_avg12') is not None:
        checks['gm_q_trend'] = 1 if _qgm['q_avg4'] >= _qgm['q_avg12'] else 0
        detail['gm_q_trend'] = f'近4季平均={_qgm["q_avg4"]}% vs 近12季平均={_qgm["q_avg12"]}%'
    else:
        checks['gm_q_trend'] = 0
        detail['gm_q_trend'] = '季度毛利率資料不足（需12季以上）'

    _opm_5y = r.get('_opm_5y') or []
    _opm_vals_5 = [v for _, v in _opm_5y if v is not None]
    _opm_vals_3 = [v for _, v in _opm_5y[:3] if v is not None]
    _opm_avg5 = sum(_opm_vals_5) / len(_opm_vals_5) if _opm_vals_5 else None
    _opm_avg3 = sum(_opm_vals_3) / len(_opm_vals_3) if _opm_vals_3 else None
    _opm_min5 = min(_opm_vals_5) if _opm_vals_5 else None
    _opm_latest = _opm_vals_5[0] if _opm_vals_5 else None

    checks['opm_avg5'] = 1 if _opm_avg5 is not None and _opm_avg5 > 10 else 0
    detail['opm_avg5'] = f'5年平均={_opm_avg5:.2f}%' if _opm_avg5 is not None else '無資料'
    checks['opm_trend'] = 1 if _opm_latest is not None and _opm_avg3 is not None and _opm_latest >= _opm_avg3 else 0
    detail['opm_trend'] = f'最近一年={_opm_latest:.2f}% vs 近3年平均={_opm_avg3:.2f}%' if _opm_latest is not None and _opm_avg3 is not None else '無資料'
    checks['opm_min5'] = 1 if _opm_min5 is not None and _opm_min5 > 5 else 0
    detail['opm_min5'] = f'5年最低={_opm_min5:.2f}%' if _opm_min5 is not None else '無資料'

    # === 安全性檢核（12項） ===
    def _5y_vals(key):
        return [v for _, v in (r.get(f'_{key}_5y') or []) if v is not None]

    # 負債比 <60%
    _dr_5y = _5y_vals('debt_ratio')
    _dr_latest = _dr_5y[0] if _dr_5y else None
    checks['debt_ratio_ok'] = 1 if _dr_latest is not None and _dr_latest <= 50 else 0
    detail['debt_ratio_ok'] = f'最近一年={_dr_latest:.2f}%' if _dr_latest is not None else '無資料'

    # 金融負債比 <30%
    _fdr_5y = _5y_vals('fin_debt_ratio')
    _fdr_latest = _fdr_5y[0] if _fdr_5y else None
    checks['fin_debt_ok'] = 1 if _fdr_latest is not None and _fdr_latest < 30 else 0
    detail['fin_debt_ok'] = f'最近一年={_fdr_latest:.2f}%' if _fdr_latest is not None else '無資料'

    # 流動比率 >= 150%
    _cr_5y = _5y_vals('current_ratio')
    _cr_latest = _cr_5y[0] if _cr_5y else None
    checks['current_ratio'] = 1 if _cr_latest is not None and _cr_latest >= 150 else 0
    detail['current_ratio'] = f'最近一年={_cr_latest:.2f}%' if _cr_latest is not None else '無資料'

    # 速動比率 >= 100%
    _qr_5y = _5y_vals('quick_ratio')
    _qr_latest = _qr_5y[0] if _qr_5y else None
    checks['quick_ratio'] = 1 if _qr_latest is not None and _qr_latest >= 100 else 0
    detail['quick_ratio'] = f'最近一年={_qr_latest:.2f}%' if _qr_latest is not None else '無資料'

    # 利息保障倍數 >5
    _icr_5y = _5y_vals('interest_coverage')
    _icr_latest = _icr_5y[0] if _icr_5y else None
    checks['icr_ok'] = 1 if _icr_latest is not None and _icr_latest > 5 else 0
    detail['icr_ok'] = f'最近一年={_icr_latest:.2f}倍' if _icr_latest is not None else '無資料'

    # 利息保障倍數近5年最低值 >3
    _icr_min = min(_icr_5y) if _icr_5y else None
    checks['icr_min5'] = 1 if _icr_min is not None and _icr_min > 3 else 0
    detail['icr_min5'] = f'5年最低={_icr_min:.2f}倍' if _icr_min is not None else '無資料'

    # 自由現金流連續5年為正
    _fcf_5y = _5y_vals('fcf')
    checks['fcf_5y_pos'] = 1 if len(_fcf_5y) >= 5 and all(v > 0 for v in _fcf_5y) else 0
    detail['fcf_5y_pos'] = f'{len(_fcf_5y)}年資料，正值{sum(1 for v in _fcf_5y if v > 0)}年' if _fcf_5y else '無資料'

    # 最近一年自由現金流 >0
    _fcf_latest = _fcf_5y[0] if _fcf_5y else None
    checks['fcf_latest_pos'] = 1 if _fcf_latest is not None and _fcf_latest > 0 else 0
    detail['fcf_latest_pos'] = f'最近一年FCF={_fcf_latest / 1000000:.0f}百萬' if _fcf_latest is not None else '無資料'

    # 盈餘品質率 >80%
    _eq_5y = _5y_vals('earnings_quality')
    _eq_latest = _eq_5y[0] if _eq_5y else None
    checks['eq_ok'] = 1 if _eq_latest is not None and _eq_latest >= 70 else 0
    detail['eq_ok'] = f'最近一年={_eq_latest:.2f}%' if _eq_latest is not None else '無資料'

    # 盈餘品質率近5年最低值 >60%
    _eq_min = min(_eq_5y) if _eq_5y else None
    checks['eq_min5'] = 1 if _eq_min is not None and _eq_min > 60 else 0
    detail['eq_min5'] = f'5年最低={_eq_min:.2f}%' if _eq_min is not None else '無資料'

    # 存貨週轉天數：最近一年 <= 近5年平均
    _invd_5y = _5y_vals('inventory_days')
    _invd_latest = _invd_5y[0] if _invd_5y else None
    _invd_avg = sum(_invd_5y) / len(_invd_5y) if _invd_5y else None
    checks['inv_days_avg'] = 1 if _invd_latest is not None and _invd_avg is not None and _invd_latest <= _invd_avg else 0
    detail['inv_days_avg'] = f'最近={_invd_latest:.1f}天 vs 5年平均={_invd_avg:.1f}天' if _invd_latest is not None and _invd_avg is not None else '無資料'

    # 存貨週轉天數：未創5年新高
    _invd_max = max(_invd_5y) if _invd_5y else None
    checks['inv_days_high'] = 1 if _invd_latest is not None and _invd_max is not None and _invd_latest < _invd_max else 0
    detail['inv_days_high'] = f'最近={_invd_latest:.1f}天 vs 5年最高={_invd_max:.1f}天' if _invd_latest is not None and _invd_max is not None else '無資料'

    # 應收帳款週轉天數：最近一年 <= 近5年平均
    _ard_5y = _5y_vals('ar_days')
    _ard_latest = _ard_5y[0] if _ard_5y else None
    _ard_avg = sum(_ard_5y) / len(_ard_5y) if _ard_5y else None
    checks['ar_days_avg'] = 1 if _ard_latest is not None and _ard_avg is not None and _ard_latest <= _ard_avg else 0
    detail['ar_days_avg'] = f'最近={_ard_latest:.1f}天 vs 5年平均={_ard_avg:.1f}天' if _ard_latest is not None and _ard_avg is not None else '無資料'

    # 應收帳款週轉天數：未創5年新高
    _ard_max = max(_ard_5y) if _ard_5y else None
    checks['ar_days_high'] = 1 if _ard_latest is not None and _ard_max is not None and _ard_latest < _ard_max else 0
    detail['ar_days_high'] = f'最近={_ard_latest:.1f}天 vs 5年最高={_ard_max:.1f}天' if _ard_latest is not None and _ard_max is not None else '無資料'

    # 近四季平均存貨週轉天數 < 近5年(20季)平均
    _qinv = r.get('_qinv')
    if _qinv:
        checks['qinv_4v20'] = 1 if _qinv['avg4'] < _qinv['avg20'] else 0
        detail['qinv_4v20'] = f'近4季平均={_qinv["avg4"]}天 vs 近20季平均={_qinv["avg20"]}天'
    else:
        checks['qinv_4v20'] = 0
        detail['qinv_4v20'] = '季度存貨資料不足'

    # === 價值評估檢核（5項） ===
    _shen_pe = r.get('shen_pe')
    _shen_eps = r.get('shen_eps')
    _eps_y = [r.get(f'eps_y{i}') for i in range(1, 6)]
    _eps_y_valid = [e for e in _eps_y if e is not None]

    # 預估(沈董)等級為A級以上
    _est_pe = r.get('est_pe')
    _used_pe = _est_pe if _est_pe is not None and _est_pe > 0 else _shen_pe
    _pe_src = '預估' if _est_pe is not None and _est_pe > 0 else '沈董'
    _est_grade = r.get('est_grade')
    _used_grade = _est_grade if _est_grade else r.get('shen_grade')
    _grade_src = '預估' if _est_grade else '沈董'
    _used_yld = r.get('est_yld') or r.get('shen_yld')
    checks['grade_a_ok'] = 1 if _used_grade in ('A', 'A1', 'A2', 'AA') else 0
    _grade_parts = [f'{_grade_src}等級={_used_grade or "無"}']
    if _used_pe is not None:
        _grade_parts.append(f'PE={_used_pe:.2f}倍')
    if _used_yld is not None:
        _grade_parts.append(f'殖利率={_used_yld:.2f}%')
    detail['grade_a_ok'] = '　'.join(_grade_parts)

    # 綜合等級為A級以上
    _blend_grade = r.get('blend_grade')
    _blend_pe = r.get('blend_pe')
    _blend_yld = r.get('blend_yld')
    checks['blend_grade_ok'] = 1 if _blend_grade in ('A', 'A1', 'A2', 'AA') else 0
    _bg_parts = [f'綜合等級={_blend_grade or "無"}']
    if _blend_pe is not None:
        _bg_parts.append(f'PE={_blend_pe:.2f}倍')
    if _blend_yld is not None:
        _bg_parts.append(f'殖利率={_blend_yld:.2f}%')
    detail['blend_grade_ok'] = '　'.join(_bg_parts)

    # EPS 來源判斷：有預估EPS用預估，沒有用沈董
    _est_eps_val = r.get('est_eps')
    _used_eps = _est_eps_val if _est_eps_val is not None and _est_eps_val > 0 else _shen_eps
    _eps_src = '預估' if _est_eps_val is not None and _est_eps_val > 0 else '沈董'
    _eps_avg5 = sum(_eps_y_valid) / len(_eps_y_valid) if _eps_y_valid else None
    _eps_y3_valid = [e for e in _eps_y[:3] if e is not None]
    _eps_avg3 = sum(_eps_y3_valid) / len(_eps_y3_valid) if _eps_y3_valid else None

    # 近五年EPS皆大於0
    checks['eps_5y_pos'] = 1 if len(_eps_y_valid) >= 5 and all(e > 0 for e in _eps_y_valid) else 0
    detail['eps_5y_pos'] = f'{len(_eps_y_valid)}年資料，正值{sum(1 for e in _eps_y_valid if e > 0)}年' if _eps_y_valid else '無資料'

    # 近五年最高EPS/最低EPS < 3
    if len(_eps_y_valid) >= 5 and all(e > 0 for e in _eps_y_valid):
        _eps_max = max(_eps_y_valid)
        _eps_min = min(_eps_y_valid)
        _eps_ratio = round(_eps_max / _eps_min, 2) if _eps_min > 0 else None
        checks['eps_5y_stable'] = 1 if _eps_ratio is not None and _eps_ratio < 3 else 0
        detail['eps_5y_stable'] = f'最高{_eps_max:.2f}/最低{_eps_min:.2f}={_eps_ratio:.2f}倍' if _eps_ratio is not None else '無資料'
    else:
        checks['eps_5y_stable'] = 0
        detail['eps_5y_stable'] = 'EPS資料不足或有負值'

    # 綜合殖利率 >= 5%
    _blend_yld = r.get('blend_yld')
    checks['wt_yld_ok'] = 1 if _blend_yld is not None and _blend_yld >= 5 else 0
    detail['wt_yld_ok'] = f'綜合殖利率={_blend_yld:.2f}%' if _blend_yld is not None else '無資料'

    # 加權配息率 40%~80%
    _wt_payout = r.get('weighted_payout')
    checks['wt_payout_ok'] = 1 if _wt_payout is not None and 40 <= _wt_payout <= 80 else 0
    detail['wt_payout_ok'] = f'加權配息率={_wt_payout:.2f}%' if _wt_payout is not None else '無資料'

    # 沈董EPS / 十年平均EPS >= 1
    _avg_eps_10y = (r.get('_gi') or {}).get('shiller_avg_eps')
    _eps_ratio_10y = None
    if _used_eps is not None and _avg_eps_10y is not None and _avg_eps_10y > 0:
        _eps_ratio_10y = round(_used_eps / _avg_eps_10y, 2)
        checks['eps_vs_10y'] = 1 if _eps_ratio_10y >= 1 else 0
        detail['eps_vs_10y'] = f'{_eps_src}EPS={_used_eps:.2f} / 10年均EPS={_avg_eps_10y:.2f} = {_eps_ratio_10y:.2f}'
    else:
        checks['eps_vs_10y'] = 0
        detail['eps_vs_10y'] = '10年均EPS<=0或無資料' if _avg_eps_10y is not None and _avg_eps_10y <= 0 else '無資料'

    # 沈董EPS >= 近5年/近3年/十年均EPS 中至少2個
    _pass_count = 0
    _cmp_parts = []
    if _used_eps is not None:
        if _eps_avg5 is not None:
            _p5 = _used_eps >= _eps_avg5
            _pass_count += 1 if _p5 else 0
            _cmp_parts.append(f'5年均{_eps_avg5:.2f} {"V" if _p5 else "X"}')
        if _eps_avg3 is not None:
            _p3 = _used_eps >= _eps_avg3
            _pass_count += 1 if _p3 else 0
            _cmp_parts.append(f'3年均{_eps_avg3:.2f} {"V" if _p3 else "X"}')
        if _avg_eps_10y is not None and _avg_eps_10y > 0:
            _p10 = _used_eps >= _avg_eps_10y
            _pass_count += 1 if _p10 else 0
            _cmp_parts.append(f'10年均{_avg_eps_10y:.2f} {"V" if _p10 else "X"}')
    checks['eps_vs_multi'] = 1 if _pass_count >= 2 else 0
    detail['eps_vs_multi'] = f'{_eps_src}EPS={_used_eps:.2f}　' + '　'.join(_cmp_parts) + f'　通過{_pass_count}/3' if _used_eps is not None and _cmp_parts else '無資料'

    # 現價 ≤ A級評價；≤ AA更佳
    if close and val_a:
        _in_a = close <= val_a + 0.005
        _below_aa = val_aa is not None and close <= val_aa + 0.005
        checks['price_val_ok'] = 1 if _in_a else 0
        _level = '≤ AA' if _below_aa else ('≤ A' if _in_a else '> A')
        detail['price_val_ok'] = f'股價{close} vs AA={val_aa} / A={val_a}（{_level}）'
    else:
        checks['price_val_ok'] = 0
        detail['price_val_ok'] = '無評價門檻'

    # 沈董法累計營業利益 / 累計稅前淨利 > 80%
    _cr = r.get('_core_ratio')
    if _cr:
        checks['core_ratio'] = 1 if _cr['ratio'] > 80 else 0
        detail['core_ratio'] = f'累計營業利益/稅前淨利={_cr["ratio"]:.2f}%（{_cr["quarters"]}季）'
    else:
        checks['core_ratio'] = 0
        detail['core_ratio'] = '無季度資料'

    # === 成長性評估檢核（5項） ===
    _gi = r.get('_gi') or {}

    # 累積營收年增率 >= 0%
    _cum_yoy = r.get('revenue_cum_yoy')
    checks['cum_rev_pos'] = 1 if _cum_yoy is not None and _cum_yoy >= 0 else 0
    detail['cum_rev_pos'] = f'累積營收年增率={_cum_yoy}%' if _cum_yoy is not None else '無資料'

    # 短期3M >= 0%（從 growth_map 讀取，不在 _gi 裡）
    _gs = (growth_map or {}).get(r['code'], {})
    _rev_3m = _gs.get('gi_rev_3m_yoy')
    checks['rev_3m_pos'] = 1 if _rev_3m is not None and _rev_3m >= 0 else 0
    detail['rev_3m_pos'] = f'短期3M={_rev_3m:.2f}%' if _rev_3m is not None else '無資料'

    # 長期12M >= 0%
    _rev_12m = _gs.get('gi_rev_12m_yoy')
    checks['rev_12m_pos'] = 1 if _rev_12m is not None and _rev_12m >= 0 else 0
    detail['rev_12m_pos'] = f'長期12M={_rev_12m:.2f}%' if _rev_12m is not None else '無資料'

    # 短期3M >= 0% 且 長期12M >= 0%（一致向上）
    checks['rev_both_pos'] = 1 if _rev_3m is not None and _rev_12m is not None and _rev_3m >= 0 and _rev_12m >= 0 else 0
    detail['rev_both_pos'] = f'3M={_rev_3m:.2f}% 12M={_rev_12m:.2f}%' if _rev_3m is not None and _rev_12m is not None else '無資料'

    # 短期3M >= 長期12M
    if _rev_3m is not None and _rev_12m is not None:
        checks['rev_3m_gt_12m'] = 1 if _rev_3m >= _rev_12m else 0
        detail['rev_3m_gt_12m'] = f'3M={_rev_3m:.2f}% vs 12M={_rev_12m:.2f}%'
    else:
        checks['rev_3m_gt_12m'] = 0
        detail['rev_3m_gt_12m'] = '無資料'

    # 趨勢燈號為多頭
    _ge_signal = _gs.get('growth_signal')
    checks['growth_green'] = 1 if _ge_signal == 'green' else 0
    detail['growth_green'] = f'燈號={_ge_signal or "無"}'

    # Neff 比率 >= 0.7
    _ge_neff_d = _gi.get('neff_d')
    _ge_neff_c = _gi.get('neff_c')
    _ge_yld = _gi.get('yield')
    _ge_pe = _gi.get('pe')
    checks['ge_neff_ratio'] = 1 if _ge_neff_d is not None and _ge_neff_d >= 0.7 else 0
    if _ge_neff_d is not None and _ge_neff_c is not None and _ge_yld is not None and _ge_pe:
        detail['ge_neff_ratio'] = f'Neff比率={_ge_neff_d:.2f}　(保守成長率{_ge_neff_c:.2f}% + 殖利率{_ge_yld:.2f}%) / PE{_ge_pe:.2f} = {round(_ge_neff_c + _ge_yld, 2)}/{_ge_pe:.2f}'
    else:
        detail['ge_neff_ratio'] = f'Neff比率={_ge_neff_d:.2f}' if _ge_neff_d is not None else '無資料'

    # PEG <= 1.0
    _ge_lynch_d = _gi.get('lynch_d')
    checks['ge_lynch_peg'] = 1 if _ge_lynch_d is not None and _ge_lynch_d <= 1.0 else 0
    if _ge_lynch_d is not None and _ge_neff_c is not None and _ge_yld is not None and _ge_pe:
        detail['ge_lynch_peg'] = f'PEG={_ge_lynch_d:.2f}　PE{_ge_pe:.2f} / (成長率{_ge_neff_c:.2f}% + 殖利率{_ge_yld:.2f}%) = {_ge_pe:.2f}/{round(_ge_neff_c + _ge_yld, 2)}'
    else:
        detail['ge_lynch_peg'] = f'PEG={_ge_lynch_d:.2f}' if _ge_lynch_d is not None else '無資料'

    # === DDM / DCF 計算（價值評估共用） ===

    # ddm_return: 股利折現模式現價潛在年報酬 >= 10%
    ddm_pe = float(user_params.get('ddmPE', 14)) if user_params and user_params.get('ddmPE') else 14
    ddm_rate = float(user_params.get('ddmRate', 0.10)) if user_params and user_params.get('ddmRate') else 0.10
    # EPS 取用順序：預估 > 系統 > 沈董（與 recalc_all_derived 一致）
    ddm_eps = est_eps
    if ddm_eps is None:
        sys_eps_val = r.get('sys_ann_eps')
        if sys_eps_val is not None and shen_eps is not None:
            ddm_eps = min(sys_eps_val, shen_eps)
        else:
            ddm_eps = sys_eps_val or shen_eps
    ddm_div = blend_div or shen_div
    ddm_ann_ret = None
    if ddm_eps and ddm_eps > 0 and close and close > 0:
        sell_price = ddm_eps * ddm_pe
        total_div = (ddm_div * 3) if ddm_div and ddm_div > 0 else 0
        ddm_div_display = f'{ddm_div}×3' if ddm_div else '0'
        if total_div > 0 or sell_price > close:
            target_price = sell_price + total_div
            total_ret = (target_price - close) / close
            ddm_ann_ret = round((pow(1 + total_ret, 1/3) - 1) * 100, 2)
    checks['val_ddm_return'] = 1 if ddm_ann_ret is not None and ddm_ann_ret >= 10 else 0
    if ddm_ann_ret is not None:
        detail['val_ddm_return'] = f'年報酬={ddm_ann_ret}%　EPS={ddm_eps} PE={ddm_pe} 股利={ddm_div_display} 折現率={ddm_rate}'
    else:
        detail['val_ddm_return'] = None

    # dcf_safe_ok: 現價 <= DCF 安全邊際價
    _dcf_fcf = r.get('_fcf_latest')  # 最新年 FCF (元)
    _dcf_cs = r.get('_common_stock')  # 股本 (元)
    _dcf_safe_price = None
    if _dcf_fcf and _dcf_fcf > 0 and _dcf_cs and _dcf_cs > 0:
        # 讀使用者自訂參數，沒有用預設
        _up = user_params or {}
        _dcf_rate = float(_up['dcfRate']) / 100 if _up.get('dcfRate') else 0.10
        _ig = (r.get('_gi') or {}).get('intrinsic_growth')
        _dcf_growth = float(_up['dcfGrowth']) / 100 if _up.get('dcfGrowth') else (
            _ig / 100 if _ig is not None else 0.05)
        _dcf_n = int(float(_up['dcfGrowthYears'])) if _up.get('dcfGrowthYears') else 5
        _dcf_tg = float(_up['dcfTermGrowth']) / 100 if _up.get('dcfTermGrowth') else 0.02
        _dcf_mg = float(_up['dcfMargin']) / 100 if _up.get('dcfMargin') else 0.80
        if _up.get('dcfFcf'):
            _dcf_fcf = float(_up['dcfFcf']) * 1000000  # 使用者輸入百萬，轉元
        # DCF 計算
        if _dcf_rate > _dcf_tg:
            _pv = 0
            _cf = _dcf_fcf
            for _i in range(1, _dcf_n + 1):
                _cf = (_dcf_fcf * (1 + _dcf_growth) if _i == 1 else _cf * (1 + _dcf_growth))
                _pv += _cf / (1 + _dcf_rate) ** _i
            _tv = _cf * (1 + _dcf_tg) / (_dcf_rate - _dcf_tg)
            _pv_tv = _tv / (1 + _dcf_rate) ** _dcf_n
            _total = _pv + _pv_tv
            _shares = _dcf_cs / 10
            _per_share = _total / _shares
            _dcf_safe_price = round(_per_share * _dcf_mg, 2)

    if _dcf_safe_price is not None and close:
        checks['dcf_safe_ok'] = 1 if close <= _dcf_safe_price + 0.005 else 0
        detail['dcf_safe_ok'] = f'股價{close} vs 安全邊際價{_dcf_safe_price}'
    else:
        checks['dcf_safe_ok'] = 0
        detail['dcf_safe_ok'] = 'FCF<=0或無資料' if _dcf_fcf and _dcf_fcf <= 0 else '無資料'

    profit_count = sum(checks.get(k, 0) for k in CHECKLIST_PROFIT_KEYS)
    safety_count = sum(checks.get(k, 0) for k in CHECKLIST_SAFETY_KEYS)
    value_count = sum(checks.get(k, 0) for k in CHECKLIST_VALUE_KEYS)
    growth_eval_count = sum(checks.get(k, 0) for k in CHECKLIST_GROWTH_EVAL_KEYS)
    pass_count = profit_count + safety_count + value_count + growth_eval_count

    # === 壓線標記（實際值落在門檻 ±10% 區間）===
    borderline = {}
    def _bl(key, actual, threshold, higher_is_pass=True):
        if actual is None or threshold is None or threshold == 0:
            return
        margin = abs(threshold * 0.1)
        if threshold - margin <= actual <= threshold + margin:
            borderline[key] = True
    _bl('roic_avg5', _roic_avg5, 15)
    _bl('roic_latest', _roic_latest, 15)
    _bl('roic_min5', _roic_min5, 10)
    _bl('opm_avg5', _opm_avg5, 10)
    _bl('opm_min5', _opm_min5, 5)
    _bl('debt_ratio_ok', _dr_latest, 50)
    _bl('fin_debt_ok', _fdr_latest, 30)
    _bl('current_ratio', _cr_latest, 150)
    _bl('quick_ratio', _qr_latest, 100)
    _bl('icr_ok', _icr_latest, 5)
    _bl('icr_min5', _icr_min, 3)
    _bl('eq_ok', _eq_latest, 70)
    _bl('eq_min5', _eq_min, 60)
    # grade_a_ok 是等級判斷，無數值壓線
    _bl('wt_yld_ok', _blend_yld, 5)
    if _eps_ratio_10y is not None:
        _bl('eps_vs_10y', _eps_ratio_10y, 1)
    if ddm_ann_ret is not None:
        _bl('val_ddm_return', ddm_ann_ret, 10)
    _bl('ge_neff_ratio', _ge_neff_d, 0.7)
    _bl('ge_lynch_peg', _ge_lynch_d, 1.0)
    if _wt_payout is not None:
        if _wt_payout < 40 and _wt_payout >= 36:
            borderline['wt_payout_ok'] = True
        elif _wt_payout > 80 and _wt_payout <= 88:
            borderline['wt_payout_ok'] = True

    # === 紅旗偵測（核心題不過 → 標記）===
    _core_keys = [it['key'] for it in CHECKLIST_ITEMS if it.get('weight') == '核心']
    red_flags = []
    for k in _core_keys:
        if checks.get(k) != 1:
            _it = next((x for x in CHECKLIST_ITEMS if x['key'] == k), None)
            if _it:
                red_flags.append(f'{_it["label"]}不過')
    # 趨勢燈號紅旗（不計分但觸發警示）
    if _gs.get('growth_signal') == 'red':
        red_flags.append('趨勢燈號為空頭')

    # 成長率指標（從 r['_gi'] 取出存入 DB）
    gi = r.get('_gi') or {}
    gi_fields = {
        'gi_neff_a': gi.get('neff_a'),
        'gi_neff_b': gi.get('neff_b'),
        'gi_neff_3a': gi.get('neff_3a'),
        'gi_neff_3b': gi.get('neff_3b'),
        'gi_neff_c': gi.get('neff_c'),
        'gi_neff_d': gi.get('neff_d'),
        'gi_intrinsic_growth': gi.get('intrinsic_growth'),
        'gi_lynch_a': gi.get('lynch_a'),
        'gi_lynch_b': gi.get('lynch_b'),
        'gi_lynch_c': gi.get('lynch_c'),
        'gi_lynch_d': gi.get('lynch_d'),
        'gi_rev_cagr_3y': gi.get('rev_cagr_3y'),
        'gi_rev_cagr_5y': gi.get('rev_cagr_5y'),
        'gi_shares_change': gi.get('shares_change'),
        'gi_yield': gi.get('yield'),
        'gi_pe': gi.get('pe'),
        'gi_gray': 1 if gi.get('gray') else 0,
        'gi_neff_gray': 1 if gi.get('neff_gray') else 0,
        'gi_lynch_gray': 1 if gi.get('lynch_gray') else 0,
        'gi_warnings': json.dumps(gi.get('warnings', []), ensure_ascii=False) if gi.get('warnings') else None,
    }

    return {
        'code': r['code'],
        **{f'chk_{k}': checks.get(k, 0) for k in CHECKLIST_ALL_KEYS},
        'pass_count': pass_count,
        'total_count': len(CHECKLIST_ALL_KEYS),
        'profit_count': profit_count,
        'safety_count': safety_count,
        'value_count': value_count,
        'growth_eval_count': growth_eval_count,
        'detail': json.dumps(detail, ensure_ascii=False),
        'borderline': json.dumps(borderline, ensure_ascii=False),
        'red_flags': json.dumps(red_flags, ensure_ascii=False),
        'eps_setting': r.get('val_eps_used') or r.get('shen_eps'),
        'div_setting': r.get('val_div_used') or r.get('shen_div'),
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
        **gi_fields,
    }

def _calc_growth_signals():
    """
    計算成長燈號：營收動能 + 獲利趨勢 + 存貨風險 → 綠/黃/紅
    回傳 dict: code → {growth_signal, growth_rev_momentum, growth_eps_trend, growth_inv_risk, growth_detail}
    """
    import json
    from collections import defaultdict
    from datetime import datetime

    result = {}
    now = datetime.now()
    cur_year = now.year
    cur_roc = cur_year - 1911

    # 1. 營收動能：財報狗 3M/12M 累計營收年增率
    rev_map = defaultdict(list)  # code → [(year, month, revenue), ...]
    try:
        rev_rows = query_db("""SELECT code, year, month, revenue FROM monthly_revenue
                               WHERE year >= ? AND revenue > 0 ORDER BY year, month""",
                            [cur_roc - 3])
        for r in rev_rows:
            rev_map[r['code']].append((r['year'], r['month'], r['revenue']))
    except Exception as e:
        print(f"[成長燈號] 營收查詢失敗: {e}")

    # 2. 季度EPS + 存貨 + 營收
    qf_map = defaultdict(list)  # code → [(year, quarter, eps, revenue, inventory), ...]
    try:
        qf_rows = query_db("""SELECT code, quarter, eps, revenue, inventory FROM quarterly_financial
                               WHERE eps IS NOT NULL""")
        for r in qf_rows:
            parts = r['quarter'].split('Q')
            if len(parts) == 2:
                y, q = int(parts[0]), int(parts[1])
                qf_map[r['code']].append((y, q, r['eps'], r['revenue'], r.get('inventory')))
    except Exception as e:
        print(f"[成長燈號] 季報查詢失敗: {e}")

    # 計算每支股票
    for code in set(list(rev_map.keys()) + list(qf_map.keys())):
        detail = {}
        rev_signal = 0   # 1=加速, 0=持平, -1=衰退
        eps_signal = 0    # 1=成長, 0=持平, -1=衰退
        inv_risk = 0      # 0=正常, 1=警訊

        # ── 營收動能（財報狗：3M/12M 累計營收年增率）──
        revs = sorted(rev_map.get(code, []), key=lambda x: (x[0], x[1]))
        rev_by_ym = {(r[0], r[1]): r[2] for r in revs}
        rev_3m = None
        rev_12m = None
        if len(revs) >= 3:
            # 近 3 月累計營收年增率
            recent_3 = revs[-3:]
            sum_cur_3 = sum(r[2] for r in recent_3)
            sum_prev_3 = sum(rev_by_ym.get((r[0] - 1, r[1]), 0) for r in recent_3)
            if sum_prev_3 > 0:
                rev_3m = round((sum_cur_3 / sum_prev_3 - 1) * 100, 2)
                detail['rev_3m'] = rev_3m
        if len(revs) >= 12:
            # 近 12 月累計營收年增率
            recent_12 = revs[-12:]
            sum_cur_12 = sum(r[2] for r in recent_12)
            sum_prev_12 = sum(rev_by_ym.get((r[0] - 1, r[1]), 0) for r in recent_12)
            if sum_prev_12 > 0:
                rev_12m = round((sum_cur_12 / sum_prev_12 - 1) * 100, 2)
                detail['rev_12m'] = rev_12m

        # 趨勢判定：短期(3M) vs 長期(12M)
        if rev_3m is not None and rev_12m is not None:
            detail['rev_cross'] = 'above' if rev_3m > rev_12m else 'below'
            if rev_3m > rev_12m and rev_3m > 0:
                rev_signal = 1   # 短>長且短正成長 = 多頭/轉強
            elif rev_3m < rev_12m and rev_12m < 0:
                rev_signal = -1  # 短<長且長為負 = 空頭
            elif rev_3m < 0 and rev_12m < 0:
                rev_signal = -1  # 雙負 = 衰退
            # 其他 = 0 持平
        elif rev_3m is not None:
            # 只有短期，用短期判斷
            if rev_3m >= 5:
                rev_signal = 1
            elif rev_3m < -5:
                rev_signal = -1

        # ── 獲利趨勢（最新季EPS vs 去年同季）──
        quarters = sorted(qf_map.get(code, []), key=lambda x: (x[0], x[1]))
        if quarters:
            latest_q = quarters[-1]
            ly, lq = latest_q[0], latest_q[1]
            # 找去年同季
            same_q_ly = [q for q in quarters if q[0] == ly - 1 and q[1] == lq]
            if same_q_ly:
                eps_now = latest_q[2] or 0
                eps_prev = same_q_ly[0][2] or 0
                if eps_prev > 0:
                    eps_growth = (eps_now - eps_prev) / abs(eps_prev) * 100
                    detail['eps_yoy'] = round(eps_growth, 1)
                    detail['eps_now_q'] = f"{ly}Q{lq}"
                    if eps_growth > 5:
                        eps_signal = 1
                    elif eps_growth < -10:
                        eps_signal = -1
                elif eps_prev <= 0 and eps_now > 0:
                    eps_signal = 1  # 虧轉盈
                    detail['eps_yoy'] = None
                    detail['eps_turnaround'] = True
                elif eps_prev <= 0 and eps_now <= 0:
                    eps_signal = -1  # 持續虧損
                    detail['eps_yoy'] = None

        # ── 存貨風險 ──
        if len(quarters) >= 5:
            latest_q = quarters[-1]
            same_q_ly = [q for q in quarters if q[0] == latest_q[0] - 1 and q[1] == latest_q[1]]
            if same_q_ly and latest_q[4] is not None and same_q_ly[0][4] is not None:
                inv_now = latest_q[4]
                inv_prev = same_q_ly[0][4]
                rev_now = latest_q[3] or 0
                rev_prev = same_q_ly[0][3] or 0
                if inv_prev > 0 and rev_prev > 0:
                    inv_growth = (inv_now - inv_prev) / inv_prev * 100
                    rev_growth = (rev_now - rev_prev) / rev_prev * 100 if rev_prev > 0 else 0
                    detail['inv_growth'] = round(inv_growth, 1)
                    detail['rev_vs_inv'] = round(rev_growth - inv_growth, 1)
                    # 存貨增速超過營收增速 20% 以上 → 警訊
                    if inv_growth > 20 and inv_growth > rev_growth + 20:
                        inv_risk = 1

        # ── 綜合燈號 ──
        # 綠燈：營收加速 且 EPS成長（或至少一個強且另一個不差）
        # 紅燈：營收衰退 或 (EPS衰退 + 存貨警訊)
        # 黃燈：其餘
        total_score = rev_signal + eps_signal
        if total_score >= 2:
            signal = 'green'
        elif total_score >= 1 and inv_risk == 0:
            signal = 'green'
        elif rev_signal == -1 and eps_signal == -1:
            signal = 'red'
        elif eps_signal == -1 and inv_risk == 1:
            signal = 'red'
        elif rev_signal == -1 and eps_signal <= 0:
            signal = 'red'
        else:
            signal = 'yellow'

        result[code] = {
            'growth_signal': signal,
            'growth_rev_momentum': rev_3m,
            'growth_eps_trend': detail.get('eps_yoy'),
            'growth_inv_risk': inv_risk,
            'growth_detail': json.dumps(detail, ensure_ascii=False),
            'gi_rev_3m_yoy': rev_3m,
            'gi_rev_12m_yoy': rev_12m,
        }

    print(f"[成長燈號] 已計算 {len(result)} 支: 綠={sum(1 for v in result.values() if v['growth_signal']=='green')}, "
          f"黃={sum(1 for v in result.values() if v['growth_signal']=='yellow')}, "
          f"紅={sum(1 for v in result.values() if v['growth_signal']=='red')}")
    return result


def calc_all_checklists():
    """批次計算所有股票的檢核表並存入 DB"""
    global _stocks_cache_time
    import json
    from datetime import datetime
    _init_checklist_db()

    cur_roc = datetime.now().year - 1911
    # 直接 SELECT 所有欄位（含衍生欄位），不再重算
    rows = query_db("SELECT * FROM stocks")

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

    # 批次計算成長燈號
    growth_map = {}
    try:
        growth_map = _calc_growth_signals()
    except Exception as e:
        print(f"[Checklist] 成長燈號計算失敗: {e}")

    # 批次查季報毛利率（一次查完，建 map）
    gm_map = {}
    try:
        qf_rows = query_db("""SELECT code, quarter, revenue, gross_profit FROM quarterly_financial
                              WHERE revenue > 0 AND gross_profit IS NOT NULL""")
        # 按 code 分組，每組按季度數值排序取最近兩季
        from collections import defaultdict
        _qf_by_code = defaultdict(list)
        for qr in qf_rows:
            _qf_by_code[qr['code']].append(qr)
        for code, qs in _qf_by_code.items():
            qs.sort(key=lambda x: (int(x['quarter'].split('Q')[0]), int(x['quarter'].split('Q')[1])), reverse=True)
            # 計算每季毛利率
            q_gms = []
            for q in qs:
                if q['revenue'] > 0 and q['gross_profit'] is not None:
                    q_gms.append(round(q['gross_profit'] / q['revenue'] * 100, 2))
            if len(qs) >= 2:
                gm0 = round(qs[0]['gross_profit'] / qs[0]['revenue'] * 100, 2)
                gm1 = round(qs[1]['gross_profit'] / qs[1]['revenue'] * 100, 2)
                # 季趨勢：近4季平均 vs 近12季平均
                _gm_q_avg4 = round(sum(q_gms[:4]) / len(q_gms[:4]), 2) if len(q_gms) >= 4 else None
                _gm_q_avg12 = round(sum(q_gms[:12]) / len(q_gms[:12]), 2) if len(q_gms) >= 12 else None
                gm_map[code] = {
                    'latest_q': qs[0]['quarter'], 'latest_gm': gm0,
                    'prev_q': qs[1]['quarter'], 'prev_gm': gm1,
                    'change': round(gm0 - gm1, 2),
                    'q_avg4': _gm_q_avg4, 'q_avg12': _gm_q_avg12,
                }
    except Exception as e:
        print(f"[Checklist] 毛利率查詢失敗: {e}")

    # 批次算季度存貨週轉天數（近4季平均 vs 近20季平均）
    qinv_map = {}  # {code: {'avg4': x, 'avg20': y}}
    try:
        qinv_rows = query_db("""SELECT code, quarter, cost, inventory FROM quarterly_financial
                                WHERE cost > 0 AND inventory IS NOT NULL
                                ORDER BY code, quarter""")
        from collections import defaultdict
        _qinv_by_code = defaultdict(list)
        for qr in qinv_rows:
            _qinv_by_code[qr['code']].append(qr)
        for code, qs in _qinv_by_code.items():
            # 數值排序（避免字串排序 99Q4 > 114Q4）
            qs.sort(key=lambda x: (int(x['quarter'].split('Q')[0]), int(x['quarter'].split('Q')[1])))
            # 算每季存貨週轉天數 = (本季存貨+上季存貨)/2 / 當季成本 × 90
            days_list = []
            for i in range(1, len(qs)):
                avg_inv = (qs[i]['inventory'] + qs[i-1]['inventory']) / 2
                days = avg_inv / qs[i]['cost'] * 90
                days_list.append(round(days, 1))
            if len(days_list) >= 4:
                avg4 = round(sum(days_list[-4:]) / 4, 1)
                n20 = min(len(days_list), 20)
                avg20 = round(sum(days_list[-n20:]) / n20, 1)
                qinv_map[code] = {'avg4': avg4, 'avg20': avg20}
    except Exception as e:
        print(f"[Checklist] 季度存貨週轉天數失敗: {e}")

    # 批次算沈董法本業佔比（累計營業利益 / 累計稅前淨利）
    core_ratio_map = {}  # {code: {'ratio': x, 'oi_sum': y, 'pti_sum': z, 'quarters': n}}
    try:
        _cr_rows = query_db("""SELECT code, quarter, operating_income, pretax_income
                               FROM quarterly_financial
                               WHERE operating_income IS NOT NULL AND pretax_income IS NOT NULL
                               ORDER BY code, quarter""")
        from collections import defaultdict
        _cr_by_code = defaultdict(list)
        for cr in _cr_rows:
            _cr_by_code[cr['code']].append(cr)
    except Exception as e:
        _cr_by_code = {}
        print(f"[Checklist] 本業佔比查詢失敗: {e}")

    # 預載10年EPS（席勒PE用）和5年ROIC
    _shiller_map = {}  # {code: [eps_list]}
    _roic_map = {}     # {code: [roic_list]}
    try:
        _fa_rows = query_db(
            """SELECT code, year, eps, operating_income, pretax_income, tax,
                      net_income, revenue, operating_cf, capex,
                      total_equity, total_assets, cash_and_equivalents,
                      short_term_debt, short_term_notes, current_long_term_debt,
                      long_term_bank_debt, other_long_term_debt, bonds_payable,
                      gross_profit, cash_dividend, weighted_shares, common_stock, current_liabilities, current_assets, inventory,
                      debt_ratio, fin_debt_ratio, interest_coverage, earnings_quality, fcf,
                      inventory_days, ar_days
               FROM financial_annual WHERE year >= ? AND revenue IS NOT NULL
               ORDER BY code, year""",
            (datetime.now().year - 11,)
        )
        from collections import defaultdict
        _fa_by_code = defaultdict(list)
        for _fr in _fa_rows:
            _fa_by_code[_fr['code']].append(_fr)
        for _code, _frs in _fa_by_code.items():
            # 席勒：收集10年EPS
            _eps_list = [_fr['eps'] for _fr in _frs if _fr.get('eps') is not None]
            if len(_eps_list) >= 7:
                _shiller_map[_code] = _eps_list
            # 最近5年各項均值（ROIC/ROE/OPM/FCF_REV）+ 趨勢用年度序列
            _roic_vals, _roe_vals, _opm_vals, _fcf_rev_vals = [], [], [], []
            _roic_yearly, _opm_yearly, _fcf_rev_yearly, _gm_yearly = [], [], [], []
            _safety_yearly = {'debt_ratio': [], 'fin_debt_ratio': [], 'interest_coverage': [],
                              'earnings_quality': [], 'fcf': [], 'inventory_days': [], 'ar_days': [],
                              'current_ratio': [], 'quick_ratio': []}
            _fcf_latest_val, _div_total_val = None, None
            for _fr in _frs[-5:]:
                _oi = _fr.get('operating_income')
                _te = _fr.get('total_equity')
                _pti = _fr.get('pretax_income')
                _tx = _fr.get('tax')
                _ni = _fr.get('net_income')
                _rev = _fr.get('revenue')
                _ocf = _fr.get('operating_cf')
                _capex = _fr.get('capex')
                _yr = _fr.get('year')
                # ROIC（Dorsey 法）
                _roic_val = None
                if _oi is not None and _fr.get('total_assets'):
                    _tr = _tx / _pti if _pti and _pti > 0 and _tx is not None else 0.2
                    _nopat = _oi * (1 - _tr)
                    _ta = _fr['total_assets']
                    _cl = _fr.get('current_liabilities') or 0
                    _cash = _fr.get('cash_and_equivalents', 0) or 0
                    _sd = _fr.get('short_term_debt', 0) or 0
                    _sn = _fr.get('short_term_notes', 0) or 0
                    _cld = _fr.get('current_long_term_debt', 0) or 0
                    _op_need = _rev * 0.05 if _rev and _rev > 0 else 0
                    _excess = max(_cash - _op_need, 0)
                    if _cl > 0:
                        _nibcl = _cl - _sd - _sn - _cld
                        _ic = _ta - _nibcl - _excess
                    else:
                        _ibd = _sd + _sn + _cld + sum(_fr.get(f, 0) or 0 for f in
                               ['long_term_bank_debt', 'other_long_term_debt', 'bonds_payable'])
                        _ic = (_te or 0) + _ibd - _excess
                    if _ic > 0:
                        _roic_val = round(_nopat / _ic * 100, 2)
                        _roic_vals.append(_roic_val)
                _roic_yearly.append((_yr, _roic_val))
                # ROE
                if _ni is not None and _te and _te > 0:
                    _roe_vals.append(round(_ni / _te * 100, 2))
                # 營益率
                _opm_val = None
                if _oi is not None and _rev and _rev > 0:
                    _opm_val = round(_oi / _rev * 100, 2)
                    _opm_vals.append(_opm_val)
                _opm_yearly.append((_yr, _opm_val))
                # FCF/營收
                _fcf_rev_val = None
                if _ocf is not None and _capex is not None and _rev and _rev > 0:
                    _fcf = _ocf + _capex
                    _fcf_rev_val = round(_fcf / _rev * 100, 2)
                    _fcf_rev_vals.append(_fcf_rev_val)
                    _fcf_latest_val = _fcf  # 最後一筆即為最新年
                _fcf_rev_yearly.append((_yr, _fcf_rev_val))
                # 年度毛利率（用 gross_profit 欄位）
                _gp = _fr.get('gross_profit')
                if _gp is not None and _rev and _rev > 0:
                    _gm_yearly.append((_yr, round(_gp / _rev * 100, 2)))
                else:
                    _gm_yearly.append((_yr, None))
                # 安全性指標（直接讀 DB 已算好的值）
                for _sk in _safety_yearly:
                    if _sk in ('current_ratio', 'quick_ratio'):
                        continue  # 下面單獨算
                    _sv = _fr.get(_sk)
                    _safety_yearly[_sk].append((_yr, _sv))
                # 流動比率/速動比率（從 current_assets/current_liabilities/inventory 即時算）
                _ca = _fr.get('current_assets')
                _cl_val = _fr.get('current_liabilities')
                _inv = _fr.get('inventory') or 0
                if _ca and _cl_val and _cl_val > 0:
                    _safety_yearly['current_ratio'].append((_yr, round(_ca / _cl_val * 100, 2)))
                    _safety_yearly['quick_ratio'].append((_yr, round((_ca - _inv) / _cl_val * 100, 2)))
                else:
                    _safety_yearly['current_ratio'].append((_yr, None))
                    _safety_yearly['quick_ratio'].append((_yr, None))
            # 最新年 FCF vs 現金股利（cash_dividend × weighted_shares千股）
            if _frs:
                _last_fr = _frs[-1]
                _cd = _last_fr.get('cash_dividend')
                _ws = _last_fr.get('weighted_shares')
                if _cd and _cd > 0 and _ws and _ws > 0:
                    _div_total_val = _cd * _ws * 1000  # 每股股利 × 加權股數(千股) × 1000
                # FCF latest（operating_cf + capex, capex 為負數）
                _ocf_last = _last_fr.get('operating_cf')
                _capex_last = _last_fr.get('capex')
                if _ocf_last is not None and _capex_last is not None:
                    _fcf_latest_val = _ocf_last + _capex_last
            if _roic_vals:
                _roic_map[_code] = _roic_vals
            # 其他均值存入同一個 map（用 tuple）
            _roic_map[_code + '_roe'] = _roe_vals
            _roic_map[_code + '_opm'] = _opm_vals
            _roic_map[_code + '_fcf_rev'] = _fcf_rev_vals
            # 趨勢序列（最近→最遠，供 checklist 用）
            _roic_map[_code + '_roic_5y'] = list(reversed(_roic_yearly))
            _roic_map[_code + '_opm_5y'] = list(reversed(_opm_yearly))
            _roic_map[_code + '_fcf_rev_5y'] = list(reversed(_fcf_rev_yearly))
            _roic_map[_code + '_gm_5y'] = list(reversed(_gm_yearly))
            _roic_map[_code + '_fcf_latest'] = _fcf_latest_val
            _roic_map[_code + '_div_total'] = _div_total_val
            # 最新年 common_stock（DCF 用）
            if _frs:
                _cs = _frs[-1].get('common_stock')
                if _cs and _cs > 0:
                    _roic_map[_code + '_common_stock'] = _cs
            # 安全性序列（最近→最遠）
            for _sk in _safety_yearly:
                _roic_map[_code + f'_{_sk}_5y'] = list(reversed(_safety_yearly[_sk]))
    except Exception as e:
        print(f"[Checklist] 席勒/ROIC 預載失敗: {e}")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    count = 0

    gs = _get_global_settings()
    for r in rows:
        r = dict(r)
        up = ue_map.get(r['code'])
        # 所有衍生欄位（shen_eps/blend_pe/val_aa等）已在 stocks 表，直接讀取
        r['_gi'] = gi_map.get(r['code']) or {}
        r['_gm_data'] = gm_map.get(r['code'])
        # 席勒PE注入 _gi（stocks 表已有 blend_pe）
        _s_eps = _shiller_map.get(r['code'])
        if _s_eps and len(_s_eps) >= 7:
            _avg_eps = sum(_s_eps) / len(_s_eps)
            r['_gi']['shiller_avg_eps'] = round(_avg_eps, 2)
            _close_val = r.get('close')
            if _avg_eps > 0 and _close_val and _close_val > 0:
                _sh_pe = round(_close_val / _avg_eps, 2)
                r['_gi']['shiller_pe'] = _sh_pe
                _bl_pe = r.get('blend_pe')
                if _bl_pe and _bl_pe > 0:
                    r['_gi']['shiller_alert'] = round(_bl_pe / _sh_pe, 2)
        # 趨勢資料（新增檢核項用）
        r['_opm_5y'] = _roic_map.get(r['code'] + '_opm_5y', [])
        r['_roic_5y'] = _roic_map.get(r['code'] + '_roic_5y', [])
        r['_gm_5y'] = _roic_map.get(r['code'] + '_gm_5y', [])
        r['_fcf_rev_5y'] = _roic_map.get(r['code'] + '_fcf_rev_5y', [])
        # 安全性指標序列
        for _sk in ('debt_ratio', 'fin_debt_ratio', 'interest_coverage', 'earnings_quality', 'fcf', 'inventory_days', 'ar_days', 'current_ratio', 'quick_ratio'):
            r[f'_{_sk}_5y'] = _roic_map.get(r['code'] + f'_{_sk}_5y', [])
        r['_qinv'] = qinv_map.get(r['code'])
        r['_qgm'] = gm_map.get(r['code'])
        r['_common_stock'] = _roic_map.get(r['code'] + '_common_stock')
        # 沈董法本業佔比：用 eps_1q 判斷當年度已公布季度
        _cr_data = _cr_by_code.get(r['code'], [])
        if _cr_data:
            _eq1 = r.get('eps_1q')  # 最新季，如 "115Q1" 或 "114Q4"
            if _eq1:
                _yr_roc = int(_eq1.split('Q')[0])
                _cur_q = int(_eq1.split('Q')[1])
                _cur_qs = [f'{_yr_roc}Q{q}' for q in range(1, _cur_q + 1)]
                if _cur_q == 4 or _yr_roc < cur_roc:
                    # Q4 全年或 fallback 到去年：用該年度全部季度
                    pass
                _cr_map_q = {cr['quarter']: cr for cr in _cr_data}
                _oi_sum = sum(_cr_map_q[q]['operating_income'] for q in _cur_qs if q in _cr_map_q)
                _pti_sum = sum(_cr_map_q[q]['pretax_income'] for q in _cur_qs if q in _cr_map_q)
                _matched = sum(1 for q in _cur_qs if q in _cr_map_q)
                if _matched > 0 and _pti_sum and _pti_sum > 0:
                    core_ratio_map[r['code']] = {
                        'ratio': round(_oi_sum / _pti_sum * 100, 2),
                        'oi_sum': _oi_sum, 'pti_sum': _pti_sum, 'quarters': _matched
                    }
        r['_core_ratio'] = core_ratio_map.get(r['code'])
        r['_fcf_latest'] = _roic_map.get(r['code'] + '_fcf_latest')
        r['_div_total_latest'] = _roic_map.get(r['code'] + '_div_total')
        user_params = up
        result = _calc_checklist_for_stock(r, user_params, gs, growth_map)

        # 合併成長燈號
        gs_data = growth_map.get(r['code'], {})
        result.update(gs_data)

        # 席勒PE警示計算
        _s_eps = _shiller_map.get(r['code'])
        if _s_eps and len(_s_eps) >= 7:
            _avg_eps = sum(_s_eps) / len(_s_eps)
            result['gi_shiller_avg_eps'] = round(_avg_eps, 2)
            _close = r.get('close')
            if _avg_eps > 0 and _close and _close > 0:
                _shiller_pe = _close / _avg_eps
                result['gi_shiller_pe'] = round(_shiller_pe, 2)
                _blend_pe = r.get('blend_pe')
                if _blend_pe and _blend_pe > 0:
                    result['gi_shiller_alert'] = round(_blend_pe / _shiller_pe, 2)

        # 體質指標5年均值
        _rv = _roic_map.get(r['code'])
        if _rv:
            result['gi_roic_avg'] = round(sum(_rv) / len(_rv), 2)
        _roe_v = _roic_map.get(r['code'] + '_roe', [])
        if _roe_v:
            result['gi_roe_avg'] = round(sum(_roe_v) / len(_roe_v), 2)
        _opm_v = _roic_map.get(r['code'] + '_opm', [])
        if _opm_v:
            result['gi_opm_avg'] = round(sum(_opm_v) / len(_opm_v), 2)
        _fcf_v = _roic_map.get(r['code'] + '_fcf_rev', [])
        if _fcf_v:
            result['gi_fcf_rev_avg'] = round(sum(_fcf_v) / len(_fcf_v), 2)

        # 動態建構 INSERT/UPDATE（名稱制 + 成長率指標欄位 + 成長燈號）
        chk_fields = [f'chk_{k}' for k in CHECKLIST_ALL_KEYS]
        all_fields = ['code'] + chk_fields + [
                       'pass_count', 'total_count', 'profit_count', 'safety_count', 'value_count', 'growth_eval_count', 'base_count', 'bonus_count', 'detail',
                       'eps_setting', 'div_setting', 'yld_high', 'yld_max', 'pe_high', 'pe_low',
                       'lt_div', 'lt_yld', 'val_a', 'val_a1', 'val_a2', 'val_aa', 'lt5', 'lt6', 'lt7',
                       'gi_neff_a', 'gi_neff_b', 'gi_neff_3a', 'gi_neff_3b',
                       'gi_neff_c', 'gi_neff_d', 'gi_intrinsic_growth',
                       'gi_lynch_a', 'gi_lynch_b', 'gi_lynch_c', 'gi_lynch_d',
                       'gi_rev_cagr_3y', 'gi_rev_cagr_5y', 'gi_shares_change', 'gi_yield', 'gi_pe',
                       'gi_gray', 'gi_neff_gray', 'gi_lynch_gray', 'gi_warnings',
                       'gi_shiller_avg_eps', 'gi_shiller_pe', 'gi_shiller_alert',
                       'gi_roic_avg', 'gi_roe_avg', 'gi_opm_avg', 'gi_fcf_rev_avg',
                       'growth_signal', 'growth_rev_momentum', 'growth_eps_trend',
                       'growth_inv_risk', 'growth_detail',
                       'gi_rev_3m_yoy', 'gi_rev_12m_yoy',
                       'borderline', 'red_flags',
                       'updated_at']
        result['updated_at'] = now
        placeholders = ','.join(['?'] * len(all_fields))
        update_clause = ', '.join(f'{f}=excluded.{f}' for f in all_fields if f != 'code')
        values = [result.get(f) for f in all_fields]
        c.execute(f"""INSERT INTO stock_checklist ({','.join(all_fields)})
                     VALUES ({placeholders})
                     ON CONFLICT(code) DO UPDATE SET {update_clause}""", values)
        count += 1

    conn.commit()
    conn.close()
    with _cache_lock:
        _stocks_cache_time = 0  # 清快取，讓下次 API 重新查詢
    print(f"[Checklist] 已計算 {count} 支股票檢核表")
    return count


def _recalc_checklist_single(code):
    """重算單支股票的檢核表（儲存預估參數或股價更新後呼叫）"""
    global _stocks_cache_time
    import json
    from datetime import datetime
    _init_checklist_db()

    rows = query_db("SELECT * FROM stocks WHERE code=?", (code,))
    if not rows:
        return

    r = dict(rows[0])
    gs = _get_global_settings()

    # 成長率指標（聶夫/林區）
    try:
        with app.test_request_context():
            gi_resp = growth_indicators()
            gi_map = json.loads(gi_resp.get_data(as_text=True) if hasattr(gi_resp, 'get_data') else gi_resp.data)
            r['_gi'] = gi_map.get(code)
    except Exception:
        pass

    # 毛利率
    try:
        qf = query_db("""SELECT quarter, revenue, gross_profit FROM quarterly_financial
                         WHERE code=? AND revenue > 0 AND gross_profit IS NOT NULL
                         ORDER BY CAST(SUBSTR(quarter,1,INSTR(quarter,'Q')-1) AS INT) DESC,
                                  CAST(SUBSTR(quarter,INSTR(quarter,'Q')+1) AS INT) DESC
                         LIMIT 2""", (code,))
        if len(qf) >= 2:
            gm0 = round(qf[0]['gross_profit'] / qf[0]['revenue'] * 100, 2)
            gm1 = round(qf[1]['gross_profit'] / qf[1]['revenue'] * 100, 2)
            r['_gm_data'] = {'latest_q': qf[0]['quarter'], 'latest_gm': gm0,
                             'prev_q': qf[1]['quarter'], 'prev_gm': gm1,
                             'change': round(gm0 - gm1, 2)}
    except Exception:
        pass

    user_params = None
    try:
        ue = query_db("SELECT params FROM user_estimates WHERE code=?", (code,))
        if ue and ue[0]['params']:
            user_params = json.loads(ue[0]['params'])
    except Exception: pass

    # 席勒PE注入 _gi
    try:
        _fa_eps = query_db("SELECT eps FROM financial_annual WHERE code=? AND year>=? AND eps IS NOT NULL ORDER BY year",
                           (code, datetime.now().year - 11))
        _eps_list = [row['eps'] for row in _fa_eps]
        if len(_eps_list) >= 7 and r.get('_gi') is not None:
            _avg = sum(_eps_list) / len(_eps_list)
            r['_gi']['shiller_avg_eps'] = round(_avg, 2)
            if _avg > 0 and r.get('close') and r['close'] > 0:
                _sp = round(r['close'] / _avg, 2)
                r['_gi']['shiller_pe'] = _sp
                if r.get('blend_pe') and r['blend_pe'] > 0:
                    r['_gi']['shiller_alert'] = round(r['blend_pe'] / _sp, 2)
    except Exception: pass

    # 趨勢資料（ROIC/OPM/毛利率/FCF 5年序列）
    try:
        _fa5 = query_db("""SELECT year, operating_income, revenue, operating_cf, capex,
                                  total_equity, pretax_income, tax, gross_profit, cash_dividend, weighted_shares,
                                  short_term_debt, short_term_notes, current_long_term_debt,
                                  long_term_bank_debt, other_long_term_debt, bonds_payable, cash_and_equivalents
                           FROM financial_annual WHERE code=? AND revenue IS NOT NULL ORDER BY year DESC LIMIT 5""", (code,))
        _opm_5y, _roic_5y, _gm_5y, _fcf_rev_5y = [], [], [], []
        _fcf_latest, _div_total = None, None
        for _fr in _fa5:
            _oi = _fr.get('operating_income'); _rev = _fr.get('revenue'); _te = _fr.get('total_equity')
            _pti = _fr.get('pretax_income'); _tx = _fr.get('tax')
            _ocf = _fr.get('operating_cf'); _capex = _fr.get('capex'); _gp = _fr.get('gross_profit')
            _yr = _fr.get('year')
            _opm_val = round(_oi / _rev * 100, 2) if _oi is not None and _rev and _rev > 0 else None
            _opm_5y.append((_yr, _opm_val))
            _roic_val = None
            if _oi is not None and _fr.get('total_assets'):
                _tr = _tx / _pti if _pti and _pti > 0 and _tx is not None else 0.2
                _nopat = _oi * (1 - _tr)
                _f_ta = _fr['total_assets']
                _f_cl = _fr.get('current_liabilities') or 0
                _f_cash = _fr.get('cash_and_equivalents', 0) or 0
                _f_sd = _fr.get('short_term_debt', 0) or 0
                _f_sn = _fr.get('short_term_notes', 0) or 0
                _f_cld = _fr.get('current_long_term_debt', 0) or 0
                _f_op = _rev * 0.05 if _rev and _rev > 0 else 0
                _f_exc = max(_f_cash - _f_op, 0)
                if _f_cl > 0:
                    _f_nibcl = _f_cl - _f_sd - _f_sn - _f_cld
                    _f_ic = _f_ta - _f_nibcl - _f_exc
                else:
                    _f_ibd = _f_sd + _f_sn + _f_cld + sum(_fr.get(f, 0) or 0 for f in ['long_term_bank_debt','other_long_term_debt','bonds_payable'])
                    _f_ic = (_te or 0) + _f_ibd - _f_exc
                if _f_ic > 0: _roic_val = round(_nopat / _f_ic * 100, 2)
            _roic_5y.append((_yr, _roic_val))
            _gm_5y.append((_yr, round(_gp / _rev * 100, 2) if _gp is not None and _rev and _rev > 0 else None))
            _fcf_val = None
            if _ocf is not None and _capex is not None and _rev and _rev > 0:
                _fcf = _ocf + _capex
                _fcf_val = round(_fcf / _rev * 100, 2)
                if _fcf_latest is None: _fcf_latest = _fcf
            _fcf_rev_5y.append((_yr, _fcf_val))
            if _div_total is None:
                _cd = _fr.get('cash_dividend'); _ws = _fr.get('weighted_shares')
                if _cd and _cd > 0 and _ws and _ws > 0: _div_total = _cd * _ws * 1000
        r['_opm_5y'] = _opm_5y; r['_roic_5y'] = _roic_5y
        r['_gm_5y'] = _gm_5y; r['_fcf_rev_5y'] = _fcf_rev_5y
        r['_fcf_latest'] = _fcf_latest; r['_div_total_latest'] = _div_total
    except Exception: pass

    # 從 DB 讀取已有的 growth_signal（單支重算不重跑成長燈號）
    _single_growth_map = {}
    try:
        _gs_row = query_db("SELECT growth_signal FROM stock_checklist WHERE code=?", (code,))
        if _gs_row:
            _single_growth_map[code] = {'growth_signal': _gs_row[0]['growth_signal']}
    except Exception: pass
    result = _calc_checklist_for_stock(r, user_params, gs, _single_growth_map)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # 動態建構 INSERT/UPDATE（與 calc_all_checklists 一致）
    chk_fields = [f'chk_{k}' for k in CHECKLIST_ALL_KEYS]
    all_fields = ['code'] + chk_fields + [
                   'pass_count', 'total_count', 'profit_count', 'safety_count', 'value_count', 'growth_eval_count', 'base_count', 'bonus_count', 'detail',
                   'eps_setting', 'div_setting', 'yld_high', 'yld_max', 'pe_high', 'pe_low',
                   'lt_div', 'lt_yld', 'val_a', 'val_a1', 'val_a2', 'val_aa', 'lt5', 'lt6', 'lt7',
                   'gi_neff_a', 'gi_neff_b', 'gi_neff_3a', 'gi_neff_3b',
                   'gi_neff_c', 'gi_neff_d', 'gi_intrinsic_growth',
                   'gi_lynch_a', 'gi_lynch_b', 'gi_lynch_c', 'gi_lynch_d',
                   'gi_rev_cagr_5y', 'gi_shares_change', 'gi_yield', 'gi_pe',
                   'gi_gray', 'gi_neff_gray', 'gi_lynch_gray', 'gi_warnings',
                   'borderline', 'red_flags',
                   'updated_at']
    result['updated_at'] = now
    placeholders = ','.join(['?'] * len(all_fields))
    update_clause = ', '.join(f'{f}=excluded.{f}' for f in all_fields if f != 'code')
    values = [result.get(f) for f in all_fields]
    c.execute(f"""INSERT INTO stock_checklist ({','.join(all_fields)})
                 VALUES ({placeholders})
                 ON CONFLICT(code) DO UPDATE SET {update_clause}""", values)
    conn.commit()
    conn.close()
    with _cache_lock:
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
                        ('priority_grade','TEXT'),('grade_source','TEXT'),
                        # 衍生計算欄位（原本在前端JS計算，現統一存DB）
                        ('shen_eps','REAL'),('shen_div','REAL'),
                        ('shen_pe','REAL'),('shen_yld','REAL'),('shen_grade','TEXT'),
                        ('weighted_eps','REAL'),('weighted_div','REAL'),('weighted_pe','REAL'),('weighted_yld','REAL'),('weighted_grade','TEXT'),
                        ('weighted_payout','REAL'),
                        ('blend_eps','REAL'),('blend_div','REAL'),('blend_pe','REAL'),('blend_yld','REAL'),('blend_grade','TEXT'),
                        ('eps_4q_sum','REAL'),('trailing_div','REAL'),('trailing_pe','REAL'),('trailing_yld','REAL'),('trailing_grade','TEXT'),
                        ('contract_chg','REAL'),
                        ('payout_1','REAL'),('payout_2','REAL'),('payout_3','REAL'),
                        ('payout_4','REAL'),('payout_5','REAL'),('payout_6','REAL'),
                        ('val_aa','REAL'),('val_a1','REAL'),('val_a2','REAL'),('val_a','REAL'),('val_lt6','REAL'),
                        ('val_eps_used','REAL'),('val_div_used','REAL'),
                        ('val_pe','REAL'),('val_yld','REAL'),('val_source','TEXT'),
                        ('est_eps','REAL'),('est_div','REAL'),('est_pe','REAL'),('est_yld','REAL'),('est_grade','TEXT'),
                        ('sys_pe','REAL'),('sys_yld','REAL'),('sys_grade','TEXT'),
                        ('gb_roic','REAL'),('gb_ey','REAL'),('gb_roic_rank','INTEGER'),('gb_ey_rank','INTEGER'),('gb_total_rank','INTEGER')]:
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
                       sys_ann_eps, sys_ann_div, sys_ann_pe, sys_ann_yld, sys_ann_confidence,
                       shen_eps, shen_div,
                       shen_pe, shen_yld, shen_grade,
                       weighted_eps, weighted_div, weighted_pe, weighted_yld, weighted_grade, weighted_payout,
                       blend_eps, blend_div, blend_pe, blend_yld, blend_grade,
                       eps_4q_sum, trailing_div, trailing_pe, trailing_yld, trailing_grade,
                       contract_chg, listed_date,
                       payout_1, payout_2, payout_3, payout_4, payout_5, payout_6,
                       val_aa, val_a1, val_a2, val_a, val_lt6,
                       val_eps_used, val_div_used,
                       est_eps, est_div, est_pe, est_yld, est_grade,
                       sys_pe, sys_yld, sys_grade,
                       gb_roic, gb_ey, gb_roic_rank, gb_ey_rank, gb_total_rank
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

    # 無篩選時用記憶體快取（30秒，存 JSON 字串避免重複佔記憶體）
    use_cache = not q and not market and not exact
    if use_cache:
        with _cache_lock:
            if _stocks_cache and (_time.time() - _stocks_cache_time < 30):
                return app.response_class(_stocks_cache, content_type='application/json')

    rows = query_db(sql, params)

    # 附加 ETF 持股資訊（批次查詢，避免 N+1）
    etf_map = {}
    try:
        etf_rows = query_db("""
            SELECT h.stock_code,
                   GROUP_CONCAT(h.etf_code || ':' || COALESCE(i.name,''), ',') as etf_list
            FROM etf_holdings h
            LEFT JOIN etf_info i ON h.etf_code = i.code
            GROUP BY h.stock_code
        """)
        for r in etf_rows:
            etf_map[r["stock_code"]] = r["etf_list"]
    except Exception: pass

    # 批次查詢月營收（當年度各月）
    rev_map = {}  # code -> [{month, revenue, yoy}, ...]
    try:
        from datetime import date
        cur_west = date.today().year
        rev_rows = query_db(
            """SELECT r.code, r.month, r.revenue, r2.revenue as prev_revenue
               FROM monthly_revenue r
               LEFT JOIN monthly_revenue r2 ON r.code = r2.code AND r2.year = r.year - 1 AND r2.month = r.month
               WHERE r.year = ?
               ORDER BY r.code, r.month""", (cur_west,))
        for r in rev_rows:
            code = r['code']
            if code not in rev_map:
                rev_map[code] = []
            yoy = None
            if r['revenue'] and r['prev_revenue'] and r['prev_revenue'] > 0:
                yoy = round((r['revenue'] - r['prev_revenue']) / r['prev_revenue'] * 100, 2)
            rev_map[code].append({'month': r['month'], 'revenue': r['revenue'], 'yoy': yoy})
    except Exception: pass

    # 批次查詢 checklist pass_count
    chk_map = {}
    try:
        _init_checklist_db()
        chk_rows = query_db("""SELECT code, pass_count, total_count,
                                profit_count, safety_count, value_count, growth_eval_count,
                                red_flags,
                                gi_neff_a, gi_neff_b, gi_neff_3a, gi_neff_3b,
                                gi_neff_c, gi_neff_d, gi_intrinsic_growth,
                                gi_lynch_a, gi_lynch_b, gi_lynch_c, gi_lynch_d,
                                gi_rev_cagr_3y, gi_rev_cagr_5y, gi_shares_change, gi_yield, gi_pe,
                                gi_gray, gi_neff_gray, gi_lynch_gray, gi_warnings,
                                gi_shiller_avg_eps, gi_shiller_pe, gi_shiller_alert,
                                gi_roic_avg, gi_roe_avg, gi_opm_avg, gi_fcf_rev_avg,
                                growth_signal, growth_rev_momentum, growth_eps_trend, growth_inv_risk,
                                gi_rev_3m_yoy, gi_rev_12m_yoy
                             FROM stock_checklist""")
        for cr in chk_rows:
            chk_map[cr['code']] = cr
    except Exception: pass

    from datetime import date as _date
    _cur_year = _date.today().year
    for row in rows:
        # 掛牌年數
        ld = row.get('listed_date')
        row['listed_years'] = _cur_year - int(ld[:4]) if ld and len(ld) >= 4 else None
        row["etf_tags"] = etf_map.get(row["code"], "")
        row["monthly_rev"] = rev_map.get(row["code"], [])
        # 衍生欄位已存 DB，直接從 SELECT 讀取，不再即時計算
        chk = chk_map.get(row["code"])
        row["_chk_pass"] = chk['pass_count'] if chk else None
        row["_chk_total"] = chk['total_count'] if chk else None
        row["_chk_profit"] = chk['profit_count'] if chk else None
        row["_chk_profit_total"] = len(CHECKLIST_PROFIT_KEYS)
        row["_chk_safety"] = chk['safety_count'] if chk else None
        row["_chk_safety_total"] = len(CHECKLIST_SAFETY_KEYS)
        row["_chk_value"] = chk['value_count'] if chk else None
        row["_chk_value_total"] = len(CHECKLIST_VALUE_KEYS)
        row["_chk_growth"] = chk['growth_eval_count'] if chk else None
        row["_chk_growth_total"] = len(CHECKLIST_GROWTH_EVAL_KEYS)
        try:
            import json as _json_rf
            _rf = _json_rf.loads(chk['red_flags']) if chk and chk.get('red_flags') else []
            row["_chk_red_flags"] = len(_rf)
        except Exception:
            row["_chk_red_flags"] = 0
        row["_growth_signal"] = chk.get('growth_signal') if chk else None
        row["_growth_rev"] = chk.get('growth_rev_momentum') if chk else None
        row["_growth_eps"] = chk.get('growth_eps_trend') if chk else None
        row["_growth_inv"] = chk.get('growth_inv_risk') if chk else None
        # 成長率指標（從 stock_checklist 讀取，不再前端獨立計算）
        if chk:
            import json as _json_mod
            row["_gi"] = {
                'neff_a': chk['gi_neff_a'], 'neff_b': chk['gi_neff_b'],
                'neff_3a': chk['gi_neff_3a'], 'neff_3b': chk['gi_neff_3b'],
                'neff_c': chk['gi_neff_c'], 'neff_d': chk['gi_neff_d'],
                'intrinsic_growth': chk['gi_intrinsic_growth'],
                'lynch_a': chk['gi_lynch_a'], 'lynch_b': chk['gi_lynch_b'],
                'lynch_c': chk['gi_lynch_c'], 'lynch_d': chk['gi_lynch_d'],
                'rev_cagr_3y': chk['gi_rev_cagr_3y'],
                'rev_cagr_5y': chk['gi_rev_cagr_5y'], 'shares_change': chk['gi_shares_change'],
                'yield': chk['gi_yield'], 'pe': chk['gi_pe'],
                'gray': bool(chk['gi_gray']), 'neff_gray': bool(chk['gi_neff_gray']),
                'lynch_gray': bool(chk['gi_lynch_gray']),
                'warnings': _json_mod.loads(chk['gi_warnings']) if chk['gi_warnings'] else [],
                'shiller_avg_eps': chk.get('gi_shiller_avg_eps'),
                'shiller_pe': chk.get('gi_shiller_pe'),
                'shiller_alert': chk.get('gi_shiller_alert'),
                'roic_avg': chk.get('gi_roic_avg'),
                'roe_avg': chk.get('gi_roe_avg'),
                'opm_avg': chk.get('gi_opm_avg'),
                'fcf_rev_avg': chk.get('gi_fcf_rev_avg'),
                'rev_3m_yoy': chk.get('gi_rev_3m_yoy'),
                'rev_12m_yoy': chk.get('gi_rev_12m_yoy'),
            }
        else:
            row["_gi"] = None

    result_data = {"count": len(rows), "data": rows}
    resp = jsonify(result_data)
    if use_cache:
        with _cache_lock:
            _stocks_cache = resp.get_data(as_text=True)  # 存 JSON 字串，不存 dict
            _stocks_cache_time = _time.time()
    return resp

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
        "is_refreshing": _is_refreshing,
        "bg_done_at":   _bg_done_at,
    })

# ── 批次重算衍生欄位（權重變更時呼叫）──────────────────────────
@app.route("/api/recalc-derived", methods=["POST"])
def api_recalc_derived():
    """重算所有股票的衍生欄位（沈董PE/殖利率/等級、加權、綜合、近四季、合約負債變動）"""
    import time as _t
    t0 = _t.time()
    # 清快取
    with _cache_lock:
        _stocks_cache_time = 0
    cnt = recalc_all_derived()
    elapsed = round(_t.time() - t0, 2)
    # 背景 push 到 Render
    if not IS_CLOUD:
        _bg_push_table('stocks',
                       ['code'] + DERIVED_COLS,
                       'code')
    return jsonify({"status": "ok", "updated": cnt, "elapsed_sec": elapsed})


# ── 手動更新營收/季報 ──────────────────────────────────────
@app.route("/api/refresh/revenue", methods=["POST"])
def refresh_revenue():
    is_cloud = bool(os.environ.get('DATABASE_URL'))
    results = {'mops_revenue': 0, 'mops_quarterly': 0, 'errors': []}

    try:
        # 1. 抓最新營收（MOPS）— 本機和 Render 都可以跑
        from mops_fetcher import fetch_mops_monthly_revenue
        mops_count = fetch_mops_monthly_revenue()
        results['mops_revenue'] = mops_count or 0
        print(f"[手動營收] MOPS 更新 {mops_count} 筆")
    except Exception as e:
        results['errors'].append(f"營收: {e}")
        print(f"[手動營收] 失敗: {e}")

    try:
        # 2. 抓最新季報（MOPS）— 本機和 Render 都可以跑
        from mops_fetcher import fetch_latest_mops_quarterly
        mops_q = fetch_latest_mops_quarterly()
        results['mops_quarterly'] = mops_q or 0
        if mops_q and mops_q > 0:
            from scraper import _sync_eps_from_quarterly
            _sync_eps_from_quarterly()
            print(f"[手動季報] MOPS 更新 {mops_q} 筆")
    except Exception as e:
        results['errors'].append(f"季報: {e}")
        print(f"[手動季報] 失敗: {e}")

    # 3. 本機才 push 到 Render（Render 上已直接寫入 PostgreSQL）
    if not is_cloud:
        try:
            from render_sync import _push_table_to_render, _push_annual_to_render
            _push_table_to_render(
                table='monthly_revenue',
                columns=['code','year','month','revenue','updated_at'],
                pk=['code','year','month'],
                since=_get_today_start(),
            )
            _push_table_to_render(
                table='quarterly_financial',
                columns=['code','quarter','revenue','cost','gross_profit','operating_expense',
                         'operating_income','non_operating','pretax_income','tax','continuing_income',
                         'net_income_parent','eps','contract_liability','inventory','updated_at'],
                pk=['code','quarter'],
                since=_get_today_start(),
            )
            _push_annual_to_render()
            print("[手動營收/季報] push Render 完成")
        except Exception as e:
            results['errors'].append(f"push: {e}")

    total = results['mops_revenue'] + results['mops_quarterly']
    if results['errors']:
        return jsonify({"status": "error", "msg": f"部分失敗: {'; '.join(results['errors'])}", "detail": results})
    if total == 0:
        return jsonify({"status": "ok", "msg": "MOPS 無新資料（可能尚未申報）", "detail": results})
    return jsonify({"status": "ok", "msg": f"更新完成：營收 {results['mops_revenue']} 筆、季報 {results['mops_quarterly']} 筆", "detail": results})

def _get_today_start():
    from datetime import date
    return date.today().strftime('%Y-%m-%d') + ' 00:00:00'

# ── 手動觸發更新（背景執行，立即回應）─────────────────────
@app.route("/api/refresh", methods=["POST"])
def refresh():
    is_cloud = bool(os.environ.get('DATABASE_URL'))
    global _is_refreshing
    if _is_refreshing:
        return jsonify({"status": "already_running", "msg": "更新中，請稍候"}), 200

    def do_refresh():
        global _is_refreshing
        with _refresh_lock:
            _is_refreshing = True
            try:
                # TWSE/TPEX 政府 API，本機和 Render 都能跑
                refresh_prices()
                from scraper import _save_daily_price
                try: _save_daily_price()
                except Exception: pass
            finally:
                _is_refreshing = False

            # 第二階段：背景慢慢跑，不卡前端
            try:
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
                if not is_cloud:
                    try: calc_all_checklists()
                    except Exception: pass
                try: recalc_all_derived()
                except Exception: pass
                # 本機才 push 到 Render
                if not is_cloud:
                    try:
                        from render_sync import _push_prices_to_render, _push_annual_to_render
                        _push_prices_to_render()
                        _push_annual_to_render()
                    except Exception as e:
                        print(f"[更新股價] push Render 失敗: {e}")
            except Exception as e:
                print(f"[背景更新] 失敗: {e}")
            global _bg_done_at
            _bg_done_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    threading.Thread(target=do_refresh, daemon=True).start()
    msg = "開始更新資料"
    return jsonify({"status": "started", "msg": msg})

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
                     ('val_a2','REAL'),('val_a','REAL'),('val_lt6','REAL'),('discount_pct','REAL'),
                     ('neff_d','REAL'),('lynch_d','REAL'),
                     ('shen_grade','TEXT'),('est_grade','TEXT'),('blend_grade','TEXT')]:
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
                          val_level, val_aa, val_a1, val_a2, val_a, val_lt6, discount_pct,
                          neff_d, lynch_d, shen_grade, est_grade, blend_grade, updated_at)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                         ON CONFLICT(stock_id, date) DO UPDATE SET
                         price=excluded.price, price_pos=excluded.price_pos,
                         fair_low=excluded.fair_low, fair_mid=excluded.fair_mid, fair_high=excluded.fair_high,
                         shen_eps=excluded.shen_eps, shen_pe=excluded.shen_pe, shen_yld=excluded.shen_yld,
                         fin_grade=excluded.fin_grade,
                         val_level=excluded.val_level, val_aa=excluded.val_aa, val_a1=excluded.val_a1,
                         val_a2=excluded.val_a2, val_a=excluded.val_a, val_lt6=excluded.val_lt6,
                         discount_pct=excluded.discount_pct,
                         neff_d=excluded.neff_d, lynch_d=excluded.lynch_d,
                         shen_grade=excluded.shen_grade, est_grade=excluded.est_grade,
                         blend_grade=excluded.blend_grade, updated_at=excluded.updated_at""",
                      (r['code'], r['date'], r.get('price'), r.get('pp'),
                       r.get('fl'), r.get('fm'), r.get('fh'),
                       r.get('se'), r.get('sp'), r.get('sy'), r.get('fg'),
                       r.get('vl'), r.get('aa'), r.get('a1'), r.get('a2'),
                       r.get('a'), r.get('lt6'), r.get('dp'),
                       r.get('neff_d'), r.get('lynch_d'),
                       r.get('shen_grade'), r.get('est_grade'), r.get('blend_grade'), now))
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

@app.route("/api/sync/industry-news", methods=["POST"])
def sync_industry_news():
    """接收本機 push 過來的產業新聞（與 /api/sync/news 同樣模式）"""
    if not check_sync_token():
        return jsonify({"status": "error", "msg": "unauthorized"}), 403
    data = request.json
    if not data or 'rows' not in data:
        return jsonify({"error": "missing rows"}), 400
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS industry_news (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT, title TEXT, link TEXT, pub_time TEXT,
        summary TEXT, created_at TEXT, archived_code TEXT, archived_at TEXT
    )""")
    inserted = 0
    for r in data['rows']:
        try:
            c.execute("SELECT id FROM industry_news WHERE title=? AND source=?",
                      (r.get('title'), r.get('source')))
            if not c.fetchone():
                c.execute("""INSERT INTO industry_news
                             (source, title, link, pub_time, summary, created_at)
                             VALUES (?,?,?,?,?,?)""",
                          (r.get('source'), r.get('title'), r.get('link'),
                           r.get('pub_time'), r.get('summary'), r.get('created_at')))
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
        if not check_sync_token():
            return jsonify({"error": "unauthorized"}), 403
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
    for col, typ in [('inventory', 'REAL'), ('continuing_income', 'REAL'),
                     ('weighted_shares', 'REAL'), ('eps_core', 'REAL'), ('eps_nonop', 'REAL')]:
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
               'net_income_parent','eps','contract_liability','inventory',
               'weighted_shares','eps_core','eps_nonop'] if col in existing_cols]
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


@app.route("/api/sync/rebuild-table", methods=["POST"])
def sync_rebuild_table():
    """重建表結構（修復主鍵等問題）"""
    if not check_sync_token():
        return jsonify({"status": "error", "msg": "unauthorized"}), 403
    table = request.json.get('table', '')
    create_sql = request.json.get('create_sql', '')
    if not table or not create_sql:
        return jsonify({"status": "error", "msg": "table and create_sql required"}), 400
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(f"DROP TABLE IF EXISTS {table}")
        c.execute(create_sql)
        conn.commit()
        conn.close()
        return jsonify({"status": "ok", "msg": f"{table} rebuilt"})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)}), 500

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
        'quarterly_financial', 'financial_annual',
        'stocks', 'stock_checklist',
        'daily_price', 'focus_tracking', 'focus_signals',
        'daily_notes', 'industry_news',
        'portfolios', 'portfolio_holdings',
        'investment_reports',
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


@app.route("/api/sync/table-merge-cleanup", methods=["POST"])
def sync_table_merge_cleanup():
    """
    合併式同步的清理：刪除本機已移除但 Render 仍存在的項目。
    只刪除「本機曾經有但已取消」的，保留「Render 獨有」的（前台操作的）。
    POST body: { "table": "user_lists", "pk": ["list_type","code"], "local_keys": [[...], ...] }
    """
    if not check_sync_token():
        return jsonify({"status": "error", "msg": "unauthorized"}), 403

    table = request.json.get('table', '').strip()
    pk = request.json.get('pk', [])
    local_keys = request.json.get('local_keys', [])

    ALLOWED_TABLES = {'user_lists', 'focus_tracking'}
    if table not in ALLOWED_TABLES:
        return jsonify({"status": "error", "msg": f"table '{table}' not allowed for merge cleanup"}), 400

    if not pk:
        return jsonify({"status": "ok", "deleted": 0})

    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        # 讀 Render 端現有的 pk 組合
        pk_str = ','.join(pk)
        render_rows = c.execute(f"SELECT {pk_str} FROM {table}").fetchall()
        local_key_set = set(tuple(k) for k in local_keys)

        deleted = 0
        for row in render_rows:
            key = tuple(str(v) for v in row)
            # 本機沒有這筆 → 本機已刪除，Render 也要刪
            # 但要區分：是本機刪的，還是 Render 獨有的（前台加的）
            # 策略：只刪除 list_type 在本機有出現過的（代表本機有管這個 type）
            if key not in local_key_set:
                # 檢查同 list_type 在本機是否有任何資料（代表本機有管）
                list_type = key[0] if len(key) > 0 else None
                local_has_type = any(k[0] == list_type for k in local_key_set)
                if local_has_type:
                    where_clause = ' AND '.join(f"{pk[i]}=?" for i in range(len(pk)))
                    c.execute(f"DELETE FROM {table} WHERE {where_clause}", list(row))
                    deleted += 1

        conn.commit()
        conn.close()
        return jsonify({"status": "ok", "deleted": deleted})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)}), 500


@app.route("/api/sync/clear-table", methods=["POST"])
def sync_clear_table():
    """清空指定資料表（同步前用，避免殘留已刪除的資料）"""
    if not check_sync_token():
        return jsonify({"status": "error", "msg": "unauthorized"}), 403
    table = request.json.get('table', '').strip()
    ALLOWED_TABLES = {
        'material_news', 'etf_holdings', 'etf_changes', 'etf_info',
        'user_lists', 'user_notes', 'user_estimates', 'focus_tracking', 'focus_signals',
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
        # eps_ytd, deepest_val_level, val_cheap_days, 衍生欄位
        for extra in ['eps_ytd', 'eps_ytd_label', 'deepest_val_level', 'val_cheap_days',
                       'shen_eps','shen_div','shen_pe','shen_yld','shen_grade',
                       'weighted_eps','weighted_div','weighted_pe','weighted_yld','weighted_grade','weighted_payout',
                       'blend_eps','blend_div','blend_pe','blend_yld','blend_grade',
                       'eps_4q_sum','trailing_div','trailing_pe','trailing_yld','trailing_grade',
                       'contract_chg',
                       'payout_1','payout_2','payout_3','payout_4','payout_5','payout_6',
                       'val_aa','val_a1','val_a2','val_a','val_lt6',
                       'val_eps_used','val_div_used','val_pe','val_yld','val_source',
                       'est_eps','est_div','est_pe','est_yld','est_grade',
                       'sys_pe','sys_yld','sys_grade',
                       'gb_roic','gb_ey','gb_roic_rank','gb_ey_rank','gb_total_rank']:
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
    # 確保新欄位存在
    for col, typ in [('eps_core', 'REAL'), ('eps_nonop', 'REAL')]:
        try: c.execute(f"ALTER TABLE financial_annual ADD COLUMN {col} {typ}")
        except Exception: pass
    try: conn.commit()
    except Exception: pass
    # 重新讀取欄位
    try:
        c.execute("SELECT * FROM financial_annual LIMIT 0")
        existing_cols = set(desc[0] for desc in c.description)
    except Exception: pass
    # 確保負債/現金/存貨/ROIC等欄位存在
    for col, typ in [('cash_and_equivalents','REAL'),('short_term_debt','REAL'),('short_term_notes','REAL'),
                     ('current_long_term_debt','REAL'),('long_term_bank_debt','REAL'),
                     ('other_long_term_debt','REAL'),('bonds_payable','REAL'),
                     ('inventory','REAL'),('contract_liability','REAL'),
                     ('current_liabilities','REAL'),('roic','REAL'),('nopat','REAL'),
                     ('invested_capital','REAL'),('fin_grade','TEXT'),
                     ('accounts_receivable','REAL'),('interest_expense','REAL'),
                     ('debt_ratio','REAL'),('fin_debt_ratio','REAL'),('interest_coverage','REAL'),
                     ('earnings_quality','REAL'),('fcf','REAL'),
                     ('inventory_days','REAL'),('ar_days','REAL')]:
        try: c.execute(f"ALTER TABLE financial_annual ADD COLUMN {col} {typ}")
        except Exception: pass
    try: conn.commit()
    except Exception: pass
    try:
        c.execute("SELECT * FROM financial_annual LIMIT 0")
        existing_cols = set(desc[0] for desc in c.description)
    except Exception: pass
    # 動態取所有欄位（排除 pk 和 updated_at），不用維護白名單
    fa_cols = [col for col in existing_cols if col not in ('code', 'year', 'updated_at')]
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

    max_annual_year = datetime.now().year - 1  # 年度上限：當年-1（排除未來年度）

    rows = query_db(
        "SELECT * FROM financial_annual WHERE code = ? AND year <= ? ORDER BY year DESC LIMIT 6",
        (code, max_annual_year)
    )

    # 快取過期 → 背景更新，先回傳現有資料
    cache_valid = False
    if rows:
        try:
            updated = datetime.strptime(rows[0]['updated_at'], '%Y-%m-%d %H:%M:%S')
            if datetime.now() - updated < timedelta(hours=24):
                cache_valid = True
        except Exception: pass

    is_cloud = IS_CLOUD
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
                "SELECT * FROM financial_annual WHERE code = ? AND year <= ? ORDER BY year DESC LIMIT 6",
                (code, max_annual_year)
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
        # ROIC / NOPAT / 投入資本 / 財務體質等級：從 DB 讀取（由 _refresh_fin_grades 統一計算）
        # d['roic'], d['nopat'], d['invested_capital'], d['fin_grade'] 已在 SELECT * 中
        # 盈餘品質率
        # 稅後淨利為負時不計算盈餘品質率（無意義）
        d['earnings_quality'] = round(ocf / ni * 100, 2) if ni and ni > 0 and ocf is not None else None
        # 負債比率
        _ta = d.get('total_assets')
        _te = d.get('total_equity')
        d['debt_ratio'] = round((_ta - _te) / _ta * 100, 2) if _ta and _ta > 0 and _te is not None else None
        # 長短期金融負債比
        _fin_debt = sum(d.get(f, 0) or 0 for f in
                        ['short_term_debt', 'short_term_notes', 'current_long_term_debt',
                         'long_term_bank_debt', 'other_long_term_debt', 'bonds_payable'])
        d['fin_debt_ratio'] = round(_fin_debt / _ta * 100, 2) if _ta and _ta > 0 and _fin_debt > 0 else (0.0 if _ta and _ta > 0 else None)
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
        # 每股盈餘-本業 / 業外：從 DB 讀取（由 _recalc_quarterly_derived 計算存入）
        # DB 為 NULL 時 fallback 即時算（新資料尚未跑過重算）
        if d.get('eps_core') is None and oi is not None and pti and pti != 0 and eps_val is not None:
            d['eps_core'] = round(oi / pti * eps_val, 2)
        if d.get('eps_nonop') is None:
            nop = d.get('non_operating')
            if nop is not None and pti and pti != 0 and eps_val is not None:
                d['eps_nonop'] = round(nop / pti * eps_val, 2)
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

    # 計算財務體質等級並寫入 stocks 表（使用 ROIC，無資料時 fallback ROE）
    # ROIC / NOPAT / 投入資本 / 等級 已由 _refresh_fin_grades() 統一算好存在 financial_annual
    # data 裡的 d['roic'], d['nopat'], d['invested_capital'], d['fin_grade'] 直接從 DB 讀取

    # 取得公司名稱
    stock_info = query_db("SELECT name, market FROM stocks WHERE code = ?", (code,))
    name = stock_info[0]['name'] if stock_info else code

    return jsonify({"code": code, "name": name, "data": data})


# ── 個股季度估計表 ──────────────────────────────────────────
@app.route("/api/stocks/<code>/quarterly")
def get_quarterly(code):
    from datetime import datetime, timedelta
    is_cloud = IS_CLOUD

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

        # 每股盈餘-本業 / 業外：從 DB 讀取，NULL 時 fallback 即時算
        if d.get('eps_core') is None and oi is not None and pti and pti != 0 and eps_val is not None:
            d['eps_core'] = round(oi / pti * eps_val, 2)
        if d.get('eps_nonop') is None:
            nop = d.get('non_operating')
            if nop is not None and pti and pti != 0 and eps_val is not None:
                d['eps_nonop'] = round(nop / pti * eps_val, 2)

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

    is_cloud = IS_CLOUD
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

    is_cloud = IS_CLOUD
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
    """資料概況：各表筆數、最後更新時間、資料新鮮度"""
    from datetime import datetime, timedelta
    env = 'Render' if IS_CLOUD else '本機'
    now = datetime.now()

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 各表筆數 + 最後更新時間
    table_info = [
        ('stocks',              '股票總表',   'updated_at'),
        ('quarterly_financial', '季度財報',   'updated_at'),
        ('financial_annual',    '年度財報',   'updated_at'),
        ('pe_history',          '歷史本益比', None),
        ('monthly_revenue',     '月營收',     'updated_at'),
        ('stock_state',         '評價快照',   'date'),
        ('material_news',       '新聞',       'created_at'),
        ('etf_holdings',        'ETF成分股',  None),
        ('user_lists',          '使用者清單', None),
        ('daily_price',         '每日收盤價', 'date'),
        ('stock_checklist',     '體質檢核表', None),
    ]
    tables = []
    for tbl, label, ts_col in table_info:
        try:
            cnt = c.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        except Exception:
            cnt = 0
        last_ts = None
        if ts_col and cnt > 0:
            try:
                r = c.execute(f"SELECT MAX({ts_col}) FROM {tbl}").fetchone()
                last_ts = r[0] if r else None
            except Exception:
                pass
        tables.append({'table': tbl, 'label': label, 'count': cnt, 'last_update': last_ts})

    # 資料新鮮度檢查
    freshness = []
    checks = [
        ("股價", "SELECT MAX(updated_at) FROM stocks WHERE close IS NOT NULL", 1),
        ("評價快照", "SELECT MAX(date) FROM stock_state", 1),
        ("每日收盤價", "SELECT MAX(date) FROM daily_price", 1),
        ("月營收", "SELECT MAX(year), MAX(month) FROM monthly_revenue WHERE year=(SELECT MAX(year) FROM monthly_revenue)", None),
        ("季報", "SELECT MAX(updated_at) FROM quarterly_financial", 7),
    ]
    for label, sql, max_days in checks:
        try:
            row = c.execute(sql).fetchone()
            if label == '月營收':
                yr, mo = row[0], row[1]
                val = f"{yr}-{mo:02d}" if yr and mo else None
            else:
                val = row[0] if row else None
        except Exception:
            val = None
        status = 'unknown'
        if val and max_days:
            try:
                ts = datetime.strptime(val[:10], '%Y-%m-%d')
                age_days = (now - ts).days
                status = 'ok' if age_days <= max_days else ('warn' if age_days <= max_days * 3 else 'error')
            except Exception:
                status = 'unknown'
        elif val and label == '月營收':
            # 月營收用 year-month 判斷：當月或上月算正常
            try:
                yr, mo = int(val[:4]), int(val[5:7])
                now_ym = now.year * 12 + now.month
                data_ym = yr * 12 + mo
                diff = now_ym - data_ym
                status = 'ok' if diff <= 1 else ('warn' if diff <= 3 else 'error')
            except Exception:
                status = 'unknown'
        freshness.append({'label': label, 'value': val, 'status': status})

    # 股價抽樣
    sample_codes = ['2330', '2317', '1101', '2454', '2881', '2618', '3008', '1301']
    prices = []
    for code in sample_codes:
        try:
            r = c.execute("SELECT code, name, close, updated_at FROM stocks WHERE code=?", (code,)).fetchone()
            if r:
                prices.append({'code': r[0], 'name': r[1], 'close': r[2], 'updated_at': r[3]})
        except Exception:
            pass

    conn.close()
    return jsonify({'env': env, 'tables': tables, 'freshness': freshness, 'prices': prices})


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
    with _cache_lock:
        if _gi_cache is not None and now - _gi_cache_time < 30:
            return app.response_class(_gi_cache, content_type='application/json')
    try:
        result = _calc_growth_indicators(_json, _dt)
        with _cache_lock:
            _gi_cache = result.get_data(as_text=True)  # 存 JSON 字串，不存 dict
            _gi_cache_time = now
        import gc; gc.collect()
        return result
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

def _calc_growth_indicators(_json, _dt):

    current_year = _dt.today().year

    # ── 1. 抓最近7年 financial_annual（需要6年算5年CAGR）
    min_year = current_year - 8  # 需要 8~9 年資料供平滑法使用
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
        "SELECT code, close, sys_ann_eps, sys_ann_div, sys_ann_pe, sys_ann_yld, blend_eps, blend_div FROM stocks"
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

    # ── 4a. 預載月營收（林區一致性用，60個月 YoY）
    _mr_min_year = current_year - 1911 - 6  # 民國年，多抓1年供 YoY 計算
    mr_rows = query_db(
        "SELECT code, year, month, revenue FROM monthly_revenue WHERE year >= ?",
        (_mr_min_year,)
    )
    mr_map = {}  # {code: {(year, month): revenue}}
    for r in mr_rows:
        mr_map.setdefault(r['code'], {})[(r['year'], r['month'])] = r['revenue']

    # ── 4b. 預載季EPS（林區一致性用，20季 YoY）
    _q_min_year = current_year - 6
    qf_rows = query_db(
        "SELECT code, quarter, eps FROM quarterly_financial WHERE quarter IS NOT NULL"
    )
    qf_map = {}  # {code: {quarter_str: eps}}
    for r in qf_rows:
        qf_map.setdefault(r['code'], {})[r['quarter']] = r.get('eps')

    # ── 5. 逐支計算
    result = {}
    for code, rows in fa_map.items():
        # 按年份排序
        rows.sort(key=lambda x: x['year'])
        st = st_map.get(code)
        if not st or not st.get('close'):
            continue
        close = st['close']

        # 端點法用 EPS > 0（避免負數算 CAGR）
        eps_pos = {r['year']: r['eps'] for r in rows
                   if r.get('eps') and r['eps'] > 0}
        # 平滑法用所有有 EPS 的年份（含虧損，平均化會抑制雜訊）
        eps_all = {r['year']: r['eps'] for r in rows
                   if r.get('eps') is not None}
        # valid list（EPS > 0）供林區等後續用
        valid = [(r['year'], r['net_income'], r['eps'], r.get('revenue')) for r in rows
                 if r.get('eps') and r['eps'] > 0]

        # 營收 CAGR（不受 EPS 虧損影響，在 EPS 判斷前計算）
        _all_revs = [(r['year'], r['revenue']) for r in rows if r.get('revenue') and r['revenue'] > 0]
        _all_revs.sort(key=lambda x: x[0])
        _rev_cagr_3y = None
        _rev_cagr_5y = None
        if len(_all_revs) >= 6:
            _rv_s, _rv_e = _all_revs[-6][1], _all_revs[-1][1]
            if _rv_s > 0 and _rv_e > 0:
                _rev_cagr_5y = ((_rv_e / _rv_s) ** (1.0 / 5) - 1) * 100
        elif len(_all_revs) >= 5:
            _rv_s, _rv_e = _all_revs[-5][1], _all_revs[-1][1]
            _n = _all_revs[-1][0] - _all_revs[-5][0]
            if _rv_s > 0 and _rv_e > 0 and _n >= 4:
                _rev_cagr_5y = ((_rv_e / _rv_s) ** (1.0 / _n) - 1) * 100
        if len(_all_revs) >= 4:
            _rv_s3, _rv_e3 = _all_revs[-4][1], _all_revs[-1][1]
            if _rv_s3 > 0 and _rv_e3 > 0:
                _rev_cagr_3y = ((_rv_e3 / _rv_s3) ** (1.0 / 3) - 1) * 100
        elif len(_all_revs) >= 3:
            _rv_s3, _rv_e3 = _all_revs[-3][1], _all_revs[-1][1]
            _n3 = _all_revs[-1][0] - _all_revs[-3][0]
            if _rv_s3 > 0 and _rv_e3 > 0 and _n3 >= 2:
                _rev_cagr_3y = ((_rv_e3 / _rv_s3) ** (1.0 / _n3) - 1) * 100

        if len(valid) < 4:
            # EPS 有效年份不足，但營收 CAGR 仍輸出
            result[code] = {
                'neff_a': None, 'neff_b': None, 'neff_3a': None, 'neff_3b': None,
                'neff_c': None, 'neff_d': None, 'intrinsic_growth': None,
                'lynch_a': None, 'lynch_b': None, 'lynch_c': None, 'lynch_d': None,
                'rev_cagr_3y': round(_rev_cagr_3y, 2) if _rev_cagr_3y is not None else None,
                'rev_cagr_5y': round(_rev_cagr_5y, 2) if _rev_cagr_5y is not None else None,
                'shares_change': None, 'yield': 0, 'pe': 0,
                'gray': True, 'neff_gray': True, 'lynch_gray': True, 'warnings': [],
            }
            continue

        years = [v[0] for v in valid]
        nis = [v[1] for v in valid]
        epss = [v[2] for v in valid]
        revs = [v[3] for v in valid]
        # latest_year 用全部資料的最新年（含虧損年）
        all_eps_years = sorted(eps_all.keys())
        latest_year = all_eps_years[-1] if all_eps_years else years[-1]

        # 保留逐年 YoY 供林區使用
        yoy_list = []
        for i in range(1, len(valid)):
            prev_eps = epss[i - 1]
            curr_eps = epss[i]
            if prev_eps and prev_eps > 0:
                yoy_list.append((curr_eps - prev_eps) / prev_eps)

        if not yoy_list:
            continue

        # ══ 四種成長率（固定 5 年 / 3 年）══

        # ── 5年端點 CAGR：最近EPS vs 5年前EPS，^(1/5)（只用 EPS>0）
        cagr_5y_endpoint = None
        y5 = latest_year - 5
        if latest_year in eps_pos and y5 in eps_pos:
            cagr_5y_endpoint = (eps_pos[latest_year] / eps_pos[y5]) ** (1.0 / 5) - 1

        # ── 5年平滑 CAGR：近3年均 vs 遠3年均，^(1/5)（含虧損年）
        # 遠端 = (Y-7, Y-6, Y-5) 中點 Y-6，近端 = (Y-2, Y-1, Y) 中點 Y-1，差5年
        cagr_5y_smooth = None
        far3 = [eps_all.get(latest_year - i) for i in [7, 6, 5]]
        near3 = [eps_all.get(latest_year - i) for i in [2, 1, 0]]
        if all(v is not None for v in far3) and all(v is not None for v in near3):
            far_avg = sum(far3) / 3
            near_avg = sum(near3) / 3
            if far_avg > 0 and near_avg > 0:
                cagr_5y_smooth = (near_avg / far_avg) ** (1.0 / 5) - 1
            elif far_avg > 0 and near_avg <= 0:
                cagr_5y_smooth = -1.0  # 近端平均虧損，成長率視為 -100%

        # ── 3年端點 CAGR：最近EPS vs 3年前EPS，^(1/3)（只用 EPS>0）
        cagr_3y_endpoint = None
        y3 = latest_year - 3
        if latest_year in eps_pos and y3 in eps_pos:
            cagr_3y_endpoint = (eps_pos[latest_year] / eps_pos[y3]) ** (1.0 / 3) - 1

        # ── 3年平滑 CAGR：近2年均 vs 遠2年均，^(1/3)（含虧損年）
        # 遠端 = (Y-4, Y-3) 中點 Y-3.5，近端 = (Y-1, Y) 中點 Y-0.5，差3年
        cagr_3y_smooth = None
        far2 = [eps_all.get(latest_year - i) for i in [4, 3]]
        near2 = [eps_all.get(latest_year - i) for i in [1, 0]]
        if all(v is not None for v in far2) and all(v is not None for v in near2):
            far2_avg = sum(far2) / 2
            near2_avg = sum(near2) / 2
            if far2_avg > 0 and near2_avg > 0:
                cagr_3y_smooth = (near2_avg / far2_avg) ** (1.0 / 3) - 1
            elif far2_avg > 0 and near2_avg <= 0:
                cagr_3y_smooth = -1.0

        # 使用前面已計算好的營收 CAGR
        rev_cagr_3y = _rev_cagr_3y
        rev_cagr_5y = _rev_cagr_5y

        # ══ 保守成長率 = min(四種方法中有值的) ══
        all_cagrs = [c for c in [cagr_5y_endpoint, cagr_5y_smooth, cagr_3y_endpoint, cagr_3y_smooth] if c is not None]
        if not all_cagrs:
            # EPS CAGR 全空，但營收 CAGR 可能有值，仍輸出
            result[code] = {
                'neff_a': None, 'neff_b': None, 'neff_3a': None, 'neff_3b': None,
                'neff_c': None, 'neff_d': None, 'intrinsic_growth': None,
                'lynch_a': None, 'lynch_b': None, 'lynch_c': None, 'lynch_d': None,
                'rev_cagr_3y': round(rev_cagr_3y, 2) if rev_cagr_3y is not None else None,
                'rev_cagr_5y': round(rev_cagr_5y, 2) if rev_cagr_5y is not None else None,
                'shares_change': None, 'yield': 0, 'pe': 0,
                'gray': True, 'neff_gray': True, 'lynch_gray': True, 'warnings': [],
            }
            continue

        neff_a = cagr_5y_endpoint   # 5年端點
        neff_b = cagr_5y_smooth     # 5年平滑
        neff_3a = cagr_3y_endpoint  # 3年端點
        neff_3b = cagr_3y_smooth    # 3年平滑

        a_pct = (neff_a * 100) if neff_a is not None else None
        b_pct = (neff_b * 100) if neff_b is not None else None
        a3_pct = (neff_3a * 100) if neff_3a is not None else None
        b3_pct = (neff_3b * 100) if neff_3b is not None else None

        all_pcts = [p for p in [a_pct, b_pct, a3_pct, b3_pct] if p is not None]
        neff_c = min(all_pcts)

        # ── 警示判斷
        warnings = []
        if 0 < neff_c < 7:
            warnings.append('成長率<7%，聶夫法參考用')
        # 5年兩種方法差距大
        if a_pct is not None and b_pct is not None:
            gap5 = abs(a_pct - b_pct)
            if gap5 > 5:
                warnings.append(f'5年端點vs平滑差距{gap5:.0f}%，成長穩定性存疑')
        # 3年 < 5年 = 減速
        best_5y = min([p for p in [a_pct, b_pct] if p is not None]) if any(p is not None for p in [a_pct, b_pct]) else None
        best_3y = min([p for p in [a3_pct, b3_pct] if p is not None]) if any(p is not None for p in [a3_pct, b3_pct]) else None
        if best_5y is not None and best_3y is not None and best_3y < best_5y - 3:
            warnings.append('近期成長減速')
        if neff_c > 20:
            warnings.append('保守成長率>20%，已封頂20%計算')
        # 中間有虧損年被跳過
        all_years = [r['year'] for r in rows]
        gap_years = len(all_years) - len(valid)
        if gap_years > 0:
            warnings.append(f'有{gap_years}年虧損被排除')

        if rev_cagr_5y is not None and a_pct is not None and a_pct > rev_cagr_5y * 1.5 and a_pct > 5:
            warnings.append('淨利成長遠快於營收，注意利潤率變化')

        # 股本變動率（輔助資訊）
        shares_change = None
        shares_list = [(r['year'], r.get('weighted_shares') or r.get('common_stock'))
                       for r in rows if r.get('weighted_shares') or r.get('common_stock')]
        if len(shares_list) >= 2:
            s_start = shares_list[0][1]
            s_end = shares_list[-1][1]
            if s_start and s_start > 0:
                shares_change = (s_end - s_start) / s_start * 100

        # ── 殖利率：預估 > min(沈董, 綜合)股利，與逍遙評價法一致
        ue = ue_map.get(code, {})
        div_val = None
        # 優先用 user_estimates 的預估股利
        vm_div = ue.get('vmDiv')
        if vm_div and str(vm_div).strip():
            try:
                div_val = float(vm_div)
            except Exception:
                pass
        # fallback: min(沈董股利, 綜合股利)
        if not div_val:
            _s_div = st.get('shen_div')
            _b_div = st.get('blend_div')
            _s_d = _s_div if _s_div and _s_div > 0 else None
            _b_d = _b_div if _b_div and _b_div > 0 else None
            if _s_d is not None and _b_d is not None:
                div_val = min(_s_d, _b_d)
            elif _s_d is not None:
                div_val = _s_d
            elif _b_d is not None:
                div_val = _b_d

        yld = (div_val / close * 100) if div_val and close > 0 else 0

        # ── PE：預估 > min(沈董, 綜合)，與逍遙評價法一致
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
        # fallback: min(沈董, 綜合)
        if pe is None:
            _s_eps = st.get('shen_eps')
            _b_eps = st.get('blend_eps')
            _s_pos = _s_eps if _s_eps and _s_eps > 0 else None
            _b_pos = _b_eps if _b_eps and _b_eps > 0 else None
            if _s_pos is not None and _b_pos is not None:
                _use_eps = min(_s_pos, _b_pos)
            elif _s_pos is not None:
                _use_eps = _s_pos
            elif _b_pos is not None:
                _use_eps = _b_pos
            else:
                _use_eps = None
            if _use_eps and _use_eps > 0:
                pe = close / _use_eps

        if pe is None or pe <= 0:
            # PE 無值，但營收 CAGR 和部分指標仍輸出
            result[code] = {
                'neff_a': round(a_pct, 2) if a_pct is not None else None,
                'neff_b': round(b_pct, 2) if b_pct is not None else None,
                'neff_3a': round(a3_pct, 2) if a3_pct is not None else None,
                'neff_3b': round(b3_pct, 2) if b3_pct is not None else None,
                'neff_c': neff_c, 'neff_d': None, 'intrinsic_growth': None,
                'lynch_a': round(a_pct, 2) if a_pct is not None else None,
                'lynch_b': None, 'lynch_c': None, 'lynch_d': None,
                'rev_cagr_3y': round(rev_cagr_3y, 2) if rev_cagr_3y is not None else None,
                'rev_cagr_5y': round(rev_cagr_5y, 2) if rev_cagr_5y is not None else None,
                'shares_change': None, 'yield': 0, 'pe': 0,
                'gray': True, 'neff_gray': True, 'lynch_gray': True, 'warnings': warnings,
            }
            continue

        # ── 林區：算術平均成長率（B）用稅後淨利（複用上面的 yoy_list）
        yoy_pct_list = [y * 100 for y in yoy_list]  # 轉成百分比
        recent_yoy = yoy_pct_list[-5:] if len(yoy_pct_list) >= 5 else yoy_pct_list
        lynch_b = sum(recent_yoy) / len(recent_yoy) if recent_yoy else None

        # ── 林區：成長一致性（C）— 新版：用月營收60筆 + 季EPS 20筆
        lynch_c = None
        _mr = mr_map.get(code, {})
        _qf = qf_map.get(code, {})

        # 月營收 YoY（最近60個月 vs 去年同月）
        _m_yoys = []
        _cur_roc = current_year - 1911  # 民國年
        for _dy in range(5):  # 往回5年
            for _dm in range(1, 13):
                _y = _cur_roc - _dy
                _rev_cur = _mr.get((_y, _dm))
                _rev_prev = _mr.get((_y - 1, _dm))
                if _rev_cur and _rev_prev and _rev_prev > 0:
                    _m_yoys.append((_rev_cur - _rev_prev) / _rev_prev * 100)

        # 季EPS YoY（最近20季 vs 去年同季）
        import re as _re_mod
        _q_yoys = []
        _all_qtrs = sorted(_qf.keys(),
                           key=lambda q: (int(q[:q.index('Q')]) if 'Q' in q else 0,
                                          int(q[q.index('Q')+1:]) if 'Q' in q else 0))
        for _qstr in _all_qtrs[-20:]:
            _m = _re_mod.match(r'(\d+)Q(\d+)', _qstr)
            if not _m:
                continue
            _qy, _qq = int(_m.group(1)), int(_m.group(2))
            _prev_qstr = f'{_qy - 1}Q{_qq}'
            _eps_cur = _qf.get(_qstr)
            _eps_prev = _qf.get(_prev_qstr)
            if _eps_cur is not None and _eps_prev is not None and _eps_prev > 0:
                _q_yoys.append((_eps_cur - _eps_prev) / _eps_prev * 100)

        # 計算一致性分數（0~1）
        if len(_m_yoys) >= 24:  # 至少2年月營收才有統計意義
            _pos_ratio = sum(1 for y in _m_yoys if y > 0) / len(_m_yoys)
            _std = (sum((y - sum(_m_yoys)/len(_m_yoys))**2 for y in _m_yoys) / len(_m_yoys)) ** 0.5
            # 最長連續負成長月數
            _max_neg = 0
            _cur_neg = 0
            for y in _m_yoys:
                if y <= 0:
                    _cur_neg += 1
                    _max_neg = max(_max_neg, _cur_neg)
                else:
                    _cur_neg = 0
            # 季EPS正成長比例
            _q_pos = sum(1 for y in _q_yoys if y > 0) / len(_q_yoys) if _q_yoys else 0.5

            lynch_c = (
                0.30 * _pos_ratio +
                0.25 * max(0, 1 - _std / 30) +
                0.25 * max(0, 1 - _max_neg / 12) +
                0.20 * _q_pos
            )
            lynch_c = round(lynch_c, 4)
        elif lynch_b is not None and a_pct is not None and a_pct > 0:
            # fallback：資料不足時用舊算法
            lynch_c = 1 - abs(lynch_b - a_pct) / a_pct
            if lynch_c < 0:
                lynch_c = 0

        # 一致性太低警示
        if lynch_c is not None and lynch_c < 0.5:
            warnings.append('景氣循環股不適用PEG')

        # ── 內生成長率（僅顯示用，不參與計算）
        intrinsic_growth = None
        roe_list = []
        payout_list = []
        for rr in rows:
            ni = rr.get('net_income')
            eq = rr.get('total_equity')
            e = rr.get('eps')
            cd = rr.get('cash_dividend')
            if ni and eq and eq > 0:
                roe_list.append(ni / eq)
            if e and e > 0 and cd is not None:
                payout_list.append(cd / e)
        if len(roe_list) >= 3 and len(payout_list) >= 3:
            avg_roe = sum(roe_list) / len(roe_list)
            avg_payout = sum(payout_list) / len(payout_list)
            if avg_payout < 1:
                intrinsic_growth = round(avg_roe * (1 - avg_payout) * 100, 2)

        # ── 保守成長率上限封頂
        neff_c = round(neff_c, 2)
        if neff_c > 20:
            neff_c = 20.0  # 超過20%以20%計
        neff_negative = neff_c <= 0
        neff_gray = neff_negative or gap_years >= 2

        # total return = 成長率 + 殖利率，> 0 就算（衰退但高殖利率仍有意義）
        total_return = neff_c + yld
        if total_return > 0 and pe > 0:
            neff_d = total_return / pe
            lynch_d = pe / total_return
        else:
            neff_d = None
            lynch_d = None

        # 林區：一致性太低 = 景氣循環股
        lynch_gray = (lynch_c is not None and lynch_c < 0.5)
        gray = neff_gray or lynch_gray

        entry = {
            'neff_a': round(a_pct, 2) if a_pct is not None else None,     # 5年端點
            'neff_b': round(b_pct, 2) if b_pct is not None else None,     # 5年平滑
            'neff_3a': round(a3_pct, 2) if a3_pct is not None else None,  # 3年端點
            'neff_3b': round(b3_pct, 2) if b3_pct is not None else None,  # 3年平滑
            'neff_c': neff_c,
            'neff_d': round(neff_d, 2) if neff_d is not None else None,
            'intrinsic_growth': intrinsic_growth,
            'lynch_a': round(a_pct, 2) if a_pct is not None else None,
            'lynch_b': round(lynch_b, 2) if lynch_b is not None else None,
            'lynch_c': round(lynch_c, 2) if lynch_c is not None else None,
            'lynch_d': round(lynch_d, 2) if lynch_d is not None else None,
            'rev_cagr_3y': round(rev_cagr_3y, 2) if rev_cagr_3y is not None else None,
            'rev_cagr_5y': round(rev_cagr_5y, 2) if rev_cagr_5y is not None else None,
            'shares_change': round(shares_change, 2) if shares_change is not None else None,
            'yield': round(yld, 2),
            'pe': round(pe, 2),
            'gray': gray,
            'neff_gray': neff_gray,
            'lynch_gray': lynch_gray,
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
    days = request.args.get("days")
    days = int(days) if days else None
    return jsonify(get_recent_news(code, tier, limit, days=days))

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

# ── 產業新聞 ──────────────────────────────────────────────────
def _init_industry_news_db():
    try:
        with sqlite3.get_conn() as conn:
            c = conn.cursor()
            c.execute("""CREATE TABLE IF NOT EXISTS industry_news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT, title TEXT, link TEXT, pub_time TEXT,
                summary TEXT, created_at TEXT, archived_code TEXT, archived_at TEXT
            )""")
            conn.commit()
    except Exception:
        pass

def fetch_industry_news():
    """抓取經濟日報 RSS + 工商時報產業新聞，存入 DB（標題去重）"""
    import xml.etree.ElementTree as ET
    import re
    _init_industry_news_db()

    import requests
    from datetime import datetime
    from email.utils import parsedate_to_datetime
    items = []
    _headers = {"User-Agent": "Mozilla/5.0"}

    def _parse_pub_time(pub_str):
        """RFC 2822 轉 YYYY-MM-DD HH:MM:SS"""
        if not pub_str:
            return ""
        try:
            dt = parsedate_to_datetime(pub_str)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            return pub_str

    # 經濟日報 RSS（產業 + 股市）
    udn_feeds = [
        ("https://money.udn.com/rssfeed/news/1001/5591", "經濟日報-產業"),
        ("https://money.udn.com/rssfeed/news/1001/5590", "經濟日報-股市"),
    ]
    for feed_url, source in udn_feeds:
        try:
            resp = requests.get(feed_url, headers=_headers, timeout=8)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
            for item in root.findall(".//item"):
                title = item.findtext("title", "").strip()
                link = item.findtext("link", "").strip()
                pub = item.findtext("pubDate", "").strip()
                desc = item.findtext("description", "").strip()
                if title:
                    items.append((source, title, link, _parse_pub_time(pub), desc[:100] if desc else ""))
        except Exception as e:
            logging.warning(f"抓取 {source} 失敗: {e}")

    # 工商時報 HTML
    try:
        resp = requests.get("https://www.chinatimes.com/newspapers/260110", headers=_headers, timeout=8)
        resp.raise_for_status()
        matches = re.findall(r'<h3[^>]*>\s*<a[^>]*href="(/newspapers/[^"]+)"[^>]*>([^<]+)</a>', resp.text)
        for path, title in matches:
            items.append(("工商時報-產業", title.strip(), f"https://www.chinatimes.com{path}", "", ""))
    except Exception as e:
        logging.warning(f"抓取工商時報失敗: {e}")

    if not items:
        return 0

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    inserted = 0
    for source, title, link, pub_time, summary in items:
        c.execute("SELECT id FROM industry_news WHERE title=? AND source=?", (title, source))
        if not c.fetchone():
            c.execute("INSERT INTO industry_news (source, title, link, pub_time, summary, created_at) VALUES (?,?,?,?,?,?)",
                      (source, title, link, pub_time, summary, now))
            inserted += 1
    conn.commit()
    conn.close()
    print(f"[產業新聞] 抓取 {len(items)} 則，新增 {inserted} 則")
    return inserted

def cleanup_old_industry_news(days=7):
    """清理超過 N 天且未歸檔的產業新聞"""
    from datetime import datetime, timedelta
    _init_industry_news_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM industry_news WHERE created_at < ? AND archived_code IS NULL", (cutoff,))
    deleted = c.rowcount
    conn.commit()
    conn.close()
    if deleted:
        print(f"[產業新聞] 清理 {deleted} 則過期新聞")

@app.route("/api/industry-news")
def api_industry_news():
    """讀取 DB 中的產業新聞（預設最近 7 天），自動比對股票名稱"""
    from datetime import datetime, timedelta
    _init_industry_news_db()
    days = int(request.args.get('days', '7'))
    since = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
    rows = query_db("""SELECT id, source, title, link, pub_time AS time, summary, created_at, archived_code
                       FROM industry_news
                       WHERE created_at >= ?
                       ORDER BY COALESCE(NULLIF(pub_time,''), created_at) DESC, id DESC""", (since,))
    result = [dict(r) for r in rows] if rows else []

    # 載入所有股票名稱做比對
    stock_rows = query_db("SELECT code, name FROM stocks")
    stock_map = {}
    if stock_rows:
        for sr in stock_rows:
            name = sr['name']
            if name and len(name) >= 2:
                stock_map[name] = sr['code']

    def _is_cjk(ch):
        return '\u4e00' <= ch <= '\u9fff'

    def _match_name(name, title):
        """比對股票名稱，2字名稱需至少一側為非中文字（避免誤判）"""
        if len(name) >= 3:
            return name in title
        idx = 0
        while True:
            pos = title.find(name, idx)
            if pos < 0:
                return False
            left_ok = pos == 0 or not _is_cjk(title[pos - 1])
            right_ok = (pos + len(name) >= len(title) or
                        not _is_cjk(title[pos + len(name)]))
            if left_ok or right_ok:
                return True
            idx = pos + 1

    for item in result:
        title = item.get('title', '')
        matched = []
        for name, code in stock_map.items():
            if _match_name(name, title):
                matched.append({'code': code, 'name': name})
        item['matched_stocks'] = matched

    return jsonify(result)

@app.route("/api/industry-news/refresh", methods=["POST"])
def refresh_industry_news():
    """手動觸發抓取產業新聞"""
    n = fetch_industry_news()
    return jsonify({"status": "ok", "inserted": n})

@app.route("/api/industry-news/<int:nid>/archive", methods=["POST"])
def archive_industry_news(nid):
    """歸檔產業新聞到個股筆記"""
    from datetime import datetime
    data = request.json or {}
    code = data.get('code', '').strip()
    if not code:
        return jsonify({"error": "需要股票代碼"}), 400

    _init_industry_news_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # 取得新聞內容
    c.execute("SELECT source, title, link, created_at FROM industry_news WHERE id=?", (nid,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "找不到該新聞"}), 404

    source, title, link, created_at = row
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    date_str = created_at[:10] if created_at else now[:10]

    # 標記已歸檔
    c.execute("UPDATE industry_news SET archived_code=?, archived_at=? WHERE id=?", (code, now, nid))

    # 寫入 user_notes.news_archive（不動 content，避免覆蓋質性研究筆記）
    c.execute("SELECT content, news_archive FROM user_notes WHERE code=?", (code,))
    existing = c.fetchone()
    note_line = f"[{date_str} {source}] {title}"
    if link:
        note_line += f"\n{link}"
    if existing:
        old_archive = existing[1] or ''
        new_archive = (old_archive + "\n\n" + note_line).strip() if old_archive.strip() else note_line
        c.execute("UPDATE user_notes SET news_archive=?, updated_at=? WHERE code=?",
                  (new_archive, now, code))
    else:
        c.execute("INSERT INTO user_notes (code, content, news_archive, updated_at) VALUES (?,?,?,?)",
                  (code, '', note_line, now))
    conn.commit()
    conn.close()

    _bg_push_table('user_notes', _USER_NOTES_COLS, ['code'], create_sql=_USER_NOTES_CREATE)
    return jsonify({"status": "ok", "code": code, "title": title})

# ── 前端首頁 ────────────────────────────────────────────────
@app.route("/")
def index():
    import time
    with open(os.path.join(app.static_folder, "index.html"), "r", encoding="utf-8") as f:
        html = f.read()
    # 注入版本戳記，強制瀏覽器不快取
    ver = int(time.time())
    html = html.replace('</head>', f'<meta name="v" content="{ver}">\n</head>', 1)
    resp = make_response(html)
    resp.headers['Content-Type'] = 'text/html; charset=utf-8'
    return resp

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
        # 建立查詢索引
        conn = sqlite3.connect()
        sqlite3.ensure_indexes(conn)
        conn.close()
        print("[DB] 初始化完成")
    except Exception as e:
        print(f"[DB] 初始化失敗（表格可能已存在）: {e}")

_init_all_db()

# ── 雲端新聞排程（Render 上每 60 分鐘抓新聞）────────────────
if os.environ.get('DATABASE_URL'):
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        _news_scheduler = BackgroundScheduler()

        def _cloud_fetch_news():
            """Render 上定期抓三種新聞（政府API+MoneyDJ+產業新聞，不需群益）"""
            try:
                from guardian import fetch_material_news, fetch_moneydj_news
                from industry_news_fetcher import fetch_industry_news
                r1 = fetch_material_news()
                print(f"[雲端新聞] 重大訊息: {r1.get('new', 0)} 則")
            except Exception as e:
                print(f"[雲端新聞] 重大訊息失敗: {e}")
            try:
                r2 = fetch_moneydj_news()
                print(f"[雲端新聞] MoneyDJ: {r2.get('new', 0)} 則")
            except Exception as e:
                print(f"[雲端新聞] MoneyDJ失敗: {e}")
            try:
                r3 = fetch_industry_news()
                print(f"[雲端新聞] 產業新聞: {r3} 則")
            except Exception as e:
                print(f"[雲端新聞] 產業新聞失敗: {e}")

        _news_scheduler.add_job(_cloud_fetch_news, 'interval', minutes=60, id='cloud_news',
                                next_run_time=None)  # 啟動後第一次由 cron 觸發
        # 每小時的第 5 分鐘跑（避免整點擁擠）
        _news_scheduler.add_job(_cloud_fetch_news, 'cron', minute=5, id='cloud_news_cron')
        _news_scheduler.start()
        print("[雲端新聞] 排程已啟動（每小時第 5 分鐘）")
    except Exception as e:
        print(f"[雲端新聞] 排程啟動失敗: {e}")

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
        news_archive TEXT,
        updated_at TEXT
    )""")
    # 確保 news_archive 欄位存在（舊表可能沒有）— 先 commit 避免 PG 死鎖
    conn.commit()
    try:
        c.execute("ALTER TABLE user_notes ADD COLUMN news_archive TEXT")
        conn.commit()
    except Exception:
        try: conn.rollback()
        except: pass
    # 質性研究結構化欄位
    for col in ['moat_strength', 'moat_source', 'structural_risk',
                'structural_risk_desc', 'growth_catalyst', 'confidence', 'lynch_override']:
        try:
            c.execute(f"ALTER TABLE user_notes ADD COLUMN {col} TEXT")
            conn.commit()
        except Exception:
            try: conn.rollback()
            except: pass
    # 清單異動歷史
    c.execute("""CREATE TABLE IF NOT EXISTS list_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT, list_type TEXT, action TEXT,
        reason TEXT, price_at REAL, timestamp TEXT)""")
    conn.commit()
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

try:
    _init_user_lists()
except Exception as e:
    print(f"[UserLists] DB 初始化失敗（不影響啟動）: {e}")

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
        # 記錄異動歷史
        try:
            c.execute("INSERT INTO list_history (code, list_type, action, reason, price_at, timestamp) VALUES (?,?,?,?,?,?)",
                      (code, list_type, 'add', data.get('reason', ''), price, now))
        except Exception: pass
    elif action == 'remove' and code:
        c.execute("DELETE FROM user_lists WHERE list_type=? AND code=?", (list_type, code))
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            c.execute("INSERT INTO list_history (code, list_type, action, reason, price_at, timestamp) VALUES (?,?,?,?,?,?)",
                      (code, list_type, 'remove', data.get('reason', ''), data.get('price'), now))
        except Exception: pass
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
    result = {}
    max_time = None
    for r in rows:
        result[r[0]] = r[1]
        t = r[2] or '2000-01-01'
        result[r[0] + '_time'] = t
        if max_time is None or t > max_time:
            max_time = t
    result['_updated_at'] = max_time
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
    # 背景重算所有衍生欄位（全域參數變更後門檻/等級需更新）
    if not IS_CLOUD:
        def _bg_recalc():
            global _stocks_cache_time, _global_settings_cache, _global_settings_time
            try:
                # 清快取讓 API 讀到新的全域設定
                with _cache_lock:
                    _stocks_cache_time = 0
                _global_settings_cache = None
                _global_settings_time = 0
                cnt = recalc_all_derived()
                print(f"[user-settings] recalc_all_derived 完成：{cnt} 支")
                _bg_push_table('stocks', ['code'] + DERIVED_COLS, 'code')
            except Exception as e:
                print(f"[user-settings] recalc 失敗: {e}")
        threading.Thread(target=_bg_recalc, daemon=True).start()
    return jsonify({"status": "ok"})


# ── 每日筆記 API ─────────────────────────────────────────────
def _init_daily_notes_db():
    try:
        with sqlite3.get_conn() as conn:
            c = conn.cursor()
            c.execute("""CREATE TABLE IF NOT EXISTS daily_notes (
                date TEXT PRIMARY KEY, content TEXT, created_at TEXT
            )""")
            conn.commit()
    except Exception:
        pass

@app.route("/api/daily-notes", methods=["GET"])
def get_daily_notes():
    """列出所有每日筆記，按日期倒序"""
    _init_daily_notes_db()
    rows = query_db("SELECT date, content, created_at FROM daily_notes ORDER BY date DESC")
    return jsonify([dict(r) for r in rows] if rows else [])

@app.route("/api/daily-notes", methods=["POST"])
def save_daily_note():
    """儲存今日筆記"""
    from datetime import datetime
    _init_daily_notes_db()
    data = request.json or {}
    content = data.get('content', '').strip()
    note_date = data.get('date') or datetime.now().strftime('%Y-%m-%d')
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if not content:
        return jsonify({"error": "內容不能為空"}), 400
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO daily_notes (date, content, created_at) VALUES (?,?,?)",
              (note_date, content, now))
    conn.commit()
    conn.close()
    _bg_push_table('daily_notes', ['date','content','created_at'], ['date'],
                   create_sql="""CREATE TABLE IF NOT EXISTS daily_notes (
                       date TEXT PRIMARY KEY, content TEXT, created_at TEXT)""")
    return jsonify({"status": "ok", "date": note_date})

@app.route("/api/daily-notes/<date>", methods=["DELETE"])
def delete_daily_note(date):
    """刪除指定日期的筆記"""
    _init_daily_notes_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM daily_notes WHERE date=?", (date,))
    deleted = c.rowcount
    conn.commit()
    conn.close()
    if deleted:
        _bg_push_table('daily_notes', ['date','content','created_at'], ['date'],
                       create_sql="""CREATE TABLE IF NOT EXISTS daily_notes (
                           date TEXT PRIMARY KEY, content TEXT, created_at TEXT)""")
    return jsonify({"status": "ok", "deleted": deleted})

@app.route("/api/user-notes/<code>", methods=["GET"])
def get_user_note(code):
    _meta_cols = ['moat_strength','moat_source','structural_risk',
                  'structural_risk_desc','growth_catalyst','confidence','lynch_override']
    try:
        rows = query_db(
            "SELECT content, news_archive, updated_at, " +
            ", ".join(_meta_cols) +
            " FROM user_notes WHERE code=?", (code,))
    except Exception:
        try:
            rows = query_db("SELECT content, news_archive, updated_at FROM user_notes WHERE code=?", (code,))
        except Exception:
            rows = query_db("SELECT content, updated_at FROM user_notes WHERE code=?", (code,))
            if rows:
                r = dict(rows[0])
                r['news_archive'] = ''
                return jsonify(r)
            return jsonify({"content": "", "news_archive": "", "updated_at": None})
    if rows:
        return jsonify(rows[0])
    result = {"content": "", "news_archive": "", "updated_at": None}
    for c in _meta_cols:
        result[c] = None
    return jsonify(result)

@app.route("/api/user-notes/<code>", methods=["POST"])
def save_user_note(code):

    from datetime import datetime
    content = request.json.get('content', '')
    meta = request.json.get('meta')  # optional structured fields
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if content.strip():
        if meta:
            # 保留 news_archive：先讀現有值
            c.execute("SELECT news_archive FROM user_notes WHERE code=?", (code,))
            existing = c.fetchone()
            news_archive = existing[0] if existing else ''
            c.execute(
                "INSERT OR REPLACE INTO user_notes "
                "(code, content, news_archive, updated_at, "
                "moat_strength, moat_source, structural_risk, structural_risk_desc, "
                "growth_catalyst, confidence, lynch_override) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (code, content, news_archive, now,
                 meta.get('moat_strength'), meta.get('moat_source'),
                 meta.get('structural_risk'), meta.get('structural_risk_desc'),
                 meta.get('growth_catalyst'), meta.get('confidence'),
                 meta.get('lynch_override')))
        else:
            # 向下相容：只更新 content，保留其他欄位
            c.execute("SELECT 1 FROM user_notes WHERE code=?", (code,))
            if c.fetchone():
                c.execute("UPDATE user_notes SET content=?, updated_at=? WHERE code=?",
                          (content, now, code))
            else:
                c.execute("INSERT INTO user_notes (code, content, updated_at) VALUES (?,?,?)",
                          (code, content, now))
    else:
        c.execute("DELETE FROM user_notes WHERE code=?", (code,))
    conn.commit()
    conn.close()
    _bg_push_table('user_notes', _USER_NOTES_COLS, ['code'], create_sql=_USER_NOTES_CREATE)
    return jsonify({"status": "ok"})

# ── 投資報告書 API ──────────────────────────────────────────
def _init_investment_reports():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS investment_reports (
        code TEXT PRIMARY KEY, content TEXT, updated_at TEXT)""")
    conn.commit()
    # 報告快照欄位（保鮮機制）
    for col, typ in [('snapshot_price', 'REAL'), ('snapshot_grade', 'TEXT'),
                     ('snapshot_eps', 'REAL'), ('snapshot_judgment', 'TEXT')]:
        try:
            c.execute(f"ALTER TABLE investment_reports ADD COLUMN {col} {typ}")
            conn.commit()
        except Exception:
            try: conn.rollback()
            except: pass
    conn.close()

@app.route("/api/investment-report/<code>", methods=["GET"])
def get_investment_report(code):
    _init_investment_reports()
    try:
        rows = query_db(
            "SELECT content, updated_at, snapshot_price, snapshot_grade, snapshot_eps, snapshot_judgment "
            "FROM investment_reports WHERE code=?", (code,))
    except Exception:
        rows = query_db("SELECT content, updated_at FROM investment_reports WHERE code=?", (code,))
    if rows:
        return jsonify(rows[0])
    return jsonify({"content": "", "updated_at": None})

@app.route("/api/investment-report/<code>", methods=["POST"])
def save_investment_report(code):
    _init_investment_reports()
    from datetime import datetime
    content = request.json.get('content', '')
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if content.strip():
        c.execute("INSERT OR REPLACE INTO investment_reports (code, content, updated_at) VALUES (?,?,?)",
                  (code, content, now))
    else:
        c.execute("DELETE FROM investment_reports WHERE code=?", (code,))
    conn.commit()
    conn.close()
    _bg_push_table('investment_reports', _REPORT_COLS, ['code'], create_sql=_REPORT_CREATE)
    return jsonify({"status": "ok"})

# ── 選股推薦 API（複用 investment_reports 表，key=_stock_picks）──
@app.route("/api/stock-picks", methods=["GET"])
def get_stock_picks():
    _init_investment_reports()
    rows = query_db("SELECT content, updated_at FROM investment_reports WHERE code='_stock_picks'")
    if rows:
        return jsonify(rows[0])
    return jsonify({"content": "", "updated_at": None})

@app.route("/api/stock-picks", methods=["POST"])
def save_stock_picks():
    _init_investment_reports()
    from datetime import datetime
    content = request.json.get('content', '')
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if content.strip():
        c.execute("INSERT OR REPLACE INTO investment_reports (code, content, updated_at) VALUES (?,?,?)",
                  ('_stock_picks', content, now))
    else:
        c.execute("DELETE FROM investment_reports WHERE code='_stock_picks'")
    conn.commit()
    conn.close()
    _bg_push_table('investment_reports', _REPORT_COLS, ['code'], create_sql=_REPORT_CREATE)
    return jsonify({"status": "ok"})

# ── 報告快照 API ────────────────────────────────────────────
@app.route("/api/report-snapshot/<code>", methods=["POST"])
def save_report_snapshot(code):
    """儲存投資報告書產出時的股價/等級/EPS/結論，用於保鮮比對"""
    _init_investment_reports()
    from datetime import datetime
    data = request.json or {}
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1 FROM investment_reports WHERE code=?", (code,))
    if c.fetchone():
        c.execute(
            "UPDATE investment_reports SET snapshot_price=?, snapshot_grade=?, "
            "snapshot_eps=?, snapshot_judgment=? WHERE code=?",
            (data.get('price'), data.get('grade'), data.get('eps'),
             data.get('judgment'), code))
    else:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        c.execute(
            "INSERT INTO investment_reports (code, content, updated_at, "
            "snapshot_price, snapshot_grade, snapshot_eps, snapshot_judgment) "
            "VALUES (?,?,?,?,?,?,?)",
            (code, '', now, data.get('price'), data.get('grade'),
             data.get('eps'), data.get('judgment')))
    conn.commit()
    conn.close()
    _bg_push_table('investment_reports', _REPORT_COLS, ['code'], create_sql=_REPORT_CREATE)
    return jsonify({"status": "ok"})

# ── 筆記摘要 API（總表用）──────────────────────────────────
@app.route("/api/notes-summary")
def get_notes_summary():
    """回傳所有有筆記的股票摘要，供總表顯示覆蓋率徽章"""
    try:
        rows = query_db(
            "SELECT code, updated_at, moat_strength, confidence "
            "FROM user_notes WHERE content IS NOT NULL AND content != ''")
    except Exception:
        rows = query_db(
            "SELECT code, updated_at FROM user_notes "
            "WHERE content IS NOT NULL AND content != ''")
    # 也查有報告的股票
    try:
        report_rows = query_db(
            "SELECT code, updated_at as report_date, snapshot_price, snapshot_judgment "
            "FROM investment_reports WHERE content IS NOT NULL AND content != '' AND code != '_stock_picks'")
    except Exception:
        report_rows = query_db(
            "SELECT code, updated_at as report_date "
            "FROM investment_reports WHERE content IS NOT NULL AND content != '' AND code != '_stock_picks'")
    report_map = {}
    for r in report_rows:
        report_map[r['code']] = dict(r)
    result = {}
    for r in rows:
        d = dict(r)
        rpt = report_map.pop(r['code'], None)
        if rpt:
            d['report_date'] = rpt.get('report_date')
            d['snapshot_price'] = rpt.get('snapshot_price')
            d['snapshot_judgment'] = rpt.get('snapshot_judgment')
        result[r['code']] = d
    # 有報告但沒筆記的也列入
    for code, rpt in report_map.items():
        result[code] = {
            'code': code, 'updated_at': None,
            'report_date': rpt.get('report_date'),
            'snapshot_price': rpt.get('snapshot_price'),
            'snapshot_judgment': rpt.get('snapshot_judgment'),
        }
    return jsonify(result)

# ── 清單異動歷史 API ───────────────────────────────────────
@app.route("/api/list-history/<code>")
def get_list_history(code):
    rows = query_db(
        "SELECT list_type, action, reason, price_at, timestamp "
        "FROM list_history WHERE code=? ORDER BY timestamp DESC LIMIT 50",
        (code,))
    return jsonify(rows)

# ── 歷史決策回顧 API ──────────────────────────────────────
@app.route("/api/review-data")
def get_review_data():
    """回傳所有有報告快照的股票 vs 現況"""
    try:
        reports = query_db(
            "SELECT code, updated_at, snapshot_price, snapshot_grade, "
            "snapshot_eps, snapshot_judgment "
            "FROM investment_reports "
            "WHERE snapshot_price IS NOT NULL AND code != '_stock_picks'")
    except Exception:
        return jsonify([])
    if not reports:
        return jsonify([])
    # 取得現在股價
    prices = {}
    for r in query_db("SELECT code, close, val_level FROM stocks"):
        prices[r['code']] = {'close': r.get('close'), 'val_level': r.get('val_level')}
    result = []
    for r in reports:
        d = dict(r)
        cur = prices.get(r['code'], {})
        d['current_price'] = cur.get('close')
        d['current_grade'] = cur.get('val_level')
        if r.get('snapshot_price') and cur.get('close'):
            d['price_change_pct'] = round(
                (cur['close'] - r['snapshot_price']) / r['snapshot_price'] * 100, 1)
        else:
            d['price_change_pct'] = None
        # 取得股票名稱
        name_rows = query_db("SELECT name FROM stocks WHERE code=?", (r['code'],))
        d['name'] = name_rows[0]['name'] if name_rows else ''
        # 取得筆記摘要
        try:
            note_rows = query_db(
                "SELECT moat_strength, confidence FROM user_notes WHERE code=?", (r['code'],))
            if note_rows:
                d['moat_strength'] = note_rows[0].get('moat_strength')
                d['confidence'] = note_rows[0].get('confidence')
        except Exception:
            pass
        result.append(d)
    return jsonify(result)

# ── 歷史決策回顧頁面 ──────────────────────────────────────
@app.route("/review.html")
def review_page():
    return send_from_directory('.', 'review.html')

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
        if r.get('borderline'):
            try: r['_borderline'] = json.loads(r['borderline'])
            except Exception: r['_borderline'] = {}
        else:
            r['_borderline'] = {}
        if r.get('red_flags'):
            try: r['_red_flags'] = json.loads(r['red_flags'])
            except Exception: r['_red_flags'] = []
        else:
            r['_red_flags'] = []
        # 附帶檢核項目定義，前端動態渲染用
        r['_items'] = CHECKLIST_ITEMS
        return jsonify(r)
    return jsonify({'_items': CHECKLIST_ITEMS})

@app.route("/api/checklist/refresh", methods=["POST"])
def refresh_checklist():
    """手動觸發重算所有檢核表"""
    count = calc_all_checklists()
    return jsonify({"status": "ok", "count": count})

def _valuation_history_removed():  # noqa
    """(已停用) 估值回測。"""
    return  # 以下為停用的程式碼
    """
    """
    import json as _json_vh
    from datetime import datetime as _dt_vh, timedelta

    # 1. 讀取歷史股價
    daily_rows = query_db(
        "SELECT date, close_price FROM daily_price WHERE code=? ORDER BY date",
        (code,))
    if not daily_rows:
        return jsonify({'error': 'no_daily_price', 'message': '無歷史股價資料'})

    # 2. 讀取季度 EPS（按年季排序）
    qf_rows = query_db(
        "SELECT quarter, eps FROM quarterly_financial WHERE code=? AND eps IS NOT NULL",
        (code,))
    # 解析並排序
    qf_list = []
    for r in qf_rows:
        parts = r['quarter'].split('Q')
        if len(parts) == 2:
            y, q = int(parts[0]), int(parts[1])
            qf_list.append((y, q, r['eps']))
    qf_list.sort(key=lambda x: (x[0], x[1]))

    # 3. 建立「每個時間點的滾動四季 EPS」查找表
    #    每季公告後 EPS 才更新，估算各季公告截止日期：
    #    Q1 → 5/15, Q2 → 8/14, Q3 → 11/14, Q4 → 3/31（隔年）
    eps_timeline = []  # [(available_date, rolling_4q_eps), ...]
    for i in range(3, len(qf_list)):
        y, q, _ = qf_list[i]
        rolling_eps = sum(qf_list[j][2] for j in range(i-3, i+1))
        # 估算此季報何時公告
        if q == 1:
            avail = f"{y + 1911}-05-15"
        elif q == 2:
            avail = f"{y + 1911}-08-14"
        elif q == 3:
            avail = f"{y + 1911}-11-14"
        else:  # Q4
            avail = f"{y + 1912}-03-31"
        eps_timeline.append((avail, round(rolling_eps, 2)))

    if not eps_timeline:
        return jsonify({'error': 'insufficient_data', 'message': '季報資料不足'})

    # 4. 讀取個股估值參數（PE/殖利率）
    gs = _get_global_settings()
    user_params = None
    try:
        ue_rows = query_db("SELECT params FROM user_estimates WHERE code=?", (code,))
        if ue_rows and ue_rows[0]['params']:
            user_params = _json_vh.loads(ue_rows[0]['params'])
    except Exception:
        pass
    pe_hi, pe_lo, y_high, y_max = _get_stock_params(user_params, gs)

    # 5. 讀取股利歷史（用來算殖利率門檻）
    div_rows = query_db(
        "SELECT year, cash_dividend FROM financial_annual WHERE code=? AND cash_dividend IS NOT NULL AND cash_dividend > 0 ORDER BY year",
        (code,))
    div_map = {r['year']: r['cash_dividend'] for r in div_rows}

    # 6. 對每個交易日計算評價等級
    daily_data = []   # 前端畫圖用
    level_periods = [] # 統計用：各便宜區間

    def _get_eps_at_date(d_str):
        """取得某日期時已知的滾動四季 EPS"""
        eps = None
        for avail, e in eps_timeline:
            if avail <= d_str:
                eps = e
        return eps

    def _get_div_at_date(d_str):
        """取得某日期時最近的股利"""
        year = int(d_str[:4])
        roc = year - 1911
        for y in range(roc, roc - 3, -1):
            if y in div_map:
                return div_map[y]
        return None

    def _calc_levels(eps, div):
        """計算 AA/A1/A2/A 門檻"""
        if not eps or eps <= 0:
            return None, None, None, None
        vals = []
        # AA = min(EPS×pe_lo, div/y_max)
        v_pe_aa = eps * pe_lo
        v_pe_a = eps * ((pe_lo + pe_hi) / 2 - (pe_hi - pe_lo) / 4)  # 偏低PE=12
        pe_mid_low = pe_lo + (pe_hi - pe_lo) / 3  # ~12.67
        parts = [v_pe_aa]
        if div and div > 0:
            parts.append(div / (y_max / 100))
        val_aa = min(parts)

        parts_a1 = [v_pe_aa]
        if div and div > 0:
            parts_a1.append(div / (y_high / 100))
        val_a1 = min(parts_a1)

        pe_low_mid = (pe_lo + (pe_hi + pe_lo) / 2) / 2  # 偏低PE
        parts_a2 = [eps * pe_low_mid]
        if div and div > 0:
            parts_a2.append(div / (y_max / 100))
        val_a2 = min(parts_a2)

        parts_a = [eps * pe_low_mid]
        if div and div > 0:
            parts_a.append(div / (y_high / 100))
        val_a = min(parts_a)

        return round(val_aa, 2), round(val_a1, 2), round(val_a2, 2), round(val_a, 2)

    LEVELS = ['AA', 'A1', 'A2', 'A']
    prev_level = None
    current_period_start = None
    price_map = {}  # date → close

    for row in daily_rows:
        d = row['date']
        price = row['close_price']
        price_map[d] = price
        eps = _get_eps_at_date(d)
        div = _get_div_at_date(d)
        val_aa, val_a1, val_a2, val_a = _calc_levels(eps, div)

        level = None
        tol = 0.005
        if val_aa and price <= val_aa + tol:
            level = 'AA'
        elif val_a1 and price <= val_a1 + tol:
            level = 'A1'
        elif val_a2 and price <= val_a2 + tol:
            level = 'A2'
        elif val_a and price <= val_a + tol:
            level = 'A'

        daily_data.append({
            'd': d, 'p': price,
            'aa': val_aa, 'a1': val_a1, 'a2': val_a2, 'a': val_a,
            'lv': level, 'eps': eps,
        })

        # 追蹤便宜區間
        if level and level != prev_level:
            if current_period_start and prev_level:
                level_periods.append({
                    'level': prev_level,
                    'start': current_period_start,
                    'end': d,
                })
            current_period_start = d
        elif not level and prev_level:
            if current_period_start:
                level_periods.append({
                    'level': prev_level,
                    'start': current_period_start,
                    'end': d,
                })
            current_period_start = None
        prev_level = level

    # 收尾
    if prev_level and current_period_start:
        level_periods.append({
            'level': prev_level,
            'start': current_period_start,
            'end': daily_rows[-1]['date'],
        })

    # 7. 統計各等級
    stats = {}
    for lv in LEVELS:
        periods = [p for p in level_periods if p['level'] == lv]
        total_days = 0
        returns_90d = []
        returns_180d = []
        for p in periods:
            # 計算天數
            start_dt = _dt_vh.strptime(p['start'], '%Y-%m-%d')
            end_dt = _dt_vh.strptime(p['end'], '%Y-%m-%d')
            days = (end_dt - start_dt).days
            total_days += max(days, 1)

            # 計算進入後 90/180 天報酬
            entry_price = price_map.get(p['start'])
            if entry_price:
                d90 = (start_dt + timedelta(days=90)).strftime('%Y-%m-%d')
                d180 = (start_dt + timedelta(days=180)).strftime('%Y-%m-%d')
                # 找最近的交易日
                for dd, pp in price_map.items():
                    if dd >= d90:
                        returns_90d.append(round((pp - entry_price) / entry_price * 100, 1))
                        break
                for dd, pp in price_map.items():
                    if dd >= d180:
                        returns_180d.append(round((pp - entry_price) / entry_price * 100, 1))
                        break

        stats[lv] = {
            'count': len(periods),
            'total_days': total_days,
            'avg_days': round(total_days / len(periods)) if periods else 0,
            'avg_return_90d': round(sum(returns_90d) / len(returns_90d), 1) if returns_90d else None,
            'avg_return_180d': round(sum(returns_180d) / len(returns_180d), 1) if returns_180d else None,
        }

    return jsonify({
        'code': code,
        'daily': daily_data,
        'stats': stats,
        'pe_params': {'pe_hi': pe_hi, 'pe_lo': pe_lo, 'y_high': y_high, 'y_max': y_max},
        'data_range': {
            'start': daily_rows[0]['date'] if daily_rows else None,
            'end': daily_rows[-1]['date'] if daily_rows else None,
            'days': len(daily_rows),
        },
    })


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
    # 即時重算該股衍生欄位（評價門檻等）+ 檢核表
    try: recalc_all_derived(codes=[code])
    except Exception: pass
    # Render 不跑 checklist 重算（financial_annual 欄位不完整會算出 null，蓋掉本機推過去的正確資料）
    if not os.environ.get('DATABASE_URL'):
        try: _recalc_checklist_single(code)
        except Exception: pass
    # 拍全量快照（更新便宜清單）+ 推 Render
    if not os.environ.get('DATABASE_URL'):
        def _snapshot_and_push():
            try:
                from guardian import snapshot_stock_states
                snapshot_stock_states()
            except Exception as e:
                print(f"[快照] 儲存後快照失敗: {e}")
            try:
                from render_sync import _push_annual_to_render
                _push_annual_to_render()
            except Exception as e:
                print(f"[Push] 儲存後同步失敗: {e}")
        threading.Thread(target=_snapshot_and_push, daemon=True).start()
    return jsonify({"status": "ok"})

# ── 持股專區 ────────────────────────────────────────────────
import hashlib, secrets, functools, time, json
from datetime import datetime

def _init_portfolio_db():
    # 建表（用完立即 commit + close，避免 PostgreSQL 鎖衝突）
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS portfolios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        portfolio_type TEXT DEFAULT 'personal',
        dividend_condition TEXT,
        dividend_ratio REAL,
        interest_rate REAL DEFAULT 0,
        invested_capital REAL DEFAULT 0,
        cash_balance REAL DEFAULT 0,
        sort_order INTEGER DEFAULT 0,
        accounts TEXT DEFAULT '[]',
        notes TEXT DEFAULT '',
        created_at TEXT,
        updated_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS portfolio_holdings (
        portfolio_id INTEGER NOT NULL,
        stock_code TEXT NOT NULL,
        account TEXT NOT NULL DEFAULT '',
        shares_lot REAL DEFAULT 0,
        added_at TEXT,
        updated_at TEXT,
        PRIMARY KEY (portfolio_id, stock_code, account)
    )""")
    try: conn.commit()
    except Exception: pass
    conn.close()
    # 補欄位（PostgreSQL 用 ADD COLUMN IF NOT EXISTS 避免錯誤）
    is_pg = bool(os.environ.get('DATABASE_URL'))
    if is_pg:
        try:
            import psycopg2
            from db import DATABASE_URL as PG_URL
            pg_conn = psycopg2.connect(PG_URL)
            pg_conn.autocommit = True  # 每條 DDL 自動 commit，不持鎖
            cur = pg_conn.cursor()
            for col, typ in [
                ('interest_rate', 'REAL DEFAULT 0'),
                ('portfolio_type', "TEXT DEFAULT 'personal'"),
                ('accounts', "TEXT DEFAULT '[]'"),
                ('notes', "TEXT DEFAULT ''"),
            ]:
                cur.execute(f"ALTER TABLE portfolios ADD COLUMN IF NOT EXISTS {col} {typ}")
            # portfolio_holdings 補 account
            try:
                cur.execute("SELECT account FROM portfolio_holdings LIMIT 1")
            except Exception:
                pg_conn.rollback()
            # 修正 SERIAL 序列號（push 資料帶明確 id，序列不會自動更新）
            for tbl in ['portfolios']:
                try:
                    cur.execute(f"SELECT setval(pg_get_serial_sequence('{tbl}', 'id'), COALESCE((SELECT MAX(id) FROM {tbl}), 0) + 1, false)")
                except Exception:
                    pg_conn.rollback()
            pg_conn.close()
        except Exception as e:
            print(f"[Portfolio] PG 補欄位失敗: {e}")
    else:
        for col_sql in [
            "ALTER TABLE portfolios ADD COLUMN interest_rate REAL DEFAULT 0",
            "ALTER TABLE portfolios ADD COLUMN portfolio_type TEXT DEFAULT 'personal'",
            "ALTER TABLE portfolios ADD COLUMN accounts TEXT DEFAULT '[]'",
            "ALTER TABLE portfolios ADD COLUMN notes TEXT DEFAULT ''",
        ]:
            try:
                cn = sqlite3.connect(DB_PATH)
                cn.cursor().execute(col_sql)
                cn.commit()
                cn.close()
            except Exception:
                try: cn.close()
                except: pass
        # portfolio_holdings 補 account 欄位 + 修正主鍵
        try:
            cn = sqlite3.connect(DB_PATH)
            cn.cursor().execute("SELECT account FROM portfolio_holdings LIMIT 1")
            cn.close()
        except Exception:
            try: cn.close()
            except: pass
            try:
                cn = sqlite3.connect(DB_PATH)
                cc = cn.cursor()
                cc.execute("ALTER TABLE portfolio_holdings RENAME TO portfolio_holdings_old")
                cc.execute("""CREATE TABLE portfolio_holdings (
                    portfolio_id INTEGER NOT NULL, stock_code TEXT NOT NULL,
                    account TEXT NOT NULL DEFAULT '', shares_lot REAL DEFAULT 0,
                    added_at TEXT, updated_at TEXT,
                    PRIMARY KEY (portfolio_id, stock_code, account))""")
                cc.execute("""INSERT INTO portfolio_holdings (portfolio_id, stock_code, account, shares_lot, added_at, updated_at)
                              SELECT portfolio_id, stock_code, '', shares_lot, added_at, updated_at FROM portfolio_holdings_old""")
                cc.execute("DROP TABLE portfolio_holdings_old")
                cn.commit()
                cn.close()
            except Exception:
                try: cn.close()
                except: pass

try:
    _init_portfolio_db()
except Exception as e:
    print(f"[Portfolio] DB 初始化失敗（不影響啟動）: {e}")

# 密碼驗證（token 存 DB，重啟不失效）
def _hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

def _check_token_db(token):
    """從 DB 檢查 token 是否有效"""
    if not token:
        return False
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT value FROM user_settings WHERE key='portfolio_token'")
        row = c.fetchone()
        c.execute("SELECT value FROM user_settings WHERE key='portfolio_token_expires'")
        exp_row = c.fetchone()
        conn.close()
        if not row or row[0] != token:
            return False
        if exp_row and float(exp_row[0]) < time.time():
            return False
        return True
    except Exception:
        return False

def _save_token_db(token, expires):
    """將 token 存入 DB"""
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO user_settings (key, value, updated_at) VALUES (?,?,?)",
                  ('portfolio_token', token, now))
        c.execute("INSERT OR REPLACE INTO user_settings (key, value, updated_at) VALUES (?,?,?)",
                  ('portfolio_token_expires', str(expires), now))
        conn.commit()
        conn.close()
    except Exception:
        pass

def require_portfolio_auth(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        token = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not _check_token_db(token):
            return jsonify({"error": "unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapper

@app.route("/api/portfolio/auth", methods=["POST"])
def portfolio_auth():
    data = request.get_json() or {}
    pw = data.get('password', '')
    if not pw:
        return jsonify({"error": "password required"}), 400

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT value FROM user_settings WHERE key='portfolio_password'")
    row = c.fetchone()
    conn.close()

    pw_hash = _hash_pw(pw)
    if row is None:
        # 首次設定密碼
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO user_settings (key, value, updated_at) VALUES (?,?,?)",
                  ('portfolio_password', pw_hash, now))
        conn.commit()
        conn.close()
        _bg_push_table('user_settings', ['key','value','updated_at'], ['key'],
                       "CREATE TABLE IF NOT EXISTS user_settings (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)")
    elif row[0] != pw_hash:
        return jsonify({"error": "wrong password"}), 401

    token = secrets.token_hex(32)
    expires = time.time() + 86400
    _save_token_db(token, expires)
    return jsonify({"token": token, "expires_in": 86400})

@app.route("/api/portfolio/set-password", methods=["POST"])
@require_portfolio_auth
def portfolio_set_password():
    data = request.get_json() or {}
    old_pw = data.get('old_password', '')
    new_pw = data.get('new_password', '')
    if not new_pw:
        return jsonify({"error": "new_password required"}), 400

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT value FROM user_settings WHERE key='portfolio_password'")
    row = c.fetchone()
    if row and row[0] != _hash_pw(old_pw):
        conn.close()
        return jsonify({"error": "wrong old password"}), 401

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    c.execute("INSERT OR REPLACE INTO user_settings (key, value, updated_at) VALUES (?,?,?)",
              ('portfolio_password', _hash_pw(new_pw), now))
    conn.commit()
    conn.close()
    _bg_push_table('user_settings', ['key','value','updated_at'], ['key'],
                   "CREATE TABLE IF NOT EXISTS user_settings (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)")
    return jsonify({"status": "ok"})

def _push_portfolios():
    _bg_push_table('portfolios',
        ['id','name','portfolio_type','dividend_condition','dividend_ratio','interest_rate','invested_capital','cash_balance','sort_order','accounts','notes','created_at','updated_at'],
        ['id'],
        """CREATE TABLE IF NOT EXISTS portfolios (
            id SERIAL PRIMARY KEY, name TEXT NOT NULL, portfolio_type TEXT DEFAULT 'personal',
            dividend_condition TEXT, dividend_ratio REAL, interest_rate REAL DEFAULT 0,
            invested_capital REAL DEFAULT 0, cash_balance REAL DEFAULT 0,
            sort_order INTEGER DEFAULT 0, accounts TEXT DEFAULT '[]', notes TEXT DEFAULT '', created_at TEXT, updated_at TEXT)""")

def _push_holdings():
    _bg_push_table('portfolio_holdings',
        ['portfolio_id','stock_code','account','shares_lot','added_at','updated_at'],
        ['portfolio_id','stock_code','account'],
        """CREATE TABLE IF NOT EXISTS portfolio_holdings (
            portfolio_id INTEGER NOT NULL, stock_code TEXT NOT NULL, account TEXT NOT NULL DEFAULT '',
            shares_lot REAL DEFAULT 0, added_at TEXT, updated_at TEXT,
            PRIMARY KEY (portfolio_id, stock_code, account))""")

@app.route("/api/portfolio/list")
@require_portfolio_auth
def portfolio_list():
  try:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, name, dividend_condition, dividend_ratio, invested_capital, cash_balance, sort_order, interest_rate, portfolio_type, accounts, notes FROM portfolios ORDER BY portfolio_type, sort_order, id")
    portfolios = []
    for row in c.fetchall():
        pid, name, div_cond, div_ratio, capital, cash, sort, interest_rate, ptype, accts_json, notes = row
        c.execute("""SELECT h.stock_code, h.shares_lot, s.name, s.close, h.account
                     FROM portfolio_holdings h LEFT JOIN stocks s ON h.stock_code = s.code
                     WHERE h.portfolio_id = ? ORDER BY h.stock_code, h.account""", (pid,))
        # 按股票代碼分組，各帳戶張數拆分
        stock_map = {}
        for h in c.fetchall():
            code, lots, sname, price, acct = h
            if code not in stock_map:
                stock_map[code] = {'code': code, 'name': sname or '', 'price': price or 0, 'accounts': {}}
            stock_map[code]['accounts'][acct or ''] = lots
        holdings = []
        total_mv = 0
        for code in sorted(stock_map.keys()):
            s = stock_map[code]
            total_lots = sum(s['accounts'].values())
            mv = total_lots * 1000 * s['price']
            total_mv += mv
            holdings.append({
                'code': s['code'], 'name': s['name'], 'price': s['price'],
                'lots': total_lots, 'market_value': mv, 'accounts': s['accounts']
            })
        # 計算比重（以總市值+現金為分母）
        total_value = total_mv + (cash or 0)
        for h in holdings:
            h['weight'] = round(h['market_value'] / total_value * 100, 2) if total_value > 0 else 0
        pnl = total_value - (capital or 0) if capital else 0
        pnl_pct = round(pnl / capital * 100, 2) if capital else 0
        # 分紅計算：(損益 - 利息) × 比例，損益不超過利息時為 0
        ir = interest_rate or 0
        interest = (capital or 0) * ir / 100
        bonus = max(0, pnl - interest) * (div_ratio or 0) if pnl > interest else 0
        try: accts = json.loads(accts_json) if accts_json else []
        except Exception: accts = []
        portfolios.append({
            'id': pid, 'name': name,
            'portfolio_type': ptype or 'personal',
            'dividend_condition': div_cond, 'dividend_ratio': div_ratio,
            'interest_rate': ir, 'accounts': accts, 'notes': notes or '',
            'invested_capital': capital, 'cash_balance': cash,
            'sort_order': sort, 'holdings': holdings,
            'total_market_value': total_mv, 'total_value': total_value,
            'pnl': pnl, 'pnl_pct': pnl_pct,
            'interest': interest, 'bonus': round(bonus)
        })
    conn.close()
    return jsonify(portfolios)
  except Exception as e:
    return jsonify({"error": str(e)}), 500

@app.route("/api/portfolio/create", methods=["POST"])
@require_portfolio_auth
def portfolio_create():
  try:
    data = request.get_json() or {}
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    accts = json.dumps(data.get('accounts', []), ensure_ascii=False)
    vals = (data.get('name','新組合'), data.get('portfolio_type','personal'),
            data.get('dividend_condition',''), data.get('dividend_ratio',0), data.get('interest_rate',0),
            data.get('invested_capital',0), data.get('cash_balance',0), accts, data.get('notes',''), data.get('sort_order',0), now, now)
    is_pg = bool(os.environ.get('DATABASE_URL'))
    if is_pg:
        # PostgreSQL: 先修正序列再 INSERT，避免 push 資料導致序列不同步
        # 用 db.py 已清理的 DATABASE_URL
        from db import DATABASE_URL as PG_URL
        import psycopg2
        pg_conn = psycopg2.connect(PG_URL)
        pg_conn.autocommit = True
        cur = pg_conn.cursor()
        cur.execute("SELECT setval('portfolios_id_seq', COALESCE((SELECT MAX(id) FROM portfolios), 0))")
        cur.execute("""INSERT INTO portfolios (name, portfolio_type, dividend_condition, dividend_ratio, interest_rate, invested_capital, cash_balance, accounts, notes, sort_order, created_at, updated_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""", vals)
        new_id = cur.fetchone()[0]
        pg_conn.close()
    else:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""INSERT INTO portfolios (name, portfolio_type, dividend_condition, dividend_ratio, interest_rate, invested_capital, cash_balance, accounts, notes, sort_order, created_at, updated_at)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", vals)
        new_id = c.lastrowid
        conn.commit()
        conn.close()
    _push_portfolios()
    return jsonify({"status": "ok", "id": new_id})
  except Exception as e:
    return jsonify({"error": str(e)}), 500

@app.route("/api/portfolio/<int:pid>", methods=["PUT"])
@require_portfolio_auth
def portfolio_update(pid):
    data = request.get_json() or {}
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    fields = []
    vals = []
    for k in ['name','portfolio_type','dividend_condition','dividend_ratio','interest_rate','invested_capital','cash_balance','notes','sort_order']:
        if k in data:
            fields.append(f"{k}=?")
            vals.append(data[k])
    if 'accounts' in data:
        fields.append("accounts=?")
        vals.append(json.dumps(data['accounts'], ensure_ascii=False))
    if not fields:
        return jsonify({"status": "ok"})
    fields.append("updated_at=?")
    vals.append(now)
    vals.append(pid)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(f"UPDATE portfolios SET {','.join(fields)} WHERE id=?", vals)
    # 設定帳戶後，把舊的 account="" 持股搬到第一個帳戶
    if 'accounts' in data and data['accounts']:
        first_acct = data['accounts'][0]
        c.execute("SELECT stock_code, shares_lot FROM portfolio_holdings WHERE portfolio_id=? AND account=''", (pid,))
        old_rows = c.fetchall()
        for code, lots in old_rows:
            if lots and lots > 0:
                # 搬到第一個帳戶（合併）
                c.execute("SELECT shares_lot FROM portfolio_holdings WHERE portfolio_id=? AND stock_code=? AND account=?",
                          (pid, code, first_acct))
                existing = c.fetchone()
                if existing:
                    c.execute("UPDATE portfolio_holdings SET shares_lot=?, updated_at=? WHERE portfolio_id=? AND stock_code=? AND account=?",
                              (lots + (existing[0] or 0), now, pid, code, first_acct))
                else:
                    c.execute("INSERT INTO portfolio_holdings (portfolio_id, stock_code, account, shares_lot, added_at, updated_at) VALUES (?,?,?,?,?,?)",
                              (pid, code, first_acct, lots, now, now))
            c.execute("DELETE FROM portfolio_holdings WHERE portfolio_id=? AND stock_code=? AND account=''", (pid, code))
    conn.commit()
    conn.close()
    _push_portfolios()
    _push_holdings()
    return jsonify({"status": "ok"})

@app.route("/api/portfolio/<int:pid>", methods=["DELETE"])
@require_portfolio_auth
def portfolio_delete(pid):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM portfolio_holdings WHERE portfolio_id=?", (pid,))
    c.execute("DELETE FROM portfolios WHERE id=?", (pid,))
    conn.commit()
    conn.close()
    _push_portfolios()
    _push_holdings()
    return jsonify({"status": "ok"})

@app.route("/api/portfolio/<int:pid>/holdings", methods=["POST"])
@require_portfolio_auth
def portfolio_add_holding(pid):
    data = request.get_json() or {}
    code = str(data.get('code', '')).strip()
    if not code:
        return jsonify({"error": "code required"}), 400
    acct = str(data.get('account', '')).strip()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO portfolio_holdings (portfolio_id, stock_code, account, shares_lot, added_at, updated_at) VALUES (?,?,?,?,?,?)",
              (pid, code, acct, data.get('lots', 0), now, now))
    conn.commit()
    conn.close()
    _push_holdings()
    return jsonify({"status": "ok"})

@app.route("/api/portfolio/<int:pid>/holdings/<code>", methods=["PUT"])
@require_portfolio_auth
def portfolio_update_holding(pid, code):
    data = request.get_json() or {}
    acct = str(data.get('account', '')).strip()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO portfolio_holdings (portfolio_id, stock_code, account, shares_lot, added_at, updated_at) VALUES (?,?,?,?,?,?)",
              (pid, code, acct, data.get('lots', 0), now, now))
    conn.commit()
    conn.close()
    _push_holdings()
    return jsonify({"status": "ok"})

@app.route("/api/portfolio/<int:pid>/holdings/<code>", methods=["DELETE"])
@require_portfolio_auth
def portfolio_delete_holding(pid, code):
    acct = str(request.args.get('account', '')).strip()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM portfolio_holdings WHERE portfolio_id=? AND stock_code=? AND account=?", (pid, code, acct))
    conn.commit()
    conn.close()
    _push_holdings()
    return jsonify({"status": "ok"})

@app.route("/api/sync/portfolio-dump")
def sync_portfolio_dump():
    """供本機 pull 用，以 Render 為準同步 portfolio 資料"""
    if not check_sync_token():
        return jsonify({"error": "unauthorized"}), 401
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, name, portfolio_type, dividend_condition, dividend_ratio, interest_rate, invested_capital, cash_balance, sort_order, accounts, notes, created_at, updated_at FROM portfolios ORDER BY id")
    portfolios = []
    for r in c.fetchall():
        portfolios.append({
            'id': r[0], 'name': r[1], 'portfolio_type': r[2], 'dividend_condition': r[3],
            'dividend_ratio': r[4], 'interest_rate': r[5], 'invested_capital': r[6],
            'cash_balance': r[7], 'sort_order': r[8], 'accounts': r[9], 'notes': r[10],
            'created_at': r[11], 'updated_at': r[12]
        })
    c.execute("SELECT portfolio_id, stock_code, account, shares_lot, added_at, updated_at FROM portfolio_holdings")
    holdings = []
    for r in c.fetchall():
        holdings.append({
            'portfolio_id': r[0], 'stock_code': r[1], 'account': r[2],
            'shares_lot': r[3], 'added_at': r[4], 'updated_at': r[5]
        })
    conn.close()
    return jsonify({"portfolios": portfolios, "holdings": holdings})

# ── 廚房管理 ────────────────────────────────────────────────
def _init_cooking_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS recipes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        category TEXT DEFAULT '',
        ingredients TEXT DEFAULT '',
        steps TEXT DEFAULT '',
        note TEXT DEFAULT '',
        servings TEXT DEFAULT '2-3人',
        created_at TEXT,
        updated_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS weekly_menu (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        week_start TEXT NOT NULL,
        day INTEGER NOT NULL,
        meal TEXT NOT NULL,
        recipe_id INTEGER,
        custom_name TEXT DEFAULT '',
        note TEXT DEFAULT '',
        UNIQUE(week_start, day, meal)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS shopping_list (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        week_start TEXT NOT NULL,
        item TEXT NOT NULL,
        quantity TEXT DEFAULT '',
        checked INTEGER DEFAULT 0,
        manual INTEGER DEFAULT 0
    )""")
    try: conn.commit()
    except Exception: pass
    conn.close()
    # PostgreSQL: 修正 SERIAL 序列號
    if os.environ.get('DATABASE_URL'):
        try:
            import psycopg2
            from db import DATABASE_URL as PG_URL
            pg_conn = psycopg2.connect(PG_URL)
            pg_conn.autocommit = True
            cur = pg_conn.cursor()
            for tbl in ['recipes', 'weekly_menu', 'shopping_list']:
                try:
                    cur.execute(f"SELECT setval(pg_get_serial_sequence('{tbl}', 'id'), COALESCE((SELECT MAX(id) FROM {tbl}), 0) + 1, false)")
                except Exception:
                    pg_conn.rollback()
            pg_conn.close()
        except Exception:
            pass

try:
    _init_cooking_db()
except Exception as e:
    print(f"[Cooking] DB 初始化失敗（不影響啟動）: {e}")

# -- 食譜 CRUD --
@app.route("/api/cooking/recipes", methods=["GET"])
@require_portfolio_auth
def cooking_recipes_list():
    q = request.args.get('q', '').strip()
    cat = request.args.get('category', '').strip()
    sql = "SELECT * FROM recipes WHERE 1=1"
    params = []
    if q:
        sql += " AND (name LIKE ? OR ingredients LIKE ?)"
        params += [f'%{q}%', f'%{q}%']
    if cat:
        sql += " AND category=?"
        params.append(cat)
    sql += " ORDER BY updated_at DESC"
    rows = query_db(sql, params)
    return jsonify([dict(r) for r in rows])

@app.route("/api/cooking/recipes", methods=["POST"])
@require_portfolio_auth
def cooking_recipe_create():
    d = request.json or {}
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""INSERT INTO recipes (name, category, ingredients, steps, note, servings, created_at, updated_at)
                 VALUES (?,?,?,?,?,?,?,?)""",
              (d.get('name',''), d.get('category',''), d.get('ingredients',''),
               d.get('steps',''), d.get('note',''), d.get('servings','2-3人'), now, now))
    rid = c.lastrowid
    conn.commit()
    conn.close()
    return jsonify({"status": "ok", "id": rid})

@app.route("/api/cooking/recipes/<int:rid>", methods=["PUT"])
@require_portfolio_auth
def cooking_recipe_update(rid):
    d = request.json or {}
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""UPDATE recipes SET name=?, category=?, ingredients=?, steps=?, note=?, servings=?, updated_at=?
                 WHERE id=?""",
              (d.get('name',''), d.get('category',''), d.get('ingredients',''),
               d.get('steps',''), d.get('note',''), d.get('servings','2-3人'), now, rid))
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})

@app.route("/api/cooking/recipes/<int:rid>", methods=["DELETE"])
@require_portfolio_auth
def cooking_recipe_delete(rid):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM recipes WHERE id=?", (rid,))
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})

# -- 每週菜單 --
@app.route("/api/cooking/menu", methods=["GET"])
@require_portfolio_auth
def cooking_menu_get():
    week = request.args.get('week', '')
    if not week:
        # 算出本週一
        from datetime import timedelta
        today = datetime.now()
        monday = today - timedelta(days=today.weekday())
        week = monday.strftime('%Y-%m-%d')
    rows = query_db("SELECT wm.*, r.name as recipe_name, r.ingredients, r.steps FROM weekly_menu wm LEFT JOIN recipes r ON wm.recipe_id=r.id WHERE wm.week_start=? ORDER BY wm.day, wm.meal", [week])
    return jsonify({"week_start": week, "items": [dict(r) for r in rows]})

@app.route("/api/cooking/menu/weeks", methods=["GET"])
@require_portfolio_auth
def cooking_menu_weeks():
    rows = query_db("SELECT DISTINCT week_start FROM weekly_menu ORDER BY week_start DESC")
    return jsonify([r['week_start'] for r in rows])

@app.route("/api/cooking/menu", methods=["POST"])
@require_portfolio_auth
def cooking_menu_save():
    d = request.json or {}
    week = d.get('week_start', '')
    items = d.get('items', [])
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM weekly_menu WHERE week_start=?", (week,))
    for item in items:
        c.execute("""INSERT INTO weekly_menu (week_start, day, meal, recipe_id, custom_name, note)
                     VALUES (?,?,?,?,?,?)""",
                  (week, item.get('day',0), item.get('meal',''),
                   item.get('recipe_id'), item.get('custom_name',''), item.get('note','')))
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})

# -- 採購清單 --
@app.route("/api/cooking/shopping", methods=["GET"])
@require_portfolio_auth
def cooking_shopping_get():
    week = request.args.get('week', '')
    if not week:
        from datetime import timedelta
        today = datetime.now()
        monday = today - timedelta(days=today.weekday())
        week = monday.strftime('%Y-%m-%d')
    rows = query_db("SELECT * FROM shopping_list WHERE week_start=? ORDER BY checked, item", [week])
    return jsonify({"week_start": week, "items": [dict(r) for r in rows]})

@app.route("/api/cooking/shopping", methods=["POST"])
@require_portfolio_auth
def cooking_shopping_save():
    d = request.json or {}
    week = d.get('week_start', '')
    items = d.get('items', [])
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM shopping_list WHERE week_start=?", (week,))
    for item in items:
        c.execute("""INSERT INTO shopping_list (week_start, item, quantity, checked, manual)
                     VALUES (?,?,?,?,?)""",
                  (week, item.get('item',''), item.get('quantity',''),
                   item.get('checked',0), item.get('manual',0)))
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})

@app.route("/api/cooking/shopping/toggle", methods=["POST"])
@require_portfolio_auth
def cooking_shopping_toggle():
    d = request.json or {}
    sid = d.get('id')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE shopping_list SET checked = CASE WHEN checked=1 THEN 0 ELSE 1 END WHERE id=?", (sid,))
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})

# ── 啟動 ────────────────────────────────────────────────────
def _wait_for_port(port, timeout=90):
    """等待 port 釋放（AirPlay 開機時可能短暫佔用 5000）"""
    import socket as _sock
    import time as _time
    start = _time.time()
    while _time.time() - start < timeout:
        s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
        try:
            s.bind(('0.0.0.0', port))
            s.close()
            return True
        except OSError:
            s.close()
            _time.sleep(3)
    return False

if __name__ == "__main__":
    is_local = not os.environ.get('DATABASE_URL')
    # launchd KeepAlive 環境下關閉 reloader，避免子程序佔 port 導致重啟失敗
    under_launchd = bool(os.environ.get('XPC_SERVICE_NAME'))
    use_reloader = is_local and not under_launchd
    if is_local and not _wait_for_port(5000):
        print("Port 5000 持續被佔用超過 90 秒，放棄啟動", flush=True)
        import sys; sys.exit(1)
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=use_reloader)
