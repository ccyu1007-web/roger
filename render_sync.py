"""
render_sync.py — 本機資料同步到 Render
所有 _push_*_to_render() 函式集中在這裡
只在本機環境（無 DATABASE_URL）執行
"""

import logging
import os
import time
from datetime import datetime
import requests
import db as sqlite3
from fetcher_utils import create_session as _create_session

logger = logging.getLogger(__name__)

RENDER_URL = "https://tock-system.onrender.com"
SYNC_TOKEN = os.environ.get('SYNC_TOKEN', 'stock-sync-2026')
SYNC_HEADERS = {'X-Sync-Token': SYNC_TOKEN}


def _is_cloud():
    return bool(os.environ.get('DATABASE_URL'))


# 最近一次同步結果（供 health API 讀取）
_last_sync_result = {'time': None, 'last_success': None, 'failures': [], 'ok': True}


def _post_with_retry(url, json_data, timeout=60, max_retries=3, label=''):
    """POST with retry，最多重試 max_retries 次，間隔遞增（2s, 4s）"""
    for attempt in range(max_retries):
        try:
            _rs = _create_session()
            resp = _rs.post(url, json=json_data, headers=SYNC_HEADERS, timeout=timeout)
            if resp.status_code == 200:
                return resp
            if attempt == max_retries - 1:
                print(f"  [{label}] HTTP {resp.status_code}（{max_retries}次重試後）")
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"  [{label}] 失敗（{max_retries}次重試後）: {e}")
        if attempt < max_retries - 1:
            time.sleep(2 * (attempt + 1))
    return None


def _push_single_table(table_name):
    """按表名 push 單張表到 Render — 自動從本機 DB 讀取所有欄位。
    用法：_push_single_table('stock_checklist')
    """
    if _is_cloud():
        return
    # 從本機 DB 取得所有欄位名
    with sqlite3.get_conn() as conn:
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        cols_info = cursor.fetchall()
    if not cols_info:
        print(f"[同步] 表 {table_name} 不存在")
        return
    columns = [c[1] for c in cols_info]
    # 取得主鍵
    pk = [c[1] for c in cols_info if c[5] > 0]  # pk index > 0
    if not pk:
        pk = [columns[0]]  # fallback: 第一個欄位
    # material_news/etf 等需要 clear_first（stock_checklist 改用 UPSERT，避免清空時前端讀到空資料）
    clear_tables = {'material_news', 'etf_holdings', 'etf_changes', 'etf_info'}
    _push_table_to_render(table_name, columns, pk, clear_first=(table_name in clear_tables))


def _push_table_to_render(table, columns, pk, create_sql=None, where=None, batch_size=500, clear_first=False, since=None):
    """
    通用全表同步：把本機任意資料表 push 到 Render
    table: 表名
    columns: 欄位列表
    pk: 主鍵欄位列表（用於 UPSERT）
    create_sql: 建表 SQL（Render 端自動建表）
    where: 可選的 WHERE 條件
    clear_first: 由 Render 端在同一 transaction 裡清空+寫入（避免空窗期）
    since: 只推 updated_at >= since 的資料（增量同步），格式 'YYYY-MM-DD HH:MM:SS'
    """
    col_str = ','.join(columns)
    sql = f"SELECT {col_str} FROM {table}"
    if where:
        sql += f" {where}"
    if since and 'updated_at' in columns:
        sql += (" AND" if where else " WHERE") + f" updated_at >= '{since}'"
    with sqlite3.get_conn() as conn:
        rows = conn.execute(sql).fetchall()

    if not rows:
        print(f"  [{table}] 無資料")
        return 0

    data = [{columns[j]: r[j] for j in range(len(columns))} for r in rows]

    failed = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        payload = {'table': table, 'columns': columns, 'pk': pk,
                   'create_sql': create_sql or '', 'data': batch}
        # 第一個 batch 帶 clear_first，Render 端在同一 transaction 清空+寫入
        if clear_first and i == 0:
            payload['clear_first'] = True

        resp = _post_with_retry(
            f'{RENDER_URL}/api/sync/table', payload,
            label=f'{table} batch {i//batch_size+1}')
        if not resp:
            failed += len(batch)

    if failed:
        logger.warning(f"[{table}] 部分同步：{len(data)-failed}/{len(data)} 筆成功，{failed} 筆失敗")
    print(f"  [{table}] {len(data)} 筆" + (f"（{failed} 筆失敗）" if failed else ""))
    return len(data) - failed


def _merge_push_to_render(table, columns, pk, create_sql=None):
    """
    合併式同步：本機資料 UPSERT 到 Render，同時刪除本機已移除的項目，
    但保留 Render 獨有的（在前台操作的）。
    適用於 user_lists 等雙邊都會操作的表。
    """
    col_str = ','.join(columns)
    with sqlite3.get_conn() as conn:
        rows = conn.execute(f"SELECT {col_str} FROM {table}").fetchall()

    local_data = [{columns[j]: r[j] for j in range(len(columns))} for r in rows]
    # 本機所有 pk 組合
    local_keys = set()
    for r in local_data:
        key = tuple(r[k] for k in pk)
        local_keys.add(key)

    # 1. UPSERT 本機資料到 Render（不用 clear_first）
    if local_data:
        failed = 0
        for i in range(0, len(local_data), 500):
            batch = local_data[i:i+500]
            payload = {'table': table, 'columns': columns, 'pk': pk,
                       'create_sql': create_sql or '', 'data': batch}
            resp = _post_with_retry(
                f'{RENDER_URL}/api/sync/table', payload,
                label=f'{table} merge batch {i//500+1}')
            if not resp:
                failed += len(batch)
        print(f"  [{table}] UPSERT {len(local_data)} 筆" + (f"（{failed} 筆失敗）" if failed else ""))

    # 2. 請 Render 刪除「本機已移除但 Render 還有」的項目
    payload = {
        'table': table,
        'pk': pk,
        'local_keys': [list(k) for k in local_keys],
    }
    resp = _post_with_retry(
        f'{RENDER_URL}/api/sync/table-merge-cleanup', payload,
        label=f'{table} merge cleanup')
    if resp:
        try:
            result = resp.json()
            deleted = result.get('deleted', 0)
            if deleted:
                print(f"  [{table}] 清理 Render 端已移除 {deleted} 筆")
        except Exception:
            pass

    return len(local_data)


def get_last_sync_result():
    """供 health API 讀取最近一次同步結果"""
    return _last_sync_result


def _pull_user_estimates_from_render():
    """從 Render 拉回 user_estimates（前台設定的預估參數），本機有的 key 優先不覆蓋"""
    if _is_cloud():
        return
    import json
    from datetime import datetime
    _rs = _create_session()
    resp = _rs.get(f'{RENDER_URL}/api/user-estimates-all', timeout=30)
    if resp.status_code != 200:
        print(f"[拉回] user_estimates API 失敗: HTTP {resp.status_code}")
        return
    render_ue = resp.json()
    if not render_ue:
        return

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    updated = 0
    with sqlite3.get_conn() as conn:
        for code, params in render_ue.items():
            if not params:
                continue
            params_str = json.dumps(params, ensure_ascii=False)
            existing = conn.execute('SELECT params FROM user_estimates WHERE code=?', (code,)).fetchone()
            if existing and existing[0]:
                local_params = json.loads(existing[0])
                merged = {**params, **local_params}  # 本機 key 優先
                merged_str = json.dumps(merged, ensure_ascii=False)
                if merged_str != existing[0]:
                    conn.execute('UPDATE user_estimates SET params=?, updated_at=? WHERE code=?',
                               (merged_str, now_str, code))
                    updated += 1
            else:
                conn.execute('INSERT OR REPLACE INTO user_estimates (code, params, updated_at) VALUES (?, ?, ?)',
                           (code, params_str, now_str))
                updated += 1
        conn.commit()
    if updated:
        print(f"[拉回] user_estimates 同步 {updated} 支到本機")


def _pull_user_settings_from_render():
    """從 Render 拉回 user_settings，時間戳較新的覆蓋較舊的"""
    if _is_cloud():
        return
    _rs = _create_session()
    resp = _rs.get(f'{RENDER_URL}/api/user-settings', timeout=30)
    if resp.status_code != 200:
        print(f"[拉回] user_settings API 失敗: HTTP {resp.status_code}")
        return
    render_data = resp.json()
    if not render_data:
        return

    # SYNC_KEYS 對應前端的同步鍵
    sync_keys = ['watch_settings', 'watch_custom_conds', 'quality_settings', 'quality_custom_conds',
                 'col_visible', 'global_div_weights', 'blend_ratio', 'global_val_params',
                 'sort_col', 'sort_asc', 'grade_filters', 'custom_col_preset']

    from datetime import datetime
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    updated = 0
    with sqlite3.get_conn() as conn:
        for key in sync_keys:
            render_val = render_data.get(key)
            render_time = render_data.get(key + '_time', '2000-01-01')
            if not render_val:
                continue
            local = conn.execute('SELECT value, updated_at FROM user_settings WHERE key=?', (key,)).fetchone()
            local_time = local[1] if local and local[1] else '2000-01-01'
            # Render 較新 → 拉回本機
            if render_time > local_time:
                conn.execute('INSERT OR REPLACE INTO user_settings (key, value, updated_at) VALUES (?, ?, ?)',
                           (key, render_val, render_time))
                updated += 1
        conn.commit()
    if updated:
        print(f"[拉回] user_settings 同步 {updated} 筆到本機")


def _pull_user_lists_from_render():
    """從 Render 拉回 user_lists，確保本機與 Render 同步。
    Render 是使用者主要操作端，以 Render 為準：
    - Render 有但本機沒有 → 加入本機
    - 本機有但 Render 沒有 → 從本機刪除（使用者在 Render 取消的）
    """
    if _is_cloud():
        return
    _rs = _create_session()
    try:
        resp = _rs.get(f'{RENDER_URL}/api/user-lists', timeout=30)
    except Exception as e:
        print(f"[拉回] user_lists API 連線失敗: {e}")
        return
    if resp.status_code != 200:
        print(f"[拉回] user_lists API 失敗: HTTP {resp.status_code}")
        return
    render_data = resp.json()

    # 把 Render 的資料展平成 set of (list_type, code) 和 dict
    render_items = {}  # (list_type, code) → {added_at, price_at}
    for list_type, items in render_data.items():
        for item in items:
            key = (list_type, item['code'])
            render_items[key] = {
                'added_at': item.get('added_at'),
                'price_at': item.get('price_at'),
            }

    added = 0
    removed = 0
    with sqlite3.get_conn() as conn:
        # 讀本機現有的 user_lists
        local_rows = conn.execute(
            'SELECT list_type, code, added_at, price_at FROM user_lists'
        ).fetchall()
        local_items = {}
        for r in local_rows:
            local_items[(r[0], r[1])] = {'added_at': r[2], 'price_at': r[3]}

        # Render 有但本機沒有 → 加入本機
        for key, vals in render_items.items():
            if key not in local_items:
                conn.execute(
                    'INSERT OR IGNORE INTO user_lists (list_type, code, added_at, price_at) VALUES (?,?,?,?)',
                    (key[0], key[1], vals['added_at'], vals['price_at'])
                )
                added += 1

        # 本機有但 Render 沒有 → 使用者在 Render 取消了，本機也刪除
        # 只處理 Render 有管理的 list_type（Render 至少有一筆該 type 的資料，或曾經有過）
        render_types = set(render_data.keys())
        # 也把空陣列的 type 算進去（代表 Render 知道這個 type 但清空了）
        for key in local_items:
            if key not in render_items:
                lt = key[0]
                # 只刪除 Render 端也有管理的 type（避免刪除 Render API 沒回傳的 type）
                # quality/watch 由前端 autoCheck 管理，也需要同步
                if lt in render_types or lt in ('track', 'skip', 'watch', 'hold', 'focus', 'quality'):
                    conn.execute(
                        'DELETE FROM user_lists WHERE list_type=? AND code=?',
                        (key[0], key[1])
                    )
                    removed += 1

        conn.commit()

    if added or removed:
        print(f"[拉回] user_lists 同步: 新增 {added} 筆, 刪除 {removed} 筆")


_LAST_SYNC_TIME_FILE = os.path.join(os.path.dirname(__file__), 'logs', '.last_sync_time')

def _get_last_sync_time():
    """讀取上次同步時間"""
    try:
        with open(_LAST_SYNC_TIME_FILE, 'r') as f:
            return f.read().strip()
    except Exception:
        return None

def _save_last_sync_time(ts):
    """儲存同步時間"""
    try:
        os.makedirs(os.path.dirname(_LAST_SYNC_TIME_FILE), exist_ok=True)
        with open(_LAST_SYNC_TIME_FILE, 'w') as f:
            f.write(ts)
    except Exception:
        pass

def _push_all_to_render():
    """一次 push 所有資料到 Render — 增量同步（只推上次同步後更新的）"""
    global _last_sync_result
    if _is_cloud():
        return

    # 先從 Render 拉回使用者設定（前台操作的參數，Render 較新的覆蓋本機）
    try:
        _pull_user_settings_from_render()
    except Exception as e:
        print(f"[拉回] user_settings 失敗: {e}")
    try:
        _pull_user_estimates_from_render()
    except Exception as e:
        print(f"[拉回] user_estimates 失敗: {e}")
    try:
        _pull_user_lists_from_render()
    except Exception as e:
        print(f"[拉回] user_lists 失敗: {e}")

    last_sync = _get_last_sync_time()
    mode = f"增量（since {last_sync}）" if last_sync else "全量（首次）"
    print(f"[同步] 開始 push 到 Render...（{mode}）")
    failures = []

    # 既有的專用 push（stocks 表結構特殊，保留原函式）
    for name, fn in [('prices', _push_prices_to_render), ('annual', _push_annual_to_render),
                     ('institutional', _push_institutional_to_render), ('estimates', _push_estimates_to_render)]:
        try:
            fn()
        except Exception as e:
            print(f"  [{name}] 失敗: {e}")
            failures.append(f"{name}: {e}")

    # 通用全表同步 — 所有獨立資料表
    SYNC_TABLES = [
        {
            'table': 'quarterly_financial',
            'columns': ['code','quarter','revenue','cost','gross_profit','operating_expense',
                        'operating_income','non_operating','pretax_income','tax','continuing_income',
                        'net_income_parent','eps','contract_liability','inventory','updated_at'],
            'pk': ['code','quarter'],
        },
        {
            'table': 'financial_annual',
            'columns': ['code','year','revenue','cost','gross_profit','operating_expense',
                        'operating_income','non_operating','pretax_income','tax','net_income',
                        'net_income_parent','total_assets','total_equity','common_stock',
                        'contract_liability','operating_cf','capex','eps',
                        'weighted_shares','cash_dividend','stock_dividend','updated_at'],
            'pk': ['code','year'],
        },
        {
            'table': 'pe_history',
            'columns': ['code','year','pe_high','pe_low','updated_at'],
            'pk': ['code','year'],
        },
        {
            'table': 'monthly_revenue',
            'columns': ['code','year','month','revenue','updated_at'],
            'pk': ['code','year','month'],
            'where': 'WHERE year >= 2020',
            'create_sql': """CREATE TABLE IF NOT EXISTS monthly_revenue (
                code TEXT NOT NULL, year INTEGER NOT NULL, month INTEGER NOT NULL,
                revenue REAL, updated_at TEXT, PRIMARY KEY (code, year, month))""",
        },
        {
            'table': 'stock_state',
            'columns': ['stock_id','date','price','price_pos','fair_low','fair_mid','fair_high',
                        'shen_eps','shen_pe','shen_yld','fin_grade','updated_at',
                        'val_level','val_aa','val_a1','val_a2','val_a','val_lt6','discount_pct',
                        'neff_d','lynch_d'],
            'pk': ['stock_id','date'],
            'create_sql': """CREATE TABLE IF NOT EXISTS stock_state (
                stock_id TEXT NOT NULL, date TEXT NOT NULL,
                price REAL, price_pos REAL, fair_low REAL, fair_mid REAL, fair_high REAL,
                shen_eps REAL, shen_pe REAL, shen_yld REAL, fin_grade TEXT, updated_at TEXT,
                val_level TEXT, val_aa REAL, val_a1 REAL, val_a2 REAL, val_a REAL,
                val_lt6 REAL, discount_pct REAL, neff_d REAL, lynch_d REAL,
                PRIMARY KEY (stock_id, date))""",
        },
        {
            'table': 'material_news',
            'columns': ['id','code','name','date','time','subject','description','tier',
                        'matched_rule','created_at','direction','link','status'],
            'pk': ['id'],
            'clear_first': True,
            'create_sql': """CREATE TABLE IF NOT EXISTS material_news (
                id INTEGER PRIMARY KEY, code TEXT, name TEXT, date TEXT, time TEXT,
                subject TEXT, description TEXT, tier INTEGER, matched_rule TEXT,
                created_at TEXT, direction TEXT, link TEXT, status TEXT)""",
        },
        {
            'table': 'etf_holdings',
            'columns': ['etf_code','stock_code','stock_name','weight','shares','updated'],
            'pk': ['etf_code','stock_code'],
            'clear_first': True,
            'create_sql': """CREATE TABLE IF NOT EXISTS etf_holdings (
                etf_code TEXT NOT NULL, stock_code TEXT NOT NULL,
                stock_name TEXT, weight REAL, shares INTEGER, updated TEXT,
                PRIMARY KEY (etf_code, stock_code))""",
        },
        {
            'table': 'etf_changes',
            'columns': ['id','etf_code','stock_code','stock_name','action','change_date','created_at'],
            'pk': ['id'],
            'create_sql': """CREATE TABLE IF NOT EXISTS etf_changes (
                id INTEGER PRIMARY KEY, etf_code TEXT, stock_code TEXT,
                stock_name TEXT, action TEXT, change_date TEXT, created_at TEXT)""",
        },
        {
            'table': 'etf_info',
            'columns': ['code','name','issuer','category','last_fetch'],
            'pk': ['code'],
            'clear_first': True,
            'create_sql': """CREATE TABLE IF NOT EXISTS etf_info (
                code TEXT PRIMARY KEY, name TEXT, issuer TEXT,
                category TEXT, last_fetch TEXT)""",
        },
        {
            'table': 'user_lists',
            'columns': ['list_type','code','added_at','price_at'],
            'pk': ['list_type','code'],
            'merge_mode': True,  # 合併式同步：本機 UPSERT + 刪除本機已移除的，保留 Render 獨有的
            'create_sql': """CREATE TABLE IF NOT EXISTS user_lists (
                list_type TEXT NOT NULL, code TEXT NOT NULL, added_at TEXT, price_at REAL,
                PRIMARY KEY (list_type, code))""",
        },
        {
            'table': 'user_notes',
            'columns': ['code', 'content', 'updated_at'],
            'pk': ['code'],
            'create_sql': """CREATE TABLE IF NOT EXISTS user_notes (
                code TEXT PRIMARY KEY, content TEXT, updated_at TEXT)""",
        },
        {
            'table': 'daily_notes',
            'columns': ['date', 'content', 'created_at'],
            'pk': ['date'],
            'clear_first': True,
            'create_sql': """CREATE TABLE IF NOT EXISTS daily_notes (
                date TEXT PRIMARY KEY, content TEXT, created_at TEXT)""",
        },
        {
            'table': 'industry_news',
            'columns': ['id', 'source', 'title', 'link', 'pub_time', 'summary', 'created_at', 'archived_code', 'archived_at'],
            'pk': ['id'],
            'clear_first': True,
            'create_sql': """CREATE TABLE IF NOT EXISTS industry_news (
                id INTEGER PRIMARY KEY, source TEXT, title TEXT, link TEXT,
                pub_time TEXT, summary TEXT, created_at TEXT,
                archived_code TEXT, archived_at TEXT)""",
        },
        {
            'table': 'user_settings',
            'columns': ['key', 'value', 'updated_at'],
            'pk': ['key'],
            'create_sql': """CREATE TABLE IF NOT EXISTS user_settings (
                key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)""",
        },
        {
            'table': 'user_estimates',
            'columns': ['code', 'params', 'updated_at', 'est_year'],
            'pk': ['code'],
            'create_sql': """CREATE TABLE IF NOT EXISTS user_estimates (
                code TEXT PRIMARY KEY, params TEXT, updated_at TEXT, est_year TEXT)""",
        },
        {
            'table': 'stock_checklist',
            'use_single_table': True,  # 動態讀 DB schema，不維護靜態欄位清單
        },
        {
            'table': 'daily_price',
            'columns': ['code','date','close_price','volume'],
            'pk': ['code','date'],
            'create_sql': """CREATE TABLE IF NOT EXISTS daily_price (
                code TEXT NOT NULL, date TEXT NOT NULL,
                close_price REAL, volume INTEGER,
                PRIMARY KEY (code, date))""",
            'where': "WHERE date >= date('now', '-30 days')",
        },
        {
            'table': 'focus_tracking',
            'columns': ['code','focus_date','focus_price','signal_mode','mode_switch_date',
                        'last_signal_date','last_signal_type','note'],
            'pk': ['code'],
            'clear_first': True,
            'create_sql': """CREATE TABLE IF NOT EXISTS focus_tracking (
                code TEXT PRIMARY KEY, focus_date TEXT, focus_price REAL,
                signal_mode TEXT DEFAULT 'initial', mode_switch_date TEXT,
                last_signal_date TEXT, last_signal_type TEXT, note TEXT)""",
        },
        {
            'table': 'focus_signals',
            'columns': ['code','date','signal_type','detail'],
            'pk': ['code','date','signal_type'],
            'clear_first': True,
            'create_sql': """CREATE TABLE IF NOT EXISTS focus_signals (
                code TEXT NOT NULL, date TEXT NOT NULL, signal_type TEXT NOT NULL,
                detail TEXT, PRIMARY KEY (code, date, signal_type))""",
            'where': "WHERE date >= date('now', '-30 days')",
        },
    ]

    from datetime import datetime
    sync_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for cfg in SYNC_TABLES:
        try:
            # clear_first 的表必須全量（新聞/ETF等需要清空重推）
            # merge_mode 的表用合併邏輯
            # 其他表：有 updated_at 欄位就用增量同步
            use_since = (last_sync
                         and not cfg.get('clear_first')
                         and not cfg.get('merge_mode')
                         and 'updated_at' in cfg['columns'])

            if cfg.get('use_single_table'):
                _push_single_table(cfg['table'])
            elif cfg.get('merge_mode'):
                _merge_push_to_render(
                    table=cfg['table'],
                    columns=cfg['columns'],
                    pk=cfg['pk'],
                    create_sql=cfg.get('create_sql'),
                )
            else:
                _push_table_to_render(
                    table=cfg['table'],
                    columns=cfg['columns'],
                    pk=cfg['pk'],
                    create_sql=cfg.get('create_sql'),
                    where=cfg.get('where'),
                    clear_first=cfg.get('clear_first', False),
                    since=last_sync if use_since else None,
                )
        except Exception as e:
            print(f"  [{cfg['table']}] 失敗: {e}")
            failures.append(f"{cfg['table']}: {e}")

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    is_ok = len(failures) == 0
    _last_sync_result = {
        'time': now_str,
        'last_success': now_str if is_ok else _last_sync_result.get('last_success'),
        'failures': failures,
        'ok': is_ok,
    }
    # 同步成功才更新時間戳
    if is_ok:
        _save_last_sync_time(sync_start)
    if failures:
        print(f"[同步] 完成，{len(failures)} 張表有失敗: {', '.join(failures)}")
    else:
        print("[同步] 完成，全部成功")


def _push_news_to_render():
    """把本機今天的新聞 push 到 Render"""
    if _is_cloud():
        return
    try:
        with sqlite3.get_conn(row_factory=True) as conn:
            rows = conn.execute("""SELECT code, name, date, subject, link, tier, matched_rule, direction, created_at
                                   FROM material_news WHERE created_at > datetime('now', '-1 day')""").fetchall()
        if not rows:
            return
        data = [dict(r) for r in rows]
        for i in range(0, len(data), 200):
            batch = data[i:i+200]
            _post_with_retry(f'{RENDER_URL}/api/sync/news', {'rows': batch}, timeout=30, label='新聞同步')
        print(f"[新聞同步] 已 push {len(data)} 筆到 Render")
    except Exception as e:
        print(f"[新聞同步] 失敗: {e}")


def _push_pe_history_to_render():
    """本機歷史本益比 push 到 Render"""
    if _is_cloud():
        return
    try:
        with sqlite3.get_conn() as conn:
            rows = conn.execute(
                "SELECT code, year, pe_high, pe_low, updated_at FROM pe_history"
            ).fetchall()

        if not rows:
            print("[PE歷史同步] 無資料")
            return

        cols = ['code', 'year', 'pe_high', 'pe_low', 'updated_at']
        data = [{cols[j]: r[j] for j in range(len(cols))} for r in rows]

        failed = 0
        for i in range(0, len(data), 500):
            batch = data[i:i+500]
            resp = _post_with_retry(f'{RENDER_URL}/api/sync/pe-history',
                                    {'data': batch}, label=f'PE歷史 batch {i//500+1}')
            if not resp:
                failed += len(batch)
        msg = f"[PE歷史同步] 已 push {len(data)} 筆到 Render"
        if failed:
            msg += f"（{failed} 筆失敗）"
        print(msg)
    except Exception as e:
        print(f"[PE歷史同步] 失敗: {e}")




def _push_financial_annual_to_render(full=False):
    """本機 financial_annual push 到 Render。預設只推當天更新的，full=True 推整表。"""
    if _is_cloud():
        return
    try:
        today_str = datetime.now().strftime('%Y-%m-%d')
        with sqlite3.get_conn() as conn:
            if full:
                where = ""
                params = ()
            else:
                where = "WHERE updated_at >= ?"
                params = (today_str,)
            rows = conn.execute(f"""SELECT code, year, revenue, cost, gross_profit, operating_expense,
                                  operating_income, non_operating, pretax_income, tax, net_income,
                                  net_income_parent, total_assets, total_equity, common_stock,
                                  contract_liability, operating_cf, capex, eps,
                                  weighted_shares, cash_dividend, stock_dividend, updated_at,
                                  eps_core, eps_nonop,
                                  cash_and_equivalents, short_term_debt, short_term_notes,
                                  current_long_term_debt, long_term_bank_debt,
                                  other_long_term_debt, bonds_payable, inventory,
                                  current_liabilities, roic, nopat, invested_capital, fin_grade,
                                  accounts_receivable, interest_expense,
                                  debt_ratio, fin_debt_ratio, interest_coverage, earnings_quality, fcf,
                                  inventory_days, ar_days
                                  FROM financial_annual {where}
                                  ORDER BY code, year""", params).fetchall()

        cols = ['code','year','revenue','cost','gross_profit','operating_expense',
                'operating_income','non_operating','pretax_income','tax','net_income',
                'net_income_parent','total_assets','total_equity','common_stock',
                'contract_liability','operating_cf','capex','eps',
                'weighted_shares','cash_dividend','stock_dividend','updated_at',
                'eps_core','eps_nonop',
                'cash_and_equivalents','short_term_debt','short_term_notes',
                'current_long_term_debt','long_term_bank_debt',
                'other_long_term_debt','bonds_payable','inventory',
                'current_liabilities','roic','nopat','invested_capital','fin_grade',
                'accounts_receivable','interest_expense',
                'debt_ratio','fin_debt_ratio','interest_coverage','earnings_quality','fcf',
                'inventory_days','ar_days']
        data = [{cols[j]: r[j] for j in range(len(cols)) if r[j] is not None} for r in rows]

        if not data:
            print("[年報同步] 沒有需要更新的年報")
            return

        failed = 0
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            resp = _post_with_retry(f'{RENDER_URL}/api/sync/financial-annual',
                                    {'data': batch}, label=f'年報 batch {i//batch_size+1}')
            if not resp:
                failed += len(batch)
        msg = f"[年報同步] 已 push {len(data)} 筆到 Render"
        if failed:
            msg += f"（{failed} 筆失敗）"
        print(msg)
    except Exception as e:
        print(f"[年報同步] 失敗: {e}")


def _push_quarterly_to_render():
    """本機季報資料 push 到 Render（只推當天更新的）"""
    if _is_cloud():
        return
    try:
        with sqlite3.get_conn() as conn:
            rows = conn.execute("""SELECT code, quarter, revenue, cost, gross_profit, operating_expense,
                                  operating_income, non_operating, pretax_income, tax, continuing_income,
                                  net_income_parent, eps, contract_liability, updated_at,
                                  inventory, weighted_shares, eps_core, eps_nonop
                                  FROM quarterly_financial
                                  ORDER BY code, quarter""").fetchall()

        cols = ['code','quarter','revenue','cost','gross_profit','operating_expense',
                'operating_income','non_operating','pretax_income','tax','continuing_income',
                'net_income_parent','eps','contract_liability','updated_at',
                'inventory','weighted_shares','eps_core','eps_nonop']
        data = [{cols[j]: r[j] for j in range(len(cols))} for r in rows]

        if not data:
            print("[季報同步] 今天沒有更新的季報")
            return

        failed = 0
        for i in range(0, len(data), 200):
            batch = data[i:i+200]
            resp = _post_with_retry(f'{RENDER_URL}/api/sync/quarterly',
                                    {'data': batch}, label=f'季報 batch {i//200+1}')
            if not resp:
                failed += len(batch)
        msg = f"[季報同步] 已 push {len(data)} 筆到 Render"
        if failed:
            msg += f"（{failed} 筆失敗）"
        print(msg)
    except Exception as e:
        print(f"[季報同步] 失敗: {e}")


def _push_annual_to_render():
    """本機年度 EPS + 股利 + 財務等級 push 到 Render"""
    if _is_cloud():
        return
    try:
        with sqlite3.get_conn() as conn:
            rows = conn.execute("""SELECT code, eps_date,
                eps_1, eps_1q, eps_2, eps_2q, eps_3, eps_3q, eps_4, eps_4q, eps_5, eps_5q,
                eps_y1, eps_y1_label, eps_y2, eps_y2_label, eps_y3, eps_y3_label,
                eps_y4, eps_y4_label, eps_y5, eps_y5_label, eps_y6, eps_y6_label,
                eps_ytd, eps_ytd_label,
                div_c1, div_s1, div_1_label, div_c2, div_s2, div_2_label,
                div_c3, div_s3, div_3_label, div_c4, div_s4, div_4_label,
                div_c5, div_s5, div_5_label, div_c6, div_s6, div_6_label,
                fin_grade_1, fin_grade_1y, fin_grade_2, fin_grade_2y,
                fin_grade_3, fin_grade_3y, fin_grade_4, fin_grade_4y,
                fin_grade_5, fin_grade_5y, fin_grade_6, fin_grade_6y,
                deepest_val_level, val_cheap_days,
                shen_eps, shen_div, shen_pe, shen_yld, shen_grade,
                weighted_eps, weighted_div, weighted_pe, weighted_yld, weighted_grade, weighted_payout,
                blend_eps, blend_div, blend_pe, blend_yld, blend_grade,
                eps_4q_sum, trailing_div, trailing_pe, trailing_yld, trailing_grade,
                contract_chg,
                payout_1, payout_2, payout_3, payout_4, payout_5, payout_6,
                val_aa, val_a1, val_a2, val_a, val_lt6,
                val_eps_used, val_div_used, val_pe, val_yld, val_source,
                est_eps, est_div, est_pe, est_yld, est_grade,
                sys_pe, sys_yld, sys_grade,
                gb_roic, gb_ey, gb_roic_rank, gb_ey_rank, gb_total_rank
                FROM stocks WHERE close IS NOT NULL""").fetchall()

        cols = ['code', 'eps_date',
            'eps_1','eps_1q','eps_2','eps_2q','eps_3','eps_3q','eps_4','eps_4q','eps_5','eps_5q',
            'eps_y1','eps_y1_label','eps_y2','eps_y2_label','eps_y3','eps_y3_label',
            'eps_y4','eps_y4_label','eps_y5','eps_y5_label','eps_y6','eps_y6_label',
            'eps_ytd','eps_ytd_label',
            'div_c1','div_s1','div_1_label','div_c2','div_s2','div_2_label',
            'div_c3','div_s3','div_3_label','div_c4','div_s4','div_4_label',
            'div_c5','div_s5','div_5_label','div_c6','div_s6','div_6_label',
            'fin_grade_1','fin_grade_1y','fin_grade_2','fin_grade_2y',
            'fin_grade_3','fin_grade_3y','fin_grade_4','fin_grade_4y',
            'fin_grade_5','fin_grade_5y','fin_grade_6','fin_grade_6y',
            'deepest_val_level','val_cheap_days',
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
            'gb_roic','gb_ey','gb_roic_rank','gb_ey_rank','gb_total_rank']
        data = [{cols[j]: r[j] for j in range(len(cols))} for r in rows]

        failed = 0
        for i in range(0, len(data), 200):
            batch = data[i:i+200]
            resp = _post_with_retry(f'{RENDER_URL}/api/sync/annual',
                                    {'data': batch}, timeout=30, label=f'年度 batch {i//200+1}')
            if not resp:
                failed += len(batch)
        msg = f"[年度同步] 已 push {len(data)} 支到 Render"
        if failed:
            msg += f"（{failed} 支失敗）"
        print(msg)
    except Exception as e:
        print(f"[年度同步] 失敗: {e}")


def _push_prices_to_render():
    """本機股價+營收欄位 push 到 Render"""
    if _is_cloud():
        return
    try:
        with sqlite3.get_conn() as conn:
            rows = conn.execute("""SELECT code, close, change, open, high, low, volume,
                                  revenue_date, revenue_year, revenue_month,
                                  revenue_yoy, revenue_mom, revenue_cum_yoy
                                  FROM stocks WHERE close IS NOT NULL""").fetchall()
        cols = ['code', 'close', 'change', 'open', 'high', 'low', 'volume',
                'revenue_date', 'revenue_year', 'revenue_month',
                'revenue_yoy', 'revenue_mom', 'revenue_cum_yoy']
        data = [{cols[j]: r[j] for j in range(len(cols))} for r in rows]

        failed = 0
        for i in range(0, len(data), 500):
            batch = data[i:i+500]
            resp = _post_with_retry(f'{RENDER_URL}/api/sync/prices',
                                    {'data': batch}, timeout=30, label=f'股價 batch {i//500+1}')
            if not resp:
                failed += len(batch)
        msg = f"[股價同步] 已 push {len(data)} 支到 Render"
        if failed:
            msg += f"（{failed} 支失敗）"
        print(msg)
    except Exception as e:
        print(f"[股價同步] 失敗: {e}")


def _push_institutional_to_render():
    """本機法人資料 push 到 Render PostgreSQL"""
    if _is_cloud():
        return
    try:
        with sqlite3.get_conn() as conn:
            rows = conn.execute("SELECT code, inst_foreign, inst_trust, inst_dealer FROM stocks WHERE inst_foreign IS NOT NULL").fetchall()
        data = [{'code': r[0], 'f': r[1], 't': r[2], 'd': r[3]} for r in rows]
        failed = 0
        for i in range(0, len(data), 500):
            batch = data[i:i+500]
            resp = _post_with_retry(f'{RENDER_URL}/api/refresh/institutional',
                                    {'data': batch}, timeout=30, label=f'法人 batch {i//500+1}')
            if not resp:
                failed += len(batch)
        msg = f"[法人同步] 已 push {len(data)} 支到 Render"
        if failed:
            msg += f"（{failed} 支失敗）"
        print(msg)
    except Exception as e:
        print(f"[法人同步] 失敗: {e}")


def _push_estimates_to_render():
    """將本機估算結果 push 到 Render"""
    if _is_cloud():
        return
    try:
        with sqlite3.get_conn(row_factory=True) as conn:
            rows = conn.execute("""SELECT code, sys_ann_eps, sys_ann_div, sys_ann_pe,
                                   sys_ann_yld, sys_ann_confidence,
                                   sys_est_eps, sys_est_quarter, sys_est_confidence
                                   FROM stocks WHERE sys_ann_eps IS NOT NULL""").fetchall()
        data = [dict(r) for r in rows]
        if not data:
            return
        resp = _post_with_retry(f"{RENDER_URL}/api/sync/estimates",
                                {"data": data}, timeout=30, label='估算')
        if resp:
            print(f"  估算 push 到 Render：{len(data)} 支")
        else:
            print(f"  估算 push 失敗（3次重試後）")
    except Exception as e:
        print(f"  估算 push 失敗：{e}")
