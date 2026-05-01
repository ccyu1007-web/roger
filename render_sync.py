"""
render_sync.py — 本機資料同步到 Render
所有 _push_*_to_render() 函式集中在這裡
只在本機環境（無 DATABASE_URL）執行
"""

import logging
import os
import time
import requests
import db as sqlite3
from fetcher_utils import DB_PATH

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
            resp = requests.post(url, json=json_data, headers=SYNC_HEADERS, timeout=timeout)
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


def _push_table_to_render(table, columns, pk, create_sql=None, where=None, batch_size=500, clear_first=False):
    """
    通用全表同步：把本機任意資料表 push 到 Render
    table: 表名
    columns: 欄位列表
    pk: 主鍵欄位列表（用於 UPSERT）
    create_sql: 建表 SQL（Render 端自動建表）
    where: 可選的 WHERE 條件
    clear_first: 由 Render 端在同一 transaction 裡清空+寫入（避免空窗期）
    """
    conn = sqlite3.connect(DB_PATH)
    col_str = ','.join(columns)
    sql = f"SELECT {col_str} FROM {table}"
    if where:
        sql += f" {where}"
    rows = conn.execute(sql).fetchall()
    conn.close()

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

    msg = f"  [{table}] {len(data)} 筆"
    if failed:
        msg += f"（{failed} 筆失敗）"
    print(msg)
    return len(data) - failed


def get_last_sync_result():
    """供 health API 讀取最近一次同步結果"""
    return _last_sync_result


def _push_all_to_render():
    """一次 push 所有資料到 Render — 通用全表同步"""
    global _last_sync_result
    if _is_cloud():
        return
    print("[全量同步] 開始 push 到 Render...")
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
                        'val_level','val_aa','val_a1','val_a2','val_a','val_lt6','discount_pct'],
            'pk': ['stock_id','date'],
            'create_sql': """CREATE TABLE IF NOT EXISTS stock_state (
                stock_id TEXT NOT NULL, date TEXT NOT NULL,
                price REAL, price_pos REAL, fair_low REAL, fair_mid REAL, fair_high REAL,
                shen_eps REAL, shen_pe REAL, shen_yld REAL, fin_grade TEXT, updated_at TEXT,
                val_level TEXT, val_aa REAL, val_a1 REAL, val_a2 REAL, val_a REAL,
                val_lt6 REAL, discount_pct REAL,
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
            'table': 'user_lists',
            'columns': ['list_type','code','added_at','price_at'],
            'pk': ['list_type','code'],
            'clear_first': True,
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
            'columns': ['code','chk_1','chk_2','chk_3','chk_4','chk_5','chk_6',
                        'chk_7','chk_8','chk_9','chk_10','chk_11','chk_12','chk_13',
                        'pass_count','total_count','detail',
                        'eps_setting','div_setting','yld_high','yld_max','pe_high','pe_low',
                        'lt_div','lt_yld','val_a','val_a1','val_a2','val_aa','lt5','lt6','lt7',
                        'updated_at'],
            'pk': ['code'],
            'create_sql': """CREATE TABLE IF NOT EXISTS stock_checklist (
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
                updated_at TEXT)""",
        },
        {
            'table': 'financial_detail',
            'columns': ['code','period','period_type','report_type','item','value','updated_at'],
            'pk': ['code','period','period_type','report_type','item'],
            'where': "WHERE DATE(updated_at) = DATE('now')",
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
            'create_sql': """CREATE TABLE IF NOT EXISTS focus_signals (
                code TEXT NOT NULL, date TEXT NOT NULL, signal_type TEXT NOT NULL,
                detail TEXT, PRIMARY KEY (code, date, signal_type))""",
            'where': "WHERE date >= date('now', '-30 days')",
        },
    ]

    for cfg in SYNC_TABLES:
        try:
            _push_table_to_render(
                table=cfg['table'],
                columns=cfg['columns'],
                pk=cfg['pk'],
                create_sql=cfg.get('create_sql'),
                where=cfg.get('where'),
                clear_first=cfg.get('clear_first', False),
            )
        except Exception as e:
            print(f"  [{cfg['table']}] 失敗: {e}")
            failures.append(f"{cfg['table']}: {e}")

    from datetime import datetime
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    is_ok = len(failures) == 0
    _last_sync_result = {
        'time': now_str,
        'last_success': now_str if is_ok else _last_sync_result.get('last_success'),
        'failures': failures,
        'ok': is_ok,
    }
    if failures:
        print(f"[全量同步] 完成，{len(failures)} 張表有失敗: {', '.join(failures)}")
    else:
        print("[全量同步] 完成，全部成功")


def _push_news_to_render():
    """把本機今天的新聞 push 到 Render"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""SELECT code, name, date, subject, link, tier, matched_rule, direction, created_at
                               FROM material_news WHERE created_at > datetime('now', '-1 day')""").fetchall()
        conn.close()
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
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT code, year, pe_high, pe_low, updated_at FROM pe_history"
        ).fetchall()
        conn.close()

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


def _push_financial_detail_to_render():
    """本機完整財報明細 push 到 Render（只推當天更新的）"""
    if _is_cloud():
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("""SELECT code, period, period_type, report_type, item, value, updated_at
                              FROM financial_detail""").fetchall()
        conn.close()

        if not rows:
            print("[財報明細同步] 今天沒有更新")
            return

        cols = ['code', 'period', 'period_type', 'report_type', 'item', 'value', 'updated_at']
        data = [{cols[j]: r[j] for j in range(len(cols))} for r in rows]

        failed = 0
        for i in range(0, len(data), 500):
            batch = data[i:i+500]
            resp = _post_with_retry(f'{RENDER_URL}/api/sync/financial-detail',
                                    {'data': batch}, label=f'財報明細 batch {i//500+1}')
            if not resp:
                failed += len(batch)
        msg = f"[財報明細同步] 已 push {len(data)} 筆到 Render"
        if failed:
            msg += f"（{failed} 筆失敗）"
        print(msg)
    except Exception as e:
        print(f"[財報明細同步] 失敗: {e}")


def _push_financial_annual_to_render():
    """本機 financial_annual 整表 push 到 Render（只推當天更新的）"""
    if _is_cloud():
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("""SELECT code, year, revenue, cost, gross_profit, operating_expense,
                              operating_income, non_operating, pretax_income, tax, net_income,
                              net_income_parent, total_assets, total_equity, common_stock,
                              contract_liability, operating_cf, capex, eps,
                              weighted_shares, cash_dividend, stock_dividend, updated_at
                              FROM financial_annual
                              ORDER BY code, year""").fetchall()
        conn.close()

        cols = ['code','year','revenue','cost','gross_profit','operating_expense',
                'operating_income','non_operating','pretax_income','tax','net_income',
                'net_income_parent','total_assets','total_equity','common_stock',
                'contract_liability','operating_cf','capex','eps',
                'weighted_shares','cash_dividend','stock_dividend','updated_at']
        data = [{cols[j]: r[j] for j in range(len(cols)) if r[j] is not None} for r in rows]

        if not data:
            print("[年報同步] 今天沒有更新的年報")
            return

        failed = 0
        for i in range(0, len(data), 200):
            batch = data[i:i+200]
            resp = _post_with_retry(f'{RENDER_URL}/api/sync/financial-annual',
                                    {'data': batch}, label=f'年報 batch {i//200+1}')
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
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("""SELECT code, quarter, revenue, cost, gross_profit, operating_expense,
                              operating_income, non_operating, pretax_income, tax, continuing_income,
                              net_income_parent, eps, contract_liability, updated_at
                              FROM quarterly_financial
                              ORDER BY code, quarter""").fetchall()
        conn.close()

        cols = ['code','quarter','revenue','cost','gross_profit','operating_expense',
                'operating_income','non_operating','pretax_income','tax','continuing_income',
                'net_income_parent','eps','contract_liability','updated_at']
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
        conn = sqlite3.connect(DB_PATH)
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
            deepest_val_level, val_cheap_days
            FROM stocks WHERE close IS NOT NULL""").fetchall()
        conn.close()

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
            'deepest_val_level','val_cheap_days']
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
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("""SELECT code, close, change, open, high, low, volume,
                              revenue_date, revenue_year, revenue_month,
                              revenue_yoy, revenue_mom, revenue_cum_yoy
                              FROM stocks WHERE close IS NOT NULL""").fetchall()
        conn.close()
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
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT code, inst_foreign, inst_trust, inst_dealer FROM stocks WHERE inst_foreign IS NOT NULL").fetchall()
        conn.close()
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
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""SELECT code, sys_ann_eps, sys_ann_div, sys_ann_pe,
                               sys_ann_yld, sys_ann_confidence,
                               sys_est_eps, sys_est_quarter, sys_est_confidence
                               FROM stocks WHERE sys_ann_eps IS NOT NULL""").fetchall()
        conn.close()
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
