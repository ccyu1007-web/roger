"""
db.py — 資料庫抽象層
本機開發用 SQLite，Render 部署用 PostgreSQL
用法：import db as sqlite3（取代原本的 import sqlite3）
"""

import os
import re
from contextlib import contextmanager

DATABASE_URL = (os.environ.get('DATABASE_URL') or '').strip() or None

# ── 表格主鍵映射（INSERT OR REPLACE 轉換用）──────────────────
TABLE_PK = {
    'stocks':               ['code'],
    'monthly_revenue':      ['code', 'year', 'month'],
    'financial_annual':     ['code', 'year'],
    'quarterly_financial':  ['code', 'quarter'],
    'pe_history':           ['code', 'year'],
    'etf_info':             ['code'],
    'etf_holdings':         ['etf_code', 'stock_code'],
    'content_fingerprint':  ['source'],
    'circuit_breaker':      ['source'],
    'api_health':           ['source'],
    'stock_state':          ['stock_id', 'date'],
    'material_news':        ['id'],
    'provider_switch_log':  ['id'],
    'daily_price':          ['code', 'date'],
    'focus_tracking':       ['code'],
    'focus_signals':        ['code', 'date', 'signal_type'],
    'user_lists':           ['list_type', 'code'],
    'user_notes':           ['code'],
    'user_estimates':       ['code'],
    'user_settings':        ['key'],
}


def _adapt_sql(sql):
    """將 SQLite SQL 轉換為 PostgreSQL 語法"""
    stripped = sql.strip()

    # PRAGMA → 空操作
    if stripped.upper().startswith('PRAGMA'):
        return "SELECT 1"

    upper = stripped.upper()

    # 偵測 INSERT OR IGNORE / REPLACE
    has_ignore = bool(re.search(r'\bINSERT\s+OR\s+IGNORE\b', upper))
    has_replace = bool(re.search(r'\bINSERT\s+OR\s+REPLACE\b', upper))
    sql = re.sub(r'\bINSERT\s+OR\s+IGNORE\b', 'INSERT', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bINSERT\s+OR\s+REPLACE\b', 'INSERT', sql, flags=re.IGNORECASE)

    # 具名參數 :name → %(name)s（避開 PostgreSQL :: 轉型）
    sql = re.sub(r'(?<!:):([a-zA-Z_]\w*)', r'%(\1)s', sql)

    # 位置參數 ? → %s
    sql = sql.replace('?', '%s')

    # datetime('now') → CURRENT_TIMESTAMP
    sql = re.sub(r"datetime\('now'\)", "CURRENT_TIMESTAMP", sql)
    sql = re.sub(
        r"datetime\('now',\s*'(-?\d+)\s+(days?|hours?|minutes?)'\)",
        lambda m: f"CURRENT_TIMESTAMP + INTERVAL '{m.group(1)} {m.group(2)}'",
        sql
    )

    # date('now') → CURRENT_DATE::TEXT
    sql = re.sub(r"date\('now'\)", "CURRENT_DATE::TEXT", sql)
    sql = re.sub(
        r"date\('now',\s*'(-?\d+)\s+(days?|hours?|minutes?)'\)",
        lambda m: f"(CURRENT_DATE + INTERVAL '{m.group(1)} {m.group(2)}')::TEXT",
        sql
    )

    # GROUP_CONCAT → STRING_AGG
    sql = sql.replace('GROUP_CONCAT(', 'STRING_AGG(')

    # INSTR(str, 'sub') → POSITION('sub' IN str)
    sql = re.sub(
        r"\bINSTR\((\w+),\s*'([^']+)'\)",
        r"POSITION('\2' IN \1)",
        sql
    )

    # INTEGER PRIMARY KEY AUTOINCREMENT → SERIAL PRIMARY KEY
    sql = re.sub(
        r'\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b',
        'SERIAL PRIMARY KEY',
        sql, flags=re.IGNORECASE
    )

    # ALTER TABLE ADD COLUMN → ADD COLUMN IF NOT EXISTS
    sql = re.sub(
        r'\bADD\s+COLUMN\s+(?!IF\b)',
        'ADD COLUMN IF NOT EXISTS ',
        sql, flags=re.IGNORECASE
    )

    # INSERT OR IGNORE → ON CONFLICT DO NOTHING
    if has_ignore:
        sql = sql.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'
    elif has_replace:
        match = re.search(r'INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)', sql, re.IGNORECASE)
        if match:
            table = match.group(1)
            cols = [c.strip() for c in match.group(2).split(',')]
            pk_cols = TABLE_PK.get(table, [cols[0]])
            non_pk = [c for c in cols if c not in pk_cols]
            conflict = ', '.join(pk_cols)
            if non_pk:
                update = ', '.join(f'{c}=EXCLUDED.{c}' for c in non_pk)
                sql = sql.rstrip().rstrip(';') + \
                    f' ON CONFLICT ({conflict}) DO UPDATE SET {update}'
            else:
                sql = sql.rstrip().rstrip(';') + \
                    f' ON CONFLICT ({conflict}) DO NOTHING'

    return sql


# ═══════════════════════════════════════════════════════════════
#  PostgreSQL 模式（Render 雲端）
# ═══════════════════════════════════════════════════════════════

if DATABASE_URL:
    import psycopg2
    import psycopg2.extras

    # Render 有時給 postgres:// 而非 postgresql://
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

    # 清理 URL（移除所有空白、換行）再加回正確的 sslmode
    DATABASE_URL = re.sub(r'\s+', '', DATABASE_URL)
    DATABASE_URL = re.sub(r'[?&]sslmode=[^&]*', '', DATABASE_URL)
    DATABASE_URL += '?sslmode=require'

    DB_TYPE = 'postgresql'

    class _PGCursor:
        """包裝 psycopg2 cursor，自動轉換 SQL"""
        def __init__(self, cursor):
            self._cur = cursor

        def execute(self, sql, args=None):
            sql = _adapt_sql(sql)
            if args is not None and isinstance(args, list):
                args = tuple(args)
            self._cur.execute(sql, args or None)
            return self

        def executemany(self, sql, args_list):
            sql = _adapt_sql(sql)
            for args in args_list:
                if isinstance(args, list):
                    args = tuple(args)
                self._cur.execute(sql, args)
            return self

        def fetchall(self):
            try:
                return self._cur.fetchall()
            except psycopg2.ProgrammingError:
                return []

        def fetchone(self):
            try:
                return self._cur.fetchone()
            except psycopg2.ProgrammingError:
                return None

        @property
        def rowcount(self):
            return self._cur.rowcount

        @property
        def description(self):
            return self._cur.description

    class _PGConnection:
        """包裝 psycopg2 connection，提供 SQLite 相容介面"""
        def __init__(self):
            self._conn = psycopg2.connect(DATABASE_URL)
            self._conn.autocommit = False
            self._use_dict = False

        @property
        def row_factory(self):
            return self._use_dict

        @row_factory.setter
        def row_factory(self, value):
            # 任何非 None 值都啟用 dict 模式（對應 sqlite3.Row）
            self._use_dict = bool(value)

        def cursor(self):
            if self._use_dict:
                raw = self._conn.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor)
            else:
                raw = self._conn.cursor()
            return _PGCursor(raw)

        def execute(self, sql, args=None):
            cur = self.cursor()
            cur.execute(sql, args)
            return cur

        def commit(self):
            self._conn.commit()

        def close(self):
            try:
                self._conn.close()
            except Exception:
                pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

    class Row:
        """佔位類別，讓 conn.row_factory = sqlite3.Row 語法通用"""
        pass

    def connect(path=None, **kwargs):
        return _PGConnection()

# ═══════════════════════════════════════════════════════════════
#  SQLite 模式（本機開發）
# ═══════════════════════════════════════════════════════════════

else:
    import sqlite3 as _sqlite3

    DB_TYPE = 'sqlite'
    Row = _sqlite3.Row

    def connect(path=None, timeout=30):
        conn = _sqlite3.connect(path or 'stocks.db', timeout=timeout)
        conn.execute("PRAGMA busy_timeout=5000")  # 遇鎖等 5 秒再失敗
        conn.execute("PRAGMA journal_mode=WAL")    # WAL 模式：讀寫不互擋
        return conn


@contextmanager
def get_conn(path=None, row_factory=False, timeout=30):
    """Context manager：自動關閉 DB 連線，避免 leak。
    用法：with sqlite3.get_conn(row_factory=True) as conn:
    """
    if DB_TYPE == 'sqlite':
        conn = connect(path=path, timeout=timeout)
    else:
        conn = connect(path=path)
    if row_factory:
        conn.row_factory = Row
    try:
        yield conn
    finally:
        conn.close()


# ── 索引建立（提升查詢效能）──────────────────────────────────
def ensure_indexes(conn):
    """建立常用查詢索引，已存在則跳過"""
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_monthly_revenue_code ON monthly_revenue(code)",
        "CREATE INDEX IF NOT EXISTS idx_monthly_revenue_code_year ON monthly_revenue(code, year)",
        "CREATE INDEX IF NOT EXISTS idx_monthly_revenue_year ON monthly_revenue(year)",
        "CREATE INDEX IF NOT EXISTS idx_quarterly_financial_code ON quarterly_financial(code)",
        "CREATE INDEX IF NOT EXISTS idx_financial_annual_code ON financial_annual(code)",
        "CREATE INDEX IF NOT EXISTS idx_financial_annual_code_year ON financial_annual(code, year)",
        "CREATE INDEX IF NOT EXISTS idx_financial_annual_year ON financial_annual(year)",
        "CREATE INDEX IF NOT EXISTS idx_etf_holdings_stock_code ON etf_holdings(stock_code)",
        "CREATE INDEX IF NOT EXISTS idx_etf_holdings_etf_code ON etf_holdings(etf_code)",
        "CREATE INDEX IF NOT EXISTS idx_stock_state_code ON stock_state(stock_id)",
        "CREATE INDEX IF NOT EXISTS idx_stock_state_date ON stock_state(date)",
        "CREATE INDEX IF NOT EXISTS idx_daily_price_code ON daily_price(code)",
        "CREATE INDEX IF NOT EXISTS idx_pe_history_code ON pe_history(code)",
        "CREATE INDEX IF NOT EXISTS idx_material_news_code ON material_news(code)",
        "CREATE INDEX IF NOT EXISTS idx_focus_signals_code ON focus_signals(code)",
        "CREATE INDEX IF NOT EXISTS idx_stock_checklist_code ON stock_checklist(code)",
    ]
    c = conn.cursor() if hasattr(conn, 'cursor') else conn
    for sql in indexes:
        try:
            c.execute(sql)
        except Exception:
            pass
    if hasattr(conn, 'commit'):
        conn.commit()
