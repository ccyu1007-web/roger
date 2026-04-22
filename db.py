"""
db.py — 資料庫抽象層
本機開發用 SQLite，Render 部署用 PostgreSQL
用法：import db as sqlite3（取代原本的 import sqlite3）
"""

import os
import re

DATABASE_URL = os.environ.get('DATABASE_URL')

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

    # 連線 SSL 設定（Render 內部可用，外部必須）
    if 'sslmode' not in DATABASE_URL:
        sep = '&' if '?' in DATABASE_URL else '?'
        DATABASE_URL += f'{sep}sslmode=prefer'

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

    def connect(path=None):
        return _PGConnection()

# ═══════════════════════════════════════════════════════════════
#  SQLite 模式（本機開發）
# ═══════════════════════════════════════════════════════════════

else:
    import sqlite3 as _sqlite3

    DB_TYPE = 'sqlite'
    Row = _sqlite3.Row

    def connect(path=None):
        return _sqlite3.connect(path or 'stocks.db')
