"""
migrate_to_pg.py — 將本機 SQLite 資料匯入 Render PostgreSQL
用法：
  DATABASE_URL="postgresql://user:pass@host/db" python migrate_to_pg.py
"""

import sqlite3
import psycopg2
import psycopg2.extras
import os
import sys

SQLITE_PATH = "stocks.db"
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    print("請設定 DATABASE_URL 環境變數")
    print('  DATABASE_URL="postgresql://user:pass@host/db" python migrate_to_pg.py')
    sys.exit(1)

if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)


def get_sqlite_tables(sconn):
    """取得所有 SQLite 表格名稱"""
    c = sconn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return [r[0] for r in c.fetchall()]


def get_table_info(sconn, table):
    """取得表格的欄位資訊"""
    c = sconn.cursor()
    c.execute(f"PRAGMA table_info({table})")
    return c.fetchall()  # (cid, name, type, notnull, default, pk)


def sqlite_type_to_pg(col_type, is_pk, is_autoincrement):
    """SQLite 型別轉 PostgreSQL"""
    col_type = col_type.upper() if col_type else 'TEXT'
    if is_autoincrement and is_pk:
        return 'SERIAL'
    if col_type in ('INTEGER', 'INT'):
        return 'INTEGER'
    if col_type in ('REAL', 'FLOAT', 'DOUBLE'):
        return 'DOUBLE PRECISION'
    return 'TEXT'


def create_pg_table(pgconn, sconn, table):
    """根據 SQLite schema 在 PostgreSQL 建立對應表格"""
    cols_info = get_table_info(sconn, table)
    if not cols_info:
        return False

    # 檢查是否有 AUTOINCREMENT
    c = sconn.cursor()
    c.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
    create_sql = c.fetchone()
    has_auto = create_sql and 'AUTOINCREMENT' in create_sql[0].upper() if create_sql else False

    # 找出主鍵
    pk_cols = [col[1] for col in cols_info if col[5] > 0]

    col_defs = []
    for col in cols_info:
        cid, name, ctype, notnull, default, pk = col
        is_auto = has_auto and pk > 0 and ctype.upper() == 'INTEGER'
        pg_type = sqlite_type_to_pg(ctype, pk > 0, is_auto)

        parts = [f'"{name}"', pg_type]
        if notnull and not is_auto:
            parts.append('NOT NULL')
        if default is not None and not is_auto:
            # SQLite 的字串預設值可能帶引號格式不同，統一處理
            d = str(default)
            if d.startswith("'") or d.startswith('"'):
                d = f"'{d.strip(chr(39)).strip(chr(34))}'"
            elif not d.replace('.','').replace('-','').lstrip().isdigit():
                d = f"'{d}'"
            parts.append(f'DEFAULT {d}')
        col_defs.append(' '.join(parts))

    # 主鍵
    if pk_cols:
        # 如果是 SERIAL 主鍵，已在欄位定義中處理
        has_serial = any(
            has_auto and col[5] > 0 and col[2].upper() == 'INTEGER'
            for col in cols_info
        )
        if not has_serial:
            pk_str = ', '.join(f'"{c}"' for c in pk_cols)
            col_defs.append(f'PRIMARY KEY ({pk_str})')

    # UNIQUE 約束
    c.execute(f"PRAGMA index_list({table})")
    for idx in c.fetchall():
        if idx[2]:  # unique
            c2 = sconn.cursor()
            c2.execute(f"PRAGMA index_info({idx[1]})")
            idx_cols = [ic[2] for ic in c2.fetchall()]
            if idx_cols and set(idx_cols) != set(pk_cols):
                unique_str = ', '.join(f'"{uc}"' for uc in idx_cols)
                col_defs.append(f'UNIQUE ({unique_str})')

    sql = f'CREATE TABLE IF NOT EXISTS "{table}" (\n  ' + \
          ',\n  '.join(col_defs) + '\n)'

    pgcur = pgconn.cursor()
    pgcur.execute(sql)
    pgconn.commit()
    return True


def migrate_table(sconn, pgconn, table):
    """搬移單一表格的資料"""
    sc = sconn.cursor()
    sc.execute(f"SELECT COUNT(*) FROM {table}")
    total = sc.fetchone()[0]
    if total == 0:
        print(f"  {table}: 空表，跳過")
        return 0

    # 取欄位名
    cols_info = get_table_info(sconn, table)
    col_names = [c[1] for c in cols_info]

    # 檢查是否有 AUTOINCREMENT 的 id 欄位
    sc.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
    create_sql = sc.fetchone()
    has_auto = create_sql and 'AUTOINCREMENT' in create_sql[0].upper() if create_sql else False

    # 排除 SERIAL 欄位（讓 PostgreSQL 自動產生）
    insert_cols = col_names
    if has_auto:
        auto_col = None
        for col in cols_info:
            if col[5] > 0 and col[2].upper() == 'INTEGER':
                auto_col = col[1]
                break
        if auto_col:
            insert_cols = [c for c in col_names if c != auto_col]

    # 批次讀取 + 寫入
    sc.execute(f'SELECT {", ".join(insert_cols)} FROM "{table}"')
    pgcur = pgconn.cursor()
    batch_size = 500
    migrated = 0

    while True:
        rows = sc.fetchmany(batch_size)
        if not rows:
            break
        placeholders = ', '.join(['%s'] * len(insert_cols))
        cols_str = ', '.join(f'"{c}"' for c in insert_cols)
        sql = f'INSERT INTO "{table}" ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'
        for row in rows:
            try:
                pgcur.execute(sql, row)
                migrated += 1
            except Exception as e:
                pgconn.rollback()
                print(f"    跳過一筆: {e}")
                continue
    pgconn.commit()

    # 如果有 SERIAL 欄位，更新 sequence
    if has_auto:
        for col in cols_info:
            if col[5] > 0 and col[2].upper() == 'INTEGER':
                try:
                    pgcur.execute(f"""
                        SELECT setval(pg_get_serial_sequence('"{table}"', '{col[1]}'),
                               COALESCE((SELECT MAX("{col[1]}") FROM "{table}"), 1))
                    """)
                    pgconn.commit()
                except Exception:
                    pgconn.rollback()
                break

    print(f"  {table}: {migrated}/{total} 筆")
    return migrated


def main():
    print(f"SQLite: {SQLITE_PATH}")
    print(f"PostgreSQL: {DATABASE_URL[:50]}...")
    print()

    sconn = sqlite3.connect(SQLITE_PATH)
    pgconn = psycopg2.connect(DATABASE_URL)

    tables = get_sqlite_tables(sconn)
    print(f"找到 {len(tables)} 個表格: {', '.join(tables)}")
    print()

    # 1. 建立表格
    print("=== 建立表格 ===")
    for table in tables:
        ok = create_pg_table(pgconn, sconn, table)
        print(f"  {table}: {'OK' if ok else 'SKIP'}")
    print()

    # 2. 搬移資料
    print("=== 搬移資料 ===")
    total = 0
    for table in tables:
        total += migrate_table(sconn, pgconn, table)
    print(f"\n完成！共搬移 {total} 筆資料")

    sconn.close()
    pgconn.close()


if __name__ == "__main__":
    main()
