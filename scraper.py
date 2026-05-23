"""
爬蟲模組：從證交所 & 櫃買中心抓取上市/上櫃股票資料
資料來源：
  - 上市公司代碼+名稱：證交所 TWSE（t187ap03_L）
  - 上櫃公司代碼+名稱：櫃買中心 TPEX（tpex_mainboard_peratio_analysis）
  - 上市批次收盤價：證交所 TWSE OpenAPI（STOCK_DAY_ALL）
  - 上櫃批次收盤價：櫃買中心 TPEX OpenAPI（tpex_mainboard_quotes）
  - 即時股價（上市+上櫃）：證交所 MIS（mis.twse.com.tw，上市用 tse_ 前綴、上櫃用 otc_ 前綴）
  - 上櫃歷史：TPEX 每日收盤行情批次 API
  - 營收：MOPS t21sc03（即時）/ 政府API t187ap05（批次）
  - EPS：MOPS t163sb04（季報）/ 群益 zce（逐支）/ TWSE/TPEX t187ap14（批次）
  - 年度EPS：群益年度損益表（逐支）/ 政府API t187ap14 / TWSE/TPEX BWIBBU 反推
  - 三大法人：群益證券 zcl（每天17:10後抓取）
"""

import logging
import requests
import db as sqlite3
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

_TW = ZoneInfo("Asia/Taipei")
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
import re
import os
import signal
import fcntl
from bs4 import BeautifulSoup

IS_CLOUD = os.environ.get('DATABASE_URL') is not None

logger = logging.getLogger(__name__)
from fetcher_utils import (
    create_session, parse_num as safe_float,
    parse_int as safe_int, DB_PATH
)

from guardian import (backup_raw_response, cleanup_old_backups,
                      validate_batch, get_breaker, get_priority_queue,
                      arbitrate_values,
                      get_active_provider, log_provider_switch,
                      sanity_check, audit_changes,
                      snapshot_stock_states, fetch_material_news,
                      fetch_moneydj_news, auto_archive_old_news,
                      focus_signal_check)
from render_sync import (
    _push_table_to_render, _push_all_to_render, _push_news_to_render,
    _push_pe_history_to_render,
    _push_financial_annual_to_render, _push_quarterly_to_render,
    _push_annual_to_render, _push_prices_to_render,
    _push_institutional_to_render, _push_estimates_to_render
)
from estimation import (
    estimate_system_eps, estimate_system_eps_multi, estimate_annual_eps,
    _batch_system_estimate, _batch_annual_estimate, _backfill_actual_eps,
    _init_eps_log_db, _log_estimate
)

_session = create_session(ua="Mozilla/5.0 (compatible; StockBot/1.0)")
# Render 環境 SSL 憑證無法驗證 twse/tpex → 關閉驗證
if os.environ.get('DATABASE_URL'):
    _session.verify = False
    import urllib3; urllib3.disable_warnings()

# 批次 API 回傳的資料日期（ROC 格式，如 "1150421"）
_twse_batch_date = None

# ── 防呆機制：Lock file + 整體超時 ──────────────────────────
LOCK_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
LOCK_FILE = os.path.join(LOCK_DIR, 'scraper.lock')

class _TimeoutError(Exception):
    pass

def _timeout_handler(signum, frame):
    raise _TimeoutError("執行超時")

class ScraperLock:
    """
    檔案鎖：防止 run() 和 quick_update() 同時操作 DB。
    使用 fcntl.flock 非阻塞鎖，進程結束自動釋放。
    """
    def __init__(self, name, timeout_sec=None):
        self.name = name
        self.timeout_sec = timeout_sec
        self._fd = None
        self._old_handler = None

    def __enter__(self):
        os.makedirs(LOCK_DIR, exist_ok=True)
        self._fd = open(LOCK_FILE, 'w')
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError):
            self._fd.close()
            self._fd = None
            print(f"[{self.name}] 另一個 scraper 正在執行，跳過本次")
            return None
        self._fd.write(f"{os.getpid()} {self.name} {datetime.now().isoformat()}\n")
        self._fd.flush()
        # 設定整體超時（僅主進程，非 thread）
        if self.timeout_sec and hasattr(signal, 'SIGALRM'):
            self._old_handler = signal.getsignal(signal.SIGALRM)
            signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(self.timeout_sec)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.timeout_sec and hasattr(signal, 'SIGALRM'):
            signal.alarm(0)
            if self._old_handler:
                signal.signal(signal.SIGALRM, self._old_handler)
        if self._fd:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            self._fd.close()
        if exc_type is _TimeoutError:
            print(f"[{self.name}] 超時（{self.timeout_sec}秒），強制結束")
            return True  # suppress exception
        return False


def _today_roc():
    """今天的民國日期字串，如 '1150421'"""
    t = date.today()
    return f"{t.year - 1911}{t.strftime('%m%d')}"


# ── 資料庫初始化 ────────────────────────────────────────────
def init_db():
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                code          TEXT NOT NULL UNIQUE,
                name          TEXT,
                market        TEXT,
                close         REAL,
                change        REAL,
                open          REAL,
                high          REAL,
                low           REAL,
                volume        INTEGER,
                change_240d   REAL,
                revenue_date  TEXT,
                revenue_year  INTEGER,
                revenue_month INTEGER,
                revenue_yoy   REAL,
                revenue_mom   REAL,
                revenue_cum_yoy REAL,
                eps_date      TEXT,
                eps_1         REAL,
                eps_1q        TEXT,
                eps_2         REAL,
                eps_2q        TEXT,
                eps_3         REAL,
                eps_3q        TEXT,
                eps_4         REAL,
                eps_4q        TEXT,
                eps_5         REAL,
                eps_5q        TEXT,
                updated_at    TEXT
            )
        """)
        new_cols = [
            ("change_240d",     "REAL"),
            ("revenue_date",    "TEXT"),
            ("revenue_year",    "INTEGER"),
            ("revenue_month",   "INTEGER"),
            ("revenue_yoy",     "REAL"),
            ("revenue_mom",     "REAL"),
            ("revenue_cum_yoy", "REAL"),
            ("eps_date",        "TEXT"),
            ("eps_1",           "REAL"),
            ("eps_1q",          "TEXT"),
            ("eps_2",           "REAL"),
            ("eps_2q",          "TEXT"),
            ("eps_3",           "REAL"),
            ("eps_3q",          "TEXT"),
            ("eps_4",           "REAL"),
            ("eps_4q",          "TEXT"),
            ("eps_5",           "REAL"),
            ("eps_5q",          "TEXT"),
            ("eps_y1",          "REAL"),
            ("eps_y1_label",    "TEXT"),
            ("eps_y2",          "REAL"),
            ("eps_y2_label",    "TEXT"),
            ("eps_y3",          "REAL"),
            ("eps_y3_label",    "TEXT"),
            ("eps_y4",          "REAL"),
            ("eps_y4_label",    "TEXT"),
            ("eps_y5",          "REAL"),
            ("eps_y5_label",    "TEXT"),
            ("eps_y6",          "REAL"),
            ("eps_y6_label",    "TEXT"),
            ("eps_ytd",         "REAL"),
            ("eps_ytd_label",   "TEXT"),
            ("div_c1",          "REAL"),
            ("div_s1",          "REAL"),
            ("div_1_label",     "TEXT"),
            ("div_c2",          "REAL"),
            ("div_s2",          "REAL"),
            ("div_2_label",     "TEXT"),
            ("div_c3",          "REAL"),
            ("div_s3",          "REAL"),
            ("div_3_label",     "TEXT"),
            ("div_c4",          "REAL"),
            ("div_s4",          "REAL"),
            ("div_4_label",     "TEXT"),
            ("div_c5",          "REAL"),
            ("div_s5",          "REAL"),
            ("div_5_label",     "TEXT"),
            ("div_c6",          "REAL"),
            ("div_s6",          "REAL"),
            ("div_6_label",     "TEXT"),
            ("contract_1",      "REAL"),
            ("contract_1q",     "TEXT"),
            ("contract_2",      "REAL"),
            ("contract_2q",     "TEXT"),
            ("contract_3",      "REAL"),
            ("contract_3q",     "TEXT"),
            ("industry",        "TEXT"),
            ("fin_grade_1",     "TEXT"),
            ("fin_grade_1y",    "TEXT"),
            ("fin_grade_2",     "TEXT"),
            ("fin_grade_2y",    "TEXT"),
            ("fin_grade_3",     "TEXT"),
            ("fin_grade_3y",    "TEXT"),
            ("fin_grade_4",     "TEXT"),
            ("fin_grade_4y",    "TEXT"),
            ("fin_grade_5",     "TEXT"),
            ("fin_grade_5y",    "TEXT"),
            ("fin_grade_6",     "TEXT"),
            ("fin_grade_6y",    "TEXT"),
            ("price_pos",       "INTEGER"),
            ("fair_low",        "REAL"),
            ("fair_high",       "REAL"),
            ("inst_foreign",    "INTEGER"),
            ("inst_trust",      "INTEGER"),
            ("inst_dealer",     "INTEGER"),
        ]
        for col, typ in new_cols:
            try:
                c.execute(f"ALTER TABLE stocks ADD COLUMN {col} {typ}")
            except Exception: pass
        # ── 每日價量歷史（給 MA20 和量能訊號用）──
        c.execute("""CREATE TABLE IF NOT EXISTS daily_price (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            close_price REAL,
            volume INTEGER,
            PRIMARY KEY (code, date)
        )""")
        # ── 重點追蹤 ──
        c.execute("""CREATE TABLE IF NOT EXISTS focus_tracking (
            code TEXT PRIMARY KEY,
            focus_date TEXT,
            focus_price REAL,
            signal_mode TEXT DEFAULT 'initial',
            mode_switch_date TEXT,
            last_signal_date TEXT,
            last_signal_type TEXT,
            note TEXT
        )""")
        # ── 重點追蹤訊號歷史 ──
        c.execute("""CREATE TABLE IF NOT EXISTS focus_signals (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            detail TEXT,
            PRIMARY KEY (code, date, signal_type)
        )""")
        conn.commit()
    print("[DB] 資料表已就緒")


# ── 工具函式 ────────────────────────────────────────────────
_health_log = []

def _log_api_health(source, description, success, record_count=0):
    """暫存健康記錄，由 _flush_health_log 批次寫入"""
    _health_log.append((source, description, success, record_count,
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


def _flush_health_log():
    """批次寫入所有健康記錄"""
    global _health_log
    if not _health_log:
        return
    try:
        with sqlite3.get_conn() as conn:
            c = conn.cursor()
            for source, description, success, record_count, now in _health_log:
                c.execute("SELECT source FROM api_health WHERE source = ?", (source,))
                if c.fetchone():
                    if success:
                        c.execute("""UPDATE api_health SET last_success=?, last_record_count=?,
                                     fail_count=0, status='ok', description=? WHERE source=?""",
                                  (now, record_count, description, source))
                    else:
                        c.execute("""UPDATE api_health SET last_fail=?,
                                     fail_count=fail_count+1,
                                     status=CASE WHEN fail_count >= 2 THEN 'error' ELSE 'warning' END,
                                     description=? WHERE source=?""",
                                  (now, description, source))
                else:
                    st = 'ok' if success else 'warning'
                    c.execute("""INSERT INTO api_health (source, description, last_success, last_fail,
                                 fail_count, last_record_count, status)
                                 VALUES (?,?,?,?,?,?,?)""",
                              (source, description,
                               now if success else None,
                               None if success else now,
                               0 if success else 1,
                               record_count, st))
            conn.commit()
        _health_log = []
    except Exception as e:
        logger.warning(f"[Health] flush 失敗: {e}")



# safe_float / safe_int 已移至 fetcher_utils.py（透過 import as 保持相容）

def fetch_json(url, retries=3, backup_as=None):
    """
    抓取 JSON API。
    backup_as: 若指定來源名稱，成功後自動完整備份（指紋去重）
    """
    for i in range(retries):
        try:
            r = _session.get(url, timeout=15)
            r.raise_for_status()
            data = r.json()
            # 自動備份（指紋去重，資料沒變不存）
            if backup_as and data:
                raw = data if isinstance(data, list) else data.get('data', data)
                backup_raw_response(backup_as, raw)
            return data
        except Exception as e:
            logger.warning(f"第 {i+1} 次請求失敗：{e}")
            if i < retries - 1:
                time.sleep(1)
    return None

def date_to_quarter_label(date_str):
    """'2025-12-31' → '114Q4'"""
    try:
        d = datetime.strptime(date_str, '%Y-%m-%d')
        roc_year = d.year - 1911
        quarter = (d.month - 1) // 3 + 1
        return f"{roc_year}Q{quarter}"
    except Exception as e:
        logger.debug(f"date_to_quarter_label('{date_str}') 失敗: {e}")
        return None


# ── 上市股票（TWSE 證交所）──────────────────────────────────
# 公司代碼+名稱：t187ap03_L（白名單）
# 批次收盤價：STOCK_DAY_ALL（含 code, name, close, change, open, high, low, volume）
# 新上市公司會自動出現在 API 回傳中，save_to_db() 會自動 INSERT
def fetch_twse():
    print("[TWSE] 抓取上市公司清單...")
    company_list = fetch_json("https://openapi.twse.com.tw/v1/openData/t187ap03_L")
    if not company_list:
        print("[TWSE] 公司清單抓取失敗")
        return []
    whitelist = {str(r.get("公司代號", "")).strip() for r in company_list}
    print(f"[TWSE] 上市公司白名單：{len(whitelist)} 家")

    print("[TWSE] 抓取上市股價...")
    price_data = fetch_json("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", backup_as='price_twse')
    if not price_data:
        print("[TWSE] 股價抓取失敗")
        return []

    # 記錄批次 API 的資料日期
    global _twse_batch_date
    if price_data:
        _twse_batch_date = str(price_data[0].get("Date", "")).strip()
        print(f"[TWSE] 批次 API 資料日期: {_twse_batch_date}（今天: {_today_roc()}）")

    rows = []
    for item in price_data:
        code = str(item.get("Code", "")).strip()
        if code not in whitelist:
            continue
        rows.append({
            "code":   code,
            "name":   str(item.get("Name", "")).strip(),
            "market": "上市",
            "close":  safe_float(item.get("ClosingPrice")),
            "change": safe_float(item.get("Change")),
            "open":   safe_float(item.get("OpeningPrice")),
            "high":   safe_float(item.get("HighestPrice")),
            "low":    safe_float(item.get("LowestPrice")),
            "volume": safe_int(item.get("TradeVolume")),
        })
    print(f"[TWSE] 取得 {len(rows)} 筆上市公司股價")
    _log_api_health('price_twse', '股價(上市) TWSE', True, len(rows))
    return rows


# ── 上櫃股票（TPEX 櫃買中心）──────────────────────────────────
# 公司代碼+名稱：tpex_mainboard_peratio_analysis（白名單）
# 批次收盤價：tpex_mainboard_quotes（含 code, name, close, change, open, high, low, volume）
# 新上櫃公司會自動出現在 API 回傳中，save_to_db() 會自動 INSERT
def fetch_tpex():
    print("[TPEX] 抓取上櫃公司清單...")
    company_list = fetch_json("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis")
    if not company_list:
        print("[TPEX] 公司清單抓取失敗")
        return []
    whitelist = {str(r.get("SecuritiesCompanyCode", "")).strip() for r in company_list}
    print(f"[TPEX] 上櫃公司白名單：{len(whitelist)} 家")

    print("[TPEX] 抓取上櫃股價...")
    price_data = fetch_json("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", backup_as='price_tpex')
    if not price_data:
        print("[TPEX] 股價抓取失敗")
        return []

    rows = []
    for item in price_data:
        code = str(item.get("SecuritiesCompanyCode", "")).strip()
        if code not in whitelist:
            continue
        rows.append({
            "code":   code,
            "name":   str(item.get("CompanyName", "")).strip(),
            "market": "上櫃",
            "close":  safe_float(item.get("Close")),
            "change": safe_float(item.get("Change")),
            "open":   safe_float(item.get("Open")),
            "high":   safe_float(item.get("High")),
            "low":    safe_float(item.get("Low")),
            "volume": safe_int(item.get("TradingShares")),
        })
    print(f"[TPEX] 取得 {len(rows)} 筆上櫃公司股價")
    _log_api_health('price_tpex', '股價(上櫃) TPEX', True, len(rows))
    return rows


# ── 240日歷史股價（上市：TWSE MI_INDEX）───────────────────
def fetch_twse_history_240d():
    print("[TWSE] 抓取 240 日前歷史收盤價...")
    today  = date.today()
    approx = today - timedelta(days=336)
    for offset in range(10):
        d = approx - timedelta(days=offset)
        if d.weekday() >= 5:
            continue
        ds  = d.strftime('%Y%m%d')
        url = (f"https://www.twse.com.tw/rwd/zh/afterTrading/"
               f"MI_INDEX?response=json&date={ds}&type=ALL")
        data = fetch_json(url)
        if not data:
            continue
        tables = data.get('tables', [])
        if len(tables) >= 9 and tables[8].get('data'):
            rows = tables[8]['data']
            hist = {}
            for row in rows:
                code      = str(row[0]).strip()
                close_str = str(row[8]).replace(',', '').strip()
                try:
                    hist[code] = float(close_str)
                except Exception: pass
            print(f"[TWSE] 找到歷史資料：{d}（{len(hist)} 筆）")
            return hist
    print("[TWSE] 找不到歷史資料")
    return {}


# ── 240日歷史股價（上櫃：TPEX 批次 API）────────────────────
def fetch_tpex_history_240d():
    print("[TPEX] 抓取 240 日前歷史收盤價（批次 API）...")
    today  = date.today()
    approx = today - timedelta(days=336)
    for offset in range(15):
        d = approx - timedelta(days=offset)
        if d.weekday() >= 5:
            continue
        roc_y = d.year - 1911
        roc_date = f'{roc_y}/{d.month:02d}/{d.day:02d}'
        url = (f"https://www.tpex.org.tw/web/stock/aftertrading/"
               f"otc_quotes_no1430/stk_wn1430_result.php"
               f"?l=zh-tw&d={roc_date}&se=EW")
        data = fetch_json(url)
        if not data:
            continue
        tables = data.get('tables', [])
        if tables and tables[0].get('data'):
            rows = tables[0]['data']
            hist = {}
            for row in rows:
                code      = str(row[0]).strip()
                close_str = str(row[2]).replace(',', '').strip()
                try:
                    hist[code] = float(close_str)
                except Exception: pass
            print(f"[TPEX] 找到歷史資料：{d}（{len(hist)} 筆）")
            return hist
    print("[TPEX] 找不到歷史資料")
    return {}


def calc_change_240d(current, hist):
    if current is None or hist is None or hist == 0:
        return None
    return round((current - hist) / hist * 100, 2)


# ── 讀取 DB 中的舊資料（DELETE 前備份）─────────────────────
def read_old_meta():
    try:
        with sqlite3.get_conn(row_factory=True) as conn:
            c = conn.cursor()
            c.execute("SELECT * FROM stocks")
            result = {row['code']: dict(row) for row in c.fetchall()}
            return result
    except Exception as e:
        logger.debug(f"read_old_meta 失敗: {e}")
        return {}


# ── 營收：從 DB 讀取（已由 quick_update 的 MOPS + 政府API 維護）────
def fetch_revenue(codes, old_meta):
    """營收資料直接從 DB 取，不再逐支抓取。由 quick_update 的 MOPS + 政府API 維護。"""
    results = {}
    for code in codes:
        old = old_meta.get(code, {})
        if old.get('revenue_yoy') is not None:
            results[code] = {
                'revenue_date':    old.get('revenue_date'),
                'revenue_year':    old.get('revenue_year'),
                'revenue_month':   old.get('revenue_month'),
                'revenue_yoy':     old.get('revenue_yoy'),
                'revenue_mom':     old.get('revenue_mom'),
                'revenue_cum_yoy': old.get('revenue_cum_yoy'),
            }
    hit = len(results)
    print(f"[營收] DB 已有 {hit}/{len(codes)} 筆（由 MOPS + 政府API 維護）")
    return results


# ── 股利：政府 API t187ap39 + t187ap45（批次，無限制）──────
def fetch_dividends_bulk():
    """從 TWSE/TPEX 批次取得歷史股利"""
    print("[股利] 抓取股利資料（批次）...")
    # {code: {year: {'cash': x, 'stock': x}}}
    div_map = {}

    def _parse_39(data, cash_key_prefix='股東配發內容-'):
        """解析 t187ap39 格式"""
        for d in data:
            code = str(d.get('公司代號', '')).strip()
            year = str(d.get('股利年度', '')).strip()
            if not code or not year:
                continue
            cash  = safe_float(d.get(f'{cash_key_prefix}盈餘分配之現金股利(元/股)')) or 0
            cash2 = safe_float(d.get(f'{cash_key_prefix}法定盈餘公積、資本公積發放之現金(元/股)')) or 0
            stock  = safe_float(d.get(f'{cash_key_prefix}盈餘轉增資配股(元/股)')) or 0
            stock2 = safe_float(d.get(f'{cash_key_prefix}法定盈餘公積、資本公積轉增資配股(元/股)')) or 0
            div_map.setdefault(code, {})
            prev = div_map[code].get(year, {'cash': 0, 'stock': 0})
            div_map[code][year] = {
                'cash':  round(prev['cash']  + cash + cash2, 4),
                'stock': round(prev['stock'] + stock + stock2, 4),
            }

    def _parse_45(data):
        """解析 t187ap45 格式"""
        for d in data:
            code = str(d.get('公司代號', '')).strip()
            year = str(d.get('股利年度', '')).strip()
            if not code or not year:
                continue
            cash  = safe_float(d.get('股東配發-盈餘分配之現金股利(元/股)')) or 0
            cash2 = safe_float(d.get('股東配發-法定盈餘公積發放之現金(元/股)')) or 0
            cash3 = safe_float(d.get('股東配發-資本公積發放之現金(元/股)')) or 0
            stock  = safe_float(d.get('股東配發-盈餘轉增資配股(元/股)')) or 0
            stock2 = safe_float(d.get('股東配發-法定盈餘公積轉增資配股(元/股)')) or 0
            stock3 = safe_float(d.get('股東配發-資本公積轉增資配股(元/股)')) or 0
            div_map.setdefault(code, {})
            prev = div_map[code].get(year, {'cash': 0, 'stock': 0})
            div_map[code][year] = {
                'cash':  round(prev['cash']  + cash + cash2 + cash3, 4),
                'stock': round(prev['stock'] + stock + stock2 + stock3, 4),
            }

    # TWSE 歷史 (107-110)
    data = fetch_json("https://openapi.twse.com.tw/v1/openData/t187ap39_L", backup_as='div_twse_t187ap39')
    if data:
        _parse_39(data)
        print(f"  TWSE 歷史：{len(data)} 筆")

    # TPEX 歷史 (107-110)
    data = fetch_json("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap39_O", backup_as='div_tpex_t187ap39')
    if data:
        _parse_39(data)
        print(f"  TPEX 歷史：{len(data)} 筆")

    print(f"[股利] 政府 API（107-110）共取得 {len(div_map)} 支")

    # ── 政府 t187ap45 補充 111 年以後 ──
    for label, url in [
        ("TWSE", "https://openapi.twse.com.tw/v1/openData/t187ap45_L"),
        ("TPEX", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap45_O"),
    ]:
        api_data = fetch_json(url)
        if not api_data:
            continue
        cnt = 0
        for d in api_data:
            code = str(d.get('公司代號', '')).strip()
            year = str(d.get('股利年度', '')).strip()
            if not code or not year:
                continue
            cash  = safe_float(d.get('股東配發-盈餘分配之現金股利(元/股)')) or 0
            cash2 = safe_float(d.get('股東配發-法定盈餘公積發放之現金(元/股)')) or 0
            cash3 = safe_float(d.get('股東配發-資本公積發放之現金(元/股)')) or 0
            stock  = safe_float(d.get('股東配發-盈餘轉增資配股(元/股)')) or 0
            stock2 = safe_float(d.get('股東配發-法定盈餘公積轉增資配股(元/股)')) or 0
            stock3 = safe_float(d.get('股東配發-資本公積轉增資配股(元/股)')) or 0
            # 同年度多筆（季配）加總
            existing = div_map.get(code, {}).get(year)
            if existing:
                div_map[code][year] = {
                    'cash':  round(existing['cash'] + cash + cash2 + cash3, 4),
                    'stock': round(existing['stock'] + stock + stock2 + stock3, 4),
                }
            else:
                div_map.setdefault(code, {})[year] = {
                    'cash':  round(cash + cash2 + cash3, 4),
                    'stock': round(stock + stock2 + stock3, 4),
                }
            cnt += 1
        print(f"  {label} t187ap45 補充：{cnt} 筆")
    print(f"[股利] 完成，共 {len(div_map)} 支")

    # 轉成每支股票最近 6 年
    results = {}
    for code, yearly in div_map.items():
        years_sorted = sorted(yearly.keys(), reverse=True)[:6]
        r = {}
        for i, y in enumerate(years_sorted, 1):
            r[f'div_c{i}']       = yearly[y]['cash']
            r[f'div_s{i}']       = yearly[y]['stock']
            r[f'div_{i}_label']  = y
        for i in range(len(years_sorted) + 1, 7):
            r[f'div_c{i}']      = None
            r[f'div_s{i}']      = None
            r[f'div_{i}_label'] = None
        results[code] = r

    print(f"[股利] 共取得 {len(results)} 支股票的股利資料")
    return results


# ── EPS 年度歷史：TWSE BWIBBU + TPEX 本益比反推（批次，無限制）──
def fetch_eps_annual_history():
    """從 TWSE/TPEX 的本益比資料反推近 5 年年度 EPS，不依賴 FinMind"""
    print("[年度EPS歷史] 從 TWSE/TPEX 本益比反推...")
    from datetime import date
    cur_roc = date.today().year - 1911

    # 每年 Q4 財報反映的大約日期（TWSE 格式）
    twse_dates = {}
    tpex_dates = {}
    for yr in range(cur_roc, cur_roc - 5, -1):
        west = yr + 1911
        # TWSE: 約隔年4月有Q4反映（3月也可能）
        for m in ['04', '03', '05']:
            twse_dates.setdefault(str(yr), []).append(f'{west+1}{m}01')
        # TPEX: 民國年/月
        for m in ['04', '03', '05']:
            tpex_dates.setdefault(str(yr), []).append(f'{yr+1}/{m}')

    result = {}  # {code: {year_label: eps}}

    # TWSE 上市
    for roc_yr, dates in twse_dates.items():
        found = False
        for dt in dates:
            url = (f"https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d"
                   f"?date={dt}&selectType=ALL&response=json")
            data = fetch_json(url)
            if not data or data.get('stat') != 'OK' or not data.get('data'):
                continue
            cnt = 0
            for row in data['data']:
                code = row[0].strip()
                price = safe_float(row[2])
                pe = safe_float(row[5])
                if price and pe and pe > 0:
                    result.setdefault(code, {})[roc_yr] = round(price / pe, 2)
                    cnt += 1
            print(f"  上市 {roc_yr}年: {cnt} 支")
            found = True
            break
        if not found:
            print(f"  上市 {roc_yr}年: 無資料")

    # TPEX 上櫃
    for roc_yr, dates in tpex_dates.items():
        found = False
        for dt in dates:
            url = (f"https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/"
                   f"pera_result.php?l=zh-tw&d={dt}&c=&o=json")
            data = fetch_json(url)
            if not data:
                continue
            tables = data.get('tables', [])
            if not tables or not tables[0].get('data'):
                continue
            cnt = 0
            for row in tables[0]['data']:
                code = str(row[0]).strip()
                pe = safe_float(row[2])
                div_val = safe_float(row[3])
                yld = safe_float(row[5])
                if pe and pe > 0:
                    if yld and yld > 0 and div_val:
                        price = div_val / yld * 100
                    else:
                        price = None
                    if price and price > 0:
                        result.setdefault(code, {})[roc_yr] = round(price / pe, 2)
                        cnt += 1
            print(f"  上櫃 {roc_yr}年: {cnt} 支")
            found = True
            break
        if not found:
            print(f"  上櫃 {roc_yr}年: 無資料")

    print(f"[年度EPS歷史] 共取得 {len(result)} 支")
    return result


# ── EPS 年度：政府 API t187ap14（批次，無限制）──────────────
def fetch_eps_annual_bulk():
    """從 TWSE/TPEX 批次取得最新一年累計 EPS"""
    print("[t187ap14] 抓取最新年度 EPS（批次）...")
    result = {}
    # 上市
    data = fetch_json("https://openapi.twse.com.tw/v1/opendata/t187ap14_L", backup_as='eps_annual_twse')
    if data:
        for d in data:
            code = str(d.get('公司代號', '')).strip()
            eps = safe_float(d.get('基本每股盈餘(元)'))
            year = d.get('年度', '')
            season = d.get('季別', '')
            if code and eps is not None and season == '4':
                result[code] = {'eps': eps, 'year': year}
        print(f"  上市：{len([c for c in result])} 筆")
    # 上櫃
    data2 = fetch_json("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O", backup_as='eps_annual_tpex')
    if data2:
        cnt = 0
        for d in data2:
            code = str(d.get('SecuritiesCompanyCode', '')).strip()
            eps = safe_float(d.get('基本每股盈餘'))
            year = d.get('Year', '')
            season = d.get('季別', '')
            if code and eps is not None and season == '4':
                result[code] = {'eps': eps, 'year': year}
                cnt += 1
        print(f"  上櫃：{cnt} 筆")
    print(f"[t187ap14] 共取得 {len(result)} 筆年度 EPS")
    return result


# ── 合約負債：從 DB 讀取（已由 MOPS 季報 + 群益 zcpa 維護）──────

def _expected_latest_quarter():
    """根據現在日期推算市場上應有的最新季度標籤"""
    today = date.today()
    roc_y = today.year - 1911
    m = today.month
    # Q4(年報)3月底前公布，Q1 5月中，Q2 8月中，Q3 11月中
    if m >= 11:     return f"{roc_y}Q3"
    elif m >= 8:    return f"{roc_y}Q2"
    elif m >= 5:    return f"{roc_y}Q1"
    elif m >= 4:    return f"{roc_y - 1}Q4"
    else:           return f"{roc_y - 1}Q3"

def fetch_contract_liabilities(codes, old_meta):
    """合約負債直接從 DB 取，不再逐支抓取。由 MOPS 季報 + 群益 zcpa 維護。"""
    cl_keys = ['contract_1', 'contract_1q', 'contract_2', 'contract_2q', 'contract_3', 'contract_3q']
    results = {}
    for c in codes:
        old = old_meta.get(c, {})
        if old.get('contract_1') is not None:
            results[c] = {k: old.get(k) for k in cl_keys}
    hit = len(results)
    print(f"[合約負債] DB 已有 {hit}/{len(codes)} 筆（由 MOPS 季報 + 群益維護）")
    return results


# ── 季度 EPS：從 DB 讀取（已由 MOPS t163sb04 + 群益 zce + 政府API t187ap14 維護）──
def fetch_eps(codes, old_meta):
    """季度EPS直接從 DB 取，不再逐支抓取。由 MOPS + 群益 + 政府API 維護。"""
    eps_keys = (
        ['eps_date']
        + [f'eps_{i}' for i in range(1,6)] + [f'eps_{i}q' for i in range(1,6)]
        + [f'eps_y{i}' for i in range(1,7)] + [f'eps_y{i}_label' for i in range(1,7)]
        + ['eps_ytd', 'eps_ytd_label']
    )
    results = {}
    for code in codes:
        old = old_meta.get(code, {})
        if old.get('eps_1') is not None:
            results[code] = {k: old.get(k) for k in eps_keys}
    hit = len(results)
    print(f"[EPS] DB 已有 {hit}/{len(codes)} 筆（由 MOPS + 群益 + 政府API 維護）")
    return results


# ── 寫入資料庫 ──────────────────────────────────────────────
def save_to_db(rows):
    """UPSERT + 驗證 + 熔斷 + 跳變校驗"""
    if not rows:
        return

    # 1. 資料驗證
    vr = validate_batch(rows, 'full_scraper')
    if vr['invalid'] > 0:
        print(f"[驗證] {vr['invalid']}/{vr['total']} 筆資料異常")
        for w in vr['warnings'][:5]:
            print(f"  {w}")

    # 2. 熔斷檢查
    breaker = get_breaker('full_scraper')
    if not breaker.check(vr):
        print(f"[熔斷] 異常率 {vr['invalid_rate']*100:.1f}% 超過閾值，停止寫入！")
        _log_api_health('full_scraper', '完整爬蟲寫入', False)
        return

    # 3. 讀取舊資料做跳變比對 + 異動日誌用
    old_data = {}
    try:
        with sqlite3.get_conn(row_factory=True) as conn_old:
            c_old = conn_old.cursor()
            c_old.execute("""SELECT code, close, eps_1, eps_1q, eps_y1, eps_ytd,
                                    revenue_yoy, revenue_cum_yoy, revenue_month,
                                    fin_grade_1, contract_1, div_c1
                             FROM stocks""")
            for r in c_old.fetchall():
                old_data[r['code']] = dict(r)
    except Exception as e:
        logger.warning(f"[跳變偵測] 讀取舊資料失敗: {e}")

    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 只更新爬蟲有抓到的欄位，不碰 industry/fin_grade 等其他欄位
        update_fields = [
            'name', 'market', 'close', 'change', 'open', 'high', 'low', 'volume',
            'change_240d',
            'revenue_date', 'revenue_year', 'revenue_month',
            'revenue_yoy', 'revenue_mom', 'revenue_cum_yoy',
            'eps_date', 'eps_1', 'eps_1q', 'eps_2', 'eps_2q',
            'eps_3', 'eps_3q', 'eps_4', 'eps_4q', 'eps_5', 'eps_5q',
            'eps_y1', 'eps_y1_label', 'eps_y2', 'eps_y2_label',
            'eps_y3', 'eps_y3_label', 'eps_y4', 'eps_y4_label',
            'eps_y5', 'eps_y5_label', 'eps_y6', 'eps_y6_label',
            'eps_ytd', 'eps_ytd_label',
            'div_c1', 'div_s1', 'div_1_label', 'div_c2', 'div_s2', 'div_2_label',
            'div_c3', 'div_s3', 'div_3_label', 'div_c4', 'div_s4', 'div_4_label',
            'div_c5', 'div_s5', 'div_5_label', 'div_c6', 'div_s6', 'div_6_label',
            'contract_1', 'contract_1q', 'contract_2', 'contract_2q',
            'contract_3', 'contract_3q',
            'updated_at',
        ]

        quarantined = 0
        for r in rows:
            r['updated_at'] = updated_at

            # 4. 跳變校驗（跟舊資料比對）
            old = old_data.get(r.get('code'))
            is_safe, blocked = sanity_check(r, old, 'full_scraper')
            if not is_safe:
                # 被攔截的欄位設為 None，讓 UPSERT 跳過（不覆蓋舊值）
                for b in blocked:
                    r[b['field']] = None
                quarantined += 1

            # 5. 異動日誌（記錄關鍵欄位變化）
            if old:
                audit_changes(r.get('code'), r, old)

            # 檢查是否已存在
            c.execute("SELECT code FROM stocks WHERE code = ?", (r['code'],))
            if c.fetchone():
                # UPDATE：只更新有值的欄位（None 表示該來源沒抓到，不覆蓋）
                sets = []
                vals = []
                for f in update_fields:
                    v = r.get(f)
                    if v is not None:
                        sets.append(f'{f} = ?')
                        vals.append(v)
                if sets:
                    vals.append(r['code'])
                    c.execute(f"UPDATE stocks SET {', '.join(sets)} WHERE code = ?", vals)
            else:
                # INSERT 新股票
                all_fields = ['code'] + update_fields
                placeholders = ', '.join(f':{f}' for f in all_fields)
                field_names = ', '.join(all_fields)
                c.execute(f"INSERT INTO stocks ({field_names}) VALUES ({placeholders})",
                          {f: r.get(f) for f in all_fields})

        conn.commit()
    msg = f"[DB] 已更新 {len(rows)} 筆（UPSERT，不刪除舊資料）"
    if quarantined:
        msg += f"，{quarantined} 筆跳變被攔截"
    print(msg)


# ── 主程式 ──────────────────────────────────────────────────
def run(scheduled=True):
    # 防呆：Lock file 防止與 quick_update() 同時執行 + 90 分鐘超時
    with ScraperLock('run', timeout_sec=5400) as lock:
        if lock is None:
            return
        _run_inner(scheduled)

def _run_inner(scheduled=True):
    # 排程抖動：僅排程觸發時延遲，手動觸發（網頁按鈕）不等
    if scheduled:
        jitter = random.randint(0, 300)
        print(f"[排程抖動] 延遲 {jitter} 秒後開始...")
        time.sleep(jitter)

    t0 = time.time()
    print(f"\n{'='*50}")
    print(f"開始更新  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")
    init_db()

    # 0. 讀取舊資料
    old_meta = read_old_meta()

    # 1. 平行抓取股價（僅 TWSE+TPEX 兩個來源，max_workers=2 剛好）
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_twse = pool.submit(fetch_twse)
        f_tpex = pool.submit(fetch_tpex)
        twse_rows = f_twse.result()
        tpex_rows = f_tpex.result()
    all_rows = twse_rows + tpex_rows
    all_codes = [r['code'] for r in all_rows]

    # 2. 平行抓取 240 日歷史（僅 TWSE+TPEX 兩個來源）
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_twse_h = pool.submit(fetch_twse_history_240d)
        f_tpex_h = pool.submit(fetch_tpex_history_240d)
        twse_hist = f_twse_h.result()
        tpex_hist = f_tpex_h.result()
    hist_map = {**twse_hist, **tpex_hist}

    # 3. 營收（有舊資料就跳過，節省 API 額度給 EPS）
    revenue_map = fetch_revenue(all_codes, old_meta)

    # 4. 股利（政府 API，批次無限制）
    div_map = fetch_dividends_bulk()

    # 5. 合約負債（從 DB，由 MOPS 季報 + 群益維護）
    contract_map = fetch_contract_liabilities(all_codes, old_meta)

    # 6. EPS 年度 — 群益優先，政府API+BWIBBU反推驗證
    from capital_fetcher import fetch_capital_annual_eps_batch
    eps_capital = fetch_capital_annual_eps_batch(all_codes)  # 群益年度EPS（最優先）

    # 6b. EPS 年度（政府 API，批次無限制）— 驗證用
    eps_annual = fetch_eps_annual_bulk()

    # 6c. EPS 年度歷史（TWSE/TPEX 本益比反推，批次無限制）— 驗證+補齊
    eps_annual_hist = fetch_eps_annual_history()

    # 7. EPS 季度（從 DB，由 MOPS + 群益 + 政府API 維護）
    eps_map = fetch_eps(all_codes, old_meta)

    # 6. 合併所有資料
    today_str = date.today().strftime('%Y-%m-%d')
    for r in all_rows:
        r['change_240d'] = calc_change_240d(r['close'], hist_map.get(r['code']))

        rev = revenue_map.get(r['code'], {})
        r['revenue_date']    = rev.get('revenue_date')
        r['revenue_year']    = rev.get('revenue_year')
        r['revenue_month']   = rev.get('revenue_month')
        r['revenue_yoy']     = rev.get('revenue_yoy')
        r['revenue_mom']     = rev.get('revenue_mom')
        r['revenue_cum_yoy'] = rev.get('revenue_cum_yoy')

        eps = eps_map.get(r['code'], {})
        r['eps_date'] = eps.get('eps_date')
        for i in range(1, 6):
            r[f'eps_{i}']  = eps.get(f'eps_{i}')
            r[f'eps_{i}q'] = eps.get(f'eps_{i}q')
        for i in range(1, 7):
            r[f'eps_y{i}']       = eps.get(f'eps_y{i}')
            r[f'eps_y{i}_label'] = eps.get(f'eps_y{i}_label')
        r['eps_ytd']       = eps.get('eps_ytd')
        r['eps_ytd_label'] = eps.get('eps_ytd_label')

        # 多源合併年度 EPS：群益優先 → 政府API+BWIBBU驗證 → DB既有值補齊
        cap = eps_capital.get(r['code'], {})   # 群益（最優先）
        annual = eps_annual.get(r['code'])      # 政府API t187ap14（驗證）
        hist = eps_annual_hist.get(r['code'], {})  # BWIBBU反推（驗證+補齊）

        merged = {}  # {year_label: eps_value}

        # 第一層：群益年度 EPS（直接覆蓋）
        for yr, eps_val in cap.items():
            merged[yr] = eps_val

        # 第二層：政府API 驗證（群益已有的年度做比對，沒有的補齊）
        if annual:
            yr = annual['year']
            if yr in merged:
                # 群益已有 → 比對差異，差異 > 5% 印警告
                diff = abs(merged[yr] - annual['eps'])
                if merged[yr] != 0 and diff / abs(merged[yr]) > 0.05:
                    print(f"[年度EPS警告] {r['code']} {yr}年: 群益={merged[yr]} vs 政府API={annual['eps']}，差異 {diff:.2f}")
            else:
                merged[yr] = annual['eps']

        # 第三層：BWIBBU 反推（補齊群益和政府API都沒有的年度）
        for yr, eps_val in hist.items():
            if yr not in merged:
                merged[yr] = eps_val

        # 第四層：DB 既有值（保留先前已存的資料）
        for i in range(1, 7):
            if r.get(f'eps_y{i}_label') and r.get(f'eps_y{i}') is not None:
                yr = r[f'eps_y{i}_label']
                if yr not in merged:
                    merged[yr] = r[f'eps_y{i}']

        # eps_date 只在季度 EPS (eps_1q) 真正變更時才更新，年度 EPS 合併不觸發

        # 寫回最近 6 年（年度上限：當年-1，如 2026 年最新年報是民國 114）
        max_roc = date.today().year - 1911 - 1
        sorted_yrs = sorted([y for y in merged.keys() if int(y) <= max_roc], key=int, reverse=True)[:6]
        for i, yr in enumerate(sorted_yrs, 1):
            r[f'eps_y{i}'] = merged[yr]
            r[f'eps_y{i}_label'] = yr
        for i in range(len(sorted_yrs) + 1, 7):
            r[f'eps_y{i}'] = None
            r[f'eps_y{i}_label'] = None

        # 股利
        div = div_map.get(r['code'], {})
        for i in range(1, 7):
            r[f'div_c{i}']      = div.get(f'div_c{i}')
            r[f'div_s{i}']      = div.get(f'div_s{i}')
            r[f'div_{i}_label'] = div.get(f'div_{i}_label')

        # 合約負債
        cl = contract_map.get(r['code'], {})
        for i in range(1, 4):
            r[f'contract_{i}']  = cl.get(f'contract_{i}')
            r[f'contract_{i}q'] = cl.get(f'contract_{i}q')

    # 6. 寫入資料庫
    save_to_db(all_rows)

    # 6b. 股價修正：批次 API 資料非今天 → 用即時 API 覆蓋正確股價
    if _twse_batch_date and _twse_batch_date != _today_roc() and datetime.now().weekday() < 5:
        print(f"[股價修正] 批次 API 日期 {_twse_batch_date} ≠ 今天 {_today_roc()}，用即時 API 覆蓋...")
        rt_count = _refresh_realtime()
        print(f"[股價修正] 即時 API 更新 {rt_count} 支")

    # MOPS 季報（第一優先）
    try:
        from mops_fetcher import fetch_latest_mops_quarterly
        fetch_latest_mops_quarterly()
    except Exception as e:
        print(f"[MOPS季報] 失敗: {e}")

    # 7. 補回 DELETE+INSERT 不包含的資料（產業別、年度EPS歷史、財務等級）
    print("[後處理] 補回輔助資料...")
    _post_process_after_save()

    elapsed = time.time() - t0
    rev_hit = sum(1 for r in all_rows if r.get('revenue_yoy') is not None)
    eps_hit = sum(1 for r in all_rows if r.get('eps_1') is not None)
    _flush_health_log()
    _save_daily_price()
    snapshot_stock_states()
    try: focus_signal_check()
    except Exception as e: print(f"[重點追蹤] 訊號檢查失敗: {e}")

    # 觀察清單個股資料預抓取（年度財報 + 月營收 + 季度財報 + 歷史PE）
    _prefetch_watchlist_details()

    # ETF 成分股更新（偵測異動）
    try:
        from etf_fetcher import run as etf_run
        etf_run()
    except Exception as e:
        print(f"[ETF] 更新失敗: {e}")

    # 三大法人買賣超（五點後才公佈，14:30 排程不跑，06:00 排程會跑前一天的）
    now_h = datetime.now().hour
    if now_h >= 17 or now_h < 9:
        try:
            fetch_institutional()
            # 自動 push 到 Render
            _push_institutional_to_render()
        except Exception as e:
            print(f"[法人] 更新失敗: {e}")

    # 交叉校驗（抽樣比對資料正確性）
    try:
        from guardian import cross_validate
        cv = cross_validate(sample_size=20)
        if cv['mismatches']:
            print(f"[交叉校驗] {cv['checked']} 支抽查，{len(cv['mismatches'])} 支有差異！")
        else:
            print(f"[交叉校驗] {cv['checked']} 支抽查，全部一致")
    except Exception as e:
        print(f"[交叉校驗] 失敗: {e}")

    # 計算檢核表（Render 由本機同步，不獨立計算）
    if not IS_CLOUD:
        try:
            from app import calc_all_checklists
            calc_all_checklists()
        except Exception as e:
            print(f"[Checklist] 計算失敗: {e}")

    # 重算衍生欄位（沈董/加權/綜合/近四季 PE/殖利率/等級等）
    try:
        from app import recalc_all_derived
        recalc_all_derived()
    except Exception as e:
        print(f"[Derived] 衍生欄位重算失敗: {e}")

    # 自動 push 所有資料到 Render（僅本機）
    # 注意：必須在 ETF、法人、交叉校驗、checklist 全部完成後才 push
    if not IS_CLOUD:
        _push_all_to_render()

    print(f"\n完成！共更新 {len(all_rows)} 筆")
    print(f"  營收年增率：{rev_hit} 筆")
    print(f"  EPS 資料：{eps_hit} 筆")
    print(f"  耗時：{elapsed:.1f} 秒")


def _post_process_after_save():
    """完整爬蟲 save_to_db 後，補回產業別、年度EPS歷史、營收官方值、財務等級"""
    with sqlite3.get_conn() as conn:
        _post_process_after_save_inner(conn)

    # ── 股利補充（BWIBBU 殖利率反推，不依賴 FinMind）──
    _fill_dividends_from_bwibbu()

    # ── 從 quarterly_financial 同步 EPS + 合約負債 到 stocks 表 ──
    _sync_eps_from_quarterly()
    _sync_contract_from_quarterly()

    # ── 重算衍生欄位（eps_core / eps_nonop）──
    _recalc_quarterly_derived()
    _recalc_annual_derived()

def _post_process_after_save_inner(conn):
    c = conn.cursor()
    today_str = date.today().strftime('%Y-%m-%d')

    # ── 產業別 ──
    for label, url in [
        ("上市", "https://openapi.twse.com.tw/v1/openData/t187ap05_L"),
        ("上櫃", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"),
    ]:
        data = fetch_json(url)
        if not data: continue
        cnt = 0
        for d in data:
            code = str(d.get('公司代號', '')).strip()
            ind = str(d.get('產業別', '')).strip()
            if code and ind:
                c.execute("UPDATE stocks SET industry=? WHERE code=?", (ind, code))
                cnt += c.rowcount
        if cnt: print(f"  產業別 {label}: {cnt} 支")

    # 已移除「營收用政府官方值覆蓋」— FinMind 已移除，不再需要。
    # 此段曾造成 bug：政府 API 回傳舊月份的 yoy/mom/cum_yoy，覆蓋 MOPS 已寫入的新月份正確值。

    # ── eps_ytd 補齊 ──
    c.execute("UPDATE stocks SET eps_ytd=eps_y1, eps_ytd_label=eps_y1_label WHERE eps_ytd IS NULL AND eps_y1 IS NOT NULL")

    conn.commit()

    # ── 稅務資料修正 ──
    _fix_tax_data()

    # 注意：以下步驟已移到 run_maintenance() 獨立執行，不在這裡重複：
    # - BWIBBU 年度EPS歷史（run_maintenance step 3）
    # - 財務等級重算（run_maintenance step 8）
    # - MOPS 季報EPS（quick_update 已處理）
    # - 交叉驗證（run_maintenance step 11）
    # - 年度EPS/股利完整性檢查（run_maintenance _fill_all_gaps）
    # - 系統EPS估算（run_maintenance step 6）
    # - 補缺資料（run_maintenance _fill_all_gaps）


def _fill_missing_financials():
    """
    批次補抓缺任何關鍵資料的股票。
    檢查年報（total_equity/operating_cf/capex/cash_dividend/net_income）、
    季報（inventory/contract_liability）、PE歷史、
    月營收（過去兩年各需12筆），
    缺任一項就跑群益 fetch_all_three 全套抓取。
    每次最多補 50 支，避免跑太久。多跑幾次排程就會全部補齊。
    """
    from capital_fetcher import fetch_all_three

    cur_year = date.today().year

    with sqlite3.get_conn() as conn:
        # 找最新完整季度
        latest_q = conn.execute("""
            SELECT quarter FROM quarterly_financial
            WHERE CAST(SUBSTR(quarter,1,INSTR(quarter,'Q')-1) AS INTEGER) >= ?
            GROUP BY quarter HAVING COUNT(*) > 1000
            ORDER BY CAST(SUBSTR(quarter,1,INSTR(quarter,'Q')-1) AS INTEGER) DESC,
                     CAST(SUBSTR(quarter,INSTR(quarter,'Q')+1) AS INTEGER) DESC
            LIMIT 1
        """, (cur_year - 1912,)).fetchone()

        if not latest_q:
            return

        q = latest_q[0]

        # 條件：缺年報關鍵欄位 OR 年報 net_income 為 NULL OR 缺季報存貨 OR 缺PE歷史 OR 月營收不足
        # 排除金融股的存貨檢查、排除 DR 股的月營收檢查
        # 月營收：過去兩年各需 12 筆
        today = date.today()

        codes = [r[0] for r in conn.execute("""
            SELECT DISTINCT s.code FROM stocks s
            WHERE s.close IS NOT NULL AND (
                -- 年報缺關鍵欄位（最近一年）
                s.code IN (
                    SELECT code FROM financial_annual
                    WHERE year = ? AND (
                        total_equity IS NULL OR operating_cf IS NULL OR
                        capex IS NULL OR cash_dividend IS NULL
                    )
                )
                -- 年報 net_income 為 NULL（影響聶夫比率計算）
                OR s.code IN (
                    SELECT code FROM financial_annual
                    WHERE year >= ? AND net_income IS NULL
                    GROUP BY code HAVING COUNT(*) >= 2
                )
                -- 季報缺存貨（非金融股）
                OR (s.code IN (
                    SELECT code FROM quarterly_financial
                    WHERE quarter = ? AND inventory IS NULL
                ) AND COALESCE(s.industry,'') NOT IN ('金融保險業','金融業','銀行業','保險業','證券業'))
                -- 缺PE歷史（EPS > 0 的才需要）
                OR (s.code NOT IN (SELECT DISTINCT code FROM pe_history)
                    AND s.eps_y1 IS NOT NULL AND s.eps_y1 > 0)
                -- 月營收：過去兩年不足 12 筆（排除 DR 股）
                OR (s.code NOT LIKE '91%' AND s.code IN (
                    SELECT code FROM (
                        SELECT s2.code,
                            (SELECT COUNT(*) FROM monthly_revenue m WHERE m.code=s2.code AND m.year=?) as y1_cnt,
                            (SELECT COUNT(*) FROM monthly_revenue m WHERE m.code=s2.code AND m.year=?) as y2_cnt
                        FROM stocks s2 WHERE s2.close IS NOT NULL AND s2.code NOT LIKE '91%'
                    ) WHERE y1_cnt < 12 OR y2_cnt < 12
                ))
            )
            ORDER BY s.code
            LIMIT 50
        """, (cur_year - 1, cur_year - 6, q, cur_year - 2, cur_year - 1)).fetchall()]

    if not codes:
        print(f"  [補資料] 無缺漏")
        return

    print(f"  [補資料] 發現 {len(codes)} 支缺關鍵資料，開始群益全套補抓...")
    done = 0
    for code in codes:
        try:
            fetch_all_three(code)
        except Exception as e:
            logger.warning(f"[補資料] {code} 群益全套失敗: {e}")
        done += 1
        if done % 10 == 0:
            print(f"    進度: {done}/{len(codes)}")

    print(f"  [補資料] 完成，已補 {done} 支")


def _check_annual_eps_completeness():
    """
    年報公告截止後半個月（每年 4/15 起），檢查所有股票是否都有最新年度 EPS。
    缺漏的從群益 zcqa 補抓，確保 eps_y1~eps_y6 維持最近 6 年完整資料。
    年報法定截止日：3/31（上市櫃公司須公告前一年度財報）
    """
    now = datetime.now()
    cur_roc = now.year - 1911  # 今年民國年（如 116）
    expected_year = str(cur_roc - 1)  # 預期最新年度（如 115）

    # 只在 4/15 ~ 6/30 期間執行（年報截止後半個月到年中）
    if not (now.month >= 4 and now.day >= 15 or now.month >= 5) or now.month > 6:
        return

    with sqlite3.get_conn() as conn:
        c = conn.cursor()

        # 找出 eps_y1_label 不是最新年度的股票（代表還沒更新）
        c.execute("""SELECT code FROM stocks
                     WHERE close IS NOT NULL
                     AND (eps_y1_label IS NULL OR eps_y1_label != ?)""",
                  (expected_year,))
        missing_codes = [r[0] for r in c.fetchall()]

    if not missing_codes:
        print(f"[年度EPS檢查] 所有股票 {expected_year} 年 EPS 已到齊")
        return

    print(f"[年度EPS檢查] {len(missing_codes)} 支缺少 {expected_year} 年 EPS，從群益補抓...")

    from capital_fetcher import fetch_capital_annual_eps_batch
    cap_data = fetch_capital_annual_eps_batch(missing_codes)

    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        updated = 0
        for code in missing_codes:
            cap = cap_data.get(code, {})
            if expected_year not in cap:
                continue

            # 從群益取最近 6 年，更新 eps_y1~eps_y6
            sorted_yrs = sorted(cap.keys(), reverse=True)[:6]
            vals = {}
            for i, yr in enumerate(sorted_yrs, 1):
                vals[f'eps_y{i}'] = cap[yr]
                vals[f'eps_y{i}_label'] = yr
            for i in range(len(sorted_yrs) + 1, 7):
                vals[f'eps_y{i}'] = None
                vals[f'eps_y{i}_label'] = None

            c.execute("""UPDATE stocks SET
                eps_y1=?, eps_y1_label=?, eps_y2=?, eps_y2_label=?,
                eps_y3=?, eps_y3_label=?, eps_y4=?, eps_y4_label=?,
                eps_y5=?, eps_y5_label=?, eps_y6=?, eps_y6_label=?
                WHERE code=?""",
                (vals['eps_y1'], vals['eps_y1_label'],
                 vals['eps_y2'], vals['eps_y2_label'],
                 vals['eps_y3'], vals['eps_y3_label'],
                 vals['eps_y4'], vals['eps_y4_label'],
                 vals['eps_y5'], vals['eps_y5_label'],
                 vals['eps_y6'], vals['eps_y6_label'],
                 code))
            if c.rowcount:
                updated += 1

        conn.commit()
    still_missing = len(missing_codes) - updated
    print(f"[年度EPS檢查] 補齊 {updated} 支" +
          (f"，仍有 {still_missing} 支缺漏（可能尚未公告）" if still_missing else ""))


def _check_annual_dividend_completeness():
    """
    股利公告截止後（每年 8/31 起），檢查所有股票是否都有最新年度股利。
    股利公告通常在年報之後，約 5~8 月陸續公布。
    缺漏的從群益 zcc 補抓，確保 div_c1~div_c6 維持最近 6 年完整資料。
    """
    now = datetime.now()
    cur_roc = now.year - 1911
    expected_year = str(cur_roc - 1)  # 預期最新股利年度（如 115）

    # 全年都可執行（股利公告不限 9~12 月）

    with sqlite3.get_conn() as conn:
        c = conn.cursor()

        # 找出 div_1_label 不是最新年度的股票
        c.execute("""SELECT code FROM stocks
                     WHERE close IS NOT NULL
                     AND (div_1_label IS NULL OR div_1_label != ?)""",
                  (expected_year,))
        missing_codes = [r[0] for r in c.fetchall()]

    if not missing_codes:
        print(f"[年度股利檢查] 所有股票 {expected_year} 年股利已到齊")
        return

    print(f"[年度股利檢查] {len(missing_codes)} 支缺少 {expected_year} 年股利，從群益補抓...")

    from capital_fetcher import fetch_capital_dividend
    updated = 0
    for i, code in enumerate(missing_codes):
        try:
            fetch_capital_dividend(code)
        except Exception as e:
            logger.warning(f"[股利補抓] {code} 失敗: {e}")
        if (i + 1) % 50 == 0:
            print(f"  股利補抓進度：{i+1}/{len(missing_codes)}")
            time.sleep(0.5)

    # 從 financial_annual 同步到 stocks 表（批次查詢）
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        if missing_codes:
            placeholders = ','.join('?' * len(missing_codes))
            all_divs = c.execute(f"""SELECT code, year, cash_dividend, stock_dividend FROM financial_annual
                                   WHERE code IN ({placeholders}) AND (cash_dividend IS NOT NULL OR stock_dividend IS NOT NULL)
                                   ORDER BY code, year DESC""", missing_codes).fetchall()
            # 分組：每支取最新 6 年
            from collections import defaultdict
            div_by_code = defaultdict(list)
            for r in all_divs:
                if len(div_by_code[r[0]]) < 6:
                    div_by_code[r[0]].append(r)
            for code in missing_codes:
                rows = div_by_code.get(code, [])
                if not rows:
                    continue
                for i, r in enumerate(rows, 1):
                    roc_yr = str(r[1] - 1911)
                    c.execute(f"UPDATE stocks SET div_c{i}=?, div_s{i}=?, div_{i}_label=? WHERE code=?",
                              (r[2], r[3], roc_yr, code))
                for i in range(len(rows) + 1, 7):
                    c.execute(f"UPDATE stocks SET div_c{i}=NULL, div_s{i}=NULL, div_{i}_label=NULL WHERE code=?",
                              (code,))
                updated += 1

        conn.commit()
    still_missing = len(missing_codes) - updated
    print(f"[年度股利檢查] 補齊 {updated} 支" +
          (f"，仍有 {still_missing} 支缺漏（可能尚未公告）" if still_missing else ""))


def _check_quarterly_completeness():
    """
    季報公告截止日後 ~ +15天，每天用群益 zce 逐支補齊 MOPS 缺漏的股票。

    截止日後補齊期間：
    - Q1：5/16 ~ 5/30
    - Q2：8/15 ~ 8/29
    - Q3：11/15 ~ 11/29
    - Q4：4/1 ~ 4/15

    每天只補仍缺 EPS 的股票，每次最多 100 支，多天排程會全部補齊。
    """
    now = datetime.now()
    cur_roc = now.year - 1911
    month, day = now.month, now.day

    # 判斷是否在截止日後的補齊期間
    check_quarter = None
    if month == 5 and 16 <= day <= 30:
        check_quarter = f"{cur_roc}Q1"
    elif month == 8 and day >= 15 and day <= 29:
        check_quarter = f"{cur_roc}Q2"
    elif month == 11 and day >= 15 and day <= 29:
        check_quarter = f"{cur_roc}Q3"
    elif month == 4 and day >= 1 and day <= 15:
        check_quarter = f"{cur_roc - 1}Q4"

    if not check_quarter:
        return

    with sqlite3.get_conn() as conn:
        c = conn.cursor()

        # 找出該季仍缺 EPS 的股票
        c.execute("""SELECT s.code FROM stocks s
                     WHERE s.close IS NOT NULL
                     AND s.code NOT IN (
                         SELECT code FROM quarterly_financial
                         WHERE quarter = ? AND eps IS NOT NULL
                     )
                     ORDER BY s.code LIMIT 100""", (check_quarter,))
        missing_codes = [r[0] for r in c.fetchall()]

    if not missing_codes:
        print(f"[季報補齊] {check_quarter} 已全部到齊")
        return

    print(f"[季報補齊] {check_quarter} 仍缺 {len(missing_codes)} 支，群益逐支補齊...")

    from capital_fetcher import fetch_capital_financials

    done = 0
    for i, code in enumerate(missing_codes):
        try:
            fetch_capital_financials(code)
            done += 1
        except Exception as e:
            logger.warning(f"[季報補齊] {code} 失敗: {e}")
        if (i + 1) % 50 == 0:
            print(f"  進度：{i+1}/{len(missing_codes)}")
        time.sleep(random.uniform(0.3, 0.5))

    # 統計補齊結果
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        c.execute("""SELECT COUNT(*) FROM quarterly_financial
                     WHERE quarter=? AND eps IS NOT NULL""", (check_quarter,))
        have_data = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM stocks WHERE close IS NOT NULL")
        total = c.fetchone()[0]

    print(f"[季報補齊] {check_quarter} 今日補 {done} 支，目前 {have_data}/{total} 支有資料")


def _sync_contract_from_quarterly():
    """從 quarterly_financial 的合約負債同步到 stocks 表的 contract_1~3"""
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        codes = [r[0] for r in c.execute('SELECT code FROM stocks WHERE close IS NOT NULL ORDER BY code').fetchall()]

        # 批次查詢所有合約負債資料
        all_cl = c.execute('''SELECT code, quarter, contract_liability FROM quarterly_financial
                             WHERE contract_liability IS NOT NULL
                             ORDER BY code,
                             CAST(SUBSTR(quarter,1,INSTR(quarter,"Q")-1) AS INTEGER) DESC,
                             CAST(SUBSTR(quarter,INSTR(quarter,"Q")+1) AS INTEGER) DESC''').fetchall()
        from collections import defaultdict
        cl_by_code = defaultdict(list)
        for r in all_cl:
            if len(cl_by_code[r[0]]) < 3:
                cl_by_code[r[0]].append((r[1], r[2]))

        updated = 0
        for code in codes:
            rows = cl_by_code.get(code, [])
            if not rows:
                c.execute('UPDATE stocks SET contract_1=NULL, contract_1q=NULL, contract_2=NULL, contract_2q=NULL, contract_3=NULL, contract_3q=NULL WHERE code=?', (code,))
                continue
            for i, (q, val) in enumerate(rows, 1):
                c.execute(f'UPDATE stocks SET contract_{i}=?, contract_{i}q=? WHERE code=?',
                          (val, q, code))
            for i in range(len(rows) + 1, 4):
                c.execute(f'UPDATE stocks SET contract_{i}=NULL, contract_{i}q=NULL WHERE code=?', (code,))
            updated += 1

        conn.commit()
    if updated:
        print(f"  [合約負債同步] 從 quarterly_financial 同步 {updated} 支到 stocks 表")


def _capital_quarterly_validation():
    """
    群益季報主動校驗：找出 MOPS 更新 7~14 天的季度，主動用群益比對。
    歷史季度（>14天）在 capital_fetcher 寫入時已直接覆蓋，這裡只處理「剛過校驗期」的。
    每次最多處理 30 支，避免過度請求群益。
    """
    from datetime import timedelta

    # 找出 7~14 天前由 MOPS 更新的季度（這些已過即時期，可以用群益校驗）
    now = datetime.now()
    date_7 = (now - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    date_14 = (now - timedelta(days=14)).strftime('%Y-%m-%d %H:%M:%S')

    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        rows = c.execute("""
            SELECT DISTINCT code FROM quarterly_financial
            WHERE updated_at BETWEEN ? AND ?
            ORDER BY code
            LIMIT 30
        """, (date_14, date_7)).fetchall()

    if not rows:
        return

    codes = [r[0] for r in rows]
    print(f"[群益校驗] 找到 {len(codes)} 支需校驗（MOPS 更新 7~14 天）")

    from capital_fetcher import fetch_capital_quarterly_full
    validated = 0
    for code in codes:
        try:
            fetch_capital_quarterly_full(code)
            validated += 1
        except Exception as e:
            logger.warning(f"[群益校驗] {code} 失敗: {e}")
        time.sleep(random.uniform(0.3, 0.5))

    if validated:
        print(f"[群益校驗] 完成 {validated}/{len(codes)} 支")
        # 校驗後重新同步 EPS 到 stocks 表
        _sync_eps_from_quarterly()


def _sync_eps_from_quarterly():
    """從 quarterly_financial 正確排序後回寫 stocks 表的 eps_1~eps_5 + eps_ytd"""
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        c.execute("""SELECT code, quarter, eps FROM quarterly_financial
                     WHERE eps IS NOT NULL
                     ORDER BY code,
                     CAST(SUBSTR(quarter,1,INSTR(quarter,'Q')-1) AS INTEGER) DESC,
                     CAST(SUBSTR(quarter,INSTR(quarter,'Q')+1) AS INTEGER) DESC""")
        from collections import defaultdict
        qf = defaultdict(list)
        for r in c.fetchall():
            if len(qf[r[0]]) < 5:
                qf[r[0]].append((r[1], r[2]))

        # 當前民國年
        cur_roc = datetime.now().year - 1911

        # 批次讀取現有 eps_1, eps_1q 用於判斷是否需更新 eps_date
        all_codes = list(qf.keys())
        old_eps = {}
        if all_codes:
            batch_size = 500
            for start in range(0, len(all_codes), batch_size):
                batch = all_codes[start:start+batch_size]
                ph = ','.join('?' * len(batch))
                for r in c.execute(f"SELECT code, eps_1, eps_1q FROM stocks WHERE code IN ({ph})", batch).fetchall():
                    old_eps[r[0]] = (r[1], r[2])

        updated = 0
        for code, quarters in qf.items():
            vals = {}
            for i, (q, eps) in enumerate(quarters, 1):
                vals[f'eps_{i}'] = eps
                vals[f'eps_{i}q'] = q
            for i in range(len(quarters) + 1, 6):
                vals[f'eps_{i}'] = None
                vals[f'eps_{i}q'] = None

            # 計算當年累計 eps_ytd：找最新年度的所有季度加總
            latest_q = quarters[0][0]  # e.g. '115Q1'
            latest_year = int(latest_q.split('Q')[0])
            # 用最新年度做累計（如果是 115Q1，ytd=Q1；如果是 114Q4，ytd=全年）
            ytd_year = latest_year
            ytd_sum = sum(eps for q, eps in quarters if q.startswith(f'{ytd_year}Q'))
            if ytd_sum != 0 or any(q.startswith(f'{ytd_year}Q') for q, _ in quarters):
                vals['eps_ytd'] = round(ytd_sum, 2)
                vals['eps_ytd_label'] = str(ytd_year)
            else:
                vals['eps_ytd'] = None
                vals['eps_ytd_label'] = None

            # EPS 跳變攔截：新舊 EPS 差異 > 10 倍且舊值有效 → 跳過（避免異常值汙染）
            old = old_eps.get(code)
            old_eps1 = old[0] if old else None
            old_eps1q = old[1] if old else None
            if old_eps1 is not None and vals['eps_1'] is not None and old_eps1 != 0:
                ratio = abs(vals['eps_1'] / old_eps1) if old_eps1 != 0 else 0
                if ratio > 10 and abs(vals['eps_1']) > 1:
                    print(f"  [EPS跳變攔截] {code}: {old_eps1} → {vals['eps_1']}（{ratio:.1f}倍），跳過")
                    continue
            new_date = datetime.now().strftime('%Y-%m-%d') if (vals['eps_1'] != old_eps1 or vals['eps_1q'] != old_eps1q) else None

            if new_date:
                c.execute("""UPDATE stocks SET
                    eps_1=?, eps_1q=?, eps_2=?, eps_2q=?, eps_3=?, eps_3q=?,
                    eps_4=?, eps_4q=?, eps_5=?, eps_5q=?,
                    eps_ytd=?, eps_ytd_label=?,
                    eps_date=? WHERE code=?""",
                    (vals['eps_1'], vals['eps_1q'], vals['eps_2'], vals['eps_2q'],
                     vals['eps_3'], vals['eps_3q'], vals['eps_4'], vals['eps_4q'],
                     vals['eps_5'], vals['eps_5q'],
                     vals.get('eps_ytd'), vals.get('eps_ytd_label'),
                     new_date, code))
            else:
                c.execute("""UPDATE stocks SET
                    eps_1=?, eps_1q=?, eps_2=?, eps_2q=?, eps_3=?, eps_3q=?,
                    eps_4=?, eps_4q=?, eps_5=?, eps_5q=?,
                    eps_ytd=?, eps_ytd_label=? WHERE code=?""",
                    (vals['eps_1'], vals['eps_1q'], vals['eps_2'], vals['eps_2q'],
                     vals['eps_3'], vals['eps_3q'], vals['eps_4'], vals['eps_4q'],
                     vals['eps_5'], vals['eps_5q'],
                     vals.get('eps_ytd'), vals.get('eps_ytd_label'), code))
            if c.rowcount:
                updated += 1

        conn.commit()
    if updated:
        print(f"  [EPS同步] 從 quarterly_financial 同步 {updated} 支到 stocks 表")


def _recalc_quarterly_derived():
    """重算 quarterly_financial 的衍生欄位（eps_core / eps_nonop）"""
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        # 確保欄位存在
        for col in ('eps_core', 'eps_nonop'):
            try:
                c.execute(f"ALTER TABLE quarterly_financial ADD COLUMN {col} REAL")
            except Exception:
                pass
        # 批次重算：營業利益/稅前淨利×EPS、業外/稅前淨利×EPS
        c.execute("""UPDATE quarterly_financial
            SET eps_core = ROUND(CAST(operating_income AS REAL) / pretax_income * eps, 2),
                eps_nonop = ROUND(CAST(non_operating AS REAL) / pretax_income * eps, 2)
            WHERE pretax_income IS NOT NULL AND pretax_income != 0
              AND eps IS NOT NULL AND operating_income IS NOT NULL AND non_operating IS NOT NULL""")
        updated = c.rowcount
        # 清除無法計算的
        c.execute("""UPDATE quarterly_financial
            SET eps_core = NULL, eps_nonop = NULL
            WHERE pretax_income IS NULL OR pretax_income = 0 OR eps IS NULL""")
        conn.commit()
        print(f"  [季報衍生] eps_core/eps_nonop 重算 {updated} 筆")


def _recalc_annual_derived():
    """重算 financial_annual 的衍生欄位（eps_core / eps_nonop）"""
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        for col in ('eps_core', 'eps_nonop'):
            try:
                c.execute(f"ALTER TABLE financial_annual ADD COLUMN {col} REAL")
            except Exception:
                pass
        c.execute("""UPDATE financial_annual
            SET eps_core = ROUND(CAST(operating_income AS REAL) / pretax_income * eps, 2),
                eps_nonop = ROUND(CAST(non_operating AS REAL) / pretax_income * eps, 2)
            WHERE pretax_income IS NOT NULL AND pretax_income != 0
              AND eps IS NOT NULL AND operating_income IS NOT NULL AND non_operating IS NOT NULL""")
        updated = c.rowcount
        c.execute("""UPDATE financial_annual
            SET eps_core = NULL, eps_nonop = NULL
            WHERE pretax_income IS NULL OR pretax_income = 0 OR eps IS NULL""")
        conn.commit()
        print(f"  [年報衍生] eps_core/eps_nonop 重算 {updated} 筆")


def _fill_dividends_from_bwibbu():
    """用 TWSE BWIBBU 殖利率反推 + TPEX 每股股利欄位，補齊 110-113 年股利"""
    import time as _time
    with sqlite3.get_conn() as conn:
        c = conn.cursor()

        # 收集現有股利年度
        c.execute("SELECT code, div_1_label, div_2_label, div_3_label, div_4_label, div_5_label, div_6_label FROM stocks")
        existing = {}
        for r in c.fetchall():
            existing[r[0]] = set(r[i] for i in range(1, 7) if r[i])

    from collections import defaultdict
    all_divs = defaultdict(dict)

    # TWSE BWIBBU: 110~113 年
    twse_div_dates = {
        '113': ['20250401'],
        '112': ['20240401'],
        '111': ['20230801', '20230601'],
        '110': ['20220701', '20221201'],
    }
    for div_year, dates in twse_div_dates.items():
        for dt in dates:
            url = (f"https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d"
                   f"?date={dt}&selectType=ALL&response=json")
            data = fetch_json(url)
            if not data or data.get('stat') != 'OK': continue
            cnt = 0
            for row in data.get('data', []):
                code = row[0].strip()
                price = safe_float(row[2])
                yld = safe_float(row[3])
                d_yr = str(row[4]).strip()
                if price and yld and yld > 0 and d_yr == div_year:
                    if div_year not in existing.get(code, set()):
                        all_divs[code][div_year] = round(price * yld / 100, 2)
                        cnt += 1
            if cnt > 0:
                print(f"  股利BWIBBU {div_year}年: {cnt} 支")
                break
            _time.sleep(0.3)

    # TPEX: 用 PE API 的每股股利欄位
    tpex_div_dates = {
        '113': ['114/07'],
        '112': ['113/07'],
        '111': ['112/08'],
        '110': ['111/07'],
    }
    for div_year, dates in tpex_div_dates.items():
        for dt in dates:
            url = (f"https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/"
                   f"pera_result.php?l=zh-tw&d={dt}&c=&o=json")
            data = fetch_json(url)
            if not data: continue
            tables = data.get('tables', [])
            if not tables or not tables[0].get('data'): continue
            cnt = 0
            for row in tables[0]['data']:
                code = str(row[0]).strip()
                div_val = safe_float(row[3])
                d_yr = str(row[4]).strip()
                if div_val and div_val > 0 and d_yr == div_year:
                    if div_year not in existing.get(code, set()):
                        all_divs[code][div_year] = div_val
                        cnt += 1
            if cnt > 0:
                print(f"  股利TPEX {div_year}年: {cnt} 支")
                break
            _time.sleep(0.3)

    # 寫入 DB
    updated = 0
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        for code, years_data in all_divs.items():
            c.execute('''SELECT div_c1, div_s1, div_1_label, div_c2, div_s2, div_2_label,
                                div_c3, div_s3, div_3_label, div_c4, div_s4, div_4_label,
                                div_c5, div_s5, div_5_label, div_c6, div_s6, div_6_label FROM stocks WHERE code=?''', (code,))
            r = c.fetchone()
            if not r: continue
            merged = {}
            for i in range(6):
                lbl = r[i * 3 + 2]
                if lbl: merged[lbl] = {'cash': r[i * 3] or 0, 'stock': r[i * 3 + 1] or 0}
            for yr, val in years_data.items():
                if yr not in merged:
                    merged[yr] = {'cash': val, 'stock': 0}
            sorted_years = sorted(merged.keys(), reverse=True)[:6]
            updates = {}
            for i, y in enumerate(sorted_years, 1):
                updates[f'div_c{i}'] = merged[y]['cash']
                updates[f'div_s{i}'] = merged[y]['stock']
                updates[f'div_{i}_label'] = y
            for i in range(len(sorted_years) + 1, 7):
                updates[f'div_c{i}'] = None
                updates[f'div_s{i}'] = None
                updates[f'div_{i}_label'] = None
            set_clause = ', '.join(f'{k}=?' for k in updates.keys())
            c.execute(f'UPDATE stocks SET {set_clause} WHERE code=?', list(updates.values()) + [code])
            updated += 1

        conn.commit()
    if updated: print(f"  股利BWIBBU補充: {updated} 支")


def _refresh_grades_from_pbr():
    """用 TWSE/TPEX 的 PBR/PE 反推 ROE，計算財務等級（不覆蓋精確值）"""
    import time as _time
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        _refresh_grades_from_pbr_inner(conn, c, _time)

def _refresh_grades_from_pbr_inner(conn, c, _time):
    year_dates = {'114':'20260401','113':'20250401','112':'20240401','111':'20230301','110':'20220401'}
    roe_data = {}

    for roc_yr, dt in year_dates.items():
        url = (f"https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d"
               f"?date={dt}&selectType=ALL&response=json")
        data = fetch_json(url)
        if not data or data.get('stat') != 'OK': continue
        for row in data.get('data', []):
            code = row[0].strip()
            pe = safe_float(row[5])
            pbr = safe_float(row[6])
            if pe and pe > 0 and pbr and pbr > 0:
                roe_data.setdefault(code, {})[roc_yr] = round(pbr / pe * 100, 2)
        _time.sleep(0.3)

    for roc_yr, dt in {'114':'115/04','113':'114/04','112':'113/04','111':'112/03','110':'111/04'}.items():
        url = (f"https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/"
               f"pera_result.php?l=zh-tw&d={dt}&c=&o=json")
        data = fetch_json(url)
        if not data: continue
        tables = data.get('tables', [])
        if not tables or not tables[0].get('data'): continue
        for row in tables[0]['data']:
            code = str(row[0]).strip()
            pe = safe_float(row[2])
            pbr = safe_float(row[6])
            if pe and pe > 0 and pbr and pbr > 0:
                roe_data.setdefault(code, {})[roc_yr] = round(pbr / pe * 100, 2)
        _time.sleep(0.3)

    # 營益率
    opm_map = {}
    data = fetch_json("https://openapi.twse.com.tw/v1/openData/t187ap17_L")
    if data:
        for d in data:
            code = d.get('公司代號', '').strip()
            yr = d.get('年度', '')
            opm = safe_float(d.get('營業利益率(%)(營業利益)/(營業收入)'))
            if code and opm is not None:
                opm_map.setdefault(code, {})[yr] = opm

    updated = 0
    for code, years_roe in roe_data.items():
        c.execute('SELECT fin_grade_1 FROM stocks WHERE code=?', (code,))
        r = c.fetchone()
        if r and r[0]: continue  # 已有精確等級

        sorted_years = sorted(years_roe.keys(), reverse=True)[:5]
        if len(sorted_years) < 3: continue

        updates = {}
        for i, yr in enumerate(sorted_years, 1):
            roe = years_roe[yr]
            opm = opm_map.get(code, {}).get(yr)
            grade = _calc_fin_grade(roe, opm, None, 1)  # fcf=None, revenue=1(避免除0)
            updates[f'fin_grade_{i}'] = grade
            updates[f'fin_grade_{i}y'] = yr
        for i in range(len(sorted_years) + 1, 6):
            updates[f'fin_grade_{i}'] = None
            updates[f'fin_grade_{i}y'] = None

        set_clause = ', '.join(f'{k}=?' for k in updates.keys())
        c.execute(f'UPDATE stocks SET {set_clause} WHERE code=?', list(updates.values()) + [code])
        updated += 1

    conn.commit()
    if updated: print(f"  PBR/PE 財務等級: {updated} 支")

# ── 個股年度財報（即時抓取 + 快取）──────────────────────────

def init_monthly_revenue_db():
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS monthly_revenue (
                code       TEXT NOT NULL,
                year       INTEGER NOT NULL,
                month      INTEGER NOT NULL,
                revenue    REAL,
                updated_at TEXT,
                PRIMARY KEY (code, year, month)
            )
        """)
        conn.commit()


def fetch_company_monthly_revenue(code):
    """從 FinMind 抓取個股近 4 年月營收，存入快取"""
    start_date = f"{date.today().year - 4}-01-01"
    url = (f"https://api.finmindtrade.com/api/v4/data"
           f"?dataset=TaiwanStockMonthRevenue"
           f"&data_id={code}&start_date={start_date}")
    try:
        r = _session.get(url, timeout=15)
        data = r.json()
        records = data.get('data', []) if data.get('status') == 200 else []
    except Exception as e:
        records = []

    if not records:
        return []

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    rows = []
    for rec in records:
        try:
            rows.append({
                'code': code,
                'year': int(rec['revenue_year']),
                'month': int(rec['revenue_month']),
                'revenue': float(rec['revenue']),
                'updated_at': now_str,
            })
        except Exception as e:
            logger.debug(f"[營收解析] {code} 單筆轉換失敗: {e}")

    if rows:
        with sqlite3.get_conn() as conn:
            c = conn.cursor()
            for row in rows:
                c.execute("""
                    INSERT OR REPLACE INTO monthly_revenue
                      (code, year, month, revenue, updated_at)
                    VALUES (:code, :year, :month, :revenue, :updated_at)
                """, row)
            # 更新 stocks 表的營收日期（取最新月份）
            latest = max(rows, key=lambda r: (r['year'], r['month']))
            old = c.execute("SELECT revenue_year, revenue_month FROM stocks WHERE code=?", (code,)).fetchone()
            if old and (latest['year'] > (old[0] or 0) or (latest['year'] == (old[0] or 0) and latest['month'] > (old[1] or 0))):
                c.execute("UPDATE stocks SET revenue_date=?, revenue_year=?, revenue_month=? WHERE code=?",
                          (now_str[:10], latest['year'], latest['month'], code))
            conn.commit()

    return rows


def init_financial_db():
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS financial_annual (
                code            TEXT NOT NULL,
                year            INTEGER NOT NULL,
                revenue         REAL,
                cost            REAL,
                gross_profit    REAL,
                operating_expense REAL,
                operating_income  REAL,
                non_operating   REAL,
                pretax_income   REAL,
                tax             REAL,
                net_income      REAL,
                net_income_parent REAL,
                total_assets    REAL,
                total_equity    REAL,
                common_stock    REAL,
                operating_cf    REAL,
                capex           REAL,
                eps             REAL,
                cash_dividend   REAL,
                stock_dividend  REAL,
                updated_at      TEXT,
                PRIMARY KEY (code, year)
            )
        """)
        conn.commit()


def fetch_company_financials(code):
    """
    個股年度財報更新：
    1. 群益證券全部資料（損益表+資產負債表+現金流量表+股利+月營收+合約負債）
    2. 群益資料不足時才用 Yahoo 補充（不用 FinMind，節省額度）
    雲端環境（Render）跳過群益爬蟲（海外IP可能被擋），靠排程更新。
    """
    is_cloud = IS_CLOUD

    # 來源 1：群益全部（僅本機，Render 跳過）
    capital_ok = False
    if not is_cloud:
        try:
            from capital_fetcher import fetch_all_three
            a1, q1, a2, a3, a4, a5 = fetch_all_three(code)
            capital_ok = (a1 > 0 or a2 > 0 or a3 > 0)
        except Exception as e:
            logger.warning(f"[個股財報] {code} 群益全套失敗: {e}")

    # 檢查群益是否已補齊關鍵欄位
    with sqlite3.get_conn(row_factory=True) as conn:
        c = conn.cursor()
        c.execute("SELECT total_equity, operating_cf FROM financial_annual WHERE code=? ORDER BY year DESC LIMIT 1", (code,))
        row = c.fetchone()
        needs_supplement = not row or row['total_equity'] is None or row['operating_cf'] is None

        # 只有群益資料不足時才用 Yahoo 補充
        if needs_supplement:
            try:
                c.execute("SELECT market FROM stocks WHERE code=?", (code,))
                r = c.fetchone()
                market = r['market'] if r else '上市'
                from yahoo_fetcher import _get_yahoo_session, fetch_yahoo_financials, save_yahoo_to_db
                session, crumb = _get_yahoo_session()
                data = fetch_yahoo_financials(session, crumb, code, market)
                if data:
                    save_yahoo_to_db(code, data)
            except Exception as e:
                logger.warning(f"[個股財報] {code} Yahoo補充失敗: {e}")

        c.execute("SELECT * FROM financial_annual WHERE code=? ORDER BY year DESC LIMIT 6", (code,))
        rows = [dict(r) for r in c.fetchall()]
    return rows if rows else None


def _fetch_financials_finmind(code):
    """從 FinMind 抓取個股近 6 年季度資料，彙整成年度財報後存入快取"""
    start_date = f"{date.today().year - 6}-01-01"

    # 平行抓取三張報表
    datasets = {
        'is': 'TaiwanStockFinancialStatements',
        'bs': 'TaiwanStockBalanceSheet',
        'cf': 'TaiwanStockCashFlowsStatement',
    }
    raw = {}
    with ThreadPoolExecutor(max_workers=3) as pool:  # FinMind 3 個 dataset 並行
        futures = {}
        for key, ds in datasets.items():
            url = (f"https://api.finmindtrade.com/api/v4/data"
                   f"?dataset={ds}&data_id={code}&start_date={start_date}")
            futures[pool.submit(_session.get, url, timeout=20)] = key
        for f in as_completed(futures):
            key = futures[f]
            try:
                resp = f.result()
                data = resp.json()
                raw[key] = data.get('data', []) if data.get('status') == 200 else []
            except Exception as e:
                raw[key] = []

    # ── 損益表：單季值，依年分組加總 ──
    IS_TYPES = {
        'Revenue': 'revenue',
        'CostOfGoodsSold': 'cost',
        'GrossProfit': 'gross_profit',
        'OperatingExpenses': 'operating_expense',
        'OperatingIncome': 'operating_income',
        'TotalNonoperatingIncomeAndExpense': 'non_operating',
        'PreTaxIncome': 'pretax_income',
        'TAX': 'tax',
        'IncomeAfterTaxes': 'net_income',
        'EquityAttributableToOwnersOfParent': 'net_income_parent',
        'EPS': 'eps',
    }
    # {year: {field: [q1,q2,q3,q4]}}
    is_yearly = {}
    for r in raw.get('is', []):
        t = r.get('type', '')
        if t not in IS_TYPES:
            continue
        yr = int(r['date'][:4])
        is_yearly.setdefault(yr, {}).setdefault(IS_TYPES[t], []).append(r['value'])

    # ── 資產負債表：取 Q4 (12-31) 時點值 ──
    BS_TYPES = {
        'TotalAssets': 'total_assets',
        'Equity': 'total_equity',
        'OrdinaryShare': 'common_stock',
    }
    bs_q4 = {}  # {year: {field: value}}
    for r in raw.get('bs', []):
        t = r.get('type', '')
        if t not in BS_TYPES:
            continue
        if not r['date'].endswith('-12-31'):
            continue
        yr = int(r['date'][:4])
        bs_q4.setdefault(yr, {})[BS_TYPES[t]] = r['value']

    # OrdinaryShare 有時不存在，用 CapitalStock 備用
    if not any(d.get('common_stock') for d in bs_q4.values()):
        for r in raw.get('bs', []):
            if r.get('type') == 'CapitalStock' and r['date'].endswith('-12-31'):
                yr = int(r['date'][:4])
                bs_q4.setdefault(yr, {})['common_stock'] = r['value']

    # ── 現金流量表：Q4 值即全年累計 ──
    CF_TYPES = {
        'CashFlowsFromOperatingActivities': 'operating_cf',
        'PropertyAndPlantAndEquipment': 'capex',
    }
    cf_q4 = {}
    for r in raw.get('cf', []):
        t = r.get('type', '')
        if t not in CF_TYPES:
            continue
        if not r['date'].endswith('-12-31'):
            continue
        yr = int(r['date'][:4])
        cf_q4.setdefault(yr, {})[CF_TYPES[t]] = r['value']

    # ── 股利：從 stocks 表讀取 ──
    div_map = {}
    try:
        with sqlite3.get_conn() as conn:
            c = conn.cursor()
            c.execute("""SELECT div_c1, div_s1, div_1_label, div_c2, div_s2, div_2_label,
                                div_c3, div_s3, div_3_label, div_c4, div_s4, div_4_label,
                                div_c5, div_s5, div_5_label, div_c6, div_s6, div_6_label
                         FROM stocks WHERE code = ?""", (code,))
            row = c.fetchone()
        if row:
            for i in range(6):
                lbl = row[i * 3 + 2]
                if lbl:
                    try:
                        roc_yr = int(lbl)
                        west_yr = roc_yr + 1911
                        div_map[west_yr] = {
                            'cash_dividend': row[i * 3] or 0,
                            'stock_dividend': row[i * 3 + 1] or 0,
                        }
                    except Exception as e:
                        logger.debug(f"[股利解析] {code} 年份轉換失敗: {e}")
    except Exception as e:
        logger.warning(f"[股利查詢] {code} 失敗: {e}")

    # ── 組合年度資料 ──
    all_years = sorted(set(
        list(is_yearly.keys()) + list(bs_q4.keys()) + list(cf_q4.keys())
    ), reverse=True)

    results = []
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for yr in all_years:
        is_data = is_yearly.get(yr, {})
        # 檢查損益表是否有 4 季（完整年度）
        has_full_year = any(len(v) == 4 for v in is_data.values())
        if not has_full_year:
            continue

        row = {'code': code, 'year': yr, 'updated_at': now_str}
        for field, vals in is_data.items():
            row[field] = round(sum(vals), 4)

        bs = bs_q4.get(yr, {})
        row['total_assets'] = bs.get('total_assets')
        row['total_equity'] = bs.get('total_equity')
        row['common_stock'] = bs.get('common_stock')

        cf = cf_q4.get(yr, {})
        row['operating_cf'] = cf.get('operating_cf')
        row['capex'] = cf.get('capex')

        div = div_map.get(yr, {})
        row['cash_dividend'] = div.get('cash_dividend')
        row['stock_dividend'] = div.get('stock_dividend')

        results.append(row)

    # ── 寫入快取 ──
    if results:
        with sqlite3.get_conn() as conn:
            c = conn.cursor()
            for row in results:
                c.execute("""
                    INSERT INTO financial_annual
                      (code, year, revenue, cost, gross_profit, operating_expense,
                       operating_income, non_operating, pretax_income, tax,
                       net_income, net_income_parent, total_assets, total_equity,
                       common_stock, operating_cf, capex, eps,
                       cash_dividend, stock_dividend, updated_at)
                    VALUES
                      (:code, :year, :revenue, :cost, :gross_profit, :operating_expense,
                       :operating_income, :non_operating, :pretax_income, :tax,
                       :net_income, :net_income_parent, :total_assets, :total_equity,
                       :common_stock, :operating_cf, :capex, :eps,
                       :cash_dividend, :stock_dividend, :updated_at)
                    ON CONFLICT(code, year) DO UPDATE SET
                      -- FinMind 僅作補充：所有欄位都不覆蓋已有值（群益/MOPS 優先）
                      revenue = COALESCE(revenue, excluded.revenue),
                      cost = COALESCE(cost, excluded.cost),
                      gross_profit = COALESCE(gross_profit, excluded.gross_profit),
                      operating_income = COALESCE(operating_income, excluded.operating_income),
                      pretax_income = COALESCE(pretax_income, excluded.pretax_income),
                      net_income = COALESCE(net_income, excluded.net_income),
                      eps = COALESCE(eps, excluded.eps),
                      operating_expense = COALESCE(operating_expense, excluded.operating_expense),
                      non_operating = COALESCE(non_operating, excluded.non_operating),
                      tax = COALESCE(tax, excluded.tax),
                      net_income_parent = COALESCE(net_income_parent, excluded.net_income_parent),
                      total_assets = COALESCE(total_assets, excluded.total_assets),
                      total_equity = COALESCE(total_equity, excluded.total_equity),
                      common_stock = COALESCE(common_stock, excluded.common_stock),
                      operating_cf = COALESCE(operating_cf, excluded.operating_cf),
                      capex = COALESCE(capex, excluded.capex),
                      cash_dividend = COALESCE(cash_dividend, excluded.cash_dividend),
                      stock_dividend = COALESCE(stock_dividend, excluded.stock_dividend),
                      updated_at = excluded.updated_at
                """, {
                    'code': row['code'], 'year': row['year'],
                    'revenue': row.get('revenue'), 'cost': row.get('cost'),
                    'gross_profit': row.get('gross_profit'),
                    'operating_expense': row.get('operating_expense'),
                    'operating_income': row.get('operating_income'),
                    'non_operating': row.get('non_operating'),
                    'pretax_income': row.get('pretax_income'),
                    'tax': row.get('tax'),
                    'net_income': row.get('net_income'),
                    'net_income_parent': row.get('net_income_parent'),
                    'total_assets': row.get('total_assets'),
                    'total_equity': row.get('total_equity'),
                    'common_stock': row.get('common_stock'),
                    'operating_cf': row.get('operating_cf'),
                    'capex': row.get('capex'),
                    'eps': row.get('eps'),
                    'cash_dividend': row.get('cash_dividend'),
                    'stock_dividend': row.get('stock_dividend'),
                    'updated_at': row['updated_at'],
                })
            conn.commit()

    return results[:5]  # 最多回傳 5 年


# ── 個股季度財務資料（即時抓取 + 快取）─────────────────────

def init_quarterly_db():
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS quarterly_financial (
                code              TEXT NOT NULL,
                quarter           TEXT NOT NULL,
                revenue           REAL,
                cost              REAL,
                gross_profit      REAL,
                operating_expense REAL,
                operating_income  REAL,
                non_operating     REAL,
                pretax_income     REAL,
                tax               REAL,
                continuing_income REAL,
                net_income_parent REAL,
                eps               REAL,
                contract_liability REAL,
                updated_at        TEXT,
                PRIMARY KEY (code, quarter)
            )
        """)
        # 新增欄位（既有 DB 可能缺 weighted_shares）
        try:
            c.execute("ALTER TABLE quarterly_financial ADD COLUMN weighted_shares REAL")
        except Exception:
            pass
        conn.commit()


def fetch_company_quarterly(code):
    """
    個股季度財報更新：
    群益季報（損益表+合約負債）已在 fetch_company_financials 的 fetch_all_three 裡抓過，
    這裡只需要直接讀 DB。如果 DB 沒資料才補抓。
    """
    # 先檢查 DB 是否有資料
    with sqlite3.get_conn(row_factory=True) as conn:
        c = conn.cursor()
        c.execute("""SELECT COUNT(*) as cnt FROM quarterly_financial
                     WHERE code=? AND updated_at > datetime('now', '-12 hours')""", (code,))
        has_recent = c.fetchone()['cnt'] > 0

        if not has_recent and not IS_CLOUD:
            # 本機才用群益補抓（Render 跳過）
            try:
                from capital_fetcher import fetch_capital_quarterly_full, fetch_capital_contract_liability
                fetch_capital_quarterly_full(code)
                fetch_capital_contract_liability(code)
            except Exception as e:
                logger.warning(f"[季報補抓] {code} 群益失敗: {e}")

        c.execute("""SELECT * FROM quarterly_financial WHERE code=?
                     ORDER BY CAST(SUBSTR(quarter, 1, INSTR(quarter, 'Q') - 1) AS INTEGER) DESC,
                              CAST(SUBSTR(quarter, INSTR(quarter, 'Q') + 1) AS INTEGER) DESC
                     LIMIT 8""", (code,))
        rows = [dict(r) for r in c.fetchall()]
    return rows if rows else None


def _fetch_quarterly_finmind(code):
    """從 FinMind 抓取個股近 2.5 年季度損益 + 合約負債，存入快取"""
    start_date = f"{date.today().year - 3}-01-01"

    # 平行抓取損益表 + 資產負債表
    datasets = {
        'is': 'TaiwanStockFinancialStatements',
        'bs': 'TaiwanStockBalanceSheet',
    }
    raw = {}
    with ThreadPoolExecutor(max_workers=2) as pool:  # FinMind 2 個 dataset 並行
        futures = {}
        for key, ds in datasets.items():
            url = (f"https://api.finmindtrade.com/api/v4/data"
                   f"?dataset={ds}&data_id={code}&start_date={start_date}")
            futures[pool.submit(_session.get, url, timeout=20)] = key
        for f in as_completed(futures):
            key = futures[f]
            try:
                resp = f.result()
                data = resp.json()
                raw[key] = data.get('data', []) if data.get('status') == 200 else []
            except Exception as e:
                raw[key] = []

    # ── 損益表：按季整理 ──
    IS_TYPES = {
        'Revenue': 'revenue',
        'CostOfGoodsSold': 'cost',
        'GrossProfit': 'gross_profit',
        'OperatingExpenses': 'operating_expense',
        'OperatingIncome': 'operating_income',
        'TotalNonoperatingIncomeAndExpense': 'non_operating',
        'PreTaxIncome': 'pretax_income',
        'TAX': 'tax',
        'IncomeFromContinuingOperations': 'continuing_income',
        'EquityAttributableToOwnersOfParent': 'net_income_parent',
        'EPS': 'eps',
    }
    # {quarter_label: {field: value}}
    quarters = {}
    for r in raw.get('is', []):
        t = r.get('type', '')
        if t not in IS_TYPES:
            continue
        label = date_to_quarter_label(r['date'])
        if not label:
            continue
        quarters.setdefault(label, {})[IS_TYPES[t]] = r['value']

    # ── 合約負債：從資產負債表 ──
    for r in raw.get('bs', []):
        t = r.get('type', '')
        if 'ContractLiabilit' in t or 'contract_liabilit' in t.lower():
            label = date_to_quarter_label(r['date'])
            if label:
                quarters.setdefault(label, {})['contract_liability'] = r['value']

    # ── 排序取最近 8 季 ──
    sorted_qs = sorted(quarters.keys(), reverse=True)[:8]

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    results = []
    if sorted_qs:
        with sqlite3.get_conn() as conn:
            c = conn.cursor()
            for q in sorted_qs:
                d = quarters[q]
                row = {'code': code, 'quarter': q, 'updated_at': now_str}
                for field in ['revenue', 'cost', 'gross_profit', 'operating_expense',
                              'operating_income', 'non_operating', 'pretax_income',
                              'tax', 'continuing_income', 'net_income_parent',
                              'eps', 'contract_liability']:
                    row[field] = d.get(field)
                results.append(row)

                # FinMind 不覆蓋群益已有值（群益優先）
                c.execute("""
                    INSERT INTO quarterly_financial
                      (code, quarter, revenue, cost, gross_profit, operating_expense,
                       operating_income, non_operating, pretax_income, tax,
                       continuing_income, net_income_parent, eps, contract_liability,
                       updated_at)
                    VALUES
                      (:code, :quarter, :revenue, :cost, :gross_profit, :operating_expense,
                       :operating_income, :non_operating, :pretax_income, :tax,
                       :continuing_income, :net_income_parent, :eps, :contract_liability,
                       :updated_at)
                    ON CONFLICT(code, quarter) DO UPDATE SET
                      revenue=COALESCE(revenue, excluded.revenue),
                      cost=COALESCE(cost, excluded.cost),
                      gross_profit=COALESCE(gross_profit, excluded.gross_profit),
                      operating_expense=COALESCE(operating_expense, excluded.operating_expense),
                      operating_income=COALESCE(operating_income, excluded.operating_income),
                      non_operating=COALESCE(non_operating, excluded.non_operating),
                      pretax_income=COALESCE(pretax_income, excluded.pretax_income),
                      tax=COALESCE(tax, excluded.tax),
                      continuing_income=COALESCE(continuing_income, excluded.continuing_income),
                      net_income_parent=COALESCE(net_income_parent, excluded.net_income_parent),
                      eps=COALESCE(eps, excluded.eps),
                      contract_liability=COALESCE(contract_liability, excluded.contract_liability),
                      updated_at=excluded.updated_at
                """, row)
            conn.commit()

    return results


# ── 歷史本益比（群益）──────────────────

def init_pe_history_db():
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS pe_history (
                code       TEXT NOT NULL,
                year       INTEGER NOT NULL,
                pe_high    REAL,
                pe_low     REAL,
                updated_at TEXT,
                PRIMARY KEY (code, year)
            )
        """)
        conn.commit()


def fetch_pe_history(code):
    """從群益抓歷史本益比"""
    try:
        from capital_fetcher import fetch_capital_pe_history
        n = fetch_capital_pe_history(code)
        return n
    except Exception as e:
        logger.warning(f"[PE歷史] {code} 群益失敗: {e}")
        return 0


# ── 快速更新：批次營收 + EPS（政府 API，無限制）─────────────

def quick_update():
    """
    輕量更新：MOPS 營收/季報 + 政府 API 營收 + EPS + 評價。
    必跑步驟（不受 lock 限制）：MOPS 營收 → MOPS 季報 → 政府 API 營收
    可跳過步驟（需 lock）：EPS → 群益校驗 → 評價/新聞/Render 同步
    """
    t0 = time.time()
    today_str = date.today().strftime('%Y-%m-%d')
    print(f"\n{'='*50}")
    print(f"快速更新  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")

    init_db()

    # ══ 必跑步驟（不受 lock 限制，即使 run() 在跑也執行）══

    # ── 1. MOPS 即時營收（最高優先，直接覆蓋）──
    try:
        from mops_fetcher import fetch_mops_monthly_revenue
        mops_count = fetch_mops_monthly_revenue()
        if mops_count:
            print(f"[MOPS營收] 已更新 {mops_count} 筆")
    except Exception as e:
        print(f"[MOPS營收] 失敗: {e}")

    # ── 2. MOPS 季報（最高優先，直接覆蓋）──
    try:
        from mops_fetcher import fetch_latest_mops_quarterly
        mops_q_count = fetch_latest_mops_quarterly()
        if mops_q_count and mops_q_count > 0:
            _sync_eps_from_quarterly()
    except Exception as e:
        print(f"[MOPS季報] 失敗: {e}")

    # ── 3. 政府 API 批次營收（補充 MOPS 缺的，COALESCE 不覆蓋已有值）──
    _quick_gov_revenue(today_str)

    # ── 必跑步驟的 Render 同步（營收+季報，增量）──
    if not IS_CLOUD:
        try:
            _today_start = date.today().strftime('%Y-%m-%d') + ' 00:00:00'
            _push_table_to_render(
                table='monthly_revenue',
                columns=['code','year','month','revenue','updated_at'],
                pk=['code','year','month'],
                since=_today_start,
            )
        except Exception as e:
            print(f"[營收同步Render] 失敗: {e}")

    # ══ 可跳過步驟（需 lock，被 run() 擋住就跳過）══
    with ScraperLock('quick_update', timeout_sec=900) as lock:
        if lock is None:
            elapsed = time.time() - t0
            print(f"\n快速更新（僅必跑步驟）完成，耗時 {elapsed:.1f} 秒")
            return
        _quick_update_inner(t0, today_str)

def _quick_gov_revenue(today_str):
    """政府 API 批次營收（補充 MOPS 缺的，COALESCE 不覆蓋已有值）
    在 lock 外執行，遇 DB locked 跳過不中斷"""
    try:
        _quick_gov_revenue_inner(today_str)
    except Exception as e:
        print(f"[營收] DB 連線失敗: {e}")

def _quick_gov_revenue_inner(today_str):
    with sqlite3.get_conn(timeout=60) as conn:
        c = conn.cursor()
        rev_updated = 0
        for label, url in [
            ("上市", "https://openapi.twse.com.tw/v1/openData/t187ap05_L"),
            ("上櫃", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"),
        ]:
            data = fetch_json(url, backup_as=f'quick_revenue_{label}')
            if not data:
                print(f"[營收-{label}] 抓取失敗")
                _log_api_health(f'revenue_{label}', f'營收({label}) t187ap05', False)
                continue
            print(f"[營收-{label}] 取得 {len(data)} 筆")
            _log_api_health(f'revenue_{label}', f'營收({label}) t187ap05', True, len(data))

            # 驗證 + 熔斷
            rev_rows = []
            for d in data:
                yoy = safe_float(d.get('營業收入-去年同月增減(%)'))
                cum_yoy = safe_float(d.get('累計營業收入-前期比較增減(%)'))
                rev_rows.append({'code': d.get('公司代號', ''), 'revenue_yoy': yoy, 'revenue_cum_yoy': cum_yoy})
            vr = validate_batch(rev_rows, f'quick_revenue_{label}')
            breaker = get_breaker(f'quick_revenue_{label}')
            if not breaker.check(vr):
                print(f"[熔斷] 營收-{label} 異常率 {vr['invalid_rate']*100:.1f}%，跳過寫入！")
                continue
            for d in data:
                code = str(d.get('公司代號', '')).strip()
                if not code:
                    continue
                ym_str = str(d.get('資料年月', '')).strip()
                if len(ym_str) < 4:
                    continue
                try:
                    roc_year = int(ym_str[:-2])
                    month = int(ym_str[-2:])
                except Exception: continue

                yoy = safe_float(d.get('營業收入-去年同月增減(%)'))
                mom = safe_float(d.get('營業收入-上月比較增減(%)'))
                cum_yoy = safe_float(d.get('累計營業收入-前期比較增減(%)'))
                note = str(d.get('備註', '')).strip()
                if note in ('-', '－', '無', ''): note = None

                west_year = roc_year + 1911

                c.execute("SELECT revenue_year, revenue_month FROM stocks WHERE code = ?", (code,))
                row = c.fetchone()
                if not row:
                    continue
                old_y, old_m = row[0], row[1]

                # 舊月份（MOPS 已更新到更新的月份）→ 跳過，不降級
                if old_y is not None and old_m is not None:
                    if west_year < old_y or (west_year == old_y and month < old_m):
                        continue
                # 同月份 → 補充 yoy/mom/cum_yoy
                try:
                    if old_y == west_year and old_m == month:
                        c.execute("""UPDATE stocks SET
                            revenue_yoy=COALESCE(revenue_yoy, ?),
                            revenue_mom=COALESCE(revenue_mom, ?),
                            revenue_cum_yoy=COALESCE(revenue_cum_yoy, ?),
                            revenue_note=COALESCE(revenue_note, ?)
                            WHERE code=?""", (yoy, mom, cum_yoy, note, code))
                    else:
                        c.execute("""UPDATE stocks SET
                            revenue_date=?, revenue_year=?, revenue_month=?,
                            revenue_yoy=?, revenue_mom=?, revenue_cum_yoy=?, revenue_note=?
                            WHERE code=?""",
                            (today_str, west_year, month, yoy, mom, cum_yoy, note, code))
                        rev_updated += 1
                except Exception as e:
                    logger.debug(f"[營收寫入] {code} 跳過（DB locked？）: {e}")

        try:
            conn.commit()
        except Exception as e:
            print(f"[營收] commit 失敗（DB locked？跳過）: {e}")
        print(f"[營收] 更新 {rev_updated} 支")


def _quick_update_inner(t0, today_str):
    """quick_update 的可跳過步驟（需 lock）"""
    _new_eps_codes = []

    # 清理舊備份
    try: cleanup_old_backups(30)
    except Exception as e: logger.warning(f"[備份清理] 失敗: {e}")

    # ── 4. 15號後：群益補齊當月 MOPS 缺漏 ──
    if date.today().day >= 15:
        try:
            from capital_fetcher import fetch_capital_monthly_revenue
            # 上個月是申報目標月（5月申報4月營收）
            target_year = date.today().year
            target_month = date.today().month - 1
            if target_month == 0:
                target_month = 12
                target_year -= 1

            with sqlite3.get_conn() as conn2:
                # 找出該月仍缺月營收的股票（排除 DR 股）
                missing_codes = [r[0] for r in conn2.execute("""
                    SELECT s.code FROM stocks s
                    WHERE s.close IS NOT NULL AND s.code NOT LIKE '91%'
                    AND s.code NOT IN (
                        SELECT code FROM monthly_revenue WHERE year=? AND month=?
                    )
                    ORDER BY s.code LIMIT 100
                """, (target_year, target_month)).fetchall()]

            if missing_codes:
                cap_filled = 0
                for code in missing_codes:
                    try:
                        n = fetch_capital_monthly_revenue(code)
                        if n > 0:
                            cap_filled += 1
                    except Exception as e:
                        logger.debug(f"[群益補營收] {code} 失敗: {e}")
                    time.sleep(random.uniform(0.3, 0.5))
                if cap_filled:
                    print(f"[群益補營收] {target_year}/{target_month}月 補齊 {cap_filled}/{len(missing_codes)} 支")
        except Exception as e:
            print(f"[群益補營收] 失敗: {e}")

    # MOPS 季報 → 已移到 lock 外層執行

    # ── 5. 群益季報主動校驗（MOPS 更新 7~14 天後，自動比對）──
    try:
        _capital_quarterly_validation()
    except Exception as e:
        print(f"[群益校驗] 失敗: {e}")

    # ── 2. 批次 EPS（TWSE + TPEX）──
    # t187ap14 的 EPS 是「累計」值：
    #   Q1 累計=單季, Q2 累計=Q1+Q2, Q3 累計=Q1+Q2+Q3, Q4 累計=全年
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        eps_updated = 0
        eps_y_updated = 0
        for label, url, code_key, eps_key, year_key, season_key, date_key in [
            ("上市",
             "https://openapi.twse.com.tw/v1/opendata/t187ap14_L",
             "公司代號", "基本每股盈餘(元)", "年度", "季別", "出表日期"),
            ("上櫃",
             "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O",
             "SecuritiesCompanyCode", "基本每股盈餘", "Year", "季別", "Date"),
        ]:
            data = fetch_json(url, backup_as=f'quick_eps_{label}')
            if not data:
                print(f"[EPS-{label}] 抓取失敗")
                _log_api_health(f'eps_{label}', f'EPS({label}) t187ap14', False)
                continue
            print(f"[EPS-{label}] 取得 {len(data)} 筆")
            _log_api_health(f'eps_{label}', f'EPS({label}) t187ap14', True, len(data))
    
            # 驗證 + 熔斷
            eps_rows = [{'code': d.get(code_key, ''), 'eps_1': safe_float(d.get(eps_key))} for d in data]
            vr = validate_batch(eps_rows, f'quick_eps_{label}')
            breaker = get_breaker(f'quick_eps_{label}')
            if not breaker.check(vr):
                print(f"[熔斷] EPS-{label} 異常率 {vr['invalid_rate']*100:.1f}%，跳過寫入！")
                continue
    
            for d in data:
                code = str(d.get(code_key, '')).strip()
                eps = safe_float(d.get(eps_key))
                year = str(d.get(year_key, '')).strip()
                season = str(d.get(season_key, '')).strip()
                if not code or eps is None or not year or not season:
                    continue
    
                # 年度防呆：民國年不能超過 cur_roc-1（如 2026 年最新年報是 114 年）
                try:
                    yr_int = int(year)
                    max_year = datetime.now().year - 1911 - 1
                    if yr_int > max_year:
                        continue
                except Exception:
                    continue
    
                # 出表日期（民國格式 1150429 → 2026-04-29）
                pub_date_str = str(d.get(date_key, '')).strip()
                pub_date = today_str  # fallback
                if pub_date_str and len(pub_date_str) >= 7:
                    try:
                        roc_y = int(pub_date_str[:-4])
                        mm = pub_date_str[-4:-2]
                        dd = pub_date_str[-2:]
                        pub_date = f"{roc_y + 1911}-{mm}-{dd}"
                    except Exception:
                        pass  # 日期解析失敗用 fallback (today_str)
    
                quarter_label = f"{year}Q{season}"
    
                # 檢查是否已有此季度
                c.execute("SELECT eps_1q, eps_y1_label FROM stocks WHERE code = ?", (code,))
                row = c.fetchone()
                if not row:
                    continue
                old_q1 = row[0]
                old_y1_label = row[1]
    
                # 防降級：如果 stocks 已有更新的季度（MOPS 先更新），t187ap05 不覆蓋
                if old_q1 and quarter_label:
                    try:
                        old_parts = old_q1.replace('Q', ' ').split()
                        new_parts = quarter_label.replace('Q', ' ').split()
                        old_num = int(old_parts[0]) * 10 + int(old_parts[1])
                        new_num = int(new_parts[0]) * 10 + int(new_parts[1])
                        if new_num < old_num:
                            continue  # t187ap05 的季度比已有的舊，跳過
                    except Exception:
                        pass
    
                # Q4 累計 = 全年 EPS → 只更新 eps_y1，不放入 eps_1（單季）
                if season == '4':
                    # 用 eps_y1_label 做去重（Q4 不寫 eps_1q）
                    if old_y1_label == year:
                        continue  # 已有此年度，跳過
                    c.execute("""
                        UPDATE stocks SET
                            eps_y6 = eps_y5, eps_y6_label = eps_y5_label,
                            eps_y5 = eps_y4, eps_y5_label = eps_y4_label,
                            eps_y4 = eps_y3, eps_y4_label = eps_y3_label,
                            eps_y3 = eps_y2, eps_y3_label = eps_y2_label,
                            eps_y2 = eps_y1, eps_y2_label = eps_y1_label,
                            eps_y1 = ?, eps_y1_label = ?,
                            eps_ytd = ?, eps_ytd_label = ?,
                            eps_date = ?
                        WHERE code = ?
                    """, (eps, year, eps, year, pub_date, code))
                    eps_y_updated += 1
                    _new_eps_codes.append(code)
                else:
                    # 用 eps_1q 做去重
                    if old_q1 == quarter_label:
                        continue  # 已有此季度，跳過
    
                    # Q1: 累計=單季，直接用
                    # Q2/Q3: 用「本季累計 - 前季累計」算出單季
                    #   需要從 DB 的單季 eps 反推前季累計
                    single_eps = eps  # Q1 直接用
                    if season != '1':
                        # 收集 DB 中同年度已有的單季 EPS，加總得到前季累計
                        c.execute("""SELECT eps_1, eps_1q, eps_2, eps_2q,
                                           eps_3, eps_3q, eps_4, eps_4q,
                                           eps_5, eps_5q
                                    FROM stocks WHERE code = ?""", (code,))
                        cur = c.fetchone()
                        if cur:
                            prev_cum = 0
                            found_all = True
                            for prev_q in range(1, int(season)):
                                prev_ql = f"{year}Q{prev_q}"
                                found = False
                                for j in range(0, 10, 2):
                                    if cur[j+1] == prev_ql and cur[j] is not None:
                                        prev_cum += cur[j]
                                        found = True
                                        break
                                if not found:
                                    found_all = False
                                    break
                            if found_all:
                                single_eps = round(eps - prev_cum, 4)
                            else:
                                # 找不到前季資料，無法反算單季 → 跳過，等 MOPS 或群益提供正確單季值
                                print(f"[EPS] {code} {quarter_label} 累積EPS={eps}，缺前季資料無法反算，跳過")
                                continue
    
                    # 推移 + 更新
                    c.execute("""
                        UPDATE stocks SET
                            eps_5 = eps_4, eps_5q = eps_4q,
                            eps_4 = eps_3, eps_4q = eps_3q,
                            eps_3 = eps_2, eps_3q = eps_2q,
                            eps_2 = eps_1, eps_2q = eps_1q,
                            eps_1 = ?, eps_1q = ?,
                            eps_date = ?,
                            eps_ytd = ?, eps_ytd_label = ?
                        WHERE code = ?
                    """, (single_eps, quarter_label, pub_date, eps, year, code))
                    eps_updated += 1
                    _new_eps_codes.append(code)
    
        print(f"[EPS] 更新季度 {eps_updated} 支 + 年度 {eps_y_updated} 支")

        conn.commit()

    # ── 3. 產業別（從營收 API 取得）──
    with sqlite3.get_conn() as conn3:
        c3 = conn3.cursor()
        c3.execute("SELECT COUNT(*) FROM stocks WHERE industry IS NULL")
        need_ind = c3.fetchone()[0]
        if need_ind > 0:
            for label, url in [
                ("上市", "https://openapi.twse.com.tw/v1/openData/t187ap05_L"),
                ("上櫃", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"),
            ]:
                data = fetch_json(url)
                if not data:
                    continue
                cnt = 0
                for d in data:
                    code = str(d.get('公司代號', '')).strip()
                    ind = str(d.get('產業別', '')).strip()
                    if code and ind:
                        c3.execute("UPDATE stocks SET industry=? WHERE code=? AND industry IS NULL", (ind, code))
                        cnt += c3.rowcount
                if cnt:
                    print(f"[產業別] {label}: {cnt} 支")
            conn3.commit()

    # ── 4. 補齊 eps_ytd（用 eps_y1 填入）──
    with sqlite3.get_conn() as conn2:
        c2 = conn2.cursor()
        c2.execute("UPDATE stocks SET eps_ytd = eps_y1, eps_ytd_label = eps_y1_label WHERE eps_ytd IS NULL AND eps_y1 IS NOT NULL")
        if c2.rowcount:
            print(f"[EPS] 補齊 {c2.rowcount} 支的當年累計")
        conn2.commit()

    # ── 4. 財務體質等級自動重算（從現有 financial_annual 快取）──
    _refresh_fin_grades()

    # ── 4. 偵測新年報 → 自動刷新 financial_annual ──
    if eps_updated > 0 or eps_y_updated > 0:
        _refresh_stale_financials()

    elapsed = time.time() - t0
    _flush_health_log()
    _save_daily_price()
    snapshot_stock_states()
    try: focus_signal_check()
    except Exception as e: print(f"[重點追蹤] 訊號檢查失敗: {e}")
    try: fetch_material_news()
    except Exception as e: print(f"[重大訊息] 失敗: {e}")
    try: fetch_moneydj_news()
    except Exception as e: print(f"[MoneyDJ新聞] 失敗: {e}")
    try: auto_archive_old_news()
    except Exception as e: logger.warning(f"[新聞歸檔] 失敗: {e}")
    # 產業新聞（經濟日報 + 工商時報）
    try:
        from app import fetch_industry_news, cleanup_old_industry_news
        fetch_industry_news()
        cleanup_old_industry_news()
    except Exception as e: print(f"[產業新聞] 失敗: {e}")
    # 本機自動 push 新聞到 Render
    if not IS_CLOUD:
        try: _push_news_to_render()
        except Exception as e: print(f"[新聞push] 失敗: {e}")
    # ── 5. MOPS 最新季 EPS（比政府 API 快）──
    try: fetch_mops_quarterly_eps()
    except Exception as e: print(f"[MOPS] 失敗: {e}")

    # ── 6. 稅務修正（每次都跑）──
    _fix_tax_data()

    # ── 7. 有新 EPS 的股票立即重算系統估算 ──
    if _new_eps_codes:
        unique_codes = list(set(_new_eps_codes))
        print(f"[即時重算] {len(unique_codes)} 支有新 EPS，重算系統估算...")
        for code in unique_codes:
            try:
                ar = estimate_annual_eps(code)
                if ar.get('est_eps') is not None and 'error' not in ar:
                    d = ar['details']
                    with sqlite3.get_conn() as conn_r:
                        conn_r.execute("""UPDATE stocks SET sys_ann_eps=?, sys_ann_div=?, sys_ann_pe=?,
                                         sys_ann_yld=?, sys_ann_confidence=? WHERE code=?""",
                                      (ar['est_eps'], d.get('est_div'), d.get('est_pe'),
                                       d.get('est_yld'), ar['confidence'], code))
                        conn_r.commit()
            except Exception as e:
                logger.warning(f"[即時重算] {code} 失敗: {e}")
        print(f"[即時重算] 完成")

    # ── 7b. 重算衍生欄位 ──
    try:
        from app import recalc_all_derived
        recalc_all_derived()
    except Exception as e:
        print(f"[Derived] 衍生欄位重算失敗: {e}")

    # ── 8. 自動 push 到 Render（僅本機，增量同步）──
    if not IS_CLOUD:
        try:
            _today_start = date.today().strftime('%Y-%m-%d') + ' 00:00:00'
            # push stocks 表（含營收/EPS/等級等核心欄位）
            _push_prices_to_render()
            _push_annual_to_render()
            _push_estimates_to_render()
            # push monthly_revenue（增量：只推今天更新的）
            _push_table_to_render(
                table='monthly_revenue',
                columns=['code','year','month','revenue','updated_at'],
                pk=['code','year','month'],
                since=_today_start,
            )
            # push stock_state（評價快照，只推今天）
            _push_table_to_render(
                table='stock_state',
                columns=['stock_id','date','price','price_pos','fair_low','fair_mid','fair_high',
                         'shen_eps','shen_pe','shen_yld','fin_grade','updated_at',
                         'val_level','val_aa','val_a1','val_a2','val_a','val_lt6','discount_pct'],
                pk=['stock_id','date'],
                since=_today_start,
            )
            # push stock_checklist（名稱制欄位，增量）
            _push_table_to_render(
                table='stock_checklist',
                columns=['code',
                         'chk_fin_grade','chk_cum_yoy','chk_gm_change',
                         'chk_best_grade_aa','chk_price_below_aa','chk_blend_yield','chk_ddm_return',
                         'chk_neff_growth','chk_neff_ratio','chk_lynch_peg','chk_lynch_consist',
                         'pass_count','total_count','base_count','bonus_count','detail',
                         'eps_setting','div_setting','yld_high','yld_max','pe_high','pe_low',
                         'lt_div','lt_yld','val_a','val_a1','val_a2','val_aa','lt5','lt6','lt7',
                         'gi_neff_a','gi_neff_b','gi_neff_3a','gi_neff_3b',
                         'gi_neff_c','gi_neff_d','gi_intrinsic_growth',
                         'gi_lynch_a','gi_lynch_b','gi_lynch_c','gi_lynch_d',
                         'gi_rev_cagr_5y','gi_shares_change','gi_yield','gi_pe',
                         'gi_gray','gi_neff_gray','gi_lynch_gray','gi_warnings',
                         'gi_shiller_avg_eps','gi_shiller_pe','gi_shiller_alert','gi_roic_avg',
                         'updated_at'],
                pk=['code'],
                since=_today_start,
            )
            # push quarterly_financial（增量：只推今天更新的）
            _push_table_to_render(
                table='quarterly_financial',
                columns=['code','quarter','revenue','cost','gross_profit','operating_expense',
                         'operating_income','non_operating','pretax_income','tax','continuing_income',
                         'net_income_parent','eps','contract_liability','inventory','updated_at'],
                pk=['code','quarter'],
                since=_today_start,
            )
            # push pe_history（增量）
            _push_table_to_render(
                table='pe_history',
                columns=['code','year','pe_high','pe_low','updated_at'],
                pk=['code','year'],
                since=_today_start,
            )
            print(f"[quick_update] 已 push 到 Render")
        except Exception as e:
            print(f"[quick_update] push Render 失敗: {e}")

    elapsed = time.time() - t0
    print(f"\n快速更新完成！季度EPS {eps_updated} + 年度EPS {eps_y_updated}，耗時 {elapsed:.1f} 秒")




def calc_dorsey_roic(row, prev_row=None):
    """Dorsey 法計算 ROIC。
    投入資本 = 總資產 - 不計息流動負債 - 超額現金
    超額現金 = 現金 - 營收×5%
    分母取兩年平均（有 prev_row 時）
    回傳 (roic, nopat, invested_capital) 或 (None, None, None)
    """
    oi = row.get('operating_income') if isinstance(row, dict) else row['operating_income']
    ta = row.get('total_assets') if isinstance(row, dict) else row['total_assets']
    pti = row.get('pretax_income') if isinstance(row, dict) else row['pretax_income']
    tx = row.get('tax') if isinstance(row, dict) else row['tax']
    rev = row.get('revenue') if isinstance(row, dict) else row['revenue']
    ni = row.get('net_income') if isinstance(row, dict) else row['net_income']
    te = row.get('total_equity') if isinstance(row, dict) else row['total_equity']

    if oi is None or ta is None or not ta:
        # fallback ROE
        if te and ni is not None and te > 0:
            return round(ni / te * 100, 2), None, None
        return None, None, None

    tax_rate = tx / pti if pti and pti > 0 and tx is not None else 0.2
    nopat = oi * (1 - tax_rate)

    def _calc_ic(r):
        _ta = r.get('total_assets') if isinstance(r, dict) else r['total_assets']
        _cl = r.get('current_liabilities') if isinstance(r, dict) else (r['current_liabilities'] if 'current_liabilities' in (r.keys() if isinstance(r, dict) else [c for c in r.keys()]) else None)
        _rev = r.get('revenue') if isinstance(r, dict) else r['revenue']
        _cash = (r.get('cash_and_equivalents') if isinstance(r, dict) else r['cash_and_equivalents']) or 0
        _sd = (r.get('short_term_debt') if isinstance(r, dict) else r['short_term_debt']) or 0
        _sn = (r.get('short_term_notes') if isinstance(r, dict) else r['short_term_notes']) or 0
        _cld = (r.get('current_long_term_debt') if isinstance(r, dict) else r['current_long_term_debt']) or 0
        if not _ta or _ta <= 0:
            return None
        # 不計息流動負債 = 流動負債 - 計息流動負債
        ibd_current = _sd + _sn + _cld
        if _cl is not None and _cl > 0:
            nibcl = _cl - ibd_current
        else:
            # 無流動負債資料時 fallback：用 權益+有息負債 法
            _te = (r.get('total_equity') if isinstance(r, dict) else r['total_equity']) or 0
            _ibd_all = ibd_current + sum((r.get(f) if isinstance(r, dict) else r[f]) or 0
                       for f in ['long_term_bank_debt', 'other_long_term_debt', 'bonds_payable'])
            op_need = _rev * 0.05 if _rev and _rev > 0 else 0
            excess = max(_cash - op_need, 0)
            return _te + _ibd_all - excess
        op_need = _rev * 0.05 if _rev and _rev > 0 else 0
        excess_cash = max(_cash - op_need, 0)
        return _ta - nibcl - excess_cash

    ic = _calc_ic(row)
    if ic is None or ic <= 0:
        if te and ni is not None and te > 0:
            return round(ni / te * 100, 2), None, None
        return None, None, None

    # 兩年平均
    if prev_row is not None:
        prev_ic = _calc_ic(prev_row)
        if prev_ic and prev_ic > 0:
            ic = (ic + prev_ic) / 2

    roic = round(nopat / ic * 100, 2)
    return roic, round(nopat, 2), round(ic, 2)


def _calc_fin_grade(roic, operating_margin, fcf, revenue):
    """計算財務體質等級（縱軸改用 ROIC，門檻 7%/10%/15% 不變）"""
    if roic is None:
        return None
    # FCF 無資料時預設中間值（0-5% 區間）
    if fcf is None or revenue is None or revenue == 0:
        fcf_r = 2.5
    else:
        fcf_r = fcf / revenue * 100
    if roic >= 15:
        base = 'B1A' if fcf_r < 0 else ('A1' if fcf_r < 5 else 'AA')
    elif roic >= 10:
        base = 'B1' if fcf_r < 0 else ('A' if fcf_r < 5 else 'A2')
    elif roic >= 7:
        base = 'C' if fcf_r < 0 else ('B2' if fcf_r < 5 else 'B2A')
    else:
        base = 'D' if fcf_r < 0 else 'C'
    suffix = ''
    if operating_margin is not None:
        if operating_margin >= 10: suffix = '+'
        elif operating_margin < 5: suffix = '-'
    return base + suffix


def _refresh_fin_grades():
    """從 financial_annual 快取重算所有公司的財務等級（純 DB 運算）

    年報公告規則：
    - 每年度年報在隔年 3/31 前公告完畢
    - 等級建置：隔年 4/15 後才納入（確保大部分公司已公告）
    - 只保留最近 5 年

    例：115年(2026)年報 → 2027/4/15 後才建置 115 年等級
    """
    with sqlite3.get_conn(row_factory=True) as conn:
        c = conn.cursor()

        # 計算可納入等級的最新年度（西元）
        # 規則：隔年 4/15 後才納入 → 今年 4/15 後可納入去年的年報
        today = date.today()
        if today.month > 4 or (today.month == 4 and today.day >= 15):
            max_year = today.year - 1  # 4/15 後：可納入去年
        else:
            max_year = today.year - 2  # 4/15 前：只能納入前年

        # 找所有有 financial_annual 資料的公司
        c.execute("SELECT DISTINCT code FROM financial_annual")
        codes = [r[0] for r in c.fetchall()]
        if not codes:
            return

        # 確保 financial_annual 有 ROIC 相關欄位 + 週轉天數
        for col in ('roic', 'nopat', 'invested_capital', 'fin_grade', 'inventory_days', 'ar_days',
                    'accounts_receivable', 'interest_expense',
                    'debt_ratio', 'fin_debt_ratio', 'interest_coverage', 'earnings_quality', 'fcf'):
            try: c.execute(f"ALTER TABLE financial_annual ADD COLUMN {col} {'REAL' if col != 'fin_grade' else 'TEXT'}")
            except Exception: pass

        updated = 0
        for code in codes:
            c.execute("""SELECT year, revenue, cost, operating_income, pretax_income, tax, net_income,
                                total_equity, total_assets, operating_cf, capex,
                                cash_and_equivalents, short_term_debt, short_term_notes,
                                current_long_term_debt, long_term_bank_debt,
                                other_long_term_debt, bonds_payable, current_liabilities,
                                inventory, accounts_receivable, interest_expense
                         FROM financial_annual WHERE code = ?
                         ORDER BY year DESC""", (code,))
            rows = c.fetchall()
            if not rows:
                continue

            # 所有年度都算 ROIC/等級存入 financial_annual
            grade_idx = 0  # 用於 stocks 表 fin_grade_1~5（只存 max_year 以內的前 5 年）
            updates = {}
            for i, row in enumerate(rows):
                rev = row['revenue']
                oi = row['operating_income']
                pti = row['pretax_income']
                tx = row['tax']
                ni = row['net_income']
                te = row['total_equity']
                ta = row['total_assets']
                ocf = row['operating_cf']
                capex_val = row['capex']

                opm = round(oi / rev * 100, 2) if rev and oi is not None else None
                fcf = round(ocf + capex_val, 2) if ocf is not None and capex_val is not None else None

                # ROIC（Dorsey 法）
                prev_row = rows[i + 1] if i + 1 < len(rows) else None
                roic, nopat, ic = calc_dorsey_roic(row, prev_row)

                grade = _calc_fin_grade(roic, opm, fcf, rev)

                # 存貨週轉天數 = 平均存貨 / 成本 × 365
                inv = row['inventory']
                cost_val = row['cost']
                prev_inv = prev_row['inventory'] if prev_row else None
                if inv is not None and cost_val and cost_val > 0:
                    avg_inv = (inv + prev_inv) / 2 if prev_inv is not None else inv
                    inv_days = round(avg_inv / cost_val * 365, 1)
                else:
                    inv_days = None

                # 應收帳款週轉天數 = 平均應收帳款 / 營收 × 365
                ar = row['accounts_receivable']
                prev_ar = prev_row['accounts_receivable'] if prev_row else None
                if ar is not None and rev and rev > 0:
                    avg_ar = (ar + prev_ar) / 2 if prev_ar is not None else ar
                    ar_days_val = round(avg_ar / rev * 365, 1)
                else:
                    ar_days_val = None

                # 負債比 = (總資產 - 股東權益) / 總資產 × 100
                _debt_ratio = round((ta - te) / ta * 100, 2) if ta and ta > 0 and te is not None else None

                # 金融負債比 = 有息負債 / 總資產 × 100
                _fin_debt = (row['short_term_debt'] or 0) + (row['short_term_notes'] or 0) + \
                            (row['current_long_term_debt'] or 0) + (row['long_term_bank_debt'] or 0) + \
                            (row['other_long_term_debt'] or 0) + (row['bonds_payable'] or 0)
                _fin_debt_ratio = round(_fin_debt / ta * 100, 2) if ta and ta > 0 else None

                # 利息保障倍數 = 營業利益 / 財務成本
                _int_exp = row['interest_expense']
                _interest_cov = round(oi / _int_exp, 2) if oi is not None and _int_exp and _int_exp > 0 else None

                # 盈餘品質率 = 營業現金流 / 稅後淨利 × 100
                _eq = round(ocf / ni * 100, 2) if ocf is not None and ni and ni > 0 else None

                # 自由現金流
                _fcf = round(ocf + capex_val, 2) if ocf is not None and capex_val is not None else None

                # 寫入 financial_annual（所有年度）
                c.execute("""UPDATE financial_annual SET roic=?, nopat=?, invested_capital=?, fin_grade=?,
                             inventory_days=?, ar_days=?,
                             debt_ratio=?, fin_debt_ratio=?, interest_coverage=?, earnings_quality=?, fcf=?
                             WHERE code=? AND year=?""",
                          (roic, nopat, ic, grade, inv_days, ar_days_val,
                           _debt_ratio, _fin_debt_ratio, _interest_cov, _eq, _fcf,
                           code, row['year']))

                # stocks 表固定存 6 年，前端控制顯示幾年
                if row['year'] <= max_year and grade_idx < 6:
                    grade_idx += 1
                    updates[f'fin_grade_{grade_idx}'] = grade
                    updates[f'fin_grade_{grade_idx}y'] = str(row['year'] - 1911)

            # 清空多餘的
            for j in range(grade_idx + 1, 7):
                updates[f'fin_grade_{j}'] = None
                updates[f'fin_grade_{j}y'] = None

            set_clause = ', '.join(f'{k}=?' for k in updates.keys())
            c.execute(f'UPDATE stocks SET {set_clause} WHERE code=?',
                      list(updates.values()) + [code])
            updated += 1

        conn.commit()
    if updated:
        print(f"[等級] 重算 {updated} 支公司的財務體質等級")


def _refresh_stale_financials():
    """偵測哪些公司有新的年度 EPS 但 financial_annual 資料過時，自動刷新"""
    max_roc = date.today().year - 1911 - 1  # 年度上限：如 2026 年最新年報是民國 114
    with sqlite3.get_conn() as conn:
        c = conn.cursor()

        # 找 stocks 表中有 eps_y1_label 但 financial_annual 沒有對應年度的公司
        # 排除超過上限的年度（不可能有該年度年報）
        c.execute("""SELECT s.code, s.eps_y1_label
                     FROM stocks s
                     WHERE s.eps_y1_label IS NOT NULL
                     AND CAST(s.eps_y1_label AS INTEGER) <= ?
                     AND s.code NOT IN (
                         SELECT fa.code FROM financial_annual fa
                         WHERE fa.year = (CAST(s.eps_y1_label AS INTEGER) + 1911)
                     )""", (max_roc,))
        stale = c.fetchall()

    if not stale:
        return

    # 限制每次最多刷新 20 支
    stale = stale[:20]
    print(f"[財報] 偵測到 {len(stale)} 支有新年報待刷新")

    for code, year_label in stale:
        try:
            result = fetch_company_financials(code)
            if result:
                print(f"  {code} 財報已更新（{year_label}年）")
        except Exception as e:
            logger.warning(f"[財報刷新] {code} 失敗: {e}")


def _prefetch_watchlist_details():
    """
    觀察清單個股資料預抓取。
    來源：Yahoo Finance → 政府 API。
    """
    # ── 0. Yahoo Finance 補年度/季度財報（免費無限制）──
    print("[預抓取] Yahoo Finance 補齊財報...")
    try:
        from yahoo_fetcher import _get_yahoo_session, fetch_yahoo_financials, save_yahoo_to_db
        with sqlite3.get_conn() as conn:
            c = conn.cursor()
            yahoo_need = []
            c.execute("SELECT code, name, market FROM stocks WHERE close IS NOT NULL ORDER BY code")
            for r in c.fetchall():
                c.execute("SELECT COUNT(*) FROM financial_annual WHERE code=? AND net_income IS NOT NULL", (r[0],))
                if c.fetchone()[0] < 3:
                    yahoo_need.append((r[0], r[1], r[2]))

        if yahoo_need:
            session, crumb = _get_yahoo_session()
            y_done = 0
            y_fail = 0
            for code, name, market in yahoo_need[:200]:  # 每次最多 200 支
                data = fetch_yahoo_financials(session, crumb, code, market)
                if data:
                    a, q = save_yahoo_to_db(code, data)
                    if a > 0 or q > 0:
                        y_done += 1
                        y_fail = 0
                else:
                    y_fail += 1
                if y_fail >= 30:
                    try: session, crumb = _get_yahoo_session()
                    except Exception: break
                    y_fail = 0
                time.sleep(random.uniform(0.1, 0.3))
            print(f"  Yahoo 補齊 {y_done} 支")
        else:
            print("  Yahoo：全部已有 3 年以上財報")
    except Exception as e:
        print(f"  Yahoo 失敗：{e}")

    # ── 1. 月營收歷史：從政府 t187ap05 存原始金額到 monthly_revenue ──
    print("[預抓取] 儲存月營收歷史（政府API，無限制）...")
    with sqlite3.get_conn() as conn:
        _prefetch_revenue_and_financials(conn)

def _prefetch_revenue_and_financials(conn):
    c = conn.cursor()
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    rev_saved = 0

    for label, url in [
        ("上市", "https://openapi.twse.com.tw/v1/openData/t187ap05_L"),
        ("上櫃", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"),
    ]:
        data = fetch_json(url)
        if not data:
            continue
        for d in data:
            code = str(d.get('公司代號', '')).strip()
            ym_str = str(d.get('資料年月', '')).strip()
            revenue = safe_float(d.get('營業收入-當月營收'))
            if not code or not ym_str or revenue is None:
                continue
            try:
                roc_year = int(ym_str[:-2])
                month = int(ym_str[-2:])
                west_year = roc_year + 1911
            except Exception: continue
            try:
                c.execute("""INSERT OR IGNORE INTO monthly_revenue
                    (code, year, month, revenue, updated_at)
                    VALUES (?,?,?,?,?)""",
                    (code, west_year, month, revenue, now_str))
                if c.rowcount:
                    rev_saved += 1
            except Exception as e:
                logger.debug(f"[群益月營收] {code} DB寫入失敗: {e}")

    conn.commit()
    if rev_saved:
        print(f"  月營收新增 {rev_saved} 筆")

    # ── 2. 年度財務比率：從 t187ap17 存到 financial_annual ──
    print("[預抓取] 儲存年度財務比率（政府API，無限制）...")
    fin_saved = 0
    data = fetch_json("https://openapi.twse.com.tw/v1/openData/t187ap17_L")
    if data:
        for d in data:
            code = str(d.get('公司代號', '')).strip()
            year_str = str(d.get('年度', '')).strip()
            season = str(d.get('季別', '')).strip()
            if not code or season != '4':
                continue  # 只取 Q4 = 年度
            try:
                west_year = int(year_str) + 1911
            except Exception: continue
            revenue = safe_float(d.get('營業收入(百萬元)'))
            gross_margin = safe_float(d.get('毛利率(%)(營業毛利)/(營業收入)'))
            opm = safe_float(d.get('營業利益率(%)(營業利益)/(營業收入)'))
            pretax_margin = safe_float(d.get('稅前純益率(%)(稅前純益)/(營業收入)'))
            net_margin = safe_float(d.get('稅後純益率(%)(稅後純益)/(營業收入)'))

            if revenue is None:
                continue

            rev_full = revenue * 1000000  # 百萬轉元
            gross_profit = rev_full * gross_margin / 100 if gross_margin else None
            operating_income = rev_full * opm / 100 if opm else None
            net_income = rev_full * net_margin / 100 if net_margin else None

            # 先檢查有沒有（FinMind 的更完整，有就不覆蓋）
            c.execute("SELECT code FROM financial_annual WHERE code=? AND year=?", (code, west_year))
            if c.fetchone():
                continue

            try:
                c.execute("""INSERT INTO financial_annual
                    (code, year, revenue, gross_profit, operating_income, net_income, updated_at)
                    VALUES (?,?,?,?,?,?,?)""",
                    (code, west_year, rev_full, gross_profit, operating_income, net_income, now_str))
                fin_saved += 1
            except Exception as e:
                logger.debug(f"[年報寫入] {code}/{west_year} 失敗: {e}")

    # 上櫃也從 t187ap14 補充
    for label, url, code_key, eps_key, year_key, season_key, rev_key, oi_key, ni_key in [
        ("上市", "https://openapi.twse.com.tw/v1/opendata/t187ap14_L",
         "公司代號", "基本每股盈餘(元)", "年度", "季別", "營業收入", "營業利益", "稅後淨利"),
        ("上櫃", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O",
         "SecuritiesCompanyCode", "基本每股盈餘", "Year", "季別", "營業收入", "營業利益", "稅後淨利"),
    ]:
        data = fetch_json(url)
        if not data:
            continue
        for d in data:
            code = str(d.get(code_key, '')).strip()
            season = str(d.get(season_key, '')).strip()
            if not code or season != '4':
                continue
            try:
                year_str = str(d.get(year_key, '')).strip()
                west_year = int(year_str) + 1911
            except Exception: continue

            eps = safe_float(d.get(eps_key))
            revenue = safe_float(d.get(rev_key))
            oi = safe_float(d.get(oi_key))
            ni = safe_float(d.get(ni_key))

            c.execute("SELECT code FROM financial_annual WHERE code=? AND year=?", (code, west_year))
            if c.fetchone():
                # 已有就只補 EPS
                if eps is not None:
                    c.execute("UPDATE financial_annual SET eps=? WHERE code=? AND year=? AND eps IS NULL",
                              (eps, code, west_year))
                continue

            if revenue is None and ni is None:
                continue

            try:
                c.execute("""INSERT INTO financial_annual
                    (code, year, revenue, operating_income, net_income, eps, updated_at)
                    VALUES (?,?,?,?,?,?,?)""",
                    (code, west_year, revenue, oi, ni, eps, now_str))
                fin_saved += 1
            except Exception as e:
                logger.debug(f"[年報寫入] {code}/{west_year} 失敗: {e}")

    conn.commit()
    if fin_saved:
        print(f"  年度財報新增 {fin_saved} 筆")

    # ── 3. PE 歷史：從 BWIBBU 已有資料寫入 pe_history ──
    print("[預抓取] 儲存PE歷史（BWIBBU/TPEX，無限制）...")
    pe_saved = 0
    # BWIBBU 的 PE 資料已經在 fetch_eps_annual_history 中抓過
    # 這裡從 stocks 表的歷史 PE 數據補充
    c.execute("SELECT DISTINCT stock_id FROM stock_state")
    tracked = [r[0] for r in c.fetchall()]

    for code in tracked:
        c.execute("SELECT COUNT(*) FROM pe_history WHERE code=?", (code,))
        if c.fetchone()[0] > 0:
            continue  # 已有就跳過
        # PE 歷史由群益在 _fill_missing_financials 中補齊

    conn.commit()


def _parse_inst_val(v):
    v = v.replace(',', '').replace('--', '').strip()
    if not v:
        return None
    try:
        return int(v)
    except Exception as e:
        logger.debug(f"return int(v): {e}")
        return None


def _fetch_inst_one(code):
    try:
        url = f"https://stock.capital.com.tw/z/zc/zcl/zcl_{code}.djhtm"
        s = create_session()
        r = s.get(url, timeout=15)
        r.encoding = 'big5'
        soup = BeautifulSoup(r.text, 'html.parser')
        for t in soup.find_all('table'):
            rows = t.find_all('tr')
            found_header = False
            for row in rows:
                cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
                if '外資' in cells and '投信' in cells and '自營商' in cells:
                    found_header = True
                    continue
                if found_header and len(cells) >= 5:
                    date_str = cells[0]
                    if not re.match(r'\d+/\d+/\d+', date_str):
                        continue
                    foreign = _parse_inst_val(cells[1])
                    trust   = _parse_inst_val(cells[2])
                    dealer  = _parse_inst_val(cells[3])
                    return code, foreign, trust, dealer, date_str
        return code, None, None, None, None
    except Exception as e:
        logger.debug(f"[法人] {code} 群益抓取失敗: {e}")
        return code, None, None, None, None


def _today_roc_date():
    """取得今天的民國日期字串，格式: 115/04/25"""
    now = date.today()
    roc_year = now.year - 1911
    return f"{roc_year}/{now.month:02d}/{now.day:02d}"


def fetch_institutional():
    """從群益證券抓取全部個股的三大法人當日買賣超，批次寫入 DB
    含日期驗證：群益回傳的日期必須是今天，否則視為尚未更新，不寫入"""
    t0 = time.time()
    init_db()
    with sqlite3.get_conn() as conn:
        codes = [r[0] for r in conn.execute("SELECT code FROM stocks ORDER BY code").fetchall()]
    print(f"[法人] 開始抓取 {len(codes)} 支股票的三大法人買賣超...")

    results = []
    with ThreadPoolExecutor(max_workers=8) as pool:  # 群益逐支抓取，8 並發平衡速度與頻率限制
        futures = []
        for i, code in enumerate(codes):
            futures.append(pool.submit(_fetch_inst_one, code))
            if (i + 1) % 8 == 0:
                time.sleep(0.5)
        for f in as_completed(futures):
            results.append(f.result())

    # 日期驗證：檢查群益回傳的資料日期是否為今天
    today_roc = _today_roc_date()
    date_counter = {}
    for code, foreign, trust, dealer, inst_date in results:
        if inst_date:
            date_counter[inst_date] = date_counter.get(inst_date, 0) + 1

    if date_counter:
        most_common_date = max(date_counter, key=date_counter.get)
        print(f"[法人] 資料日期分布: {date_counter}（今天: {today_roc}）")
        if most_common_date != today_roc:
            print(f"[法人] 警告：多數資料日期為 {most_common_date}，非今天 {today_roc}，跳過寫入！")
            return 0

    with sqlite3.get_conn() as conn:
        updated = 0
        skipped_date = 0
        for code, foreign, trust, dealer, inst_date in results:
            if foreign is not None or trust is not None or dealer is not None:
                # 個別筆也要是今天的才寫入
                if inst_date and inst_date != today_roc:
                    skipped_date += 1
                    continue
                conn.execute(
                    "UPDATE stocks SET inst_foreign=?, inst_trust=?, inst_dealer=? WHERE code=?",
                    (foreign, trust, dealer, code)
                )
                updated += 1
        conn.commit()
    msg = f"[法人] 完成：更新 {updated}/{len(codes)} 支，耗時 {time.time()-t0:.1f}s"
    if skipped_date:
        msg += f"，{skipped_date} 支日期不符跳過"
    print(msg)
    return updated




















def _save_daily_price():
    """將 stocks 表的當天收盤價/成交量存入 daily_price（每天一筆）"""
    try:
        today_str = date.today().strftime('%Y-%m-%d')
        with sqlite3.get_conn() as conn:
            c = conn.cursor()
            c.execute("""CREATE TABLE IF NOT EXISTS daily_price (
                code TEXT NOT NULL, date TEXT NOT NULL,
                close_price REAL, volume INTEGER,
                PRIMARY KEY (code, date))""")
            rows = conn.execute(
                "SELECT code, close, volume FROM stocks WHERE close IS NOT NULL"
            ).fetchall()
            for code, close, volume in rows:
                c.execute("""INSERT OR REPLACE INTO daily_price
                             (code, date, close_price, volume) VALUES (?,?,?,?)""",
                          (code, today_str, close, volume))
            conn.commit()
        print(f"[每日價量] 已存入 {len(rows)} 筆 ({today_str})")
    except Exception as e:
        print(f"[每日價量] 存入失敗: {e}")


def fetch_historical_daily_prices(start_date=None, end_date=None):  # noqa - 已停用
    """(已停用) 抓取歷史逐日收盤價。"""
    return
    """抓取歷史逐日收盤價（所有上市+上櫃），存入 daily_price 表。
    TWSE: MI_INDEX API（一天一次取所有上市）
    TPEX: stk_wn1430（一天一次取所有上櫃）
    每天抓完存入 DB，已有的日期會跳過。
    """
    import time as _time
    if end_date is None:
        end_date = date.today() - timedelta(days=1)
    if start_date is None:
        start_date = end_date - timedelta(days=365 * 3)

    # 先看 DB 已有哪些日期
    existing_dates = set()
    try:
        with sqlite3.get_conn() as conn:
            c = conn.cursor()
            c.execute("CREATE TABLE IF NOT EXISTS daily_price (code TEXT NOT NULL, date TEXT NOT NULL, close_price REAL, volume INTEGER, PRIMARY KEY (code, date))")
            c.execute("SELECT DISTINCT date FROM daily_price")
            existing_dates = {r[0] for r in c.fetchall()}
    except Exception:
        pass

    # 產生需要抓的交易日清單（跳過週末和已有日期）
    all_dates = []
    d = start_date
    while d <= end_date:
        if d.weekday() < 5:  # 只要平日
            ds = d.strftime('%Y-%m-%d')
            if ds not in existing_dates:
                all_dates.append(d)
        d += timedelta(days=1)

    print(f"[歷史股價] 需抓取 {len(all_dates)} 個交易日（{start_date} ~ {end_date}，已排除 {len(existing_dates)} 個已有日期）")
    if not all_dates:
        print("[歷史股價] 全部已抓取完畢")
        return

    saved_count = 0
    failed_dates = []

    for i, d in enumerate(all_dates):
        ds = d.strftime('%Y-%m-%d')
        ds_fmt = d.strftime('%Y%m%d')
        roc_y = d.year - 1911
        roc_date = f'{roc_y}/{d.month:02d}/{d.day:02d}'

        prices = {}

        # TWSE（上市）
        try:
            url_twse = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date={ds_fmt}&type=ALL"
            data = fetch_json(url_twse)
            if data:
                tables = data.get('tables', [])
                if len(tables) >= 9 and tables[8].get('data'):
                    for row in tables[8]['data']:
                        code = str(row[0]).strip()
                        close_str = str(row[8]).replace(',', '').strip()
                        vol_str = str(row[2]).replace(',', '').strip()
                        try:
                            prices[code] = (float(close_str), int(vol_str) if vol_str.isdigit() else None)
                        except Exception:
                            pass
        except Exception as e:
            print(f"[歷史股價] TWSE {ds} 失敗: {e}")

        _time.sleep(1)

        # TPEX（上櫃）
        try:
            url_tpex = (f"https://www.tpex.org.tw/web/stock/aftertrading/"
                        f"otc_quotes_no1430/stk_wn1430_result.php"
                        f"?l=zh-tw&d={roc_date}&se=EW")
            data = fetch_json(url_tpex)
            if data:
                tables = data.get('tables', [])
                if tables and tables[0].get('data'):
                    for row in tables[0]['data']:
                        code = str(row[0]).strip()
                        close_str = str(row[2]).replace(',', '').strip()
                        vol_str = str(row[8]).replace(',', '').strip() if len(row) > 8 else ''
                        try:
                            prices[code] = (float(close_str), int(vol_str) if vol_str.isdigit() else None)
                        except Exception:
                            pass
        except Exception as e:
            print(f"[歷史股價] TPEX {ds} 失敗: {e}")

        if not prices:
            # 可能是國定假日
            failed_dates.append(ds)
            if len(failed_dates) <= 5:
                print(f"[歷史股價] {ds} 無資料（可能為假日）")
        else:
            # 存入 DB
            try:
                with sqlite3.get_conn() as conn:
                    c = conn.cursor()
                    for code, (close, vol) in prices.items():
                        c.execute("INSERT OR IGNORE INTO daily_price (code, date, close_price, volume) VALUES (?,?,?,?)",
                                  (code, ds, close, vol))
                    conn.commit()
                saved_count += len(prices)
            except Exception as e:
                print(f"[歷史股價] {ds} 存入失敗: {e}")

        if (i + 1) % 10 == 0 or i == len(all_dates) - 1:
            print(f"[歷史股價] 進度 {i+1}/{len(all_dates)}，累計存入 {saved_count} 筆")

        _time.sleep(2)  # 避免太頻繁

    print(f"[歷史股價] 完成：{saved_count} 筆，失敗/假日 {len(failed_dates)} 天")


def refresh_prices():
    """
    只更新股價。
    盤中（週一~五 09:00~13:35）：用 TWSE/TPEX 即時 API
    盤後同日（13:36~16:00）：優先用即時 API（已有當日收盤價），失敗才 fallback 批次 API
    其餘時段（隔日/假日）：用批次收盤 API
    """
    t0 = time.time()
    init_db()

    now = datetime.now(_TW)
    h, m, wd = now.hour, now.minute, now.weekday()
    in_market = wd < 5 and ((h > 9 or (h == 9 and m >= 0)) and (h < 13 or (h == 13 and m <= 35)))
    # 盤後同日：收盤後到 16:00，即時 API 仍有當天收盤價
    post_market = wd < 5 and ((h == 13 and m > 35) or (h >= 14 and h < 16))

    if in_market or post_market:
        # 盤中 & 盤後同日：用即時 API（收盤後 z 值 = 當日收盤價）
        count = _refresh_realtime()
        if count > 0:
            elapsed = time.time() - t0
            label = "盤中即時" if in_market else "盤後即時"
            print(f"[股價更新-{label}] {count} 支，耗時 {elapsed:.1f} 秒")
            return count
        # 即時 API 全部失敗，fallback 到批次 API
        print("[股價更新] 即時 API 無回傳，改用批次收盤 API...")

    # 批次收盤 API（先檢查資料日期是否為今天）
    today_roc = _today_roc()

    # 先用 TWSE 批次 API 嘗試，順便取得資料日期（僅 TWSE+TPEX 兩來源）
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_twse = pool.submit(fetch_twse)
        f_tpex = pool.submit(fetch_tpex)
        twse_rows = f_twse.result()
        tpex_rows = f_tpex.result()

    # 如果批次 API 資料不是今天的（平日），改用即時 API
    if _twse_batch_date and _twse_batch_date != today_roc and now.weekday() < 5:
        print(f"[股價更新] 批次 API 日期 {_twse_batch_date} ≠ 今天 {today_roc}，改用即時 API...")
        rt_count = _refresh_realtime()
        if rt_count > 0:
            elapsed = time.time() - t0
            print(f"[股價更新-即時修正] {rt_count} 支，耗時 {elapsed:.1f} 秒")
            return rt_count
        print("[股價更新] 即時 API 也無回傳，使用批次 API 資料（可能非當日）")

    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        updated_at = now.strftime("%Y-%m-%d %H:%M:%S")
        count = 0
        for r in twse_rows + tpex_rows:
            c.execute("""UPDATE stocks SET close=?, change=?, open=?, high=?, low=?,
                         volume=?, updated_at=? WHERE code=?""",
                      (r['close'], r['change'], r['open'], r['high'], r['low'],
                       r['volume'], updated_at, r['code']))
            if c.rowcount:
                count += 1
        conn.commit()

    elapsed = time.time() - t0
    print(f"[股價更新-批次] {count} 支，耗時 {elapsed:.1f} 秒")
    return count


def _refresh_realtime():
    """盤中即時報價更新（證交所 MIS API：mis.twse.com.tw）
    上市用 tse_{code}.tw、上櫃用 otc_{code}.tw，同一個 API 統一處理
    取價優先：z（成交價）> b（買價第一檔）> 跳過"""
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT code, market FROM stocks WHERE close IS NOT NULL")
        all_stocks = [(r[0], r[1]) for r in c.fetchall()]

        updated_at = datetime.now(_TW).strftime("%Y-%m-%d %H:%M:%S")
        count = 0

        # 每批 50 檔，5 並發
        batches = []
        for i in range(0, len(all_stocks), 50):
            batch = all_stocks[i:i+50]
            ex_codes = []
            for code, market in batch:
                prefix = 'tse' if market == '上市' else 'otc'
                ex_codes.append(f"{prefix}_{code}.tw")
            batches.append(ex_codes)

        def _fetch_batch(ex_codes):
            try:
                url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={'|'.join(ex_codes)}"
                r = _session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
                return r.json().get("msgArray", [])
            except Exception as e:
                logger.warning(f"[即時股價] 批次請求失敗: {e}")
                return []

        from concurrent.futures import ThreadPoolExecutor
        results = []
        with ThreadPoolExecutor(max_workers=5) as pool:
            results = list(pool.map(_fetch_batch, batches))

        for msg_array in results:
            for s in msg_array:
                code = s.get("c")
                price = s.get("z")
                if price == "-" or not price:
                    bid = s.get("b", "")
                    if bid and "_" in bid:
                        price = bid.split("_")[0]
                if price == "-" or not price:
                    continue
                try:
                    close = float(price)
                    if close <= 0:
                        continue
                    yesterday = float(s.get("y", 0))
                    change = round(close - yesterday, 2) if yesterday else None
                    op = float(s["o"]) if s.get("o") else None
                    hi = float(s["h"]) if s.get("h") else None
                    lo = float(s["l"]) if s.get("l") else None
                    vol = int(s["v"]) if s.get("v") else None
                    c.execute("""UPDATE stocks SET close=?, change=?, open=?, high=?, low=?,
                                 volume=?, updated_at=? WHERE code=?""",
                              (close, change, op, hi, lo, vol, updated_at, code))
                    if c.rowcount:
                        count += 1
                except Exception as e:
                    logger.debug(f"[即時股價] {code} 寫入失敗: {e}")

        conn.commit()
    return count


def fetch_mops_quarterly_eps():
    """
    從公開資訊觀測站抓取最新一季的 EPS（比政府 API 快數天）。

    重要防呆（累計→單季轉換）：
    - MOPS 回傳的所有數值都是「累計值」，不是單季值！
    - Q1：累計=單季，直接使用
    - Q2/Q3：單季 = 本季累計 - DB中前季累計。若DB缺前季資料 → 跳過不寫入
    - Q4：沒有前3季 → 只存年度EPS，不存季度（避免累計值汙染單季欄位）
    - 曾因此 bug 導致累計EPS被當成單季寫入，造成數據嚴重錯誤

    寫入 quarterly_financial + 同步 stocks 表。
    後續由群益 zce 覆蓋更正確的單季數據，FinMind 最後補齊。
    """
    from bs4 import BeautifulSoup
    from mops_fetcher import is_quarterly_filing_period

    # 只在申報期內才抓
    in_period, target_year, target_season = is_quarterly_filing_period()
    if not in_period:
        return 0

    seasons_to_try = [(target_year, target_season)]

    total_updated = 0
    for yr, sn in seasons_to_try:
        quarter_key = f"{yr}Q{sn}"
        for typek, label in [('sii', '上市'), ('otc', '上櫃')]:
            try:
                _mops_s = create_session()
                resp = _mops_s.post(
                    'https://mopsov.twse.com.tw/mops/web/ajax_t163sb19',
                    data={'encodeURIComponent': '1', 'step': '1', 'firstin': '1',
                          'off': '1', 'TYPEK': typek, 'year': str(yr), 'season': str(sn)},
                    headers={'Content-Type': 'application/x-www-form-urlencoded'},
                    timeout=15
                )
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, 'html.parser')

                with sqlite3.get_conn() as conn:
                    c = conn.cursor()
                    count = 0
                    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    def parse_k(s):
                        try: return float(s.replace(',', '')) * 1000
                        except Exception: return None

                    for table in soup.find_all('table'):
                        for row in table.find_all('tr'):
                            cells = [td.get_text(strip=True) for td in row.find_all('td')]
                            if len(cells) < 9 or not cells[0].isdigit() or len(cells[0]) != 4:
                                continue
                            code = cells[0]
                            try:
                                cum_eps = float(cells[3].replace(',', ''))
                            except Exception: continue
                            cum_revenue = parse_k(cells[5])
                            cum_oi = parse_k(cells[6])
                            cum_nonop = parse_k(cells[7])
                            cum_ni = parse_k(cells[8])
    
                            # === 累計→單季轉換 ===
                            # 重要：MOPS 回傳的是累計值，必須減去前季累計才是單季
                            # Q1：累計=單季，直接用
                            # Q2/Q3/Q4：單季 = 本季累計 - 前季累計
                            # 防呆：若 DB 缺前季資料無法轉換，跳過不寫入（避免累計值被當成單季值）
                            single_eps = cum_eps
                            single_rev = cum_revenue
                            single_oi = cum_oi
                            single_nonop = cum_nonop
                            single_ni = cum_ni
    
                            if sn > 1:
                                # 從 DB 取得前季的累計值（前幾季的 quarterly_financial 加總）
                                prev_quarters = [f"{yr}Q{q}" for q in range(1, sn)]
                                placeholders = ','.join('?' * len(prev_quarters))
                                prev = c.execute(f"""SELECT SUM(revenue), SUM(operating_income),
                                    SUM(non_operating), SUM(net_income_parent), SUM(eps)
                                    FROM quarterly_financial
                                    WHERE code=? AND quarter IN ({placeholders})""",
                                    [code] + prev_quarters).fetchone()
    
                                if prev and prev[4] is not None:
                                    single_eps = round(cum_eps - prev[4], 4)
                                    single_rev = round(cum_revenue - prev[0], 2) if cum_revenue and prev[0] else cum_revenue
                                    single_oi = round(cum_oi - prev[1], 2) if cum_oi and prev[1] else cum_oi
                                    single_nonop = round(cum_nonop - prev[2], 2) if cum_nonop and prev[2] else cum_nonop
                                    single_ni = round(cum_ni - prev[3], 2) if cum_ni and prev[3] else cum_ni
                                elif sn == 4:
                                    # Q4：沒有前3季資料，EPS 是全年累計→存到年度，不存季度
                                    # 寫入 stocks 表的 eps_y1
                                    cur_y = c.execute("SELECT eps_y1_label FROM stocks WHERE code=?", (code,)).fetchone()
                                    if cur_y and str(cur_y[0]) != str(yr):
                                        c.execute("""UPDATE stocks SET
                                            eps_y6=eps_y5, eps_y6_label=eps_y5_label,
                                            eps_y5=eps_y4, eps_y5_label=eps_y4_label,
                                            eps_y4=eps_y3, eps_y4_label=eps_y3_label,
                                            eps_y3=eps_y2, eps_y3_label=eps_y2_label,
                                            eps_y2=eps_y1, eps_y2_label=eps_y1_label,
                                            eps_y1=?, eps_y1_label=?,
                                            eps_ytd=?, eps_ytd_label=?
                                            WHERE code=?""",
                                            (cum_eps, str(yr), cum_eps, str(yr), code))
                                        count += 1
                                    continue  # Q4 不寫 quarterly_financial 的 eps
                                else:
                                    # Q2/Q3 缺前季資料，無法從累計轉單季 → 跳過，避免累計值被當成單季值寫入
                                    continue
    
                            # 寫入 quarterly_financial（單季值）
                            # 群益優先：損益表欄位用 COALESCE(existing, new) 不覆蓋群益已有值
                            c.execute("""INSERT INTO quarterly_financial
                                (code, quarter, revenue, operating_income, non_operating,
                                 net_income_parent, eps, updated_at)
                                VALUES (?,?,?,?,?,?,?,?)
                                ON CONFLICT(code, quarter) DO UPDATE SET
                                revenue=COALESCE(revenue, excluded.revenue),
                                operating_income=COALESCE(operating_income, excluded.operating_income),
                                non_operating=COALESCE(non_operating, excluded.non_operating),
                                net_income_parent=COALESCE(net_income_parent, excluded.net_income_parent),
                                eps=COALESCE(eps, excluded.eps),
                                updated_at=excluded.updated_at""",
                                (code, quarter_key, single_rev, single_oi, single_nonop,
                                 single_ni, single_eps, now_str))
    
                            # 同步到 stocks 表（推移 eps_1~eps_5，存單季 EPS）
                            cur = c.execute("SELECT eps_1q FROM stocks WHERE code=?", (code,)).fetchone()
                            if cur and cur[0] != quarter_key:
                                c.execute("""UPDATE stocks SET
                                    eps_5=eps_4, eps_5q=eps_4q,
                                    eps_4=eps_3, eps_4q=eps_3q,
                                    eps_3=eps_2, eps_3q=eps_2q,
                                    eps_2=eps_1, eps_2q=eps_1q,
                                    eps_1=?, eps_1q=?
                                    WHERE code=?""",
                                    (single_eps, quarter_key, code))
                                count += 1
                            elif cur and cur[0] == quarter_key:
                                c.execute("UPDATE stocks SET eps_1=? WHERE code=?",
                                          (single_eps, code))

                    conn.commit()
                if count > 0:
                    print(f"[MOPS] {label} {quarter_key}: 新增 {count} 支 EPS")
                total_updated += count
            except Exception as e:
                print(f"[MOPS] {label} {quarter_key}: 失敗 {e}")

    if total_updated > 0:
        _sync_eps_from_quarterly()
    return total_updated


def cross_validate_financial():
    """
    交叉驗證財報資料：比對 quarterly_financial（群益/MOPS）vs stocks（政府API）。
    差異 > 5% 的記錄到 data_validation_log 表。
    """
    with sqlite3.get_conn() as conn:
        return _cross_validate_financial_inner(conn)

def _cross_validate_financial_inner(conn):
    c = conn.cursor()

    # 建表
    c.execute("""CREATE TABLE IF NOT EXISTS data_validation_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        check_date  TEXT NOT NULL,
        code        TEXT NOT NULL,
        field       TEXT NOT NULL,
        source_a    TEXT,
        value_a     REAL,
        source_b    TEXT,
        value_b     REAL,
        diff_pct    REAL,
        resolved    INTEGER DEFAULT 0
    )""")

    now_str = datetime.now().strftime('%Y-%m-%d')

    # 清除今天已有的（避免重複）
    c.execute("DELETE FROM data_validation_log WHERE check_date=?", (now_str,))

    issues = 0
    # 1. 比對 quarterly_financial EPS vs stocks 表 eps_1
    rows = conn.execute("""
        SELECT q.code, q.quarter, q.eps as q_eps, s.eps_1, s.eps_1q, s.name
        FROM quarterly_financial q
        JOIN stocks s ON q.code = s.code AND q.quarter = s.eps_1q
        WHERE q.eps IS NOT NULL AND s.eps_1 IS NOT NULL
    """).fetchall()

    for r in rows:
        q_eps = r[2]
        s_eps = r[3]
        if q_eps == 0 and s_eps == 0:
            continue
        base = max(abs(q_eps), abs(s_eps), 0.01)
        diff = abs(q_eps - s_eps) / base * 100
        if diff > 5:
            c.execute("""INSERT INTO data_validation_log
                (check_date, code, field, source_a, value_a, source_b, value_b, diff_pct)
                VALUES (?,?,?,?,?,?,?,?)""",
                (now_str, r[0], 'EPS', '季度財報', q_eps, '總表', s_eps, round(diff, 2)))
            issues += 1

    # 2. 比對 quarterly_financial 各季營收加總 vs financial_annual 年營收
    roc_year = datetime.now().year - 1911
    for yr in [roc_year - 1, roc_year - 2]:
        q_revs = conn.execute("""
            SELECT code, SUM(revenue) as q_total
            FROM quarterly_financial
            WHERE quarter LIKE ? AND revenue IS NOT NULL
            GROUP BY code
        """, (f'{yr}Q%',)).fetchall()

        for qr in q_revs:
            a_rev = conn.execute("""
                SELECT revenue FROM financial_annual
                WHERE code=? AND year=?
            """, (qr[0], yr + 1911)).fetchone()
            if not a_rev or not a_rev[0]:
                continue
            q_total = qr[1]
            a_total = a_rev[0]
            base = max(abs(q_total), abs(a_total), 1)
            diff = abs(q_total - a_total) / base * 100
            if diff > 5:
                c.execute("""INSERT INTO data_validation_log
                    (check_date, code, field, source_a, value_a, source_b, value_b, diff_pct)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (now_str, qr[0], f'{yr}年營收', '季度加總', round(q_total),
                     '年度財報', round(a_total), round(diff, 2)))
                issues += 1

    # 3. 比對 quarterly_financial 各季 EPS 加總 vs financial_annual 年 EPS
    for yr in [roc_year - 1, roc_year - 2]:
        q_eps_sum = conn.execute("""
            SELECT code, SUM(eps) as q_total, COUNT(*) as q_cnt
            FROM quarterly_financial
            WHERE quarter LIKE ? AND eps IS NOT NULL
            GROUP BY code HAVING q_cnt = 4
        """, (f'{yr}Q%',)).fetchall()

        for qr in q_eps_sum:
            a_eps = conn.execute("""
                SELECT eps FROM financial_annual WHERE code=? AND year=?
            """, (qr[0], yr + 1911)).fetchone()
            if not a_eps or a_eps[0] is None:
                continue
            q_total = round(qr[1], 2)
            a_total = a_eps[0]
            base = max(abs(q_total), abs(a_total), 0.01)
            diff = abs(q_total - a_total) / base * 100
            if diff > 10:  # EPS 容差大一點
                c.execute("""INSERT INTO data_validation_log
                    (check_date, code, field, source_a, value_a, source_b, value_b, diff_pct)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (now_str, qr[0], f'{yr}年EPS', '季度加總', q_total,
                     '年度財報', a_total, round(diff, 2)))
                issues += 1

    # 4. 季報內部一致性：毛利 - 營業費用 = 營業利益
    rows = conn.execute("""
        SELECT code, quarter, gross_profit, operating_expense, operating_income
        FROM quarterly_financial
        WHERE gross_profit IS NOT NULL AND operating_expense IS NOT NULL AND operating_income IS NOT NULL
        AND quarter LIKE ?
    """, (f'{roc_year}Q%',)).fetchall()
    for r in rows:
        expected_oi = r[2] - r[3]
        actual_oi = r[4]
        if abs(expected_oi - actual_oi) > 1000000:  # 容差 1M
            diff_pct = abs(expected_oi - actual_oi) / max(abs(actual_oi), 1) * 100
            c.execute("""INSERT INTO data_validation_log
                (check_date, code, field, source_a, value_a, source_b, value_b, diff_pct)
                VALUES (?,?,?,?,?,?,?,?)""",
                (now_str, r[0], f'{r[1]}營業利益', '毛利-費用', round(expected_oi),
                 'DB值', round(actual_oi), round(diff_pct, 2)))
            issues += 1

    # 5. 季報內部一致性：稅前淨利 > 稅後淨利（正常情況）
    rows = conn.execute("""
        SELECT code, quarter, pretax_income, net_income_parent
        FROM quarterly_financial
        WHERE pretax_income IS NOT NULL AND net_income_parent IS NOT NULL
        AND net_income_parent > pretax_income * 1.1 AND pretax_income > 0
        AND quarter LIKE ?
    """, (f'{roc_year}Q%',)).fetchall()
    for r in rows:
        diff_pct = (r[3] - r[2]) / r[2] * 100
        c.execute("""INSERT INTO data_validation_log
            (check_date, code, field, source_a, value_a, source_b, value_b, diff_pct)
            VALUES (?,?,?,?,?,?,?,?)""",
            (now_str, r[0], f'{r[1]}稅後>稅前', '稅前淨利', round(r[2]),
             '稅後淨利', round(r[3]), round(diff_pct, 2)))
        issues += 1

    # 6. stocks 表 div_label 排序正確性（最新年度在 div_1）
    rows = conn.execute("""
        SELECT code, div_1_label, div_2_label FROM stocks
        WHERE div_1_label IS NOT NULL AND div_2_label IS NOT NULL
        AND CAST(div_1_label AS INTEGER) < CAST(div_2_label AS INTEGER)
    """).fetchall()
    for r in rows:
        c.execute("""INSERT INTO data_validation_log
            (check_date, code, field, source_a, value_a, source_b, value_b, diff_pct)
            VALUES (?,?,?,?,?,?,?,?)""",
            (now_str, r[0], '股利排序錯誤', 'div_1_label', int(r[1]),
             'div_2_label', int(r[2]), 0))
        issues += 1

    conn.commit()
    if issues:
        print(f"  [交叉驗證] 發現 {issues} 筆差異")
    else:
        print(f"  [交叉驗證] 全部一致")
    return issues


def _fix_tax_data():
    """修正 DB 中 tax=0 的異常資料（本機+Render 通用）"""
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        fixed = 0

        # 季度：tax=0 但 pti > nip
        c.execute('''UPDATE quarterly_financial
            SET tax = ROUND(pretax_income - net_income_parent, 2)
            WHERE tax = 0 AND pretax_income IS NOT NULL AND net_income_parent IS NOT NULL
            AND pretax_income > net_income_parent AND pretax_income - net_income_parent > 100''')
        fixed += c.rowcount

        # 季度：pti == nip 異常
        c.execute('''UPDATE quarterly_financial
            SET tax = ROUND(pretax_income * 0.20, 2),
                net_income_parent = ROUND(pretax_income * 0.80, 2)
            WHERE pretax_income IS NOT NULL AND net_income_parent IS NOT NULL
            AND ABS(pretax_income - net_income_parent) < 1 AND pretax_income > 1000000''')
        fixed += c.rowcount

        # 年度：同上
        c.execute('''UPDATE financial_annual
            SET tax = ROUND(pretax_income - net_income, 2)
            WHERE tax = 0 AND pretax_income IS NOT NULL AND net_income IS NOT NULL
            AND pretax_income > net_income AND pretax_income - net_income > 100''')
        fixed += c.rowcount

        c.execute('''UPDATE financial_annual
            SET tax = ROUND(pretax_income * 0.20, 2),
                net_income = ROUND(pretax_income * 0.80, 2)
            WHERE pretax_income IS NOT NULL AND net_income IS NOT NULL
            AND ABS(pretax_income - net_income) < 1 AND pretax_income > 1000000''')
        fixed += c.rowcount

        # 年度：nip 補填
        c.execute('''UPDATE financial_annual
            SET net_income_parent = net_income
            WHERE net_income_parent IS NULL AND net_income IS NOT NULL''')
        fixed += c.rowcount

        conn.commit()
    if fixed > 0:
        print(f"  稅務資料修正：{fixed} 筆")


# ══════════════════════════════════════════════════════════════
# 三層架構：run_prices / run_maintenance / quick_update
# ══════════════════════════════════════════════════════════════

def run_prices(scheduled=True):
    """14:30 盤後更新：股價 + 等級 + 評價 + push。目標 2~3 分鐘完成。"""
    with ScraperLock('run_prices', timeout_sec=300) as lock:
        if lock is None:
            return
        _run_prices_inner(scheduled)

def _run_prices_inner(scheduled=True):
    if scheduled:
        jitter = random.randint(0, 30)
        print(f"[排程抖動] 延遲 {jitter} 秒後開始...")
        time.sleep(jitter)

    t0 = time.time()
    def _elapsed():
        return f"{time.time()-t0:.1f}s"

    print(f"\n{'='*50}")
    print(f"股價更新  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")
    init_db()

    # 1. 平行抓取股票清單 + 股價
    t1 = time.time()
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_twse = pool.submit(fetch_twse)
        f_tpex = pool.submit(fetch_tpex)
        twse_rows = f_twse.result()
        tpex_rows = f_tpex.result()
    all_rows = twse_rows + tpex_rows
    print(f"[1.股價] {len(all_rows)} 支，{time.time()-t1:.1f}s")

    # 2. 直接寫入股價（不走 save_to_db，避免 audit lock）
    t1 = time.time()
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        new_count = 0
        for r in all_rows:
            code = r.get('code')
            if not code:
                continue
            # 新股 INSERT（首次出現）
            c.execute("SELECT code FROM stocks WHERE code=?", (code,))
            if not c.fetchone():
                c.execute("INSERT INTO stocks (code, name, market) VALUES (?,?,?)",
                          (code, r.get('name'), r.get('market')))
                new_count += 1
            # UPDATE 股價欄位
            c.execute("""UPDATE stocks SET
                close=?, change=?, volume=?, open=?, high=?, low=?,
                updated_at=?
                WHERE code=?""",
                (r.get('close'), r.get('change'), r.get('volume'),
                 r.get('open'), r.get('high'), r.get('low'),
                 now_str, code))
        conn.commit()
    if new_count:
        print(f"[2.寫入DB] {len(all_rows)} 筆（{new_count} 支新股），{time.time()-t1:.1f}s")
    else:
        print(f"[2.寫入DB] {len(all_rows)} 筆，{time.time()-t1:.1f}s")

    # 3. 股價修正：批次 API 日期不對 → 即時 API 覆蓋
    if _twse_batch_date and _twse_batch_date != _today_roc() and datetime.now().weekday() < 5:
        t1 = time.time()
        rt_count = _refresh_realtime()
        print(f"[3.股價修正] 即時API {rt_count} 支，{time.time()-t1:.1f}s")

    # 4. 每日價量 + 評價快照（等級重算由 quick_update / run_maintenance 負責）
    t1 = time.time()
    _save_daily_price()
    snapshot_stock_states()
    try: focus_signal_check()
    except Exception as e: print(f"[重點追蹤] 失敗: {e}")
    print(f"[4.評價快照] {time.time()-t1:.1f}s")

    # 5. Checklist + 衍生欄位（Checklist 由本機同步，Render 不獨立計算）
    t1 = time.time()
    try:
        from app import calc_all_checklists, recalc_all_derived
        if not IS_CLOUD:
            calc_all_checklists()
        recalc_all_derived()
    except Exception as e:
        print(f"[Checklist/Derived] 失敗: {e}")
    print(f"[5.Checklist] {time.time()-t1:.1f}s")

    # 6. Push 到 Render（只推股價+等級+評價）
    t1 = time.time()
    if not IS_CLOUD:
        try:
            with ThreadPoolExecutor(max_workers=3) as pool:
                pool.submit(_push_prices_to_render)
                pool.submit(_push_annual_to_render)
                pool.submit(_push_estimates_to_render)
        except Exception as e:
            print(f"[Push] 失敗: {e}")
    print(f"[6.Push Render] {time.time()-t1:.1f}s")

    print(f"\n股價更新完成！{len(all_rows)} 支，總耗時 {_elapsed()}")


def run_maintenance(scheduled=True):
    """06:00 每日維護：補缺資料 + 股利 + ETF + 驗證 + 全量 push。不趕時間。"""
    with ScraperLock('run_maintenance', timeout_sec=5400) as lock:
        if lock is None:
            return
        _run_maintenance_inner(scheduled)

def _run_maintenance_inner(scheduled=True):
    if scheduled:
        jitter = random.randint(0, 60)
        print(f"[排程抖動] 延遲 {jitter} 秒後開始...")
        time.sleep(jitter)

    t0 = time.time()
    def _elapsed():
        return f"{time.time()-t0:.1f}s"

    print(f"\n{'='*50}")
    print(f"每日維護  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")
    init_db()

    # 1. 240 日歷史收盤價
    t1 = time.time()
    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(fetch_twse_history_240d)
        f2 = pool.submit(fetch_tpex_history_240d)
        twse_hist = f1.result()
        tpex_hist = f2.result()
    hist_map = {**twse_hist, **tpex_hist}
    # 更新 change_240d
    if hist_map:
        with sqlite3.get_conn() as conn:
            c = conn.cursor()
            for code, hist_price in hist_map.items():
                c.execute("SELECT close FROM stocks WHERE code=?", (code,))
                row = c.fetchone()
                if row and row[0]:
                    chg = calc_change_240d(row[0], hist_price)
                    if chg is not None:
                        c.execute("UPDATE stocks SET change_240d=? WHERE code=?", (chg, code))
            conn.commit()
    print(f"[1.240日歷史] {len(hist_map)} 支，{time.time()-t1:.1f}s")

    # 2. 股利（政府 API 批次 → financial_annual → stocks）
    t1 = time.time()
    div_map = fetch_dividends_bulk()
    # 先寫入 financial_annual（COALESCE 不覆蓋群益已有值）
    if div_map:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.get_conn() as conn:
            c = conn.cursor()
            for code, div in div_map.items():
                # div 裡是 div_c1/div_s1/div_1_label 格式，需要轉成 year/cash/stock
                for i in range(1, 7):
                    label = div.get(f'div_{i}_label')
                    cash = div.get(f'div_c{i}')
                    stock = div.get(f'div_s{i}')
                    if label is None:
                        continue
                    year = int(label) + 1911
                    if year >= datetime.now().year:
                        continue  # 當年及未來年度的年報不存在，跳過
                    c.execute("""INSERT INTO financial_annual (code, year, cash_dividend, stock_dividend, updated_at)
                        VALUES (?,?,?,?,?)
                        ON CONFLICT(code, year) DO UPDATE SET
                        cash_dividend = COALESCE(financial_annual.cash_dividend, excluded.cash_dividend),
                        stock_dividend = COALESCE(financial_annual.stock_dividend, excluded.stock_dividend),
                        updated_at = excluded.updated_at""",
                        (code, year, cash, stock, now_str))
            conn.commit()
    # 從 financial_annual 統一同步到 stocks（確保完整性）
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        all_codes = [r[0] for r in c.execute("SELECT code FROM stocks WHERE close IS NOT NULL").fetchall()]
    if all_codes:
        _sync_dividends_from_financial(all_codes)
    print(f"[2.股利] {len(div_map)} 支，{time.time()-t1:.1f}s")

    # 3. 年度 EPS 歷史（BWIBBU 反推 → financial_annual → stocks）
    t1 = time.time()
    hist = fetch_eps_annual_history()
    # 先寫入 financial_annual（COALESCE 不覆蓋已有值）
    if hist:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.get_conn() as conn:
            c = conn.cursor()
            cur_west_year = datetime.now().year  # 當年年報尚未公告，不寫入
            for code, years in hist.items():
                for yr, eps_val in years.items():
                    west_year = int(yr) + 1911
                    if west_year >= cur_west_year:
                        continue  # 當年及未來年度的年報不存在，跳過
                    c.execute("""INSERT INTO financial_annual (code, year, eps, updated_at)
                        VALUES (?,?,?,?)
                        ON CONFLICT(code, year) DO UPDATE SET
                        eps = COALESCE(financial_annual.eps, excluded.eps),
                        updated_at = excluded.updated_at""",
                        (code, west_year, eps_val, now_str))
            conn.commit()
    # 從 financial_annual 統一同步到 stocks（確保完整性）
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        all_codes = [r[0] for r in c.execute("SELECT code FROM stocks WHERE close IS NOT NULL").fetchall()]
    if all_codes:
        _sync_annual_eps_from_financial(all_codes, None)
    print(f"[3.BWIBBU] {len(hist)} 支，{time.time()-t1:.1f}s")

    # 4. 產業別 + 營收官方值
    t1 = time.time()
    with sqlite3.get_conn() as conn:
        _post_process_after_save_inner(conn)
    print(f"[4.產業別+等級] {time.time()-t1:.1f}s")

    # 5. 合併補缺（群益 8 並發）
    t1 = time.time()
    _fill_all_gaps()
    print(f"[5.補缺] {time.time()-t1:.1f}s")

    # 6. 系統 EPS 估算
    t1 = time.time()
    _batch_system_estimate()
    _batch_annual_estimate()
    print(f"[6.系統估算] {time.time()-t1:.1f}s")

    # 7. BWIBBU 股利補充 + EPS/合約負債同步
    t1 = time.time()
    _fill_dividends_from_bwibbu()
    _sync_eps_from_quarterly()
    _sync_contract_from_quarterly()
    print(f"[7.股利補充+同步] {time.time()-t1:.1f}s")

    # 8. 等級重算 + 評價快照
    t1 = time.time()
    _refresh_fin_grades()
    _refresh_grades_from_pbr()
    _save_daily_price()
    snapshot_stock_states()
    print(f"[8.等級+快照] {time.time()-t1:.1f}s")

    # 9. Yahoo 財報補充 + 月營收歷史
    t1 = time.time()
    _prefetch_watchlist_details()
    print(f"[9.Yahoo+月營收] {time.time()-t1:.1f}s")

    # 10. ETF 成分股
    t1 = time.time()
    try:
        from etf_fetcher import run as etf_run
        etf_run()
    except Exception as e:
        print(f"[ETF] 失敗: {e}")
    print(f"[10.ETF] {time.time()-t1:.1f}s")

    # 11. 交叉校驗
    t1 = time.time()
    try:
        from guardian import cross_validate
        cv = cross_validate(sample_size=20)
        if cv['mismatches']:
            print(f"[交叉校驗] {cv['checked']} 支抽查，{len(cv['mismatches'])} 支有差異")
        else:
            print(f"[交叉校驗] {cv['checked']} 支全部一致")
    except Exception as e:
        print(f"[交叉校驗] 失敗: {e}")
    print(f"[11.校驗] {time.time()-t1:.1f}s")

    # 12. Checklist + 衍生欄位（Checklist 由本機同步，Render 不獨立計算）
    t1 = time.time()
    try:
        from app import calc_all_checklists, recalc_all_derived
        if not IS_CLOUD:
            calc_all_checklists()
        recalc_all_derived()
    except Exception as e:
        print(f"[Checklist] 失敗: {e}")
    print(f"[12.Checklist] {time.time()-t1:.1f}s")

    # 13. 全量 Push 到 Render
    t1 = time.time()
    if not IS_CLOUD:
        _push_all_to_render()
    print(f"[13.Push Render] {time.time()-t1:.1f}s")

    print(f"\n每日維護完成！總耗時 {_elapsed()}")


def _fill_all_gaps():
    """
    合併補缺：一次掃描所有股票，找出缺 EPS/股利/財報/PE 的，
    每支一次補齊所有缺漏，群益 8 並發。
    取代原本分散的 _check_annual_eps/dividend_completeness + _fill_missing_financials。
    """
    from capital_fetcher import (
        fetch_capital_annual_eps, fetch_capital_dividend,
        fetch_all_three
    )

    cur_year = date.today().year
    cur_roc = cur_year - 1911
    expected_year = str(cur_roc - 1)  # 預期最新年度 EPS/股利（如 114）

    # 查缺
    with sqlite3.get_conn() as conn:
        c = conn.cursor()

        # 缺年度 EPS（4~6 月才檢查）
        needs_eps = set()
        if 4 <= date.today().month <= 6:
            for r in c.execute(
                "SELECT code FROM stocks WHERE close IS NOT NULL AND (eps_y1_label IS NULL OR eps_y1_label != ?)",
                (expected_year,)
            ).fetchall():
                needs_eps.add(r[0])

        # 缺股利
        needs_div = set()
        for r in c.execute(
            "SELECT code FROM stocks WHERE close IS NOT NULL AND (div_1_label IS NULL OR div_1_label != ?)",
            (expected_year,)
        ).fetchall():
            needs_div.add(r[0])

        # 缺關鍵財報（年報 equity/cf/capex/accounts_receivable 或 PE 歷史）
        needs_financial = set()
        for r in c.execute("""
            SELECT DISTINCT s.code FROM stocks s
            WHERE s.close IS NOT NULL AND (
                s.code IN (
                    SELECT code FROM financial_annual WHERE year = ? AND (
                        total_equity IS NULL OR operating_cf IS NULL OR capex IS NULL
                        OR accounts_receivable IS NULL
                    )
                )
                OR (s.code NOT IN (SELECT DISTINCT code FROM pe_history)
                    AND s.eps_y1 IS NOT NULL AND s.eps_y1 > 0)
            )
        """, (cur_year - 1,)).fetchall():
            needs_financial.add(r[0])

    # 合併去重
    all_needs = {}  # {code: {'eps', 'div', 'fin'}}
    for code in needs_eps:
        all_needs.setdefault(code, set()).add('eps')
    for code in needs_div:
        all_needs.setdefault(code, set()).add('div')
    for code in needs_financial:
        all_needs.setdefault(code, set()).add('fin')

    if not all_needs:
        print(f"  [補缺] 無缺漏")
        return

    total_eps = sum(1 for v in all_needs.values() if 'eps' in v)
    total_div = sum(1 for v in all_needs.values() if 'div' in v)
    total_fin = sum(1 for v in all_needs.values() if 'fin' in v)
    print(f"  [補缺] {len(all_needs)} 支需補齊（EPS:{total_eps} 股利:{total_div} 財報:{total_fin}）")

    def _fill_one(code, needs):
        """單支股票補齊所有缺漏"""
        try:
            if 'fin' in needs:
                # 需要財報 → 跑全套（已包含 EPS + 股利）
                fetch_all_three(code)
            else:
                if 'eps' in needs:
                    fetch_capital_annual_eps(code)
                if 'div' in needs:
                    fetch_capital_dividend(code)
            time.sleep(random.uniform(0.1, 0.3))
            return code, True
        except Exception as e:
            logger.debug(f"[補缺] {code} 失敗: {e}")
            return code, False

    # 8 並發
    done, ok = 0, 0
    codes_list = list(all_needs.items())
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {}
        for i, (code, needs) in enumerate(codes_list):
            futures[pool.submit(_fill_one, code, needs)] = code
            if (i + 1) % 8 == 0:
                time.sleep(0.3)
        for f in as_completed(futures):
            code, success = f.result()
            done += 1
            if success:
                ok += 1
            if done % 100 == 0:
                print(f"    補缺進度：{done}/{len(codes_list)}")

    # 補完後同步 EPS + 股利到 stocks 表
    if needs_eps:
        _sync_annual_eps_from_financial(list(needs_eps), expected_year)
    if needs_div:
        _sync_dividends_from_financial(list(needs_div))

    print(f"  [補缺] 完成 {ok}/{len(codes_list)} 支")


def _sync_annual_eps_from_financial(codes, expected_year=None):
    """從 financial_annual 同步年度 EPS 到 stocks 表"""
    from collections import defaultdict
    max_year = date.today().year - 1  # 年度上限：當年-1（如 2026 年最新年報是 2025）
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        # 批次查詢所有 codes 的 EPS（排除未來年度）
        placeholders = ','.join('?' * len(codes))
        all_eps = c.execute(f"""SELECT code, year, eps FROM financial_annual
                               WHERE code IN ({placeholders}) AND eps IS NOT NULL
                               AND year <= ?
                               ORDER BY code, year DESC""", codes + [max_year]).fetchall()
        eps_by_code = defaultdict(list)
        for r in all_eps:
            if len(eps_by_code[r[0]]) < 6:
                eps_by_code[r[0]].append(r)
        for code in codes:
            rows = eps_by_code.get(code, [])
            if not rows:
                continue
            for i, (_, year, eps) in enumerate(rows, 1):
                roc_yr = str(year - 1911)
                c.execute(f"UPDATE stocks SET eps_y{i}=?, eps_y{i}_label=? WHERE code=?",
                          (eps, roc_yr, code))
            for i in range(len(rows) + 1, 7):
                c.execute(f"UPDATE stocks SET eps_y{i}=NULL, eps_y{i}_label=NULL WHERE code=?",
                          (code,))
        conn.commit()


def _sync_dividends_from_financial(codes):
    """從 financial_annual 同步股利到 stocks 表"""
    from collections import defaultdict
    max_year = date.today().year - 1  # 年度上限：當年-1（排除未來年度）
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        placeholders = ','.join('?' * len(codes))
        all_divs = c.execute(f"""SELECT code, year, cash_dividend, stock_dividend FROM financial_annual
                               WHERE code IN ({placeholders}) AND (cash_dividend IS NOT NULL OR stock_dividend IS NOT NULL)
                               AND year <= ?
                               ORDER BY code, year DESC""", codes + [max_year]).fetchall()
        div_by_code = defaultdict(list)
        for r in all_divs:
            if len(div_by_code[r[0]]) < 6:
                div_by_code[r[0]].append(r)
        for code in codes:
            rows = div_by_code.get(code, [])
            for i, r in enumerate(rows, 1):
                roc_yr = str(r[1] - 1911)
                c.execute(f"UPDATE stocks SET div_c{i}=?, div_s{i}=?, div_{i}_label=? WHERE code=?",
                          (r[2], r[3], roc_yr, code))
        conn.commit()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--quick':
        quick_update()
    elif len(sys.argv) > 1 and sys.argv[1] == '--prices':
        run_prices()
    elif len(sys.argv) > 1 and sys.argv[1] == '--maintenance':
        run_maintenance()
    else:
        run()

