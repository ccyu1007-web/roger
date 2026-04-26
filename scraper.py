"""
爬蟲模組：從證交所 & 櫃買中心抓取上市/上櫃股票資料
資料來源：
  - 上市公司代碼+名稱：證交所 TWSE（t187ap03_L）
  - 上櫃公司代碼+名稱：櫃買中心 TPEX（tpex_mainboard_peratio_analysis）
  - 上市批次收盤價：證交所 TWSE OpenAPI（STOCK_DAY_ALL）
  - 上櫃批次收盤價：櫃買中心 TPEX OpenAPI（tpex_mainboard_quotes）
  - 即時股價（上市+上櫃）：證交所 MIS（mis.twse.com.tw，上市用 tse_ 前綴、上櫃用 otc_ 前綴）
  - 上櫃歷史：TPEX 每日收盤行情批次 API
  - 營收：政府API t187ap05（主要）/ FinMind TaiwanStockMonthRevenue（補充）
  - EPS：FinMind TaiwanStockFinancialStatements
  - 最新累計EPS：TWSE/TPEX t187ap14（批次，無限制）
  - 三大法人：群益證券 zcl（每天17:10後抓取）
"""

import requests
import db as sqlite3
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
import re
import os
from bs4 import BeautifulSoup
from guardian import (backup_raw_response, cleanup_old_backups,
                      validate_batch, get_breaker, get_priority_queue,
                      track_finmind_call, should_skip_finmind,
                      arbitrate_values,
                      get_active_provider, log_provider_switch,
                      sanity_check, audit_changes,
                      snapshot_stock_states, fetch_material_news,
                      fetch_moneydj_news, auto_archive_old_news)

DB_PATH = "stocks.db"

_session = requests.Session()
_session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; StockBot/1.0)"})

# 批次 API 回傳的資料日期（ROC 格式，如 "1150421"）
_twse_batch_date = None


def _today_roc():
    """今天的民國日期字串，如 '1150421'"""
    t = date.today()
    return f"{t.year - 1911}{t.strftime('%m%d')}"


# ── 資料庫初始化 ────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
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
        except:
            pass
    conn.commit()
    conn.close()
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
        conn = sqlite3.connect(DB_PATH)
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
        conn.close()
        _health_log = []
    except:
        pass


def safe_float(val):
    try:
        v = str(val).replace(",", "").strip()
        return float(v) if v not in ("", "--", "---", "N/A") else None
    except:
        return None

def safe_int(val):
    try:
        v = str(val).replace(",", "").strip()
        return int(v) if v not in ("", "--") else None
    except:
        return None

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
            print(f"  [警告] 第 {i+1} 次請求失敗：{e}")
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
    except:
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
                except:
                    pass
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
                except:
                    pass
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
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM stocks")
        result = {row['code']: dict(row) for row in c.fetchall()}
        conn.close()
        return result
    except:
        return {}


# ── 營收：FinMind（只抓缺少資料的股票）─────────────────────
def _fetch_revenue(code, start_date):
    try:
        time.sleep(random.uniform(0.1, 0.5))
        url = (f"https://api.finmindtrade.com/api/v4/data"
               f"?dataset=TaiwanStockMonthRevenue"
               f"&data_id={code}&start_date={start_date}")
        r = _session.get(url, timeout=15)
        data = r.json()
        if data.get('status') == 200:
            return code, data.get('data', [])
    except:
        pass
    return code, None

def _calc_revenue_metrics(records):
    if not records:
        return None
    rev_map = {}
    for r in records:
        try:
            y = int(r['revenue_year'])
            m = int(r['revenue_month'])
            v = float(r['revenue'])
            rev_map[(y, m)] = v
        except:
            pass
    if not rev_map:
        return None
    latest_ym = max(rev_map.keys())
    ly, lm    = latest_ym
    cur_rev   = rev_map[latest_ym]

    yoy_rev     = rev_map.get((ly - 1, lm))
    revenue_yoy = round((cur_rev / yoy_rev - 1) * 100, 2) if yoy_rev else None

    prev_ym = (ly - 1, 12) if lm == 1 else (ly, lm - 1)
    mom_rev     = rev_map.get(prev_ym)
    revenue_mom = round((cur_rev / mom_rev - 1) * 100, 2) if mom_rev else None

    cur_cum  = sum(rev_map.get((ly,     m), 0) for m in range(1, lm + 1))
    prev_cum = sum(rev_map.get((ly - 1, m), 0) for m in range(1, lm + 1))
    revenue_cum_yoy = round((cur_cum / prev_cum - 1) * 100, 2) if prev_cum else None

    return {
        'revenue_year': ly, 'revenue_month': lm,
        'revenue_yoy': revenue_yoy, 'revenue_mom': revenue_mom,
        'revenue_cum_yoy': revenue_cum_yoy,
    }

def fetch_revenue(codes, old_meta):
    today_str = date.today().strftime('%Y-%m-%d')
    rev_start = (date.today() - timedelta(days=425)).strftime('%Y-%m-%d')

    # 跳過已有今日營收的股票
    need_codes = [c for c in codes if not old_meta.get(c, {}).get('revenue_yoy')]
    cached     = len(codes) - len(need_codes)
    if cached:
        print(f"[營收] 已有 {cached} 支有舊資料，需抓取 {len(need_codes)} 支")
    else:
        print(f"[營收] 抓取 {len(need_codes)} 支")

    results = {}
    # 先把有舊資料的帶入
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

    if need_codes:
        done = 0
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(_fetch_revenue, c, rev_start): c for c in need_codes}
            for f in as_completed(futures):
                code, records = f.result()
                old = old_meta.get(code, {})
                metrics = _calc_revenue_metrics(records) if records else None
                if metrics:
                    old_y, old_m = old.get('revenue_year'), old.get('revenue_month')
                    if metrics['revenue_year'] != old_y or metrics['revenue_month'] != old_m:
                        rev_date = today_str
                    else:
                        rev_date = old.get('revenue_date') or today_str
                    results[code] = {**metrics, 'revenue_date': rev_date}
                done += 1
                if done % 200 == 0:
                    print(f"  營收進度：{done}/{len(need_codes)}")

    hit = sum(1 for v in results.values() if v.get('revenue_yoy') is not None)
    print(f"[營收] 完成：{hit}/{len(codes)} 筆含年增率")
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

    # ── FinMind 補充 111 年以後（含季配年度加總）──
    import re
    def _fetch_div_finmind(code):
        try:
            time.sleep(random.uniform(0.1, 0.5))
            url = (f"https://api.finmindtrade.com/api/v4/data"
                   f"?dataset=TaiwanStockDividend&data_id={code}&start_date=2020-01-01")
            r = _session.get(url, timeout=15)
            d = r.json()
            return d.get('data', []) if d.get('status') == 200 else []
        except:
            return []

    all_codes_set = set(div_map.keys())
    print(f"[股利] FinMind 補充 111 年以後（{len(all_codes_set)} 支）...")
    done = 0
    fail_streak = 0
    fm_div_calls = 0
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_fetch_div_finmind, c): c for c in all_codes_set}
        for f in as_completed(futures):
            code = futures[f]
            records = f.result()
            fm_div_calls += 1
            if records:
                fm_yearly = {}
                for rec in records:
                    yr_str = rec.get('year', '')
                    m_match = re.match(r'(\d+)年', yr_str)
                    if not m_match:
                        continue
                    roc_yr = m_match.group(1)
                    if int(roc_yr) <= 110:
                        continue  # 110 以前用政府 API
                    cash = float(rec.get('CashEarningsDistribution', 0) or 0)
                    cash2 = float(rec.get('CashStatutorySurplus', 0) or 0)
                    stock = float(rec.get('StockEarningsDistribution', 0) or 0)
                    stock2 = float(rec.get('StockStatutorySurplus', 0) or 0)
                    prev = fm_yearly.get(roc_yr, {'cash': 0, 'stock': 0})
                    fm_yearly[roc_yr] = {
                        'cash':  round(prev['cash'] + cash + cash2, 4),
                        'stock': round(prev['stock'] + stock + stock2, 4),
                    }
                # FinMind 覆蓋 111 年以後
                for roc_yr, vals in fm_yearly.items():
                    div_map.setdefault(code, {})[roc_yr] = vals
                fail_streak = 0
            else:
                fail_streak += 1
            done += 1
            if done % 200 == 0:
                print(f"  股利補充進度：{done}/{len(all_codes_set)}")
            if fail_streak >= 50 and done > 100:
                print(f"  [股利] 偵測到限速，提前結束")
                break
            if should_skip_finmind():
                break

    # ── 政府 t187ap45 補最新季度（只補 FinMind 沒有的年度）──
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
            # FinMind 已有此年度 → 跳過（FinMind 的加總更完整）
            if code in div_map and year in div_map[code]:
                continue
            cash  = safe_float(d.get('股東配發-盈餘分配之現金股利(元/股)')) or 0
            cash2 = safe_float(d.get('股東配發-法定盈餘公積發放之現金(元/股)')) or 0
            cash3 = safe_float(d.get('股東配發-資本公積發放之現金(元/股)')) or 0
            stock  = safe_float(d.get('股東配發-盈餘轉增資配股(元/股)')) or 0
            stock2 = safe_float(d.get('股東配發-法定盈餘公積轉增資配股(元/股)')) or 0
            stock3 = safe_float(d.get('股東配發-資本公積轉增資配股(元/股)')) or 0
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
    track_finmind_call(fm_div_calls)
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


# ── 合約負債：FinMind TaiwanStockBalanceSheet ──────────────
def _fetch_contract_liability(code, start_date):
    try:
        time.sleep(random.uniform(0.1, 0.5))
        url = (f"https://api.finmindtrade.com/api/v4/data"
               f"?dataset=TaiwanStockBalanceSheet"
               f"&data_id={code}&start_date={start_date}")
        r = _session.get(url, timeout=15)
        data = r.json()
        if data.get('status') == 200:
            return code, data.get('data', [])
    except:
        pass
    return code, None

def _calc_contract_metrics(records):
    if not records:
        return None
    # 找合約負債（ContractLiabilities），排除 _per 百分比型態，同季度加總（流動+非流動）
    quarter_sums = {}  # {label: {'date': date, 'value': total}}
    for r in records:
        t = r.get('type', '')
        if '_per' in t:
            continue
        if 'ContractLiabilit' in t or 'contract_liabilit' in t.lower():
            label = date_to_quarter_label(r['date'])
            if not label:
                continue
            if label not in quarter_sums:
                quarter_sums[label] = {'date': r['date'], 'value': 0}
            quarter_sums[label]['value'] += r['value']
    if not quarter_sums:
        return None
    # 按季度排序取最近 3 季
    sorted_qs = sorted(quarter_sums.items(), key=lambda x: x[1]['date'], reverse=True)[:3]
    result = {}
    for i, (label, data) in enumerate(sorted_qs, 1):
        result[f'contract_{i}']  = data['value']
        result[f'contract_{i}q'] = label
    for i in range(len(sorted_qs) + 1, 4):
        result[f'contract_{i}']  = None
        result[f'contract_{i}q'] = None
    return result

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
    today_str = date.today().strftime('%Y-%m-%d')
    cl_start = (date.today() - timedelta(days=500)).strftime('%Y-%m-%d')
    expected_q = _expected_latest_quarter()

    # 跳過已有最新季度資料的股票
    cl_keys = ['contract_1', 'contract_1q', 'contract_2', 'contract_2q', 'contract_3', 'contract_3q']
    need_codes = []
    results = {}
    for c in codes:
        old = old_meta.get(c, {})
        if old.get('contract_1q') and old['contract_1q'] >= expected_q:
            results[c] = {k: old.get(k) for k in cl_keys}
        else:
            need_codes.append(c)
    cached = len(codes) - len(need_codes)
    if cached:
        print(f"[合約負債] 已有 {cached} 支為最新（{expected_q}），需抓取 {len(need_codes)} 支")

    if need_codes:
        # 優先權排序：重要股票先抓
        need_codes = get_priority_queue(need_codes, 'contract')
        print(f"[合約負債] 開始抓取 {len(need_codes)} 支（已依優先權排序）...")
        done = 0
        fail_streak = 0
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(_fetch_contract_liability, c, cl_start): c
                       for c in need_codes}
            for f in as_completed(futures):
                code, records = f.result()
                track_finmind_call()
                metrics = _calc_contract_metrics(records) if records else None
                if metrics:
                    results[code] = metrics
                    fail_streak = 0
                else:
                    fail_streak += 1
                done += 1
                if done % 200 == 0:
                    print(f"  合約負債進度：{done}/{len(need_codes)}")
                if fail_streak >= 50 and done > 100:
                    print(f"  [合約負債] 偵測到限速，提前結束")
                    break
                if should_skip_finmind():
                    print(f"  [合約負債] 額度即將用盡，已完成 {done} 支")
                    break

    hit = sum(1 for v in results.values() if v.get('contract_1') is not None)
    print(f"[合約負債] 完成：{hit}/{len(codes)} 筆")
    return results


# ── EPS：FinMind 逐支抓取 ──────────────────────────────────
def _fetch_eps(code, start_date):
    try:
        time.sleep(random.uniform(0.1, 0.5))
        url = (f"https://api.finmindtrade.com/api/v4/data"
               f"?dataset=TaiwanStockFinancialStatements"
               f"&data_id={code}&start_date={start_date}")
        r = _session.get(url, timeout=15)
        data = r.json()
        if data.get('status') == 200:
            return code, data.get('data', [])
    except:
        pass
    return code, None

def _calc_eps_metrics(records):
    if not records:
        return None
    eps_list = []
    for r in records:
        if r.get('type') == 'EPS':
            label = date_to_quarter_label(r['date'])
            if label:
                eps_list.append({'date': r['date'], 'label': label, 'value': r['value']})
    if not eps_list:
        return None
    eps_list.sort(key=lambda x: x['date'], reverse=True)

    # 最近 5 季
    latest_5 = eps_list[:5]
    result = {}
    for i, ep in enumerate(latest_5, 1):
        result[f'eps_{i}']  = ep['value']
        result[f'eps_{i}q'] = ep['label']
    for i in range(len(latest_5) + 1, 6):
        result[f'eps_{i}']  = None
        result[f'eps_{i}q'] = None

    # 年度 EPS：依年份分組加總（只取四季齊全的年份）
    cur_roc_year = date.today().year - 1911  # 115
    yearly = {}  # {roc_year: {quarter: value}}
    for ep in eps_list:
        parts = ep['label'].split('Q')
        y, q = int(parts[0]), int(parts[1])
        yearly.setdefault(y, {})[q] = ep['value']

    # 當年度累計
    if cur_roc_year in yearly:
        qs = yearly[cur_roc_year]
        result['eps_ytd'] = round(sum(qs.values()), 2)
        result['eps_ytd_label'] = str(cur_roc_year)
    else:
        result['eps_ytd'] = None
        result['eps_ytd_label'] = None

    # 最近 6 個完整年度（4 季齊全）
    full_years = sorted(
        [y for y, qs in yearly.items() if len(qs) == 4 and y != cur_roc_year],
        reverse=True
    )[:6]
    for i, y in enumerate(full_years, 1):
        result[f'eps_y{i}'] = round(sum(yearly[y].values()), 2)
        result[f'eps_y{i}_label'] = str(y)
    for i in range(len(full_years) + 1, 7):
        result[f'eps_y{i}'] = None
        result[f'eps_y{i}_label'] = None

    return result

def fetch_eps(codes, old_meta):
    today_str = date.today().strftime('%Y-%m-%d')
    eps_start = (date.today() - timedelta(days=2200)).strftime('%Y-%m-%d')  # ~6 年

    # 需要抓取的條件：沒有完整季度 EPS（至少要有 eps_2q 才算完整）
    eps_keys = (
        ['eps_date']
        + [f'eps_{i}' for i in range(1,6)] + [f'eps_{i}q' for i in range(1,6)]
        + [f'eps_y{i}' for i in range(1,7)] + [f'eps_y{i}_label' for i in range(1,7)]
        + ['eps_ytd', 'eps_ytd_label']
    )

    expected_q = _expected_latest_quarter()
    need_codes = []
    for c in codes:
        old = old_meta.get(c, {})
        # 沒有完整季度，或最新季度過時 → 需重抓
        if not old.get('eps_2q') or (old.get('eps_1q') and old['eps_1q'] < expected_q):
            need_codes.append(c)
    cached = len(codes) - len(need_codes)
    if cached:
        print(f"[EPS] 已有 {cached} 支為最新（{expected_q}），需抓取 {len(need_codes)} 支")

    results = {}
    # 帶入舊 EPS 資料（只帶入不需重抓的）
    for code in codes:
        if code not in need_codes:
            old = old_meta.get(code, {})
            if old.get('eps_2q'):
                results[code] = {k: old.get(k) for k in eps_keys}

    if need_codes:
        # 優先權排序：重要股票先抓
        need_codes = get_priority_queue(need_codes, 'eps')
        print(f"[EPS] 開始抓取 {len(need_codes)} 支（已依優先權排序）...")
        done = 0
        fail_streak = 0
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(_fetch_eps, c, eps_start): c for c in need_codes}
            for f in as_completed(futures):
                code, records = f.result()
                track_finmind_call()
                old = old_meta.get(code, {})
                metrics = _calc_eps_metrics(records) if records else None
                if metrics:
                    new_q = metrics.get('eps_1q')
                    old_q = old.get('eps_1q')
                    eps_date = today_str if new_q != old_q else (old.get('eps_date') or today_str)
                    results[code] = {**metrics, 'eps_date': eps_date}
                    fail_streak = 0
                else:
                    fail_streak += 1
                done += 1
                if done % 200 == 0:
                    print(f"  EPS 進度：{done}/{len(need_codes)}")
                # 連續失敗太多次 → 可能被限速，提前結束
                if fail_streak >= 50 and done > 100:
                    print(f"  [EPS] 偵測到連續 {fail_streak} 次失敗，可能被限速，提前結束")
                    break
                # 額度預警：超過 90% 停止抓取
                if should_skip_finmind():
                    print(f"  [EPS] 額度即將用盡，已完成 {done} 支，剩餘留待下次")
                    break

    hit = sum(1 for v in results.values() if v.get('eps_1') is not None)
    print(f"[EPS] 完成：{hit}/{len(codes)} 筆含最近一季")
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
        conn_old = sqlite3.connect(DB_PATH)
        conn_old.row_factory = sqlite3.Row
        c_old = conn_old.cursor()
        c_old.execute("""SELECT code, close, eps_1, eps_1q, eps_y1, eps_ytd,
                                revenue_yoy, revenue_cum_yoy, revenue_month,
                                fin_grade_1, contract_1, div_c1
                         FROM stocks""")
        for r in c_old.fetchall():
            old_data[r['code']] = dict(r)
        conn_old.close()
    except:
        pass

    conn = sqlite3.connect(DB_PATH)
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
    conn.close()
    msg = f"[DB] 已更新 {len(rows)} 筆（UPSERT，不刪除舊資料）"
    if quarantined:
        msg += f"，{quarantined} 筆跳變被攔截"
    print(msg)


# ── 主程式 ──────────────────────────────────────────────────
def run(scheduled=True):
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

    # 1. 平行抓取股價
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_twse = pool.submit(fetch_twse)
        f_tpex = pool.submit(fetch_tpex)
        twse_rows = f_twse.result()
        tpex_rows = f_tpex.result()
    all_rows = twse_rows + tpex_rows
    all_codes = [r['code'] for r in all_rows]

    # 2. 平行抓取 240 日歷史
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

    # 5. 合約負債（FinMind）
    contract_map = fetch_contract_liabilities(all_codes, old_meta)

    # 6. EPS 年度 — 群益優先，政府API+BWIBBU反推驗證，FinMind補齊
    from capital_fetcher import fetch_capital_annual_eps_batch
    eps_capital = fetch_capital_annual_eps_batch(all_codes)  # 群益年度EPS（最優先）

    # 6b. EPS 年度（政府 API，批次無限制）— 驗證用
    eps_annual = fetch_eps_annual_bulk()

    # 6c. EPS 年度歷史（TWSE/TPEX 本益比反推，批次無限制）— 驗證+補齊
    eps_annual_hist = fetch_eps_annual_history()

    # 7. EPS 季度+歷史年度（FinMind，有速率限制）— 最後補齊
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

        # 多源合併年度 EPS：群益優先 → 政府API+BWIBBU驗證 → FinMind補齊
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

        # 第四層：FinMind（最後補齊）
        for i in range(1, 7):
            if r.get(f'eps_y{i}_label') and r.get(f'eps_y{i}') is not None:
                yr = r[f'eps_y{i}_label']
                if yr not in merged:
                    merged[yr] = r[f'eps_y{i}']

        if not r.get('eps_date') and merged:
            r['eps_date'] = today_str

        # 寫回最近 6 年
        sorted_yrs = sorted(merged.keys(), reverse=True)[:6]
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

    # 7. 補回 DELETE+INSERT 不包含的資料（產業別、年度EPS歷史、財務等級）
    print("[後處理] 補回輔助資料...")
    _post_process_after_save()

    elapsed = time.time() - t0
    rev_hit = sum(1 for r in all_rows if r.get('revenue_yoy') is not None)
    eps_hit = sum(1 for r in all_rows if r.get('eps_1') is not None)
    _flush_health_log()
    snapshot_stock_states()

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
    except:
        pass

    # 自動 push 年度資料到 Render（僅本機）
    if not os.environ.get('DATABASE_URL'):
        try: _push_annual_to_render()
        except: pass

    print(f"\n完成！共更新 {len(all_rows)} 筆")
    print(f"  營收年增率：{rev_hit} 筆")
    print(f"  EPS 資料：{eps_hit} 筆")
    print(f"  耗時：{elapsed:.1f} 秒")


def _post_process_after_save():
    """完整爬蟲 save_to_db 後，補回產業別、年度EPS歷史、營收官方值、財務等級"""
    conn = sqlite3.connect(DB_PATH)
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

    # ── 營收用政府官方值覆蓋（避免 FinMind 自算值） ──
    for label, url in [
        ("上市", "https://openapi.twse.com.tw/v1/openData/t187ap05_L"),
        ("上櫃", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"),
    ]:
        data = fetch_json(url)
        if not data: continue
        for d in data:
            code = str(d.get('公司代號', '')).strip()
            if not code: continue
            ym_str = str(d.get('資料年月', '')).strip()
            if len(ym_str) < 4: continue
            try:
                roc_year = int(ym_str[:-2])
                month = int(ym_str[-2:])
            except: continue
            yoy = safe_float(d.get('營業收入-去年同月增減(%)'))
            mom = safe_float(d.get('營業收入-上月比較增減(%)'))
            cum_yoy = safe_float(d.get('累計營業收入-前期比較增減(%)'))
            c.execute("""UPDATE stocks SET revenue_yoy=?, revenue_mom=?, revenue_cum_yoy=?
                         WHERE code=?""", (yoy, mom, cum_yoy, code))

    # ── eps_ytd 補齊 ──
    c.execute("UPDATE stocks SET eps_ytd=eps_y1, eps_ytd_label=eps_y1_label WHERE eps_ytd IS NULL AND eps_y1 IS NOT NULL")

    # ── 年度EPS歷史（TWSE/TPEX 本益比反推） ──
    hist = fetch_eps_annual_history()
    for code, years in hist.items():
        for yr, eps_val in years.items():
            # 只填空的年度
            for i in range(1, 7):
                c.execute(f"SELECT eps_y{i}_label FROM stocks WHERE code=?", (code,))
                r = c.fetchone()
                if r and r[0] == yr: break  # 已有
                if r and r[0] is None:
                    c.execute(f"UPDATE stocks SET eps_y{i}=?, eps_y{i}_label=? WHERE code=?", (eps_val, yr, code))
                    break

    conn.commit()
    conn.close()

    # ── 股利補充（BWIBBU 殖利率反推，不依賴 FinMind）──
    _fill_dividends_from_bwibbu()

    # ── 從 quarterly_financial 同步 EPS + 合約負債 到 stocks 表 ──
    _sync_eps_from_quarterly()
    _sync_contract_from_quarterly()

    # ── 財務等級重算（各自管理 DB 連線）──
    _refresh_fin_grades()
    _refresh_grades_from_pbr()

    # ── MOPS 最新季 EPS ──
    try: fetch_mops_quarterly_eps()
    except: pass

    # ── 稅務資料修正 ──
    _fix_tax_data()

    # ── 交叉驗證 ──
    try: cross_validate_financial()
    except: pass

    # ── 年報公告截止後：確認年度 EPS + 股利到齊 ──
    try: _check_annual_eps_completeness()
    except: pass
    try: _check_annual_dividend_completeness()
    except: pass

    # ── 季報公告截止後：用群益批次確認季度資料 ──
    try: _check_quarterly_completeness()
    except: pass

    # ── 系統 EPS 估算（季+年，批次更新所有股票）──
    _batch_system_estimate()
    _batch_annual_estimate()
    print("  後處理完成")


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

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 找出 eps_y1_label 不是最新年度的股票（代表還沒更新）
    c.execute("""SELECT code FROM stocks
                 WHERE close IS NOT NULL
                 AND (eps_y1_label IS NULL OR eps_y1_label != ?)""",
              (expected_year,))
    missing_codes = [r[0] for r in c.fetchall()]
    conn.close()

    if not missing_codes:
        print(f"[年度EPS檢查] 所有股票 {expected_year} 年 EPS 已到齊")
        return

    print(f"[年度EPS檢查] {len(missing_codes)} 支缺少 {expected_year} 年 EPS，從群益補抓...")

    from capital_fetcher import fetch_capital_annual_eps_batch
    cap_data = fetch_capital_annual_eps_batch(missing_codes)

    conn = sqlite3.connect(DB_PATH)
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
            eps_y5=?, eps_y5_label=?, eps_y6=?, eps_y6_label=?,
            eps_date=? WHERE code=?""",
            (vals['eps_y1'], vals['eps_y1_label'],
             vals['eps_y2'], vals['eps_y2_label'],
             vals['eps_y3'], vals['eps_y3_label'],
             vals['eps_y4'], vals['eps_y4_label'],
             vals['eps_y5'], vals['eps_y5_label'],
             vals['eps_y6'], vals['eps_y6_label'],
             now.strftime('%Y-%m-%d'), code))
        if c.rowcount:
            updated += 1

    conn.commit()
    conn.close()
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

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 找出 div_1_label 不是最新年度的股票
    c.execute("""SELECT code FROM stocks
                 WHERE close IS NOT NULL
                 AND (div_1_label IS NULL OR div_1_label != ?)""",
              (expected_year,))
    missing_codes = [r[0] for r in c.fetchall()]
    conn.close()

    if not missing_codes:
        print(f"[年度股利檢查] 所有股票 {expected_year} 年股利已到齊")
        return

    print(f"[年度股利檢查] {len(missing_codes)} 支缺少 {expected_year} 年股利，從群益補抓...")

    from capital_fetcher import fetch_capital_dividend
    updated = 0
    for i, code in enumerate(missing_codes):
        try:
            fetch_capital_dividend(code)
        except:
            pass
        if (i + 1) % 50 == 0:
            print(f"  股利補抓進度：{i+1}/{len(missing_codes)}")
            time.sleep(0.5)

    # 從 financial_annual 同步到 stocks 表
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for code in missing_codes:
        rows = c.execute("""SELECT year, cash_dividend, stock_dividend FROM financial_annual
                           WHERE code=? AND (cash_dividend IS NOT NULL OR stock_dividend IS NOT NULL)
                           ORDER BY year DESC LIMIT 6""", (code,)).fetchall()
        if not rows:
            continue
        for i, r in enumerate(rows, 1):
            roc_yr = str(r[0] - 1911)
            c.execute(f"UPDATE stocks SET div_c{i}=?, div_s{i}=?, div_{i}_label=? WHERE code=?",
                      (r[1], r[2], roc_yr, code))
        for i in range(len(rows) + 1, 7):
            c.execute(f"UPDATE stocks SET div_c{i}=NULL, div_s{i}=NULL, div_{i}_label=NULL WHERE code=?",
                      (code,))
        updated += 1

    conn.commit()
    conn.close()
    still_missing = len(missing_codes) - updated
    print(f"[年度股利檢查] 補齊 {updated} 支" +
          (f"，仍有 {still_missing} 支缺漏（可能尚未公告）" if still_missing else ""))


def _check_quarterly_completeness():
    """
    季報公告截止後 30 天，用群益 zce 批次比對 quarterly_financial 的資料。
    差異 > 5% 的用群益覆蓋（群益為準），並記 log。

    季報公告截止日 + 30天：
    - Q1：5/15 + 30 = 6/15
    - Q2：8/14 + 30 = 9/14
    - Q3：11/14 + 30 = 12/14
    - Q4：3/31 + 30 = 4/30
    """
    now = datetime.now()
    cur_roc = now.year - 1911
    month, day = now.month, now.day

    # 判斷現在應該確認哪一季
    check_quarter = None
    if month == 6 and day >= 15 or month == 7:
        check_quarter = f"{cur_roc}Q1"
    elif month == 9 and day >= 14 or month == 10:
        check_quarter = f"{cur_roc}Q2"
    elif month == 12 and day >= 14 or month == 1:
        yr = cur_roc if month == 12 else cur_roc - 1
        check_quarter = f"{yr}Q3"
    elif month == 4 and day >= 30 or month == 5:
        check_quarter = f"{cur_roc - 1}Q4"

    if not check_quarter:
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 檢查是否已經確認過（避免重複跑）
    c.execute("""SELECT COUNT(*) FROM cross_validation
                 WHERE details LIKE ? AND checked_at > datetime('now', '-7 days')""",
              (f'%quarterly_check_{check_quarter}%',))
    if c.fetchone()[0] > 0:
        conn.close()
        return

    # 找出有該季度資料的所有股票
    c.execute("SELECT code FROM stocks WHERE close IS NOT NULL ORDER BY code")
    codes = [r[0] for r in c.fetchall()]
    conn.close()

    print(f"[季報確認] 開始用群益確認 {check_quarter} 資料（{len(codes)} 支）...")

    from capital_fetcher import fetch_capital_financials
    import json

    mismatches = []
    updated = 0
    for i, code in enumerate(codes):
        try:
            fetch_capital_financials(code)
        except:
            pass
        if (i + 1) % 100 == 0:
            print(f"  進度：{i+1}/{len(codes)}")
            time.sleep(0.3)

    # 比對群益寫入的值 vs 原本 MOPS 的值（群益已覆蓋，所以這裡只統計有更新的筆數）
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""SELECT COUNT(*) FROM quarterly_financial
                 WHERE quarter=? AND eps IS NOT NULL""", (check_quarter,))
    have_data = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM stocks WHERE close IS NOT NULL")
    total = c.fetchone()[0]

    missing = total - have_data

    # 記錄確認結果
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS cross_validation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            checked_at TEXT, sample_size INTEGER, ok_count INTEGER, mismatch_count INTEGER,
            details TEXT)""")
        c.execute("""INSERT INTO cross_validation
            (checked_at, sample_size, ok_count, mismatch_count, details)
            VALUES (?,?,?,?,?)""",
            (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
             total, have_data, missing,
             json.dumps({'type': f'quarterly_check_{check_quarter}',
                        'have_data': have_data, 'missing': missing},
                       ensure_ascii=False)))
        conn.commit()
    except:
        pass
    conn.close()

    print(f"[季報確認] {check_quarter} 完成：{have_data}/{total} 支有資料，{missing} 支缺漏")


def _sync_contract_from_quarterly():
    """從 quarterly_financial 的合約負債同步到 stocks 表的 contract_1~3"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    codes = [r[0] for r in c.execute('SELECT code FROM stocks WHERE close IS NOT NULL ORDER BY code').fetchall()]

    updated = 0
    for code in codes:
        rows = c.execute('''SELECT quarter, contract_liability FROM quarterly_financial
                           WHERE code=? AND contract_liability IS NOT NULL
                           ORDER BY CAST(SUBSTR(quarter,1,INSTR(quarter,"Q")-1) AS INTEGER) DESC,
                                    CAST(SUBSTR(quarter,INSTR(quarter,"Q")+1) AS INTEGER) DESC
                           LIMIT 3''', (code,)).fetchall()
        if not rows:
            c.execute('UPDATE stocks SET contract_1=NULL, contract_1q=NULL, contract_2=NULL, contract_2q=NULL, contract_3=NULL, contract_3q=NULL WHERE code=?', (code,))
            continue
        for i, r in enumerate(rows, 1):
            c.execute(f'UPDATE stocks SET contract_{i}=?, contract_{i}q=? WHERE code=?',
                      (r[1], r[0], code))
        for i in range(len(rows) + 1, 4):
            c.execute(f'UPDATE stocks SET contract_{i}=NULL, contract_{i}q=NULL WHERE code=?', (code,))
        updated += 1

    conn.commit()
    conn.close()
    if updated:
        print(f"  [合約負債同步] 從 quarterly_financial 同步 {updated} 支到 stocks 表")


def _sync_eps_from_quarterly():
    """從 quarterly_financial 正確排序後回寫 stocks 表的 eps_1~eps_5 + eps_ytd"""
    conn = sqlite3.connect(DB_PATH)
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
    conn.close()
    if updated:
        print(f"  [EPS同步] 從 quarterly_financial 同步 {updated} 支到 stocks 表")


def _fill_dividends_from_bwibbu():
    """用 TWSE BWIBBU 殖利率反推 + TPEX 每股股利欄位，補齊 110-113 年股利"""
    import time as _time
    conn = sqlite3.connect(DB_PATH)
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
    conn.close()
    if updated: print(f"  股利BWIBBU補充: {updated} 支")


def _refresh_grades_from_pbr():
    """用 TWSE/TPEX 的 PBR/PE 反推 ROE，計算財務等級（不覆蓋精確值）"""
    import time as _time
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

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
    conn.close()
    if updated: print(f"  PBR/PE 財務等級: {updated} 支")

# ── 個股年度財報（即時抓取 + 快取）──────────────────────────

def init_monthly_revenue_db():
    conn = sqlite3.connect(DB_PATH)
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
    conn.close()


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
    except:
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
        except:
            pass

    if rows:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        for row in rows:
            c.execute("""
                INSERT OR REPLACE INTO monthly_revenue
                  (code, year, month, revenue, updated_at)
                VALUES (:code, :year, :month, :revenue, :updated_at)
            """, row)
        conn.commit()
        conn.close()

    return rows


def init_financial_db():
    conn = sqlite3.connect(DB_PATH)
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
    conn.close()


def fetch_company_financials(code):
    """
    個股年度財報更新：
    1. 群益證券全部資料（損益表+資產負債表+現金流量表+股利+月營收+合約負債）
    2. 群益資料不足時才用 Yahoo 補充（不用 FinMind，節省額度）
    雲端環境（Render）跳過群益爬蟲（海外IP可能被擋），靠排程更新。
    """
    is_cloud = os.environ.get('DATABASE_URL') is not None

    # 來源 1：群益全部（僅本機，Render 跳過）
    capital_ok = False
    if not is_cloud:
        try:
            from capital_fetcher import fetch_all_three
            a1, q1, a2, a3, a4, a5 = fetch_all_three(code)
            capital_ok = (a1 > 0 or a2 > 0 or a3 > 0)
        except:
            pass

    # 檢查群益是否已補齊關鍵欄位
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
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
        except:
            pass

    c.execute("SELECT * FROM financial_annual WHERE code=? ORDER BY year DESC LIMIT 6", (code,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
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
    with ThreadPoolExecutor(max_workers=3) as pool:
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
            except:
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
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""SELECT div_c1, div_s1, div_1_label, div_c2, div_s2, div_2_label,
                            div_c3, div_s3, div_3_label, div_c4, div_s4, div_4_label,
                            div_c5, div_s5, div_5_label, div_c6, div_s6, div_6_label
                     FROM stocks WHERE code = ?""", (code,))
        row = c.fetchone()
        conn.close()
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
                    except:
                        pass
    except:
        pass

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
        conn = sqlite3.connect(DB_PATH)
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
                  -- 損益表共用欄位：不覆蓋已有值（群益優先）
                  revenue = COALESCE(revenue, excluded.revenue),
                  cost = COALESCE(cost, excluded.cost),
                  gross_profit = COALESCE(gross_profit, excluded.gross_profit),
                  operating_income = COALESCE(operating_income, excluded.operating_income),
                  pretax_income = COALESCE(pretax_income, excluded.pretax_income),
                  net_income = COALESCE(net_income, excluded.net_income),
                  eps = COALESCE(eps, excluded.eps),
                  -- 群益有提供的欄位：不覆蓋已有值
                  operating_expense = COALESCE(operating_expense, excluded.operating_expense),
                  non_operating = COALESCE(non_operating, excluded.non_operating),
                  -- FinMind 獨有/補充欄位：群益年報無直接提供，優先用 FinMind
                  tax = COALESCE(excluded.tax, tax),
                  net_income_parent = COALESCE(excluded.net_income_parent, net_income_parent),
                  total_assets = COALESCE(excluded.total_assets, total_assets),
                  total_equity = COALESCE(excluded.total_equity, total_equity),
                  common_stock = COALESCE(excluded.common_stock, common_stock),
                  operating_cf = COALESCE(excluded.operating_cf, operating_cf),
                  capex = COALESCE(excluded.capex, capex),
                  cash_dividend = COALESCE(excluded.cash_dividend, cash_dividend),
                  stock_dividend = COALESCE(excluded.stock_dividend, stock_dividend),
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
        conn.close()

    return results[:5]  # 最多回傳 5 年


# ── 個股季度財務資料（即時抓取 + 快取）─────────────────────

def init_quarterly_db():
    conn = sqlite3.connect(DB_PATH)
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
    conn.commit()
    conn.close()


def fetch_company_quarterly(code):
    """
    個股季度財報更新：
    群益季報（損益表+合約負債）已在 fetch_company_financials 的 fetch_all_three 裡抓過，
    這裡只需要直接讀 DB。如果 DB 沒資料才補抓。
    """
    # 先檢查 DB 是否有資料
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""SELECT COUNT(*) as cnt FROM quarterly_financial
                 WHERE code=? AND updated_at > datetime('now', '-12 hours')""", (code,))
    has_recent = c.fetchone()['cnt'] > 0

    if not has_recent and not os.environ.get('DATABASE_URL'):
        # 本機才用群益補抓（Render 跳過）
        try:
            from capital_fetcher import fetch_capital_financials, fetch_capital_contract_liability
            fetch_capital_financials(code)
            fetch_capital_contract_liability(code)
        except:
            pass

    c.execute("""SELECT * FROM quarterly_financial WHERE code=?
                 ORDER BY CAST(SUBSTR(quarter, 1, INSTR(quarter, 'Q') - 1) AS INTEGER) DESC,
                          CAST(SUBSTR(quarter, INSTR(quarter, 'Q') + 1) AS INTEGER) DESC
                 LIMIT 8""", (code,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
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
    with ThreadPoolExecutor(max_workers=2) as pool:
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
            except:
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
        conn = sqlite3.connect(DB_PATH)
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
        conn.close()

    return results


# ── 歷史本益比（FinMind TaiwanStockPER）──────────────────

def init_pe_history_db():
    conn = sqlite3.connect(DB_PATH)
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
    conn.close()


def fetch_pe_history(code):
    """群益優先抓歷史本益比，FinMind 為 fallback"""
    # 來源 1：群益（免費無限制）
    try:
        from capital_fetcher import fetch_capital_pe_history
        n = fetch_capital_pe_history(code)
        if n > 0:
            return []  # 群益成功，不用 FinMind
    except:
        pass

    # 來源 2：FinMind（有額度限制）
    start_date = f"{date.today().year - 8}-01-01"
    url = (f"https://api.finmindtrade.com/api/v4/data"
           f"?dataset=TaiwanStockPER&data_id={code}&start_date={start_date}")
    try:
        r = _session.get(url, timeout=20)
        data = r.json()
        records = data.get('data', []) if data.get('status') == 200 else []
    except:
        records = []

    if not records:
        return []

    # 按年分組取最高最低
    from collections import defaultdict
    yearly = defaultdict(list)
    for rec in records:
        yr = int(rec['date'][:4])
        pe = rec.get('PER')
        if pe and pe > 0:
            yearly[yr].append(pe)

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cur_year = date.today().year
    results = []
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for yr in sorted(yearly.keys()):
        if yr == cur_year:
            continue  # 跳過當年度（不完整）
        vals = yearly[yr]
        row = {
            'code': code, 'year': yr,
            'pe_high': round(max(vals), 2),
            'pe_low': round(min(vals), 2),
            'updated_at': now_str,
        }
        results.append(row)
        c.execute("""
            INSERT OR REPLACE INTO pe_history (code, year, pe_high, pe_low, updated_at)
            VALUES (:code, :year, :pe_high, :pe_low, :updated_at)
        """, row)
    conn.commit()
    conn.close()
    return results


# ── 快速更新：批次營收 + EPS（政府 API，無限制）─────────────

def quick_update():
    """
    輕量更新：只用政府批次 API 更新營收 & EPS。
    每次僅 4 個 HTTP 請求，< 5 秒完成，適合高頻排程。
    """
    # 排程抖動：隨機延遲 0~30 秒（快速更新不用等太久）
    jitter = random.randint(0, 30)
    if jitter > 5:
        print(f"[排程抖動] 延遲 {jitter} 秒...")
        time.sleep(jitter)

    t0 = time.time()
    today_str = date.today().strftime('%Y-%m-%d')
    print(f"\n{'='*50}")
    print(f"快速更新  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")

    # 清理舊備份
    try: cleanup_old_backups(30)
    except: pass

    init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # ── 1. 批次營收（TWSE + TPEX）──
    # 使用政府 t187ap05 API 的官方數值（比 FinMind 自算更準確）
    rev_updated = 0
    rev_corrected = 0
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
            # 解析資料年月 "11503" → year=115+1911=2026, month=3
            ym_str = str(d.get('資料年月', '')).strip()
            if len(ym_str) < 4:
                continue
            try:
                roc_year = int(ym_str[:-2])
                month = int(ym_str[-2:])
            except:
                continue

            yoy = safe_float(d.get('營業收入-去年同月增減(%)'))
            mom = safe_float(d.get('營業收入-上月比較增減(%)'))
            cum_yoy = safe_float(d.get('累計營業收入-前期比較增減(%)'))
            note = str(d.get('備註', '')).strip()
            if note in ('-', '－', '無', ''): note = None

            west_year = roc_year + 1911

            # 檢查是否為新月份
            c.execute("SELECT revenue_year, revenue_month FROM stocks WHERE code = ?", (code,))
            row = c.fetchone()
            if not row:
                continue
            old_y, old_m = row[0], row[1]

            # 新月份 → 設 revenue_date 為今天
            # 同月份 → 用政府官方值覆蓋（確保數據正確），但不改 revenue_date
            if old_y == west_year and old_m == month:
                c.execute("""UPDATE stocks SET
                    revenue_yoy=?, revenue_mom=?, revenue_cum_yoy=?, revenue_note=?
                    WHERE code=?""", (yoy, mom, cum_yoy, note, code))
            else:
                c.execute("""UPDATE stocks SET
                    revenue_date=?, revenue_year=?, revenue_month=?,
                    revenue_yoy=?, revenue_mom=?, revenue_cum_yoy=?, revenue_note=?
                    WHERE code=?""",
                    (today_str, west_year, month, yoy, mom, cum_yoy, note, code))
                rev_updated += 1

    print(f"[營收] 更新 {rev_updated} 支")

    # ── 2. 批次 EPS（TWSE + TPEX）──
    # t187ap14 的 EPS 是「累計」值：
    #   Q1 累計=單季, Q2 累計=Q1+Q2, Q3 累計=Q1+Q2+Q3, Q4 累計=全年
    eps_updated = 0
    eps_y_updated = 0
    for label, url, code_key, eps_key, year_key, season_key in [
        ("上市",
         "https://openapi.twse.com.tw/v1/opendata/t187ap14_L",
         "公司代號", "基本每股盈餘(元)", "年度", "季別"),
        ("上櫃",
         "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O",
         "SecuritiesCompanyCode", "基本每股盈餘", "Year", "季別"),
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

            quarter_label = f"{year}Q{season}"

            # 檢查是否已有此季度
            c.execute("SELECT eps_1q, eps_y1_label FROM stocks WHERE code = ?", (code,))
            row = c.fetchone()
            if not row:
                continue
            old_q1 = row[0]
            old_y1_label = row[1]

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
                """, (eps, year, eps, year, today_str, code))
                eps_y_updated += 1
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
                        # 若找不到前季資料，single_eps 保持累計值（不完美但安全）

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
                """, (single_eps, quarter_label, today_str, eps, year, code))
                eps_updated += 1

    print(f"[EPS] 更新季度 {eps_updated} 支 + 年度 {eps_y_updated} 支")

    conn.commit()
    conn.close()

    # ── 3. 產業別（從營收 API 取得）──
    conn3 = sqlite3.connect(DB_PATH)
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
    conn3.close()

    # ── 4. 補齊 eps_ytd（用 eps_y1 填入）──
    conn2 = sqlite3.connect(DB_PATH)
    c2 = conn2.cursor()
    c2.execute("UPDATE stocks SET eps_ytd = eps_y1, eps_ytd_label = eps_y1_label WHERE eps_ytd IS NULL AND eps_y1 IS NOT NULL")
    if c2.rowcount:
        print(f"[EPS] 補齊 {c2.rowcount} 支的當年累計")
    conn2.commit()
    conn2.close()

    # ── 4. 財務體質等級自動重算（從現有 financial_annual 快取）──
    _refresh_fin_grades()

    # ── 4. 偵測新年報 → 自動刷新 financial_annual ──
    if eps_updated > 0 or eps_y_updated > 0:
        _refresh_stale_financials()

    elapsed = time.time() - t0
    _flush_health_log()
    snapshot_stock_states()
    try: fetch_material_news()
    except: pass
    try: fetch_moneydj_news()
    except: pass
    try: auto_archive_old_news()
    except: pass
    # 本機自動 push 新聞到 Render
    if not os.environ.get('DATABASE_URL'):
        try: _push_news_to_render()
        except: pass
    # ── 5. MOPS 最新季 EPS（比政府 API 快）──
    try: fetch_mops_quarterly_eps()
    except Exception as e: print(f"[MOPS] 失敗: {e}")

    # ── 6. 稅務修正（每次都跑）──
    _fix_tax_data()

    print(f"\n快速更新完成！營收 {rev_updated} + 季度EPS {eps_updated} + 年度EPS {eps_y_updated}，耗時 {elapsed:.1f} 秒")


def _push_news_to_render():
    """把本機今天的新聞 push 到 Render"""
    RENDER_URL = "https://tock-system.onrender.com"
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
            requests.post(f'{RENDER_URL}/api/sync/news', json={'rows': batch}, timeout=30)
        print(f"[新聞同步] 已 push {len(data)} 筆到 Render")
    except Exception as e:
        print(f"[新聞同步] 失敗: {e}")


def _calc_fin_grade(roe, operating_margin, fcf, revenue):
    """計算財務體質等級"""
    if roe is None:
        return None
    # FCF 無資料時預設中間值（0-5% 區間）
    if fcf is None or revenue is None or revenue == 0:
        fcf_r = 2.5
    else:
        fcf_r = fcf / revenue * 100
    if roe >= 15:
        base = 'B1A' if fcf_r < 0 else ('A1' if fcf_r < 5 else 'AA')
    elif roe >= 10:
        base = 'B1' if fcf_r < 0 else ('A' if fcf_r < 5 else 'A2')
    elif roe >= 7:
        base = 'C' if fcf_r < 0 else ('B2' if fcf_r < 5 else 'B2A')
    else:
        base = 'D' if fcf_r < 0 else 'C'
    suffix = ''
    if operating_margin is not None:
        if operating_margin >= 10: suffix = '+'
        elif operating_margin < 5: suffix = '-'
    return base + suffix


def _refresh_fin_grades():
    """從 financial_annual 快取重算所有公司的財務等級（純 DB 運算）"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 找所有有 financial_annual 資料的公司
    c.execute("SELECT DISTINCT code FROM financial_annual")
    codes = [r[0] for r in c.fetchall()]
    if not codes:
        conn.close()
        return

    updated = 0
    for code in codes:
        c.execute("""SELECT year, revenue, operating_income, net_income,
                            total_equity, operating_cf, capex
                     FROM financial_annual WHERE code = ?
                     ORDER BY year DESC LIMIT 6""", (code,))
        rows = c.fetchall()
        if not rows:
            continue

        updates = {}
        for i, row in enumerate(rows, 1):
            rev = row['revenue']
            oi = row['operating_income']
            ni = row['net_income']
            te = row['total_equity']
            ocf = row['operating_cf']
            capex = row['capex']

            roe = round(ni / te * 100, 2) if te and ni is not None else None
            opm = round(oi / rev * 100, 2) if rev and oi is not None else None
            fcf = round(ocf + capex, 2) if ocf is not None and capex is not None else None

            grade = _calc_fin_grade(roe, opm, fcf, rev)
            updates[f'fin_grade_{i}'] = grade
            updates[f'fin_grade_{i}y'] = str(row['year'] - 1911)

        for i in range(len(rows) + 1, 7):
            updates[f'fin_grade_{i}'] = None
            updates[f'fin_grade_{i}y'] = None

        set_clause = ', '.join(f'{k}=?' for k in updates.keys())
        c.execute(f'UPDATE stocks SET {set_clause} WHERE code=?',
                  list(updates.values()) + [code])
        updated += 1

    conn.commit()
    conn.close()
    if updated:
        print(f"[等級] 重算 {updated} 支公司的財務體質等級")


def _refresh_stale_financials():
    """偵測哪些公司有新的年度 EPS 但 financial_annual 資料過時，自動刷新"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 找 stocks 表中有 eps_y1_label 但 financial_annual 沒有對應年度的公司
    c.execute("""SELECT s.code, s.eps_y1_label
                 FROM stocks s
                 WHERE s.eps_y1_label IS NOT NULL
                 AND s.code NOT IN (
                     SELECT fa.code FROM financial_annual fa
                     WHERE fa.year = (CAST(s.eps_y1_label AS INTEGER) + 1911)
                 )""")
    stale = c.fetchall()
    conn.close()

    if not stale:
        return

    # 限制每次最多刷新 20 支（避免用光 FinMind 額度）
    stale = stale[:20]
    print(f"[財報] 偵測到 {len(stale)} 支有新年報待刷新")

    for code, year_label in stale:
        try:
            result = fetch_company_financials(code)
            if result:
                print(f"  {code} 財報已更新（{year_label}年）")
        except:
            pass


def _prefetch_watchlist_details():
    """
    觀察清單個股資料預抓取。
    三層來源：Yahoo Finance → 政府 API → FinMind。
    """
    from guardian import should_skip_finmind, track_finmind_call

    # ── 0. Yahoo Finance 補年度/季度財報（免費無限制）──
    print("[預抓取] Yahoo Finance 補齊財報...")
    try:
        from yahoo_fetcher import _get_yahoo_session, fetch_yahoo_financials, save_yahoo_to_db
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        yahoo_need = []
        c.execute("SELECT code, name, market FROM stocks WHERE close IS NOT NULL ORDER BY code")
        for r in c.fetchall():
            c.execute("SELECT COUNT(*) FROM financial_annual WHERE code=? AND net_income IS NOT NULL", (r[0],))
            if c.fetchone()[0] < 3:
                yahoo_need.append((r[0], r[1], r[2]))
        conn.close()

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
                    except: break
                    y_fail = 0
                time.sleep(random.uniform(0.1, 0.3))
            print(f"  Yahoo 補齊 {y_done} 支")
        else:
            print("  Yahoo：全部已有 3 年以上財報")
    except Exception as e:
        print(f"  Yahoo 失敗：{e}")

    # ── 1. 月營收歷史：從政府 t187ap05 存原始金額到 monthly_revenue ──
    print("[預抓取] 儲存月營收歷史（政府API，無限制）...")
    conn = sqlite3.connect(DB_PATH)
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
            except:
                continue
            try:
                c.execute("""INSERT OR IGNORE INTO monthly_revenue
                    (code, year, month, revenue, updated_at)
                    VALUES (?,?,?,?,?)""",
                    (code, west_year, month, revenue, now_str))
                if c.rowcount:
                    rev_saved += 1
            except:
                pass

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
            except:
                continue
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
            except:
                pass

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
            except:
                continue

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
            except:
                pass

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
        # 從 stock_state 歷史估算（簡易版：用最近的 shen_pe 做紀錄）
        # 真正的 PE 歷史需要 FinMind，這裡先跳過

    conn.commit()
    conn.close()

    # ── 4. FinMind 補充（有額度才跑）──
    if should_skip_finmind():
        print("[預抓取] FinMind 額度不足，跳過個股補充")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT DISTINCT stock_id FROM stock_state")
    tracked = [r[0] for r in c.fetchall()]

    need_fetch = []
    now = datetime.now()
    for code in tracked:
        c.execute("SELECT updated_at FROM quarterly_financial WHERE code=? ORDER BY updated_at DESC LIMIT 1", (code,))
        r = c.fetchone()
        if not r or not r['updated_at']:
            need_fetch.append(code)
            continue
        try:
            updated = datetime.strptime(r['updated_at'], '%Y-%m-%d %H:%M:%S')
            if (now - updated).days >= 7:
                need_fetch.append(code)
        except:
            need_fetch.append(code)

    conn.close()

    if not need_fetch:
        print("[預抓取] FinMind 補充：觀察清單都是最新的")
        return

    batch = need_fetch[:15]
    print(f"[預抓取] FinMind 補充 {len(batch)} 支（季度財報+PE歷史）")

    done = 0
    for code in batch:
        if should_skip_finmind():
            break
        try:
            r3 = fetch_company_quarterly(code)
            track_finmind_call(2)
            r4 = fetch_pe_history(code)
            track_finmind_call(1)
            if r3 or r4:
                print(f"  {code}: 季度{len(r3) if r3 else 0}季, PE{len(r4) if r4 else 0}年")
            done += 1
            time.sleep(random.uniform(0.3, 1.0))
        except:
            pass

    print(f"[預抓取] FinMind 補充完成 {done}/{len(batch)} 支")


def _parse_inst_val(v):
    v = v.replace(',', '').replace('--', '').strip()
    if not v:
        return None
    try:
        return int(v)
    except:
        return None


def _fetch_inst_one(code):
    try:
        url = f"https://stock.capital.com.tw/z/zc/zcl/zcl_{code}.djhtm"
        s = requests.Session()
        s.headers.update({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'})
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
    except:
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
    conn = sqlite3.connect(DB_PATH)
    codes = [r[0] for r in conn.execute("SELECT code FROM stocks ORDER BY code").fetchall()]
    conn.close()
    print(f"[法人] 開始抓取 {len(codes)} 支股票的三大法人買賣超...")

    results = []
    with ThreadPoolExecutor(max_workers=8) as pool:
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

    conn = sqlite3.connect(DB_PATH)
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
    conn.close()
    msg = f"[法人] 完成：更新 {updated}/{len(codes)} 支，耗時 {time.time()-t0:.1f}s"
    if skipped_date:
        msg += f"，{skipped_date} 支日期不符跳過"
    print(msg)
    return updated


def _push_annual_to_render():
    """本機年度 EPS + 股利 + 財務等級 push 到 Render"""
    if os.environ.get('DATABASE_URL'):
        return  # Render 不 push 自己
    RENDER_URL = "https://tock-system.onrender.com"
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("""SELECT code,
            eps_y1, eps_y1_label, eps_y2, eps_y2_label, eps_y3, eps_y3_label,
            eps_y4, eps_y4_label, eps_y5, eps_y5_label, eps_y6, eps_y6_label,
            div_c1, div_s1, div_1_label, div_c2, div_s2, div_2_label,
            div_c3, div_s3, div_3_label, div_c4, div_s4, div_4_label,
            div_c5, div_s5, div_5_label, div_c6, div_s6, div_6_label,
            fin_grade_1, fin_grade_1y, fin_grade_2, fin_grade_2y,
            fin_grade_3, fin_grade_3y, fin_grade_4, fin_grade_4y,
            fin_grade_5, fin_grade_5y, fin_grade_6, fin_grade_6y
            FROM stocks WHERE close IS NOT NULL""").fetchall()
        conn.close()

        cols = ['code',
            'eps_y1','eps_y1_label','eps_y2','eps_y2_label','eps_y3','eps_y3_label',
            'eps_y4','eps_y4_label','eps_y5','eps_y5_label','eps_y6','eps_y6_label',
            'div_c1','div_s1','div_1_label','div_c2','div_s2','div_2_label',
            'div_c3','div_s3','div_3_label','div_c4','div_s4','div_4_label',
            'div_c5','div_s5','div_5_label','div_c6','div_s6','div_6_label',
            'fin_grade_1','fin_grade_1y','fin_grade_2','fin_grade_2y',
            'fin_grade_3','fin_grade_3y','fin_grade_4','fin_grade_4y',
            'fin_grade_5','fin_grade_5y','fin_grade_6','fin_grade_6y']
        data = [{cols[j]: r[j] for j in range(len(cols))} for r in rows]

        for i in range(0, len(data), 200):
            batch = data[i:i+200]
            requests.post(f'{RENDER_URL}/api/sync/annual',
                         json={'data': batch}, timeout=30)
        print(f"[年度同步] 已 push {len(data)} 支到 Render")
    except Exception as e:
        print(f"[年度同步] 失敗: {e}")


def _push_institutional_to_render():
    """本機法人資料 push 到 Render PostgreSQL"""
    RENDER_URL = "https://tock-system.onrender.com"
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT code, inst_foreign, inst_trust, inst_dealer FROM stocks WHERE inst_foreign IS NOT NULL").fetchall()
        conn.close()
        data = [{'code': r[0], 'f': r[1], 't': r[2], 'd': r[3]} for r in rows]
        for i in range(0, len(data), 500):
            batch = data[i:i+500]
            requests.post(f'{RENDER_URL}/api/refresh/institutional',
                         json={'data': batch}, timeout=30)
        print(f"[法人同步] 已 push {len(data)} 支到 Render")
    except Exception as e:
        print(f"[法人同步] 失敗: {e}")


def refresh_prices():
    """
    只更新股價。
    盤中（週一~五 09:00~13:35）：用 TWSE/TPEX 即時 API
    盤後同日（13:36~16:00）：優先用即時 API（已有當日收盤價），失敗才 fallback 批次 API
    其餘時段（隔日/假日）：用批次收盤 API
    """
    t0 = time.time()
    init_db()

    now = datetime.now()
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

    # 先用 TWSE 批次 API 嘗試，順便取得資料日期
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

    conn = sqlite3.connect(DB_PATH)
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
    conn.close()

    elapsed = time.time() - t0
    print(f"[股價更新-批次] {count} 支，耗時 {elapsed:.1f} 秒")
    return count


def _refresh_realtime():
    """盤中即時報價更新（證交所 MIS API：mis.twse.com.tw）
    上市用 tse_{code}.tw、上櫃用 otc_{code}.tw，同一個 API 統一處理
    取價優先：z（成交價）> b（買價第一檔）> 跳過"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT code, market FROM stocks WHERE close IS NOT NULL")
    all_stocks = [(r[0], r[1]) for r in c.fetchall()]

    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    count = 0

    # 每批 50 檔
    for i in range(0, len(all_stocks), 50):
        batch = all_stocks[i:i+50]
        ex_codes = []
        for code, market in batch:
            prefix = 'tse' if market == '上市' else 'otc'
            ex_codes.append(f"{prefix}_{code}.tw")

        try:
            url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={'|'.join(ex_codes)}"
            r = _session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            data = r.json()
            for s in data.get("msgArray", []):
                code = s.get("c")
                # 取價：成交 > 買價 > 昨收
                price = s.get("z")
                if price == "-" or not price:
                    bid = s.get("b", "")
                    if bid and "_" in bid:
                        price = bid.split("_")[0]
                if price == "-" or not price:
                    continue  # 完全沒有價格就跳過

                try:
                    close = float(price)
                    if close <= 0:
                        continue  # 無效價格不寫入
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
                except:
                    pass
        except:
            pass

    conn.commit()
    conn.close()
    return count


def _batch_system_estimate():
    """批次更新所有股票的系統 EPS 估算（取第一季結果存入 stocks 表）"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    for col, typ in [('sys_est_eps','REAL'),('sys_est_quarter','TEXT'),('sys_est_confidence','TEXT')]:
        try: conn.execute(f"ALTER TABLE stocks ADD COLUMN {col} {typ}")
        except: pass
    try: conn.commit()
    except: pass
    rows = conn.execute("SELECT code, industry FROM stocks ORDER BY code").fetchall()
    conn.close()

    success = 0
    for r in rows:
        code = r['code']
        if (r['industry'] or '') in _EXCLUDED_INDUSTRIES:
            continue
        try:
            result = estimate_system_eps(code)
            if result.get('est_eps') is not None and 'error' not in result:
                c2 = sqlite3.connect(DB_PATH)
                c2.execute(
                    "UPDATE stocks SET sys_est_eps=?, sys_est_quarter=?, sys_est_confidence=? WHERE code=?",
                    (result['est_eps'], result['quarter'], result['confidence'], code)
                )
                c2.commit()
                c2.close()
                success += 1
        except:
            pass
    print(f"  系統 EPS 估算：{success}/{len(rows)} 支完成")


# 不適用系統估算的產業（損益結構與一般製造/服務業不同）
_EXCLUDED_INDUSTRIES = {'金融保險業', '金融業', '保險業', '銀行業', '證券業', '票券業', '期貨業', '建材營造'}


def _batch_annual_estimate():
    """批次更新所有股票的年度 EPS 估算"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    for col, typ in [('sys_ann_eps','REAL'),('sys_ann_div','REAL'),('sys_ann_pe','REAL'),
                     ('sys_ann_yld','REAL'),('sys_ann_confidence','TEXT')]:
        try: conn.execute(f"ALTER TABLE stocks ADD COLUMN {col} {typ}")
        except: pass
    try: conn.commit()
    except: pass
    rows = conn.execute("SELECT code, industry FROM stocks ORDER BY code").fetchall()
    conn.close()

    success = skip = 0
    for r in rows:
        code = r['code']
        ind = r['industry'] or ''
        # 不適用產業標記 N/A
        if ind in _EXCLUDED_INDUSTRIES:
            try:
                c2 = sqlite3.connect(DB_PATH)
                c2.execute("UPDATE stocks SET sys_ann_confidence='N/A' WHERE code=?", (code,))
                c2.commit(); c2.close()
            except: pass
            skip += 1
            continue
        try:
            ar = estimate_annual_eps(code)
            if ar.get('est_eps') is not None and 'error' not in ar:
                d = ar['details']
                _log_estimate(code, ar, 'annual')
                c2 = sqlite3.connect(DB_PATH)
                c2.execute("""UPDATE stocks SET sys_ann_eps=?, sys_ann_div=?, sys_ann_pe=?,
                              sys_ann_yld=?, sys_ann_confidence=? WHERE code=?""",
                           (ar['est_eps'], d.get('est_div'), d.get('est_pe'),
                            d.get('est_yld'), ar['confidence'], code))
                c2.commit(); c2.close()
                success += 1
        except:
            pass
    print(f"  年度 EPS 估算：{success} 支完成，{skip} 支產業排除")

    # 估算結果 push 到 Render
    if not os.environ.get('DATABASE_URL'):
        _push_estimates_to_render()

    # 自動回填實際 EPS（回測用）
    _backfill_actual_eps()


def _push_estimates_to_render():
    """將本機估算結果 push 到 Render"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""SELECT code, sys_ann_eps, sys_ann_div, sys_ann_pe,
                               sys_ann_yld, sys_ann_confidence
                               FROM stocks WHERE sys_ann_eps IS NOT NULL""").fetchall()
        conn.close()
        data = [dict(r) for r in rows]
        if not data:
            return
        import requests
        resp = requests.post(
            "https://tock-system.onrender.com/api/sync/estimates",
            json={"data": data}, timeout=30
        )
        if resp.status_code == 200:
            print(f"  估算 push 到 Render：{len(data)} 支")
        else:
            print(f"  估算 push 失敗：{resp.status_code}")
    except Exception as e:
        print(f"  估算 push 失敗：{e}")


def _backfill_actual_eps():
    """自動回填實際 EPS 到 system_eps_actual（回測用）"""
    try:
        _init_eps_log_db()
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        roc_year = datetime.now().year - 1911

        # 回填已公布的季度 EPS
        quarters = conn.execute("""
            SELECT DISTINCT code, quarter, eps, revenue, updated_at
            FROM quarterly_financial
            WHERE eps IS NOT NULL
            AND CAST(SUBSTR(quarter, 1, INSTR(quarter, 'Q') - 1) AS INTEGER) >= ?
        """, (roc_year - 1,)).fetchall()

        filled = 0
        for q in quarters:
            try:
                conn.execute("""INSERT OR IGNORE INTO system_eps_actual
                    (code, target_period, actual_revenue, actual_eps, report_date)
                    VALUES (?, ?, ?, ?, ?)""",
                    (q['code'], q['quarter'], q['revenue'], q['eps'], q['updated_at']))
                filled += conn.total_changes
            except:
                pass

        # 回填已公布的年度 EPS
        annuals = conn.execute("""
            SELECT code, year, revenue, eps, updated_at
            FROM financial_annual
            WHERE eps IS NOT NULL AND year >= ?
        """, (datetime.now().year - 2,)).fetchall()

        for a in annuals:
            try:
                period = f"{a['year'] - 1911}年度"
                conn.execute("""INSERT OR IGNORE INTO system_eps_actual
                    (code, target_period, actual_revenue, actual_eps, report_date)
                    VALUES (?, ?, ?, ?, ?)""",
                    (a['code'], period, a['revenue'], a['eps'], a['updated_at']))
            except:
                pass

        conn.commit()
        conn.close()
    except:
        pass


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

    now = datetime.now()
    roc_year = now.year - 1911
    month = now.month

    # 判斷要抓哪些季度
    seasons_to_try = []
    if month >= 4:
        seasons_to_try.append((roc_year, 1))       # Q1（4~5月公布）
    if month >= 8:
        seasons_to_try.append((roc_year, 2))       # Q2（8~9月公布）
    if month >= 11:
        seasons_to_try.append((roc_year, 3))       # Q3（11月公布）
    if month <= 4:
        seasons_to_try.append((roc_year - 1, 4))   # 去年Q4（3~4月公布）

    total_updated = 0
    for yr, sn in seasons_to_try:
        quarter_key = f"{yr}Q{sn}"
        for typek, label in [('sii', '上市'), ('otc', '上櫃')]:
            try:
                resp = requests.post(
                    'https://mopsov.twse.com.tw/mops/web/ajax_t163sb19',
                    data={'encodeURIComponent': '1', 'step': '1', 'firstin': '1',
                          'off': '1', 'TYPEK': typek, 'year': str(yr), 'season': str(sn)},
                    headers={'Content-Type': 'application/x-www-form-urlencoded'},
                    timeout=15
                )
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, 'html.parser')

                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                count = 0
                now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                def parse_k(s):
                    try: return float(s.replace(',', '')) * 1000
                    except: return None

                for table in soup.find_all('table'):
                    for row in table.find_all('tr'):
                        cells = [td.get_text(strip=True) for td in row.find_all('td')]
                        if len(cells) < 9 or not cells[0].isdigit() or len(cells[0]) != 4:
                            continue
                        code = cells[0]
                        try:
                            cum_eps = float(cells[3].replace(',', ''))
                        except:
                            continue
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
                                        eps_ytd=?, eps_ytd_label=?, eps_date=?
                                        WHERE code=?""",
                                        (cum_eps, str(yr), cum_eps, str(yr),
                                         date.today().strftime('%Y-%m-%d'), code))
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
                                eps_1=?, eps_1q=?, eps_date=?
                                WHERE code=?""",
                                (single_eps, quarter_key,
                                 date.today().strftime('%Y-%m-%d'), code))
                            count += 1
                        elif cur and cur[0] == quarter_key:
                            c.execute("UPDATE stocks SET eps_1=? WHERE code=?",
                                      (single_eps, code))

                conn.commit()
                conn.close()
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
    conn = sqlite3.connect(DB_PATH)
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

    conn.commit()
    conn.close()
    if issues:
        print(f"  [交叉驗證] 發現 {issues} 筆差異")
    else:
        print(f"  [交叉驗證] 全部一致")
    return issues


def _fix_tax_data():
    """修正 DB 中 tax=0 的異常資料（本機+Render 通用）"""
    conn = sqlite3.connect(DB_PATH)
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
    conn.close()
    if fixed > 0:
        print(f"  稅務資料修正：{fixed} 筆")


# ── 系統 EPS 估算核心 ─────────────────────────────────────────
def _get_est_common_data(code):
    """取得估算所需的共用資料（季度財報 + 月營收 + 年度財報）"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    hist = [dict(r) for r in conn.execute("""
        SELECT * FROM quarterly_financial WHERE code = ?
        ORDER BY CAST(SUBSTR(quarter, 1, INSTR(quarter, 'Q') - 1) AS INTEGER) DESC,
                 CAST(SUBSTR(quarter, INSTR(quarter, 'Q') + 1) AS INTEGER) DESC
        LIMIT 16
    """, (code,)).fetchall()]
    rev_rows = conn.execute("""
        SELECT year, month, revenue FROM monthly_revenue
        WHERE code = ? ORDER BY year DESC, month DESC LIMIT 48
    """, (code,)).fetchall()
    rev_map = {(r['year'], r['month']): r['revenue'] for r in rev_rows}
    ann_rows = [dict(r) for r in conn.execute("""
        SELECT * FROM financial_annual WHERE code = ?
        ORDER BY year DESC LIMIT 5
    """, (code,)).fetchall()]
    conn.close()
    return hist, rev_map, rev_rows, ann_rows


def _estimate_quarter_core(hist, rev_map, rev_rows, roc_year, q_num, west_year, ann_rows=None):
    """
    估算單一季度 EPS 的核心邏輯。
    回傳 dict（含 quarter, est_eps, confidence, details）或含 error 的 dict。
    """
    import statistics

    Q_MONTHS = {1: [1,2,3], 2: [4,5,6], 3: [7,8,9], 4: [10,11,12]}
    months = Q_MONTHS[q_num]
    quarter_key = f"{roc_year}Q{q_num}"

    # --- Step 1: 月營收 ---
    est_rev_total = 0
    rev_actual = 0
    rev_estimated = 0
    monthly_revs = []
    for m in months:
        actual = rev_map.get((west_year, m))
        if actual:
            est_rev_total += actual
            rev_actual += 1
            monthly_revs.append({"month": m, "revenue": round(actual), "is_actual": True})
        else:
            prev = rev_map.get((west_year - 1, m))
            est_m = 0
            if prev:
                growths = []
                for rm in rev_rows:
                    ry, rmm = rm['year'], rm['month']
                    prev_rev = rev_map.get((ry - 1, rmm))
                    if prev_rev and prev_rev > 0:
                        growths.append(rm['revenue'] / prev_rev)
                        if len(growths) >= 12:
                            break
                avg_growth = statistics.mean(growths) if growths else 1.0
                est_m = prev * avg_growth
            elif hist and hist[0].get('revenue'):
                est_m = hist[0]['revenue'] / 3
            est_rev_total += est_m
            rev_estimated += 1
            monthly_revs.append({"month": m, "revenue": round(est_m), "is_actual": False})

    if est_rev_total <= 0:
        return {"error": "無營收資料"}

    # --- Step 2: 毛利率（年度×0.7 + 季度×0.3 錨定） ---
    recent = [r for r in hist[:4] if r.get('revenue') and r['revenue'] > 0
              and r.get('gross_profit') is not None]

    gm_pool = [r['gross_profit'] / r['revenue'] for r in recent]

    if not gm_pool:
        return {"error": "無毛利率資料"}

    if len(gm_pool) >= 2:
        weights = [0.4, 0.3, 0.2, 0.1][:len(gm_pool)]
        wsum = sum(weights)
        gm_q = sum(gm_pool[i] * weights[i] for i in range(len(weights))) / wsum
        # 年度錨定
        ann_gm = None
        if ann_rows:
            for ar in ann_rows:
                if ar.get('revenue') and ar['revenue'] > 0 and ar.get('gross_profit') is not None:
                    ann_gm = ar['gross_profit'] / ar['revenue']
                    break
        est_gm = gm_q * 0.3 + ann_gm * 0.7 if ann_gm is not None else gm_q
    else:
        est_gm = gm_pool[0]

    est_gross_profit = est_rev_total * est_gm

    # --- Step 3: 營業費用（年度×0.7 + 季度×0.3 錨定） ---
    opex_data = [(r['operating_expense'], r['revenue'])
                 for r in hist[:8]
                 if r.get('operating_expense') is not None
                 and r.get('revenue') and r['revenue'] > 0]

    if len(opex_data) >= 2:
        # 季度費用率（近4季加權）
        q_opex_rates = [opex_data[i][0] / opex_data[i][1] for i in range(min(4, len(opex_data)))]
        w = [0.4, 0.3, 0.2, 0.1][:len(q_opex_rates)]
        q_rate = sum(q_opex_rates[i] * w[i] for i in range(len(w))) / sum(w)

        # 年度費用率錨定
        ann_opex_rate = None
        if ann_rows:
            for ar in ann_rows:
                if ar.get('operating_expense') and ar.get('revenue') and ar['revenue'] > 0:
                    ann_opex_rate = ar['operating_expense'] / ar['revenue']
                    break
        if ann_opex_rate is not None:
            est_opex_rate = ann_opex_rate * 0.7 + q_rate * 0.3
        else:
            est_opex_rate = q_rate
        est_opex = est_opex_rate * est_rev_total
    elif opex_data:
        est_opex = opex_data[0][0]
    else:
        est_opex = est_gross_profit * 0.3

    est_oi = est_gross_profit - est_opex

    # --- Step 4: 業外（年度÷4 × 0.7 + 季度加權 × 0.3） ---
    nonop_list = [r['non_operating'] for r in hist[:8] if r.get('non_operating') is not None]
    nonop_stable = True
    nonop_level = 'stable'
    if nonop_list:
        # 季度加權
        recent = nonop_list[:4]
        weights = [0.4, 0.3, 0.2, 0.1][:len(recent)]
        wsum = sum(weights)
        q_nonop = sum(recent[i] * weights[i] for i in range(len(weights))) / wsum

        # 年度錨定（年度業外 / 4 = 季度基準）
        ann_nonop_q = None
        if ann_rows:
            for ar in ann_rows:
                if ar.get('non_operating') is not None:
                    ann_nonop_q = ar['non_operating'] / 4
                    break
        if ann_nonop_q is not None:
            est_nonop = ann_nonop_q * 0.7 + q_nonop * 0.3
        else:
            est_nonop = q_nonop

        # 穩定度判斷（用於信心等級）
        if len(nonop_list) >= 3:
            avg = statistics.mean(nonop_list)
            sd = statistics.stdev(nonop_list)
            cv = sd / abs(avg) if abs(avg) > 0 else 0
            if cv >= 1.0:
                nonop_level = 'volatile'
                nonop_stable = False
            elif cv >= 0.5:
                nonop_level = 'moderate'
    else:
        est_nonop = 0

    est_pti = est_oi + est_nonop

    # --- Step 5: 稅率（年度×0.7 + 季度累計×0.3，限 5~40%） ---
    tax_sum = pti_sum = 0
    for r in hist[:4]:
        pti = r.get('pretax_income')
        if pti and pti > 0:
            nip_r = r.get('net_income_parent')
            tax = r.get('tax')
            # 反算：tax=NULL 或 tax=0 但 pti>nip
            if nip_r is not None:
                calc_tax = pti - nip_r
                if tax is None or (tax == 0 and abs(calc_tax) > 100):
                    tax = calc_tax
                # pti==nip 異常
                if abs(pti - nip_r) < 1 and pti > 1000000:
                    tax = pti * 0.20
            if tax is not None:
                pti_sum += pti; tax_sum += tax
    q_tax_rate = tax_sum / pti_sum if pti_sum > 0 else 0.20
    # 年度稅率錨定
    ann_tax_rate = None
    if ann_rows:
        for ar in ann_rows:
            ar_pti = ar.get('pretax_income')
            ar_ni = ar.get('net_income') or ar.get('net_income_parent')
            ar_tax = ar.get('tax')
            # 反算年度稅
            if ar_pti and ar_ni:
                calc = ar_pti - ar_ni
                if ar_tax is None or (ar_tax == 0 and abs(calc) > 100):
                    ar_tax = calc
                if abs(ar_pti - ar_ni) < 1 and ar_pti > 1000000:
                    ar_tax = ar_pti * 0.20
            if ar_pti and ar_pti > 0 and ar_tax:
                ann_tax_rate = ar_tax / ar_pti
                break
    if ann_tax_rate is not None:
        est_tax_rate = max(0.05, min(ann_tax_rate * 0.7 + q_tax_rate * 0.3, 0.40))
    else:
        est_tax_rate = max(0.05, min(q_tax_rate, 0.40))
    est_ni = est_pti * (1 - est_tax_rate) if est_pti > 0 else est_pti * 0.80

    # --- Step 6: 歸屬母公司權重 ---
    pw_list = []
    for r in hist[:4]:
        ci = r.get('continuing_income') or r.get('net_income_parent')
        nip = r.get('net_income_parent')
        if ci and ci != 0 and nip is not None:
            pw = nip / ci
            if 0.3 <= pw <= 1.1:
                pw_list.append(pw)
    est_pw = statistics.mean(pw_list) if pw_list else 1.0
    est_nip = est_ni * est_pw

    # --- Step 7: 股數 ---
    est_shares = None
    for r in hist[:4]:
        if r.get('eps') and r['eps'] != 0 and r.get('net_income_parent') is not None:
            est_shares = r['net_income_parent'] / r['eps']
            break
    if not est_shares:
        return {"error": "無法計算股數"}

    est_eps = round(est_nip / est_shares, 2)

    # EPS-本業 = 營業利益 × (1-稅率) / 股數
    est_eps_core = round(est_oi * (1 - est_tax_rate) / est_shares, 2) if est_shares else None
    # EPS-業外 = EPS - EPS本業
    est_eps_nonop = round(est_eps - est_eps_core, 2) if est_eps_core is not None else None

    # --- Step 8: 信心等級 ---
    issues = []
    if rev_estimated >= 2: issues.append("營收多數為預估")
    if nonop_level == 'volatile': issues.append("業外高波動")
    elif nonop_level == 'moderate': issues.append("業外中波動")
    if len(gm_pool) < 2: issues.append("毛利率資料不足")
    if len(opex_data) < 4: issues.append("費用資料較少")
    confidence = "A" if not issues else ("B" if len(issues) == 1 else "C")

    # 同期比較
    yoy_q = f"{roc_year - 1}Q{q_num}"
    yoy_eps = next((r.get('eps') for r in hist if r['quarter'] == yoy_q), None)

    return {
        "quarter": quarter_key,
        "est_eps": est_eps,
        "confidence": confidence,
        "details": {
            "est_revenue": round(est_rev_total),
            "est_gm": round(est_gm * 100, 2),
            "est_gross_profit": round(est_gross_profit),
            "est_opex": round(est_opex),
            "est_oi": round(est_oi),
            "est_nonop": round(est_nonop),
            "est_pti": round(est_pti),
            "est_tax_rate": round(est_tax_rate * 100, 2),
            "est_ni": round(est_ni),
            "est_pw": round(est_pw * 100, 2),
            "est_nip": round(est_nip),
            "est_shares": round(est_shares),
            "est_eps_core": est_eps_core,
            "est_eps_nonop": est_eps_nonop,
            "rev_actual": rev_actual,
            "rev_estimated": rev_estimated,
            "yoy_eps": yoy_eps,
            "issues": issues,
            "monthly_revs": monthly_revs,
        }
    }


def estimate_system_eps(code):
    """單季估算（向下相容，用於批次 stocks 表更新）"""
    hist, rev_map, rev_rows, ann_rows = _get_est_common_data(code)
    if len(hist) < 4:
        return {"error": "季度資料不足（需至少 4 季）", "confidence": "N/A"}

    latest = hist[0]['quarter']
    ly, lq = int(latest.split('Q')[0]), int(latest.split('Q')[1])
    ny, nq = (ly + 1, 1) if lq == 4 else (ly, lq + 1)
    return _estimate_quarter_core(hist, rev_map, rev_rows, ny, nq, ny + 1911, ann_rows)


def estimate_system_eps_multi(code):
    """
    多季估算：根據當年度可用月營收數量，動態產生 1~4 欄預估。
    - 有 1~3 月 → Q1
    - 有 4~6 月 → Q1 + Q2
    - 有 7~9 月 → Q1 + Q2 + Q3
    - 有 10~12 月 → Q1 + Q2 + Q3 + Q4
    - 實際季報公布 15 天後 → 移除該欄
    """
    hist, rev_map, rev_rows, ann_rows = _get_est_common_data(code)
    if len(hist) < 4:
        return {"quarters": [], "error": "季度資料不足"}

    now = datetime.now()
    west_year = now.year
    roc_year = west_year - 1911

    available_months = sum(1 for m in range(1, 13) if rev_map.get((west_year, m)))

    estimates = []
    for qn in range(1, 5):
        # 該季需要的最小月份（Q1→月1, Q2→月4, Q3→月7, Q4→月10）
        min_month_needed = (qn - 1) * 3 + 1
        if available_months < min_month_needed:
            break  # 營收月份不夠，不估這季

        quarter_key = f"{roc_year}Q{qn}"

        # 檢查實際季報是否已公布（且超過 15 天）
        actual_q = next((r for r in hist if r['quarter'] == quarter_key), None)
        if actual_q and actual_q.get('updated_at'):
            try:
                upd = datetime.strptime(actual_q['updated_at'], '%Y-%m-%d %H:%M:%S')
                days_since = (now - upd).days
                if days_since > 15:
                    continue  # 已公布超過 15 天，跳過
            except:
                pass

        result = _estimate_quarter_core(hist, rev_map, rev_rows, roc_year, qn, west_year, ann_rows)
        if 'error' not in result:
            # 附加實際 vs 預估對照
            if actual_q and actual_q.get('eps') is not None:
                result['has_actual'] = True
                result['actual_eps'] = actual_q.get('eps')
            else:
                result['has_actual'] = False
            estimates.append(result)

    return {"quarters": estimates}


def estimate_annual_eps(code):
    """
    年度 EPS 估算：
    營收：月份佔比法（主）+ YoY衰減法（輔）取保守值
    費用：年度財報回歸（固定+變動）
    業外：年度財報三級制
    稅率/歸屬/股數：季度資料
    """
    import statistics

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # 月營收
    rev_rows = conn.execute("""
        SELECT year, month, revenue FROM monthly_revenue
        WHERE code = ? ORDER BY year DESC, month DESC LIMIT 60
    """, (code,)).fetchall()
    rev_map = {(r['year'], r['month']): r['revenue'] for r in rev_rows}

    # 年度財報
    ann_rows = [dict(r) for r in conn.execute("""
        SELECT * FROM financial_annual WHERE code = ?
        ORDER BY year DESC LIMIT 5
    """, (code,)).fetchall()]

    # 季度財報（稅率/歸屬/股數用）
    q_rows = [dict(r) for r in conn.execute("""
        SELECT * FROM quarterly_financial WHERE code = ?
        ORDER BY CAST(SUBSTR(quarter, 1, INSTR(quarter, 'Q') - 1) AS INTEGER) DESC,
                 CAST(SUBSTR(quarter, INSTR(quarter, 'Q') + 1) AS INTEGER) DESC
        LIMIT 8
    """, (code,)).fetchall()]
    conn.close()

    if len(ann_rows) < 2:
        return {"error": "年度財報不足（需至少 2 年）"}

    west_year = datetime.now().year
    roc_year = west_year - 1911

    # === Step 1: 年度營收預估 ===
    # 當年度已公布月份
    ytd_months = []
    for m in range(1, 13):
        rev = rev_map.get((west_year, m))
        if rev:
            ytd_months.append((m, rev))
        else:
            break
    n_months = len(ytd_months)
    if n_months == 0:
        return {"error": "當年度無月營收資料"}

    ytd_revenue = sum(r for _, r in ytd_months)

    # --- 方法A：月份佔比法（近3年每月佔全年比重） ---
    weight_years = []
    for y in range(west_year - 1, west_year - 4, -1):
        year_total = sum(rev_map.get((y, m), 0) for m in range(1, 13))
        if year_total > 0:
            ytd_weight = sum(rev_map.get((y, m), 0) for m in range(1, n_months + 1)) / year_total
            if ytd_weight > 0:
                weight_years.append(ytd_weight)

    if weight_years:
        avg_weight = statistics.mean(weight_years)
        rev_method_a = ytd_revenue / avg_weight
    else:
        rev_method_a = ytd_revenue / n_months * 12  # fallback

    # --- 方法B：YoY 衰減 + 均值回歸 ---
    prev_ytd = sum(rev_map.get((west_year - 1, m), 0) for m in range(1, n_months + 1))
    prev_annual = sum(rev_map.get((west_year - 1, m), 0) for m in range(1, 13))

    if prev_ytd > 0 and prev_annual > 0:
        ytd_growth = ytd_revenue / prev_ytd - 1

        # 過去 3 年平均年成長率
        hist_growths = []
        for i in range(len(ann_rows) - 1):
            r1, r2 = ann_rows[i], ann_rows[i + 1]
            if r1.get('revenue') and r2.get('revenue') and r2['revenue'] > 0:
                hist_growths.append(r1['revenue'] / r2['revenue'] - 1)
        hist_avg_growth = statistics.mean(hist_growths) if hist_growths else 0

        # 衰減係數
        decay = {1: 0.6, 2: 0.6, 3: 0.6, 4: 0.75, 5: 0.75, 6: 0.75,
                 7: 0.85, 8: 0.85, 9: 0.85, 10: 0.95, 11: 0.95, 12: 0.95}
        d = decay.get(n_months, 0.8)

        # 均值回歸：0.7 × YTD + 0.3 × 歷史
        adj_growth = (ytd_growth * 0.7 + hist_avg_growth * 0.3) * d
        # 上下限 -30% ~ +30%
        adj_growth = max(-0.30, min(adj_growth, 0.30))

        rev_method_b = prev_annual * (1 + adj_growth)
    else:
        rev_method_b = rev_method_a  # fallback

    # --- 取保守值 + 信心 ---
    if rev_method_a > 0 and rev_method_b > 0:
        gap_pct = abs(rev_method_a - rev_method_b) / min(rev_method_a, rev_method_b) * 100
        est_revenue = min(rev_method_a, rev_method_b)
        rev_confidence = 'A' if gap_pct < 10 else ('B' if gap_pct < 25 else 'C')
    else:
        est_revenue = rev_method_a
        rev_confidence = 'C'

    # 累計營收成長率
    rev_growth_pct = (ytd_revenue / prev_ytd - 1) * 100 if prev_ytd > 0 else None
    adj_growth_pct = (est_revenue / prev_annual - 1) * 100 if prev_annual > 0 else None

    # 單月 YoY 異常檢測
    anomaly = False
    monthly_yoys = []
    for m, rev in ytd_months:
        prev_m = rev_map.get((west_year - 1, m))
        if prev_m and prev_m > 0:
            monthly_yoys.append((rev / prev_m - 1) * 100)
    if len(monthly_yoys) >= 1:
        # 近 24 月 YoY 標準差
        all_yoys = []
        for rm in rev_rows:
            prev_r = rev_map.get((rm['year'] - 1, rm['month']))
            if prev_r and prev_r > 0:
                all_yoys.append((rm['revenue'] / prev_r - 1) * 100)
                if len(all_yoys) >= 24:
                    break
        if len(all_yoys) >= 6:
            yoy_mean = statistics.mean(all_yoys)
            yoy_sd = statistics.stdev(all_yoys)
            for my in monthly_yoys:
                if abs(my - yoy_mean) > yoy_sd * 2:
                    anomaly = True
                    break

    # === Step 2: 毛利率（年度×0.7 + 季度×0.3） ===
    gm_list = []
    for r in q_rows[:4]:
        if r.get('revenue') and r['revenue'] > 0 and r.get('gross_profit') is not None:
            gm_list.append(r['gross_profit'] / r['revenue'])
    if len(gm_list) >= 2:
        weights = [0.4, 0.3, 0.2, 0.1][:len(gm_list)]
        wsum = sum(weights)
        gm_q = sum(gm_list[i] * weights[i] for i in range(len(weights))) / wsum
        ann_gm = None
        for ar in ann_rows:
            if ar.get('revenue') and ar['revenue'] > 0 and ar.get('gross_profit') is not None:
                ann_gm = ar['gross_profit'] / ar['revenue']
                break
        est_gm = gm_q * 0.3 + ann_gm * 0.7 if ann_gm is not None else gm_q
    elif gm_list:
        est_gm = gm_list[0]
    else:
        return {"error": "無毛利率資料"}

    est_gross_profit = est_revenue * est_gm

    # === Step 3: 營業費用（年度×0.7 + 季度×0.3 錨定） ===
    opex_data = [(r['operating_expense'], r['revenue'])
                 for r in ann_rows
                 if r.get('operating_expense') is not None
                 and r.get('revenue') and r['revenue'] > 0]

    if len(opex_data) >= 1:
        # 年度費用率（近年加權）
        ann_opex_rates = [opex_data[i][0] / opex_data[i][1] for i in range(min(3, len(opex_data)))]
        w_a = [0.5, 0.3, 0.2][:len(ann_opex_rates)]
        ann_rate = sum(ann_opex_rates[i] * w_a[i] for i in range(len(w_a))) / sum(w_a)

        # 季度費用率
        q_opex_data = [(r['operating_expense'], r['revenue'])
                       for r in q_rows[:4]
                       if r.get('operating_expense') is not None
                       and r.get('revenue') and r['revenue'] > 0]
        if q_opex_data:
            q_rates = [q_opex_data[i][0] / q_opex_data[i][1] for i in range(min(4, len(q_opex_data)))]
            w_q = [0.4, 0.3, 0.2, 0.1][:len(q_rates)]
            q_rate = sum(q_rates[i] * w_q[i] for i in range(len(w_q))) / sum(w_q)
        else:
            q_rate = ann_rate

        est_opex = (ann_rate * 0.7 + q_rate * 0.3) * est_revenue
    elif opex_data:
        est_opex = opex_data[0][0]
    else:
        est_opex = est_gross_profit * 0.3

    est_oi = est_gross_profit - est_opex

    # === Step 4: 業外（年度加權×0.7 + 季度加權×0.3） ===
    nonop_ann = [r['non_operating'] for r in ann_rows if r.get('non_operating') is not None]
    nonop_q = [r['non_operating'] for r in q_rows[:4] if r.get('non_operating') is not None]
    nonop_level = 'stable'
    if nonop_ann:
        # 年度加權（近年權重高）
        w_ann = [0.4, 0.3, 0.2, 0.1][:len(nonop_ann)]
        ann_avg = sum(nonop_ann[i] * w_ann[i] for i in range(len(w_ann))) / sum(w_ann)

        # 季度加權 × 4 → 年化
        if nonop_q:
            w_q = [0.4, 0.3, 0.2, 0.1][:len(nonop_q)]
            q_annual = sum(nonop_q[i] * w_q[i] for i in range(len(w_q))) / sum(w_q) * 4
        else:
            q_annual = ann_avg

        est_nonop = ann_avg * 0.7 + q_annual * 0.3

        # 穩定度判斷（信心等級用）
        if len(nonop_ann) >= 3:
            avg = statistics.mean(nonop_ann)
            sd = statistics.stdev(nonop_ann)
            cv = sd / abs(avg) if abs(avg) > 0 else 0
            if cv >= 1.0: nonop_level = 'volatile'
            elif cv >= 0.5: nonop_level = 'moderate'
    elif nonop_q:
        est_nonop = sum(nonop_q) / len(nonop_q) * 4
    else:
        est_nonop = 0

    est_pti = est_oi + est_nonop

    # === Step 5: 稅率（年度×0.7 + 季度累計×0.3，含反算） ===
    tax_sum = pti_sum = 0
    for r in q_rows[:4]:
        pti = r.get('pretax_income')
        if pti and pti > 0:
            nip_r = r.get('net_income_parent')
            tax = r.get('tax')
            if nip_r is not None:
                calc_tax = pti - nip_r
                if tax is None or (tax == 0 and abs(calc_tax) > 100):
                    tax = calc_tax
                if abs(pti - nip_r) < 1 and pti > 1000000:
                    tax = pti * 0.20
            if tax is not None:
                pti_sum += pti; tax_sum += tax
    q_tax_rate = tax_sum / pti_sum if pti_sum > 0 else 0.20
    ann_tax_rate = None
    for ar in ann_rows:
        ar_pti = ar.get('pretax_income')
        ar_ni = ar.get('net_income') or ar.get('net_income_parent')
        ar_tax = ar.get('tax')
        if ar_pti and ar_ni:
            calc = ar_pti - ar_ni
            if ar_tax is None or (ar_tax == 0 and abs(calc) > 100):
                ar_tax = calc
            if abs(ar_pti - ar_ni) < 1 and ar_pti > 1000000:
                ar_tax = ar_pti * 0.20
        if ar_pti and ar_pti > 0 and ar_tax:
            ann_tax_rate = ar_tax / ar_pti
            break
    if ann_tax_rate is not None:
        est_tax_rate = max(0.05, min(ann_tax_rate * 0.7 + q_tax_rate * 0.3, 0.40))
    else:
        est_tax_rate = max(0.05, min(q_tax_rate, 0.40))
    est_ni = est_pti * (1 - est_tax_rate) if est_pti > 0 else est_pti * 0.80

    # === Step 6: 歸屬母公司權重 ===
    pw_list = []
    for r in q_rows[:4]:
        ci = r.get('continuing_income') or r.get('net_income_parent')
        nip = r.get('net_income_parent')
        if ci and ci != 0 and nip is not None:
            pw = nip / ci
            if 0.3 <= pw <= 1.1:
                pw_list.append(pw)
    est_pw = statistics.mean(pw_list) if pw_list else 1.0
    est_nip = est_ni * est_pw

    # === Step 7: 股數 ===
    est_shares = None
    for r in q_rows[:4]:
        if r.get('eps') and r['eps'] != 0 and r.get('net_income_parent') is not None:
            est_shares = r['net_income_parent'] / r['eps']
            break
    if not est_shares:
        return {"error": "無法計算股數"}

    est_eps = round(est_nip / est_shares, 2)

    # EPS-本業 / EPS-業外
    est_eps_core = round(est_oi * (1 - est_tax_rate) / est_shares, 2) if est_shares else None
    est_eps_nonop = round(est_eps - est_eps_core, 2) if est_eps_core is not None else None

    # === 信心等級 ===
    issues = []
    if anomaly: issues.append("單月YoY異常")
    if nonop_level == 'volatile': issues.append("業外高波動")
    elif nonop_level == 'moderate': issues.append("業外中波動")
    if rev_confidence == 'C': issues.append("兩法差距大")
    if n_months <= 3: issues.append("僅" + str(n_months) + "月資料")
    # 控股型偵測：業外佔比 > 200%
    if est_oi != 0 and abs(est_nonop / est_oi) > 2:
        issues.append("業外主導型")

    confidence = rev_confidence
    if anomaly and confidence == 'A':
        confidence = 'B'
    if est_oi != 0 and abs(est_nonop / est_oi) > 2 and confidence == 'A':
        confidence = 'B'

    # 去年 EPS
    last_year_eps = ann_rows[0].get('eps') if ann_rows else None

    # === 配息率估算（近 5 年加權，排除 EPS ≤ 0） ===
    payout_data = []
    for r in ann_rows:
        r_eps = r.get('eps')
        r_div = r.get('cash_dividend') or 0
        if r_eps and r_eps > 0:
            pr = min(r_div / r_eps, 1.0)  # 上限 100%
            payout_data.append(pr)

    if payout_data:
        weights = [0.35, 0.25, 0.20, 0.12, 0.08][:len(payout_data)]
        wsum = sum(weights)
        est_payout = sum(payout_data[i] * weights[i] for i in range(len(weights))) / wsum
    else:
        est_payout = None

    # 系統預估股利 & 本益比 & 殖利率
    est_div = round(est_eps * est_payout, 2) if est_payout is not None and est_eps > 0 else None

    # 取目前股價
    try:
        conn2 = sqlite3.connect(DB_PATH)
        conn2.row_factory = sqlite3.Row
        price_row = conn2.execute("SELECT close FROM stocks WHERE code = ?", (code,)).fetchone()
        conn2.close()
        cur_price = price_row['close'] if price_row and price_row['close'] else None
    except:
        cur_price = None

    est_pe = round(cur_price / est_eps, 2) if cur_price and est_eps and est_eps > 0 else None
    est_yld = round(est_div / cur_price * 100, 2) if cur_price and est_div and cur_price > 0 else None

    # === 系統價值評估矩陣 ===
    # PE 倍數：最低10, 偏低12, 合理14, 偏高16, 最高18
    # 殖利率：偏高5.5%, 最高6%
    # 長期殖利率：6%
    val = {}
    if est_eps and est_eps > 0 and est_div is not None:
        pe_low, pe_below, pe_fair = 10, 12, 14
        yld_high, yld_max = 5.5, 6.0

        # 加權股利（用近 5 年加權平均股利）
        wt_div_data = []
        for r in ann_rows:
            d = (r.get('cash_dividend') or 0) + (r.get('stock_dividend') or 0)
            if d > 0:
                wt_div_data.append(d)
        if wt_div_data:
            ww = [0.35, 0.25, 0.20, 0.12, 0.08][:len(wt_div_data)]
            wt_div = sum(wt_div_data[i] * ww[i] for i in range(len(ww))) / sum(ww)
        else:
            wt_div = est_div if est_div else 0

        lt_yld = 6.0
        lt6_val = round(wt_div / lt_yld * 100 + est_div, 2) if wt_div > 0 else None

        val['aa'] = round(min(est_eps * pe_low, est_div / yld_max * 100, lt6_val or 9999), 2)
        val['a1'] = round(min(est_eps * pe_low, est_div / yld_high * 100, lt6_val or 9999), 2)
        val['a2'] = round(min(est_eps * pe_below, est_div / yld_max * 100, lt6_val or 9999), 2)
        val['a']  = round(min(est_eps * pe_below, est_div / yld_high * 100, lt6_val or 9999), 2)
        val['lt6'] = lt6_val

    return {
        "target": f"{roc_year}年度",
        "est_eps": est_eps,
        "confidence": confidence,
        "details": {
            "n_months": n_months,
            "ytd_revenue": round(ytd_revenue),
            "rev_growth_pct": round(rev_growth_pct, 2) if rev_growth_pct is not None else None,
            "adj_growth_pct": round(adj_growth_pct, 2) if adj_growth_pct is not None else None,
            "rev_method_a": round(rev_method_a),
            "rev_method_b": round(rev_method_b),
            "est_revenue": round(est_revenue),
            "est_gm": round(est_gm * 100, 2),
            "est_gross_profit": round(est_gross_profit),
            "est_opex": round(est_opex),
            "est_oi": round(est_oi),
            "est_nonop": round(est_nonop),
            "est_pti": round(est_pti),
            "est_tax_rate": round(est_tax_rate * 100, 2),
            "est_ni": round(est_ni),
            "est_pw": round(est_pw * 100, 2),
            "est_nip": round(est_nip),
            "est_shares": round(est_shares),
            "est_eps_core": est_eps_core,
            "est_eps_nonop": est_eps_nonop,
            "last_year_eps": last_year_eps,
            "anomaly": anomaly,
            "issues": issues,
            "est_payout": round(est_payout * 100, 2) if est_payout is not None else None,
            "est_div": est_div,
            "est_pe": est_pe,
            "est_yld": est_yld,
            "cur_price": cur_price,
            "val": val,
        }
    }


def _init_eps_log_db():
    """建立估算日誌表"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS system_eps_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            code            TEXT NOT NULL,
            est_date        TEXT NOT NULL,
            target_period   TEXT NOT NULL,
            months_used     INTEGER,
            rev_growth      REAL,
            est_revenue     REAL,
            est_gm          REAL,
            est_opex        REAL,
            est_nonop       REAL,
            est_tax_rate    REAL,
            est_eps         REAL,
            confidence      TEXT,
            method_version  TEXT DEFAULT 'v1'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS system_eps_actual (
            code            TEXT NOT NULL,
            target_period   TEXT NOT NULL,
            actual_revenue  REAL,
            actual_eps      REAL,
            report_date     TEXT,
            PRIMARY KEY (code, target_period)
        )
    """)
    conn.commit()
    conn.close()


def _log_estimate(code, result, est_type='annual'):
    """將估算結果寫入日誌（新增不覆蓋）"""
    _init_eps_log_db()
    if 'error' in result:
        return
    d = result['details']
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO system_eps_log
        (code, est_date, target_period, months_used, rev_growth,
         est_revenue, est_gm, est_opex, est_nonop, est_tax_rate,
         est_eps, confidence, method_version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        code,
        datetime.now().strftime('%Y-%m-%d'),
        result.get('target') or result.get('quarter', ''),
        d.get('n_months') or d.get('rev_actual', 0),
        d.get('adj_growth_pct') or d.get('rev_growth_pct'),
        d.get('est_revenue'),
        d.get('est_gm'),
        d.get('est_opex'),
        d.get('est_nonop'),
        d.get('est_tax_rate'),
        result.get('est_eps'),
        result.get('confidence'),
        'v1'
    ))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--quick':
        quick_update()
    else:
        run()
