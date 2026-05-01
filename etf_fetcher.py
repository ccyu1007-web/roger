"""
etf_fetcher.py — 抓取 ETF 成分股持股明細
資料來源：
  1. 元大投信 API（0050/0056 等元大系列）
  2. TWSE 指數成分股公告
  3. 各投信官網（國泰/富邦等，未來擴充）

用法：
  python etf_fetcher.py          # 更新所有追蹤的 ETF
  python etf_fetcher.py 0050     # 只更新指定 ETF
"""

import db as sqlite3
import json
import time
import re
from datetime import datetime, date
from bs4 import BeautifulSoup
from fetcher_utils import create_session, DB_PATH

_session = create_session(
    ua='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
       'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    extra_headers={
        'Accept': 'application/json, text/html, */*',
        'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
    }
)

# 追蹤的 ETF 清單（依規模×換股頻率×幅度篩選，2026/4 更新）
TRACKED_ETFS = {
    # ── 主動式（每日揭露持股）──
    '00981A': {'name': '統一台股增長',         'issuer': 'usite',   'category': '主動式'},
    '00992A': {'name': '群益科技創新',         'issuer': 'megaetf', 'category': '主動式'},
    '00982A': {'name': '群益台灣強棒',         'issuer': 'megaetf', 'category': '主動式'},
    '00991A': {'name': '復華未來50',           'issuer': 'fhfund',  'category': '主動式'},
    # ── 高股息（季調，高換股幅度）──
    '00919':  {'name': '群益台灣精選高息',     'issuer': 'megaetf', 'category': '高股息'},
    '00929':  {'name': '復華台灣科技優息',     'issuer': 'fhfund',  'category': '高股息'},
    '00934':  {'name': '中信成長高股息',       'issuer': 'ctbc',    'category': '高股息'},
    '00918':  {'name': '大華優利高填息30',     'issuer': 'dh',      'category': '高股息'},
    '00940':  {'name': '元大台灣價值高息',     'issuer': 'yuanta',  'category': '高股息'},
    '00900':  {'name': '富邦特選高股息30',     'issuer': 'fubon',   'category': '高股息'},
    # ── 指標型（規模大，頻率低但具參考價值）──
    '0056':   {'name': '元大高股息',           'issuer': 'yuanta',  'category': '指標型'},
    '00878':  {'name': '國泰永續高股息',       'issuer': 'cathay',  'category': '指標型'},
    '0050':   {'name': '元大台灣50',           'issuer': 'yuanta',  'category': '指標型'},
    '006208': {'name': '富邦台50',             'issuer': 'ftse_tw50_sync', 'category': '指標型'},
    # ── 國際指數（僅展示成分股，不追蹤異動）──
    'MSCI_TW':   {'name': 'MSCI台灣指數',     'issuer': 'msci',    'category': '國際指數', 'track_changes': False},
    'FTSE_TW50': {'name': 'FTSE台灣50指數',   'issuer': 'ftse',    'category': '國際指數', 'track_changes': False},
}


# ── 資料庫初始化 ────────────────────────────────────────────
def init_etf_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS etf_holdings (
            etf_code   TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            weight     REAL,
            shares     INTEGER,
            updated    TEXT NOT NULL,
            PRIMARY KEY (etf_code, stock_code)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS etf_changes (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            etf_code   TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            action     TEXT NOT NULL,
            change_date TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    # ETF 基本資訊表
    c.execute("""
        CREATE TABLE IF NOT EXISTS etf_info (
            code       TEXT PRIMARY KEY,
            name       TEXT,
            issuer     TEXT,
            category   TEXT,
            last_fetch TEXT
        )
    """)
    # 舊表可能沒有 category 欄位，嘗試補上
    try:
        c.execute("ALTER TABLE etf_info ADD COLUMN category TEXT")
    except Exception: pass

    conn.commit()
    conn.close()
    print("[ETF DB] 資料表已就緒")


# ── Nuxt SSR 解析工具 ───────────────────────────────────────
def _parse_nuxt_args(text):
    """解析 Nuxt SSR __NUXT__ IIFE 的參數名→值對應表"""
    # 取得函數參數名: window.__NUXT__=(function(a,b,c,...){...})(val1,val2,...)
    param_match = re.match(r'window\.__NUXT__=\(function\(([^)]+)\)', text)
    if not param_match:
        return {}
    params = param_match.group(1).split(',')

    # 取得引數值：找 }( 分隔點
    idx = text.rfind('}(')
    if idx < 0:
        return {}
    args_str = text[idx + 2:]
    # 去掉結尾的 )); 或 ))
    args_str = args_str.rstrip(';').rstrip(')')

    # 手動解析逗號分隔的 JS 值
    args = []
    current = ''
    in_str = False
    escape = False
    for ch in args_str:
        if escape:
            current += ch
            escape = False
            continue
        if ch == '\\':
            current += ch
            escape = True
            continue
        if ch == '"' and not escape:
            in_str = not in_str
            current += ch
            continue
        if ch == ',' and not in_str:
            args.append(_parse_js_val(current.strip()))
            current = ''
            continue
        current += ch
    if current.strip():
        args.append(_parse_js_val(current.strip()))

    # 建立對應表
    var_map = {}
    for i, p in enumerate(params):
        if i < len(args):
            var_map[p] = args[i]
    return var_map


def _parse_js_val(s):
    """將 JS 字面值轉為 Python 值"""
    if s == 'null':
        return None
    if s == 'true':
        return True
    if s == 'false':
        return False
    if s.startswith('"'):
        try:
            return json.loads(s)
        except Exception:
            return s.strip('"')
    try:
        return int(s)
    except ValueError:
        try:
            return float(s)
        except ValueError:
            return s


def _resolve_nuxt_val(var_map, key):
    """解析 Nuxt 變數（可能是直接值或變數參考）"""
    if key in var_map:
        return var_map[key]
    return key  # 已是字面值


# ── 來源 1：元大投信（Nuxt SSR 解析）─────────────────────────
def _fetch_yuanta(etf_code):
    """從元大投信網站的 Nuxt SSR 資料抓取 ETF 持股權重"""
    try:
        url = f'https://www.yuantaetfs.com/product/detail/{etf_code}/ratio'
        r = _session.get(url, timeout=20)
        if r.status_code != 200:
            print(f"  [元大] HTTP {r.status_code}")
            return []

        soup = BeautifulSoup(r.text, 'html.parser')
        for script in soup.find_all('script'):
            text = script.string or ''
            if '__NUXT__' not in text:
                continue

            var_map = _parse_nuxt_args(text)
            if not var_map:
                print("  [元大] 無法解析 __NUXT__ 參數")
                return []

            print(f"  [元大] 解析 Nuxt 變數 {len(var_map)} 個")

            # 提取 StockWeights 陣列
            # 格式: StockWeights:[{code:xx,...},{code:yy,...},...],
            sw_start = text.find('StockWeights:[')
            if sw_start < 0:
                print("  [元大] 找不到 StockWeights")
                return []

            # 找到對應的 ] 結尾（跳過巢狀的 {}）
            bracket_depth = 0
            sw_end = sw_start + len('StockWeights:[')
            for ci in range(sw_end, min(sw_end + 50000, len(text))):
                ch = text[ci]
                if ch == '[':
                    bracket_depth += 1
                elif ch == ']':
                    if bracket_depth == 0:
                        sw_end = ci
                        break
                    bracket_depth -= 1

            sw_text = text[sw_start + len('StockWeights:['):sw_end]
            # 解析每一筆 {code:xx,name:xx,weights:xx,qty:xx}
            items = re.findall(r'\{([^}]+)\}', sw_text)
            holdings = []
            for item_str in items:
                # 用 regex 抓 key:value，避免字串中的逗號干擾
                pairs = re.findall(
                    r'(\w+):("(?:[^"\\]|\\.)*?"|\w+(?:\.\w+)?)', item_str)
                fields = {k: v for k, v in pairs}

                code_raw = fields.get('code', '').strip('"')
                name_raw = fields.get('name', '').strip('"')
                # 如果值不帶引號，可能是 Nuxt 變數
                code = str(_resolve_nuxt_val(var_map, code_raw)) if code_raw else ''
                name = str(_resolve_nuxt_val(var_map, name_raw)) if name_raw else ''

                weight = None
                w_str = fields.get('weights', '')
                try:
                    weight = float(w_str)
                except Exception: pass

                qty = None
                q_str = fields.get('qty', '')
                try:
                    qty = int(q_str)
                except Exception: pass

                if re.match(r'^\d{4,6}$', code):
                    holdings.append({
                        'stock_code': code,
                        'stock_name': name,
                        'weight': weight,
                        'shares': qty,
                    })

            print(f"  [元大] 解析出 {len(holdings)} 筆持股")
            return holdings

    except Exception as e:
        print(f"  [元大] 失敗: {e}")

    return []


# ── 來源 2：MoneyDJ（通用 fallback，抓 top 10）──────────────
def _fetch_moneydj(etf_code):
    """從 MoneyDJ 抓取 ETF 前 10 大持股"""
    try:
        url = f'https://www.moneydj.com/ETF/X/Basic/Basic0007.xdjhtm?etfid={etf_code}.TW'
        r = _session.get(url, timeout=15)
        r.encoding = 'utf-8'
        if r.status_code != 200:
            print(f"  [MoneyDJ] HTTP {r.status_code}")
            return []

        soup = BeautifulSoup(r.text, 'html.parser')
        holdings = []

        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            text = table.get_text()
            # 找含有「投資比例」和股票代碼的表格
            if '投資比例' in text and re.search(r'\d{4}\.TW', text):
                for row in rows[1:]:  # 跳過表頭
                    cols = row.find_all(['td', 'th'])
                    if len(cols) >= 2:
                        cell_text = cols[0].get_text(strip=True)
                        # 格式: 國泰金(2882.TW)
                        m = re.match(r'(.+?)\((\d{4,6})\.TW\)', cell_text)
                        if m:
                            name = m.group(1)
                            code = m.group(2)
                            weight = None
                            if len(cols) >= 2:
                                try:
                                    weight = float(cols[1].get_text(strip=True))
                                except Exception: pass
                            shares = None
                            if len(cols) >= 3:
                                try:
                                    shares = int(cols[2].get_text(strip=True).replace(',', '').replace('.00', ''))
                                except Exception: pass
                            holdings.append({
                                'stock_code': code,
                                'stock_name': name,
                                'weight': weight,
                                'shares': shares,
                            })
                break

        if holdings:
            print(f"  [MoneyDJ] 解析出 {len(holdings)} 筆持股（前10大）")
        return holdings

    except Exception as e:
        print(f"  [MoneyDJ] 失敗: {e}")
        return []


# ── 寫入資料庫 ──────────────────────────────────────────────
def _save_holdings(etf_code, holdings):
    """儲存持股明細，並比對前後差異產生異動紀錄"""
    if not holdings:
        print(f"  [{etf_code}] 無資料，跳過")
        return 0

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today = date.today().isoformat()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 取得舊持股
    c.execute("SELECT stock_code, stock_name FROM etf_holdings WHERE etf_code=?", (etf_code,))
    old_codes = {row[0]: row[1] for row in c.fetchall()}
    new_codes = {h['stock_code']: h.get('stock_name', '') for h in holdings}

    # 是否追蹤異動（國際指數等設定 track_changes=False 的不追蹤）
    track = TRACKED_ETFS.get(etf_code, {}).get('track_changes', True)

    # 計算異動（只在已有基準線時才記錄，第一次跑不記）
    added = set(new_codes.keys()) - set(old_codes.keys())
    removed = set(old_codes.keys()) - set(new_codes.keys())

    # 防止部分來源（如 MoneyDJ 前10大）覆蓋完整持股造成假異動
    # 新資料筆數不到舊資料的一半 → 判定為不完整資料，不記異動也不覆蓋
    if old_codes and len(new_codes) < len(old_codes) * 0.5:
        print(f"  [{etf_code}] 新資料 {len(new_codes)} 筆遠少於舊資料 {len(old_codes)} 筆，疑似不完整，跳過覆蓋")
        conn.close()
        return len(old_codes)

    if track and old_codes:  # 有舊資料才是真正的異動
        for code in added:
            c.execute("""INSERT INTO etf_changes (etf_code, stock_code, stock_name, action, change_date, created_at)
                         VALUES (?, ?, ?, 'add', ?, ?)""",
                      (etf_code, code, new_codes.get(code, ''), today, now))
            print(f"  [異動] {etf_code} 新增成分股: {code} {new_codes.get(code, '')}")

        for code in removed:
            c.execute("""INSERT INTO etf_changes (etf_code, stock_code, stock_name, action, change_date, created_at)
                         VALUES (?, ?, ?, 'remove', ?, ?)""",
                      (etf_code, code, old_codes.get(code, ''), today, now))
            print(f"  [異動] {etf_code} 剔除成分股: {code} {old_codes.get(code, '')}")
    elif added and track:
        print(f"  [{etf_code}] 首次建立基準線，{len(added)} 筆持股（不記為異動）")
    elif added and not track:
        print(f"  [{etf_code}] 僅展示，寫入 {len(added)} 筆持股（不追蹤異動）")

    # 全量覆蓋持股
    c.execute("DELETE FROM etf_holdings WHERE etf_code=?", (etf_code,))
    for h in holdings:
        c.execute("""INSERT INTO etf_holdings (etf_code, stock_code, stock_name, weight, shares, updated)
                     VALUES (?, ?, ?, ?, ?, ?)""",
                  (etf_code, h['stock_code'], h.get('stock_name', ''),
                   h.get('weight'), h.get('shares'), today))

    # 更新 ETF info
    etf_info = TRACKED_ETFS.get(etf_code, {})
    c.execute("""INSERT OR REPLACE INTO etf_info (code, name, issuer, category, last_fetch)
                 VALUES (?, ?, ?, ?, ?)""",
              (etf_code, etf_info.get('name', ''), etf_info.get('issuer', ''),
               etf_info.get('category', ''), now))

    conn.commit()
    conn.close()

    print(f"  [{etf_code}] 寫入 {len(holdings)} 筆持股"
          f"{f', 新增 {len(added)}' if added else ''}"
          f"{f', 剔除 {len(removed)}' if removed else ''}")
    return len(holdings)


# ── 來源 3：MSCI 台灣指數（群益權值計算機）───────────────────
def _fetch_msci_tw():
    """從群益權值計算機抓取 MSCI 台灣指數成分股"""
    try:
        url = 'https://stock.capital.com.tw/z/zm/zmd/zmdc.djhtm?MSCI=0'
        r = _session.get(url, timeout=20)
        r.encoding = 'big5'
        if r.status_code != 200:
            print(f"  [MSCI] HTTP {r.status_code}")
            return []

        soup = BeautifulSoup(r.text, 'html.parser')
        holdings = []
        seen = set()

        # 連結格式: javascript:Link2Stk('2330')，文字: 2330台積電
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            m = re.search(r"Link2Stk\('(\d{4,6})'\)", href)
            if m:
                code = m.group(1)
                if code in seen:
                    continue
                seen.add(code)
                text = a_tag.get_text(strip=True)
                # 文字格式: "2330台積電" → 去掉前面的代號
                name = re.sub(r'^\d{4,6}', '', text).strip()
                holdings.append({
                    'stock_code': code,
                    'stock_name': name,
                    'weight': None,
                    'shares': None,
                })

        if holdings:
            print(f"  [MSCI] 解析出 {len(holdings)} 筆成分股")
        else:
            print("  [MSCI] 無法解析成分股")
        return holdings

    except Exception as e:
        print(f"  [MSCI] 失敗: {e}")
        return []


# ── 來源 4：FTSE 台灣50（同步自 0050）─────────────────────────
def _fetch_ftse_tw50():
    """FTSE 台灣50 跟 0050 追蹤同一指數，直接同步"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT stock_code, stock_name, weight, shares FROM etf_holdings WHERE etf_code='0050'")
    rows = c.fetchall()
    conn.close()
    if rows:
        holdings = [dict(r) for r in rows]
        print(f"  [FTSE TW50] 同步自 0050，{len(holdings)} 筆")
        return holdings
    print("  [FTSE TW50] 0050 尚無持股資料，跳過")
    return []


# ── 主抓取邏輯 ──────────────────────────────────────────────
def fetch_etf_holdings(etf_code):
    """抓取單一 ETF 持股明細"""
    info = TRACKED_ETFS.get(etf_code, {})
    issuer = info.get('issuer', 'unknown')
    print(f"\n[ETF] 抓取 {etf_code} {info.get('name', '')} (發行商: {issuer})")

    holdings = []

    # 國際指數：專用來源
    if etf_code == 'MSCI_TW':
        holdings = _fetch_msci_tw()
        if holdings:
            return _save_holdings(etf_code, holdings)
        return 0

    if etf_code == 'FTSE_TW50':
        holdings = _fetch_ftse_tw50()
        if holdings:
            return _save_holdings(etf_code, holdings)
        return 0

    # 006208 富邦台50：與 0050 追蹤同一指數，直接同步
    if etf_code == '006208':
        holdings = _fetch_ftse_tw50()  # 同樣從 0050 同步
        if holdings:
            return _save_holdings(etf_code, holdings)
        return 0

    # 元大系列：Nuxt SSR 解析（完整持股）
    if issuer == 'yuanta':
        holdings = _fetch_yuanta(etf_code)

    # 所有 ETF 的通用 fallback：MoneyDJ（前 10 大持股）
    if not holdings:
        print(f"  發行商來源無資料，嘗試 MoneyDJ...")
        holdings = _fetch_moneydj(etf_code)

    if holdings:
        return _save_holdings(etf_code, holdings)
    else:
        print(f"  [{etf_code}] 所有來源皆無資料")
        return 0


def run(target_etf=None):
    """執行 ETF 持股更新"""
    init_etf_db()

    if target_etf:
        codes = [target_etf]
    else:
        # FTSE_TW50 依賴 0050 資料，確保排在最後
        codes = [c for c in TRACKED_ETFS.keys() if c != 'FTSE_TW50']
        if 'FTSE_TW50' in TRACKED_ETFS:
            codes.append('FTSE_TW50')

    total = 0
    for code in codes:
        count = fetch_etf_holdings(code)
        total += count
        time.sleep(1)  # 禮貌性延遲

    print(f"\n[ETF] 完成！共更新 {total} 筆持股資料")
    return total


# ── 查詢功能（供 app.py 使用）─────────────────────────────
def get_stock_etf_membership(stock_code):
    """查詢某股票被哪些 ETF 持有"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT h.etf_code, i.name as etf_name, h.weight, h.updated
        FROM etf_holdings h
        LEFT JOIN etf_info i ON h.etf_code = i.code
        WHERE h.stock_code = ?
        ORDER BY h.weight DESC
    """, (stock_code,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_etf_holdings_list(etf_code):
    """查詢某 ETF 的所有持股"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT stock_code, stock_name, weight, shares, updated
        FROM etf_holdings
        WHERE etf_code = ?
        ORDER BY weight DESC
    """, (etf_code,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_etf_changes(etf_code=None, limit=50):
    """查詢成分股異動紀錄"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    if etf_code:
        c.execute("""
            SELECT * FROM etf_changes
            WHERE etf_code = ?
            ORDER BY created_at DESC LIMIT ?
        """, (etf_code, limit))
    else:
        c.execute("""
            SELECT * FROM etf_changes
            ORDER BY created_at DESC LIMIT ?
        """, (limit,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


if __name__ == '__main__':
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else None
    run(target)
