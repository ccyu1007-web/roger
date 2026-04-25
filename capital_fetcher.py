"""
capital_fetcher.py — 從群益證券（嘉實系統）抓取財務三表
免費、無額度限制、有完整歷史資料
來源優先級最高，損益表欄位直接覆蓋 FinMind/Yahoo

三表 URL：
  損益表(季): zce/zce_{code}.djhtm
  損益表(年): zcq/zcqa.djhtm?a={code}
  資產負債表(年): zcp/zcpb/zcpb.djhtm?a={code}
  資產負債表(季): zcp/zcpa/zcpa.djhtm?a={code}
  現金流量表(年): zc3/zc3a.djhtm?a={code}
  現金流量表(季): zc3/zc3.djhtm?a={code}
"""
import requests
import db as sqlite3
import time
import random
import re
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = "stocks.db"

_session = requests.Session()
_session.headers.update({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'})


def _parse_num(s):
    """解析群益的數值（含千分位逗號、負號）"""
    if not s:
        return None
    s = s.replace(',', '').replace('%', '').strip()
    if s in ('', '-', '--', 'N/A'):
        return None
    try:
        return float(s)
    except:
        return None


def _fetch_page(url):
    """抓取群益頁面並解析 table-cell"""
    try:
        r = _session.get(url, timeout=15)
        r.encoding = 'big5'
        soup = BeautifulSoup(r.text, 'html.parser')
        cells = soup.find_all(class_=lambda x: x and 'table-cell' in x)
        texts = [c.get_text(strip=True) for c in cells if c.get_text(strip=True)]
        return texts
    except:
        return []


def _extract_yearly_data(texts, row_labels):
    """
    從群益年表頁面提取資料。
    texts: table-cell 的文字列表
    row_labels: {顯示名稱: db欄位名} 的對應表
    回傳: {year: {field: value, ...}, ...}
    """
    if not texts:
        return {}

    # 找期別行取得年份列表
    years = []
    period_idx = None
    for i, t in enumerate(texts):
        if t == '期別':
            period_idx = i
            # 後續的數字就是年份
            for j in range(i + 1, min(i + 10, len(texts))):
                if texts[j].replace('.', '').isdigit():
                    years.append(texts[j])
                else:
                    break
            break

    if not years:
        return {}

    n_years = len(years)
    result = {y: {} for y in years}

    # 逐行找資料
    for i, t in enumerate(texts):
        if t in row_labels and i + n_years < len(texts):
            field = row_labels[t]
            vals = texts[i + 1: i + 1 + n_years]
            for j, y in enumerate(years):
                if j < len(vals):
                    result[y][field] = _parse_num(vals[j])

    return result


def _extract_quarterly_data(texts, row_labels):
    """
    從群益季表頁面提取資料（含 table-cell class 的頁面）。
    回傳: {quarter_label: {field: value, ...}, ...}
    """
    if not texts:
        return {}

    # 找期別行取得季度列表 (如 "2025.4Q", "2025.3Q", ...)
    quarters = []
    for i, t in enumerate(texts):
        if t == '期別':
            for j in range(i + 1, min(i + 10, len(texts))):
                if re.match(r'\d{4}\.\d+Q', texts[j]):
                    quarters.append(texts[j])
                elif texts[j] in ('種類', '合併'):
                    break
                else:
                    break
            break

    if not quarters:
        return {}

    n_q = len(quarters)
    result = {q: {} for q in quarters}

    for i, t in enumerate(texts):
        if t in row_labels and i + n_q < len(texts):
            field = row_labels[t]
            vals = texts[i + 1: i + 1 + n_q]
            for j, q in enumerate(quarters):
                if j < len(vals):
                    result[q][field] = _parse_num(vals[j])

    return result


# ── 損益表（季表，用原本的 zce 格式）────────────────────────

def fetch_capital_financials(code):
    """從群益抓取個股季度損益表，存入 financial_annual + quarterly_financial"""
    try:
        url = f"https://stock.capital.com.tw/z/zc/zce/zce_{code}.djhtm"
        r = _session.get(url, timeout=15)
        r.encoding = 'big5'
        soup = BeautifulSoup(r.text, 'html.parser')
    except:
        return 0, 0

    # 找有「季別」表頭的表格
    target_table = None
    for t in soup.find_all('table'):
        rows = t.find_all('tr')
        for row in rows[:3]:
            cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
            if '季別' in cells and '營業收入' in cells:
                target_table = t
                break
        if target_table:
            break

    if not target_table:
        return 0, 0

    rows = target_table.find_all('tr')
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    quarterly_saved = 0
    annual_data = {}

    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
        if len(cells) < 10 or not re.match(r'\d+\.\d+Q', cells[0]):
            continue

        q_label = cells[0]
        m = re.match(r'(\d+)\.(\d+)Q', q_label)
        if not m:
            continue

        roc_year = int(m.group(1))
        quarter = int(m.group(2))
        west_year = roc_year + 1911
        quarter_label = f"{roc_year}Q{quarter}"

        revenue = _parse_num(cells[1])
        cost = _parse_num(cells[2])
        gross_profit = _parse_num(cells[3])
        operating_income = _parse_num(cells[5])
        non_operating = _parse_num(cells[7])
        pretax_income = _parse_num(cells[8])
        net_income = _parse_num(cells[9])
        eps = _parse_num(cells[10]) if len(cells) > 10 else None

        mul = 1000000
        if revenue is not None: revenue *= mul
        if cost is not None: cost *= mul
        if gross_profit is not None: gross_profit *= mul
        if operating_income is not None: operating_income *= mul
        if non_operating is not None: non_operating *= mul
        if pretax_income is not None: pretax_income *= mul
        if net_income is not None: net_income *= mul

        # 反算營業費用 = 毛利 - 營業利益
        opex = None
        if gross_profit is not None and operating_income is not None:
            opex = round(gross_profit - operating_income, 4)

        # 群益損益表直接覆蓋（最高優先級）
        try:
            c.execute("""INSERT INTO quarterly_financial
                (code, quarter, revenue, cost, gross_profit, operating_expense,
                 operating_income, non_operating, pretax_income, net_income_parent, eps, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(code, quarter) DO UPDATE SET
                revenue=excluded.revenue,
                cost=excluded.cost,
                gross_profit=excluded.gross_profit,
                operating_expense=excluded.operating_expense,
                operating_income=excluded.operating_income,
                non_operating=excluded.non_operating,
                pretax_income=excluded.pretax_income,
                net_income_parent=excluded.net_income_parent,
                eps=excluded.eps,
                updated_at=excluded.updated_at""",
                (code, quarter_label, revenue, cost, gross_profit, opex, operating_income,
                 non_operating, pretax_income, net_income, eps, now_str))
            quarterly_saved += 1
        except:
            pass

        # 累計到年度
        if west_year not in annual_data:
            annual_data[west_year] = {'revenue': 0, 'cost': 0, 'gross_profit': 0,
                                      'operating_income': 0, 'non_operating': 0,
                                      'pretax_income': 0, 'net_income': 0,
                                      'eps': 0, 'quarters': 0}
        ad = annual_data[west_year]
        if revenue: ad['revenue'] += revenue
        if cost: ad['cost'] += cost
        if gross_profit: ad['gross_profit'] += gross_profit
        if operating_income: ad['operating_income'] += operating_income
        if non_operating is not None: ad['non_operating'] += non_operating
        if pretax_income: ad['pretax_income'] += pretax_income
        if net_income: ad['net_income'] += net_income
        if eps: ad['eps'] += eps
        ad['quarters'] += 1

    # 寫入 financial_annual（只寫四季齊全的年度）
    annual_saved = 0
    for yr, ad in annual_data.items():
        if ad['quarters'] != 4:
            continue

        opex = None
        if ad['gross_profit'] and ad['operating_income'] is not None:
            opex = round(ad['gross_profit'] - ad['operating_income'], 4)

        try:
            c.execute("""INSERT INTO financial_annual
                (code, year, revenue, cost, gross_profit, operating_expense,
                 operating_income, non_operating, pretax_income, net_income, eps, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(code, year) DO UPDATE SET
                revenue=excluded.revenue,
                cost=excluded.cost,
                gross_profit=excluded.gross_profit,
                operating_expense=excluded.operating_expense,
                operating_income=excluded.operating_income,
                non_operating=excluded.non_operating,
                pretax_income=excluded.pretax_income,
                net_income=excluded.net_income,
                eps=excluded.eps,
                updated_at=excluded.updated_at""",
                (code, yr, ad['revenue'], ad['cost'], ad['gross_profit'],
                 opex, ad['operating_income'], ad['non_operating'],
                 ad['pretax_income'], ad['net_income'], ad['eps'], now_str))
            annual_saved += 1
        except:
            pass

    conn.commit()
    conn.close()
    return annual_saved, quarterly_saved


# ── 資產負債表（年表）────────────────────────────────────

def fetch_capital_balance_sheet(code):
    """從群益抓取年度資產負債表，補寫 total_assets / total_equity / common_stock / inventory / contract_liability"""
    url = f"https://stock.capital.com.tw/z/zc/zcp/zcpb/zcpb.djhtm?a={code}"
    texts = _fetch_page(url)
    if not texts:
        return 0

    row_labels = {
        '資產總額': 'total_assets',
        '股東權益總額': 'total_equity',
        '股本': 'common_stock',
        '存貨': 'inventory',
        '合約負債－流動': 'contract_liability',
    }
    data = _extract_yearly_data(texts, row_labels)
    if not data:
        return 0

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # 確保欄位存在
    for col in ['inventory', 'contract_liability']:
        try: c.execute(f"ALTER TABLE financial_annual ADD COLUMN {col} REAL")
        except: pass
    mul = 1000000  # 百萬 → 元

    saved = 0
    for year_str, fields in data.items():
        yr = int(float(year_str))
        ta = fields.get('total_assets')
        te = fields.get('total_equity')
        cs = fields.get('common_stock')
        inv = fields.get('inventory')
        cl = fields.get('contract_liability')

        for v_name in ['ta', 'te', 'cs', 'inv', 'cl']:
            v = locals()[v_name]
            if v is not None:
                locals()[v_name] = v * mul

        ta = fields.get('total_assets')
        te = fields.get('total_equity')
        cs = fields.get('common_stock')
        inv = fields.get('inventory')
        cl = fields.get('contract_liability')
        if ta is not None: ta *= mul
        if te is not None: te *= mul
        if cs is not None: cs *= mul
        if inv is not None: inv *= mul
        if cl is not None: cl *= mul

        if ta is None and te is None:
            continue

        try:
            c.execute("""INSERT INTO financial_annual (code, year, total_assets, total_equity, common_stock,
                         inventory, contract_liability, updated_at)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(code, year) DO UPDATE SET
                total_assets=COALESCE(excluded.total_assets, total_assets),
                total_equity=COALESCE(excluded.total_equity, total_equity),
                common_stock=COALESCE(excluded.common_stock, common_stock),
                inventory=COALESCE(excluded.inventory, inventory),
                contract_liability=COALESCE(excluded.contract_liability, contract_liability),
                updated_at=excluded.updated_at""",
                (code, yr, ta, te, cs, inv, cl, now_str))
            saved += 1
        except:
            pass

    conn.commit()
    conn.close()
    return saved


# ── 資產負債表（季表）→ 合約負債 ─────────────────────────

def fetch_capital_contract_liability(code):
    """從群益季度資產負債表抓取合約負債-流動，寫入 quarterly_financial"""
    url = f"https://stock.capital.com.tw/z/zc/zcp/zcpa/zcpa.djhtm?a={code}"
    texts = _fetch_page(url)
    if not texts:
        return 0

    # 找期別（季度格式：2025.4Q, 2025.3Q, ...）
    quarters = []
    for i, t in enumerate(texts):
        if t == '期別':
            for j in range(i + 1, min(i + 10, len(texts))):
                if re.match(r'\d{4}\.\d+Q', texts[j]):
                    quarters.append(texts[j])
                elif texts[j] in ('種類', '合併'):
                    break
                else:
                    break
            break

    if not quarters:
        return 0

    n_q = len(quarters)

    # 找合約負債-流動 和 存貨
    cl_values = {}
    inv_values = {}
    for i, t in enumerate(texts):
        if t == '合約負債－流動' and i + n_q < len(texts):
            vals = texts[i + 1: i + 1 + n_q]
            for j, q in enumerate(quarters):
                if j < len(vals):
                    cl_values[q] = _parse_num(vals[j])
        if t == '存貨' and i + n_q < len(texts):
            vals = texts[i + 1: i + 1 + n_q]
            for j, q in enumerate(quarters):
                if j < len(vals):
                    inv_values[q] = _parse_num(vals[j])

    if not cl_values and not inv_values:
        return 0

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # 確保欄位存在
    try: c.execute("ALTER TABLE quarterly_financial ADD COLUMN inventory REAL")
    except: pass
    mul = 1000000  # 百萬 → 元

    saved = 0
    all_quarters = set(list(cl_values.keys()) + list(inv_values.keys()))
    for q_label in all_quarters:
        cl = cl_values.get(q_label)
        inv = inv_values.get(q_label)
        if cl is not None: cl *= mul
        if inv is not None: inv *= mul

        # 轉換季度格式：2025.4Q → 114Q4
        m = re.match(r'(\d{4})\.(\d+)Q', q_label)
        if not m:
            continue
        west_year = int(m.group(1))
        quarter = int(m.group(2))
        roc_year = west_year - 1911
        quarter_key = f"{roc_year}Q{quarter}"

        try:
            sets = []
            vals = []
            if cl is not None:
                sets.append("contract_liability = ?")
                vals.append(cl)
            if inv is not None:
                sets.append("inventory = ?")
                vals.append(inv)
            sets.append("updated_at = ?")
            vals.append(now_str)
            vals.extend([code, quarter_key])
            c.execute(f"UPDATE quarterly_financial SET {', '.join(sets)} WHERE code = ? AND quarter = ?", vals)
            if c.rowcount:
                saved += 1
        except:
            pass

    conn.commit()
    conn.close()
    return saved


# ── 股利政策（zcc）────────────────────────────────────────

def fetch_capital_dividend(code):
    """從群益抓取歷年股利，寫入 financial_annual 的 cash_dividend / stock_dividend"""
    try:
        url = f"https://stock.capital.com.tw/z/zc/zcc/zcc.djhtm?a={code}"
        r = _session.get(url, timeout=15)
        r.encoding = 'big5'
        soup = BeautifulSoup(r.text, 'html.parser')
    except:
        return 0

    tds = soup.find_all('td', class_=re.compile(r't3n[01]'))
    texts = [td.get_text(strip=True) for td in tds]

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    saved = 0
    i = 0
    while i < len(texts):
        if re.match(r'20\d{2}$', texts[i]) and i + 8 < len(texts):
            row = texts[i:i + 9]
            year = int(row[0])
            # row[1]=盈餘發放, row[2]=公積發放, row[3]=小計(現金)
            # row[4]=盈餘配股, row[5]=公積配股, row[6]=小計(股票)
            # 必須用小計（含公積發放），不能只讀盈餘發放（如台泥114年公積發放0.8會漏掉）
            cash_div = _parse_num(row[3])  # 小計(現金) = 盈餘發放 + 公積發放
            stock_div_total = _parse_num(row[6])  # 小計(股票) = 盈餘配股 + 公積配股

            if cash_div is not None or stock_div_total is not None:
                try:
                    c.execute("""INSERT INTO financial_annual (code, year, cash_dividend, stock_dividend, updated_at)
                        VALUES (?,?,?,?,?)
                        ON CONFLICT(code, year) DO UPDATE SET
                        cash_dividend = excluded.cash_dividend,
                        stock_dividend = excluded.stock_dividend,
                        updated_at = excluded.updated_at""",
                        (code, year, cash_div, stock_div_total, now_str))
                    saved += 1
                except:
                    pass
            i += 9
        else:
            i += 1

    conn.commit()
    conn.close()
    return saved


# ── 現金流量表（年表）────────────────────────────────────

def fetch_capital_cashflow(code):
    """從群益抓取年度現金流量表，補寫 operating_cf / capex"""
    url = f"https://stock.capital.com.tw/z/zc/zc3/zc3a.djhtm?a={code}"
    texts = _fetch_page(url)
    if not texts:
        return 0

    row_labels = {
        '來自營運之現金流量': 'operating_cf',
        '購置不動產廠房設備（含預付）－CFI': 'capex',
        '投資活動之現金流量': 'investing_cf',
        '籌資活動之現金流量': 'financing_cf',
    }
    data = _extract_yearly_data(texts, row_labels)
    if not data:
        return 0

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    mul = 1000000

    saved = 0
    for year_str, fields in data.items():
        yr = int(float(year_str))
        ocf = fields.get('operating_cf')
        capex = fields.get('capex')

        if ocf is not None: ocf *= mul
        if capex is not None:
            capex *= mul
            # 群益的資本支出是負數（購置），確保是負數
            if capex > 0:
                capex = -capex

        if ocf is None and capex is None:
            continue

        try:
            c.execute("""INSERT INTO financial_annual (code, year, operating_cf, capex, updated_at)
                VALUES (?,?,?,?,?)
                ON CONFLICT(code, year) DO UPDATE SET
                operating_cf=COALESCE(excluded.operating_cf, operating_cf),
                capex=COALESCE(excluded.capex, capex),
                updated_at=excluded.updated_at""",
                (code, yr, ocf, capex, now_str))
            saved += 1
        except:
            pass

    conn.commit()
    conn.close()
    return saved


# ── 年度損益表（群益 zcqa）── 年度 EPS 最優先來源 ─────────

def fetch_capital_annual_eps(code):
    """從群益年度損益表抓取個股近 8 年每股盈餘，回傳 {民國年: eps}"""
    try:
        url = f"https://stock.capital.com.tw/z/zc/zcq/zcqa.djhtm?a={code}"
        r = _session.get(url, timeout=15)
        r.encoding = 'big5'
        soup = BeautifulSoup(r.text, 'html.parser')
    except:
        return {}

    spans = soup.find_all('span', class_=lambda c: c and 'table-cell' in c)
    if not spans:
        return {}

    # 第一列是「期別, 2025, 2024, ...」，取得年份列表
    years = []
    for sp in spans[1:9]:  # 最多 8 年
        txt = sp.get_text(strip=True)
        try:
            west_year = int(txt)
            roc_year = west_year - 1911
            years.append(str(roc_year))
        except:
            years.append(None)

    if not years:
        return {}

    # 每 (1+len(years)) 個 span 一列，找「每股盈餘」（排除完全稀釋）
    cols = 1 + len(years)
    result = {}
    for i in range(0, len(spans), cols):
        row = spans[i:i+cols]
        if len(row) < cols:
            continue
        label = row[0].get_text(strip=True)
        if label == '每股盈餘':
            for j, yr in enumerate(years):
                if yr is None:
                    continue
                val = _parse_num(row[j + 1].get_text(strip=True))
                if val is not None:
                    result[yr] = val
            break

    return result


def fetch_capital_annual_eps_batch(codes):
    """批次抓取群益年度 EPS，回傳 {code: {民國年: eps}}
    用於年度 EPS 主要來源 + 公告期結束後批次驗證"""
    print(f"[群益年度EPS] 開始抓取 {len(codes)} 支...")
    t0 = time.time()
    result = {}

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {}
        for i, code in enumerate(codes):
            futures[pool.submit(fetch_capital_annual_eps, code)] = code
            if (i + 1) % 8 == 0:
                time.sleep(0.5)
        for f in as_completed(futures):
            code = futures[f]
            try:
                data = f.result()
                if data:
                    result[code] = data
            except:
                pass

    print(f"[群益年度EPS] 完成：{len(result)}/{len(codes)} 支有資料，耗時 {time.time()-t0:.1f}s")
    return result


# ── 月營收（群益 zch）────────────────────────────────────

def fetch_capital_monthly_revenue(code):
    """從群益抓取個股歷史月營收，存入 monthly_revenue"""
    try:
        url = f"https://stock.capital.com.tw/z/zc/zch/zch.djhtm?a={code}"
        r = _session.get(url, timeout=15)
        r.encoding = 'big5'
        soup = BeautifulSoup(r.text, 'html.parser')
    except:
        return 0

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 確保表存在
    c.execute("""CREATE TABLE IF NOT EXISTS monthly_revenue (
        code TEXT NOT NULL, year INTEGER NOT NULL, month INTEGER NOT NULL,
        revenue REAL, updated_at TEXT, PRIMARY KEY (code, year, month))""")

    saved = 0
    for t in soup.find_all('table'):
        for row in t.find_all('tr'):
            cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
            if not cells or not re.match(r'\d+/\d+', cells[0]):
                continue
            if len(cells) < 2:
                continue

            # 格式: "115/03", "12,412,837", "44.44%", ...
            ym = cells[0]
            m = re.match(r'(\d+)/(\d+)', ym)
            if not m:
                continue

            roc_year = int(m.group(1))
            month = int(m.group(2))
            west_year = roc_year + 1911
            revenue = _parse_num(cells[1])

            if revenue is None or revenue <= 0:
                continue

            # 群益單位是仟元，轉為元
            revenue *= 1000

            try:
                c.execute("""INSERT INTO monthly_revenue (code, year, month, revenue, updated_at)
                    VALUES (?,?,?,?,?)
                    ON CONFLICT(code, year, month) DO UPDATE SET
                    revenue=excluded.revenue, updated_at=excluded.updated_at""",
                    (code, west_year, month, revenue, now_str))
                saved += 1
            except:
                pass

    conn.commit()
    conn.close()
    return saved


# ── 四表一次抓取 ────────────────────────────────────────

# ── 歷史本益比（zca 基本資料）─────────────────────────────

def fetch_capital_pe_history(code):
    """從群益基本資料頁面抓取歷年最高/最低本益比"""
    try:
        url = f"https://stock.capital.com.tw/z/zc/zca/zca.djhtm?a={code}"
        r = _session.get(url, timeout=15)
        r.encoding = 'big5'
        soup = BeautifulSoup(r.text, 'html.parser')
    except:
        return 0

    years = []
    pe_highs = []
    pe_lows = []

    for t in soup.find_all('table'):
        for row in t.find_all('tr'):
            cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
            if not cells:
                continue
            if cells[0] == '年度' and len(cells) > 2:
                years = [c for c in cells[1:] if re.match(r'\d+', c)]
            elif cells[0] == '最高本益比' and len(cells) > 2:
                pe_highs = [_parse_num(c) for c in cells[1:1+len(years)]]
            elif cells[0] == '最低本益比' and len(cells) > 2:
                pe_lows = [_parse_num(c) for c in cells[1:1+len(years)]]

    if not years or not pe_highs or not pe_lows:
        return 0

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 確保表存在
    c.execute("""CREATE TABLE IF NOT EXISTS pe_history (
        code TEXT NOT NULL, year INTEGER NOT NULL,
        pe_high REAL, pe_low REAL, updated_at TEXT,
        PRIMARY KEY (code, year))""")

    saved = 0
    cur_year = datetime.now().year
    for i, yr_str in enumerate(years):
        yr = int(yr_str) + 1911  # 民國轉西曆
        if yr >= cur_year:
            continue  # 跳過當年（不完整）
        pe_h = pe_highs[i] if i < len(pe_highs) else None
        pe_l = pe_lows[i] if i < len(pe_lows) else None
        if pe_h is None or pe_l is None or pe_h <= 0 or pe_l <= 0:
            continue
        try:
            c.execute("""INSERT INTO pe_history (code, year, pe_high, pe_low, updated_at)
                VALUES (?,?,?,?,?)
                ON CONFLICT(code, year) DO UPDATE SET
                pe_high=excluded.pe_high, pe_low=excluded.pe_low, updated_at=excluded.updated_at""",
                (code, yr, pe_h, pe_l, now_str))
            saved += 1
        except:
            pass

    conn.commit()
    conn.close()
    return saved


def fetch_all_three(code):
    """一次抓取個股全部資料：損益表+資產負債表+現金流量表+股利+月營收+合約負債+本益比歷史"""
    a1, q1 = fetch_capital_financials(code)
    time.sleep(random.uniform(0.2, 0.4))
    a2 = fetch_capital_balance_sheet(code)
    time.sleep(random.uniform(0.2, 0.4))
    a3 = fetch_capital_cashflow(code)
    time.sleep(random.uniform(0.2, 0.4))
    a6 = fetch_capital_dividend(code)
    time.sleep(random.uniform(0.2, 0.4))
    a4 = fetch_capital_monthly_revenue(code)
    time.sleep(random.uniform(0.2, 0.4))
    a5 = fetch_capital_contract_liability(code)
    time.sleep(random.uniform(0.2, 0.4))
    a7 = fetch_capital_pe_history(code)
    return a1, q1, a2, a3, a4, a5


# ── 批次補齊全部股票 ────────────────────────────────────

def backfill_all(force=False):
    """
    批次補齊所有股票的三表資料。
    force=True: 全部重新抓取
    force=False: 只補缺 total_equity 或 operating_cf 的
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    if force:
        c.execute("SELECT code, name FROM stocks WHERE close IS NOT NULL ORDER BY code")
        need = c.fetchall()
    else:
        # 找缺 total_equity 或 operating_cf 的股票
        c.execute("""
            SELECT DISTINCT s.code, s.name FROM stocks s
            LEFT JOIN financial_annual fa ON s.code = fa.code AND fa.year >= 2020
            WHERE s.close IS NOT NULL
            GROUP BY s.code
            HAVING SUM(CASE WHEN fa.total_equity IS NOT NULL THEN 1 ELSE 0 END) < 3
                OR SUM(CASE WHEN fa.operating_cf IS NOT NULL THEN 1 ELSE 0 END) < 3
            ORDER BY s.code
        """)
        need = c.fetchall()
    conn.close()

    if not need:
        print("[群益三表] 所有股票已補齊")
        return

    print(f"[群益三表] 待補: {len(need)} 支")

    done = 0
    fail_streak = 0
    t0 = time.time()

    for code, name in need:
        try:
            a1, q1, a2, a3, a4, a5 = fetch_all_three(code)
            if a1 > 0 or q1 > 0 or a2 > 0 or a3 > 0 or a4 > 0 or a5 > 0:
                done += 1
                fail_streak = 0
            else:
                fail_streak += 1
        except Exception as e:
            fail_streak += 1

        if done % 100 == 0 and done > 0:
            elapsed = time.time() - t0
            rate = done / elapsed * 60
            print(f"  進度: {done}/{len(need)}（{rate:.0f} 支/分）")

        if fail_streak >= 100:
            print(f"  連續失敗 {fail_streak} 次，停止")
            break

        time.sleep(random.uniform(0.3, 0.8))

    elapsed = time.time() - t0
    print(f"[群益三表] 完成: {done}/{len(need)}，耗時 {elapsed:.0f} 秒")


if __name__ == "__main__":
    import sys
    force = '--force' in sys.argv
    backfill_all(force=force)
