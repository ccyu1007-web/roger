"""
capital_fetcher.py — 從群益證券（嘉實系統）抓取財務三表
免費、無額度限制、有完整歷史資料
季報：COALESCE 補空欄位 + 校驗 MOPS（差異>5%記 cross_validation，MOPS 異常才覆蓋）
年報/BS/CF/股利：補充來源，不覆蓋 MOPS 已有值

三表 URL：
  損益表(季): zce/zce_{code}.djhtm
  損益表(年): zcq/zcqa.djhtm?a={code}
  資產負債表(年): zcp/zcpb/zcpb.djhtm?a={code}
  資產負債表(季): zcp/zcpa/zcpa.djhtm?a={code}
  現金流量表(年): zc3/zc3a.djhtm?a={code}
  現金流量表(季): zc3/zc3.djhtm?a={code}
"""
import logging
import db as sqlite3
import time
import random
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)
from bs4 import BeautifulSoup
from fetcher_utils import (
    create_session, parse_num as _parse_num,
    fetch_page as _fetch_page_raw, DB_PATH
)

_session = create_session()

# 季度排序 SQL（數值排序，避免字串排序 "99Q4" > "114Q4" 的錯誤）
_Q_ORDER_DESC = """ORDER BY CAST(SUBSTR(quarter, 1, INSTR(quarter, 'Q') - 1) AS INTEGER) DESC,
                           CAST(SUBSTR(quarter, INSTR(quarter, 'Q') + 1) AS INTEGER) DESC"""


def _fetch_page(url):
    """抓取群益頁面並解析 table-cell"""
    return _fetch_page_raw(_session, url)


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


# ── 完整損益表（季表，zcq 格式，含所得稅/繼續營業/歸屬母公司）────────

# zcq 行標籤 → DB 欄位對照（用 startswith 匹配，避免全名不一致）
_ZCQ_ROW_MAP = {
    '營業收入淨額': 'revenue',
    '營業成本': 'cost',
    '營業毛利': 'gross_profit',
    '營業費用': 'operating_expense',
    '營業利益': 'operating_income',
    '營業外收入及支出': 'non_operating',
    '稅前淨利': 'pretax_income',
    '所得稅費用': 'tax',
    '繼續營業單位損益': 'continuing_income',
    '歸屬母公司淨利（損）': 'net_income_parent',
    '每股盈餘': 'eps',
    '加權平均股數': 'weighted_shares',
}


def fetch_capital_quarterly_full(code):
    """
    從群益 zcq/zcq.djhtm 抓取完整季損益表。
    包含所得稅費用、繼續營業單位損益、歸屬母公司淨利、加權平均股數。
    回傳更新筆數。
    """
    url = f"https://stock.capital.com.tw/z/zc/zcq/zcq.djhtm?a={code}"
    try:
        r = _session.get(url, timeout=15)
        r.encoding = 'big5'
    except Exception as e:
        logger.warning(f"[群益季報-zce] {code} 頁面抓取失敗: {e}")
        return 0

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(r.text, 'html.parser')
    cells = soup.find_all(class_=lambda x: x and 'table-cell' in x)
    texts = [c.get_text(strip=True) for c in cells if c.get_text(strip=True)]

    if len(texts) < 18:
        return 0

    # 解析期別（第一行 index 0~8）
    # texts[0] = '期別', texts[1]~[8] = '2025.4Q', '2025.3Q', ...
    quarters = []
    for i in range(1, 9):
        if i >= len(texts):
            break
        q_text = texts[i]  # e.g. "2025.4Q"
        m = re.match(r'(\d+)\.(\d+)Q', q_text)
        if m:
            west_year = int(m.group(1))
            quarter = int(m.group(2))
            roc_year = west_year - 1911
            quarters.append({'label': f"{roc_year}Q{quarter}", 'west_year': west_year})
        else:
            quarters.append(None)

    n_cols = len(quarters)
    if n_cols == 0:
        return 0

    # 解析各行資料
    row_size = n_cols + 1  # 1 label + n_cols data
    data_rows = {}  # {db_field: [val_q1, val_q2, ...]}

    for i in range(0, len(texts), row_size):
        if i >= len(texts):
            break
        label = texts[i]
        # 匹配行標籤
        matched_field = None
        for row_label, db_field in _ZCQ_ROW_MAP.items():
            if label == row_label or label.startswith(row_label):
                matched_field = db_field
                break
        if not matched_field:
            continue
        # 已經有更精確的匹配就跳過（避免「營業收入毛額」先匹配到「營業收入」）
        if matched_field in data_rows:
            continue

        vals = []
        for j in range(1, n_cols + 1):
            idx = i + j
            if idx < len(texts):
                vals.append(_parse_num(texts[idx]))
            else:
                vals.append(None)
        data_rows[matched_field] = vals

    # 防呆：如果解析出來全是 None，可能 HTML 結構變了
    all_none = all(
        all(v is None for v in vals)
        for vals in data_rows.values()
    )
    if data_rows and all_none:
        logger.warning(f"[群益季報] {code} 解析全空，HTML 結構可能已變更")
        return 0

    # 寫入 quarterly_financial
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        saved = 0
        mul = 1000000  # 群益單位百萬

        for qi, q_info in enumerate(quarters):
            if q_info is None:
                continue
            quarter_label = q_info['label']

            # 組合該季度的資料
            row_data = {}
            for field, vals in data_rows.items():
                if qi < len(vals) and vals[qi] is not None:
                    if field == 'eps':
                        row_data[field] = vals[qi]  # EPS 不乘百萬
                    elif field == 'weighted_shares':
                        row_data[field] = vals[qi] * 1000  # 加權股數單位是仟股，乘1000
                    else:
                        row_data[field] = vals[qi] * mul
                else:
                    row_data[field] = None

            # 至少要有 revenue 或 eps 才寫入
            if row_data.get('revenue') is None and row_data.get('eps') is None:
                continue

            # 判斷是否為最新一季（14天內 MOPS 寫入）→ 走校驗邏輯
            from datetime import timedelta
            existing = c.execute("""SELECT revenue, eps, updated_at
                FROM quarterly_financial WHERE code=? AND quarter=?""",
                (code, quarter_label)).fetchone()

            is_recent_mops = False
            if existing and existing[2]:
                try:
                    updated_dt = datetime.strptime(existing[2], '%Y-%m-%d %H:%M:%S')
                    if (datetime.now() - updated_dt).days <= 14:
                        is_recent_mops = True
                except Exception:
                    pass

            if not is_recent_mops:
                # 歷史季度：群益直接覆蓋（權威來源）
                c.execute("""INSERT INTO quarterly_financial
                    (code, quarter, revenue, cost, gross_profit, operating_expense,
                     operating_income, non_operating, pretax_income, tax,
                     continuing_income, net_income_parent, eps, weighted_shares, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(code, quarter) DO UPDATE SET
                    revenue=excluded.revenue,
                    cost=excluded.cost,
                    gross_profit=excluded.gross_profit,
                    operating_expense=excluded.operating_expense,
                    operating_income=excluded.operating_income,
                    non_operating=excluded.non_operating,
                    pretax_income=excluded.pretax_income,
                    tax=excluded.tax,
                    continuing_income=excluded.continuing_income,
                    net_income_parent=excluded.net_income_parent,
                    eps=excluded.eps,
                    weighted_shares=excluded.weighted_shares,
                    updated_at=excluded.updated_at""",
                    (code, quarter_label,
                     row_data.get('revenue'), row_data.get('cost'), row_data.get('gross_profit'),
                     row_data.get('operating_expense'), row_data.get('operating_income'),
                     row_data.get('non_operating'), row_data.get('pretax_income'),
                     row_data.get('tax'), row_data.get('continuing_income'),
                     row_data.get('net_income_parent'), row_data.get('eps'),
                     row_data.get('weighted_shares'), now_str))
            else:
                # 最新一季：校驗 MOPS，差異大才記錄，異常才覆蓋
                mops_rev = existing[0]
                mops_eps = existing[1]
                capital_rev = row_data.get('revenue')
                capital_eps = row_data.get('eps')
                capital_override = False

                if mops_rev is not None and capital_rev is not None and mops_rev != 0:
                    rev_diff_pct = abs(capital_rev - mops_rev) / abs(mops_rev) * 100
                    eps_diff = abs(capital_eps - mops_eps) if (capital_eps is not None and mops_eps is not None) else 0

                    if rev_diff_pct > 5 or eps_diff > 0.5:
                        import json
                        mismatch_detail = {
                            'type': 'quarterly_zcq_vs_mops',
                            'code': code, 'quarter': quarter_label,
                            'capital': {'revenue': capital_rev, 'eps': capital_eps},
                            'mops': {'revenue': mops_rev, 'eps': mops_eps},
                            'rev_diff_pct': round(rev_diff_pct, 2),
                            'eps_diff': round(eps_diff, 4) if eps_diff else 0
                        }
                        try:
                            c.execute("""CREATE TABLE IF NOT EXISTS cross_validation (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                checked_at TEXT, sample_size INTEGER, ok_count INTEGER,
                                mismatch_count INTEGER, details TEXT)""")
                            c.execute("""INSERT INTO cross_validation
                                (checked_at, sample_size, ok_count, mismatch_count, details)
                                VALUES (?,1,0,1,?)""",
                                (now_str, json.dumps(mismatch_detail, ensure_ascii=False)))
                        except Exception as e:
                            logger.debug(f"[zcq校驗] cross_validation 寫入失敗: {e}")
                        logger.warning(f"[zcq校驗] {code} {quarter_label} 差異大: 營收差{rev_diff_pct:.1f}%")

                        # MOPS 明顯異常才覆蓋
                        if mops_rev < 0 and capital_rev > 0:
                            capital_override = True
                        if mops_eps is not None and capital_eps is not None:
                            if mops_eps < 0 and capital_eps > 0 and abs(capital_eps) > 0.5:
                                capital_override = True

                if capital_override:
                    c.execute("""INSERT INTO quarterly_financial
                        (code, quarter, revenue, cost, gross_profit, operating_expense,
                         operating_income, non_operating, pretax_income, tax,
                         continuing_income, net_income_parent, eps, weighted_shares, updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(code, quarter) DO UPDATE SET
                        revenue=excluded.revenue, cost=excluded.cost,
                        gross_profit=excluded.gross_profit, operating_expense=excluded.operating_expense,
                        operating_income=excluded.operating_income, non_operating=excluded.non_operating,
                        pretax_income=excluded.pretax_income, tax=excluded.tax,
                        continuing_income=excluded.continuing_income, net_income_parent=excluded.net_income_parent,
                        eps=excluded.eps, weighted_shares=excluded.weighted_shares,
                        updated_at=excluded.updated_at""",
                        (code, quarter_label,
                         row_data.get('revenue'), row_data.get('cost'), row_data.get('gross_profit'),
                         row_data.get('operating_expense'), row_data.get('operating_income'),
                         row_data.get('non_operating'), row_data.get('pretax_income'),
                         row_data.get('tax'), row_data.get('continuing_income'),
                         row_data.get('net_income_parent'), row_data.get('eps'),
                         row_data.get('weighted_shares'), now_str))
                else:
                    # MOPS 正常：只補空欄位（tax/continuing_income/net_income_parent/weighted_shares）
                    c.execute("""UPDATE quarterly_financial SET
                        tax=COALESCE(tax, ?),
                        continuing_income=COALESCE(continuing_income, ?),
                        net_income_parent=COALESCE(net_income_parent, ?),
                        weighted_shares=COALESCE(weighted_shares, ?)
                        WHERE code=? AND quarter=?""",
                        (row_data.get('tax'), row_data.get('continuing_income'),
                         row_data.get('net_income_parent'), row_data.get('weighted_shares'),
                         code, quarter_label))
            saved += 1

        conn.commit()
    return saved


# ── 損益表（季表，用原本的 zce 格式 - 簡化版）────────────────────────

def fetch_capital_financials(code):
    """從群益抓取個股季度損益表（zce 簡化版），存入 financial_annual + quarterly_financial"""
    try:
        url = f"https://stock.capital.com.tw/z/zc/zce/zce_{code}.djhtm"
        r = _session.get(url, timeout=15)
        r.encoding = 'big5'
        soup = BeautifulSoup(r.text, 'html.parser')
    except Exception as e:
        logger.warning(f"[群益季報-zce] {code} 頁面抓取失敗: {e}")
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

    # 防呆：找到表格但沒有任何資料列（可能 HTML 結構變更）
    has_data_row = False
    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
        if len(cells) >= 10 and re.match(r'\d+\.\d+Q', cells[0]):
            # 檢查數值欄是否全空
            vals = [_parse_num(cells[j]) for j in range(1, min(11, len(cells)))]
            if any(v is not None for v in vals):
                has_data_row = True
                break
    if not has_data_row:
        logger.warning(f"[群益季報-zce] {code} 找到表格但無有效資料列，HTML 結構可能已變更")
        return 0, 0

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with sqlite3.get_conn() as conn:
        c = conn.cursor()

        quarterly_saved = 0

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

            # 群益損益表寫入策略：
            #   歷史季度（>14天）→ 群益直接覆蓋（群益是歷史資料權威來源）
            #   最新一季（≤14天內 MOPS 寫入）→ 走校驗邏輯（MOPS 即時優先，群益校驗）
            try:
                existing = c.execute("""SELECT revenue, eps, operating_income, pretax_income, updated_at
                    FROM quarterly_financial WHERE code=? AND quarter=?""",
                    (code, quarter_label)).fetchone()

                # 判斷是否為 MOPS 近期寫入的最新一季
                is_recent_mops = False
                if existing and existing[4]:
                    try:
                        from datetime import timedelta
                        updated_dt = datetime.strptime(existing[4], '%Y-%m-%d %H:%M:%S')
                        if (datetime.now() - updated_dt).days <= 14:
                            is_recent_mops = True
                    except Exception:
                        pass

                if not is_recent_mops:
                    # 歷史季度或無資料：群益直接覆蓋
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
                else:
                    # 最新一季（MOPS 14天內寫入）：校驗邏輯
                    mops_rev = existing[0]
                    mops_eps = existing[1]
                    capital_override = False

                    if mops_rev is not None and revenue is not None:
                        rev_diff_pct = abs(revenue - mops_rev) / abs(mops_rev) * 100 if mops_rev != 0 else 0
                        eps_diff = abs(eps - mops_eps) if (eps is not None and mops_eps is not None) else 0

                        if rev_diff_pct > 5 or eps_diff > 0.5:
                            # 差異大，記入 cross_validation
                            import json
                            mismatch_detail = {
                                'type': 'quarterly_capital_vs_mops',
                                'code': code, 'quarter': quarter_label,
                                'capital': {'revenue': revenue, 'eps': eps},
                                'mops': {'revenue': mops_rev, 'eps': mops_eps},
                                'rev_diff_pct': round(rev_diff_pct, 2),
                                'eps_diff': round(eps_diff, 4)
                            }
                            try:
                                c.execute("""CREATE TABLE IF NOT EXISTS cross_validation (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    checked_at TEXT, sample_size INTEGER, ok_count INTEGER,
                                    mismatch_count INTEGER, details TEXT)""")
                                c.execute("""INSERT INTO cross_validation
                                    (checked_at, sample_size, ok_count, mismatch_count, details)
                                    VALUES (?,1,0,1,?)""",
                                    (now_str, json.dumps(mismatch_detail, ensure_ascii=False)))
                            except Exception as e:
                                logger.debug(f"[校驗] cross_validation 寫入失敗: {e}")
                            logger.warning(f"[校驗] {code} {quarter_label} 群益vs MOPS 差異大: "
                                           f"營收差{rev_diff_pct:.1f}% EPS差{eps_diff:.4f}")

                            # MOPS 明顯異常才覆蓋
                            if mops_rev < 0 and revenue > 0:
                                capital_override = True
                            if mops_eps is not None and eps is not None:
                                if mops_eps < 0 and eps > 0 and abs(eps) > 0.5:
                                    capital_override = True
                            if capital_override:
                                logger.warning(f"[校驗] {code} {quarter_label} MOPS 明顯異常，以群益資料覆蓋")

                    if capital_override:
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
                    else:
                        # MOPS 正常，只補空欄位
                        c.execute("""INSERT INTO quarterly_financial
                            (code, quarter, revenue, cost, gross_profit, operating_expense,
                             operating_income, non_operating, pretax_income, net_income_parent, eps, updated_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                            ON CONFLICT(code, quarter) DO UPDATE SET
                            revenue=COALESCE(quarterly_financial.revenue, excluded.revenue),
                            cost=COALESCE(quarterly_financial.cost, excluded.cost),
                            gross_profit=COALESCE(quarterly_financial.gross_profit, excluded.gross_profit),
                            operating_expense=COALESCE(quarterly_financial.operating_expense, excluded.operating_expense),
                            operating_income=COALESCE(quarterly_financial.operating_income, excluded.operating_income),
                            non_operating=COALESCE(quarterly_financial.non_operating, excluded.non_operating),
                            pretax_income=COALESCE(quarterly_financial.pretax_income, excluded.pretax_income),
                            net_income_parent=COALESCE(quarterly_financial.net_income_parent, excluded.net_income_parent),
                            eps=COALESCE(quarterly_financial.eps, excluded.eps),
                            updated_at=excluded.updated_at""",
                            (code, quarter_label, revenue, cost, gross_profit, opex, operating_income,
                             non_operating, pretax_income, net_income, eps, now_str))
                quarterly_saved += 1
            except Exception as e:
                logger.warning(f"[群益季報] {code} {quarter_label} 寫入失敗: {e}")

            # 年度損益表數據由 fetch_capital_annual_eps()（群益 zcqa）直接抓取寫入，
            # 不再從季度加總（季度加總的 EPS/營收在增資或四捨五入時會失真）

        conn.commit()

    # 同步到 stocks 表
    if quarterly_saved > 0:
        sync_to_stocks(code)

    return 0, quarterly_saved


# ── 資產負債表（年表）────────────────────────────────────

def fetch_capital_balance_sheet(code):
    """從群益抓取年度資產負債表，補寫 total_assets / total_equity / common_stock / inventory / contract_liability
       + ROIC 所需欄位：cash_and_equivalents / short_term_debt / short_term_notes /
         current_long_term_debt / long_term_bank_debt / other_long_term_debt / bonds_payable"""
    url = f"https://stock.capital.com.tw/z/zc/zcp/zcpb/zcpb.djhtm?a={code}"
    texts = _fetch_page(url)
    if not texts:
        return 0
    if len(texts) < 5:
        logger.warning(f"[群益BS] {code} 頁面內容不足（{len(texts)} 個 cell），可能被擋或結構變更")
        return 0

    row_labels = {
        '資產總額': 'total_assets',
        '股東權益總額': 'total_equity',
        '股本': 'common_stock',
        '存貨': 'inventory',
        '合約負債－流動': 'contract_liability',
        '流動負債': 'current_liabilities',
        # ROIC 所需欄位
        '應收帳款及票據': 'accounts_receivable',
        '現金及約當現金': 'cash_and_equivalents',
        '短期借款': 'short_term_debt',
        '應付商業本票／承兌匯票': 'short_term_notes',
        '一年內到期長期負債': 'current_long_term_debt',
        '銀行借款－非流動': 'long_term_bank_debt',
        '其他長期借款－非流動': 'other_long_term_debt',
        '應付公司債－非流動': 'bonds_payable',
    }
    data = _extract_yearly_data(texts, row_labels)
    if not data:
        return 0

    # ROIC 新增欄位清單
    roic_cols = ['cash_and_equivalents', 'short_term_debt', 'short_term_notes',
                 'current_long_term_debt', 'long_term_bank_debt', 'other_long_term_debt',
                 'bonds_payable', 'current_liabilities']
    all_extra_cols = ['inventory', 'contract_liability', 'accounts_receivable'] + roic_cols

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        # 確保欄位存在
        for col in all_extra_cols:
            try: c.execute(f"ALTER TABLE financial_annual ADD COLUMN {col} REAL")
            except Exception: pass
        mul = 1000000  # 百萬 → 元

        # 所有需要寫入的欄位（含基本 + ROIC）
        all_fields = ['total_assets', 'total_equity', 'common_stock',
                      'inventory', 'contract_liability', 'accounts_receivable'] + roic_cols

        saved = 0
        for year_str, fields in data.items():
            yr = int(float(year_str))
            vals = {}
            for f in all_fields:
                v = fields.get(f)
                vals[f] = v * mul if v is not None else None

            if vals['total_assets'] is None and vals['total_equity'] is None:
                continue

            # 動態組 SQL
            col_names = ', '.join(all_fields)
            placeholders = ', '.join(['?'] * len(all_fields))
            coalesce_parts = ', '.join(f'{f}=COALESCE(excluded.{f}, {f})' for f in all_fields)

            try:
                c.execute(f"""INSERT INTO financial_annual (code, year, {col_names}, updated_at)
                    VALUES (?,?,{placeholders},?)
                    ON CONFLICT(code, year) DO UPDATE SET
                    {coalesce_parts},
                    updated_at=excluded.updated_at""",
                    [code, yr] + [vals[f] for f in all_fields] + [now_str])
                saved += 1
            except Exception as e: logger.debug(f"[群益BS] {code} {yr} 寫入失敗: {e}")

        conn.commit()
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
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        # 確保欄位存在
        try: c.execute("ALTER TABLE quarterly_financial ADD COLUMN inventory REAL")
        except Exception: pass
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
            except Exception as e: logger.debug(f"[群益季BS] {code} 寫入失敗: {e}")

        conn.commit()
    return saved


# ── 股利政策（zcc）────────────────────────────────────────

def fetch_capital_dividend(code):
    """從群益抓取歷年股利，寫入 financial_annual 的 cash_dividend / stock_dividend"""
    try:
        url = f"https://stock.capital.com.tw/z/zc/zcc/zcc.djhtm?a={code}"
        r = _session.get(url, timeout=15)
        r.encoding = 'big5'
        soup = BeautifulSoup(r.text, 'html.parser')
    except Exception as e:
        logger.warning(f"[群益股利] {code} 頁面抓取失敗: {e}")
        return 0

    tds = soup.find_all('td', class_=re.compile(r't3n[01]'))
    texts = [td.get_text(strip=True) for td in tds]
    if len(texts) < 5:
        logger.warning(f"[群益股利] {code} 頁面內容不足（{len(texts)} 個 cell），可能被擋或結構變更")
        return 0

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with sqlite3.get_conn() as conn:
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
                    except Exception as e: logger.debug(f"[群益股利] {code} {year} 寫入失敗: {e}")
                i += 9
            else:
                i += 1

        # 自動同步到 stocks 表的 div_c1~c6（不再依賴 scraper 的月份限制）
        if saved > 0:
            rows = c.execute("""SELECT year, cash_dividend, stock_dividend FROM financial_annual
                               WHERE code=? AND (cash_dividend IS NOT NULL OR stock_dividend IS NOT NULL)
                               ORDER BY year DESC LIMIT 6""", (code,)).fetchall()
            for i, r in enumerate(rows, 1):
                roc_yr = str(r[0] - 1911)
                c.execute(f"UPDATE stocks SET div_c{i}=?, div_s{i}=?, div_{i}_label=? WHERE code=?",
                          (r[1], r[2], roc_yr, code))
            for i in range(len(rows) + 1, 7):
                c.execute(f"UPDATE stocks SET div_c{i}=NULL, div_s{i}=NULL, div_{i}_label=NULL WHERE code=?",
                          (code,))

        # financial_annual + stocks 一次 commit（原子操作）
        conn.commit()
    return saved


# ── 現金流量表（年表）────────────────────────────────────

def fetch_capital_cashflow(code):
    """從群益抓取年度現金流量表，補寫 operating_cf / capex"""
    url = f"https://stock.capital.com.tw/z/zc/zc3/zc3a.djhtm?a={code}"
    texts = _fetch_page(url)
    if not texts:
        return 0
    if len(texts) < 5:
        logger.warning(f"[群益CF] {code} 頁面內容不足（{len(texts)} 個 cell），可能被擋或結構變更")
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
    with sqlite3.get_conn() as conn:
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
            except Exception as e: logger.debug(f"[群益CF] {code} {yr} 寫入失敗: {e}")

        conn.commit()
    return saved


# ── 年度損益表（群益 zcqa）── 年度 EPS 最優先來源 ─────────

def fetch_capital_annual_eps(code):
    """從群益年度損益表(zcqa)抓取完整年度損益數據，只寫入 DB 缺的年度。
    回傳 {民國年: eps}。"""
    try:
        url = f"https://stock.capital.com.tw/z/zc/zcq/zcqa.djhtm?a={code}"
        r = _session.get(url, timeout=15)
        r.encoding = 'big5'
        soup = BeautifulSoup(r.text, 'html.parser')
    except Exception as e:
        logger.warning(f"[群益年度EPS] {code} 頁面抓取失敗: {e}")
        return {}

    spans = soup.find_all('span', class_=lambda c: c and 'table-cell' in c)
    if not spans:
        return {}

    # 第一列是「期別, 2025, 2024, ...」，取得年份列表
    years = []
    for sp in spans[1:9]:
        txt = sp.get_text(strip=True)
        try:
            years.append(str(int(txt) - 1911))
        except Exception:
            years.append(None)

    if not years:
        return {}

    # zcqa 欄位 → DB 欄位對照
    LABEL_MAP = {
        '營業收入淨額': 'revenue',
        '營業成本':     'cost',
        '營業毛利':     'gross_profit',
        '營業費用':     'operating_expense',
        '營業利益':     'operating_income',
        '營業外收入及支出': 'non_operating',
        '稅前淨利':     'pretax_income',
        '所得稅費用':   'tax',
        '繼續營業單位損益': 'net_income',
        '歸屬母公司淨利（損）': 'net_income_parent',
        '每股盈餘':     'eps',
        '加權平均股數': 'weighted_shares',
    }
    NO_MUL_FIELDS = {'eps', 'weighted_shares'}

    # 解析所有列
    cols = 1 + len(years)
    field_data = {}
    for i in range(0, len(spans), cols):
        row = spans[i:i+cols]
        if len(row) < cols:
            continue
        label = row[0].get_text(strip=True)
        db_field = LABEL_MAP.get(label)
        if not db_field:
            continue
        for j, yr in enumerate(years):
            if yr is None:
                continue
            val = _parse_num(row[j + 1].get_text(strip=True))
            if val is not None:
                field_data.setdefault(db_field, {})[yr] = val

    result = field_data.get('eps', {})
    if not field_data:
        return result

    # 寫入 financial_annual（所有年度都寫，確保資料正確）
    # 效率控制在 batch 層（只抓缺最新年度的股票），不在這裡跳年度
    try:
        with sqlite3.get_conn() as conn:
            c = conn.cursor()

            all_years = set()
            for m in field_data.values():
                all_years.update(m.keys())

            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            mul = 1000000

            for yr in all_years:
                west_year = int(yr) + 1911
                cols_list = ['code', 'year']
                vals = [code, west_year]

                for db_field, yr_map in field_data.items():
                    if yr not in yr_map:
                        continue
                    val = yr_map[yr]
                    if db_field == 'weighted_shares':
                        val = val * 1000
                    elif db_field not in NO_MUL_FIELDS:
                        val = val * mul
                    cols_list.append(db_field)
                    vals.append(val)

                # 衍生欄位
                eps_val = field_data.get('eps', {}).get(yr)
                oi_val = field_data.get('operating_income', {}).get(yr)
                pt_val = field_data.get('pretax_income', {}).get(yr)
                if eps_val is not None and oi_val is not None and pt_val and pt_val != 0:
                    eps_core = round(oi_val / pt_val * eps_val, 2)
                    eps_nonop = round(eps_val - eps_core, 2)
                    cols_list.extend(['eps_core', 'eps_nonop'])
                    vals.extend([eps_core, eps_nonop])

                cols_list.append('updated_at')
                vals.append(now_str)

                placeholders = ','.join('?' * len(cols_list))
                col_names = ','.join(cols_list)
                # INSERT 新年度，或 UPDATE 已存在但缺損益表的年度
                update_parts = [f"{c}=excluded.{c}" for c in cols_list if c not in ('code', 'year')]
                c.execute(f"""INSERT INTO financial_annual ({col_names}) VALUES ({placeholders})
                    ON CONFLICT(code, year) DO UPDATE SET {','.join(update_parts)}""", vals)

            conn.commit()
    except Exception as e:
        logger.debug(f"[群益年度] {code} 寫入失敗: {e}")

    return result


def fetch_capital_annual_eps_batch(codes):
    """批次抓取群益年度 EPS，回傳 {code: {民國年: eps}}
    用於年度 EPS 主要來源 + 公告期結束後批次驗證。
    非公告期（5~12月）只抓 DB 缺年度EPS的股票，避免每天重複抓不會變的資料。
    """
    from datetime import date as _date
    month = _date.today().month

    # 公告期（1~4月）：全量抓取，確保新年報資料完整
    # 非公告期（5~12月）：只抓 DB 缺最新年度 EPS 的股票
    if month >= 5:
        import db as _db
        latest_year = str(_date.today().year - 1911 - 1)  # 如 2026 → 114
        with _db.get_conn() as conn:
            existing = set()
            for r in conn.execute(
                "SELECT code FROM stocks WHERE eps_y1_label = ?", (latest_year,)
            ).fetchall():
                existing.add(r[0])
        need_codes = [c for c in codes if c not in existing]
        if not need_codes:
            print(f"[群益年度EPS] 非公告期，DB 已有 {len(existing)} 支最新年度({latest_year})，跳過")
            return {}
        print(f"[群益年度EPS] 非公告期，{len(existing)} 支已有最新，只抓 {len(need_codes)} 支缺的")
        codes = need_codes

    print(f"[群益年度EPS] 開始抓取 {len(codes)} 支...")
    t0 = time.time()
    result = {}

    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = {}
        for i, code in enumerate(codes):
            futures[pool.submit(fetch_capital_annual_eps, code)] = code
            if (i + 1) % 16 == 0:
                time.sleep(0.3)
        for f in as_completed(futures):
            code = futures[f]
            try:
                data = f.result()
                if data:
                    result[code] = data
            except Exception as e: logger.debug(f"[群益批次] {code} 失敗: {e}")

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
    except Exception as e:
        logger.warning(f"[群益月營收] {code} 頁面抓取失敗: {e}")
        return 0

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with sqlite3.get_conn() as conn:
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
                except Exception as e: logger.debug(f"[群益月營收] {code} {west_year}/{month} 寫入失敗: {e}")

        # 更新 stocks 表的營收日期（取 monthly_revenue 中最新月份）
        if saved > 0:
            try:
                with sqlite3.get_conn() as conn2:
                    latest = conn2.execute(
                        "SELECT year, month FROM monthly_revenue WHERE code=? ORDER BY year DESC, month DESC LIMIT 1",
                        (code,)).fetchone()
                    if latest:
                        old = conn2.execute(
                            "SELECT revenue_year, revenue_month FROM stocks WHERE code=?", (code,)).fetchone()
                        if old and (latest[0] > (old[0] or 0) or (latest[0] == (old[0] or 0) and latest[1] > (old[1] or 0))):
                            conn2.execute(
                                "UPDATE stocks SET revenue_date=?, revenue_year=?, revenue_month=? WHERE code=?",
                                (now_str[:10], latest[0], latest[1], code))
                            conn2.commit()
            except Exception as e:
                logger.debug(f"[群益月營收] {code} stocks更新失敗: {e}")

        conn.commit()
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
    except Exception as e:
        logger.warning(f"[群益PE] {code} 頁面抓取失敗: {e}")
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
    with sqlite3.get_conn() as conn:
        c = conn.cursor()

        # 確保表存在
        c.execute("""CREATE TABLE IF NOT EXISTS pe_history (
            code TEXT NOT NULL, year INTEGER NOT NULL,
            pe_high REAL, pe_low REAL, updated_at TEXT,
            PRIMARY KEY (code, year))""")

        saved = 0
        for i, yr_str in enumerate(years):
            yr = int(yr_str) + 1911  # 民國轉西曆
            pe_h = pe_highs[i] if i < len(pe_highs) else None
            pe_l = pe_lows[i] if i < len(pe_lows) else None
            # 0 代表該年有虧損期間，視為無效
            if pe_h is not None and pe_h <= 0:
                pe_h = None
            if pe_l is not None and pe_l <= 0:
                pe_l = None
            # 高低至少要有一個有效值
            if pe_h is None and pe_l is None:
                continue
            try:
                c.execute("""INSERT INTO pe_history (code, year, pe_high, pe_low, updated_at)
                    VALUES (?,?,?,?,?)
                    ON CONFLICT(code, year) DO UPDATE SET
                    pe_high=COALESCE(excluded.pe_high, pe_high),
                    pe_low=COALESCE(excluded.pe_low, pe_low),
                    updated_at=excluded.updated_at""",
                    (code, yr, pe_h, pe_l, now_str))
                saved += 1
            except Exception as e: logger.debug(f"[群益PE] {code} {yr} 寫入失敗: {e}")

        conn.commit()
    return saved


def sync_to_stocks(code):
    """將 financial_annual + quarterly_financial 的資料同步到 stocks 表（原子操作）"""
    from datetime import date as _date
    max_year = _date.today().year - 1  # 年度上限：當年-1（排除未來年度）
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        try:
            # 1. 年度EPS（eps_y1~y6）— 從 financial_annual 取最近6年有EPS的
            rows = c.execute("""SELECT year, eps FROM financial_annual
                               WHERE code=? AND eps IS NOT NULL AND year <= ?
                               ORDER BY year DESC LIMIT 6""", (code, max_year)).fetchall()
            for i, r in enumerate(rows, 1):
                roc_yr = str(r[0] - 1911)
                c.execute(f"UPDATE stocks SET eps_y{i}=?, eps_y{i}_label=? WHERE code=?",
                          (r[1], roc_yr, code))
            for i in range(len(rows) + 1, 7):
                c.execute(f"UPDATE stocks SET eps_y{i}=NULL, eps_y{i}_label=NULL WHERE code=?", (code,))

            # 2. 股利（div_c1~c6）— 從 financial_annual 取最近6年有股利的
            rows = c.execute("""SELECT year, cash_dividend, stock_dividend FROM financial_annual
                               WHERE code=? AND (cash_dividend IS NOT NULL OR stock_dividend IS NOT NULL)
                               AND year <= ?
                               ORDER BY year DESC LIMIT 6""", (code, max_year)).fetchall()
            for i, r in enumerate(rows, 1):
                roc_yr = str(r[0] - 1911)
                c.execute(f"UPDATE stocks SET div_c{i}=?, div_s{i}=?, div_{i}_label=? WHERE code=?",
                          (r[1], r[2], roc_yr, code))
            for i in range(len(rows) + 1, 7):
                c.execute(f"UPDATE stocks SET div_c{i}=NULL, div_s{i}=NULL, div_{i}_label=NULL WHERE code=?",
                          (code,))

            # 3. 季度EPS（eps_1~5）— 從 quarterly_financial 取最近5季
            rows = c.execute(f"""SELECT quarter, eps FROM quarterly_financial
                               WHERE code=? AND eps IS NOT NULL
                               {_Q_ORDER_DESC} LIMIT 5""", (code,)).fetchall()
            for i, r in enumerate(rows, 1):
                c.execute(f"UPDATE stocks SET eps_{i}=?, eps_{i}q=? WHERE code=?",
                          (r[1], r[0], code))
            for i in range(len(rows) + 1, 6):
                c.execute(f"UPDATE stocks SET eps_{i}=NULL, eps_{i}q=NULL WHERE code=?", (code,))
            # eps_date 不在這裡更新 — 由政府 API（quick_update）第一次偵測到新季度時設定

            # 4. 合約負債（contract_1~3）— 從 quarterly_financial 取最近3季
            rows = c.execute(f"""SELECT quarter, contract_liability FROM quarterly_financial
                               WHERE code=? AND contract_liability IS NOT NULL
                               {_Q_ORDER_DESC} LIMIT 3""", (code,)).fetchall()
            for i, r in enumerate(rows, 1):
                c.execute(f"UPDATE stocks SET contract_{i}=?, contract_{i}q=? WHERE code=?",
                          (r[1], r[0], code))

            # 5. 近四季EPS合計
            eps_rows = c.execute(f"""SELECT eps FROM quarterly_financial
                                   WHERE code=? AND eps IS NOT NULL
                                   {_Q_ORDER_DESC} LIMIT 4""", (code,)).fetchall()
            if len(eps_rows) == 4:
                ytd = round(sum(r[0] for r in eps_rows), 2)
                latest_q = c.execute(f"""SELECT quarter FROM quarterly_financial
                                       WHERE code=? AND eps IS NOT NULL
                                       {_Q_ORDER_DESC} LIMIT 1""", (code,)).fetchone()
                ytd_label = latest_q[0].split('Q')[0] if latest_q else None
                c.execute("UPDATE stocks SET eps_ytd=?, eps_ytd_label=? WHERE code=?",
                          (ytd, ytd_label, code))

            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.warning(f"[sync_to_stocks] {code} 同步失敗，已回滾: {e}")


def fetch_all_three(code):
    """一次抓取個股全部資料：損益表+資產負債表+現金流量表+股利+月營收+合約負債+本益比歷史"""
    q1 = fetch_capital_quarterly_full(code)
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

    # 全部抓完後，統一同步到 stocks 表
    sync_to_stocks(code)

    return 0, q1, a2, a3, a4, a5


# ── 批次補齊全部股票 ────────────────────────────────────

def backfill_all(force=False):
    """
    批次補齊所有股票的三表資料。
    force=True: 全部重新抓取
    force=False: 只補缺 total_equity 或 operating_cf 的
    """
    with sqlite3.get_conn() as conn:
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
