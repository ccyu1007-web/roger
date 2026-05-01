"""
MOPS 公開資訊觀測站資料抓取（第一優先來源）
- 月營收：t21sc03（即時，比 t187ap05 快）
- 季度綜合損益表：t163sb04（累積值，需反算單季）
"""

import sqlite3
import requests
import re
import time
from datetime import date, datetime
from bs4 import BeautifulSoup

DB_PATH = "stocks.db"

_session = requests.Session()
_session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Referer': 'https://mopsov.twse.com.tw/mops/web/t163sb04',
})


def _safe_float(s):
    """解析數值，處理逗號和特殊字元"""
    if s is None:
        return None
    s = str(s).strip().replace(',', '')
    if s in ('', '--', '-', 'N/A', 'NA', '不適用', '－'):
        return None
    # 處理括號表示負數 (1,234) → -1234
    neg = False
    if s.startswith('(') and s.endswith(')'):
        s = s[1:-1]
        neg = True
    try:
        v = float(s)
        return -v if neg else v
    except ValueError:
        return None


# ══════════════════════════════════════════════════════════════
# 月營收（MOPS t21sc03）
# ══════════════════════════════════════════════════════════════

def fetch_mops_monthly_revenue(roc_year=None, month=None):
    """
    從 MOPS 抓取月營收（上市+上櫃）
    比 t187ap05 API 更即時，公司申報後立即可見
    回傳更新筆數
    """
    if roc_year is None or month is None:
        # 預設抓上個月（每月1~10日公布上月營收）
        today = date.today()
        if today.month > 1:
            month = today.month - 1
            roc_year = today.year - 1911
        else:
            month = 12
            roc_year = today.year - 1911 - 1

    west_year = roc_year + 1911
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    today_str = date.today().strftime('%Y-%m-%d')
    total = 0

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # 確保表存在
    c.execute("""CREATE TABLE IF NOT EXISTS monthly_revenue (
        code TEXT NOT NULL, year INTEGER NOT NULL, month INTEGER NOT NULL,
        revenue REAL, updated_at TEXT, PRIMARY KEY (code, year, month))""")

    for mtype, mpath in [('上市', 'sii'), ('上櫃', 'otc')]:
        try:
            url = f"https://mopsov.twse.com.tw/nas/t21/{mpath}/t21sc03_{roc_year}_{month}_0.html"
            r = _session.get(url, timeout=15)
            r.encoding = 'big5'
            soup = BeautifulSoup(r.text, 'html.parser')

            cnt = 0
            for tr in soup.find_all('tr'):
                tds = tr.find_all(['td', 'th'])
                texts = [td.get_text(strip=True) for td in tds]
                if len(texts) < 8 or not texts[0].isdigit() or len(texts[0]) != 4:
                    continue

                code = texts[0]
                rev = _safe_float(texts[2])  # 當月營收（千元）
                if rev is None or rev <= 0:
                    continue
                rev_val = rev * 1000  # 千元 → 元

                mom = _safe_float(texts[5])   # 上月比較增減(%)
                yoy = _safe_float(texts[6])   # 去年同月增減(%)
                cum_yoy = _safe_float(texts[9]) if len(texts) > 9 else None  # 累計前期比較增減(%)

                # 寫 monthly_revenue（MOPS 最高優先，直接覆蓋）
                c.execute("""INSERT INTO monthly_revenue (code, year, month, revenue, updated_at)
                    VALUES (?,?,?,?,?) ON CONFLICT(code, year, month) DO UPDATE SET
                    revenue=excluded.revenue, updated_at=excluded.updated_at""",
                    (code, west_year, month, rev_val, now_str))

                # 更新 stocks 表的營收日期
                old = c.execute("SELECT revenue_year, revenue_month FROM stocks WHERE code=?", (code,)).fetchone()
                if old and (west_year > (old[0] or 0) or (west_year == (old[0] or 0) and month > (old[1] or 0))):
                    c.execute("""UPDATE stocks SET revenue_date=?, revenue_year=?, revenue_month=?,
                        revenue_yoy=?, revenue_mom=?, revenue_cum_yoy=? WHERE code=?""",
                        (today_str, west_year, month, yoy, mom, cum_yoy, code))

                cnt += 1

            total += cnt
            if cnt:
                print(f"  [MOPS營收-{mtype}] {cnt} 筆（{roc_year}年{month}月）")
        except Exception as e:
            print(f"  [MOPS營收-{mtype}] 失敗: {e}")

    conn.commit()
    conn.close()
    return total


# ══════════════════════════════════════════════════════════════
# 季度綜合損益表（MOPS t163sb04）
# ══════════════════════════════════════════════════════════════

def _parse_mops_quarterly_table(soup):
    """
    解析 MOPS 綜合損益表 HTML，回傳 [{code, name, revenue, cost, ...}, ...]
    注意：MOPS 有多張表（金融/一般/保險/證券），欄位不同
    一般行業在 Table 3，欄位：營業收入/營業成本/營業毛利/營業費用/營業利益/業外/稅前/稅/淨利/EPS
    """
    results = []
    tables = soup.find_all('table')

    for t in tables:
        rows = t.find_all('tr')
        if len(rows) < 3:
            continue

        # 找標題行（一般行業 or 金融業）
        header_row = None
        is_financial = False
        for row in rows[:3]:
            cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
            if any('營業收入' in c for c in cells) and any('營業成本' in c for c in cells):
                header_row = cells
                break
            # 金融業（銀行/金控/保險/證券）：沒有營業收入/營業成本，改用其他欄位
            if any('公司代號' in c for c in cells) and any('基本每股盈餘' in c for c in cells):
                if any('利息' in c or '淨收益' in c or '收入' == c for c in cells):
                    header_row = cells
                    is_financial = True
                    break

        if not header_row:
            continue

        # 建立欄位索引
        col_map = {}
        for i, h in enumerate(header_row):
            if '公司代號' in h or h == '公司代號':
                col_map['code'] = i
            elif '公司名稱' in h:
                col_map['name'] = i
            elif h == '營業收入':
                col_map['revenue'] = i
            elif h == '營業成本':
                col_map['cost'] = i
            elif '營業毛利' in h and '淨額' not in h:
                col_map['gross_profit'] = i
            elif h == '營業費用':
                col_map['operating_expense'] = i
            elif '營業利益' in h:
                col_map['operating_income'] = i
            elif '營業外' in h:
                col_map['non_operating'] = i
            elif '稅前' in h and ('淨利' in h or '純益' in h or '損益' in h):
                col_map['pretax_income'] = i
            elif '所得稅' in h:
                col_map['tax'] = i
            elif '繼續營業' in h and '稅後' in h:
                col_map['continuing_income'] = i
            elif h.startswith('本期') and ('淨利' in h or '淨損' in h or '純益' in h):
                col_map['net_income'] = i
            elif '歸屬於母公司' in h and '淨利' in h:
                col_map['net_income_parent'] = i
            elif '基本每股盈餘' in h:
                col_map['eps'] = i

        # 一般行業需要 revenue，金融業只需要 code + eps
        if 'code' not in col_map:
            continue
        if not is_financial and 'revenue' not in col_map:
            continue

        # 解析資料行
        for row in rows:
            cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
            ci = col_map.get('code', 0)
            if ci >= len(cells) or not cells[ci].isdigit() or len(cells[ci]) != 4:
                continue

            def _get(key):
                idx = col_map.get(key)
                if idx is None or idx >= len(cells):
                    return None
                return _safe_float(cells[idx])

            record = {
                'code': cells[ci],
                'revenue': _get('revenue'),
                'cost': _get('cost'),
                'gross_profit': _get('gross_profit'),
                'operating_expense': _get('operating_expense'),
                'operating_income': _get('operating_income'),
                'non_operating': _get('non_operating'),
                'pretax_income': _get('pretax_income'),
                'tax': _get('tax'),
                'continuing_income': _get('continuing_income'),
                'net_income_parent': _get('net_income_parent'),
                'eps': _get('eps'),
            }
            # net_income_parent 可能在不同欄位名
            if record['net_income_parent'] is None:
                record['net_income_parent'] = _get('net_income')

            results.append(record)

    return results


def fetch_mops_quarterly(roc_year, season):
    """
    從 MOPS 抓取指定年度季度的綜合損益表（上市+上櫃）
    回傳：累積值的 list（尚未反算單季）
    """
    all_records = []

    for mtype, typek in [('上市', 'sii'), ('上櫃', 'otc')]:
        try:
            url = 'https://mopsov.twse.com.tw/mops/web/ajax_t163sb04'
            payload = {
                'encodeURIComponent': 1,
                'step': 1,
                'firstin': 1,
                'off': 1,
                'TYPEK': typek,
                'year': roc_year,
                'season': season,
            }
            r = _session.post(url, data=payload, timeout=30)
            r.encoding = 'utf-8'

            if r.status_code != 200:
                print(f"  [MOPS季報-{mtype}] HTTP {r.status_code}")
                continue

            soup = BeautifulSoup(r.text, 'html.parser')
            records = _parse_mops_quarterly_table(soup)
            all_records.extend(records)
            print(f"  [MOPS季報-{mtype}] {len(records)} 筆（{roc_year}Q{season}）")

            time.sleep(1)  # 禮貌延遲

        except Exception as e:
            print(f"  [MOPS季報-{mtype}] 失敗: {e}")

    return all_records


def _subtract_records(current, previous):
    """
    用累積值反算單季：current(累積) - previous(累積) = 單季
    current 和 previous 都是 dict，key 對應損益表欄位
    """
    result = {'code': current['code']}
    fields = ['revenue', 'cost', 'gross_profit', 'operating_expense',
              'operating_income', 'non_operating', 'pretax_income',
              'tax', 'continuing_income', 'net_income_parent']

    for f in fields:
        cv = current.get(f)
        pv = previous.get(f) if previous else None
        if cv is not None and pv is not None:
            result[f] = cv - pv
        elif cv is not None:
            result[f] = cv
        else:
            result[f] = None

    # EPS 也要反算（累積EPS - 前季累積EPS = 單季EPS）
    c_eps = current.get('eps')
    p_eps = previous.get('eps') if previous else None
    if c_eps is not None and p_eps is not None:
        result['eps'] = round(c_eps - p_eps, 4)
    elif c_eps is not None:
        result['eps'] = c_eps
    else:
        result['eps'] = None

    return result


def fetch_and_save_mops_quarterly(roc_year, season):
    """
    從 MOPS 抓取季報並存入 DB（自動處理累積→單季轉換）
    MOPS 是最高優先來源，直接覆蓋
    回傳更新筆數
    """
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    quarter_label = f"{roc_year}Q{season}"

    # 抓當季累積值
    current_records = fetch_mops_quarterly(roc_year, season)
    if not current_records:
        print(f"[MOPS季報] {quarter_label} 無資料")
        return 0

    # Q1 直接用，Q2~Q4 需要前一季累積值來反算
    prev_records_map = {}
    if season > 1:
        prev = fetch_mops_quarterly(roc_year, season - 1)
        prev_records_map = {r['code']: r for r in prev}

    # 反算單季
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    saved = 0

    for rec in current_records:
        code = rec['code']

        if season == 1:
            single = rec.copy()
        else:
            prev_rec = prev_records_map.get(code)
            if prev_rec:
                single = _subtract_records(rec, prev_rec)
            else:
                # 沒有前季資料，無法反算，跳過（避免存入累積值當單季）
                continue

        # 單位：MOPS 是千元，乘 1000 轉為元
        for f in ['revenue', 'cost', 'gross_profit', 'operating_expense',
                  'operating_income', 'non_operating', 'pretax_income',
                  'tax', 'continuing_income', 'net_income_parent']:
            if single.get(f) is not None:
                single[f] = single[f] * 1000

        # MOPS 最高優先，直接覆蓋
        c.execute("""
            INSERT INTO quarterly_financial
              (code, quarter, revenue, cost, gross_profit, operating_expense,
               operating_income, non_operating, pretax_income, tax,
               continuing_income, net_income_parent, eps, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
              updated_at=excluded.updated_at
        """, (code, quarter_label,
              single.get('revenue'), single.get('cost'), single.get('gross_profit'),
              single.get('operating_expense'), single.get('operating_income'),
              single.get('non_operating'), single.get('pretax_income'),
              single.get('tax'), single.get('continuing_income'),
              single.get('net_income_parent'), single.get('eps'), now_str))
        saved += 1

    conn.commit()
    conn.close()

    print(f"[MOPS季報] {quarter_label} 已存 {saved} 筆（單季值）")
    return saved


def fetch_latest_mops_quarterly():
    """
    自動判斷應抓哪一季，依法定申報期限：
    - Q1: 5/15 前公布 → 5月開始抓
    - Q2: 8/14 前公布 → 8月開始抓
    - Q3: 11/14 前公布 → 11月開始抓
    - Q4(年報): 3/31 前公布 → 3月開始抓（隔年）
    回傳更新筆數
    """
    today = date.today()
    m = today.month
    roc_year = today.year - 1911

    # 決定要抓哪一季
    if m >= 5 and m < 8:
        # 5~7月：抓當年 Q1
        target_year, target_season = roc_year, 1
    elif m >= 8 and m < 11:
        # 8~10月：抓當年 Q2
        target_year, target_season = roc_year, 2
    elif m >= 11:
        # 11~12月：抓當年 Q3
        target_year, target_season = roc_year, 3
    else:
        # 1~4月：抓去年 Q4
        target_year, target_season = roc_year - 1, 4

    print(f"[MOPS季報] 自動判斷：抓取 {target_year}Q{target_season}")
    return fetch_and_save_mops_quarterly(target_year, target_season)
