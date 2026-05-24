"""
MOPS 公開資訊觀測站資料抓取（第一優先來源）
- 月營收：t21sc03（即時，比 t187ap05 快）
- 季度綜合損益表：t163sb04（累積值，需反算單季）
"""

import db as sqlite3
import requests
import re
import time
from datetime import date, datetime
from bs4 import BeautifulSoup
from fetcher_utils import create_session

DB_PATH = "stocks.db"

_session = create_session(extra_headers={
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

    # 第一步：先從網頁抓資料存到記憶體（不碰 DB）
    all_records = []
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
                cum_yoy = _safe_float(texts[9]) if len(texts) > 9 else None

                all_records.append({
                    'code': code, 'revenue': rev_val,
                    'yoy': yoy, 'mom': mom, 'cum_yoy': cum_yoy
                })
                cnt += 1

            if cnt:
                print(f"  [MOPS營收-{mtype}] {cnt} 筆（{roc_year}年{month}月）")
        except Exception as e:
            print(f"  [MOPS營收-{mtype}] 失敗: {e}")

    if not all_records:
        return 0

    # 第二步：寫入 DB（重試最多 3 次，每次間隔 10 秒）
    import time as _time
    for attempt in range(3):
        try:
            with sqlite3.get_conn(timeout=60) as conn:
                c = conn.cursor()

                for rec in all_records:
                    code = rec['code']
                    c.execute("""INSERT INTO monthly_revenue (code, year, month, revenue, updated_at)
                        VALUES (?,?,?,?,?) ON CONFLICT(code, year, month) DO UPDATE SET
                        revenue=excluded.revenue, updated_at=excluded.updated_at""",
                        (code, west_year, month, rec['revenue'], now_str))

                    old = c.execute("SELECT revenue_year, revenue_month FROM stocks WHERE code=?", (code,)).fetchone()
                    if old and (west_year > (old[0] or 0) or (west_year == (old[0] or 0) and month > (old[1] or 0))):
                        c.execute("""UPDATE stocks SET revenue_date=?, revenue_year=?, revenue_month=?,
                            revenue_yoy=?, revenue_mom=?, revenue_cum_yoy=? WHERE code=?""",
                            (today_str, west_year, month, rec['yoy'], rec['mom'], rec['cum_yoy'], code))

                conn.commit()
                total = len(all_records)
            break  # 成功，跳出重試
        except Exception as e:
            if attempt < 2:
                print(f"  [MOPS營收] 寫入失敗，{10}秒後重試（{attempt+1}/3）: {e}")
                _time.sleep(10)
            else:
                print(f"  [MOPS營收] 3次重試仍失敗: {e}")

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

    # 反算單季（先算好存記憶體，再寫 DB）
    write_rows = []
    for rec in current_records:
        code = rec['code']

        if season == 1:
            single = rec.copy()
        else:
            prev_rec = prev_records_map.get(code)
            if prev_rec:
                single = _subtract_records(rec, prev_rec)
            else:
                continue

        # 單位：MOPS 是千元，乘 1000 轉為元
        for f in ['revenue', 'cost', 'gross_profit', 'operating_expense',
                  'operating_income', 'non_operating', 'pretax_income',
                  'tax', 'continuing_income', 'net_income_parent']:
            if single.get(f) is not None:
                single[f] = single[f] * 1000

        write_rows.append(single)

    # 寫入 DB（重試最多 3 次）
    import time as _time
    saved = 0
    for attempt in range(3):
        try:
            with sqlite3.get_conn(timeout=60) as conn:
                c = conn.cursor()

                for single in write_rows:
                    c.execute("""
                        INSERT INTO quarterly_financial
                          (code, quarter, revenue, cost, gross_profit, operating_expense,
                           operating_income, non_operating, pretax_income, tax,
                           continuing_income, net_income_parent, eps, updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(code, quarter) DO UPDATE SET
                          revenue=excluded.revenue, cost=excluded.cost,
                          gross_profit=excluded.gross_profit, operating_expense=excluded.operating_expense,
                          operating_income=excluded.operating_income, non_operating=excluded.non_operating,
                          pretax_income=excluded.pretax_income, tax=excluded.tax,
                          continuing_income=excluded.continuing_income,
                          net_income_parent=excluded.net_income_parent,
                          eps=excluded.eps, updated_at=excluded.updated_at
                    """, (single.get('code'), quarter_label,
                          single.get('revenue'), single.get('cost'), single.get('gross_profit'),
                          single.get('operating_expense'), single.get('operating_income'),
                          single.get('non_operating'), single.get('pretax_income'),
                          single.get('tax'), single.get('continuing_income'),
                          single.get('net_income_parent'), single.get('eps'), now_str))
                    saved += 1

                conn.commit()
            break
        except Exception as e:
            saved = 0
            if attempt < 2:
                print(f"  [MOPS季報] 寫入失敗，10秒後重試（{attempt+1}/3）: {e}")
                _time.sleep(10)
            else:
                print(f"  [MOPS季報] 3次重試仍失敗: {e}")

    print(f"[MOPS季報] {quarter_label} 已存 {saved} 筆（單季值）")
    return saved


def is_quarterly_filing_period():
    """
    判斷今天是否在季報申報期。
    法定截止日：Q1=5/15, Q2=8/14, Q3=11/14, Q4=3/31
    視窗延長到截止日後兩週，確保遲申報公司也能被抓到。
    回傳 (在申報期, 目標民國年, 目標季度) 或 (False, None, None)
    """
    today = date.today()
    m, d = today.month, today.day
    roc_year = today.year - 1911

    # (申報期起始月/日, 截止月/日, 目標民國年偏移, 目標季度)
    # 截止日延長兩週：5/15→5/31, 8/14→8/31, 11/14→11/30, 3/31→4/15
    periods = [
        (4, 1, 5, 31, 0, 1),    # Q1: 4/1 ~ 5/31
        (6, 30, 8, 31, 0, 2),   # Q2: 6/30 ~ 8/31
        (10, 1, 11, 30, 0, 3),  # Q3: 10/1 ~ 11/30
        (2, 14, 4, 15, -1, 4),  # Q4: 2/14 ~ 4/15（去年Q4）
    ]

    for sm, sd, em, ed, yr_offset, season in periods:
        start = date(today.year, sm, sd)
        end = date(today.year, em, ed)
        if start <= today <= end:
            return True, roc_year + yr_offset, season

    return False, None, None


def fetch_latest_mops_quarterly():
    """
    自動判斷應抓哪一季，只在申報期內才抓。
    申報視窗：Q1=4/1~5/31, Q2=6/30~8/31, Q3=10/1~11/30, Q4=2/14~4/15
    回傳更新筆數，非申報期回傳 0
    """
    in_period, target_year, target_season = is_quarterly_filing_period()

    if not in_period:
        return 0

    print(f"[MOPS季報] 申報期內，抓取 {target_year}Q{target_season}")
    return fetch_and_save_mops_quarterly(target_year, target_season)


# ══════════════════════════════════════════════════════════════
# 季度資產負債表（MOPS t164sb03）— 存貨 + 合約負債
# ══════════════════════════════════════════════════════════════

_bs_session = None

def _get_bs_session():
    """取得或建立 BS 專用 session（被封時可重建）"""
    global _bs_session
    if _bs_session is None:
        _bs_session = create_session(extra_headers={
            'Referer': 'https://mopsov.twse.com.tw/mops/web/t164sb03',
        })
    return _bs_session

def _reset_bs_session():
    """重建 BS session（被封後使用）"""
    global _bs_session
    _bs_session = create_session(extra_headers={
        'Referer': 'https://mopsov.twse.com.tw/mops/web/t164sb03',
    })

def _fetch_mops_balance_sheet(code, roc_year, season):
    """
    從 MOPS 個股 API（t164sb03）抓取季度資產負債表。
    回傳 dict: {inventory, contract_liability, current_assets, current_liabilities, ...} 或 None
    單位：千元 → 乘 1000 轉成元
    """
    url = 'https://mopsov.twse.com.tw/mops/web/ajax_t164sb03'
    payload = {
        'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1',
        'co_id': code, 'year': str(roc_year), 'season': f'{int(season):02d}',
    }
    try:
        r = _get_bs_session().post(url, data=payload, timeout=15, allow_redirects=False)
        if r.status_code in (301, 302, 307):
            _reset_bs_session()  # 立即重建 session
            time.sleep(2)
            # 用新 session 重試一次
            r = _get_bs_session().post(url, data=payload, timeout=15, allow_redirects=False)
            if r.status_code in (301, 302, 307):
                return None
        r.encoding = 'utf-8'
        if r.status_code != 200:
            return None
        if '安全性考量' in r.text or 'SECURITY REASONS' in r.text:
            return None  # 被封鎖
        soup = BeautifulSoup(r.text, 'html.parser')
        tables = soup.find_all('table')
        if len(tables) < 2:
            return None

        result = {}
        target_fields = {
            '存貨': 'inventory',
            '合約負債－流動': 'contract_liability',
            '流動資產': 'current_assets',
            '流動負債': 'current_liabilities',
            '應收帳款及票據': 'accounts_receivable',
        }

        t = tables[1]
        for row in t.find_all('tr'):
            cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
            if len(cells) >= 2:
                label = cells[0]
                if label in target_fields:
                    val = _safe_float(cells[1])
                    if val is not None:
                        result[target_fields[label]] = val * 1000  # 千元 → 元
        return result if result else None
    except Exception:
        return None


def fetch_mops_quarterly_bs(roc_year=None, season=None):
    """
    抓取所有缺存貨/合約負債的股票的季度資產負債表（MOPS t164sb03）。
    一次補完：每批 60 支換 session，每支 1.5 秒延遲，每批寫入 DB。
    """
    if roc_year is None or season is None:
        now = date.today()
        cur_year = now.year - 1911
        cur_month = now.month
        if cur_month >= 11:
            roc_year, season = cur_year, 3
        elif cur_month >= 8:
            roc_year, season = cur_year, 2
        elif cur_month >= 5:
            roc_year, season = cur_year, 1
        else:
            roc_year, season = cur_year - 1, 4

    quarter = f'{roc_year}Q{season}'

    # 找缺存貨的股票（有損益表但沒存貨，排除金融股）
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        c.execute("""SELECT qf.code FROM quarterly_financial qf
                     JOIN stocks s ON qf.code = s.code
                     WHERE qf.quarter = ? AND qf.revenue IS NOT NULL AND qf.inventory IS NULL
                     AND COALESCE(s.industry,'') NOT IN ('金融保險業','金融業','銀行業','保險業','證券業')""",
                  (quarter,))
        codes = [r[0] for r in c.fetchall()]

    if not codes:
        print(f"[MOPS-BS] {quarter} 無需補缺")
        return 0

    print(f"[MOPS-BS] {quarter} 缺存貨 {len(codes)} 支，開始補齊")

    total_ok = 0
    total_no_data = 0
    batch_size = 60
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for i in range(0, len(codes), batch_size):
        batch = codes[i:i + batch_size]
        _reset_bs_session()
        time.sleep(3)

        batch_updates = []
        for code in batch:
            result = _fetch_mops_balance_sheet(code, roc_year, season)
            if result:
                batch_updates.append((code, result))
            time.sleep(1.5)

        # 每批寫入 DB
        if batch_updates:
            with sqlite3.get_conn() as conn:
                c = conn.cursor()
                for col in ['inventory', 'contract_liability', 'current_assets', 'current_liabilities', 'accounts_receivable']:
                    try:
                        c.execute(f"ALTER TABLE quarterly_financial ADD COLUMN {col} REAL")
                    except Exception:
                        pass
                for code, result in batch_updates:
                    sets = [f'{f}=?' for f in result.keys()] + ['updated_at=?']
                    vals = list(result.values()) + [now_str, code, quarter]
                    c.execute(f"UPDATE quarterly_financial SET {', '.join(sets)} WHERE code=? AND quarter=?", vals)
                conn.commit()

        total_ok += len(batch_updates)
        total_no_data += len(batch) - len(batch_updates)
        done = min(i + batch_size, len(codes))
        print(f"  [MOPS-BS] 進度 {done}/{len(codes)}，成功 {total_ok}，無資料 {total_no_data}")

    print(f"[MOPS-BS] {quarter} 完成：{total_ok} 支寫入，{total_no_data} 支 MOPS 尚未公佈")
    return total_ok
