"""
後端 API：Flask
提供股票資料給前端網頁
"""

from flask import Flask, jsonify, request
import sqlite3
import threading
from guardian import (generate_health_report, get_provider_status, PROVIDER_TIERS,
                      get_all_breakers, get_breaker,
                      get_quarantine_list, resolve_quarantine,
                      get_fingerprint_stats, get_coverage_map,
                      get_audit_log, get_daily_briefing,
                      get_recent_news)
from scraper import (run as scraper_run, refresh_prices, init_db, init_financial_db,
                     init_monthly_revenue_db, init_quarterly_db,
                     init_pe_history_db, fetch_company_financials,
                     fetch_company_monthly_revenue, fetch_company_quarterly,
                     fetch_pe_history, _calc_fin_grade)

app = Flask(__name__, static_folder=".", static_url_path="")
DB_PATH = "stocks.db"

# ── 爬蟲狀態鎖（避免同時跑兩次）──────────────────────────
_refresh_lock   = threading.Lock()
_is_refreshing  = False

def query_db(sql, args=()):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(sql, args)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

# ── 取得全部股票 ────────────────────────────────────────────
@app.route("/api/stocks")
def get_stocks():
    q      = request.args.get("q", "").strip()
    market = request.args.get("market", "")

    sql    = """SELECT code, name, market, industry, close, change, change_240d,
                       revenue_date, revenue_yoy, revenue_mom, revenue_cum_yoy,
                       eps_date, eps_1, eps_1q, eps_2, eps_2q,
                       eps_3, eps_3q, eps_4, eps_4q, eps_5, eps_5q,
                       eps_y1, eps_y1_label, eps_y2, eps_y2_label,
                       eps_y3, eps_y3_label, eps_y4, eps_y4_label,
                       eps_y5, eps_y5_label, eps_ytd, eps_ytd_label,
                       div_c1, div_s1, div_1_label, div_c2, div_s2, div_2_label,
                       div_c3, div_s3, div_3_label, div_c4, div_s4, div_4_label,
                       div_c5, div_s5, div_5_label,
                       contract_1, contract_1q, contract_2, contract_2q,
                       contract_3, contract_3q,
                       fin_grade_1, fin_grade_1y, fin_grade_2, fin_grade_2y,
                       fin_grade_3, fin_grade_3y, fin_grade_4, fin_grade_4y,
                       fin_grade_5, fin_grade_5y,
                       price_pos, fair_low, fair_high
                FROM stocks WHERE 1=1"""
    params = []
    if q:
        sql += " AND (code LIKE ? OR name LIKE ?)"
        params += [f"%{q}%", f"%{q}%"]
    if market in ("上市", "上櫃"):
        sql += " AND market = ?"
        params.append(market)
    sql += " ORDER BY code ASC"

    rows = query_db(sql, params)
    return jsonify({"count": len(rows), "data": rows})

# ── 狀態（資料筆數 + 最後更新時間）────────────────────────
@app.route("/api/status")
def status():
    rows    = query_db("SELECT updated_at FROM stocks ORDER BY updated_at DESC LIMIT 1")
    updated = rows[0]["updated_at"] if rows else None
    total   = query_db("SELECT COUNT(*) as n FROM stocks")[0]["n"]
    # API 健康狀態
    health_rows = query_db("SELECT source, description, status, last_success, fail_count FROM api_health ORDER BY status DESC, source")
    alerts = [dict(r) for r in health_rows if r['status'] != 'ok']

    return jsonify({
        "updated_at":   updated,
        "api_alerts":   alerts,
        "total":        total,
        "is_refreshing": _is_refreshing
    })

# ── 手動觸發更新（背景執行，立即回應）─────────────────────
@app.route("/api/refresh", methods=["POST"])
def refresh():
    global _is_refreshing
    if _is_refreshing:
        return jsonify({"status": "already_running", "msg": "更新中，請稍候"}), 200

    def do_refresh():
        global _is_refreshing
        with _refresh_lock:
            _is_refreshing = True
            try:
                refresh_prices()
            finally:
                _is_refreshing = False

    threading.Thread(target=do_refresh, daemon=True).start()
    return jsonify({"status": "started", "msg": "開始更新資料"})

# ── 更新進度查詢 ────────────────────────────────────────────
@app.route("/api/refresh/status")
def refresh_status():
    return jsonify({"is_refreshing": _is_refreshing})

# ── 個股年度財報 ────────────────────────────────────────────
@app.route("/api/stocks/<code>/financials")
def get_financials(code):
    from datetime import datetime, timedelta

    # 檢查快取（24 小時內有效）
    rows = query_db(
        "SELECT * FROM financial_annual WHERE code = ? ORDER BY year DESC LIMIT 5",
        (code,)
    )
    cache_valid = False
    if rows:
        try:
            updated = datetime.strptime(rows[0]['updated_at'], '%Y-%m-%d %H:%M:%S')
            if datetime.now() - updated < timedelta(hours=24):
                cache_valid = True
        except:
            pass

    if not cache_valid:
        result = fetch_company_financials(code)
        if result:
            rows = query_db(
                "SELECT * FROM financial_annual WHERE code = ? ORDER BY year DESC LIMIT 5",
                (code,)
            )

    # 計算衍生指標
    data = []
    for r in rows:
        d = dict(r)
        rev = d.get('revenue')
        ni  = d.get('net_income')
        ocf = d.get('operating_cf')
        capex = d.get('capex')
        ta  = d.get('total_assets')
        te  = d.get('total_equity')
        cs  = d.get('common_stock')
        eps_val = d.get('eps')
        cd  = d.get('cash_dividend')
        sd  = d.get('stock_dividend')

        # 毛利率
        d['gross_margin'] = round(d['gross_profit'] / rev * 100, 2) if rev and d.get('gross_profit') is not None else None
        # 營業利益率
        d['operating_margin'] = round(d['operating_income'] / rev * 100, 2) if rev and d.get('operating_income') is not None else None
        # 稅前淨利率
        d['pretax_margin'] = round(d['pretax_income'] / rev * 100, 2) if rev and d.get('pretax_income') is not None else None
        # 稅後淨利率
        d['net_margin'] = round(ni / rev * 100, 2) if rev and ni is not None else None
        # ROA
        d['roa'] = round(ni / ta * 100, 2) if ta and ni is not None else None
        # ROE
        d['roe'] = round(ni / te * 100, 2) if te and ni is not None else None
        # 盈餘品質率
        d['earnings_quality'] = round(ocf / ni * 100, 2) if ni and ni != 0 and ocf is not None else None
        # 自由現金流（capex 為負值）
        d['fcf'] = round(ocf + capex, 2) if ocf is not None and capex is not None else None
        # 每股自由現金流
        shares = cs / 10 if cs and cs > 0 else None
        d['fcf_per_share'] = round(d['fcf'] / shares, 2) if d.get('fcf') is not None and shares else None
        # 配息率
        total_div = ((cd or 0) + (sd or 0))
        d['payout_ratio'] = round(total_div / eps_val * 100, 2) if eps_val and eps_val > 0 and total_div > 0 else None
        # 年度標籤（民國年）
        d['year_label'] = str(d['year'] - 1911)

        data.append(d)

    # 計算財務體質等級並寫入 stocks 表
    if data:
        conn2 = sqlite3.connect(DB_PATH)
        c2 = conn2.cursor()
        updates = {}
        for i, d in enumerate(data[:5], 1):
            grade = _calc_fin_grade(d.get('roe'), d.get('operating_margin'), d.get('fcf'), d.get('revenue'))
            updates[f'fin_grade_{i}'] = grade
            updates[f'fin_grade_{i}y'] = d.get('year_label')
        for i in range(len(data[:5]) + 1, 6):
            updates[f'fin_grade_{i}'] = None
            updates[f'fin_grade_{i}y'] = None
        set_clause = ', '.join(f'{k}=?' for k in updates.keys())
        c2.execute(f'UPDATE stocks SET {set_clause} WHERE code=?',
                   list(updates.values()) + [code])
        conn2.commit()
        conn2.close()

    # 取得公司名稱
    stock_info = query_db("SELECT name, market FROM stocks WHERE code = ?", (code,))
    name = stock_info[0]['name'] if stock_info else code

    return jsonify({"code": code, "name": name, "data": data})


# ── 個股季度估計表 ──────────────────────────────────────────
@app.route("/api/stocks/<code>/quarterly")
def get_quarterly(code):
    from datetime import datetime, timedelta

    rows = query_db(
        "SELECT * FROM quarterly_financial WHERE code = ? ORDER BY quarter DESC LIMIT 8",
        (code,)
    )
    cache_valid = False
    if rows:
        try:
            updated = datetime.strptime(rows[0]['updated_at'], '%Y-%m-%d %H:%M:%S')
            if datetime.now() - updated < timedelta(hours=12):
                cache_valid = True
        except:
            pass

    if not cache_valid:
        fetch_company_quarterly(code)
        rows = query_db(
            "SELECT * FROM quarterly_financial WHERE code = ? ORDER BY quarter DESC LIMIT 8",
            (code,)
        )

    data = []
    for r in rows:
        d = dict(r)
        rev = d.get('revenue')
        pti = d.get('pretax_income')
        tax = d.get('tax')
        oi  = d.get('operating_income')
        ci  = d.get('continuing_income')
        nip = d.get('net_income_parent')
        eps_val = d.get('eps')
        opex = d.get('operating_expense')

        # 毛利率
        d['gross_margin'] = round(d['gross_profit'] / rev * 100, 2) if rev and d.get('gross_profit') is not None else None
        # 營業費用占營收比率
        d['opex_ratio'] = round(opex / rev * 100, 2) if rev and opex is not None else None
        # 稅率
        d['tax_rate'] = round(tax / pti * 100, 2) if pti and pti != 0 and tax is not None else None
        # 歸屬母公司權重
        d['parent_weight'] = round(nip / ci * 100, 2) if ci and ci != 0 and nip is not None else None
        # 加權平均股數（從 EPS 反算，單位：千股）
        if eps_val and eps_val != 0 and nip is not None:
            shares = nip / eps_val  # 元 / (元/股) = 股
            d['weighted_shares'] = round(shares / 1000, 0)  # 千股
        else:
            d['weighted_shares'] = None
        # 每股盈餘-本業
        shares_raw = nip / eps_val if eps_val and eps_val != 0 and nip is not None else None
        eff_tax = tax / pti if pti and pti != 0 and tax is not None else None
        if oi is not None and shares_raw and eff_tax is not None:
            d['eps_core'] = round(oi * (1 - eff_tax) / shares_raw, 2)
        else:
            d['eps_core'] = None
        # 每股盈餘-業外
        if d.get('eps_core') is not None and eps_val is not None:
            d['eps_nonop'] = round(eps_val - d['eps_core'], 2)
        else:
            d['eps_nonop'] = None

        data.append(d)

    stock_info = query_db("SELECT name FROM stocks WHERE code = ?", (code,))
    name = stock_info[0]['name'] if stock_info else code
    return jsonify({"code": code, "name": name, "data": data})


# ── 歷史本益比 ──────────────────────────────────────────────
@app.route("/api/stocks/<code>/pe-history")
def get_pe_history(code):
    from datetime import datetime, timedelta
    import statistics

    rows = query_db(
        "SELECT * FROM pe_history WHERE code = ? ORDER BY year ASC",
        (code,)
    )
    cache_valid = False
    if rows:
        try:
            updated = datetime.strptime(rows[-1]['updated_at'], '%Y-%m-%d %H:%M:%S')
            if datetime.now() - updated < timedelta(days=7):
                cache_valid = True
        except:
            pass

    if not cache_valid:
        fetch_pe_history(code)
        rows = query_db(
            "SELECT * FROM pe_history WHERE code = ? ORDER BY year ASC",
            (code,)
        )

    data = [dict(r) for r in rows]
    # 取最近 8 年
    data = data[-8:] if len(data) > 8 else data

    # 統計推估
    est = {}
    if len(data) >= 3:
        highs = [d['pe_high'] for d in data]
        lows  = [d['pe_low'] for d in data]
        est['avg_high'] = round(sum(highs) / len(highs), 2)
        est['avg_low']  = round(sum(lows) / len(lows), 2)
        est['median_high'] = round(statistics.median(highs), 2)
        est['median_low']  = round(statistics.median(lows), 2)
        # 去極值平均（去掉最高和最低各一個）
        if len(highs) >= 5:
            trimmed_h = sorted(highs)[1:-1]
            trimmed_l = sorted(lows)[1:-1]
            est['trimmed_avg_high'] = round(sum(trimmed_h) / len(trimmed_h), 2)
            est['trimmed_avg_low']  = round(sum(trimmed_l) / len(trimmed_l), 2)

    stock_info = query_db("SELECT name FROM stocks WHERE code = ?", (code,))
    name = stock_info[0]['name'] if stock_info else code
    return jsonify({"code": code, "name": name, "data": data, "estimate": est})


# ── 個股月營收 ──────────────────────────────────────────────
@app.route("/api/stocks/<code>/monthly-revenue")
def get_monthly_revenue(code):
    from datetime import datetime, timedelta
    import math

    # 檢查快取
    rows = query_db(
        "SELECT * FROM monthly_revenue WHERE code = ? ORDER BY year DESC, month ASC",
        (code,)
    )
    cache_valid = False
    if rows:
        try:
            updated = datetime.strptime(rows[0]['updated_at'], '%Y-%m-%d %H:%M:%S')
            if datetime.now() - updated < timedelta(hours=4):
                cache_valid = True
        except:
            pass

    if not cache_valid:
        fetch_company_monthly_revenue(code)
        rows = query_db(
            "SELECT * FROM monthly_revenue WHERE code = ? ORDER BY year DESC, month ASC",
            (code,)
        )

    # 建立 {(year, month): revenue} 查找表
    rev_map = {}
    for r in rows:
        rev_map[(r['year'], r['month'])] = r['revenue']

    # 找出最近 3 個有資料的年度（加上前一年用來算年增率）
    all_years = sorted(set(r['year'] for r in rows), reverse=True)
    display_years = all_years[:3]  # 最近 3 年顯示
    if not display_years:
        stock_info = query_db("SELECT name FROM stocks WHERE code = ?", (code,))
        name = stock_info[0]['name'] if stock_info else code
        return jsonify({"code": code, "name": name, "years": [], "data": []})

    # 組合每月資料，計算增率
    data = []
    for m in range(1, 13):
        row = {"month": m}
        for yr in display_years:
            cur = rev_map.get((yr, m))
            # 上月營收（上月或去年12月）
            if m == 1:
                prev_m = rev_map.get((yr - 1, 12))
            else:
                prev_m = rev_map.get((yr, m - 1))
            # 去年同月
            prev_y = rev_map.get((yr - 1, m))
            # 當月沒有營收 → 所有衍生指標都不算
            if cur is None:
                row[str(yr)] = {"revenue": None, "mom": None, "yoy": None, "cum_yoy": None}
                continue

            # 月增率
            mom = round((cur / prev_m - 1) * 100, 2) if prev_m else None
            # 年增率
            yoy = round((cur / prev_y - 1) * 100, 2) if prev_y else None
            # 累積營收年增率（只累計有資料的月份）
            cum_cur = sum(rev_map.get((yr, i), 0) for i in range(1, m + 1) if rev_map.get((yr, i)))
            cum_prev = sum(rev_map.get((yr - 1, i), 0) for i in range(1, m + 1) if rev_map.get((yr - 1, i)))
            cum_yoy = round((cum_cur / cum_prev - 1) * 100, 2) if cum_prev and cum_cur else None

            row[str(yr)] = {
                "revenue": cur,
                "mom": mom,
                "yoy": yoy,
                "cum_yoy": cum_yoy,
            }
        data.append(row)

    stock_info = query_db("SELECT name FROM stocks WHERE code = ?", (code,))
    name = stock_info[0]['name'] if stock_info else code

    return jsonify({
        "code": code,
        "name": name,
        "years": sorted(display_years),
        "data": data,
    })


# ── 系統健康報告 ──────────────────────────────────────────
@app.route("/api/health")
def health():
    return jsonify(generate_health_report())

@app.route("/api/providers")
def providers():
    return jsonify(get_provider_status())

@app.route("/api/breakers")
def breakers():
    return jsonify(get_all_breakers())

@app.route("/api/breakers/<source>/reset", methods=["POST"])
def reset_breaker(source):
    b = get_breaker(source)
    b.reset()
    return jsonify({"status": "ok", "source": source, "state": "CLOSED"})

@app.route("/api/quarantine")
def quarantine():
    return jsonify(get_quarantine_list(100))

@app.route("/api/quarantine/<int:qid>/<action>", methods=["POST"])
def handle_quarantine(qid, action):
    if action not in ('accept', 'reject'):
        return jsonify({"error": "action must be accept or reject"}), 400
    ok = resolve_quarantine(qid, action)
    return jsonify({"status": "ok" if ok else "error"})

@app.route("/api/fingerprints")
def fingerprints():
    return jsonify(get_fingerprint_stats())

@app.route("/api/coverage")
def coverage():
    return jsonify(get_coverage_map())

@app.route("/api/db-status")
def db_status():
    import os
    alert_file = "logs/DB_ALERT"
    db_file = "stocks.db"
    size = os.path.getsize(db_file) if os.path.exists(db_file) else 0
    alert = os.path.exists(alert_file)
    icloud_alert = os.path.exists("logs/ICLOUD_ALERT")
    icloud_ok = os.path.exists(os.path.expanduser(
        "~/Library/Mobile Documents/com~apple~CloudDocs/Documents/"))
    # 讀最近一筆 guard log
    last_check = None
    try:
        with open("logs/db_guard.log") as f:
            lines = f.readlines()
            if lines:
                last_check = lines[-1].strip()
    except:
        pass
    return jsonify({
        "size_bytes": size,
        "size_mb": round(size / 1024 / 1024, 2),
        "alert": alert,
        "icloud": icloud_ok and not icloud_alert,
        "last_check": last_check,
    })

@app.route("/api/daily-briefing")
def daily_briefing():
    return jsonify(get_daily_briefing())

@app.route("/api/realtime")
def realtime():
    """盤中即時報價（前端傳入代碼清單）"""
    import requests as req
    codes_param = request.args.get("codes", "")
    if not codes_param:
        return jsonify([])

    code_list = [c.strip() for c in codes_param.split(",") if c.strip()]
    if not code_list:
        return jsonify([])

    # 查市場別
    rows = query_db("SELECT code, market FROM stocks WHERE code IN ({})".format(
        ",".join("?" for _ in code_list)), code_list)
    market_map = {r['code']: r['market'] for r in rows}

    # 組 TWSE 即時 API 參數（每批最多 50 檔）
    all_results = []
    ex_codes = []
    for code in code_list:
        mkt = market_map.get(code, '上市')
        prefix = 'tse' if mkt == '上市' else 'otc'
        ex_codes.append(f"{prefix}_{code}.tw")

    for i in range(0, len(ex_codes), 50):
        batch = ex_codes[i:i+50]
        try:
            url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={'|'.join(batch)}"
            r = req.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            data = r.json()
            for s in data.get("msgArray", []):
                price = s.get("z")
                if price == "-" or not price:
                    # z 沒值時取最佳買價（五檔第一筆）
                    bid = s.get("b", "")
                    if bid and "_" in bid:
                        price = bid.split("_")[0]
                if price == "-" or not price:
                    price = s.get("y")  # 最後 fallback 昨收
                all_results.append({
                    "code": s.get("c"),
                    "name": s.get("n"),
                    "price": float(price) if price else None,
                    "open": float(s["o"]) if s.get("o") else None,
                    "high": float(s["h"]) if s.get("h") else None,
                    "low": float(s["l"]) if s.get("l") else None,
                    "volume": int(s["v"]) if s.get("v") else None,
                    "time": s.get("t"),
                    "yesterday": float(s["y"]) if s.get("y") else None,
                })
        except:
            pass

    return jsonify(all_results)

@app.route("/api/news")
def news():
    code = request.args.get("code")
    tier = int(request.args.get("tier", 1))
    limit = int(request.args.get("limit", 50))
    if request.args.get("important") == "1" and code:
        rows = query_db("""SELECT * FROM material_news
                          WHERE code=? AND status='important' AND created_at > datetime('now', '-30 days')
                          ORDER BY created_at DESC LIMIT ?""", (code, limit))
        return jsonify(rows)
    return jsonify(get_recent_news(code, tier, limit))

@app.route("/api/news/<int:nid>/upgrade", methods=["POST"])
def upgrade_news(nid):
    """把 Tier 0 升級到 Tier 1（使用者認為被誤過濾）"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE material_news SET tier=1, matched_rule='使用者升級' WHERE id=? AND tier=0", (nid,))
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})

@app.route("/api/news/<int:nid>/status", methods=["POST"])
def update_news_status(nid):
    status = request.json.get("status") if request.is_json else request.args.get("status")
    if status not in ('important', 'dismissed'):
        return jsonify({"error": "status must be important or dismissed"}), 400
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE material_news SET status=? WHERE id=?", (status, nid))
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"})

@app.route("/api/news-flags")
def news_flags():
    """回傳有重要新聞的股票代碼清單（給總表標記用）"""
    rows = query_db("""SELECT code, COUNT(*) as cnt FROM material_news
                       WHERE status='important' AND created_at > datetime('now', '-30 days')
                       GROUP BY code""")
    return jsonify({r['code']: r['cnt'] for r in rows})

@app.route("/api/audit")
def audit():
    code = request.args.get("code")
    limit = int(request.args.get("limit", 100))
    return jsonify(get_audit_log(limit, code))

# ── 同業比較 ────────────────────────────────────────────────
@app.route("/api/industry-compare/<code>")
def industry_compare(code):
    """回傳同產業所有股票的關鍵指標，以及目標股票在同業中的排名"""
    # 1. 取得目標股票的產業
    target = query_db("SELECT code, name, industry FROM stocks WHERE code = ?", (code,))
    if not target or not target[0].get("industry"):
        return jsonify({"error": "找不到股票或無產業分類"}), 404
    industry = target[0]["industry"]

    # 2. 撈同產業全部股票的關鍵欄位
    peers = query_db("""
        SELECT code, name, close, eps_y1, eps_y2, eps_y1_label,
               revenue_yoy, revenue_cum_yoy, div_c1, div_1_label,
               price_pos, change_240d, market
        FROM stocks
        WHERE industry = ? AND close IS NOT NULL AND close > 0
        ORDER BY code
    """, (industry,))

    # 3. 計算衍生指標
    for p in peers:
        eps = p.get("eps_y1")
        close = p.get("close")
        # 本益比
        if eps and eps > 0 and close:
            p["pe"] = round(close / eps, 2)
        else:
            p["pe"] = None
        # 殖利率 (%)
        div = p.get("div_c1") or 0
        if close and close > 0:
            p["yield_pct"] = round(div / close * 100, 2)
        else:
            p["yield_pct"] = None
        # EPS 成長率 (%)
        eps1 = p.get("eps_y1")
        eps2 = p.get("eps_y2")
        if eps1 is not None and eps2 is not None and eps2 != 0:
            p["eps_growth"] = round((eps1 - eps2) / abs(eps2) * 100, 2)
        else:
            p["eps_growth"] = None

    # 4. 排名函式（數值越大排名越前）
    def rank_desc(lst, key):
        vals = [(i, x.get(key)) for i, x in enumerate(lst)]
        valid = [(i, v) for i, v in vals if v is not None]
        valid.sort(key=lambda t: t[1], reverse=True)
        ranks = {}
        for rank, (i, _) in enumerate(valid, 1):
            ranks[i] = rank
        total = len(valid)
        return ranks, total

    # 數值越小排名越前（本益比低 = 好）
    def rank_asc(lst, key):
        vals = [(i, x.get(key)) for i, x in enumerate(lst)]
        valid = [(i, v) for i, v in vals if v is not None]
        valid.sort(key=lambda t: t[1])
        ranks = {}
        for rank, (i, _) in enumerate(valid, 1):
            ranks[i] = rank
        total = len(valid)
        return ranks, total

    metrics = [
        ("pe",              "asc"),    # 本益比越低越好
        ("eps_y1",          "desc"),   # EPS 越高越好
        ("eps_growth",      "desc"),   # EPS 成長越高越好
        ("revenue_yoy",     "desc"),   # 營收年增越高越好
        ("revenue_cum_yoy", "desc"),   # 累計營收年增越高越好
        ("yield_pct",       "desc"),   # 殖利率越高越好
        ("change_240d",     "desc"),   # 240日漲幅越高越好
    ]

    # 計算每個指標的排名
    ranking_data = {}
    for key, direction in metrics:
        if direction == "desc":
            ranks, total = rank_desc(peers, key)
        else:
            ranks, total = rank_asc(peers, key)
        ranking_data[key] = {"ranks": ranks, "total": total}

    # 5. 找出目標股票的 index
    target_idx = None
    for i, p in enumerate(peers):
        if p["code"] == code:
            target_idx = i
            break

    # 6. 組裝目標股票的排名摘要
    summary = {}
    if target_idx is not None:
        for key, _ in metrics:
            rd = ranking_data[key]
            rank = rd["ranks"].get(target_idx)
            total = rd["total"]
            if rank and total:
                summary[key] = {
                    "rank": rank,
                    "total": total,
                    "percentile": round((1 - (rank - 1) / total) * 100, 1)
                }
            else:
                summary[key] = None

    # 7. 把排名塞進每筆 peer 資料
    for i, p in enumerate(peers):
        p["rankings"] = {}
        for key, _ in metrics:
            rd = ranking_data[key]
            rank = rd["ranks"].get(i)
            total = rd["total"]
            if rank:
                p["rankings"][key] = {"rank": rank, "total": total}
            else:
                p["rankings"][key] = None

    # 8. 計算同業中位數
    import statistics
    medians = {}
    for key, _ in metrics:
        vals = [p.get(key) for p in peers if p.get(key) is not None]
        if vals:
            medians[key] = round(statistics.median(vals), 2)
        else:
            medians[key] = None

    return jsonify({
        "code": code,
        "name": target[0]["name"],
        "industry": industry,
        "peer_count": len(peers),
        "summary": summary,
        "medians": medians,
        "peers": peers
    })

# ── 前端首頁 ────────────────────────────────────────────────
@app.route("/")
def index():
    return app.send_static_file("index.html")

# ── 啟動時初始化資料庫 ──────────────────────────────────────
if __name__ == "__main__":
    init_db()
    init_financial_db()
    init_monthly_revenue_db()
    init_quarterly_db()
    init_pe_history_db()
    app.run(host="0.0.0.0", port=5000, debug=False)
