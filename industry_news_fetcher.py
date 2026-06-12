"""產業新聞爬蟲（經濟日報 RSS + 工商時報）— 獨立模組，不依賴 Flask"""
import logging
import db as sqlite3
logger = logging.getLogger(__name__)


def _init_industry_news_db():
    try:
        with sqlite3.get_conn() as conn:
            c = conn.cursor()
            c.execute("""CREATE TABLE IF NOT EXISTS industry_news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT, title TEXT, link TEXT, pub_time TEXT,
                summary TEXT, created_at TEXT, archived_code TEXT, archived_at TEXT
            )""")
            conn.commit()
    except Exception:
        pass


def fetch_industry_news():
    """抓取經濟日報 RSS + 工商時報產業新聞，存入 DB（標題去重）"""
    import xml.etree.ElementTree as ET
    import re
    _init_industry_news_db()

    import requests
    from datetime import datetime
    from email.utils import parsedate_to_datetime
    items = []
    _headers = {"User-Agent": "Mozilla/5.0"}

    def _parse_pub_time(pub_str):
        """RFC 2822 轉 YYYY-MM-DD HH:MM:SS"""
        if not pub_str:
            return ""
        try:
            dt = parsedate_to_datetime(pub_str)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            return pub_str

    # 經濟日報 RSS（產業 + 股市）
    udn_feeds = [
        ("https://money.udn.com/rssfeed/news/1001/5591", "經濟日報-產業"),
        ("https://money.udn.com/rssfeed/news/1001/5590", "經濟日報-股市"),
    ]
    for feed_url, source in udn_feeds:
        try:
            resp = requests.get(feed_url, headers=_headers, timeout=8)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
            for item in root.findall(".//item"):
                title = item.findtext("title", "").strip()
                link = item.findtext("link", "").strip()
                pub = item.findtext("pubDate", "").strip()
                desc = item.findtext("description", "").strip()
                if title:
                    items.append((source, title, link, _parse_pub_time(pub), desc[:100] if desc else ""))
        except Exception as e:
            logging.warning(f"抓取 {source} 失敗: {e}")

    # 工商時報 HTML
    try:
        resp = requests.get("https://www.chinatimes.com/newspapers/260110", headers=_headers, timeout=8)
        resp.raise_for_status()
        matches = re.findall(r'<h3[^>]*>\s*<a[^>]*href="(/newspapers/[^"]+)"[^>]*>([^<]+)</a>', resp.text)
        for path, title in matches:
            items.append(("工商時報-產業", title.strip(), f"https://www.chinatimes.com{path}", "", ""))
    except Exception as e:
        logging.warning(f"抓取工商時報失敗: {e}")

    if not items:
        return 0

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        inserted = 0
        for source, title, link, pub_time, summary in items:
            c.execute("SELECT id FROM industry_news WHERE title=? AND source=?", (title, source))
            if not c.fetchone():
                c.execute("INSERT INTO industry_news (source, title, link, pub_time, summary, created_at) VALUES (?,?,?,?,?,?)",
                          (source, title, link, pub_time, summary, now))
                inserted += 1
        conn.commit()
    print(f"[產業新聞] 抓取 {len(items)} 則，新增 {inserted} 則")
    return inserted


def cleanup_old_industry_news(days=7):
    """清理超過 N 天且未歸檔的產業新聞"""
    from datetime import datetime, timedelta
    _init_industry_news_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
    with sqlite3.get_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM industry_news WHERE created_at < ? AND archived_code IS NULL", (cutoff,))
        deleted = c.rowcount
        conn.commit()
    if deleted:
        print(f"[產業新聞] 清理 {deleted} 則過期新聞")
