"""
fetcher_utils.py — 爬蟲共用工具函式
統一 session 建立、數值解析、頁面抓取等重複邏輯
供 scraper.py / capital_fetcher.py / etf_fetcher.py 共用
"""

import ssl
import requests
from requests.adapters import HTTPAdapter
from datetime import datetime
from bs4 import BeautifulSoup

DB_PATH = "stocks.db"


class _TWCASSLAdapter(HTTPAdapter):
    """自訂 SSL adapter — 解決 TWCA Global Root CA 缺少 Subject Key Identifier
    Python 3.13 + OpenSSL 3.0 對 SKI 驗證變嚴格，導致群益/櫃買中心連線失敗"""
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.verify_flags = ssl.VERIFY_DEFAULT  # 不啟用嚴格的 X509_V_FLAG_X509_STRICT
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)


def create_session(ua=None, extra_headers=None):
    """建立 requests.Session，統一管理 headers"""
    s = requests.Session()
    s.headers.update({
        'User-Agent': ua or 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
    })
    if extra_headers:
        s.headers.update(extra_headers)
    # 掛載自訂 SSL adapter 處理 TWCA 憑證問題
    adapter = _TWCASSLAdapter()
    s.mount('https://', adapter)
    return s


def parse_num(s):
    """
    解析數值字串（含千分位逗號、負號、百分號）
    合併原 capital_fetcher._parse_num + scraper.safe_float
    """
    if s is None:
        return None
    s = str(s).replace(',', '').replace('%', '').strip()
    if s in ('', '-', '--', '---', 'N/A'):
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def parse_int(s):
    """解析整數字串（含千分位逗號）"""
    if s is None:
        return None
    s = str(s).replace(',', '').strip()
    if s in ('', '-', '--'):
        return None
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def fetch_page(session, url, encoding='big5', timeout=15):
    """
    抓取群益 HTML 頁面，回傳 table-cell 文字列表
    原 capital_fetcher._fetch_page
    """
    try:
        r = session.get(url, timeout=timeout)
        r.encoding = encoding
        soup = BeautifulSoup(r.text, 'html.parser')
        cells = soup.find_all(class_=lambda x: x and 'table-cell' in x)
        return [c.get_text(strip=True) for c in cells if c.get_text(strip=True)]
    except Exception:
        return []


def now_str():
    """統一時間戳格式"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
