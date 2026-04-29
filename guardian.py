"""
guardian.py — 資料守護系統
1. 原始資料備份（Data Lake Lite）
2. 資料驗證與異常偵測
3. 熔斷機制（Circuit Breaker）
4. 智慧優先權隊列
5. Provider Tier 管理
6. 來源信賴度評分（Reliability Score）
7. 資料鮮度偵測（Staleness Detector）
8. FinMind 額度預測
9. 多源仲裁（Data Arbitration）
"""

import json, os, gzip, math, hashlib
import db as sqlite3
from datetime import datetime, date, timedelta
from pathlib import Path
from collections import defaultdict

DB_PATH = "stocks.db"
BACKUP_DIR = "raw_backup"


def _init_fingerprint_table():
    """建立指紋表（只跑一次）"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS content_fingerprint (
            source TEXT PRIMARY KEY,
            last_hash TEXT,
            last_changed TEXT,
            check_count INTEGER DEFAULT 0,
            skip_count INTEGER DEFAULT 0
        )""")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[Guardian] fingerprint table init: {e}")

_init_fingerprint_table()


# ═══════════════════════════════════════════════════════════
# 1. 原始資料備份（Data Lake Lite）+ 指紋去重
#    API 回傳的 JSON 原封不動壓縮存檔
#    用 MD5 指紋比對，資料沒變就跳過備份，省空間
#    解析出錯時可用 reparse_from_backup() 重新處理
# ═══════════════════════════════════════════════════════════

def _compute_hash(data):
    """計算資料的 MD5 指紋（排序 key 確保穩定）"""
    raw = json.dumps(data, ensure_ascii=False, sort_keys=True).encode('utf-8')
    return hashlib.md5(raw).hexdigest()


def backup_raw_response(source_name, data, metadata=None):
    """
    備份 API 原始回應（含指紋去重）。
    回傳: 'saved' | 'unchanged' | 'error'
    """
    # 計算指紋
    content_hash = _compute_hash(data)

    # 查上次指紋
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT last_hash FROM content_fingerprint WHERE source=?", (source_name,))
        row = c.fetchone()
        old_hash = row[0] if row else None

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if old_hash == content_hash:
            # 資料沒變 → 跳過備份，更新計數
            c.execute("""UPDATE content_fingerprint
                         SET check_count = check_count + 1, skip_count = skip_count + 1
                         WHERE source=?""", (source_name,))
            conn.commit()
            conn.close()
            return 'unchanged'

        # 資料有變 → 更新指紋
        c.execute("""INSERT INTO content_fingerprint (source, last_hash, last_changed, check_count, skip_count)
                     VALUES (?, ?, ?, 1, 0)
                     ON CONFLICT(source) DO UPDATE SET
                     last_hash=excluded.last_hash, last_changed=excluded.last_changed,
                     check_count=check_count+1, skip_count=0""",
                  (source_name, content_hash, now_str))
        conn.commit()
        conn.close()
    except:
        pass

    # 存完整原始資料（gzip 壓縮）
    today = date.today().strftime('%Y%m%d')
    dir_path = Path(BACKUP_DIR) / today
    dir_path.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime('%H%M%S')
    filename = f"{source_name}_{ts}.json.gz"
    filepath = dir_path / filename

    payload = {
        'source': source_name,
        'timestamp': datetime.now().isoformat(),
        'content_hash': content_hash,
        'metadata': metadata or {},
        'record_count': len(data) if isinstance(data, list) else 1,
        'data': data,
    }

    try:
        with gzip.open(filepath, 'wt', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False)
        return 'saved'
    except:
        return 'error'


def load_from_backup(date_str, source_name=None):
    """
    從 raw_backup 讀取備份資料（Stage 2 重跑用）。
    date_str: 'YYYYMMDD'
    source_name: 指定來源（None = 該日全部）
    回傳: [{source, timestamp, data, metadata, content_hash}, ...]
    """
    dir_path = Path(BACKUP_DIR) / date_str
    if not dir_path.exists():
        return []

    results = []
    for f in sorted(dir_path.glob('*.json.gz')):
        if source_name and not f.name.startswith(source_name):
            continue
        try:
            with gzip.open(f, 'rt', encoding='utf-8') as fp:
                payload = json.load(fp)
            results.append(payload)
        except:
            pass
    return results


def get_fingerprint_stats():
    """取得所有來源的指紋統計（監控用）"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM content_fingerprint ORDER BY last_changed DESC")
        stats = [dict(r) for r in c.fetchall()]
        conn.close()
        return stats
    except:
        return []


def cleanup_old_backups(days=30):
    """清理超過 N 天的備份"""
    import shutil
    cutoff = date.today().toordinal() - days
    backup_path = Path(BACKUP_DIR)
    if not backup_path.exists():
        return
    removed = 0
    for d in backup_path.iterdir():
        if d.is_dir():
            try:
                dir_date = datetime.strptime(d.name, '%Y%m%d').date()
                if dir_date.toordinal() < cutoff:
                    shutil.rmtree(d)
                    removed += 1
            except ValueError:
                pass
    return removed


# ═══════════════════════════════════════════════════════════
# 2. 資料驗證與異常偵測
#    寫入前檢查數據合理性，防止髒數據污染
# ═══════════════════════════════════════════════════════════

# 各欄位的合理範圍
VALIDATION_RULES = {
    'close':        {'min': 0.1, 'max': 50000, 'label': '收盤價'},
    'change':       {'min': -5000, 'max': 5000, 'label': '漲跌'},
    'revenue_yoy':  {'min': -100, 'max': 10000, 'label': '營收年增率'},
    'revenue_cum_yoy': {'min': -100, 'max': 10000, 'label': '累積營收年增率'},
    'eps_1':        {'min': -500, 'max': 500, 'label': '季度EPS'},
    'eps_y1':       {'min': -500, 'max': 2000, 'label': '年度EPS'},
}


def validate_row(row, source='unknown'):
    """驗證單筆資料，回傳 (is_valid, warnings)"""
    warnings = []
    for field, rules in VALIDATION_RULES.items():
        val = row.get(field)
        if val is None:
            continue
        try:
            v = float(val)
            if v < rules['min'] or v > rules['max']:
                warnings.append(f"{rules['label']}({field})={v} 超出範圍 [{rules['min']}, {rules['max']}]")
        except (ValueError, TypeError):
            warnings.append(f"{rules['label']}({field})={val} 非數值")

    is_valid = len(warnings) == 0
    return is_valid, warnings


def validate_batch(rows, source='unknown'):
    """批次驗證，回傳統計"""
    total = len(rows)
    invalid_count = 0
    all_warnings = []

    for r in rows:
        valid, warns = validate_row(r, source)
        if not valid:
            invalid_count += 1
            for w in warns:
                all_warnings.append(f"{r.get('code', '?')}: {w}")

    return {
        'total': total,
        'invalid': invalid_count,
        'invalid_rate': invalid_count / total if total > 0 else 0,
        'warnings': all_warnings[:20],  # 只保留前 20 條
    }


# ═══════════════════════════════════════════════════════════
# 2b. 跳變校驗（Sanity Check）
#     比對新舊資料，偵測不合理的暴增暴跌
#     被攔截的資料不寫入主表，記錄到 quarantine 表
# ═══════════════════════════════════════════════════════════

# 跳變閾值：new/old 的倍率超過此值即觸發
JUMP_THRESHOLDS = {
    'close':        {'max_ratio': 2.0,  'label': '收盤價'},    # 單日漲跌不超過 2 倍
    'eps_1':        {'max_ratio': 10.0, 'label': '季度EPS'},   # 單季 EPS 不應跳 10 倍
    'eps_y1':       {'max_ratio': 5.0,  'label': '年度EPS'},   # 年度 EPS 不應跳 5 倍
    'revenue_yoy':  {'max_abs_jump': 500, 'label': '營收年增率'},  # 年增率不應跳 500 個百分點
    'revenue_cum_yoy': {'max_abs_jump': 300, 'label': '累積年增率'},
}


def _init_quarantine_table():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS data_quarantine (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT,
            field TEXT,
            old_value REAL,
            new_value REAL,
            reason TEXT,
            source TEXT,
            quarantined_at TEXT,
            resolved INTEGER DEFAULT 0
        )""")
        conn.commit()
        conn.close()
    except:
        pass

_init_quarantine_table()


def sanity_check(new_row, old_row, source='unknown'):
    """
    逐筆比對新舊資料，偵測不合理的跳變。
    回傳: (is_safe, blocked_fields)
    - is_safe=True：資料正常，可寫入
    - is_safe=False：有欄位被攔截，blocked_fields 列出被擋的欄位
    """
    if not old_row:
        return True, []

    code = new_row.get('code', '?')
    blocked = []

    for field, rules in JUMP_THRESHOLDS.items():
        new_val = new_row.get(field)
        old_val = old_row.get(field)
        if new_val is None or old_val is None:
            continue

        try:
            new_v = float(new_val)
            old_v = float(old_val)
        except (ValueError, TypeError):
            continue

        reason = None

        # 倍率檢查（暴漲暴跌都擋）
        if 'max_ratio' in rules and old_v != 0 and new_v != 0:
            ratio = max(abs(new_v / old_v), abs(old_v / new_v))
            if ratio > rules['max_ratio']:
                reason = f"{rules['label']} 跳變 {ratio:.1f} 倍（{old_v} → {new_v}）"

        # 絕對值跳幅檢查
        if 'max_abs_jump' in rules:
            jump = abs(new_v - old_v)
            if jump > rules['max_abs_jump']:
                reason = f"{rules['label']} 跳幅 {jump:.0f}（{old_v} → {new_v}）"

        if reason:
            blocked.append({
                'field': field, 'old': old_v, 'new': new_v, 'reason': reason
            })
            # 記錄到隔離表
            try:
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("""INSERT INTO data_quarantine
                    (code, field, old_value, new_value, reason, source, quarantined_at)
                    VALUES (?,?,?,?,?,?,?)""",
                    (code, field, old_v, new_v, reason, source,
                     datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                conn.commit()
                conn.close()
            except:
                pass

    is_safe = len(blocked) == 0
    if not is_safe:
        print(f"[跳變攔截] {code}: {', '.join(b['reason'] for b in blocked)}")

    return is_safe, blocked


def get_quarantine_list(limit=50):
    """取得未解決的隔離資料"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("""SELECT * FROM data_quarantine
                     WHERE resolved = 0
                     ORDER BY quarantined_at DESC LIMIT ?""", (limit,))
        rows = [dict(r) for r in c.fetchall()]
        conn.close()
        return rows
    except:
        return []


def resolve_quarantine(quarantine_id, action='accept'):
    """
    處理隔離資料。
    action='accept'：強制接受並寫入
    action='reject'：丟棄
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("UPDATE data_quarantine SET resolved=1 WHERE id=?", (quarantine_id,))

        if action == 'accept':
            # 讀取隔離資料，強制更新到 stocks 表
            c.execute("SELECT code, field, new_value FROM data_quarantine WHERE id=?",
                      (quarantine_id,))
            row = c.fetchone()
            if row:
                c.execute(f"UPDATE stocks SET {row[1]}=? WHERE code=?", (row[2], row[0]))

        conn.commit()
        conn.close()
        return True
    except:
        return False


# ═══════════════════════════════════════════════════════════
# 2c. 關鍵欄位異動日誌（Audit Log）
#     追蹤重要欄位的變化，幫助回溯決策依據
# ═══════════════════════════════════════════════════════════

# 需要追蹤的關鍵欄位
AUDIT_FIELDS = {
    'close': '收盤價', 'eps_1': '最新季EPS', 'eps_1q': '最新季度',
    'eps_y1': '最新年度EPS', 'eps_ytd': '近四季EPS',
    'revenue_yoy': '營收年增率', 'revenue_month': '營收月份',
    'fin_grade_1': '財務等級', 'contract_1': '合約負債',
    'div_c1': '最新現金股利',
}


def _init_audit_table():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT,
            field TEXT,
            field_label TEXT,
            old_value TEXT,
            new_value TEXT,
            changed_at TEXT
        )""")
        # 自動清理 90 天前的記錄
        c.execute("""DELETE FROM audit_log
                     WHERE changed_at < datetime('now', '-90 days')""")
        conn.commit()
        conn.close()
    except:
        pass

_init_audit_table()


def audit_changes(code, new_row, old_row):
    """比對關鍵欄位，有變化就記錄到 audit_log"""
    if not old_row:
        return 0

    changes = []
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for field, label in AUDIT_FIELDS.items():
        new_val = new_row.get(field)
        old_val = old_row.get(field)
        if new_val is None or new_val == old_val:
            continue
        changes.append((code, field, label, str(old_val), str(new_val), now_str))

    if changes:
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.executemany("""INSERT INTO audit_log
                (code, field, field_label, old_value, new_value, changed_at)
                VALUES (?,?,?,?,?,?)""", changes)
            conn.commit()
            conn.close()
        except:
            pass

    return len(changes)


def get_audit_log(limit=100, code=None):
    """取得異動日誌"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        if code:
            c.execute("""SELECT * FROM audit_log WHERE code=?
                         ORDER BY changed_at DESC LIMIT ?""", (code, limit))
        else:
            c.execute("""SELECT * FROM audit_log
                         ORDER BY changed_at DESC LIMIT ?""", (limit,))
        rows = [dict(r) for r in c.fetchall()]
        conn.close()
        return rows
    except:
        return []


# ═══════════════════════════════════════════════════════════
# 3. 熔斷機制（Circuit Breaker）
#    異常率超過閾值時，停止寫入並警報
# ═══════════════════════════════════════════════════════════

class CircuitBreaker:
    """
    熔斷器（三態）：
    - CLOSED（正常）：資料正常寫入
    - OPEN（熔斷）：停止寫入，等待冷卻
    - HALF_OPEN（試探）：冷卻後允許一次寫入，成功→CLOSED，失敗→OPEN
    """
    def __init__(self, source_name, threshold=0.1, min_samples=50, cooldown_minutes=10):
        self.source_name = source_name
        self.threshold = threshold
        self.min_samples = min_samples
        self.cooldown_minutes = cooldown_minutes
        self.state = 'CLOSED'
        self.fail_count = 0
        self.total_count = 0
        self.tripped_at = None     # 熔斷觸發時間
        self.last_check_at = None  # 最後檢查時間
        self.trip_count = 0        # 累計熔斷次數
        self._load_from_db()

    def _load_from_db(self):
        """從 DB 恢復熔斷狀態（重啟不遺失）"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""CREATE TABLE IF NOT EXISTS circuit_breaker (
                source TEXT PRIMARY KEY,
                state TEXT DEFAULT 'CLOSED',
                tripped_at TEXT,
                trip_count INTEGER DEFAULT 0
            )""")
            conn.commit()
            c.execute("SELECT state, tripped_at, trip_count FROM circuit_breaker WHERE source=?",
                      (self.source_name,))
            row = c.fetchone()
            if row:
                self.state = row[0]
                self.tripped_at = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S') if row[1] else None
                self.trip_count = row[2] or 0
                # 冷卻期已過 → 自動切到 HALF_OPEN
                if self.state == 'OPEN' and self.tripped_at:
                    elapsed = (datetime.now() - self.tripped_at).total_seconds() / 60
                    if elapsed >= self.cooldown_minutes:
                        self.state = 'HALF_OPEN'
            conn.close()
        except:
            pass

    def _save_to_db(self):
        """持久化熔斷狀態"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            tripped_str = self.tripped_at.strftime('%Y-%m-%d %H:%M:%S') if self.tripped_at else None
            c.execute("""INSERT INTO circuit_breaker (source, state, tripped_at, trip_count)
                         VALUES (?, ?, ?, ?)
                         ON CONFLICT(source) DO UPDATE SET
                         state=excluded.state, tripped_at=excluded.tripped_at,
                         trip_count=excluded.trip_count""",
                      (self.source_name, self.state, tripped_str, self.trip_count))
            conn.commit()
            conn.close()
        except:
            pass

    def check(self, validation_result):
        """根據驗證結果決定是否允許寫入"""
        self.total_count = validation_result['total']
        self.fail_count = validation_result['invalid']
        self.last_check_at = datetime.now()

        if self.total_count < self.min_samples:
            return True  # 樣本太少，不熔斷

        rate = validation_result['invalid_rate']

        if self.state == 'OPEN':
            # 檢查冷卻期
            if self.tripped_at:
                elapsed = (datetime.now() - self.tripped_at).total_seconds() / 60
                if elapsed >= self.cooldown_minutes:
                    self.state = 'HALF_OPEN'
                    print(f"[熔斷器-{self.source_name}] 冷卻完成，進入 HALF_OPEN 試探")
                    self._save_to_db()
                    # HALF_OPEN 允許這次寫入做試探
                    if rate <= self.threshold:
                        self.state = 'CLOSED'
                        print(f"[熔斷器-{self.source_name}] 試探成功，恢復 CLOSED")
                        self._save_to_db()
                        return True
                    else:
                        self.state = 'OPEN'
                        self.tripped_at = datetime.now()
                        self.trip_count += 1
                        print(f"[熔斷器-{self.source_name}] 試探失敗，重新熔斷")
                        self._save_to_db()
                        return False
            return False  # 仍在冷卻中

        if self.state == 'HALF_OPEN':
            if rate <= self.threshold:
                self.state = 'CLOSED'
                print(f"[熔斷器-{self.source_name}] 試探成功，恢復 CLOSED")
                self._save_to_db()
                return True
            else:
                self.state = 'OPEN'
                self.tripped_at = datetime.now()
                self.trip_count += 1
                print(f"[熔斷器-{self.source_name}] 試探失敗，重新熔斷")
                self._save_to_db()
                return False

        # CLOSED 狀態
        if rate > self.threshold:
            self.state = 'OPEN'
            self.tripped_at = datetime.now()
            self.trip_count += 1
            print(f"[熔斷器-{self.source_name}] 異常率 {rate*100:.1f}% 超標，熔斷！(第 {self.trip_count} 次)")
            self._save_to_db()
            return False

        return True

    def reset(self):
        """手動重置熔斷器"""
        self.state = 'CLOSED'
        self.tripped_at = None
        self.fail_count = 0
        self._save_to_db()

    def get_status(self):
        return {
            'state': self.state,
            'source': self.source_name,
            'fail_count': self.fail_count,
            'total_count': self.total_count,
            'rate': self.fail_count / self.total_count if self.total_count > 0 else 0,
            'tripped_at': self.tripped_at.strftime('%Y-%m-%d %H:%M:%S') if self.tripped_at else None,
            'trip_count': self.trip_count,
            'cooldown_minutes': self.cooldown_minutes,
            'last_check_at': self.last_check_at.strftime('%Y-%m-%d %H:%M:%S') if self.last_check_at else None,
        }


# 全域熔斷器
_breakers = {}

def get_breaker(source):
    if source not in _breakers:
        _breakers[source] = CircuitBreaker(source)
    return _breakers[source]

def get_all_breakers():
    """取得所有熔斷器狀態（含 DB 中持久化的）"""
    # 載入 DB 中所有記錄
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT source FROM circuit_breaker")
        for row in c.fetchall():
            if row[0] not in _breakers:
                _breakers[row[0]] = CircuitBreaker(row[0])
        conn.close()
    except:
        pass
    return {name: b.get_status() for name, b in _breakers.items()}


# ═══════════════════════════════════════════════════════════
# 4. 智慧優先權隊列
#    FinMind 額度有限，優先抓重要的股票
# ═══════════════════════════════════════════════════════════

def get_priority_queue(need_codes, priority_type='eps'):
    """
    根據優先權排序要抓取的股票：
    1. 觀察清單中的股票（從 localStorage 無法讀取，改用 DB 標記）
    2. 體質篩選通過的股票
    3. 市值大的（用收盤價×成交量估算）
    4. 其他
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 取各股票的優先權分數
    scores = {}
    for code in need_codes:
        score = 0
        c.execute("SELECT close, volume, fin_grade_1 FROM stocks WHERE code=?", (code,))
        r = c.fetchone()
        if r:
            # 有財務等級的加分
            if r[2] and r[2].startswith('A'):
                score += 100
            elif r[2] and r[2].startswith('B'):
                score += 50
            # 市值估算加分（取 log 避免極端值）
            if r[0] and r[1] and r[0] > 0 and r[1] > 0:
                import math
                score += int(math.log10(r[0] * r[1] + 1))
        scores[code] = score

    conn.close()

    # 按分數降序排列
    return sorted(need_codes, key=lambda c: scores.get(c, 0), reverse=True)


# ═══════════════════════════════════════════════════════════
# 5. Provider Tier 管理
#    定義來源優先級，自動降級切換
# ═══════════════════════════════════════════════════════════

PROVIDER_TIERS = {
    'revenue': [
        {'name': '政府t187ap05', 'tier': 1, 'limited': False},
        {'name': 'FinMind', 'tier': 2, 'limited': True},
    ],
    'eps_latest': [
        {'name': '政府t187ap14', 'tier': 1, 'limited': False},
        {'name': 'FinMind', 'tier': 2, 'limited': True},
    ],
    'eps_annual': [
        {'name': 'TWSE_BWIBBU', 'tier': 1, 'limited': False},
        {'name': 'TPEX_PE', 'tier': 1, 'limited': False},
        {'name': '政府t187ap14', 'tier': 2, 'limited': False},
        {'name': 'FinMind', 'tier': 3, 'limited': True},
    ],
    'dividend': [
        {'name': '政府t187ap39', 'tier': 1, 'limited': False},
        {'name': '政府t187ap45', 'tier': 1, 'limited': False},
        {'name': 'BWIBBU殖利率', 'tier': 2, 'limited': False},
        {'name': 'FinMind', 'tier': 3, 'limited': True},
    ],
    'eps_quarterly': [
        {'name': 'FinMind', 'tier': 1, 'limited': True},
    ],
    'contract_liability': [
        {'name': 'FinMind', 'tier': 1, 'limited': True},
    ],
    'price': [
        {'name': 'TWSE_openapi', 'tier': 1, 'limited': False},
        {'name': 'TPEX_openapi', 'tier': 1, 'limited': False},
    ],
    'financial_grade': [
        {'name': 'PBR/PE反推', 'tier': 1, 'limited': False},
        {'name': 'FinMind精確', 'tier': 2, 'limited': True},
    ],
}


def get_provider_status():
    """取得所有 Provider 的健康狀態（含 Tier 資訊）"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM api_health ORDER BY source")
    health = {r['source']: dict(r) for r in c.fetchall()}
    conn.close()

    result = {}
    for data_type, providers in PROVIDER_TIERS.items():
        tier_status = []
        for p in providers:
            h = health.get(p['name'], {})
            tier_status.append({
                **p,
                'status': h.get('status', 'unknown'),
                'last_success': h.get('last_success'),
                'fail_count': h.get('fail_count', 0),
                'last_record_count': h.get('last_record_count', 0),
            })
        result[data_type] = tier_status

    return result


def get_active_provider(data_type):
    """
    根據 Tier 和健康狀態，回傳當前應使用的 Provider。
    自動降級：Tier 1 失敗 → Tier 2 → Tier 3。
    回傳: {'name': str, 'tier': int, 'limited': bool} 或 None
    """
    providers = PROVIDER_TIERS.get(data_type, [])
    if not providers:
        return None

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 依 Tier 排序後逐一檢查
    sorted_providers = sorted(providers, key=lambda p: p['tier'])
    for p in sorted_providers:
        c.execute("SELECT status, fail_count FROM api_health WHERE source=?", (p['name'],))
        row = c.fetchone()
        if not row:
            # 從未用過，視為可用
            conn.close()
            return p
        if row['status'] != 'error':
            conn.close()
            return p
        # error 狀態但連續失敗 < 5 次，仍嘗試
        if row['fail_count'] < 5:
            conn.close()
            return p

    conn.close()
    # 所有來源都失敗，回傳 Tier 最低的（最後手段）
    return sorted_providers[-1] if sorted_providers else None


def log_provider_switch(data_type, from_provider, to_provider, reason):
    """記錄 Provider 降級切換事件"""
    print(f"[Provider 降級] {data_type}: {from_provider} → {to_provider}（{reason}）")
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS provider_switch_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_type TEXT, from_provider TEXT, to_provider TEXT,
            reason TEXT, switched_at TEXT
        )""")
        c.execute("INSERT INTO provider_switch_log VALUES (NULL,?,?,?,?,?)",
                  (data_type, from_provider, to_provider, reason,
                   datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        conn.close()
    except:
        pass


# ═══════════════════════════════════════════════════════════
# 6. 來源信賴度評分（Reliability Score）
#    根據歷史成功率動態調整權重
# ═══════════════════════════════════════════════════════════

def calc_reliability_scores():
    """計算各 API 來源的信賴度分數（0-100）"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM api_health")
    scores = {}
    for r in c.fetchall():
        source = r['source']
        fail_count = r['fail_count'] or 0
        last_success = r['last_success']
        record_count = r['last_record_count'] or 0

        # 基礎分：有成功過就 80 分起跳
        score = 80 if last_success else 20

        # 連續失敗扣分（每次 -15，最多扣到 0）
        score -= min(fail_count * 15, 80)

        # 資料量加分（有回傳大量資料表示來源穩定）
        if record_count > 500:
            score += 10
        elif record_count > 100:
            score += 5

        # 最近成功時間加分
        if last_success:
            try:
                last_dt = datetime.strptime(last_success, '%Y-%m-%d %H:%M:%S')
                hours_ago = (datetime.now() - last_dt).total_seconds() / 3600
                if hours_ago < 1:
                    score += 10
                elif hours_ago < 24:
                    score += 5
                elif hours_ago > 72:
                    score -= 10
            except:
                pass

        scores[source] = max(0, min(100, score))
    conn.close()
    return scores


# ═══════════════════════════════════════════════════════════
# 7. 資料鮮度偵測（Staleness Detector）
#    偵測資料是否「邏輯性過期」
# ═══════════════════════════════════════════════════════════

def detect_staleness():
    """檢查各項資料的鮮度，回傳過期警告"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today = date.today()
    weekday = today.weekday()  # 0=Mon, 6=Sun
    alerts = []

    # 股價：交易日 15:00 後應該有今天的
    c.execute("SELECT updated_at FROM stocks ORDER BY updated_at DESC LIMIT 1")
    r = c.fetchone()
    if r and r[0]:
        try:
            last_update = datetime.strptime(r[0], '%Y-%m-%d %H:%M:%S')
            hours_ago = (datetime.now() - last_update).total_seconds() / 3600
            if weekday < 5 and hours_ago > 20:  # 交易日超過20小時沒更新
                alerts.append({
                    'type': 'staleness', 'severity': 'warning',
                    'data': '股價', 'message': f'已 {hours_ago:.0f} 小時未更新',
                    'last_update': r[0],
                })
        except:
            pass

    # 營收：每月 1-10 日應該有上月營收陸續公布
    if today.day >= 12 and today.day <= 15:
        expected_month = today.month - 1 if today.month > 1 else 12
        c.execute("SELECT COUNT(*) FROM stocks WHERE revenue_month = ?", (expected_month,))
        rev_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM stocks WHERE revenue_yoy IS NOT NULL")
        total_rev = c.fetchone()[0]
        if total_rev > 0 and rev_count / total_rev < 0.5:
            alerts.append({
                'type': 'staleness', 'severity': 'info',
                'data': '營收', 'message': f'上月營收僅 {rev_count}/{total_rev} 支已更新',
            })

    # EPS：季報期限後應該有新季度
    from scraper import _expected_latest_quarter
    expected_q = _expected_latest_quarter()
    c.execute("SELECT COUNT(*) FROM stocks WHERE eps_1q = ?", (expected_q,))
    eps_new = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM stocks WHERE eps_1 IS NOT NULL")
    eps_total = c.fetchone()[0]
    if eps_total > 0 and eps_new / eps_total < 0.3:
        alerts.append({
            'type': 'staleness', 'severity': 'info',
            'data': 'EPS', 'message': f'最新季度 {expected_q} 僅 {eps_new}/{eps_total} 支',
        })

    conn.close()
    return alerts


# ═══════════════════════════════════════════════════════════
# 8. FinMind 額度預測
#    追蹤消耗速度，預測今天是否夠用
# ═══════════════════════════════════════════════════════════

_finmind_calls_today = 0
_finmind_day = None

def track_finmind_call(count=1):
    """記錄 FinMind API 呼叫次數"""
    global _finmind_calls_today, _finmind_day
    today = date.today()
    if _finmind_day != today:
        _finmind_calls_today = 0
        _finmind_day = today
    _finmind_calls_today += count


def should_skip_finmind(is_priority=False):
    """
    額度預警：超過 90% 時，非重要股票自動跳過。
    is_priority=True 的股票（觀察清單/A 級）不受限。
    回傳 True 表示應跳過。
    """
    global _finmind_calls_today
    daily_limit = 2000
    if _finmind_calls_today < daily_limit * 0.9:
        return False  # 還沒到 90%，正常抓
    if is_priority:
        return False  # 重要股票不跳過
    print(f"[額度預警] FinMind 已用 {_finmind_calls_today}/{daily_limit}（≥90%），跳過非重要股票")
    return True


def get_finmind_quota():
    """預測 FinMind 額度使用狀況"""
    global _finmind_calls_today
    daily_limit = 2000  # 免費帳號估計
    used = _finmind_calls_today
    remaining = max(0, daily_limit - used)

    # 估算還需要多少
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM stocks WHERE eps_2 IS NULL")
    need_eps = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM stocks WHERE contract_1 IS NULL")
    need_cl = c.fetchone()[0]
    conn.close()

    total_need = need_eps + need_cl
    days_to_complete = math.ceil(total_need / daily_limit) if daily_limit > 0 else 999

    return {
        'daily_limit': daily_limit,
        'used_today': used,
        'remaining': remaining,
        'pct_used': round(used / daily_limit * 100, 1),
        'stocks_need_eps': need_eps,
        'stocks_need_contract': need_cl,
        'est_days_to_complete': days_to_complete,
    }


# ═══════════════════════════════════════════════════════════
# 9. 多源仲裁（Data Arbitration）
#    同一資料有多來源時，比對差異並標記
# ═══════════════════════════════════════════════════════════

def arbitrate_values(values_by_source, tolerance_pct=5):
    """
    多來源數值仲裁。
    values_by_source: {'政府API': 66.26, 'BWIBBU': 66.25, 'FinMind': 66.26}
    回傳: {'best_value': 66.26, 'confidence': 'high', 'discrepancies': []}
    """
    if not values_by_source:
        return {'best_value': None, 'confidence': 'none', 'discrepancies': []}

    sources = list(values_by_source.items())
    if len(sources) == 1:
        return {'best_value': sources[0][1], 'confidence': 'single', 'discrepancies': []}

    values = [v for _, v in sources if v is not None]
    if not values:
        return {'best_value': None, 'confidence': 'none', 'discrepancies': []}

    # 中位數作為基準
    median = sorted(values)[len(values) // 2]
    if median == 0:
        median = 0.01  # 避免除零

    # 檢查偏差
    discrepancies = []
    for name, val in sources:
        if val is not None and abs(val - median) / abs(median) * 100 > tolerance_pct:
            discrepancies.append({
                'source': name, 'value': val,
                'deviation': round((val - median) / abs(median) * 100, 2),
            })

    confidence = 'high' if not discrepancies else ('medium' if len(discrepancies) == 1 else 'low')

    # 最可信數值：無異議用中位數，有異議用來源最多的值
    from collections import Counter
    rounded = [round(v, 2) for v in values]
    most_common = Counter(rounded).most_common(1)[0][0]

    return {
        'best_value': most_common,
        'confidence': confidence,
        'median': median,
        'discrepancies': discrepancies,
    }


# ═══════════════════════════════════════════════════════════
# 10. 多源交叉校驗（Cross Validation）
#     定期抽樣比對 DB 資料 vs 外部來源，標記差異
# ═══════════════════════════════════════════════════════════

def cross_validate(sample_size=20):
    """
    抽樣交叉校驗：從 DB 抽 N 支股票，比對群益 vs 政府API 的關鍵數字。
    回傳: {'checked': N, 'mismatches': [...], 'ok': N}
    """
    import requests
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 隨機抽樣有資料的股票
    c.execute("SELECT code, name, close, eps_y1, revenue_yoy FROM stocks WHERE close IS NOT NULL ORDER BY RANDOM() LIMIT ?", (sample_size,))
    samples = [dict(r) for r in c.fetchall()]
    conn.close()

    # 取政府 API 的股價和營收作為比對基準
    try:
        twse = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
                           headers={"User-Agent": "Mozilla/5.0"}, timeout=10).json()
        price_map = {str(r.get('Code','')).strip(): float(r.get('ClosingPrice','0').replace(',','') or 0) for r in twse}
    except:
        price_map = {}

    try:
        rev_data = requests.get("https://openapi.twse.com.tw/v1/openData/t187ap05_L",
                               headers={"User-Agent": "Mozilla/5.0"}, timeout=10).json()
        rev_map = {}
        for r in rev_data:
            code = str(r.get('公司代號','')).strip()
            yoy = r.get('營業收入-去年同月增減(%)')
            if code and yoy:
                try: rev_map[code] = float(yoy)
                except: pass
    except:
        rev_map = {}

    mismatches = []
    ok_count = 0
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for s in samples:
        code = s['code']
        issues = []

        # 股價比對（容許 0.5% 差異，因為可能是不同時間點）
        if code in price_map and s['close'] and price_map[code] > 0:
            db_price = s['close']
            api_price = price_map[code]
            diff_pct = abs(db_price - api_price) / api_price * 100
            if diff_pct > 5:  # 超過 5% 才算異常
                issues.append({
                    'field': '股價', 'db': db_price, 'api': api_price,
                    'diff': f'{diff_pct:.1f}%', 'source': '政府API'
                })

        # 營收年增率比對（容許 1% 差異）
        if code in rev_map and s['revenue_yoy'] is not None:
            db_yoy = s['revenue_yoy']
            api_yoy = rev_map[code]
            diff = abs(db_yoy - api_yoy)
            if diff > 1:
                issues.append({
                    'field': '營收年增率', 'db': db_yoy, 'api': api_yoy,
                    'diff': f'{diff:.2f}%', 'source': '政府API'
                })

        if issues:
            mismatches.append({'code': code, 'name': s['name'], 'issues': issues})
        else:
            ok_count += 1

    # 寫入校驗記錄
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS cross_validation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            checked_at TEXT, sample_size INTEGER, ok_count INTEGER, mismatch_count INTEGER,
            details TEXT)""")
        c.execute("INSERT INTO cross_validation (checked_at, sample_size, ok_count, mismatch_count, details) VALUES (?,?,?,?,?)",
                  (now_str, len(samples), ok_count, len(mismatches), json.dumps(mismatches, ensure_ascii=False)))
        conn.commit()
        conn.close()
    except:
        pass

    return {
        'checked': len(samples),
        'ok': ok_count,
        'mismatches': mismatches,
        'checked_at': now_str,
    }


def cross_validate_revenue():
    """
    營收交叉校驗：全量比對 monthly_revenue（政府API）vs 群益 zch 的月營收。
    每週日執行，驗證政府 API 寫入的營收資料是否正確。
    差異 > 1% 的記入 cross_validation 並印警告。
    """
    import requests
    from bs4 import BeautifulSoup

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # 全量：從 monthly_revenue 取所有有資料的股票
    rows = conn.execute("""
        SELECT DISTINCT code FROM monthly_revenue
        WHERE revenue IS NOT NULL AND revenue > 0
    """).fetchall()
    codes = [r['code'] for r in rows]

    # 取得最近一筆營收的年月（用來跟群益比對）
    rev_map = {}
    for code in codes:
        r = conn.execute("""
            SELECT year, month, revenue FROM monthly_revenue
            WHERE code=? AND revenue IS NOT NULL
            ORDER BY year DESC, month DESC LIMIT 1
        """, (code,)).fetchone()
        if r:
            rev_map[code] = {'year': r['year'], 'month': r['month'], 'revenue': r['revenue']}
    conn.close()

    if not rev_map:
        print("[營收校驗] 無資料可比對")
        return {'checked': 0, 'ok': 0, 'mismatches': []}

    # 從群益 zch 逐支抓取營收比對
    mismatches = []
    ok_count = 0
    checked = 0
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'})

    for code, db_data in rev_map.items():
        try:
            url = f"https://stock.capital.com.tw/z/zc/zch/zch.djhtm?a={code}"
            r = session.get(url, timeout=15)
            r.encoding = 'big5'
            soup = BeautifulSoup(r.text, 'html.parser')

            target_ym = f"{db_data['year'] - 1911}/{db_data['month']:02d}"
            capital_revenue = None

            for t in soup.find_all('table'):
                for row in t.find_all('tr'):
                    cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
                    if not cells or len(cells) < 2:
                        continue
                    if cells[0] == target_ym:
                        # 群益營收單位是仟元
                        rev_str = cells[1].replace(',', '').strip()
                        try:
                            capital_revenue = float(rev_str) * 1000  # 轉為元
                        except:
                            pass
                        break
                if capital_revenue is not None:
                    break

            if capital_revenue is None:
                continue

            checked += 1
            db_revenue = db_data['revenue']

            # 政府 API 單位是仟元，群益也轉成元了，比對差異
            if db_revenue > 0 and capital_revenue > 0:
                diff_pct = abs(db_revenue - capital_revenue) / db_revenue * 100
                if diff_pct > 1:
                    mismatches.append({
                        'code': code,
                        'year_month': target_ym,
                        'db_revenue': db_revenue,
                        'capital_revenue': capital_revenue,
                        'diff_pct': round(diff_pct, 2),
                    })
                    print(f"[營收校驗] {code} {target_ym} 差異 {diff_pct:.2f}%"
                          f"（政府API: {db_revenue:,.0f} vs 群益: {capital_revenue:,.0f}）")
                else:
                    ok_count += 1
        except:
            pass

        # 避免太快被擋
        import time
        time.sleep(0.3)

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 寫入校驗記錄
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS cross_validation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            checked_at TEXT, sample_size INTEGER, ok_count INTEGER, mismatch_count INTEGER,
            details TEXT)""")
        c.execute("INSERT INTO cross_validation (checked_at, sample_size, ok_count, mismatch_count, details) VALUES (?,?,?,?,?)",
                  (now_str, checked, ok_count, len(mismatches),
                   json.dumps({'type': 'revenue', 'mismatches': mismatches}, ensure_ascii=False)))
        conn.commit()
        conn.close()
    except:
        pass

    print(f"[營收校驗] 完成：共 {checked} 支，{ok_count} 支一致，{len(mismatches)} 支有差異")
    return {'checked': checked, 'ok': ok_count, 'mismatches': mismatches}


def get_latest_validation():
    """取得最近一次交叉校驗結果"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM cross_validation ORDER BY id DESC LIMIT 1")
        row = c.fetchone()
        conn.close()
        if row:
            result = dict(row)
            result['details'] = json.loads(result['details']) if result['details'] else []
            return result
    except:
        pass
    return None


def get_coverage_map():
    """
    資料斷層地圖：檢查各項關鍵資料的覆蓋率。
    回傳按季度/月份的完成度，讓你知道哪些資料有缺口。
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM stocks")
    total = c.fetchone()[0]
    if total == 0:
        conn.close()
        return {'total': 0, 'coverage': []}

    coverage = []

    # 股價覆蓋率
    c.execute("SELECT COUNT(*) FROM stocks WHERE close IS NOT NULL")
    n = c.fetchone()[0]
    coverage.append({'item': '收盤價', 'have': n, 'total': total,
                     'pct': round(n/total*100, 1)})

    # 營收覆蓋率（依月份）
    c.execute("SELECT revenue_month, COUNT(*) FROM stocks WHERE revenue_yoy IS NOT NULL GROUP BY revenue_month ORDER BY revenue_month DESC LIMIT 1")
    r = c.fetchone()
    if r:
        coverage.append({'item': f'營收（{r[0]}月）', 'have': r[1], 'total': total,
                         'pct': round(r[1]/total*100, 1)})

    # 季度 EPS 覆蓋率
    c.execute("SELECT COUNT(*) FROM stocks WHERE eps_1 IS NOT NULL")
    n = c.fetchone()[0]
    coverage.append({'item': '季度EPS', 'have': n, 'total': total,
                     'pct': round(n/total*100, 1)})

    # 年度 EPS
    c.execute("SELECT COUNT(*) FROM stocks WHERE eps_y1 IS NOT NULL")
    n = c.fetchone()[0]
    coverage.append({'item': '年度EPS', 'have': n, 'total': total,
                     'pct': round(n/total*100, 1)})

    # 股利
    c.execute("SELECT COUNT(*) FROM stocks WHERE div_c1 IS NOT NULL")
    n = c.fetchone()[0]
    coverage.append({'item': '股利', 'have': n, 'total': total,
                     'pct': round(n/total*100, 1)})

    # 合約負債
    c.execute("SELECT COUNT(*) FROM stocks WHERE contract_1 IS NOT NULL")
    n = c.fetchone()[0]
    coverage.append({'item': '合約負債', 'have': n, 'total': total,
                     'pct': round(n/total*100, 1)})

    # 財務等級
    c.execute("SELECT COUNT(*) FROM stocks WHERE fin_grade_1 IS NOT NULL")
    n = c.fetchone()[0]
    coverage.append({'item': '財務等級', 'have': n, 'total': total,
                     'pct': round(n/total*100, 1)})

    # 產業別
    c.execute("SELECT COUNT(*) FROM stocks WHERE industry IS NOT NULL")
    n = c.fetchone()[0]
    coverage.append({'item': '產業別', 'have': n, 'total': total,
                     'pct': round(n/total*100, 1)})

    conn.close()
    return {'total': total, 'coverage': coverage}


# ═══════════════════════════════════════════════════════════
# 10. 重大訊息分類（Material News Classification）
#     從公開資訊觀測站抓取，rule-based 三層分類
# ═══════════════════════════════════════════════════════════

import re as _re

def _init_news_table():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS material_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT,
            name TEXT,
            date TEXT,
            time TEXT,
            subject TEXT,
            description TEXT,
            tier INTEGER,
            matched_rule TEXT,
            direction TEXT DEFAULT 'neutral',
            link TEXT,
            created_at TEXT,
            UNIQUE(code, date, time, subject)
        )""")
        c.execute("""CREATE INDEX IF NOT EXISTS idx_news_code_date
                     ON material_news(code, created_at DESC)""")
        # 90 天前自動清理
        c.execute("DELETE FROM material_news WHERE created_at < datetime('now', '-90 days')")
        conn.commit()
        conn.close()
    except:
        pass

_init_news_table()

# ── 分類規則 ──

def _has(text, keyword):
    return keyword in text

def _any(text, keywords):
    return any(k in text for k in keywords)

def _has_amount(text):
    """檢查內文是否有具體金額（數字+萬/億/元）"""
    return bool(_re.search(r'\d[\d,.]*\s*[萬億元]', text))

# Tier 0：丟棄（例行性，無決策價值）— 9 條
_TIER0_RULES = [
    ('召開通知',   lambda s, d: _has(s, '召開') and _any(s, ['董事會', '股東會', '股東常會', '股東臨時會'])),
    ('法說會通知', lambda s, d: _has(s, '法人說明會') and _any(s, ['通知', '旁聽', '時間', '地點'])),
    ('除權息基準', lambda s, d: _any(s, ['除權', '除息', '基準日', '停止過戶'])),
    ('庫藏股進度', lambda s, d: _has(s, '庫藏股') and _any(s, ['買回進度', '每日買回', '執行情形']) and not _any(s, ['決議', '通過買回'])),
    ('內部人轉讓', lambda s, d: _has(s, '申報轉讓')),
    ('補充更正',   lambda s, d: _any(s, ['補充公告', '更正公告', '更正本公司'])),
    ('更名公告',   lambda s, d: _any(s, ['更名為', '名稱變更'])),
    ('年報上傳',   lambda s, d: _any(s, ['年報']) and _any(s, ['上傳', '申報', '公告'])),
    ('私募進度',   lambda s, d: _has(s, '私募') and _any(s, ['進度', '辦理情形'])),
]

# Tier 2：重要催化劑 — 20 條（含方向性）
# 格式: (規則名, 判斷函式, 方向 positive/negative/neutral)
# 規則有優先級：依序檢查，命中第一條就停止
_TIER2_RULES = [
    # 警鈴（流動性/信用危機，極罕見但致命）
    ('退票',         lambda s, d: _any(s, ['退票', '存款不足']),                                    'alarm'),
    ('下市終止',     lambda s, d: _any(s, ['全額交割', '變更交易', '停止交易', '下市', '終止上市']),  'alarm'),
    ('破產重整',     lambda s, d: _any(s, ['重整', '破產', '無法償還', '違約']) and not _any(s, ['合約違約金']), 'alarm'),
    ('會計保留',     lambda s, d: _any(s, ['保留意見', '無法表示意見', '更換會計師']) and not _any(s, ['例行輪換']), 'alarm'),
    # 正向催化劑
    ('併購合併',     lambda s, d: _any(s, ['併購', '收購', '公開收購', '股份轉換']) or (_has(s, '合併') and not _any(s, ['財務報告', '財報', '營收', '報表', '自結', '損益'])),  'positive'),
    ('財測上修',     lambda s, d: _any(s, ['上修', '調升']) and _any(d, ['財測', '業績預估', '獲利預估', '營收預估']), 'positive'),
    ('重大訂單',     lambda s, d: _any(s, ['重大訂單', '重大合約', '取得訂單', '簽訂合約']),          'positive'),
    ('擴建產能',     lambda s, d: _any(s, ['新廠', '擴建', '擴廠', '產能擴張', '新建廠房', '興建']), 'positive'),
    ('技術里程碑',   lambda s, d: _any(s, ['取得', '通過']) and _any(s, ['專利', '認證', '藥證', 'FDA', '執照', '許可']), 'positive'),
    ('策略合作',     lambda s, d: _any(s, ['策略合作', '合作協議', '共同開發', '合資', 'MOU', '備忘錄']), 'positive'),
    ('庫藏股決議',   lambda s, d: _has(s, '庫藏股') and _any(s, ['決議', '通過買回', '董事會決議']),  'positive'),
    ('大股東增持',   lambda s, d: _any(s, ['持股增加']) or (_any(s, ['董事', '大股東']) and _has(s, '取得')), 'positive'),
    ('重大投資',     lambda s, d: _any(s, ['轉投資', '取得股權']),                                   'positive'),
    ('技術授權',     lambda s, d: _any(s, ['技術授權', '授權合約', '專利授權']),                      'positive'),
    ('減資退還',     lambda s, d: _has(s, '減資') and _any(d, ['退還股款', '現金退還']),              'positive'),
    ('法說會內容',   lambda s, d: _has(s, '法人說明會') and not _any(s, ['通知', '旁聽', '時間', '地點']), 'positive'),
    ('營收創高',     lambda s, d: _any(s, ['營收創', '營收新高', '歷史新高']) and _any(d, ['營收', '營業收入']), 'positive'),
    # 中性
    ('處分資產',     lambda s, d: _any(s, ['處分', '出售']) and _any(s, ['資產', '不動產', '土地', '廠房', '持股', '股權']), 'neutral'),
    # 負向催化劑
    ('減損虧損',     lambda s, d: _any(s, ['重大虧損', '減損', '資產減損', '下修', '財測下修']),      'negative'),
    ('高管突發異動', lambda s, d: _any(s, ['董事長', '總經理', '財務長', '財務主管', '技術長']) and _any(s, ['辭任', '解任']) and not _any(s, ['屆期', '改選', '退休', '任期屆滿']), 'negative'),
    ('高管正常異動', lambda s, d: _any(s, ['董事長', '總經理', '財務長', '財務主管', '技術長']) and _any(s, ['異動', '變動', '新任', '到任', '接任', '出任', '改選']), 'neutral'),
    ('訴訟仲裁',     lambda s, d: _any(s, ['訴訟', '仲裁', '判決']),                                'negative'),
    ('停工事故',     lambda s, d: _any(s, ['停工', '火災', '重大災害', '事故']) and not _any(s, ['春節', '歲修', '年節', '連假', '例行', '維修', '保養']), 'negative'),
    ('重大裁罰',     lambda s, d: _any(s, ['裁罰', '罰鍰', '行政處分']),                            'negative'),
    ('財報重編',     lambda s, d: _any(s, ['重編', '財務報表更正']),                                  'negative'),
    ('增資減資',     lambda s, d: _any(s, ['現金增資']) or (_has(s, '減資') and _any(d, ['彌補虧損'])), 'negative'),
    ('客戶供應商',   lambda s, d: _any(s, ['主要客戶', '主要供應商', '重要客戶']) and _any(s, ['變動', '異動', '終止']), 'negative'),
]


def classify_news(subject, description=''):
    """
    分類重大訊息。
    回傳: (tier, matched_rule, direction)
    tier: 0=丟棄, 1=摘要, 2=重要
    direction: positive/negative/neutral
    """
    s = subject or ''
    d = (subject or '') + (description or '')

    # Step 1: 先檢查 Tier 0
    for rule_name, fn in _TIER0_RULES:
        if fn(s, d):
            return 0, rule_name, 'neutral'

    # Step 2: 檢查 Tier 2
    for rule_name, fn, direction in _TIER2_RULES:
        if fn(s, d):
            return 2, rule_name, direction

    # Step 3: 未分類 → Tier 1（進摘要）
    return 1, '未分類', 'neutral'


def fetch_material_news():
    """
    從公開資訊觀測站抓取最新重大訊息，分類後存入 DB。
    只儲存觀察清單 + 體質通過的股票（有 stock_state 記錄的）。
    回傳: {'new': int, 'tier0': int, 'tier1': int, 'tier2': int}
    """
    import requests

    # 取得追蹤中的股票代碼
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT stock_id FROM stock_state")
    tracked = {r[0] for r in c.fetchall()}

    stats = {'new': 0, 'tier0': 0, 'tier1': 0, 'tier2': 0}

    # 上市 + 上櫃
    apis = [
        ('https://openapi.twse.com.tw/v1/openData/t187ap04_L',
         '公司代號', '公司名稱', '發言日期', '發言時間', '主旨 ', '說明'),
        ('https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O',
         'SecuritiesCompanyCode', 'CompanyName', '發言日期', '發言時間', '主旨', '說明'),
    ]

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for url, ck, nk, dk, tk, sk, desc_k in apis:
        try:
            r = requests.get(url, timeout=15)
            data = r.json()
        except:
            continue

        for item in data:
            code = str(item.get(ck, '')).strip()
            if not code or code not in tracked:
                continue

            name = str(item.get(nk, '')).strip()
            news_date = str(item.get(dk, '')).strip()
            news_time = str(item.get(tk, '')).strip()
            subject = str(item.get(sk, '')).strip()
            description = str(item.get(desc_k, '')).strip()

            if not subject:
                continue

            # 分類
            tier, rule, direction = classify_news(subject, description)

            # 公開資訊觀測站連結
            mops_link = f"https://mops.twse.com.tw/mops/web/t05sr01_1"

            # 存入（UNIQUE 去重，已存的不重複）
            try:
                c.execute("""INSERT INTO material_news
                    (code, name, date, time, subject, description, tier, matched_rule, direction, link, created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (code, name, news_date, news_time, subject, description, tier, rule, direction, mops_link, now_str))
                stats['new'] += 1
                stats[f'tier{tier}'] += 1
            except:
                pass  # UNIQUE 衝突，已存在

    conn.commit()
    conn.close()

    if stats['new'] > 0:
        print(f"[重大訊息] 新增 {stats['new']} 則（Tier0:{stats['tier0']} Tier1:{stats['tier1']} Tier2:{stats['tier2']}）")

    return stats


def fetch_moneydj_news():
    """
    從 MoneyDJ 爬取個股情報 + 產業分析，
    比對觀察清單股票名稱，命中的分類後存入 DB。
    """
    import requests
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("[MoneyDJ] 需要 beautifulsoup4：pip3 install beautifulsoup4")
        return {'new': 0}

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 取所有股票名稱（用於標題比對）
    c.execute("SELECT code, name FROM stocks WHERE close IS NOT NULL")
    stock_map = {}  # name → code
    for r in c.fetchall():
        name = r['name']
        if name and len(name) >= 2:
            stock_map[name] = r['code']
    # 按名稱長度降序排列，優先匹配最長的（避免「精華」誤配「精華生醫」）
    stock_names_sorted = sorted(stock_map.keys(), key=len, reverse=True)

    # 追蹤中的股票
    c.execute("SELECT DISTINCT stock_id FROM stock_state")
    tracked = {r[0] for r in c.fetchall()}

    categories = [
        ('https://www.moneydj.com/kmdj/common/listnewarticles.aspx?svc=NW&a=X0200000', '個股情報'),
        ('https://www.moneydj.com/kmdj/common/listnewarticles.aspx?svc=NW&a=X0300000', '產業分析'),
    ]

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'}
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    today_str = date.today().strftime('%m/%d')
    stats = {'new': 0}

    for url, cat_name in categories:
        try:
            r = requests.get(url, headers=headers, timeout=15)
            r.encoding = 'utf-8'
            soup = BeautifulSoup(r.text, 'html.parser')
        except:
            continue

        links = soup.find_all('a', href=lambda h: h and 'newsviewer' in h)

        for a in links:
            title = a.get_text(strip=True)
            if not title:
                continue

            # 取連結
            href = a.get('href', '')
            if href and not href.startswith('http'):
                href = 'https://www.moneydj.com' + href

            # 取日期（從同一行的 td）
            tr = a.find_parent('tr')
            tds = tr.find_all('td') if tr else []
            news_date = tds[0].get_text(strip=True) if tds else ''

            # 比對股票名稱（長名優先 + 短名邊界檢查）
            matched_code = None
            matched_name = None
            for name in stock_names_sorted:
                idx = title.find(name)
                if idx < 0:
                    continue
                # 2 字短名才做邊界檢查（避免「精華」配到「精華生醫」）
                if len(name) <= 2:
                    after = idx + len(name)
                    if after < len(title) and '\u4e00' <= title[after] <= '\u9fff':
                        continue
                matched_code = stock_map[name]
                matched_name = name
                break

            if not matched_code or matched_code not in tracked:
                continue

            # 分類（用跟重大訊息同一套規則）
            tier, rule, direction = classify_news(title, '')

            # Tier 0 的新聞也存，因為 MoneyDJ 的新聞格式跟公開資訊不同
            # 但把被 Tier 0 規則命中的降為 Tier 1（MoneyDJ 新聞本身有編輯價值）
            if tier == 0:
                tier = 1
                rule = cat_name

            # 存入
            try:
                c.execute("""INSERT INTO material_news
                    (code, name, date, time, subject, description, tier, matched_rule, direction, link, created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (matched_code, matched_name, news_date, '', title, f'[MoneyDJ {cat_name}]',
                     tier, rule, direction, href, now_str))
                stats['new'] += 1
            except:
                pass  # UNIQUE 衝突

    conn.commit()
    conn.close()

    if stats['new'] > 0:
        print(f"[MoneyDJ] 新增 {stats['new']} 則")

    return stats


def get_recent_news(code=None, tier_min=1, limit=50):
    """
    取得最近的重大訊息（預設只取 Tier 1+2）。
    含冷卻機制：同公司 Tier 2 訊息 7 天內第二則起標記 cooled=True。
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        if code:
            c.execute("""SELECT * FROM material_news
                         WHERE code=? AND tier >= ?
                         ORDER BY created_at DESC LIMIT ?""", (code, tier_min, limit))
        else:
            c.execute("""SELECT * FROM material_news
                         WHERE tier >= ?
                         ORDER BY created_at DESC LIMIT ?""", (tier_min, limit))
        rows = [dict(r) for r in c.fetchall()]
        conn.close()

        # 冷卻機制：同公司 Tier 2，7 天內只有第一則維持 Tier 2，後續標記 cooled
        seen_t2 = {}  # code → first_date
        for r in rows:
            if r['tier'] == 2:
                if r['code'] in seen_t2:
                    # 檢查距離第一則是否 < 7 天
                    first = seen_t2[r['code']]
                    r['cooled'] = True  # 標記被冷卻（前端可降級顯示）
                else:
                    seen_t2[r['code']] = r['created_at']
                    r['cooled'] = False
            else:
                r['cooled'] = False

        return rows
    except:
        return []


def auto_archive_old_news():
    """
    超過 20 天未處理的 Tier 2 新聞，自動存摘要到 audit_log，
    然後標記為 dismissed。重要資訊不堆積也不遺失。
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""SELECT id, code, name, date, subject, matched_rule
                     FROM material_news
                     WHERE tier = 2 AND status IS NULL
                     AND created_at < datetime('now', '-20 days')""")
        old_news = c.fetchall()
        if not old_news:
            conn.close()
            return 0

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        archived = 0
        for nid, code, name, news_date, subject, rule in old_news:
            summary = f"[{rule}] {subject}".replace('\r', '').replace('\n', ' ')[:80]
            c.execute("""INSERT INTO audit_log
                (code, field, field_label, old_value, new_value, changed_at)
                VALUES (?, 'news_archived', '自動歸檔新聞', ?, ?, ?)""",
                (code, news_date or '', summary, now_str))
            c.execute("UPDATE material_news SET status='dismissed' WHERE id=?", (nid,))
            archived += 1

        conn.commit()
        conn.close()
        if archived:
            print(f"[自動歸檔] {archived} 則超過 20 天的重要新聞已存入紀錄")
        return archived
    except:
        return 0


# ═══════════════════════════════════════════════════════════
# 11. 每日狀態快照（Stock State）
#     追蹤觀察清單 + 體質通過的股票狀態變化
# ═══════════════════════════════════════════════════════════

# 預設估值參數（與前端 DEFAULT_GLOBAL_VAL 一致）
DEFAULT_PE_HIGH = 18
DEFAULT_PE_LOW = 10
DEFAULT_YLD_HIGH = 5.5
DEFAULT_YLD_MAX = 6.0
DEFAULT_YLD_FLOOR = 5.0


def _calc_matrix_grade(pe, yld, pe_high=None, pe_low=None, yld_max=None, yld_high=None, yld_floor=None):
    """
    矩陣等級計算（與前端 _calcMatrixGrade 一致）。
    回傳: AA/A1/A2/A/BA1/BA2/B1/B2/觀察/臨界點/X
    """
    if pe is None or pe <= 0 or yld is None or yld <= 0:
        return 'X'
    if not pe_high: pe_high = DEFAULT_PE_HIGH
    if not pe_low: pe_low = DEFAULT_PE_LOW
    if not yld_max: yld_max = DEFAULT_YLD_MAX
    if not yld_high: yld_high = DEFAULT_YLD_HIGH
    if not yld_floor: yld_floor = DEFAULT_YLD_FLOOR

    pe_fair = (pe_high + pe_low) / 2
    pe_above = (pe_high + pe_fair) / 2
    pe_below = (pe_fair + pe_low) / 2
    pe_cols = [(-9999, pe_low), (pe_low, pe_below), (pe_below, pe_fair),
               (pe_fair, pe_above), (pe_above, pe_high), (pe_high, 9999)]
    y_rows = [(yld_max, 9999), (yld_high, yld_max), (yld_floor, yld_high), (-9999, yld_floor)]
    grades = [
        ['AA', 'A2', 'BA2', '觀察', '臨界點', 'X'],
        ['A1', 'A', 'B2', '臨界點', 'X', 'X'],
        ['BA1', 'B1', '臨界點', 'X', 'X', 'X'],
        ['觀察', '臨界點', 'X', 'X', 'X', 'X'],
    ]
    col = next((i for i, (lo, hi) in enumerate(pe_cols) if pe >= lo and pe < hi), -1)
    row = next((i for i, (lo, hi) in enumerate(y_rows) if yld >= lo and yld < hi), -1)
    if col >= 0 and row >= 0:
        return grades[row][col]
    return 'X'


def _calc_priority_grade(row, close):
    """
    等級優先順序：預估 > 系統 > 沈董 > 綜合 > 加權 > 近四季 > X
    每個等級都用矩陣計算（PE + 殖利率）。
    回傳: (grade, source)
    """
    # 1. 預估等級（使用者手動輸入，存在 user_estimates）
    # DB 裡沒有預算好的預估 PE/殖利率，跳過（前端才有）

    # 2. 系統等級
    sys_pe = row.get('sys_ann_pe')
    sys_yld = row.get('sys_ann_yld')
    if sys_pe and sys_yld and sys_pe > 0 and sys_yld > 0:
        g = _calc_matrix_grade(sys_pe, sys_yld)
        if g:
            return g, 'sys'

    # 3. 沈董等級
    shen_eps = _calc_shen_eps(row)
    if shen_eps and shen_eps > 0 and close and close > 0:
        shen_pe = round(close / shen_eps, 2)
        div_total = (row.get('div_c1') or 0) + (row.get('div_s1') or 0)
        eps_y1 = row.get('eps_y1')
        shen_yld = 0
        if div_total > 0 and eps_y1 and eps_y1 > 0:
            payout = min(1.0, div_total / eps_y1)
            shen_div = shen_eps * payout
            shen_yld = round(shen_div / close * 100, 2)
        if shen_pe > 0 and shen_yld > 0:
            g = _calc_matrix_grade(shen_pe, shen_yld)
            if g:
                return g, 'shen'

    # 4~6. 綜合/加權/近四季 — 需要前端的 blend/weighted 計算，後端簡化跳過
    # 這些在前端 calcDerived 裡算，後端沒有完整資料

    # 7. 都沒有
    return 'X', 'none'


def _calc_val_levels(close, shen_eps, shen_div, blend_div,
                     pe_low=None, pe_high=None, yld_high=None, yld_max=None,
                     est_eps=None, est_div=None):
    """
    計算評價門檻（AA/A1/A2/A/長期6%）和當前等級。
    EPS/股利優先用預估值（est_eps/est_div），沒有才用沈董值，與前端一致。
    回傳: {val_aa, val_a1, val_a2, val_a, val_lt6, val_level, discount_pct}
    """
    if pe_low is None: pe_low = DEFAULT_PE_LOW
    if pe_high is None: pe_high = DEFAULT_PE_HIGH
    if yld_high is None: yld_high = DEFAULT_YLD_HIGH
    if yld_max is None: yld_max = DEFAULT_YLD_MAX

    # 與前端一致：預估值優先，沒有才用沈董值
    eps = est_eps if est_eps is not None else shen_eps
    div = est_div if est_div is not None else shen_div

    pe_mid = (pe_low + pe_high) / 2
    pe_lo_bias = (pe_mid + pe_low) / 2  # 偏低PE

    def _calc_val(pe_val, yld_val):
        if eps is None or eps <= 0 or div is None or div <= 0:
            return None
        v1 = eps * pe_val
        v2 = div / (yld_val / 100)
        v3 = blend_div / 0.06 + div if blend_div and blend_div > 0 else None
        candidates = [v1, v2]
        if v3 is not None: candidates.append(v3)
        return round(min(candidates), 2)

    val_aa = _calc_val(pe_low, yld_max)
    val_a1 = _calc_val(pe_low, yld_high)
    val_a2 = _calc_val(pe_lo_bias, yld_max)
    val_a  = _calc_val(pe_lo_bias, yld_high)
    val_lt6 = round(blend_div / 0.06, 2) if blend_div and blend_div > 0 else None

    # 判定等級
    val_level = None
    discount_pct = None
    if close and val_aa and close <= val_aa + 0.005:
        val_level = 'AA'
        discount_pct = round((val_aa - close) / val_aa * 100, 2) if val_aa > 0 else 0
    elif close and val_a1 and close <= val_a1 + 0.005:
        val_level = 'A1'
        denom = val_a1 - val_aa if val_aa and val_a1 - val_aa > 0 else val_a1
        discount_pct = round((val_a1 - close) / denom * 100, 2) if denom > 0 else 0
    elif close and val_a2 and close <= val_a2 + 0.005:
        val_level = 'A2'
        denom = val_a2 - val_a1 if val_a1 and val_a2 - val_a1 > 0 else val_a2
        discount_pct = round((val_a2 - close) / denom * 100, 2) if denom > 0 else 0
    elif close and val_a and close <= val_a + 0.005:
        val_level = 'A'
        denom = val_a - val_a2 if val_a2 and val_a - val_a2 > 0 else val_a
        discount_pct = round((val_a - close) / denom * 100, 2) if denom > 0 else 0
    else:
        val_level = 'above'

    return {
        'val_aa': val_aa, 'val_a1': val_a1, 'val_a2': val_a2, 'val_a': val_a,
        'val_lt6': val_lt6, 'val_level': val_level, 'discount_pct': discount_pct,
    }

def _init_stock_state_table():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS stock_state (
            stock_id TEXT NOT NULL,
            date TEXT NOT NULL,
            price REAL,
            price_pos INTEGER,
            fair_low REAL,
            fair_mid REAL,
            fair_high REAL,
            shen_eps REAL,
            shen_pe REAL,
            shen_yld REAL,
            fin_grade TEXT,
            updated_at TEXT,
            UNIQUE(stock_id, date)
        )""")
        c.execute("""CREATE INDEX IF NOT EXISTS idx_stock_state_sd
                     ON stock_state(stock_id, date DESC)""")
        conn.commit()
        conn.close()
    except:
        pass

_init_stock_state_table()


def _calc_shen_eps(row):
    """後端版沈董 EPS 計算（與前端 calcDerived 邏輯一致）"""
    cur_roc = date.today().year - 1911

    # 收集所有季度 EPS
    all_eps = []
    for i in range(1, 6):
        q = row.get(f'eps_{i}q')
        v = row.get(f'eps_{i}')
        if q and v is not None:
            all_eps.append((q, v))

    # 當年度已公佈的季度
    cur_year_eps = [(q, v) for q, v in all_eps if q and q.split('Q')[0] == str(cur_roc)]
    n = len(cur_year_eps)

    if n >= 4:
        return round(sum(v for _, v in cur_year_eps), 2)
    elif n > 0:
        s = sum(v for _, v in cur_year_eps)
        return round(s / n * 4, 2)
    else:
        # fallback: 近四季加總 → 年度EPS
        eps4 = [row.get(f'eps_{i}') for i in range(1, 5)]
        eps4 = [v for v in eps4 if v is not None]
        if len(eps4) == 4:
            return round(sum(eps4), 2)
        return row.get('eps_y1') or row.get('eps_ytd')


def _calc_price_pos(close, shen_eps, pe_high=None, pe_low=None):
    """
    五級價位判斷。
    回傳: (price_pos, fair_low, fair_mid, fair_high)
    price_pos: 1=超便宜 2=便宜 3=偏低 4=合理 5=偏高
    """
    if not pe_high: pe_high = DEFAULT_PE_HIGH
    if not pe_low: pe_low = DEFAULT_PE_LOW

    if shen_eps is None or shen_eps <= 0 or close is None:
        return None, None, None, None

    fair_high = round(shen_eps * pe_high, 2)
    fair_low = round(shen_eps * pe_low, 2)
    fair_mid = round((fair_high + fair_low) / 2, 2)
    super_cheap = round(fair_low * 0.9, 2)

    if close <= super_cheap:
        pos = 1  # 超便宜
    elif close <= fair_low:
        pos = 2  # 便宜
    elif close <= fair_mid:
        pos = 3  # 偏低
    elif close <= fair_high:
        pos = 4  # 合理
    else:
        pos = 5  # 偏高

    return pos, fair_low, fair_mid, fair_high


def snapshot_stock_states():
    """
    對觀察清單 + 體質通過的股票拍快照，寫入 stock_state。
    在 run() 和 quick_update() 結尾呼叫。
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 確保 stock_state 有評價欄位
    for col, typ in [('val_level','TEXT'),('val_aa','REAL'),('val_a1','REAL'),
                     ('val_a2','REAL'),('val_a','REAL'),('val_lt6','REAL'),
                     ('discount_pct','REAL')]:
        try: c.execute(f"ALTER TABLE stock_state ADD COLUMN {col} {typ}")
        except: pass
    # stocks 表加欄位
    for col, typ in [('deepest_val_level','TEXT'),('val_cheap_days','INTEGER DEFAULT 0'),
                     ('priority_grade','TEXT'),('grade_source','TEXT')]:
        try: c.execute(f"ALTER TABLE stocks ADD COLUMN {col} {typ}")
        except: pass

    # 取所有有收盤價的股票
    try:
        c.execute("""SELECT code, close, volume,
                            eps_1, eps_1q, eps_2, eps_2q, eps_3, eps_3q,
                            eps_4, eps_4q, eps_5, eps_5q,
                            eps_y1, eps_ytd, fin_grade_1,
                            div_c1, div_s1, deepest_val_level, val_cheap_days,
                            sys_ann_eps, sys_ann_div, sys_ann_pe, sys_ann_yld
                     FROM stocks WHERE close IS NOT NULL""")
    except:
        c.execute("""SELECT code, close, volume,
                            eps_1, eps_1q, eps_2, eps_2q, eps_3, eps_3q,
                            eps_4, eps_4q, eps_5, eps_5q,
                            eps_y1, eps_ytd, fin_grade_1,
                            div_c1, div_s1
                     FROM stocks WHERE close IS NOT NULL""")
    rows = [dict(r) for r in c.fetchall()]

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # 用資料日（最近一次更新的日期），不用執行日
    c.execute("SELECT updated_at FROM stocks ORDER BY updated_at DESC LIMIT 1")
    r = c.fetchone()
    data_date = r[0][:10] if r and r[0] else date.today().strftime('%Y-%m-%d')

    count = 0
    for row in rows:
        code = row['code']
        close = row['close']
        shen_eps = _calc_shen_eps(row)
        price_pos, fair_low, fair_mid, fair_high = _calc_price_pos(close, shen_eps)

        # 沈董殖利率
        shen_yld = None
        if shen_eps and shen_eps > 0 and close and close > 0:
            # 沈董股利 = 沈董EPS * 簡易配息率（用最近一年股利/EPS）
            div_total = (row.get('div_c1') or 0) + (row.get('div_s1') or 0)
            eps_y1 = row.get('eps_y1')
            if div_total > 0 and eps_y1 and eps_y1 > 0:
                payout = min(1.0, div_total / eps_y1)
                shen_div = shen_eps * payout
                shen_yld = round(shen_div / close * 100, 2)

        # 沈董 PE
        shen_pe = round(close / shen_eps, 2) if shen_eps and shen_eps > 0 and close else None

        # 計算評價等級
        # 沈董股利（簡易版）
        shen_div = None
        blend_div = None
        div_total = (row.get('div_c1') or 0) + (row.get('div_s1') or 0)
        eps_y1_val = row.get('eps_y1')
        if shen_eps and shen_eps > 0 and div_total > 0 and eps_y1_val and eps_y1_val > 0:
            payout = min(1.0, div_total / eps_y1_val)
            shen_div = round(shen_eps * payout, 4)
            blend_div = shen_div

        # 預估值（與前端一致：sys_ann_eps/sys_ann_div 優先）
        est_eps = row.get('sys_ann_eps')
        est_div = row.get('sys_ann_div')

        vl = _calc_val_levels(close, shen_eps, shen_div, blend_div,
                              est_eps=est_eps, est_div=est_div)

        # 矩陣等級（優先順序：預估 > 系統 > 沈董 > X）
        matrix_grade, grade_source = _calc_priority_grade(row, close)
        vl['val_level'] = matrix_grade  # 覆蓋原本的 above/AA/A1 等級

        # 折價%：用 val_aa 門檻算
        if vl['val_aa'] and vl['val_aa'] > 0 and close:
            vl['discount_pct'] = round((vl['val_aa'] - close) / vl['val_aa'] * 100, 2)
        else:
            vl['discount_pct'] = None

        c.execute("""INSERT INTO stock_state
                     (stock_id, date, price, price_pos, fair_low, fair_mid, fair_high,
                      shen_eps, shen_pe, shen_yld, fin_grade,
                      val_level, val_aa, val_a1, val_a2, val_a, val_lt6, discount_pct,
                      updated_at)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                     ON CONFLICT(stock_id, date) DO UPDATE SET
                     price=excluded.price, price_pos=excluded.price_pos,
                     fair_low=excluded.fair_low, fair_mid=excluded.fair_mid,
                     fair_high=excluded.fair_high, shen_eps=excluded.shen_eps,
                     shen_pe=excluded.shen_pe, shen_yld=excluded.shen_yld,
                     fin_grade=excluded.fin_grade,
                     val_level=excluded.val_level, val_aa=excluded.val_aa,
                     val_a1=excluded.val_a1, val_a2=excluded.val_a2,
                     val_a=excluded.val_a, val_lt6=excluded.val_lt6,
                     discount_pct=excluded.discount_pct,
                     updated_at=excluded.updated_at""",
                  (code, data_date, close, price_pos, fair_low, fair_mid, fair_high,
                   shen_eps, shen_pe, shen_yld, row.get('fin_grade_1'),
                   vl['val_level'], vl['val_aa'], vl['val_a1'], vl['val_a2'],
                   vl['val_a'], vl['val_lt6'], vl['discount_pct'], now_str))

        # 更新便宜天數和歷史最深等級
        cur_level = vl['val_level']
        old_cheap_days = row.get('val_cheap_days') or 0
        old_deepest = row.get('deepest_val_level')

        # 便宜天數：AA/A1/A2/A 等級才累加，其他歸零
        CHEAP_GRADES = {'AA', 'A1', 'A2', 'A'}
        if cur_level in CHEAP_GRADES:
            new_cheap_days = old_cheap_days + 1
        else:
            new_cheap_days = 0

        # 歷史最深等級
        LEVEL_DEPTH = {'AA': 5, 'A1': 4, 'A2': 3, 'A': 2, 'above': 0, None: 0}
        cur_depth = LEVEL_DEPTH.get(cur_level, 0)
        old_depth = LEVEL_DEPTH.get(old_deepest, 0)
        new_deepest = cur_level if cur_depth > old_depth else old_deepest

        # 同步寫回 stocks 表（含優先順序等級）
        c.execute("""UPDATE stocks SET price_pos=?, fair_low=?, fair_high=?,
                     deepest_val_level=?, val_cheap_days=?,
                     priority_grade=?, grade_source=? WHERE code=?""",
                  (price_pos, fair_low, fair_high, new_deepest, new_cheap_days,
                   matrix_grade, grade_source, code))
        count += 1

    # 清理 180 天前的舊資料
    c.execute("DELETE FROM stock_state WHERE date < date('now', '-180 days')")

    conn.commit()
    conn.close()
    print(f"[狀態快照] {count} 支已記錄（{data_date}）")

    # 本機自動 push 評價資料到 Render
    if not os.environ.get('DATABASE_URL'):
        try:
            _push_snapshot_to_render(data_date)
        except Exception as e:
            print(f"[評價同步] push 失敗: {e}")

    return count


def _push_snapshot_to_render(data_date):
    """把本機的評價快照 push 到 Render"""
    import requests as _req
    RENDER_URL = "https://tock-system.onrender.com"
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""SELECT stock_id, date, val_level, val_aa, val_a1, val_a2, val_a, val_lt6, discount_pct
                           FROM stock_state WHERE date=? AND val_level IS NOT NULL""", (data_date,)).fetchall()
    extras = {}
    for r in conn.execute("SELECT code, deepest_val_level, val_cheap_days FROM stocks"):
        extras[r[0]] = (r[1], r[2])
    conn.close()

    if not rows:
        return

    conn2 = sqlite3.connect(DB_PATH)
    # 撈完整快照（含 price 等欄位）
    full_rows = conn2.execute("""SELECT stock_id, date, price, price_pos, fair_low, fair_mid, fair_high,
                                       shen_eps, shen_pe, shen_yld, fin_grade,
                                       val_level, val_aa, val_a1, val_a2, val_a, val_lt6, discount_pct
                                FROM stock_state WHERE date=?""", (data_date,)).fetchall()
    conn2.close()

    if not full_rows:
        return

    data = []
    for r in full_rows:
        ex = extras.get(r[0], (None, 0))
        data.append({
            'code': r[0], 'date': r[1], 'price': r[2], 'pp': r[3],
            'fl': r[4], 'fm': r[5], 'fh': r[6],
            'se': r[7], 'sp': r[8], 'sy': r[9], 'fg': r[10],
            'vl': r[11], 'aa': r[12], 'a1': r[13], 'a2': r[14], 'a': r[15], 'lt6': r[16], 'dp': r[17],
            'deepest': ex[0], 'cheap_days': ex[1],
        })

    for i in range(0, len(data), 500):
        batch = data[i:i+500]
        _req.post(f'{RENDER_URL}/api/sync/snapshot', json={'rows': batch},
                  headers={'X-Sync-Token': os.environ.get('SYNC_TOKEN', 'stock-sync-2026')}, timeout=30)

    print(f"[評價同步] 已 push {len(data)} 支到 Render")


def get_daily_briefing():
    """
    產生每日報告資料。
    比對最近兩筆快照，偵測狀態變化。
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 取所有有快照的股票的最近兩筆
    try:
        c.execute("""SELECT stock_id, date, price, price_pos, fair_low, fair_mid, fair_high,
                            shen_eps, shen_pe, shen_yld, fin_grade,
                            val_level, val_aa, val_a1, val_a2, val_a, val_lt6, discount_pct
                     FROM stock_state ORDER BY stock_id, date DESC""")
    except:
        # 評價欄位尚未建立，rollback 後用舊查詢
        try: conn.commit()
        except: pass
        conn.close()
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("""SELECT stock_id, date, price, price_pos, fair_low, fair_mid, fair_high,
                            shen_eps, shen_pe, shen_yld, fin_grade
                     FROM stock_state ORDER BY stock_id, date DESC""")
    all_rows = c.fetchall()

    # 按 stock_id 分組，取最近 6 筆（去抖動需要看 5 日歷史）
    grouped = {}
    for row in all_rows:
        sid = row['stock_id']
        if sid not in grouped:
            grouped[sid] = []
        if len(grouped[sid]) < 6:
            grouped[sid].append(dict(row))

    # 取股票名稱
    c.execute("SELECT code, name FROM stocks")
    names = {r['code']: r['name'] for r in c.fetchall()}

    conn.close()

    alerts = []       # 紅色警示（等級下降）
    opportunities = [] # 藍色機會（往便宜移動）
    near_cheap = []   # 接近便宜區
    summary = {'total': 0, 'to_cheap': 0, 'to_expensive': 0, 'grade_change': 0, 'no_change': 0}

    # 矩陣等級深度
    LEVEL_DEPTH = {
        'AA': 10, 'A1': 9, 'A2': 8, 'A': 7,
        'BA1': 6, 'BA2': 5, 'B1': 4, 'B2': 3,
        '觀察': 2, '臨界點': 1, 'X': 0,
        'above': 0, None: 0,
    }
    CHEAP_GRADES_SET = {'AA', 'A1', 'A2', 'A'}

    for sid, snapshots in grouped.items():
        name = names.get(sid, sid)
        latest = snapshots[0]
        summary['total'] += 1
        cur_level = latest.get('val_level')

        # 首日：只有一筆
        if len(snapshots) < 2:
            if cur_level in CHEAP_GRADES_SET:
                opportunities.append({
                    'code': sid, 'name': name,
                    'price': latest['price'], 'val_level': cur_level,
                    'shen_pe': latest.get('shen_pe'), 'shen_yld': latest.get('shen_yld'),
                    'val_aa': latest.get('val_aa'), 'discount_pct': latest.get('discount_pct'),
                    'change': '首次記錄',
                })
            continue

        prev = snapshots[1]
        prev_level = prev.get('val_level')
        cur_depth = LEVEL_DEPTH.get(cur_level, 0)
        prev_depth = LEVEL_DEPTH.get(prev_level, 0)

        # 財務等級變化（fin_grade，獨立於矩陣等級）
        cur_grade = latest.get('fin_grade')
        prev_grade = prev.get('fin_grade')
        if cur_grade and prev_grade and cur_grade != prev_grade:
            grade_order = {'AA': 9, 'A1': 8, 'A': 7, 'A2': 7, 'B1A': 6, 'B2A': 6,
                          'B1': 5, 'B2': 5, 'C': 3, 'D': 2}
            cur_rank = grade_order.get(cur_grade.rstrip('+-'), 0)
            prev_rank = grade_order.get(prev_grade.rstrip('+-'), 0)
            if cur_rank < prev_rank:
                alerts.append({
                    'code': sid, 'name': name, 'type': 'grade_down',
                    'price': latest['price'],
                    'old_grade': prev_grade, 'new_grade': cur_grade,
                })
                summary['grade_change'] += 1

        # 矩陣等級變化
        if cur_level != prev_level:
            if cur_depth > prev_depth:
                # 變便宜
                summary['to_cheap'] += 1
                opportunities.append({
                    'code': sid, 'name': name,
                    'price': latest['price'], 'val_level': cur_level,
                    'shen_pe': latest.get('shen_pe'), 'shen_yld': latest.get('shen_yld'),
                    'val_aa': latest.get('val_aa'), 'discount_pct': latest.get('discount_pct'),
                    'change': f"{prev_level or '—'} → {cur_level}",
                })
            elif cur_depth < prev_depth:
                # 變貴
                summary['to_expensive'] += 1
            else:
                summary['no_change'] += 1
        else:
            summary['no_change'] += 1

            # 持續便宜
            if cur_level in CHEAP_GRADES_SET:
                already_in = any(o['code'] == sid for o in opportunities)
                if not already_in:
                    opportunities.append({
                        'code': sid, 'name': name,
                        'price': latest['price'], 'val_level': cur_level,
                        'shen_pe': latest.get('shen_pe'), 'shen_yld': latest.get('shen_yld'),
                        'val_aa': latest.get('val_aa'), 'discount_pct': latest.get('discount_pct'),
                        'change': '持續',
                    })

    # 排序：等級深度高（AA）在前
    opportunities.sort(key=lambda x: (-LEVEL_DEPTH.get(x.get('val_level'), 0), x['code']))

    # ETF 成分股異動（近 30 天）
    etf_changes = []
    try:
        conn2 = sqlite3.connect(DB_PATH)
        conn2.row_factory = sqlite3.Row
        c2 = conn2.cursor()
        c2.execute("""SELECT ec.etf_code, ei.name as etf_name, ec.stock_code, ec.stock_name,
                             ec.action, ec.change_date, ec.created_at
                      FROM etf_changes ec
                      LEFT JOIN etf_info ei ON ec.etf_code = ei.code
                      WHERE ec.created_at > datetime('now', '-30 days')
                      ORDER BY ec.created_at DESC LIMIT 50""")
        etf_changes = [dict(r) for r in c2.fetchall()]
        conn2.close()
    except:
        pass

    # ── 評價監控（矩陣等級）──
    CHEAP_GRADES = CHEAP_GRADES_SET

    # 取 stocks 表的便宜天數和歷史最深
    stock_extra = {}
    try:
        conn3 = sqlite3.connect(DB_PATH)
        conn3.row_factory = sqlite3.Row
        c3 = conn3.cursor()
        try:
            c3.execute("SELECT code, volume, deepest_val_level, val_cheap_days FROM stocks")
            for r in c3.fetchall():
                stock_extra[r['code']] = dict(r)
        except:
            try: conn3.commit()
            except: pass
            c3 = conn3.cursor()
            c3.execute("SELECT code, volume FROM stocks")
            for r in c3.fetchall():
                stock_extra[r['code']] = dict(r)
        conn3.close()
    except:
        pass

    val_level_changes = []  # 閃電機會
    val_cheap_list = []     # 便宜清單
    val_dist = {'AA': 0, 'A1': 0, 'A2': 0, 'A': 0}  # 累積分布
    val_dist_prev = {'AA': 0, 'A1': 0, 'A2': 0, 'A': 0}

    for sid, snapshots in grouped.items():
        name = names.get(sid, sid)
        latest = snapshots[0]
        cur_level = latest.get('val_level')
        extra = stock_extra.get(sid, {})

        if cur_level and cur_level in CHEAP_GRADES:
            # 累積制：AA 包含在 A1 裡
            for lv in val_dist:
                if LEVEL_DEPTH.get(lv, 0) <= LEVEL_DEPTH.get(cur_level, 0):
                    val_dist[lv] += 1

        # 前一日分布
        if len(snapshots) >= 2:
            prev_level = snapshots[1].get('val_level')
            if prev_level and prev_level in CHEAP_GRADES:
                for lv in val_dist_prev:
                    if LEVEL_DEPTH.get(lv, 0) <= LEVEL_DEPTH.get(prev_level, 0):
                        val_dist_prev[lv] += 1

            # 跨級變動
            if cur_level != prev_level and cur_level and prev_level:
                cur_d = LEVEL_DEPTH.get(cur_level, 0)
                prev_d = LEVEL_DEPTH.get(prev_level, 0)

                # 門檻是否變動
                threshold_changed = False
                for f in ['val_aa', 'val_a1', 'val_a2', 'val_a']:
                    if latest.get(f) and snapshots[1].get(f) and abs(latest[f] - snapshots[1][f]) > 0.01:
                        threshold_changed = True
                        break

                if cur_d != prev_d:
                    val_level_changes.append({
                        'code': sid, 'name': name,
                        'from': prev_level, 'to': cur_level,
                        'direction': 'cheaper' if cur_d > prev_d else 'expensive',
                        'price': latest.get('price'),
                        'discount_pct': latest.get('discount_pct'),
                        'threshold_changed': threshold_changed,
                        'val_aa': latest.get('val_aa'),
                        'val_a1': latest.get('val_a1'),
                            })

        # 便宜清單（AA/A1/A2/A 才列入）
        if cur_level and cur_level in CHEAP_GRADES:
            cheap_days = extra.get('val_cheap_days', 0)
            deepest = extra.get('deepest_val_level')
            volume = extra.get('volume', 0)
            is_new = False
            if len(snapshots) >= 2:
                prev_lv = snapshots[1].get('val_level')
                is_new = (prev_lv not in CHEAP_GRADES)

            # 是否從深處回升
            recovering = (deepest and LEVEL_DEPTH.get(deepest, 0) > LEVEL_DEPTH.get(cur_level, 0))

            val_cheap_list.append({
                'code': sid, 'name': name,
                'level': cur_level,
                'price': latest.get('price'),
                'discount_pct': latest.get('discount_pct'),
                'cheap_days': cheap_days,
                'is_new': is_new,
                'deepest': deepest,
                'recovering': recovering,
                'volume': volume,
                'shen_yld': latest.get('shen_yld'),
                'val_aa': latest.get('val_aa'),
                'val_a': latest.get('val_a'),
            })

    # 排序
    val_level_changes.sort(key=lambda x: (0 if x['direction'] == 'cheaper' else 1, x['code']))
    val_cheap_list.sort(key=lambda x: (-LEVEL_DEPTH.get(x['level'], 0), -(x.get('discount_pct') or 0)))

    # 分布增減
    val_dist_delta = {}
    for lv in val_dist:
        val_dist_delta[lv] = val_dist[lv] - val_dist_prev.get(lv, 0)

    return {
        'alerts': alerts,
        'opportunities': opportunities,
        'near_cheap': near_cheap,
        'etf_changes': etf_changes,
        'summary': summary,
        'val_level_changes': val_level_changes,
        'val_dist': val_dist,
        'val_dist_delta': val_dist_delta,
        'val_cheap_list': val_cheap_list,
        'data_date': grouped[list(grouped.keys())[0]][0]['date'] if grouped else None,
    }


def generate_health_report():
    """產生完整的系統健康報告"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    report = {
        'generated_at': datetime.now().isoformat(),
        'data_completeness': {},
        'api_health': {},
        'breaker_status': {},
        'backup_status': {},
    }

    # 資料完成度
    checks = [
        ('close', '收盤價'), ('industry', '產業別'),
        ('revenue_yoy', '營收'), ('eps_ytd', '近四季EPS'),
        ('eps_y1', '年度EPS'), ('div_c1', '股利'),
        ('fin_grade_1', '財務等級'), ('eps_2', '季度EPS明細'),
        ('contract_1', '合約負債'),
    ]
    c.execute("SELECT COUNT(*) FROM stocks")
    total = c.fetchone()[0]
    for field, label in checks:
        c.execute(f"SELECT COUNT(*) FROM stocks WHERE {field} IS NOT NULL")
        n = c.fetchone()[0]
        report['data_completeness'][label] = {
            'count': n, 'total': total, 'pct': round(n / total * 100, 1)
        }

    # API 健康
    c.execute("SELECT * FROM api_health")
    for r in c.fetchall():
        cols = [d[0] for d in c.description]
        report['api_health'][r[0]] = dict(zip(cols, r))

    # 熔斷器（含 DB 持久化的）
    report['breaker_status'] = get_all_breakers()

    # 備份
    backup_path = Path(BACKUP_DIR)
    if backup_path.exists():
        total_size = sum(f.stat().st_size for f in backup_path.rglob('*.gz'))
        report['backup_status'] = {
            'total_size_mb': round(total_size / 1024 / 1024, 2),
            'days': len([d for d in backup_path.iterdir() if d.is_dir()]),
        }

    # 來源信賴度
    report['reliability_scores'] = calc_reliability_scores()

    # 資料鮮度
    try:
        report['staleness_alerts'] = detect_staleness()
    except:
        report['staleness_alerts'] = []

    # FinMind 額度
    report['finmind_quota'] = get_finmind_quota()

    conn.close()
    return report
