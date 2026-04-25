#!/bin/bash
# db_guard.sh — 資料庫守護腳本
# 每天檢查 stocks.db 的完整性，異常時自動備份並告警

cd /Users/roger/Documents/AI機器人/stock_system

DB_FILE="stocks.db"
BACKUP_DIR="db_backups"
LOG_FILE="logs/db_guard.log"
MIN_SIZE=500000          # 最小合理大小（500KB，正常應 >900KB）
ALERT_FILE="logs/DB_ALERT"

mkdir -p "$BACKUP_DIR" logs

NOW=$(date '+%Y-%m-%d %H:%M:%S')

# ── 1. 檢查檔案是否存在 ──
if [ ! -f "$DB_FILE" ]; then
    echo "[$NOW] 嚴重：$DB_FILE 不存在！" >> "$LOG_FILE"
    touch "$ALERT_FILE"
    exit 1
fi

# ── 2. 檢查檔案大小 ──
FILE_SIZE=$(stat -f "%z" "$DB_FILE" 2>/dev/null || stat -c "%s" "$DB_FILE" 2>/dev/null)

if [ "$FILE_SIZE" -lt "$MIN_SIZE" ]; then
    echo "[$NOW] 警告：$DB_FILE 大小異常！目前 $FILE_SIZE bytes（低於 $MIN_SIZE）" >> "$LOG_FILE"
    touch "$ALERT_FILE"
    # 不覆蓋備份，保留異常檔案供調查
    cp "$DB_FILE" "$BACKUP_DIR/stocks_ABNORMAL_$(date +%Y%m%d_%H%M%S).db"
    exit 1
fi

# ── 3. SQLite 完整性檢查 ──
INTEGRITY=$(/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -c "
import sqlite3
conn = sqlite3.connect('$DB_FILE')
result = conn.execute('PRAGMA integrity_check').fetchone()[0]
count = conn.execute('SELECT COUNT(*) FROM stocks').fetchone()[0]
conn.close()
print(f'{result}|{count}')
" 2>&1)

STATUS=$(echo "$INTEGRITY" | cut -d'|' -f1)
ROW_COUNT=$(echo "$INTEGRITY" | cut -d'|' -f2)

if [ "$STATUS" != "ok" ]; then
    echo "[$NOW] 嚴重：SQLite integrity_check 失敗！結果=$STATUS" >> "$LOG_FILE"
    touch "$ALERT_FILE"
    cp "$DB_FILE" "$BACKUP_DIR/stocks_CORRUPT_$(date +%Y%m%d_%H%M%S).db"
    exit 1
fi

if [ "$ROW_COUNT" -lt 100 ]; then
    echo "[$NOW] 警告：stocks 表只有 $ROW_COUNT 筆（預期 >1800）" >> "$LOG_FILE"
    touch "$ALERT_FILE"
    exit 1
fi

# ── 4. 每週 VACUUM + 營收交叉校驗（週日）──
if [ "$(date +%u)" = "7" ]; then
    /Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -c "
import sqlite3; conn = sqlite3.connect('$DB_FILE'); conn.execute('VACUUM'); conn.close()
" 2>/dev/null && echo "[$NOW] VACUUM 完成" >> "$LOG_FILE"

    # 營收交叉校驗：政府API vs 群益，全量比對
    /Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -c "
from guardian import cross_validate_revenue
result = cross_validate_revenue()
" >> "$LOG_FILE" 2>&1 && echo "[$NOW] 營收交叉校驗完成" >> "$LOG_FILE"
fi

# ── 5. 每日正常備份（本機保留 7 天）──
cp "$DB_FILE" "$BACKUP_DIR/stocks_$(date +%Y%m%d).db"
find "$BACKUP_DIR" -name "stocks_2*.db" -mtime +7 -delete 2>/dev/null

# ── 6. iCloud 雲端備份（每日同步最新 DB）──
ICLOUD_BACKUP="$HOME/Library/Mobile Documents/com~apple~CloudDocs/stock_backup"
ICLOUD_ALERT="logs/ICLOUD_ALERT"
if [ -d "$HOME/Library/Mobile Documents/com~apple~CloudDocs/" ]; then
    mkdir -p "$ICLOUD_BACKUP"
    cp "$DB_FILE" "$ICLOUD_BACKUP/stocks_latest.db"
    cp "$DB_FILE" "$ICLOUD_BACKUP/stocks_$(date +%Y%m%d).db"
    # iCloud 備份只保留 30 天
    find "$ICLOUD_BACKUP" -name "stocks_2*.db" -mtime +30 -delete 2>/dev/null
    rm -f "$ICLOUD_ALERT"
    ICLOUD_STATUS="ok"
    echo "[$NOW] iCloud 備份完成：$ICLOUD_BACKUP/stocks_latest.db" >> "$LOG_FILE"
else
    touch "$ICLOUD_ALERT"
    ICLOUD_STATUS="未同步"
    echo "[$NOW] 警告：iCloud 文件同步已關閉！請至系統設定重新開啟" >> "$LOG_FILE"
fi

# ── 6. 清除告警旗標 ──
rm -f "$ALERT_FILE"

echo "[$NOW] 正常：大小=${FILE_SIZE}B, 筆數=${ROW_COUNT}, integrity=ok, iCloud=${ICLOUD_STATUS}" >> "$LOG_FILE"

# 只保留最近 200 行 log
if [ -f "$LOG_FILE" ] && [ $(wc -l < "$LOG_FILE") -gt 400 ]; then
    tail -200 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
fi
