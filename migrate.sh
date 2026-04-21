#!/bin/bash
# ══════════════════════════════════════════════════════
# migrate.sh — 台股選股系統搬家腳本
#
# 使用方式：
#   1. 把整個 stock_system 資料夾複製到新電腦的
#      /Users/<你的帳號>/Documents/AI機器人/stock_system/
#   2. 開終端機，cd 到該資料夾，執行：
#      bash migrate.sh
# ══════════════════════════════════════════════════════

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
USER_HOME="$HOME"
LAUNCH_DIR="$USER_HOME/Library/LaunchAgents"

echo "=========================================="
echo "  台股選股系統 — 搬家安裝"
echo "=========================================="
echo ""
echo "系統路徑: $SCRIPT_DIR"
echo "使用者:   $(whoami)"
echo ""

# ── 1. 檢查 Python ──
echo "[1/5] 檢查 Python..."
if command -v python3 &>/dev/null; then
    PY=$(which python3)
    PY_VER=$(python3 --version)
    echo "  找到 $PY_VER ($PY)"
else
    echo "  未安裝 Python3！"
    echo "  請先安裝：https://www.python.org/downloads/"
    echo "  或執行：brew install python3"
    exit 1
fi

# ── 2. 安裝套件 ──
echo "[2/5] 安裝 Python 套件..."
pip3 install flask requests --quiet 2>/dev/null || python3 -m pip install flask requests --quiet
echo "  flask + requests 已安裝"

# ── 3. 更新腳本中的路徑 ──
echo "[3/5] 更新路徑設定..."

# 更新 shell 腳本中的路徑
for f in run_quick.sh run_scraper.sh db_guard.sh; do
    if [ -f "$SCRIPT_DIR/$f" ]; then
        sed -i '' "s|cd .*stock_system|cd $SCRIPT_DIR|g" "$SCRIPT_DIR/$f"
        chmod +x "$SCRIPT_DIR/$f"
        echo "  $f 路徑已更新"
    fi
done

# 更新 Python 路徑
PY_PATH=$(which python3)
for f in run_quick.sh run_scraper.sh db_guard.sh; do
    if [ -f "$SCRIPT_DIR/$f" ]; then
        sed -i '' "s|/Library/Frameworks/Python.framework/Versions/[0-9.]*/bin/python3|$PY_PATH|g" "$SCRIPT_DIR/$f"
    fi
done
echo "  Python 路徑更新為 $PY_PATH"

# ── 4. 安裝排程 ──
echo "[4/5] 安裝排程..."
mkdir -p "$LAUNCH_DIR"

# 先卸載舊的（如果有）
for label in com.stock.quick com.stock.scraper com.stock.webapp com.stock.dbguard; do
    launchctl unload "$LAUNCH_DIR/$label.plist" 2>/dev/null || true
done

# 生成 plist 檔案
cat > "$LAUNCH_DIR/com.stock.webapp.plist" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>com.stock.webapp</string>
    <key>ProgramArguments</key>
    <array>
        <string>$PY_PATH</string>
        <string>$SCRIPT_DIR/app.py</string>
    </array>
    <key>WorkingDirectory</key><string>$SCRIPT_DIR</string>
    <key>RunAtLoad</key><true/>
    <key>KeepAlive</key><true/>
    <key>StandardErrorPath</key><string>$SCRIPT_DIR/logs/webapp_err.log</string>
</dict>
</plist>
PLIST

cat > "$LAUNCH_DIR/com.stock.quick.plist" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>com.stock.quick</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>$SCRIPT_DIR/run_quick.sh</string>
    </array>
    <key>StartInterval</key><integer>1800</integer>
</dict>
</plist>
PLIST

cat > "$LAUNCH_DIR/com.stock.scraper.plist" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>com.stock.scraper</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>$SCRIPT_DIR/run_scraper.sh</string>
    </array>
    <key>StartCalendarInterval</key>
    <array>
        <dict><key>Weekday</key><integer>1</integer><key>Hour</key><integer>14</integer><key>Minute</key><integer>30</integer></dict>
        <dict><key>Weekday</key><integer>2</integer><key>Hour</key><integer>14</integer><key>Minute</key><integer>30</integer></dict>
        <dict><key>Weekday</key><integer>3</integer><key>Hour</key><integer>14</integer><key>Minute</key><integer>30</integer></dict>
        <dict><key>Weekday</key><integer>4</integer><key>Hour</key><integer>14</integer><key>Minute</key><integer>30</integer></dict>
        <dict><key>Weekday</key><integer>5</integer><key>Hour</key><integer>14</integer><key>Minute</key><integer>30</integer></dict>
        <dict><key>Hour</key><integer>6</integer><key>Minute</key><integer>0</integer></dict>
    </array>
</dict>
</plist>
PLIST

cat > "$LAUNCH_DIR/com.stock.dbguard.plist" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>com.stock.dbguard</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>$SCRIPT_DIR/db_guard.sh</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict><key>Hour</key><integer>3</integer><key>Minute</key><integer>0</integer></dict>
</dict>
</plist>
PLIST

# 載入排程
for label in com.stock.webapp com.stock.quick com.stock.scraper com.stock.dbguard; do
    launchctl load "$LAUNCH_DIR/$label.plist"
    echo "  $label 已啟動"
done

# ── 5. 初始化 ──
echo "[5/5] 初始化資料庫..."
mkdir -p "$SCRIPT_DIR/logs" "$SCRIPT_DIR/raw_backup" "$SCRIPT_DIR/db_backups"
cd "$SCRIPT_DIR"
python3 -c "from scraper import init_db, init_financial_db, init_monthly_revenue_db, init_quarterly_db, init_pe_history_db; init_db(); init_financial_db(); init_monthly_revenue_db(); init_quarterly_db(); init_pe_history_db(); print('  資料表已就緒')"

echo ""
echo "=========================================="
echo "  安裝完成！"
echo "=========================================="
echo ""
echo "  網頁: http://localhost:5000"
echo "  監控: http://localhost:5000/health.html"
echo ""
echo "  排程:"
echo "    每 30 分鐘  快速更新（營收+EPS）"
echo "    每天 06:00  完整更新"
echo "    週一~五 14:30  完整更新"
echo "    每天 03:00  DB 健康檢查"
echo ""
