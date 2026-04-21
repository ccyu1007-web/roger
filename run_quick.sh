#!/bin/bash
cd /Users/roger/Documents/AI機器人/stock_system
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/quick.log"

# 只保留最近 1000 行（避免 log 無限成長）
if [ -f "$LOG_FILE" ] && [ $(wc -l < "$LOG_FILE") -gt 2000 ]; then
  tail -1000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
fi

/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 scraper.py --quick >> "$LOG_FILE" 2>&1
