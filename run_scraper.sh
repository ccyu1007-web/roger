#!/bin/bash
cd /Users/roger/Documents/AI機器人/stock_system
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/scraper_$(date +%Y%m%d_%H%M%S).log"

/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 scraper.py >> "$LOG_FILE" 2>&1

# 只保留最近 30 天的 log
find "$LOG_DIR" -name "scraper_*.log" -mtime +30 -delete 2>/dev/null
