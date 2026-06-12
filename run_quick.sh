#!/bin/bash
# Wrapper for quick_update — 確保 launchd 永遠看到 exit 0
cd /Users/roger/Documents/AI機器人/stock_system
/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 scraper.py --quick >> logs/quick_stdout.log 2>> logs/quick_stderr.log
exit 0
