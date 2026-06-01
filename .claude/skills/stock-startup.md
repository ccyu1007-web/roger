# 逍遙投資系統快速啟動

當使用者說「打開逍遙投資系統」、「啟動系統」、「換電腦還原」或類似指令時觸發。

## 啟動流程

自動判斷環境並依序執行：

1. **檢查程式碼**：`/Users/roger/Documents/AI機器人/stock_system/` 是否存在
   - 不存在 → `cd /Users/roger/Documents/AI機器人 && git clone https://github.com/ccyu1007-web/roger.git stock_system && cd stock_system`

2. **檢查 DB**：`stocks.db` 是否存在（且大小 > 1MB）
   - 不存在或太小 → 從 iCloud 還原：`cp ~/Library/Mobile\ Documents/com~apple~CloudDocs/stock_backup/stocks_latest.db stocks.db`
   - **已存在的 DB 不要覆蓋**

3. **檢查套件**：未安裝就 `pip3 install -r requirements.txt`

4. **檢查 Flask**：port 5000 是否有 **python** 在跑（`lsof -i :5000 | grep python`）
   - 沒有 → `python3 app.py &`
   - macOS Monterey+ AirPlay 會佔 5000，要確認是 python 不是 AirPlay

5. **開啟瀏覽器**：`open http://localhost:5000/`

6. **排程檢查**：如果是新電腦（沒有 launchd 排程），提醒使用者要不要重建排程

## 換電腦還原

說「打開逍遙投資系統」即可，上述流程會自動處理：
1. 沒程式碼 → git clone
2. 沒 DB → iCloud 還原
3. 沒套件 → pip install
4. 沒啟動 → python3 app.py
5. 沒排程 → 提醒重建 launchd
