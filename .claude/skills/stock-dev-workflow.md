# 建立投資系統的開發流程

建新的投資分析系統時參考，以逍遙投資系統的經驗為基礎。

## 一、爬蟲開發程序

### 1. 資料來源規劃
- 先列出需要哪些資料（股價/營收/EPS/股利/財報/法人等）
- 每種資料找 2~3 個來源，排優先級（免費 > 付費，政府 > 民間）
- 確認各來源的限制（頻率限制、IP 封鎖、資料延遲）

### 2. 來源優先級與寫入規則
- **最高優先級來源直接覆蓋** DB（如群益損益表）
- **補充來源用 COALESCE**（不覆蓋已有值）
- 同一欄位不能多來源互搶，必須明確定義誰能覆蓋誰

### 3. 爬蟲架構
- 共用工具抽到 `fetcher_utils.py`（session、數值解析、頁面抓取）
- 每個來源獨立 fetcher（`capital_fetcher.py`、`yahoo_fetcher.py`）
- 主編排程式 `scraper.py` 只做流程控制，不做解析
- Render 同步獨立到 `render_sync.py`

### 4. 防呆機制
- 數值單位轉換（群益百萬要乘 1,000,000）
- 日期驗證（批次 API 可能回傳昨天的資料）
- 排序用數值不用字串（99Q4 字串排序 > 114Q4）
- 熔斷器：異常率 > 10% 自動停止
- 交叉校驗：定期抽樣比對不同來源

### 5. 排程設計
- 區分「完整更新」和「快速更新」
- 注意資料公布時間（法人五點後、批次股價四點後）
- 本機抓資料 → 自動 push 到雲端（雲端不爬蟲）

## 二、後端開發程序

### 1. DB 設計
- 用 `db.py` 抽象層自動切換 SQLite（本機）/ PostgreSQL（雲端）
- 主鍵設計要能 UPSERT（INSERT OR REPLACE）
- 新增欄位用 `ALTER TABLE ADD COLUMN`，搭配 try/except 容錯

### 2. API 設計
- 讀取 API 不觸發爬蟲（Render 上純讀 DB）
- 寫入 API 要 token 驗證
- 記憶體快取減少 DB 查詢（30 秒 TTL）
- 快取存 dict 不存 response 物件（避免 gzip 衝突）

### 3. 本機→雲端同步
- 全量同步（不用 WHERE 日期過濾，避免漏資料）
- 會刪除資料的表（新聞/ETF/清單）要 `clear_first` 再重推
- 同步函式集中在 `render_sync.py`，新增表只要加設定

### 4. 錯誤處理
- 不用 bare `except:`（會吃掉 KeyboardInterrupt）
- 用 `except Exception:` + logging
- ALTER TABLE 等預期失敗的用 `except Exception: pass`

## 三、前端開發程序

### 1. 使用者資料持久化
- 所有使用者設定存 DB（`user_estimates`、`user_notes`）
- 前端雙寫 localStorage + DB，載入時 DB 覆蓋 localStorage
- 不能只存 localStorage（換瀏覽器會遺失）

### 2. 參數設計
- 需要使用者可調的參數，提供預設值 + 個股覆蓋
- 預設值定義為**全域變數**（不是某個 tab 的區域變數）
- 所有 tab 都要能存取預設值，否則切 tab 時會 undefined

### 3. 區塊連動
- 上游資料改變時，下游自動重算（如 EPS 改 → 評價重算）
- 用函式呼叫串連，不要靠使用者手動觸發

### 4. 快取控制
- HTML 頁面：`no-cache`（確保載入最新版本）
- API JSON：伺服端記憶體快取（減少 DB 查詢）
- 修改後端程式碼後 Flask 要重啟（或用 `use_reloader=True`）

### 5. 個股頁面設計
- 不同功能區塊（估算/參數/評價）各自獨立儲存/清除
- 使用者沒填的欄位要有 fallback（預設值或系統計算值）
- async 載入的資料（如系統估算）要確保在使用前已就緒
