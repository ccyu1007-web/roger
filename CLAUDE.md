# 逍遙投資系統

台股投資分析系統，位於 `stock_system/` 目錄。

### 網址
- 本機：http://localhost:5000/
- 前台：https://tock-system.onrender.com/
- GitHub：https://github.com/ccyu1007-web/roger.git

### 核心檔案
| 檔案 | 用途 |
|------|------|
| `app.py` | Flask 後端 (port 5000)，含 API + Render APScheduler |
| `scraper.py` | 爬蟲主程式（批次 API + 群益逐支補充）|
| `capital_fetcher.py` | 群益證券抓取（損益表+資產負債表+現金流量表+股利+月營收+合約負債）|
| `yahoo_fetcher.py` | Yahoo Finance 財報抓取（補充用）|
| `guardian.py` | 資料守護（備份/驗證/熔斷/交叉校驗/新聞/每日報告）|
| `mops_fetcher.py` | 公開資訊觀測站抓取（月營收+季報，第一優先來源）|
| `etf_fetcher.py` | ETF 成分股抓取與異動追蹤 |
| `db.py` | 資料庫抽象層，自動切換 SQLite/PostgreSQL |
| `index.html` | 總表 |
| `company.html` | 個股頁（年度財報/季度估計表/月營收/同業比較）|
| `daily.html` | 每日新聞報告 |
| `valuation.html` | 每日評價報告（閃電機會/市場體感/深度挖寶）|
| `health.html` | 系統監控 |
| `portfolio.html` | 持股專區（密碼保護，多組投資組合）|
| `startup_catchup.py` | 開機補跑腳本（等網路/清lock/補資料）|

---

### 資料來源優先級（最重要的規則！）

**公開資訊觀測站（MOPS）的資料都是第一優先**

| 資料 | 第一來源 | 補充來源 | 說明 |
|------|---------|---------|------|
| 損益表 | MOPS t163sb04（季，累積值需反算單季）| 群益 zce（補充用）| MOPS 直接覆蓋，群益不可覆蓋 |
| 資產負債表 | 群益 zcpb（年）zcpa（季）| Yahoo | total_assets/total_equity/合約負債 |
| 現金流量表 | 群益 zc3a（年）| Yahoo | operating_cf/capex |
| 股利 | 群益 zcc | 政府API t187ap45 | cash_dividend/stock_dividend |
| 月營收 | MOPS t21sc03（即時）| 政府API t187ap05（批次）> 群益 zch（個股）| MOPS 更即時，公司申報後立即可見 |
| 股價 | 政府API TWSE/TPEX（批次）| TWSE mis（即時）| 含日期驗證 |
| 三大法人 | 群益 zcl（每天17:10後）| | 五點後才公佈 |
| EPS/營收增率 | MOPS > 政府API（批次）| 群益 zce（逐支）| |

**為什麼 MOPS 優先？** 公開資訊觀測站是台灣法定申報系統，資料最即時最權威。t187ap05 API 更新較慢（可能延遲數天），MOPS 公司申報後立即可見。群益作為補充來源（BS/CF/股利等 MOPS 沒有的欄位）。

#### 寫入規則
- `mops_fetcher.py`（MOPS）：**直接覆蓋**（最高優先級）
- `capital_fetcher.py`（群益）：`COALESCE(existing, new)` **不覆蓋 MOPS 已有值**（補充 BS/CF/股利等）
- `yahoo_fetcher.py`（Yahoo）：有 "skip if existing" 邏輯，只補寫 BS/CF 欄位
- MOPS 季報注意：Q2/Q3/Q4 回傳**累積值**，必須用 `當季累積 - 前季累積` 反算單季

#### 群益 URL 對照表
| 資料 | URL |
|------|-----|
| 損益表(季) | `zce/zce_{code}.djhtm` |
| 損益表(年) | `zcq/zcqa.djhtm?a={code}` |
| 資產負債表(年) | `zcp/zcpb/zcpb.djhtm?a={code}` |
| 資產負債表(季) | `zcp/zcpa/zcpa.djhtm?a={code}` |
| 現金流量表(年) | `zc3/zc3a.djhtm?a={code}` |
| 股利 | `zcc/zcc.djhtm?a={code}` |
| 月營收 | `zch/zch.djhtm?a={code}` |
| 三大法人 | `zcl/zcl_{code}.djhtm` |
- 群益 HTML 用 `<span class="t3n1 table-cell">` 標籤，不是 `<td>`
- 群益數值單位是「百萬」，存入 DB 要乘 1,000,000
- 現金流量表 capex 欄位名含 `－CFI` 後綴

---

### 股價更新流程（曾多次出 bug，不可簡化！）

#### refresh_prices() — 三段時間邏輯 + 日期驗證
1. **盤中（09:00~13:35）** → TWSE mis 即時 API
2. **盤後同日（13:36~16:00）** → **仍用即時 API**（z值=收盤價），失敗才 fallback
3. **其餘時段** → 批次收盤 API，**但驗證資料日期**
   - `_twse_batch_date` ≠ `_today_roc()` 且平日 → 改用即時 API

#### 為什麼？
- TWSE 批次 API 通常 16:00 後才更新，有時 18:00+ 仍未更新
- 14:30 排程用批次 API 會抓到昨天收盤價（已踩坑多次）

#### refresh_prices() 步驟
- 寫入 DB 後檢查批次日期，不符就用即時 API 覆蓋

#### 法人買賣超
- **五點後才公佈**，14:30 排程不跑法人，由獨立排程 17:10 處理

---

### 個股頁面載入優化

**先回舊資料，背景更新（秒開）**
- DB 有資料 + 快取未過期 → 直接回傳（毫秒級）
- DB 有資料 + 快取過期 → 先回傳舊資料，背景 thread 更新
- DB 完全沒資料 → 同步抓（僅第一次）

#### 快取時間
- 年報：24小時
- 季報：12小時
- 月營收：24小時
- PE歷史：7天

#### Render 環境（is_cloud）
- **API 請求不觸發爬蟲**（`is_cloud` 判斷），純讀 DB
- **APScheduler 背景排程**會主動抓取政府 API / MOPS 資料（股價、營收、季報、新聞）
- 群益在海外 IP 會被擋 → 法人/BS/CF 仍需本機
- 本機近 20 分鐘內已 push 時，雲端排程自動跳過避免重複

---

### 季度估計表衍生計算

群益季表沒有的欄位，API 層反算：
- 稅（所得稅費用）= 稅前淨利 - 稅後淨利
- 繼續營業單位損益 ≈ 稅後淨利
- 稅率 = 稅 / 稅前淨利
- 歸屬母公司權重 = 稅後淨利 / 繼續營業單位損益
- 每股盈餘-本業 = 營業利益 / 稅前淨利 × EPS
- 每股盈餘-業外 = 業外收支 / 稅前淨利 × EPS
- 配息率：EPS > 0 正常算；EPS ≤ 0 但有配息 → 100%

---

### 總表欄位邏輯

#### 動態欄位（季度/年度/股利）
- 法定期限為基準，但如果實際資料有更新的季度就以資料為準
- 股利欄位：有新資料就加欄，12月才剔除最舊

#### 計算欄位
- **沈董EPS**：當年度已公佈季度年化推算
- **加權EPS**（原名權重EPS）：五年加權平均（預設 30/25/20/15/10）
- **權重EPS**（新增）：沈董EPS × 沈董權重% + 加權EPS × 加權權重%（預設各50%）

#### 財務等級建置規則
- 年報公告期限：每年度年報在**隔年 3/31** 前公告完畢
- 等級建置時間：**隔年 4/15 後**才納入新年度等級（確保大部分公司已公告）
- 例：115年(2026)年報 → 2027/4/15 後才建置 115 年等級
- 固定保留**最近 6 年**等級，與 EPS/股利欄位一致

#### 財務等級顏色
- **A級**（AA/A1/A2/B1A/B2A）：紅色
- **B級**（B1/B2/B1A/B2A）：橘色
- **C以下**（C/D/X）：灰色

#### 季報排序
- `quarterly_financial` 的 quarter 欄位必須用**數值排序**（字串排序 99Q4 > 114Q4 是錯的）

---

### DB 資料表
| 表名 | 用途 | 主鍵 |
|------|------|------|
| stocks | 總表 | code |
| financial_annual | 年度財報三表+股利 | code, year |
| quarterly_financial | 季度損益+合約負債 | code, quarter |
| monthly_revenue | 月營收歷史 | code, year, month |
| pe_history | 歷史本益比 | code, year |
| etf_holdings | ETF成分股 | etf_code, stock_code |
| etf_changes | ETF異動 | id |
| material_news | 新聞 | id |
| user_lists | 觀察/持股/重點/體質清單 | list_type, code |
| user_notes | 個股筆記 | code |
| user_estimates | 個股估值參數 | code |
| stock_state | 每日快照（含評價等級）| stock_id, date |
| cross_validation | 交叉校驗記錄 | id |
| stock_checklist | 體質檢查表+個股評價參數 | code |
| daily_price | 每日收盤價歷史 | code, date |
| focus_tracking | 重點追蹤清單 | code |
| focus_signals | 重點追蹤訊號 | code, date, signal_type |
| portfolios | 投資組合 | id |
| portfolio_holdings | 組合持股明細 | portfolio_id, stock_code |

---

### 排程設定

#### 本機（macOS launchd）
| 排程 | 時間 | 動作 |
|------|------|------|
| com.stock.scraper | 週一~五 14:30 | 股價更新 run_prices()（股價+等級+評價+push，2~3 分鐘） |
| com.stock.maintenance | 每天 06:00 | 每日維護 run_maintenance()（補缺+股利+BWIBBU+ETF+驗證+全量push） |
| com.stock.quick | 每 60 分鐘 | 快速更新 quick_update()（MOPS營收/季報+政府API+評價快照） |
| com.stock.institutional | 週一~五 17:10 | 法人買賣超（群益 zcl） |
| com.stock.backfill | 每天 04:00 | 補缺資料 |
| com.stock.dbguard | 每天 03:00 | DB 備份到 iCloud |
| com.stock.webapp | 開機啟動 | Flask |
| com.stock.startup | 開機啟動 | 補跑腳本（等網路/清lock/檢查缺漏/補資料）|

#### 雲端（Render）
Render 上用 APScheduler 跑以下排程，**本機電腦關機時也能更新**（不含群益來源）：

| 排程 | 時間 | 動作 | 跳過條件 |
|------|------|------|---------|
| cloud_news_cron | 每小時 :05 | 重大訊息/MoneyDJ/產業新聞 | — |
| cloud_prices | 週一~五 16:00 | 政府API批次股價 + 衍生重算 + 評價快照 | 本機 20 分鐘內已更新 |
| cloud_quick_update | 每小時 :35 | MOPS營收/季報 + 政府API營收 | 本機 20 分鐘內已更新 |

- 本機開機時：本機排程先跑 + push 到 Render，雲端排程偵測到近期已更新會自動跳過
- 本機關機時：雲端排程自動接手，股價/營收/季報持續更新
- **仍需本機的資料**：群益相關（法人買賣超、資產負債表、現金流量表、財報補缺），一季開機一次即可補齊

#### 三層架構設計原則

| 層級 | 函式 | 時間敏感度 | 頻率 | 預估耗時 |
|------|------|-----------|------|---------|
| 即時 | run_prices() | 分鐘級 | 14:30 收盤後 | 2~3 分鐘 |
| 定期 | quick_update() | 小時級 | 每 60 分鐘 | 3~5 分鐘 |
| 維護 | run_maintenance() | 天級 | 06:00 凌晨 | 慢慢跑，不趕時間 |

**為什麼拆成三層？**
- run_prices() 只做股價+評價，收盤後 3 分鐘內前台就能看到最新結果
- 補缺資料（群益逐支）、ETF、交叉驗證等不急的事丟到凌晨
- 避免 14:30 排程佔住 lock 90+ 分鐘，導致 quick_update 無法寫入

#### run_prices() 內容（14:30）
1. 股票清單 + 批次股價（TWSE/TPEX）
2. 股價日期驗證 + 即時 API 修正（三段時間邏輯）
3. 等級重算 + EPS 同步
4. 每日價量存入 + 評價快照
5. Checklist + 衍生欄位重算
6. Push 股價+等級+評價到 Render

#### run_maintenance() 內容（06:00）
1. 240 日歷史收盤價
2. 股利（政府 API 批次）
3. 年度 EPS 歷史（BWIBBU 反推）
4. 產業別更新
5. 合併補缺（群益 8 並發，每支一次補齊 EPS+股利+財報）
6. 系統 EPS 估算
7. BWIBBU 股利補充
8. Yahoo 財報補充
9. ETF 成分股
10. 交叉驗證
11. 全量 Push 到 Render

#### quick_update 執行順序（不變，MOPS 最優先！）

**必跑（不受 lock 限制）：**
1. **MOPS 即時營收**（t21sc03）→ 最高優先，直接覆蓋
2. **MOPS 季報**（t163sb04）→ 最高優先，直接覆蓋
3. **政府 API 批次營收**（t187ap05）→ 補充 MOPS 缺的

**可跳過（需 lock）：**
4. **批次 EPS**（TWSE/TPEX t187ap14）
5. **MOPS 最新季 EPS**（補充）
6. 群益校驗（驗證用）
7. 評價快照 + 新聞 + Render 同步

#### 防呆機制
- Lock file 互斥鎖（`logs/scraper.lock`）：各排程不同時執行
- 整體超時：run_prices 5 分鐘 / run_maintenance 90 分鐘 / quick_update 15 分鐘
- SQLite WAL 模式：讀寫不互擋，消除大部分 database is locked
- SQLite busy_timeout=5000ms：遇鎖等 5 秒再失敗

**關鍵**：MOPS 營收和季報是每 60 分鐘自動檢查，營收申報密集期（每月 1~10 日）會持續更新。即使其他排程在執行中，MOPS 資料仍會即時更新。

#### 本機→Render 自動同步（關鍵！）
Render 上群益被擋、公開資訊觀測站也不穩，所以**所有資料由本機抓取後自動 push 到 Render**。

**統一同步函式**：`_push_all_to_render()` 在 `run_maintenance()` 結尾自動呼叫，`run_prices()` 只 push 股價+等級+評價，包含以下所有 push：

| 資料 | Push 函式 | API |
|------|----------|-----|
| stocks 表（EPS/股利/財務等級）| `_push_annual_to_render()` | `/api/sync/annual` |
| 季報（quarterly_financial 整表）| `_push_quarterly_to_render()` | `/api/sync/quarterly` |
| 年報（financial_annual 整表）| `_push_financial_annual_to_render()` | `/api/sync/financial-annual` |
| 三大法人 | `_push_institutional_to_render()` | `/api/refresh/institutional` |
| 系統估算 | `_push_estimates_to_render()` | `/api/sync/estimates` |
| 評價快照 | `_push_snapshot_to_render()` | `/api/sync/snapshot` |
| 新聞 | `_push_news_to_render()` | `/api/sync/news` |

**重要原則**：
- 所有 push 只在本機（無 DATABASE_URL）才執行
- **修改任何資料後，必須確認 Render 也同步更新，不能只改本機就說完成**
- 本機 SQLite 和 Render PostgreSQL 是獨立的資料庫，改本機不等於改 Render
- 手動修資料時，要呼叫對應的 push 函式或直接呼叫 `_push_all_to_render()`
- 程式碼修改後要立即 `git push origin master`，不要等 Stop hook

---

### 部署架構

#### 自動部署流程
1. Claude Code 修改程式碼
2. 結束時 Stop hook 自動 `git push origin master`
3. Render 偵測到 → 自動重新部署（1-2 分鐘）

#### 本機 vs Render 差異
| 項目 | 本機 | Render |
|------|------|--------|
| DB | SQLite (stocks.db) | PostgreSQL (stock-db-5kc8) |
| 切換 | db.py 自動 | db.py 自動 |
| 排程 | macOS launchd | APScheduler |
| 爬蟲 | 群益+政府API+Yahoo | 政府API+Yahoo（群益海外IP被擋）|
| 個股頁面 | 快取過期→背景群益更新 | **純讀DB不爬蟲** |

#### Render 付費方案
- Web Service：Starter $7/月（不休眠）
- PostgreSQL：Basic $6/月（無到期限制）
- 總計：$13/月

#### 注意事項
- Render 上**不能跑群益爬蟲**（海外IP被擋 + gunicorn 120秒超時）
- Render 的 API **不觸發爬蟲**（`is_cloud` 判斷），純讀 DB
- 法人/評價/新聞 由本機自動 push 到 Render
- `os.environ.get('DATABASE_URL')` 判斷是否為雲端環境
- **本機 launchd plist 絕對不能有 DATABASE_URL**（否則本機資料會寫到 Render）
- Render 的 iCloud 警示自動隱藏（前端判斷 hostname）
- gzip 壓縮用 flask-compress，不用手動 after_request（會衝突）
- stocks API 有 30 秒記憶體快取（存 dict 不是 response 物件）

---

### 備份架構
| 項目 | 位置 | 頻率 |
|------|------|------|
| 程式碼 | GitHub（Stop hook 自動 push） | 每次 Claude Code 結束 |
| stocks.db | 本機 db_backups/ | 每天 03:00，保留 7 天 |
| stocks.db | iCloud stock_backup/ | 每天 03:00，保留 30 天 |
| 使用者清單/筆記 | 在 stocks.db 裡 | 跟 DB 一起 |

---

### 使用者資料持久化
- **存 DB**（跨瀏覽器同步）：觀察清單、持股、重點觀察、體質、筆記、估值參數（含個股自訂 PE/殖利率）
- **存 DB `user_settings` 表**（全域設定）：加權股息權重、權重混合比、預設估值參數（PE/殖利率）
- **存 localStorage**（單一瀏覽器）：欄位收合、縮放、篩選設定
- 前端操作同時寫 localStorage + DB（雙寫）
- 載入時從 DB 讀取覆蓋 localStorage
- **所有使用者可設定的參數都必須存 DB**（`user_estimates` / `user_settings` 表），不能只存 localStorage

#### 全域設定（user_settings 表）
後端 `_get_global_settings()` 讀取，前端 `syncUserSettings()` 同步：
| DB key | 內容 | 預設值 |
|--------|------|--------|
| `global_val_params` | PE/殖利率預設 | peHigh=18, peLow=10, yldHigh=5.5, yldMax=6 |
| `blend_ratio` | 權重混合比 | shen=50, wt=50 |
| `global_div_weights` | 加權股息權重 | [30, 30, 20, 10, 10] |
**重要**：後端 `recalc_all_derived()` 讀取這些設定來計算衍生欄位，前端和後端必須用同一組值。DEFAULT_BLEND 預設值統一為 50/50。

#### 個股估值參數優先順序（最重要！）
- 使用者在個股頁設定的 PE/殖利率參數，**優先於系統預設值**
- 優先順序：個股自訂參數 > 全域預設值（PE 18/10、殖利率 6%/5.5%）
- **所有等級計算**（預估等級、系統等級、總表各等級、評價門檻 AA/A1/A2/A）都必須套用個股自訂參數
- 後端（guardian.py `snapshot_stock_states`、estimation.py `estimate_annual_eps`）和前端（index.html、company.html）都要讀取 `user_estimates` 的個股參數
- 個股頁「參數設定」區（PE/殖利率）和「EPS 估算」區的儲存/清除是分開的
- **儲存 user_estimates 後立刻觸發 `recalc_all_derived(codes=[code])` 單支重算**，並 push stocks 表到 Render
- 個股頁儲存/清除後會重新 fetch API 取回 DB 值刷新顯示（確保與總表一致）

---

### 系統監控（health.html）
- 熔斷器：異常率 > 10% → 熔斷，冷卻 10 分鐘
- 跳變校驗：股價跳變 > 2倍、EPS > 10倍 → 攔截
- 交叉校驗：抽樣 20-30 支比對 DB vs 政府 API
- 資料斷層地圖：各欄位覆蓋率

---

### 每日新聞報告（daily.html）
- 價位變動（便宜/昂貴區間移動）
- 財務等級升降
- ETF 成分股異動
- 新聞分類（Tier 0/1/2）
- 新聞可標記「重要/略過」+**撤銷**
- 存入筆記：寫到 `notes_{code}`，質性研究筆記區可看到

---

### 每日評價報告（valuation.html）

#### 一、閃電機會區（今日跨級變動）
- 變便宜（A→A1 等）：展開顯示，含折價%、門檻調整標記
- 變貴中：摺疊顯示

#### 二、市場體感區（累積分布統計）
- ≤ AA / ≤ A1 / ≤ A2 / ≤ A 四張卡片
- **累積制**（≤A1 包含 AA），箭頭方向乾淨
- 與前一交易日比較增減

#### 三、深度挖寶區（完整便宜清單）
- 按 AA > A1 > A2 > A 分組，可收合
- 同組內按折價%排序
- 標籤：新進榜 / 已N天 / 低檔轉強 / 高殖利率 / 低流動性（灰色淡顯）
- 隱藏低流動性開關

#### 評價等級計算
```
評價AA = min(EPS×最低PE, 股利/最高殖利率%, 權重股利/6%+股利)
評價A1 = min(EPS×最低PE, 股利/偏高殖利率%, 權重股利/6%+股利)
評價A2 = min(EPS×偏低PE, 股利/最高殖利率%, 權重股利/6%+股利)
評價A  = min(EPS×偏低PE, 股利/偏高殖利率%, 權重股利/6%+股利)
長期6% = 權重股利 / 6%
```
- EPS = 預估EPS，沒有就用 min(沈董EPS, 綜合EPS)（沈董已改用本業推估法，綜合含50%沈董+50%加權歷史）
- 股利 = 跟隨EPS來源（綜合EPS→綜合股利，沈董EPS→沈董股利）
- 最低PE=10, 最高PE=18, 合理PE=14, 偏低PE=12
- 偏高殖利率=5.5%, 最高殖利率=6%

#### 評價資料儲存
- `stock_state` 表：val_level / val_aa / val_a1 / val_a2 / val_a / val_lt6 / discount_pct
- `stocks` 表：deepest_val_level（歷史最深等級）/ val_cheap_days（連續便宜天數）
- 便宜天數：離開 ≤A（變 above）才歸零，A1→A2 不歸零
- 折價%：AA 用 `(val_aa-股價)/val_aa`，其他用 `(門檻-股價)/(門檻-下一級門檻)`

#### 總表評價欄位
- 位置：漲跌右側，可收合（「評價」按鈕）
- 股價低於門檻時醒目底色：AA紅/A1黃/A2黃/A藍/長期6%綠

---

### ETF 成分股
- 元大系列：Nuxt SSR 解析（完整持股）
- 006208 富邦台50：同步自 0050（同指數）
- 非元大系列：MoneyDJ 前 10 大（fallback）
- 排程更新時自動偵測異動，記入 etf_changes 表

---

### 持股專區（portfolio.html）

#### 密碼保護
- 密碼 hash 存在 `user_settings` 表（key=`portfolio_password`），SHA-256
- 首次進入時設定密碼，之後每次需驗證
- Token 存在後端記憶體（`_portfolio_tokens`），24 小時過期，重啟 webapp 需重新登入
- 所有 portfolio API 需 `Authorization: Bearer <token>` header

#### 多組投資組合
- `portfolios` 表：名稱、分紅條件(文字備註)、保利利率(%)、分紅比例、投入本金、現金餘額
- `portfolio_holdings` 表：portfolio_id + stock_code，張數
- 股價/名稱從 stocks 表即時連動

#### 分紅計算
- 利息 = 投入本金 x 保利利率%
- 分紅 = (當年度損益 - 利息) x 分紅比例（損益未超過利息時為 0）

---

### 開機補跑機制（startup_catchup.py）
- `com.stock.startup`（RunAtLoad）：開機自動執行
- 流程：等網路(120s) → 清殘留 lock → 確認 webapp → 檢查 stock_state 快照 → 補跑
- 盤後：quick_update + run_prices；盤前：只跑 quick_update
- 週末自動跳過
- webapp 啟動失敗時 fallback 用 nohup 直接拉起
- app.py 啟動時等 port 5000 釋放（防 AirPlay 搶 port）

---

### 防呆清單（歷史踩坑，每條都有血淚教訓）

#### 股價相關
1. 批次 API 股價可能是昨天的 → 必須日期驗證（`_twse_batch_date`）
2. 即時 API z 值為空時 → fallback 到買價(b) → 跳過
3. 法人資料五點後才公佈 → 法人由獨立排程 17:10 處理，run_prices() 和 run_maintenance() 都不跑法人

#### 資料來源
4. 群益 capex 欄位名含 `－CFI` 後綴 → 精確匹配
5. 群益數值單位「百萬」→ 乘 1,000,000

#### EPS 排序（重大 bug，曾導致 302 支評價錯誤）
7. stocks 表的 eps_1~eps_5 可能排序錯誤（多來源寫入順序不同）
8. **必須從 quarterly_financial 正確排序後回寫**（`_sync_eps_from_quarterly()`）
9. 此函式在 `_post_process_after_save()` 裡自動執行
10. quarterly_financial 字串排序 99Q4 > 114Q4 → 必須數值排序

#### 欄位連動（總表修改時必須同步）
11a. **總表欄位修改時，以下三處必須一起更新**：
  - `FIELD_OPTIONS`（觀察挑選自訂條件選項）
  - `QUALITY_FIELD_OPTIONS`（體質判斷自訂條件選項）
  - `CUSTOM_COLS`（自訂欄位設定面板）
  - `COL_GROUPS`（欄位收合按鈕）
  - 新增/移除/合併欄位群組時，CSS 的 `body.hide-g-xxx` 也要同步

#### 前端/UI
10a. **成長性指標顏色規則**：Neff 比率 >= 1 紅色粗體、< 1 灰色；PEG <= 1 紅色粗體、> 1 灰色。兩者邏輯一致：紅色 = 好訊號
11. 年報正值不加 `+` 號
12. 配息率 EPS ≤ 0 但有配息 → 100%
13. 稅率計算：虧損（稅前淨利 ≤ 0）不算，限制 0~100%
14. 評價等級浮點比較用容差（`price <= val_aa + 0.005`）

#### Render 部署
15. Render 上**不能跑群益爬蟲**（海外IP被擋 + timeout）→ `is_cloud` 跳過
16. Render API **不觸發任何爬蟲**，純讀 DB → 避免 500 超時
17. **所有資料**（stocks/季報/年報/法人/估算/評價/新聞）由本機 `_push_all_to_render()` 自動 push 到 Render
17a. **修改本機資料後必須同步 Render**，不能只改本機就說完成（曾多次踩坑）
17b. 手動修資料時呼叫對應的 push 函式同步到 Render
17c. 程式碼修改後立即 `git push origin master`，不等 Stop hook
17d. **使用者主要在 Render 前台操作**，確認資料時要查 Render 而非本機
17e. 任何資料更新（營收/季報/股價/EPS 等）完成後都要 push 到 Render，不能只更新本機
17f. **程式碼修改影響 DB schema 或資料格式時**（如新增欄位、改欄位名、重算資料），push 程式碼後要手動 push 受影響的資料表到 Render，不能等排程
18. PostgreSQL `ALTER TABLE ADD COLUMN` 後的 transaction 可能 abort → 查詢要容錯（try/except + 重新連線）
18a. **PostgreSQL ALTER TABLE 會死鎖**：`CREATE TABLE IF NOT EXISTS` 持有 ShareLock 未 commit 時，同 thread 的 ALTER TABLE 需要 AccessExclusiveLock → 死鎖。必須先 commit+close 建表連線，再用獨立連線跑 ALTER TABLE。PG 用 `ADD COLUMN IF NOT EXISTS` + `autocommit=True` 最安全
18b. **新增 DB 欄位時必須檢查所有 CRUD 路徑**：SELECT（list API）、INSERT（create API）、UPDATE（update API）、push columns（render_sync + _push_xxx）、CREATE TABLE（render_sync + _init_xxx）全部都要加新欄位，漏任何一處就會 500 或資料遺失
18c. **不要頻繁推未測試完整的程式碼到 Render**：每次 git push 都觸發 Render 重新部署（build 3-4 分鐘），部署期間服務中斷、token 失效。改 DB schema 時要在本機想清楚 PG 行為，一次推對，不要推上去才 debug
18d. **持股專區 token 存 DB（user_settings 表）**，不存記憶體。Render 重啟/重新部署不會讓使用者被踢出
19. PostgreSQL 沒有 `INSERT OR IGNORE` → db.py 會轉換，但 material_news 的自增 id 不適用 ON CONFLICT → 改用 SELECT 去重
20. Render 的 iCloud 警示要隱藏（`window.location.hostname.includes('onrender.com')`）

#### Git 操作（重大踩坑！）
21a. **禁止用 worktree 模式做 git commit/push** — worktree 的 commit 不會回到主分支 master，導致 GitHub 上的 master 不變、Render 不觸發自動部署。曾因此造成 Render 跑舊程式碼 17 天（4/23~5/10）。所有 git 操作必須在 stock_system 主目錄執行。

#### 本機環境
21. port 5000 可能被 AirPlay 佔用 → 檢查 process name 是 python
22. launchd 的 plist **不能有 DATABASE_URL**（否則本機排程會連到 Render PostgreSQL）
23. launchd KeepAlive=true 會自動重啟舊版 Flask → 修改後要 unload + kill + load
24. iCloud 備份只在本機觸發（`db_guard.sh` 每天 03:00）
25. Stop hook push 前 .gitignore 已排除 *.db
26. clone 路徑是 roger/ → 需指定 `git clone ... stock_system`
27. flask-compress 和手動 gzip `after_request` 會衝突 → 只用 flask-compress
28. 記憶體快取 `_stocks_cache` 要存 dict 不是 response 物件（否則 gzip 後再回傳會爆 RuntimeError）

#### 體質判斷與挑選
33a. **`autoQualityCheck()` 判斷完必須同步 DB**（`fetch('/api/user-lists/quality', {action:'sync', codes:[...]})`），否則背景 `loadUserLists()` 從 DB 讀回舊清單會覆蓋
33b. **`autoWatchCheck()` 挑選依賴 `qualitySet`（記憶體）**，判斷後可立即挑選

#### BWIBBU / 年報寫入限制
34a. **BWIBBU 反推的 EPS 不寫入當年及未來年度**（`if west_year >= cur_west_year: continue`），避免 financial_annual 產生空記錄
34b. **checklist 查詢 financial_annual 加 `AND revenue IS NOT NULL`**，過濾只有 EPS 沒有其他欄位的空記錄
34c. 股利寫入同樣限制：`if year >= datetime.now().year: continue`

#### 資料自動補齊
29. `_fill_all_gaps()`：在 run_maintenance() 中合併補缺，每支股票一次查清所有缺漏（EPS/股利/財報/PE），群益 8 並發補齊
30. 不限制 50 支上限，凌晨有足夠時間一次補完
31. 檢查範圍：年報（total_equity/operating_cf/capex/cash_dividend/accounts_receivable）、季報（inventory/contract_liability）、PE歷史
32. 每月底執行 `verify_full.py` 全欄位覆蓋率驗證

#### 補缺效率原則（重要！曾踩坑）
33c. **只打最小頁面**：不要跑 `fetch_all_three`（7 個頁面），只打包含該欄位的單一頁面
33d. **只寫入需要的欄位**：不要呼叫會覆蓋整張表的現有 fetcher。如果現有 fetcher 用 COALESCE（如 BS/CF），可以直接加欄位呼叫；如果用覆蓋寫入（如 zcqa），**必須寫獨立腳本只 UPDATE 新欄位**
33e. **踩坑教訓**：為了補 `interest_expense` 一個欄位，呼叫 `fetch_capital_annual_eps` 導致覆蓋整張年度損益表 — 應該寫針對性 UPDATE。正確範例是 `accounts_receivable`：在 COALESCE 的 `fetch_capital_balance_sheet` 加欄位
33f. 群益 fetcher 寫入邏輯：**COALESCE（安全）**：`fetch_capital_balance_sheet`、`fetch_capital_cashflow` ／ **覆蓋（危險）**：`fetch_capital_annual_eps`(zcqa)
33g. 補完後跑對應重算函式（如 `_refresh_fin_grades()`），再 push **受影響的單張表**到 Render
33h. **推單張表用 `_push_single_table('表名')`**，自動從 DB schema 讀欄位和主鍵，不要手動複製 columns list
33i. **Render 接收端欄位清單用動態讀取**（`existing_cols`），不要維護白名單。`sync_financial_annual` 的 `fa_cols` 已改為動態，新增欄位不需要再改接收端

#### 評價門檻與等級（曾多處各算各的，已統一）
29a. **評價門檻 val_aa/a1/a2/a/lt6 只由 `recalc_all_derived()` 計算**，存入 stocks 表。stock_checklist 和 stock_state 都從 stocks 表讀取，不獨立計算
29b. **val_level（評價等級）由門檻等級決定**（股價 vs val_aa/a1/a2/a），不被矩陣等級(priority_grade)覆蓋
29c. **A1 和 A2 視為同等級**（LEVEL_DEPTH 相同），A1↔A2 為平移不是升降
29d. **趨勢燈號用財報狗 3M/12M 累計營收年增率**：短>長>0=多頭、短<長<0=空頭，取代舊的近3月vs前3月差值
29e. **Render 不獨立跑 `calc_all_checklists()`**，由本機計算後同步（Render 缺 financial_annual 部分欄位會算出 null）
29f. **`_push_all_to_render()` 前先 `_pull_user_settings_from_render()`**，用時間戳比較避免本機舊設定覆蓋 Render 使用者設定
29g. **Render push 新欄位時**，要先觸發 Render API（讓 ALTER TABLE 跑過），再 push 資料，否則新欄位值為 null

#### 通用全表同步（本機→Render）
33. `_push_table_to_render()`：通用函式，可同步任意資料表到 Render
34. `_push_all_to_render()` 裡用 `SYNC_TABLES` 設定清單，新增資料表只要加一筆設定
35. Render 端 `/api/sync/table` 萬用接收 API（白名單控管），支援自動建表 + UPSERT
36. 目前同步的表：stocks/quarterly_financial/financial_annual/pe_history/monthly_revenue/stock_state/material_news/etf_holdings/etf_changes/user_lists/user_notes/daily_notes/industry_news/user_settings/user_estimates/stock_checklist/daily_price/focus_tracking/focus_signals/portfolios/portfolio_holdings

#### Agent 背景任務（重大踩坑！）
35a. **Agent 不可自行寫入 DB 或 Render**：用 Agent 背景跑質性研究筆記、投資報告等產出任務時，prompt 必須明確寫「只回傳內容，不要自行寫入任何 DB、API 或檔案」。由主流程統一寫入：本機 DB 先寫 → push Render。
35b. **踩坑教訓**：2026-08-22 跑 10 支持股質性筆記，金洲/華研的 agent 自作主張寫入本機 stocks.db，觸發同步後把所有股票的舊版筆記推上 Render，覆蓋已寫入的新版。4536 拓凱被覆蓋兩次。
35c. **質性筆記/投資報告寫入流程**：產出內容 → 寫入本機 `user_notes`/`investment_report` → POST 到 Render API。不可跳過本機 DB 直接寫 Render（否則下次同步會用本機舊版覆蓋）。

---

### 系統架構原則：資料一致性

**所有數據必須先計算存入 DB，前端一律從 DB 讀取，不可前端獨立計算。**

這是整個系統最重要的架構原則。違反此原則會導致：
- 總表和個股頁/檢核表顯示不同數值（計算時間點不同）
- 更新股價後部分欄位消失（前端重載時遺失獨立計算的資料）
- 除錯困難（不知道數據來自 DB 還是即時計算）

#### 正確模式
```
資料來源（爬蟲/API）→ 計算 → 存入 DB → 前端從 DB 讀取
```

#### 具體實踐
- **財務等級**：`_refresh_fin_grades()` 算完存 `stocks` 表 → 總表和檢核表都從 `stocks` 讀
- **成長率指標（聶夫/林區）**：`calc_all_checklists()` 算完存 `stock_checklist` 表 → 總表從 `/api/stocks`（含 `_gi`）讀取、檢核表從 `stock_checklist` 讀取
- **評價門檻**：`recalc_all_derived()` 統一計算 val_aa/a1/a2/a/lt6 存入 `stocks` 表 → 總表、**個股頁**、檢核表(stock_checklist)、每日評價(stock_state) 都從 `stocks` 表讀取，不各自計算
- **個股頁逍遙評價法**：初始載入讀 DB 的 `val_aa/a1/a2/a/lt6`（`_dbValThresholds`），使用者修改參數時才切前端即時預覽（`_useDbVal=false`），儲存後後端重算並重新讀回 DB 值
- **預估/系統等級**：`recalc_all_derived()` 統一計算 est_eps/div/pe/yld/grade、sys_pe/yld/grade 存入 `stocks` 表 → 前端不獨立計算
- **評價快照**：`snapshot_stock_states()` 從 `stocks` 表讀取門檻值寫入 `stock_state` → 不獨立計算門檻
- **新增任何計算欄位**，都必須存入 DB 後再讓前端讀取
- **EPS/股利取用順序統一**：使用者預估(user_estimates) > min(沈董EPS, 綜合EPS)。沈董EPS已改用本業推估法（保留季節性），綜合EPS含50%沈董+50%加權歷史。min()在成長股仍保留50%成長（透過blend），在極端季節性股票提供安全帽。股利跟隨EPS來源。所有計算門檻和等級的地方都用此順序，不可各自定義
