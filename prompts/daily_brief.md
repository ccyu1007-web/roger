# 逍遙投資日報速讀 — 產出指令

請幫我產出 {日期} 的逍遙投資日報速讀。

## 執行步驟（必須按順序，不可跳過或簡化）

### Step 1：撈全部新聞

```sql
-- material_news（用 created_at 篩選，不可用 date 欄位，因上市/上櫃格式不同）
SELECT code, name, subject, description, tier, direction, matched_rule
FROM material_news WHERE created_at >= '{日期} 00:00:00' AND created_at < '{日期+1} 00:00:00' AND tier >= 1

-- industry_news（全撈，不可用關鍵字預篩）
SELECT id, title, summary, archived_code
FROM industry_news WHERE created_at LIKE '{日期}%' OR pub_time LIKE '{日期}%'
```

> **注意**：material_news 的 `date` 欄位是 API 原始值，上市為民國格式（`1150727`），上櫃為 `MM/DD HH:MM`，格式不統一。一律用 `created_at`（西元 `YYYY-MM-DD HH:MM:SS`）篩選。

### Step 2：撈體質佳清單 + 體質資料

```sql
-- 體質佳股票代碼（約300支）
SELECT code FROM user_lists WHERE list_type='quality'

-- 每支股票的體質資料
SELECT code, pass_count, growth_signal, gi_pe, gi_yield, val_a, val_aa, red_flags
FROM stock_checklist WHERE code IN (體質佳清單)

-- 股價與基本資料
SELECT code, name, close, fin_grade_1, fin_grade_1y, deepest_val_level, revenue_cum_yoy
FROM stocks WHERE code IN (體質佳清單)
```

### Step 3：交叉比對（核心步驟，不可簡化）

**對每一則 industry_news（全部），用股票名稱比對體質佳清單：**
- 取 stocks 表的 name 欄位
- 在新聞的 title + summary 中搜尋該名稱
- 命中的記錄下來：哪支股票、哪則新聞

**對每一則 material_news，檢查 code 是否在體質佳清單中。**

禁止用 SQL 關鍵字（如 `LIKE '%財報%'`）預篩 industry_news，這會漏掉大量新聞。

### Step 4：分類整理

對 Step 3 命中的每支體質佳股票，根據新聞內容分類：
- **成長動能**：營收創高、新產品、產能擴張、大單、法說上調展望、臨床進展、新領域切入、capex追加
- **價值動能**：庫藏股、大股東加碼、股利優於預期
- **風險警示**：董監轉讓、減資、私募、財報重編、產業逆風

**非體質佳股票**：只有併購等級的重大訊息才納入（如股份轉換、合併、收購、matched_rule 為「併購合併」）。

**material_news 判讀**：
- tier 1 中性 + 內容為例行公告（董事改選、股東會修正、名稱變更、面額變更）→ 跳過
- tier 1/2 + 有實質內容（臨床結果、產能投資、併購、大額交易）→ 納入

**沒有新聞的股票不納入**，純估值訊號（落入便宜區、殖利率高）不算。

### Step 5：補充總經產業

從 industry_news 中整理與個股無關但重要的總經/產業趨勢（大盤走勢、外資動態、油價、利率、財報週等）。

### Step 6：產出報告

每則 2-3 句精簡描述，標註股票代碼、財務等級、評價位置。

```markdown
# 逍遙投資日報速讀 — {日期}

---

## 一、成長動能
**{代碼} {名稱}｜{財務等級}｜評價 {位置}**
{2-3句描述}

## 二、價值動能
（同上格式）

## 三、風險警示
（同上格式）

## 四、總經產業摘要
- {要點1}
- {要點2}

## 五、今日焦點整理
| 標的 | 訊號 | 評價位置 | 體質佳 | 建議動作 |
|------|------|----------|--------|----------|
```

### 評價位置判定
- 收盤 < val_aa → 很便宜
- 收盤 < val_a → 便宜區
- 收盤略高於 val_a（< 1.1倍）→ 略高於便宜價
- 收盤 >> val_a → 偏高
- val_a 為 NULL → 未設定

### 注意事項
- 沒有值得寫的區塊就寫「無」，不湊字數
- 每則新聞只歸入一個分類，不重複

## 儲存

完成後呼叫 Render API：
```
POST https://tock-system.onrender.com/api/ai-briefs
{"date": "{日期}", "content": "報告內容（Markdown）"}
```
