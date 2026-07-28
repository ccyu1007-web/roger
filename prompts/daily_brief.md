# 逍遙投資日報速讀 — 產出指令

## 複製指令（改日期即可）

```
請依照 stock_system/prompts/daily_brief.md 的步驟，幫我產出 {日期} 的逍遙投資日報速讀。完成後 POST /api/ai-briefs 儲存。
```

---

## 執行步驟（必須按順序，不可跳過或簡化）

### Step 1：撈全部新聞

用 Python 連接 `stock_system/stocks.db` 執行，不可手寫 SQL 猜格式。

```python
import sqlite3, os
DB = os.path.expanduser('~/Documents/AI機器人/stock_system/stocks.db')
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
TARGET_DATE = '{日期}'  # 西元格式 YYYY-MM-DD
NEXT_DATE = '{日期+1}'  # 西元格式 YYYY-MM-DD

# material_news — 必須用 created_at 篩選
# ⚠ 不可用 date 欄位！上市為民國格式(1150728)、上櫃為 MM/DD HH:MM，格式不統一
material = conn.execute("""
    SELECT code, name, subject, description, tier, direction, matched_rule
    FROM material_news
    WHERE created_at >= ? AND created_at < ? AND tier >= 1
    ORDER BY tier DESC, code
""", (f"{TARGET_DATE} 00:00:00", f"{NEXT_DATE} 00:00:00")).fetchall()

# industry_news — 全撈，不可用關鍵字預篩
industry = conn.execute("""
    SELECT id, title, summary, archived_code
    FROM industry_news
    WHERE pub_time LIKE ? OR created_at LIKE ?
    ORDER BY pub_time
""", (f"{TARGET_DATE}%", f"{TARGET_DATE}%")).fetchall()
```

### Step 2：撈體質佳清單 + 體質資料

```python
# 體質佳股票代碼（約300支）
quality_codes = [r[0] for r in conn.execute(
    "SELECT code FROM user_lists WHERE list_type='quality'").fetchall()]
quality_set = set(quality_codes)

# 股票名稱對照（用於 industry_news 比對）
stock_names = dict(conn.execute("SELECT code, name FROM stocks").fetchall())
name_to_code = {v: k for k, v in stock_names.items() if v and len(v) >= 2}

# 體質資料（pass_count, growth_signal, 估值區間, 紅旗）
placeholders = ','.join('?' * len(quality_codes))
checklist = {r['code']: dict(r) for r in conn.execute(f"""
    SELECT code, pass_count, growth_signal, gi_pe, gi_yield, val_a, val_aa, red_flags
    FROM stock_checklist WHERE code IN ({placeholders})
""", quality_codes).fetchall()}

# 股價與財務等級
stocks_info = {r['code']: dict(r) for r in conn.execute(f"""
    SELECT code, name, close, fin_grade_1, fin_grade_1y, deepest_val_level, revenue_cum_yoy
    FROM stocks WHERE code IN ({placeholders})
""", quality_codes).fetchall()}
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
- **成長動能**：營收創高、新產品、產能擴張、大單、法說上調展望、臨床進展、新領域切入、capex追加、EPS 創高
- **價值動能**：庫藏股、大股東加碼、股利優於預期
- **風險警示**：董監轉讓、減資、私募、財報重編、產業逆風、仲裁/訴訟

**非體質佳股票**：只有併購等級的重大訊息才納入（如股份轉換、合併、收購、matched_rule 為「併購合併」）。

**material_news 判讀**：
- tier 1 中性 + 內容為例行公告（董事改選、股東會修正、名稱變更、面額變更、召開通知）→ 跳過
- tier 1/2 + 有實質內容（臨床結果、產能投資、併購、大額交易、EPS 公告、庫藏股、法說展望）→ 納入

**沒有新聞的股票不納入**，純估值訊號（落入便宜區、殖利率高）不算。

### Step 5：補充總經產業

從 industry_news 中整理與個股無關但重要的總經/產業趨勢（大盤走勢、外資動態、油價、利率、財報週等）。

### Step 6：評價位置判定

```python
# 從 stock_checklist 取 val_a, val_aa，與 stocks.close 比較
def get_val_position(code):
    ck = checklist.get(code, {})
    si = stocks_info.get(code, {})
    close = si.get('close')
    val_aa = ck.get('val_aa')
    val_a = ck.get('val_a')
    if not close or (not val_a and not val_aa):
        return '未設定'
    if val_aa and close <= val_aa:
        return '很便宜'
    if val_a and close <= val_a:
        return '便宜區'
    if val_a and close <= val_a * 1.1:
        return '略高於便宜價'
    return '偏高'
```

### Step 7：產出報告

每則 2-3 句精簡描述，標註股票代碼、財務等級（`stocks.fin_grade_1`）、評價位置。

```markdown
# 逍遙投資日報速讀 — {日期}

---

## 一、成長動能
**{代碼} {名稱}｜{fin_grade_1}｜評價 {位置}**
{2-3句描述}

## 二、價值動能
（同上格式）

## 三、風險警示
（同上格式）

## 四、總經產業摘要
- {要點1}
- {要點2}

## 五、今日焦點整理
| 標的 | 訊號 | 財務等級 | 評價位置 | 體質佳 | 建議動作 |
|------|------|----------|----------|--------|----------|
```

### 注意事項
- 沒有值得寫的區塊就寫「無」，不湊字數
- 每則新聞只歸入一個分類，不重複

## 儲存

完成後呼叫 POST /api/ai-briefs 儲存（本機或 Render 皆可）：
```
POST http://localhost:5000/api/ai-briefs
{"date": "{日期}", "content": "報告內容（Markdown）"}
```

若本機 Flask 未啟動，改用 Render：
```
POST https://tock-system.onrender.com/api/ai-briefs
{"date": "{日期}", "content": "報告內容（Markdown）"}
```
