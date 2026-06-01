# 新增欄位到 stocks 表

當需要在逍遙投資系統的 stocks 表新增計算欄位時，依照以下 7 步驟完成，缺一不可。

## 完整流程

1. **`DERIVED_COLS`** 加欄位名
2. **`_calc_derived_fields()`** 加計算邏輯
3. **`get_stocks` API 的 ALTER TABLE 區**加欄位定義
4. **`get_stocks` API 的 SELECT**加欄位
5. **`sync_annual` 接收端**的欄位清單加欄位
6. **`_push_annual_to_render` 發送端**的 SELECT 和 cols 加欄位
7. **前端**移除獨立計算，改讀 API 回傳值

## 注意事項
- 所有數據必須先計算存入 DB，前端一律從 DB 讀取，不可前端獨立計算
- 修改後必須 `git push origin master` 讓 Render 部署
- 如果影響 DB schema，push 程式碼後要手動 push 受影響的資料表到 Render
- Render push 新欄位時，要先觸發 Render API（讓 ALTER TABLE 跑過），再 push 資料
