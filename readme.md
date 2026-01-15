# 📊 財經新聞爬蟲與情緒分析報告系統

這是一個自動化的新聞數據分析系統，整合了網路爬蟲、LLM 摘要與情緒分析，並將結果自動寄送 Email 報告。

## 🌟 功能特色
- **自動爬蟲**：平行爬取鉅亨網、工商時報、經濟日報盤後到隔日盤前新聞
- **AI 智能分析**：使用 Ollama (預設模型 `ministral-3:3b`) 生成新聞摘要與情緒分析
- **多維度評分**：結合 AI 語意分析與 FinBERT 模型進行加權情緒評分
- **數據洞察**：自動歸戶統計各公司新聞文章數、平均情緒分數，並生成公司總結
- **自動報告**：每日自動寄送 Email 報告（含 HTML 表格與 CSV 附件）

## 📁 專案架構
```
|   config.py               # 專案設定檔 (讀取環境變數、設定參數)
|   main.py                 # 主程式入口
|   pipeline.py             # 核心流程控制 (Pipeline)
|   requirements.txt        # 套件依賴清單
|   prompts.py              # LLM 提示詞管理
|   .env.example           # 環境變數範例檔
|
+---utils
|   |   date_utils.py       # 日期與時間計算工具
|   |   logger.py           # 日誌工具
|   |   ollama_service.py   # Ollama 服務管理
|
+---crawler                 # 新聞爬蟲模組 (CNYES, ECO, CTEE)
+---email_sender            # Email 發送模組
+---summarize               # 摘要、情緒分析、公司歸戶模組
```

## ⚙️ 安裝需求

### 1. 系統環境
- Python 3.8+
- [Ollama](https://ollama.com/) (需安裝並啟動)

### 2. 安裝 Python 套件
```bash
pip install -r requirements.txt
```
*注意：本專案使用 `playwright` 進行爬蟲，首次執行可能需要安裝瀏覽器驅動：*
```bash
playwright install
```

### 3. 下載 Ollama 模型
預設使用 `ministral-3:3b`，您也可以在 `.env` 中更改。
```bash
ollama pull ministral-3:3b
```

## 🚀 設定與使用

### 1. 設定環境變數
請複製 `.env.example` 為 `.env`，並填入您的設定：

```bash
cp .env.example .env
```
或直接建立 `.env` 檔案：

```ini
# Ollama 模型設定
OLLAMA_MODEL=ministral-3:3b

# Email 發送設定 (建議使用 Gmail 應用程式密碼)
EMAIL_SENDER=your_email@gmail.com
EMAIL_PASSWORD=your_app_password
EMAIL_RECEIVERS=receiver1@gmail.com,receiver2@gmail.com
```

### 2. 執行程式
```bash
python main.py
```

程式將自動執行以下流程：
1. **爬蟲**：根據時間區間（針對台股盤後優化）爬取三大財經網站。
2. **分析**：
    - 生成新聞摘要
    - 進行情緒分析 (AI + FinBERT)
    - 翻譯與公司歸戶
3. **報告**：生成統計 CSV 文件 (`company_sentiment_stats.csv`)。
4. **通知**：將統計結果與摘要寄送至指定的 Email 信箱。

## 🛠️ 開發與維護
- **Prompt 管理**：所有的 AI 提示詞皆集中於 `prompts.py`，方便統一調整。
- **爬蟲擴充**：在 `crawler/` 新增模組後，於 `crawler/execute.py` 的 `MultiCrawlerManager` 中註冊即可。
- **參數調整**：權重設定、執行緒數量等皆可在 `config.py` 中調整。

---
Happy Coding! 🚀
