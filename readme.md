# 📊 財經新聞爬蟲與情緒分析報告系統

這是一個自動化的新聞數據分析系統，整合了網路爬蟲、LLM 摘要與情緒分析，並將結果自動寄送 Email 報告。

## 🌟 功能特色
- **自動爬蟲**：爬取鉅亨網、工商時報、經濟日報盤後到隔日盤前新聞
- **AI 智能分析**：使用 Ollama (Gemma 3 4b) 生成新聞摘要與情緒分析
- **數據洞察**：統計各公司新聞的文章數、平均情緒分數
- **自動報告**：每日自動寄送 Email 報告（含 HTML 表格與 CSV 附件）

## 📁 專案架構
```
|   config.py               # 專案設定檔 (讀取環境變數)
|   main.py                 # 主程式入口
|   requirements.txt        # 套件依賴清單
|   .env.example           # 環境變數範例檔
|
+---utils
|   |   date_utils.py       # 日期與時間計算工具
|   |   logger.py           # 日誌工具
|
+---crawler                 # 新聞爬蟲模組
+---email_sender            # Email 發送模組
+---summarize               # 摘要與情緒分析模組
```

## ⚙️ 安裝需求

### 1. 系統環境
- Python 3.8+
- [Ollama](https://ollama.com/) (需安裝並啟動)

### 2. 安裝 Python 套件
```bash
pip install -r requirements.txt
```

### 3. 下載 Ollama 模型
```bash
ollama pull gemma3:4b
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
OLLAMA_MODEL=gemma3:4b

# Email 發送設定 (建議使用 Gmail 應用程式密碼)
EMAIL_SENDER=your_email@gmail.com
EMAIL_PASSWORD=your_app_password
EMAIL_RECEIVERS=receiver1@gmail.com,receiver2@gmail.com
```

> **如何取得 Gmail 應用程式密碼？**
> 1. 前往 Google 帳號安全性設定
> 2. 啟用「兩步驟驗證」
> 3. 搜尋「應用程式密碼」並新增一個應用程式
> 4. 將產生的 16 碼密碼填入 `EMAIL_PASSWORD` (不含空格)

### 2. 執行程式
```bash
python main.py
```

程式將自動執行：
1. 計算爬取時間區間（針對台股盤後時間優化）
2. 平行爬取三大財經網站新聞
3. 使用 AI 進行摘要與情緒判讀
4. 生成統計報表並寄送 Email

## 🛠️ 開發與維護
- **新增爬蟲源**：在 `crawler/` 目錄下新增對應模組，並在 `main.py` 中註冊。
- **調整 AI 模型**：修改 `.env` 中的 `OLLAMA_MODEL` 即可切換不同模型（如 `llama3`, `mistral` 等）。

---
Happy Coding! 🚀
