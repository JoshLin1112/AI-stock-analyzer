import os
import asyncio

# --- Config & Utils ---
from config import Config
from utils.date_utils import get_taipei_time_window

# --- 爬蟲 ---
from crawler.cnyes import CNYESCrawler
from crawler.eco import ECOCrawler
from crawler.ctee import CTEECrawler

# --- 摘要 / 情緒 ---
from summarize.summarizer import NewsSummarizer
from summarize.sentiment import SentimentAnalyzer
from summarize.company import CompanyAnalyzer
from utils.ollama_service import OllamaService

# --- Email 發送 ---
from email_sender.sender import send_email

# --- Logger ---
import logging
from utils.logger import setup_logger
logger = logging.getLogger("StockNewsCrawler")


async def run_crawlers(output_dir="./news", start_time=None, end_time=None):
    """同時執行三個爬蟲；CNYES/ECO 為同步，用 asyncio.to_thread 包裝；CTEE 為 async 直接 await。"""
    os.makedirs(output_dir, exist_ok=True)

    cnyes = CNYESCrawler(start_time, end_time, os.path.join(output_dir, "cnyes_news.csv"))
    eco = ECOCrawler(start_time, end_time, os.path.join(output_dir, "eco_news.csv"))
    ctee = CTEECrawler(start_time, end_time, os.path.join(output_dir, "ctee_news.csv"))

    async def run_sync_crawler(crawler):
        # 包裝同步 .run() 進 thread，不阻塞事件迴圈
        await asyncio.to_thread(crawler.run)

    await asyncio.gather(
        run_sync_crawler(cnyes),
        run_sync_crawler(eco),
        ctee.run(), 
    )
    logger.info("=== 爬蟲完成 ===")

    return [
        cnyes.output_path,
        eco.output_path,
        ctee.output_path,
    ]


def analyze_and_summarize(news_files, output_stats_path):
    """執行摘要 / 情緒 / 翻譯 / FinBERT / 公司匯總，並產出 CSV。"""
    dir_path = os.path.dirname(os.path.abspath(output_stats_path))
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

    # 初始化服務與分析器
    ollama_service = OllamaService()
    summarizer = NewsSummarizer()
    sentiment_analyzer = SentimentAnalyzer()
    company_analyzer = CompanyAnalyzer()

    # 1. 啟動 Ollama
    ollama_service.start()

    try:
        # 2. 摘要
        news_df = summarizer.load_news_data(news_files)
        news_df = summarizer.process_summaries(news_df)

        # 3. 情緒 + 翻譯 + FinBERT
        news_df = sentiment_analyzer.process_sentiment_and_translation(news_df)
        news_df = sentiment_analyzer.apply_finbert_analysis(news_df)

        # 4. 公司匯總
        expanded_df = company_analyzer.expand_news_by_company(news_df)
        company_stats = company_analyzer.calculate_company_sentiment_stats(expanded_df)
        company_stats = company_analyzer.add_company_summary(company_stats)

        # 5. 保存
        if not company_stats.empty:
            company_stats.sort_values('avg_weighted_score', ascending=False).to_csv(
                output_stats_path, index=False, encoding='utf-8-sig'
            )
            print(f"分析完成！結果已保存至 {output_stats_path}")
        else:
            print("分析結果為空，未保存檔案。")

        return news_df, expanded_df, company_stats, output_stats_path

    finally:
        # 6. 關閉 Ollama (若是我們開啟的)
        ollama_service.stop()


async def main():
    # 1) 設定 Logger
    setup_logger()
    Config.validate() # Check if env vars are set

    # 2) 爬蟲（並行）
    start_time, end_time = get_taipei_time_window()
    logger.info("=== 開始執行爬蟲（並行） ===")
    
    # Use Config.NEWS_OUTPUT_DIR
    news_files = await run_crawlers(output_dir=Config.NEWS_OUTPUT_DIR, start_time=start_time, end_time=end_time)

    # 3) 摘要 / 情緒 / 匯總
    logger.info("=== 開始分析新聞數據 ===")
    
    # Use Config.STATS_OUTPUT_PATH
    news_df, expanded_df, stats, stats_path = analyze_and_summarize(
        news_files, output_stats_path=Config.STATS_OUTPUT_PATH
    )

    # 4) 發信
    logger.info("=== 開始發送 Email ===")
    
    if Config.EMAIL_SENDER and Config.EMAIL_PASSWORD and Config.EMAIL_RECEIVERS:
        email_success = send_email(
            attachments=[stats_path], # Use the returned path or Config path
            subject="每日財經新聞情緒統計",
            body="附件與下表為今日新聞情緒統計。",
            sender_email=Config.EMAIL_SENDER,
            receiver_email=Config.EMAIL_RECEIVERS,
            password=Config.EMAIL_PASSWORD
        )
        
        if email_success:
            logger.info("=== 所有流程執行完成！ ===")
        else:
            logger.warning("Email 發送失敗，但數據分析已完成")
    else:
        logger.warning("Email 設定不完整，跳過發信步驟。請檢查 .env 設定。")

    # 供其他流程調用時使用
    return news_df, expanded_df, stats

if __name__ == "__main__":
    asyncio.run(main())

# Date logic moved to utils/date_utils.py


async def run_crawlers(output_dir="./news", start_time=None, end_time=None):
    """同時執行三個爬蟲；CNYES/ECO 為同步，用 asyncio.to_thread 包裝；CTEE 為 async 直接 await。"""
    os.makedirs(output_dir, exist_ok=True)

    cnyes = CNYESCrawler(start_time, end_time, os.path.join(output_dir, "cnyes_news.csv"))
    eco = ECOCrawler(start_time, end_time, os.path.join(output_dir, "eco_news.csv"))
    ctee = CTEECrawler(start_time, end_time, os.path.join(output_dir, "ctee_news.csv"))

    async def run_sync_crawler(crawler):
        # 包裝同步 .run() 進 thread，不阻塞事件迴圈
        await asyncio.to_thread(crawler.run)

    await asyncio.gather(
        run_sync_crawler(cnyes),
        run_sync_crawler(eco),
        ctee.run(), 
    )
    logger.info("=== 爬蟲完成 ===")

    return [
        cnyes.output_path,
        eco.output_path,
        ctee.output_path,
    ]


def analyze_and_summarize(news_files, output_stats_path):
    """執行摘要 / 情緒 / 翻譯 / FinBERT / 公司匯總，並產出 CSV。"""
    dir_path = os.path.dirname(os.path.abspath(output_stats_path))
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

    # 初始化服務與分析器
    ollama_service = OllamaService()
    summarizer = NewsSummarizer()
    sentiment_analyzer = SentimentAnalyzer()
    company_analyzer = CompanyAnalyzer()

    # 1. 啟動 Ollama
    ollama_service.start()

    try:
        # 2. 摘要
        news_df = summarizer.load_news_data(news_files)
        news_df = summarizer.process_summaries(news_df)

        # 3. 情緒 + 翻譯 + FinBERT
        news_df = sentiment_analyzer.process_sentiment_and_translation(news_df)
        news_df = sentiment_analyzer.apply_finbert_analysis(news_df)

        # 4. 公司匯總
        expanded_df = company_analyzer.expand_news_by_company(news_df)
        company_stats = company_analyzer.calculate_company_sentiment_stats(expanded_df)
        company_stats = company_analyzer.add_company_summary(company_stats)

        # 5. 保存
        if not company_stats.empty:
            company_stats.sort_values('avg_weighted_score', ascending=False).to_csv(
                output_stats_path, index=False, encoding='utf-8-sig'
            )
            print(f"分析完成！結果已保存至 {output_stats_path}")
        else:
            print("分析結果為空，未保存檔案。")

        return news_df, expanded_df, company_stats, output_stats_path

    finally:
        # 6. 關閉 Ollama (若是我們開啟的)
        ollama_service.stop()


async def main():
    # 1) 設定 Logger
    setup_logger()
    Config.validate() # Check if env vars are set

    # 2) 爬蟲（並行）
    start_time, end_time = get_taipei_time_window()
    logger.info("=== 開始執行爬蟲（並行） ===")
    
    # Use Config.NEWS_OUTPUT_DIR
    news_files = await run_crawlers(output_dir=Config.NEWS_OUTPUT_DIR, start_time=start_time, end_time=end_time)

    # 3) 摘要 / 情緒 / 匯總
    logger.info("=== 開始分析新聞數據 ===")
    
    # Use Config.STATS_OUTPUT_PATH
    news_df, expanded_df, stats, stats_path = analyze_and_summarize(
        news_files, output_stats_path=Config.STATS_OUTPUT_PATH
    )

    # 4) 發信
    logger.info("=== 開始發送 Email ===")
    
    if Config.EMAIL_SENDER and Config.EMAIL_PASSWORD and Config.EMAIL_RECEIVERS:
        email_success = send_email(
            attachments=[stats_path], # Use the returned path or Config path
            subject="每日財經新聞情緒統計",
            body="附件與下表為今日新聞情緒統計。",
            sender_email=Config.EMAIL_SENDER,
            receiver_email=Config.EMAIL_RECEIVERS,
            password=Config.EMAIL_PASSWORD
        )
        
        if email_success:
            logger.info("=== 所有流程執行完成！ ===")
        else:
            logger.warning("Email 發送失敗，但數據分析已完成")
    else:
        logger.warning("Email 設定不完整，跳過發信步驟。請檢查 .env 設定。")

    # 供其他流程調用時使用
    return news_df, expanded_df, stats

# eeeg xpgp wwph chji
if __name__ == "__main__":
    asyncio.run(main())