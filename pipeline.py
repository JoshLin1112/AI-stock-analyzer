import os
import logging
import asyncio
from typing import Tuple, List, Optional
import pandas as pd

from config import Config
from crawler.execute import MultiCrawlerManager
from summarize.summarizer import NewsSummarizer
from summarize.sentiment import SentimentAnalyzer
from summarize.company import CompanyAnalyzer
from summarize.time_series import TimeSeriesManager
from utils.ollama_service import OllamaService
from email_sender.sender import send_email

logger = logging.getLogger("StockNewsCrawler")

class NewsPipeline:
    """
    Orchestrates the entire news processing pipeline:
    Crawler -> Summary -> Sentiment -> Company Aggregation -> Time Series -> Email
    """
    
    def __init__(self):
        self.crawler_manager = MultiCrawlerManager()
        self.summarizer = NewsSummarizer()
        self.sentiment_analyzer = SentimentAnalyzer()
        self.company_analyzer = CompanyAnalyzer()
        self.time_series_manager = TimeSeriesManager()
        self.ollama_service = OllamaService()
        
    async def run(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Run the full pipeline"""
        logger.info("=== Starting News Pipeline ===")
        
        # 1. Run Crawlers
        news_files = await self.crawler_manager.run_all()
        
        # 2. Start Ollama Service
        self.ollama_service.start()
        
        try:
            # 3. Analyze Data
            news_df, expanded_df, company_stats = self.analyze_data(news_files)
            
            # 4. Save Results
            self.save_results(company_stats)
            
            # 5. Send Email
            self.send_notification(company_stats)
            
            return news_df, expanded_df, company_stats
            
        finally:
            # 6. Stop Ollama
            self.ollama_service.stop()
            logger.info("=== Pipeline Finished ===")

    def analyze_data(self, news_files: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Execute the analysis phase"""
        logger.info("=== Starting Analysis Phase ===")
        
        # Load Raw News
        news_df = self.summarizer.load_news_data(news_files)
        if news_df.empty:
            logger.warning("No news data found. Skipping analysis.")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        # Generate Summaries
        news_df = self.summarizer.process_summaries(news_df)
        
        # Analyze Sentiment & Translate
        news_df = self.sentiment_analyzer.process_sentiment_and_translation(news_df)
        
        # Aggregate by Company
        expanded_df = self.company_analyzer.expand_news_by_company(news_df)
        company_stats = self.company_analyzer.calculate_company_sentiment_stats(expanded_df)
        company_stats = self.company_analyzer.add_company_summary(company_stats)
        
        return news_df, expanded_df, company_stats

    def save_results(self, company_stats: pd.DataFrame):
        """Save analysis results to CSV"""
        if company_stats.empty:
            logger.warning("Analysis result is empty. Nothing to save.")
            return

        # Save stats
        company_stats.sort_values('avg_weighted_score', ascending=False).to_csv(
            Config.STATS_OUTPUT_PATH, index=False, encoding='utf-8-sig'
        )
        logger.info(f"資料已保存至 {Config.STATS_OUTPUT_PATH}")
        
        # Update Time Series
        self.time_series_manager.update_daily_scores(
            company_stats,
            history_file=Config.SENTIMENT_HISTORY_PATH
        )

    def send_notification(self, company_stats: pd.DataFrame):
        """Send email notification if configured"""
        logger.info("=== Sending Notification ===")
        
        if company_stats.empty:
            logger.info("No stats to email.")
            return

        if Config.EMAIL_SENDER and Config.EMAIL_PASSWORD and Config.EMAIL_RECEIVERS:
            email_success = send_email(
                attachments=[Config.STATS_OUTPUT_PATH],
                subject="每日財經新聞情緒統計",
                body="附件與下表為今日新聞情緒統計。",
                sender_email=Config.EMAIL_SENDER,
                receiver_email=Config.EMAIL_RECEIVERS,
                password=Config.EMAIL_PASSWORD
            )
            
            if email_success:
                pass
            else:
                logger.warning("Email failed to send.")
        else:
            logger.warning("Email configuration missing. Skipping email.")
