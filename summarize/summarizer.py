import pandas as pd
import ollama
import logging
from typing import List, Optional
from config import Config
from prompts import Prompts

logger = logging.getLogger("StockNewsCrawler")

class NewsSummarizer:
    """新聞摘要生成器"""
    
    def __init__(self):
        self.ollama_model = Config.OLLAMA_MODEL

    def load_news_data(self, file_paths: List[str]) -> pd.DataFrame:
        """載入新聞數據"""
        logger.info("正在載入新聞數據...")
        all_news_df = pd.DataFrame()
        
        for file_path in file_paths:
            try:
                df = pd.read_csv(file_path, usecols=["title", "content"])
                all_news_df = pd.concat([all_news_df, df], ignore_index=True)
            except FileNotFoundError:
                logger.warning(f"找不到檔案: {file_path}，跳過。")
            except Exception as e:
                logger.error(f"讀取檔案 {file_path} 失敗: {e}")
        
        logger.info(f"總共載入 {len(all_news_df)} 篇新聞")
        return all_news_df
    
    def generate_summary(self, title: str, content: str) -> str:
        """生成新聞摘要"""
        prompt = Prompts.NEWS_SUMMARY.format(title=title, content=content)
        
        try:
            response = ollama.chat(
                model=self.ollama_model,
                messages=[{"role": "user", "content": prompt}]
            )
            return response['message']['content'].strip()
        except Exception as e:
            logger.error(f"生成摘要失敗: {e}")
            return ""
    
    def process_summaries(self, df: pd.DataFrame) -> pd.DataFrame:
        """批量處理摘要生成"""
        logger.info("正在生成摘要...")
        summaries = []
        
        for idx, row in df.iterrows():
            summary = self.generate_summary(row['title'], row['content'])
            summaries.append(summary)
            
            if (idx + 1) % 10 == 0:
                logger.info(f"摘要進度: [{idx + 1}/{len(df)}]")
        
        df['summary'] = summaries
        return df
