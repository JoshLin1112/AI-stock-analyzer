import pandas as pd
import ollama
import logging
from config import Config

logger = logging.getLogger("StockNewsCrawler")

class NewsSummarizer:
    """新聞摘要生成器"""
    
    def __init__(self):
        self.ollama_model = Config.OLLAMA_MODEL

    def load_news_data(self, file_paths):
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
    
    def generate_summary(self, title, content):
        """生成新聞摘要"""
        prompt = f"""
        你是一位財經新聞摘要助手。請閱讀以下新聞，並用繁體中文寫出不超過四句話的總結，並遵守以下規範：
        1. 直接產出總結內容，不需要說明或引言。
        2. 總結內容請聚焦於投資相關資訊，並強調新聞中提及的股票公司。
        3. 於總結最後條列出這個新聞報導的所有股票公司，格式範例如下 :「新聞提及公司:台積電(2330)、鴻海(2317)」，並使用「、」隔開公司。若無提及公司則不需列出。

        標題：{title}
        內文：{content}
        """
        
        try:
            response = ollama.chat(
                model=self.ollama_model,
                messages=[{"role": "user", "content": prompt}]
            )
            return response['message']['content'].strip()
        except Exception as e:
            logger.error(f"生成摘要失敗: {e}")
            return ""
    
    def process_summaries(self, df):
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
