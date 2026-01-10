import pandas as pd
import re
import ollama
import logging
from config import Config

logger = logging.getLogger("StockNewsCrawler")

class CompanyAnalyzer:
    """公司情緒分析器"""
    
    def __init__(self):
        self.ollama_model = Config.OLLAMA_MODEL

    @staticmethod
    def normalize_company_name(company_name: str) -> str:
        """標準化公司名稱字串"""
        return (
            company_name.strip()
                    .replace('、', '')
                    .replace(',', '')
                    .replace('　', '')
                    .replace(' ', '')
        )
    
    @staticmethod
    def extract_companies_from_text(text):
        """從文本中提取公司資訊，僅保留正確代號"""
        # 建立字典 {公司名稱: 正確代號}
        try:
            company_df = pd.read_csv("summarize/company_codes.csv", dtype=str)
            NAME2CODE = dict(zip(company_df["company_name"], company_df["stock_code"]))
        except FileNotFoundError:
            logger.error("找不到 summarize/company_codes.csv，無法進行公司代號驗證。")
            return []

        companies = []
        pattern = r'新聞提及公司[：:](.*?)(?=\n|$)'
        match = re.search(pattern, text)

        if match:
            company_text = match.group(1)
            company_pattern = r'([^(]+)\((\d+)\)'
            matches = re.findall(company_pattern, company_text)

            for company_name, stock_code in matches:
                company_name = CompanyAnalyzer.normalize_company_name(company_name)

                # 如果公司名稱存在於對照表
                if company_name in NAME2CODE:
                    correct_code = NAME2CODE[company_name]

                    # 代號符合對照表 → 保留
                    if stock_code == correct_code:
                        companies.append(f"{company_name}({stock_code})")
                    # 代號不符 → 丟掉，不加入
        return companies

    
    @staticmethod
    def label_to_score(label, label_type='ai'):
        """將標籤轉換為數值分數"""
        if label_type == 'ai':
            mapping = {'正面': 1, '中性': 0, '負面': -1}
        else:  # finbert
            mapping = {'positive': 1, 'neutral': 0, 'negative': -1}
        
        return mapping.get(label, 0)
    
    def expand_news_by_company(self, df):
        """展開新聞數據，每個公司一行"""
        logger.info("正在展開新聞數據...")
        expanded_data = []
        
        for _, row in df.iterrows():
            companies = self.extract_companies_from_text(row['summary'])
            
            if not companies:
                expanded_row = row.copy()
                expanded_row['company'] = None
                expanded_data.append(expanded_row)
            else:
                for company in companies:
                    expanded_row = row.copy()
                    expanded_row['company'] = company
                    expanded_data.append(expanded_row)
        
        expanded_df = pd.DataFrame(expanded_data)
        if not expanded_df.empty:
            expanded_df = expanded_df.drop_duplicates(subset=['summary', 'company'])
            
        logger.info(f"展開後數據行數: {len(expanded_df)}筆")
        return expanded_df
    
    def calculate_company_sentiment_stats(self, expanded_df, w_ai=0.6, w_finbert=0.4):
        """計算公司情緒統計"""
        logger.info("正在計算公司情緒統計...")
        
        if expanded_df.empty:
            return pd.DataFrame()

        # 移除無公司資訊的行
        valid_df = expanded_df[expanded_df['company'].notna()].copy()
        
        if valid_df.empty:
            return pd.DataFrame()

        # 轉換標籤為數值分數
        valid_df['ai_score'] = valid_df['ai_label'].apply(
            lambda x: self.label_to_score(x, 'ai')
        )
        valid_df['finbert_score'] = valid_df['finbert_label'].apply(
            lambda x: self.label_to_score(x, 'finbert')
        )
        
        # 計算加權情緒分數
        valid_df['weighted_score'] = (
            w_ai * valid_df['ai_score'] + w_finbert * valid_df['finbert_score']
        )
        
        # 按公司分組統計
        company_stats = valid_df.groupby('company').agg(
            total_articles=('summary', 'count'),
            avg_weighted_score=('weighted_score', 'mean'),
            whole_news_content=('summary', self._combine_news_content)
        ).reset_index()
        
        # 按新聞數量排序
        company_stats = company_stats.sort_values('total_articles', ascending=False)

        logger.info(f"分析了 {len(company_stats)} 家公司")
        return company_stats
    
    @staticmethod
    def _combine_news_content(summaries):
        """將多則新聞摘要整合成一段文字"""
        combined_content = ""
        for i, summary in enumerate(summaries, 1):
            # 移除新聞提及公司的部分，只保留摘要內容
            content = summary.split("新聞提及公司")[0].strip()
            combined_content += f"第{i}則新聞:{content} \\n "
        
        # 移除最後的空格和換行符
        return combined_content.rstrip(" \\n ")
    
    def add_company_summary(self, company_stats):
        """為每間公司的整合新聞內容生成總結"""
        if company_stats.empty:
            return company_stats

        logger.info("正在為各公司生成整合總結...")
        company_summaries = []
        
        for idx, row in company_stats.iterrows():
            company_name = row['company']
            whole_content = row['whole_news_content']
            
            summary_prompt = f"""
            你是一位財經新聞總結助手。請閱讀以下關於{company_name}公司的多則新聞摘要，並用繁體中文寫出一份綜合總結，並遵守以下規範：
            1. 直接產出總結內容，不需要說明或引言。
            2. 綜合分析這家公司的整體狀況，包含正面和負面資訊。
            3. 總結長度控制在6句話以內。
            4. 重點關注對投資決策有用的資訊。

            公司：{company_name}
            相關新聞摘要：{whole_content}
            """
            
            try:
                response = ollama.chat(
                    model=self.ollama_model,
                    messages=[{"role": "user", "content": summary_prompt}]
                )
                company_summary = response['message']['content'].strip()
            except Exception as e:
                logger.error(f"生成公司總結失敗: {e}")
                company_summary = "無法生成總結"

            company_summaries.append(company_summary)
            
            if (idx + 1) % 5 == 0:
                logger.info(f"公司總結進度: [{idx + 1}/{len(company_stats)}]")
        
        company_stats['company_summary'] = company_summaries
        logger.info("公司總結完成")
        return company_stats
