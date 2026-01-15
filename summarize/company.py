import logging
import pandas as pd
import re
import ollama
import concurrent.futures
from typing import Dict, List, Optional, Tuple
from config import Config
from prompts import Prompts
from summarize.validator import SummaryValidator

logger = logging.getLogger("StockNewsCrawler")

class CompanyAnalyzer:
    """公司情緒分析器"""
    
    def __init__(self):
        self.ollama_model = Config.OLLAMA_MODEL
        self.name2code = self._load_company_codes()
        self.validator = SummaryValidator()

    def _load_company_codes(self) -> Dict[str, str]:
        """載入公司代號對照表"""
        try:
            company_df = pd.read_csv(Config.COMPANY_CODES_PATH, dtype=str)
            return dict(zip(company_df["company_name"], company_df["stock_code"]))
        except FileNotFoundError:
            logger.error(f"找不到 {Config.COMPANY_CODES_PATH}，無法進行公司代號驗證。")
            return {}
        except Exception as e:
            logger.error(f"讀取公司代號表失敗: {e}")
            return {}

    @staticmethod
    def normalize_company_name(company_name: str) -> str:
        """標準化公司名稱字串"""
        return (
            company_name.strip()
                    .replace('、', '')
                    .replace(',', '')
                    .replace('　', '')
                    .replace(' ', '')
                    .replace('-TW', '')
        )
    
    def extract_companies_from_text(self, text: str) -> List[str]:
        """從文本中提取公司資訊，僅保留正確代號"""
        if not self.name2code:
            return []

        companies = []
        pattern = r'新聞提及公司[：:](.*?)(?=\n|$)'
        match = re.search(pattern, text)

        if match:
            company_text = match.group(1)
            company_pattern = r'([^(]+)\((\d+)\)'
            matches = re.findall(company_pattern, company_text)

            for company_name, stock_code in matches:
                company_name = self.normalize_company_name(company_name)

                # 如果公司名稱存在於對照表
                if company_name in self.name2code:
                    correct_code = self.name2code[company_name]
                    # 代號符合對照表 → 保留
                    if stock_code == correct_code:
                        companies.append(f"{company_name}({stock_code})")
        return companies
    
    def expand_news_by_company(self, df: pd.DataFrame) -> pd.DataFrame:
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
    
    def calculate_company_sentiment_stats(self, expanded_df: pd.DataFrame) -> pd.DataFrame:
        """計算公司情緒統計"""
        logger.info("正在計算公司情緒統計...")
        
        if expanded_df.empty:
            return pd.DataFrame()

        # 移除無公司資訊的行
        valid_df = expanded_df[expanded_df['company'].notna()].copy()
        
        if valid_df.empty:
            return pd.DataFrame()

        # 這裡假設已經有 final_score，因為 sentiment analyzer 已經計算了
        if 'final_score' in valid_df.columns:
            valid_df['weighted_score'] = valid_df['final_score'].astype(float)
        else:
             # Just in case fallback
            logger.warning("未自 Dataframe 找到 final_score，使用預設值 0.5")
            valid_df['weighted_score'] = 0.5
        
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
    def _combine_news_content(summaries: pd.Series) -> str:
        """將多則新聞摘要整合成一段文字"""
        combined_content = []
        for i, summary in enumerate(summaries, 1):
            # 移除新聞提及公司的部分，只保留摘要內容
            content = summary.split("新聞提及公司")[0].strip()
            combined_content.append(f"第{i}則新聞:{content}")
        
        return " \\n ".join(combined_content)
    
    def add_company_summary(self, company_stats: pd.DataFrame) -> pd.DataFrame:
        """為每間公司的整合新聞內容生成總結 (平行化)"""
        if company_stats.empty:
            return company_stats

        logger.info("正在平行為各公司生成整合總結...")
        
        results = {}
        total = len(company_stats)
        completed = 0
        
        # 建立索引對應表 (因為 groupby 後的 index 可能不連續)
        idx_list = list(company_stats.index)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            future_to_idx = {
                executor.submit(
                    self._generate_single_company_summary, 
                    row['company'], 
                    row['whole_news_content']
                ): idx 
                for idx, row in company_stats.iterrows()
            }
            
            for future in concurrent.futures.as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    summary = future.result()
                    results[idx] = summary
                except Exception as e:
                    logger.error(f"公司 {idx} 總結生成失敗: {e}")
                    results[idx] = None
                
                completed += 1
                if completed % 5 == 0:
                    logger.info(f"公司總結進度: [{completed}/{total}]")
        
        # 填回結果
        company_stats['company_summary'] = company_stats.index.map(lambda x: results.get(x))
        
        # 排除無效資料 (刪除 company_summary 為 None 的行)
        original_count = len(company_stats)
        company_stats = company_stats.dropna(subset=['company_summary'])
        filtered_count = len(company_stats)
        
        if original_count != filtered_count:
            logger.info(f"已排除 {original_count - filtered_count} 筆無效或格式錯誤的總結")

        logger.info("公司總結完成")
        return company_stats
    
    def _generate_single_company_summary(self, company_name: str, whole_content: str) -> Optional[str]:
        """生成單一公司的總結"""
        summary_prompt = Prompts.COMPANY_SUMMARY.format(
            company_name=company_name,
            whole_content=whole_content
        )
        
        try:
            response = ollama.chat(
                model=self.ollama_model,
                messages=[{"role": "user", "content": summary_prompt}]
            )
            company_summary = response['message']['content'].strip()
            
            # 驗證機制
            is_valid = self.validator.validate(company_summary, company_name)
            if not is_valid:
                return None
            
            return company_summary
        except Exception as e:
            logger.error(f"生成公司總結失敗 ({company_name}): {e}")
            return None

