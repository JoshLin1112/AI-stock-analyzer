import pandas as pd
import os
import logging
from datetime import datetime

logger = logging.getLogger("StockNewsCrawler")

class TimeSeriesManager:
    """管理每日公司情緒分數的時序資料"""

    def __init__(self, company_codes_path="summarize/company_codes.csv"):
        self.company_codes_path = company_codes_path

    def update_daily_scores(self, daily_stats_df, history_file="sentiment_history.csv", date_str=None):
        """
        更新每日分數至歷史紀錄
        :param daily_stats_df: 今日分析完成的 DataFrame (包含 company, final_score)
        :param history_file: 歷史紀錄 CSV 路徑
        :param date_str: 日期字串 (YYYYMMDD)，預設為今日
        """
        logger.info("正在更新時序情緒資料...")

        # 1. 準備基礎公司列表 (所有上市櫃公司)
        if not os.path.exists(self.company_codes_path):
            logger.error(f"找不到公司代號表: {self.company_codes_path}，無法建立完整時序表。")
            return

        try:
            # 讀取公司代號表
            codes_df = pd.read_csv(self.company_codes_path, dtype=str)
            # 建立 Key: 公司名稱(股票代號)
            # 注意: company_codes.csv 欄位為 company_name, stock_code
            codes_df['company_key'] = codes_df.apply(lambda row: f"{row['company_name']}({row['stock_code']})", axis=1)
            
            # 使用 company_key 作為主要索引的 DataFrame
            base_df = codes_df[['company_key']].copy()
            base_df.columns = ['company'] # 統一欄位名稱
            
        except Exception as e:
            logger.error(f"讀取公司代號表失敗: {e}")
            return

        # 2. 準備今日數據
        if daily_stats_df is None or daily_stats_df.empty:
            logger.warning("今日無情緒數據，將填入空值")
            today_data = pd.DataFrame(columns=['company', 'final_score'])
        else:
            # 確保有需要的欄位 (CompanyAnalyzer 產出的是 avg_weighted_score)
            target_col = 'avg_weighted_score'
            if target_col not in daily_stats_df.columns:
                # 嘗試找找看有沒有 final_score (兼容性)
                if 'final_score' in daily_stats_df.columns:
                    target_col = 'final_score'
                else:
                    logger.warning(f"今日數據缺少 {target_col} 欄位，無法更新時序")
                    return
            
            today_data = daily_stats_df[['company', target_col]].copy()
            today_data = today_data.rename(columns={target_col: 'score'})

        # 設定日期欄位名稱
        if not date_str:
            date_str = datetime.now().strftime("%Y%m%d")

        # 3. 讀取或建立歷史檔案
        if os.path.exists(history_file):
            try:
                history_df = pd.read_csv(history_file)
            except Exception as e:
                logger.error(f"讀取歷史檔案失敗: {e}，將建立新檔案")
                history_df = base_df.copy()
        else:
            logger.info(f"歷史檔案不存在，建立新檔案: {history_file}")
            history_df = base_df.copy()

        # 4. 合併邏輯
        # 確保 history_df 包含所有的公司 (如果公司代號表有更新)
        # 使用 outer join 或者以 base_df 為主 left join
        # 這裡策略: 以最新的 company_codes (base_df) 為準，確保包含所有現存公司
        
        # 先將 history_df merge 到 base_df (保留歷史數據，並新增新掛牌公司，刪除下市公司?) -> 用 left join
        # 假設 history_df 第一欄是 company
        
        merged_df = pd.merge(base_df, history_df, on='company', how='left')

        # 準備今日數據的 Series (轉成 dict mapping 比較快)
        # 注意: daily_stats_df 裡面的 company 格式必須也是 "公司名稱(股票代號)"
        # 假設 CompanyAnalyzer 輸出的 company 格式已經是這樣 (因為它也是對照 company_codes 生成的)
        
        # 處理重複: 如果 daily_stats_df 有重複 company (不應該發生)，取平均或第一筆
        today_data = today_data.drop_duplicates(subset=['company'])
        today_map = today_data.set_index('company')['score'].to_dict()

        # 新增今日欄位
        # 如果欄位已存在 (例如同日重跑)，則覆蓋
        if date_str in merged_df.columns:
            logger.info(f"日期 {date_str} 已存在，將更新數據")
            
        merged_df[date_str] = merged_df['company'].map(today_map)
        
        # 格式化分數: 取小數點後兩位 (map 後可能是 float 或 NaN)
        merged_df[date_str] = merged_df[date_str].apply(lambda x: round(float(x), 2) if pd.notnull(x) and x != "" else None)

        # 5. 保存
        try:
            merged_df.to_csv(history_file, index=False, encoding='utf-8-sig')
            logger.info(f"時序資料更新完成，已保存至 {history_file}")
        except Exception as e:
            logger.error(f"保存時序資料失敗: {e}")

