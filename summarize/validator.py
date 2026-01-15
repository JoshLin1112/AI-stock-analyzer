import logging
import ollama
from config import Config
from prompts import Prompts

logger = logging.getLogger("StockNewsCrawler")

class SummaryValidator:
    """摘要驗證器"""

    def __init__(self):
        self.ollama_model = Config.OLLAMA_MODEL

    def validate(self, summary, company_name):
        """
        驗證摘要是否符合標準
        :param summary: 待驗證的摘要內容
        :param company_name: 目標公司名稱
        :return: (bool) True 表示合格, False 表示不合格
        """
        if not summary or len(summary) < 5:
            # 內容過短直接視為無效
            return False

        prompt = Prompts.SUMMARY_VALIDATION.format(
            summary=summary,
            company_name=company_name
        )

        try:
            response = ollama.chat(
                model=self.ollama_model,
                messages=[{"role": "user", "content": prompt}]
            )
            result = response['message']['content'].strip().upper()
            
            # 簡單判斷：只要包含 YES 就當作通過
            if "YES" in result:
                return True
            else:
                logger.warning(f"摘要驗證未通過 ({company_name}): {result}")
                logger.debug(f"被拒絕的摘要: {summary[:50]}...")
                return False
                
        except Exception as e:
            logger.error(f"驗證過程發生錯誤: {e}")
            # 若驗證過程出錯，保守起見保留內容，或視需求決定策略。
            # 這裡假設若 LLM 壞了，就不因為驗證失敗而丟失資料，暫回 True。
            # 但使用者要求嚴格檢查，也可以回 False。
            # 考慮到依賴性，這裡回 False 比較安全 (避免顯示錯誤資訊)
            return False
