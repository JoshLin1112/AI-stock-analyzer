import ollama
from transformers import pipeline
import logging
from config import Config

logger = logging.getLogger("StockNewsCrawler")

class SentimentAnalyzer:
    """情緒分析與翻譯器"""
    
    def __init__(self):
        self.ollama_model = Config.OLLAMA_MODEL
        self.finbert_pipe = None

    def generate_sentiment_label(self, summary):
        """生成情緒標籤 (LLM)"""
        prompt = f"""
        你是一位財經新聞閱讀助手。請閱讀以下新聞摘要，判斷這段新聞的資訊，以投資的角度而言是正面訊息還是負面訊息，並遵守以下規範：
        1. 直接輸出「正面」或「負面」，不需要說明或引言。
        2. 若沒有明顯的正面或負面資訊，則輸出「中性」，但請盡量避免此種判斷。

        新聞摘要：{summary}
        """
        
        try:
            response = ollama.chat(
                model=self.ollama_model,
                messages=[{"role": "user", "content": prompt}]
            )
            return response['message']['content'].strip()
        except Exception as e:
            logger.error(f"生成情緒標籤失敗: {e}")
            return "中性"

    def translate_to_english(self, summary):
        """翻譯摘要為英文"""
        try:
            # 移除公司資訊部分
            content = summary.split("新聞提及公司")[0]
            
            prompt = f"""
            你是一位財經新聞翻譯助手。請用英文翻譯以下新聞摘要，並遵守以下規範：
            1. 直接輸出翻譯結果，不需要說明或引言。
            2. 請不要改變新聞原意與使用某些偏正向的詞彙。

            新聞摘要：{content}
            """
            
            response = ollama.chat(
                model=self.ollama_model,
                messages=[{"role": "user", "content": prompt}]
            )
            
            return response['message']['content'].strip()
        except Exception as e:
            logger.error(f"翻譯失敗: {e}")
            return ""

    def initialize_finbert(self):
        """初始化 FinBERT 模型"""
        if self.finbert_pipe is None:
            logger.info("正在載入 FinBERT 模型...")
            try:
                self.finbert_pipe = pipeline("text-classification", model="ProsusAI/finbert")
                logger.info("FinBERT 模型載入完成")
            except Exception as e:
                logger.error(f"FinBERT 模型初始化失敗: {e}")

    def apply_finbert_analysis(self, df):
        """應用 FinBERT 分析"""
        self.initialize_finbert()
        if not self.finbert_pipe:
            logger.warning("FinBERT 未初始化，跳過分析")
            df['finbert_label'] = "neutral"
            return df
            
        logger.info("正在應用 FinBERT 分析...")
        
        def get_finbert_label(text):
            if not text: return "neutral"
            try:
                return self.finbert_pipe(text)[0]['label']
            except Exception:
                return "neutral"

        df['finbert_label'] = df['translation'].apply(get_finbert_label)
        return df

    def process_sentiment_and_translation(self, df):
        """批量處理情緒分析和翻譯"""
        logger.info("正在進行情緒分析和翻譯...")
        labels = []
        translations = []
        
        for idx, row in df.iterrows():
            label = self.generate_sentiment_label(row['summary'])
            translation = self.translate_to_english(row['summary'])
            
            labels.append(label)
            translations.append(translation)
            
            if (idx + 1) % 10 == 0:
                logger.info(f"情緒分析進度: [{idx + 1}/{len(df)}]")
        
        df['ai_label'] = labels
        df['translation'] = translations
        return df
