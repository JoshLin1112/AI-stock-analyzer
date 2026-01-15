import ollama
from transformers import pipeline
import logging
import concurrent.futures
import json
import re
from typing import Tuple, Dict, Any

from config import Config
from prompts import Prompts

logger = logging.getLogger("StockNewsCrawler")

class SentimentAnalyzer:
    """情緒分析與翻譯器"""
    
    def __init__(self):
        self.ollama_model = Config.OLLAMA_MODEL
        self.finbert_pipe = None

    def generate_sentiment_label(self, summary: str) -> Tuple[str, float]:
        """生成情緒標籤 (LLM) - 返回 label 與 confidence"""
        prompt = Prompts.SENTIMENT_ANALYSIS.format(summary=summary)

        try:
            response = ollama.chat(
                model=self.ollama_model,
                messages=[{"role": "user", "content": prompt}]
            )
            content = response['message']['content'].strip()
            
            # --- 解析邏輯 ---
            # 1. 嘗試清理 Markdown
            clean_content = content
            if "```json" in content:
                clean_content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                clean_content = content.split("```")[1].split("```")[0].strip()
            
            # 2. 嘗試直接 JSON loads
            try:
                result = json.loads(clean_content)
                if isinstance(result, list) and result: result = result[0]
                if isinstance(result, dict):
                    label = result.get("label", "neu")
                    confidence = float(result.get("confidence", 0.5))
                    return self._normalize_label(label), confidence
            except json.JSONDecodeError:
                pass # 繼續嘗試 Regex
            
            # 3. Regex Fallback
            label_match = re.search(r'"label"\s*:\s*"(\w+)"', content)
            label = "neu"
            if label_match:
                label = label_match.group(1)
            
            conf_match = re.search(r'"confidence"\s*:\s*([\d\.]+)', content)
            confidence = 0.5
            if conf_match:
                try:
                    confidence = float(conf_match.group(1))
                except ValueError:
                    confidence = 0.5
                    
            if label_match or conf_match:
                return self._normalize_label(label), confidence

            logger.warning(f"LLM 回傳無法解析: {content[:100]}...")
            return "neu", 0.5

        except Exception as e:
            logger.error(f"生成情緒標籤失敗: {e}")
            return "neu", 0.5

    def _normalize_label(self, label: str) -> str:
        """正規化情緒標籤"""
        map_label = {
            "positive": "pos", "pos": "pos",
            "neutral": "neu", "neu": "neu",
            "negative": "neg", "neg": "neg",
            "正面": "pos", "中性": "neu", "負面": "neg"
        }
        return map_label.get(str(label).lower(), "neu")

    def translate_to_english(self, summary: str) -> str:
        """翻譯摘要為英文"""
        try:
            # 移除公司資訊部分
            content = summary.split("新聞提及公司")[0]
            
            prompt = Prompts.TRANSLATION.format(content=content)
            
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

    def get_finbert_result(self, text: str) -> Tuple[str, float]:
        """獲取 FinBERT 結果 (Label, Confidence)"""
        if not self.finbert_pipe or not text:
            return "neu", 0.5
        
        try:
            result = self.finbert_pipe(text)[0]
            label_map = {"positive": "pos", "neutral": "neu", "negative": "neg"}
            label = label_map.get(result['label'], "neu")
            confidence = result['score']
            return label, confidence
        except Exception as e:
            return "neu", 0.5

    def calculate_score(self, label: str, confidence: float) -> float:
        """計算情緒分數"""
        term = (confidence / 0.67 - 33 / 67) / 2
        
        if label == "pos":
            return 0.5 + term
        elif label == "neu":
            return 0.5
        else: # neg
            return 0.5 - term

    def process_row_task(self, row: Any) -> Dict[str, Any]:
        """處理單行數據的任務：並行執行 LLM 和 翻譯+FinBERT"""
        summary = row['summary']
        
        def task_llm():
            return self.generate_sentiment_label(summary)
            
        def task_trans_finbert():
            translation = self.translate_to_english(summary)
            f_label, f_conf = self.get_finbert_result(translation)
            return translation, f_label, f_conf

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_llm = executor.submit(task_llm)
            future_trans = executor.submit(task_trans_finbert)
            
            llm_label, llm_conf = future_llm.result()
            translation, finbert_label, fin_conf = future_trans.result()
            
        llm_score = self.calculate_score(llm_label, llm_conf)
        finbert_score = self.calculate_score(finbert_label, fin_conf)
        
        # Determine weights
        final_score = round(
            Config.WEIGHT_AI * llm_score + Config.WEIGHT_FINBERT * finbert_score, 
            2
        )
        
        return {
            "translation": translation,
            "ai_label": llm_label,
            "llm_confidence": llm_conf,
            "llm_score": llm_score,
            "finbert_label": finbert_label,
            "finbert_confidence": fin_conf,
            "finbert_score": finbert_score,
            "final_score": final_score
        }

    def process_sentiment_and_translation(self, df):
        """批量處理情緒分析 (包含 LLM, 翻譯, FinBERT, 分數計算)"""
        self.initialize_finbert()
        logger.info("正在進行並行情緒分析與翻譯...")
        
        results = {}
        total = len(df)
        completed = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            future_to_idx = {
                executor.submit(self.process_row_task, row): idx 
                for idx, row in df.iterrows()
            }
            
            for future in concurrent.futures.as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    res = future.result()
                    results[idx] = res
                except Exception as e:
                    logger.error(f"Row {idx} 處理失敗: {e}")
                    results[idx] = {
                        "translation": "", "ai_label": "neu", "llm_confidence": 0.5, 
                        "llm_score": 0.5, "finbert_label": "neu", "finbert_confidence": 0.5, 
                        "finbert_score": 0.5, "final_score": 0.5
                    }
                
                completed += 1
                if completed % 5 == 0:
                    logger.info(f"分析進度: [{completed}/{total}]")

        # 將結果填回 DataFrame
        new_cols = ["translation", "ai_label", "llm_confidence", "llm_score", 
                    "finbert_label", "finbert_confidence", "finbert_score", "final_score"]
        
        for col in new_cols:
            df[col] = None
            
        for idx, res in results.items():
            for col in new_cols:
                df.at[idx, col] = res.get(col)
                
        return df

