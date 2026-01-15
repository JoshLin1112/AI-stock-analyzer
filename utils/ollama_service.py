import subprocess
import time
import logging
import ollama
from config import Config

logger = logging.getLogger("StockNewsCrawler")

class OllamaService:
    """Ollama 服務管理"""
    
    def __init__(self):
        self._ollama_proc = None
        self.model = Config.OLLAMA_MODEL

    def start(self):
        """確保 Ollama 伺服器在跑；若沒跑就以背景模式啟動 `ollama serve`。"""
        try:
            # 若伺服器已在跑，這會成功；否則會拋例外
            _ = ollama.list()
            logger.info("Ollama 伺服器已在執行。")
            
            # 確保模型存在
            self._pull_model()
            return
        except Exception:
            logger.info("未偵測到 Ollama 伺服器，啟動 Ollama")

        # 背景啟動 serve（不阻塞）
        try:
            self._ollama_proc = subprocess.Popen(
                ["ollama", "serve"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
                start_new_session=True  # 避免跟父程序綁在一起
            )
            # 簡單等候就緒
            for _ in range(30):
                try:
                    _ = ollama.list()
                    break
                except Exception:
                    time.sleep(0.5)
            else:
                logger.warning("⚠️ 無法確認 Ollama 伺服器啟動是否成功，稍後呼叫時若失敗請檢查本機環境。")
        except Exception as e:
            logger.error(f"⚠️ 啟動 `ollama serve` 失敗：{e}")

        # 確保模型存在
        self._pull_model()

    def _pull_model(self):
        try:
            logger.info(f"檢查/拉取模型 {self.model} ...")
            ollama.pull(self.model)
        except Exception as e:
            logger.warning(f"⚠️ 拉取模型失敗（可能已存在）：{e}")

    def stop(self):
        """若是我們自己啟動的 serve，幫忙關掉；若原本就有，則不處理。"""
        if self._ollama_proc and self._ollama_proc.poll() is None:
            logger.info("正在關閉由本程式啟動的 Ollama 伺服器 ...")
            self._ollama_proc.terminate()
            try:
                self._ollama_proc.wait(timeout=5)
            except Exception:
                self._ollama_proc.kill()
            logger.info("Ollama 已關閉")
        else:
            logger.info("略過關閉：偵測到 Ollama 並非由本程式啟動或已不在執行。")
