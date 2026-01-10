
import logging
import sys

def setup_logger():
    """設定並返回一個 logger 實例"""
    logger = logging.getLogger("StockNewsCrawler")
    logger.setLevel(logging.INFO)
    
    # 如果已經有 handler，就不再新增，避免重複輸出
    if logger.hasHandlers():
        return

    # 設定格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 輸出到 console
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
