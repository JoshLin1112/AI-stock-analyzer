import asyncio
import time
import logging
from utils.logger import setup_logger
from config import Config
from pipeline import NewsPipeline

async def main():
    start_time = time.time()

    # 1. Setup Logger
    setup_logger()
    logger = logging.getLogger("StockNewsCrawler")
    
    # 2. Validate Config
    Config.validate()

    # 3. Run Pipeline
    pipeline = NewsPipeline()
    await pipeline.run()

    end_time = time.time()
    elapsed_time = (end_time - start_time)/60
    # 修改為分鐘
    logger.info(f"總執行時間: {elapsed_time:.2f} 分鐘")

if __name__ == "__main__":
    asyncio.run(main())
