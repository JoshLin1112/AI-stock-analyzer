import asyncio
import os
from datetime import datetime, timedelta
import pytz
import sys

import logging

# Add project root to the Python path to resolve module imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

logger = logging.getLogger("StockNewsCrawler")


from crawler.cnyes import CNYESCrawler
from crawler.eco import ECOCrawler
from crawler.ctee import CTEECrawler


class MultiCrawlerManager:
    def __init__(self, output_dir="./news"):
        tz = pytz.timezone("Asia/Taipei")
        now = datetime.now(tz)
        yesterday_14 = (now - timedelta(days=2)).replace(hour=14, minute=0, second=0, microsecond=0)

        self.start_time = yesterday_14
        self.end_time = now
        self.output_dir = output_dir

        self.crawlers = [
            CNYESCrawler(self.start_time, self.end_time, os.path.join(output_dir, "cnyes_news.csv")),
            ECOCrawler(self.start_time, self.end_time, os.path.join(output_dir, "eco_news.csv")),
            CTEECrawler(self.start_time, self.end_time, os.path.join(output_dir, "ctee_news.csv"), max_loads=2),
        ]

    async def run_all(self):
        """執行所有爬蟲"""
        logger.info("=== 開始執行所有爬蟲 ===")

        # 先跑同步的兩個 (CNYES + ECO)
        for crawler in self.crawlers[:2]:
            # logger.info(f"🚀 執行 {crawler.__class__.__name__}")
            crawler.run()

        # 再跑非同步的 CTEE
        # logger.info(f"🚀 執行 {self.crawlers[2].__class__.__name__}")
        await self.crawlers[2].run()

        logger.info("=== 所有爬蟲執行完畢 ===")
        self.check_output_files()

    def check_output_files(self):
        """檢查輸出檔案是否生成"""
        logger.info("\n=== 檢查輸出檔案 ===")
        for crawler in self.crawlers:
            file_path = crawler.output_path
            if os.path.exists(file_path):
                size = os.path.getsize(file_path)
                logger.info(f"✓ {file_path} 存在 ({size} bytes)")
            else:
                logger.warning(f"✗ {file_path} 不存在")


if __name__ == "__main__":
    manager = MultiCrawlerManager()
    asyncio.run(manager.run_all())
