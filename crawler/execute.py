import asyncio
import os
from datetime import datetime
import logging
import sys
from typing import List, Optional

# Add project root to the Python path to resolve module imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config import Config
from utils.date_utils import get_taipei_time_window
from crawler.cnyes import CNYESCrawler
from crawler.eco import ECOCrawler
from crawler.ctee import CTEECrawler

logger = logging.getLogger("StockNewsCrawler")

class MultiCrawlerManager:
    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = output_dir or Config.NEWS_OUTPUT_DIR
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Get time window
        self.start_time, self.end_time = get_taipei_time_window()
        
        # Initialize Crawlers
        self.cnyes = CNYESCrawler(
            self.start_time, 
            self.end_time, 
            os.path.join(self.output_dir, "cnyes_news.csv")
        )
        self.eco = ECOCrawler(
            self.start_time, 
            self.end_time, 
            os.path.join(self.output_dir, "eco_news.csv")
        )
        self.ctee = CTEECrawler(
            self.start_time, 
            self.end_time, 
            os.path.join(self.output_dir, "ctee_news.csv"), 
            max_loads=2
        )

    async def run_all(self) -> List[str]:
        """
        Execute all crawlers concurrently.
        CNYES and ECO are synchronous, so they are wrapped in threads.
        CTEE is asynchronous, so it is awaited directly.
        """
        logger.info("=== Starting Multi-Crawler Execution ===")
        logger.info(f"Time Window: {self.start_time} ~ {self.end_time}")

        async def run_sync_crawler(crawler):
            await asyncio.to_thread(crawler.run)

        await asyncio.gather(
            run_sync_crawler(self.cnyes),
            run_sync_crawler(self.eco),
            self.ctee.run(), # CTEE is natively async
        )

        logger.info("=== All Crawlers Finished ===")
        self.check_output_files()
        
        return [
            self.cnyes.output_path,
            self.eco.output_path,
            self.ctee.output_path,
        ]

    def check_output_files(self):
        """Check if output files exist and log their status."""
        logger.info("\n=== Checking Output Files ===")
        crawlers = [self.cnyes, self.eco, self.ctee]
        for crawler in crawlers:
            file_path = crawler.output_path
            if os.path.exists(file_path):
                size = os.path.getsize(file_path)
                logger.info(f"✓ {os.path.basename(file_path)} exists ({size} bytes)")
            else:
                logger.warning(f"✗ {os.path.basename(file_path)} NOT found")

if __name__ == "__main__":
    from utils.logger import setup_logger
    setup_logger()
    manager = MultiCrawlerManager()
    asyncio.run(manager.run_all())
