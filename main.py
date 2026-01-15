import asyncio
from utils.logger import setup_logger
from config import Config
from pipeline import NewsPipeline

async def main():
    # 1. Setup Logger
    setup_logger()
    
    # 2. Validate Config
    Config.validate()

    # 3. Run Pipeline
    pipeline = NewsPipeline()
    await pipeline.run()

if __name__ == "__main__":
    asyncio.run(main())
