import asyncio
import os
import sys
from zoneinfo import ZoneInfo
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
import pandas as pd

import logging

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

logger = logging.getLogger("StockNewsCrawler")

class CTEECrawler:
    def __init__(self, start_time: datetime, end_time: datetime, output_path: str, max_loads: int = 2, headless: bool = False):
        self.start_time = start_time
        self.end_time = end_time
        self.output_path = output_path
        self.max_loads = max_loads
        self.headless = headless
        self.browser = None
        self.context = None
        self.page = None
        self.seen_links = set()
        self.all_links = []
        self.results = []
        
    async def init_browser(self):
        """初始化瀏覽器"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
        )
        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        self.page = await self.context.new_page()
        
    async def close_browser(self):
        """關閉瀏覽器"""
        if self.browser:
            await self.browser.close()
        if hasattr(self, 'playwright'):
            await self.playwright.stop()
            
    # def parse_news_datetime(self, date_time_str):
    #     """解析新聞時間格式 , ex:'2025.08.22 14:00'"""
    #     try:
    #         return datetime.strptime(date_time_str.strip(), '%Y.%m.%d %H:%M')
    #     except ValueError as e:
    #         logger.warning(f"[警告] 時間格式解析失敗: {date_time_str}, 錯誤: {e}")
    #         return None
# from datetime import datetime
    from zoneinfo import ZoneInfo

    def parse_news_datetime(self, date_time_str):
        """解析工商時報新聞的日期格式，支援多種情況"""
        from zoneinfo import ZoneInfo
        local_tz = ZoneInfo("Asia/Taipei")
        date_time_str = date_time_str.strip().replace("　", "").replace("\xa0", "")  # 移除奇怪空白

        formats = [
            "%Y.%m.%d %H:%M",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y.%m.%d %H:%M",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_time_str, fmt)
                if fmt == "%Y.%m.%d":
                    dt = dt.replace(hour=0, minute=0)
                return dt.replace(tzinfo=local_tz)
            except ValueError:
                continue

        logger.warning(f"[警告] 時間格式解析失敗: {date_time_str}")
        return None

    def should_skip_article(self, news_datetime):
        """判斷文章時間是否在指定範圍內"""
        if news_datetime is None:
            return True  # 無法解析時間，跳過該文章
        return not (self.start_time <= news_datetime <= self.end_time)
        
    def should_stop_scraping(self, news_datetime):
        """判斷是否應該停止爬取（時間早於開始時間）"""
        if news_datetime is None:
            return False
        return news_datetime < self.start_time
        
    async def scrape_current_page(self):
        """抓取當前頁面的新聞連結"""
        cards = await self.page.locator('.newslist__card').all()
        for card in cards:
            try:
                title_el = card.locator('h3.news-title a')
                title = await title_el.inner_text()
                href = await title_el.get_attribute('href')
                date_el = card.locator('time.news-time')
                news_date = await date_el.inner_text()
                
                if href and href not in self.seen_links:
                    self.seen_links.add(href)
                    self.all_links.append({
                        "title": title.strip(),
                        "href": href,
                        "date": news_date.strip()
                    })
            except Exception as e:
                logger.error(f"[錯誤] 抓取新聞失敗: {e}")
                
    async def load_news_list(self):
        """載入新聞列表頁面"""
        logger.info("🚀 執行工商時報新聞爬蟲...")
        await self.page.goto("https://www.ctee.com.tw/stock/twmarket", timeout=60000)
        
        # 第一次抓取
        await self.scrape_current_page()
        
        # 模擬滑動並點擊載入更多
        for i in range(self.max_loads):
            try:
                await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                await self.page.wait_for_timeout(1500)
                
                load_more_btn = self.page.locator("button:has-text('載入更多')")
                if await load_more_btn.is_visible():
                    await load_more_btn.click()
                    await self.page.wait_for_timeout(2000)  # 等待新聞載入
                    await self.scrape_current_page()
                    logger.info(f"✅ 已完成第 {i+1} 次載入，累積 {len(self.all_links)} 則新聞")
                else:
                    logger.warning("⚠️ 找不到『載入更多』按鈕，提前結束")
                    break
            except Exception as e:
                logger.error(f"[錯誤] 點擊載入更多失敗: {e}")
                break
                
    async def scrape_article_content(self, item):
        """抓取單篇文章內容"""
        link = item['href']
        full_url = link if link.startswith("http") else f"https://www.ctee.com.tw{link}"
        
        try:
            await self.page.goto(full_url, timeout=30000, wait_until="domcontentloaded")
            logger.info(f"✅ 已進入 {full_url}")
            
            # 抓取標題
            title_el = await self.page.query_selector("h1.main-title")
            title = (await title_el.inner_text()).strip() if title_el else ""
            
            # 抓取日期和時間
            await self.page.wait_for_selector("ul.news-credit li.publish-date time", timeout=5000)
            date_el = await self.page.query_selector("ul.news-credit li.publish-date time")
            time_el = await self.page.query_selector("ul.news-credit li.publish-time time")
            date_text = (await date_el.inner_text()).strip() if date_el else ""
            time_text = (await time_el.inner_text()).strip() if time_el else ""
            
            # 組合完整時間字串
            datetime_str = f"{date_text} {time_text}".strip()
            print(datetime_str)
            # 檢查時間是否在指定範圍內
            news_datetime = self.parse_news_datetime(datetime_str)
            print(news_datetime)
            # 如果時間早於開始時間，停止爬取
            if self.should_stop_scraping(news_datetime):
                logger.info(f"⏹️ 新聞時間 {datetime_str} 早於開始時間 {self.start_time}，停止爬取")
                return False
            
            # 如果時間不在範圍內，跳過這篇文章
            if self.should_skip_article(news_datetime):
                logger.info(f"⏭️ 新聞時間 {datetime_str} 不在指定範圍內，跳過該文章")
                return True
            
            # 抓取內容
            paragraphs = await self.page.query_selector_all("article p")
            content = "\n".join([
                (await p.inner_text()).strip() 
                for p in paragraphs 
                if (await p.inner_text()).strip()
            ])
            
            self.results.append({
                "time": datetime_str,
                "title": title,
                "content": content
            })
            
            return True  # 返回 True 表示繼續爬取
            
        except Exception as e:
            logger.error(f"[錯誤] 進入內頁失敗: {e}")
            return True  # 發生錯誤但繼續爬取下一篇
            
    async def scrape_all_articles(self):
        """爬取所有文章內容"""
        logger.info(f"📰 開始爬取 {len(self.all_links)} 則新聞內容...")
        logger.info(f"⏰ 時間範圍: {self.start_time} 至 {self.end_time}")
        
        for i, item in enumerate(self.all_links, 1):
            logger.info(f"📄 處理第 {i}/{len(self.all_links)} 篇新聞")
            
            # 如果返回 False，表示應該停止爬取
            should_continue = await self.scrape_article_content(item)
            if not should_continue:
                logger.info(f"🛑 已停止爬取，共處理了 {len(self.results)} 篇新聞")
                break
                
    def save_to_csv(self):
        """儲存結果到 CSV 檔案"""
        if not self.results:
            logger.warning("⚠️ 沒有數據可供儲存")
            return
            
        df = pd.DataFrame(self.results)
        output_dir = os.path.dirname(self.output_path)
        
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
        logger.info(f"✅ 資料儲存至 {self.output_path}")
        
    async def run(self):
        """執行完整的爬蟲流程"""
        try:
            # logger.info("🚀 開始執行新聞爬蟲...")
            
            # 初始化瀏覽器
            await self.init_browser()
            
            # 載入新聞列表
            await self.load_news_list()
            logger.info(f"📋 共找到 {len(self.all_links)} 則新聞連結")
            
            # 爬取文章內容
            await self.scrape_all_articles()
            
            # 儲存結果
            self.save_to_csv()
            
        except Exception as e:
            logger.error(f"[嚴重錯誤] {e}")
        finally:
            await self.close_browser()
            logger.info("🔚 爬蟲執行完成")


async def main():
    """主函式"""
    # 設定時間範圍（例如：昨天14:00到今天14:00）
    start_time = datetime.now() - timedelta(days=1)
    start_time = start_time.replace(hour=14, minute=0, second=0, microsecond=0)
    
    end_time = datetime.now().replace(hour=14, minute=0, second=0, microsecond=0)
    
    output_path = "news/ctee_news.csv"
    max_loads = 2

    scraper = CTEECrawler(
        start_time=start_time,
        end_time=end_time,
        output_path=output_path,
        max_loads=max_loads,
        headless=False  # 設定 headless=True 可在背景執行
    )
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())