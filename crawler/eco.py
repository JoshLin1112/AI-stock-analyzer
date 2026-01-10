import os
import sys
import time
import requests
import urllib.parse
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
import logging
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

logger = logging.getLogger("StockNewsCrawler")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://money.udn.com/money/cate/5590',
    'Connection': 'keep-alive'
}

class ECOCrawler:
    
    BASE_URL = "https://money.udn.com/"
    LIST_URL = "https://money.udn.com/money/cate/5590"

    def __init__(self, start_time: datetime, end_time: datetime, output_path: str, sleep: float = 0.5):
        """
        :param start_time: 開始時間 (datetime, tz-aware)
        :param end_time: 結束時間 (datetime, tz-aware)
        :param output_path: 輸出檔案路徑 (str)
        :param sleep: 抓取文章時的延遲秒數 (float)
        """
        self.start_time = start_time
        self.end_time = end_time
        self.output_path = output_path
        self.sleep = sleep
        self.data = []

    def _fetch_list_page(self):
        resp = requests.get(self.LIST_URL, headers=HEADERS, timeout=10)
        resp.encoding = resp.apparent_encoding or "utf-8"
        return BeautifulSoup(resp.text, "html.parser")

    def _fetch_links_from_soup(self, soup):
        results = []
        for wrapper in soup.select("div.story-headline-wrapper"):
            link_tag = wrapper.select_one("div.story__content a")
            title_tag = wrapper.select_one("h3.story__headline")
            time_tag = wrapper.select_one("time.rank__time") or wrapper.select_one("time")

            if not link_tag or not title_tag or not time_tag:
                continue

            href = urllib.parse.urljoin(self.BASE_URL, link_tag.get("href", "").strip())
            title = title_tag.get_text(strip=True)
            time_str = time_tag.get_text(strip=True)

            results.append({"title": title, "link": href, "time": time_str})
        return results

    @staticmethod
    def _parse_time_to_tz(time_str, tz=pytz.timezone("Asia/Taipei")):
        s = time_str.strip()
        # 嘗試常見格式
        for fmt in ("%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y.%m.%d"):
            try:
                dt = datetime.strptime(s, fmt)
                return tz.localize(dt)
            except Exception:
                continue
        raise ValueError(f"無法解析時間字串: {time_str}")

    def _filter_links_by_time(self, links):
        filtered = []
        for item in links:
            try:
                news_dt = self._parse_time_to_tz(item["time"])
            except Exception:
                continue

            if self.start_time <= news_dt <= self.end_time:
                filtered.append(item)
        return filtered

    @staticmethod
    def _fetch_article_content(url, timeout=10):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
        except Exception as e:
            logger.error(f"[fetch_article_content] 請求失敗: {url} -> {e}")
            return ""

        resp.encoding = resp.apparent_encoding or "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")

        content_section = soup.select_one("section.article-body__editor") or soup.select_one("#article_body")
        if not content_section:
            return ""

        paragraphs = []
        for p in content_section.find_all("p"):
            text = p.get_text(separator=" ", strip=True)
            if len(text) < 5:
                continue
            paragraphs.append(text)

        return "\n\n".join(paragraphs)

    def crawl(self):
        """執行爬蟲，將結果存入 self.data"""
        logger.info("🚀 執行經濟日報新聞爬蟲...")
        soup = self._fetch_list_page()
        links = self._fetch_links_from_soup(soup)
        logger.info(f"抓到 {len(links)} 篇候選文章（未過濾）")

        filtered_links = self._filter_links_by_time(links)
        logger.info(f"過濾後剩下 {len(filtered_links)} 篇")

        for i, item in enumerate(filtered_links, start=1):
            if i % 5 == 0:
                logger.info(f"處理第 {i} 筆新聞...")

            content = self._fetch_article_content(item["link"])
            self.data.append({
                "time": item["time"],
                "title": item["title"],
                "content": content
            })
            time.sleep(self.sleep)

    def save(self):
        if not self.data:
            logger.warning("⚠️ No data to save.")
            return

        df = pd.DataFrame(self.data)
        output_dir = os.path.dirname(self.output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        df.to_csv(self.output_path, index=False, encoding="utf-8-sig")
        logger.info(f"✅ 資料儲存至 {self.output_path}")

    def run(self):
        self.crawl()
        self.save()


# --- 測試 ---
if __name__ == "__main__":
    tz = pytz.timezone("Asia/Taipei")
    now = datetime.now(tz)
    yesterday_14 = (now - timedelta(days=1)).replace(hour=14, minute=0, second=0, microsecond=0)

    crawler = ECOCrawler(
        start_time=yesterday_14,
        end_time=now,
        output_path="./news/eco_news.csv"
    )
    crawler.run()
