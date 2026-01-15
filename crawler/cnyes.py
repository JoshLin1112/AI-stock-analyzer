import sys
import requests
import os
import re
from html import unescape
from datetime import datetime, timedelta, timezone
import pandas as pd

import logging

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

logger = logging.getLogger("StockNewsCrawler")


class CNYESCrawler:
    def __init__(self, start_time: datetime, end_time: datetime, output_path: str, max_page: int = 3):
        """
        :param start_time: 開始時間 (datetime)
        :param end_time: 結束時間 (datetime)
        :param output_path: 輸出檔案路徑 (str)
        :param max_page: 要抓取的最大頁數 (int, 預設3頁)
        """
        self.tz = timezone(timedelta(hours=8))  # 台灣時區

        # 確保時間轉換成台灣時區後再取 timestamp
        start_time = start_time.astimezone(self.tz)
        end_time = end_time.astimezone(self.tz)

        self.start_time_dt = start_time  # 保留 datetime 方便後續過濾
        self.end_time_dt = end_time

        self.start_time = int(start_time.timestamp())
        self.end_time = int(end_time.timestamp())
        self.output_path = output_path
        self.max_page = max_page
        self.data = []

    def _fetch_page(self, page=1, limit=30, isCategoryHeadline=0):
        """抓取單頁新聞"""
        url = "https://api.cnyes.com/media/api/v1/newslist/category/tw_stock"
        params = {
            "page": page,
            "limit": limit,
            "isCategoryHeadline": isCategoryHeadline,
            "startAt": self.start_time,
            "endAt": self.end_time
        }
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        return resp.json()["items"]

    @staticmethod
    def _clean_html_content(text):
        """清理 HTML 標籤和亂碼"""
        if not text:
            return ""
        return re.sub(r'<[^>]+>|&[a-z0-9]+;|\s+', ' ', unescape(text)).strip()

    def crawl(self):
        """執行爬蟲，將結果存入 self.data"""
        logger.info("執行鉅亨網新聞爬蟲...")
        for page in range(1, self.max_page + 1):
            items = self._fetch_page(page=page, limit=30)
            logger.info(f"鉅亨網總新聞數: {items['total']} 筆  當前頁: {items['current_page']} 頁")

            for news in items["data"]:
                # 解析新聞時間 → 強制使用台灣時區
                published = datetime.fromtimestamp(news["publishAt"], tz=self.tz)
                content = self._clean_html_content(news["content"])

                self.data.append({
                    "time": published,
                    "title": news["title"],
                    "content": content
                })

    def save(self):
        """儲存爬取結果到 CSV (並過濾時間區間)"""
        if not self.data:
            logger.warning("⚠️ No data to save.")
            return

        df = pd.DataFrame(self.data)

        # 過濾時間：只保留 [start_time_dt, end_time_dt]
        mask = (df["time"] >= self.start_time_dt) & (df["time"] <= self.end_time_dt)
        filtered_df = df.loc[mask].copy()

        # 格式化時間欄位
        filtered_df["time"] = filtered_df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")

        output_dir = os.path.dirname(self.output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        filtered_df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
        logger.info(f"資料儲存至 {self.output_path}")

    def run(self):
        """完整流程：爬蟲 → 存檔"""
        self.crawl()
        self.save()


# --- 測試用 ---
if __name__ == "__main__":
    now = datetime.now()
    yesterday_14 = (now - timedelta(days=2)).replace(hour=14, minute=0, second=0, microsecond=0)

    crawler = CNYESCrawler(
        start_time=yesterday_14,
        end_time=now,
        output_path="./news/cnyes_news.csv"
    )
    crawler.run()