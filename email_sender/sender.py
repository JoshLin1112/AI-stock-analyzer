import smtplib
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.header import Header
import os
from datetime import datetime
import logging

logger = logging.getLogger("StockNewsCrawler")
def send_email(
    attachments,
    subject,
    body,
    sender_email="josh4102@gmail.com",
    receiver_email=None,
    smtp_server="smtp.gmail.com",
    smtp_port=587,
    password=None,
):
    if receiver_email is None:
        raise ValueError("receiver_email 不能為空")

    if isinstance(receiver_email, str):
        receiver_email = [receiver_email]

    # --- 1. 準備資料 ---
    df = pd.read_csv(attachments[0], encoding='utf-8')
    if "whole_news_content" in df.columns:
        df = df.drop(columns="whole_news_content")

    df = df.round({"avg_weighted_score": 2})
    df = df.rename(columns={
        "company": "公司",
        "total_articles": "文章數",
        "avg_weighted_score": "平均情緒分數",
        "company_summary": "公司盤前摘要",
    })

    # --- 2. 建立美化的 HTML 表格 ---
    # 為分數添加顏色邏輯
    def color_score(val):
        color = '#27ae60' if val > 0 else '#e74c3c' if val < 0 else '#7f8c8d'
        return f'style="color: {color}; font-weight: bold;"'

    # 手動構建 HTML 表格以確保高度自定義 (避開 Pandas 預設簡陋標籤)
    table_rows = ""
    for _, row in df.iterrows():
        score_style = color_score(row['平均情緒分數'])
        table_rows += f"""
        <tr style="border-bottom: 1px solid #edf2f7;">
            <td style="padding: 12px 15px; font-weight: 600; color: #2d3748;">{row['公司']}</td>
            <td style="padding: 12px 15px; text-align: center;">{row['文章數']}</td>
            <td style="padding: 12px 15px; text-align: center;" {score_style}>{row['平均情緒分數']}</td>
            <td style="padding: 12px 15px; color: #4a5568; font-size: 13px; line-height: 1.5;">{row['公司盤前摘要']}</td>
        </tr>
        """

    date_str = datetime.now().strftime("%Y-%m-%d")

    # --- 3. 完整現代風格 Email 樣版 ---
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
    </head>
    <body style="margin: 0; padding: 0; background-color: #f4f7f9; font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
        <table align="center" border="0" cellpadding="0" cellspacing="0" width="100%" style="max-width: 800px; margin: 20px auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.1);">
            <tr>
                <td style="background: linear-gradient(135deg, #1a73e8 0%, #1557b0 100%); padding: 30px; text-align: center;">
                    <h1 style="color: #ffffff; margin: 0; font-size: 24px; letter-spacing: 1px;">{subject}</h1>
                    <p style="color: #e0eaff; margin: 10px 0 0 0; font-size: 14px;">報告生成時間: {date_str}</p>
                </td>
            </tr>
            
            <tr>
                <td style="padding: 30px;">
                    <p style="color: #4a5568; font-size: 16px; margin-bottom: 24px; line-height: 1.6;">
                        {body}
                    </p>
                    
                    <div style="overflow-x: auto;">
                        <table width="100%" style="border-collapse: collapse; min-width: 600px;">
                            <thead>
                                <tr style="background-color: #f8fafc; border-bottom: 2px solid #e2e8f0;">
                                    <th style="padding: 15px; text-align: left; color: #718096; font-size: 13px; text-transform: uppercase;">公司</th>
                                    <th style="padding: 15px; text-align: center; color: #718096; font-size: 13px; text-transform: uppercase;">文章數</th>
                                    <th style="padding: 15px; text-align: center; color: #718096; font-size: 13px; text-transform: uppercase;">情緒分數</th>
                                    <th style="padding: 15px; text-align: left; color: #718096; font-size: 13px; text-transform: uppercase;">摘要</th>
                                </tr>
                            </thead>
                            <tbody>
                                {table_rows}
                            </tbody>
                        </table>
                    </div>
                </td>
            </tr>
            
            <tr>
                <td style="padding: 20px; text-align: center; background-color: #f8fafc; color: #a0aec0; font-size: 12px;">
                    此郵件為系統自動發送，請勿直接回覆。<br>
                    © {datetime.now().year} AI 智能情緒分析系統. 保留所有權利。
                </td>
            </tr>
        </table>
    </body>
    </html>
    """

    # --- 4. 發送設定 (保持原有機制) ---
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(receiver_email)
    msg["Subject"] = Header(subject, "utf-8").encode()
    msg.attach(MIMEText(html_content, "html", "utf-8"))

    for file in attachments:
        if os.path.exists(file):
            with open(file, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f'attachment; filename="sentiment_report_{date_str}.csv"')
                msg.attach(part)

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, password)
            server.send_message(msg)
        logger.info(f"成功發送報告至 {', '.join(receiver_email)} !")
        return True
    except Exception as e:
        print(f"⚠️ 發送失敗: {e}")
        return False


if __name__ == "__main__":
    # Test execution only if configured via environment variables
    import os
    from dotenv import load_dotenv
    load_dotenv()

    sender = os.getenv("EMAIL_SENDER")
    pwd = os.getenv("EMAIL_PASSWORD")
    receiver = os.getenv("EMAIL_RECEIVERS").split(',')
    print(f"Sender: {sender}, Receiver: {receiver}", f'pwd:{pwd}')

    if sender and pwd and receiver:
        send_email(
            attachments=["company_sentiment_stats.csv"],
            subject="每日財經新聞情緒統計",
            body="附件與下表為今日新聞情緒統計。",
            sender_email=sender,
            receiver_email=receiver, # Test with first receiver
            password=pwd
        )
    else:
        print("未設定環境變數，跳過測試發信。")
        
