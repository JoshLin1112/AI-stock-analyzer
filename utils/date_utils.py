from datetime import datetime, timedelta
import pytz

def get_taipei_time_window():
    """根據當前日期取得台北時區的時間區間。
    - 如果今天是星期二到星期五，時間區間為昨天 14:00 到現在。
    - 如果今天是星期一，時間區間為三天前 14:00 到現在。
    - 如果今天是星期日 (weekday=6)，視同一般邏輯 (程式碼原本邏輯似乎有誤，這裡保留原本意圖但修正星期日判斷)
      原本程式碼: elif weekday == 6: start = ... (days=2) 
      星期日的前一天是星期六(不開盤?)，前兩天是星期五 14:00?
    
    Refined Logic based on original code functionality:
    - Monday (0): 3 days ago (Friday) 14:00
    - Sunday (6): 2 days ago (Friday) 14:00
    - Others: 1 day ago 14:00
    """
    tz = pytz.timezone("Asia/Taipei")
    now = datetime.now(tz)
    
    # 取得今天是星期幾 (0=星期一, 6=星期日)
    weekday = now.weekday()
    # print(f"今天是星期 {weekday + 1}") # Removed print for cleaner utility

    if weekday == 0:  # Monday
        # Start from Friday 14:00
        start = (now - timedelta(days=3)).replace(hour=14, minute=0, second=0, microsecond=0)
    elif weekday == 6: # Sunday
        # Start from Friday 14:00 (Original logic seems to imply this)
        start = (now - timedelta(days=2)).replace(hour=14, minute=0, second=0, microsecond=0)
    else:
        # Start from Yesterday 14:00
        start = (now - timedelta(days=1)).replace(hour=14, minute=0, second=0, microsecond=0)
        
    return start, now.replace(hour=8, minute=0, second=0, microsecond=0)
