import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    # Ollama Settings
    OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma3:4b")
    
    # Email Settings
    EMAIL_SENDER = os.getenv("EMAIL_SENDER")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    # Receivers can be a comma-separated string in .env
    EMAIL_RECEIVERS_STR = os.getenv("EMAIL_RECEIVERS", "")
    EMAIL_RECEIVERS = [email.strip() for email in EMAIL_RECEIVERS_STR.split(",") if email.strip()]

    # Paths
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    NEWS_OUTPUT_DIR = os.path.join(BASE_DIR, "news")
    STATS_OUTPUT_PATH = os.path.join(BASE_DIR, "company_sentiment_stats.csv")

    # Validate critical config
    @classmethod
    def validate(cls):
        missing = []
        if not cls.EMAIL_SENDER: missing.append("EMAIL_SENDER")
        if not cls.EMAIL_PASSWORD: missing.append("EMAIL_PASSWORD")
        if not cls.EMAIL_RECEIVERS: missing.append("EMAIL_RECEIVERS")
        
        if missing:
            print(f"Warning: Missing environment variables: {', '.join(missing)}. Email functionality may not work.")
