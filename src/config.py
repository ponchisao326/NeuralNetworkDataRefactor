import os
from dotenv import load_dotenv

# Load environment variables once at module level
load_dotenv()

class Config:
    """Global configuration accessibility."""
    API_URL = os.getenv("API_URL", "")
    API_KEY = os.getenv("API_KEY", "")
    RAW_DIR = os.getenv("RAW_DATA_DIR", "data/raw")
    CLEAN_DIR = os.getenv("CLEAN_DATA_DIR", "data/clean")
    REPORT_DIR = os.getenv("REPORT_DIR", "data/reports")

    @classmethod
    def ensure_dirs(cls):
        """Ensures all necessary data directories exist."""
        os.makedirs(cls.RAW_DIR, exist_ok=True)
        os.makedirs(cls.CLEAN_DIR, exist_ok=True)
        os.makedirs(cls.REPORT_DIR, exist_ok=True)