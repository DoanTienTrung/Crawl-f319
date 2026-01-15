import os
from dataclasses import dataclass
from typing import List
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabaseConfig:
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", "5432"))
    database: str = os.getenv("DB_NAME", "f319_data")
    user: str = os.getenv("DB_USER", "postgres")
    password: str = os.getenv("DB_PASSWORD", "postgres")


@dataclass
class CrawlerConfig:
    chromedriver_path: str = os.getenv(
        "CHROMEDRIVER_PATH",
        r"D:\Chat\chromedriver-win64\chromedriver-win64\chromedriver.exe"
    )


    page_load_timeout: int = 60
    implicit_wait: int = 10
    delay_between_requests: int = 2
    min_random_delay: int = 3
    max_random_delay: int = 15

    max_retries: int = 3
    retry_delay: int = 3
    max_thread_workers: int = 2  # Số threads crawl song song

    
    batch_size: int = 100  # Số posts insert cùng lúc

    headless: bool = False
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )


@dataclass
class SchedulerConfig:
    # Full crawler - chạy hàng ngày lúc 2h sáng
    full_crawler_enabled: bool = True
    full_crawler_hour: int = 2
    full_crawler_minute: int = 0
    
    # Hybrid crawler - chạy mỗi 1 tiếng
    hybrid_crawler_enabled: bool = True
    hybrid_crawler_interval_hours: int = 1
    
    # Chung
    timezone: str = "Asia/Ho_Chi_Minh"


