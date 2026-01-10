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

    base_url: str = "https://f319.com"
    find_new_posts_url: str = "https://f319.com/find-new/posts"

    page_load_timeout: int = 60
    implicit_wait: int = 10
    delay_between_requests: int = 2
    min_random_delay: int = 3
    max_random_delay: int = 15

    max_retries: int = 3
    retry_delay: int = 3
    max_thread_workers: int = 2  # Số threads (khác nhau) crawl song song

    early_stop_threshold: int = 20  # Số posts cũ liên tiếp để dừng crawl
    batch_size: int = 100  # Số posts insert cùng lúc

    headless: bool = False
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )


SKIP_LINKS: List[str] = [
    "https://f319.com/threads/quy-dinh-dien-dan-f319-com-topic-giai-quyet-thac-mac-va-yeu-cau.1804467/",
    "https://f319.com/threads/admin-gioi-thieu-phan-mem-tu-van-dau-tu-an-tuong-nhat-tu-truoc-toi-nay.1857301/",
    "https://f319.com/threads/phan-tich-ky-thuat-ta-chi-chia-se-kien-thuc-ko-spam.13691/",
    "https://f319.com/threads/canh-bao-bao-mat-nghiem-trong-voi-dien-thoai-chay-he-dieu-hanh-android.1859185/"
]


class F319Selectors:
    DISCUSSION_LIST_ITEMS = "discussionListItems"
    DISCUSSION_LIST_ITEM = "discussionListItem"
    TITLE = "title"
    DATETIME = "DateTime"
    LAST_POST_INFO = "lastPostInfo"
    PREVIEW_TOOLTIP = "PreviewTooltip"
    MINOR = "minor"
    MAJOR = "major"

    MAIN_CONTENT = "mainContent"
    MESSAGE_LIST = "messageList"
    MESSAGE = "message"
    MESSAGE_CONTENT = "messageContent"
    USERNAME = "username"
    DATE_PERMALINK = "datePermalink"
    QUOTE = "quote"
    ATTRIBUTION_TYPE = "attribution type"
    PAGE_NAV_GROUP = "pageNavLinkGroup"
    PAGE_NAV_HEADER = "pageNavHeader"

    NEW_POSTS_LIST = "discussionListItems"
