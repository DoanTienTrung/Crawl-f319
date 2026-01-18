
import time
import random
import re
import logging
import requests
from typing import Optional, List
from datetime import datetime
from dataclasses import dataclass
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException, InvalidSessionIdException
from webdriver_manager.chrome import ChromeDriverManager

from config import CrawlerConfig
from database import Database

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ThreadData:
    id: str
    title: str
    author: str
    start_date: str
    last_post_date: str
    link: str
    views: str
    replies: str


@dataclass
class PostData:
    id: str
    thread_id: str
    author: str
    author_link: str
    post_date: int
    content: str


class F319HybridCrawler:
    """
    Hybrid crawler: Selenium cho pagination + Requests cho thread content
    Tối ưu tốc độ: Selenium chỉ dùng để navigate, Requests crawl nội dung
    """

    def __init__(self, db: Database, config: Optional[CrawlerConfig] = None):
        self.db = db
        self.config = config or CrawlerConfig()
        self.driver: Optional[webdriver.Chrome] = None
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config.user_agent
        })

    def _setup_driver(self) -> webdriver.Chrome:
        chrome_options = Options()

        if self.config.headless:
            chrome_options.add_argument("--headless")

        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument(f"user-agent={self.config.user_agent}")

        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            logger.warning(f"webdriver-manager failed: {e}")
            service = Service(self.config.chromedriver_path)
            driver = webdriver.Chrome(service=service, options=chrome_options)

        driver.set_page_load_timeout(self.config.page_load_timeout)
        driver.implicitly_wait(self.config.implicit_wait)

        return driver

    def start(self):
        if not self.driver:
            self.driver = self._setup_driver()
            logger.info("Selenium driver started")

    def stop(self):
        if self.driver:
            self.driver.quit()
            self.driver = None
            logger.info("Selenium driver stopped")

    def _random_delay(self):
        delay = random.uniform(
            self.config.min_random_delay,
            self.config.max_random_delay
        )
        time.sleep(delay)

    def _normalize_url(self, href: str) -> str:
        if not href or href == "NA":
            return "NA"
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            return f"https://f319.com{href}"
        return f"https://f319.com/{href}"

    def _extract_thread_id(self, url: str) -> str:
        match = re.search(r'\.(\d+)/?', url)
        return match.group(1) if match else "unknown_thread_id"

    def _parse_date(self, date_str: str) -> int:
        try:
            dt = datetime.strptime(date_str.strip(), "%d/%m/%Y, %H:%M")
            return int(dt.timestamp())
        except ValueError:
            return int(datetime.now().timestamp())

    def _extract_thread_data_selenium(self, item) -> Optional[ThreadData]:
        try:
            thread_id = item.get_attribute('id') or 'Unknown_ID'

            title_elem = item.find_element(By.CLASS_NAME, 'title')
            title = title_elem.text if title_elem else "NA"

            author = item.get_attribute('data-author') or 'Unknown'

            datetime_elem = item.find_element(By.CLASS_NAME, 'DateTime')
            start_date = datetime_elem.text if datetime_elem else "NA"

            try:
                last_post_info = item.find_element(By.CLASS_NAME, 'lastPostInfo')
                last_date_elem = last_post_info.find_element(By.CLASS_NAME, 'DateTime')
                last_post_date = last_date_elem.text
            except:
                last_post_date = "NA"

            link_elem = item.find_element(By.CLASS_NAME, 'PreviewTooltip')
            href = link_elem.get_attribute('href')
            link = self._normalize_url(href)

            try:
                views_elem = item.find_element(By.CLASS_NAME, 'minor')
                views = views_elem.text
            except:
                views = "NA"

            try:
                replies_elem = item.find_element(By.CLASS_NAME, 'major')
                replies = replies_elem.text
            except:
                replies = "NA"

            return ThreadData(
                id=thread_id,
                title=title,
                author=author,
                start_date=start_date,
                last_post_date=last_post_date,
                link=link,
                views=views,
                replies=replies
            )

        except Exception as e:
            logger.error(f"Error extracting thread data: {e}")
            return None

    def _fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        for attempt in range(self.config.max_retries):
            try:
                response = self.session.get(url, timeout=self.config.page_load_timeout)
                response.raise_for_status()
                return BeautifulSoup(response.content, 'lxml')
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{self.config.max_retries} failed for {url}: {e}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(self.config.retry_delay)

        logger.error(f"Failed to fetch {url} after {self.config.max_retries} attempts")
        return None

    def _get_total_pages(self, soup: BeautifulSoup) -> int:
        try:
            page_nav = soup.select_one('.pageNavLinkGroup')
            if page_nav:
                page_header = page_nav.select_one('.pageNavHeader')
                if page_header:
                    text = page_header.get_text(strip=True)
                    match = re.search(r'/\s*(\d+)', text)
                    if match:
                        return int(match.group(1))
            return 1
        except Exception:
            return 1

    def _extract_post_data(self, post_elem, thread_id: str) -> Optional[PostData]:
        try:
            post_id = post_elem.get('id')
            if not post_id:
                return None

            author = post_elem.get('data-author', 'Unknown')

            username_elem = post_elem.select_one('.username')
            author_link = self._normalize_url(
                username_elem.get('href', '') if username_elem else "NA"
            )

            content_elem = post_elem.select_one('.messageContent')
            if not content_elem:
                return None

            for quote in content_elem.select('.bbCodeBlock-expandContent'):
                quote.decompose()
            for attr in content_elem.select('.attribution.type'):
                attr.decompose()

            content = content_elem.get_text(strip=True)

            date_elem = post_elem.select_one('.datePermalink')
            date_str = date_elem.get_text(strip=True) if date_elem else ""
            post_date = self._parse_date(date_str)

            return PostData(
                id=post_id,
                thread_id=thread_id,
                author=author,
                author_link=author_link,
                post_date=post_date,
                content=content
            )

        except Exception as e:
            logger.error(f"Error extracting post data: {e}")
            return None

    def _parse_thread_date(self, date_str: str) -> Optional[datetime]:
        """Parse ngày tạo thread từ string"""
        try:
            formats = [
                "%d/%m/%Y, %H:%M",  # 15/01/2025, 14:30
                "%d/%m/%Y",         # 15/01/2025
                "%d-%m-%Y, %H:%M",  # 15-01-2025, 14:30
                "%d-%m-%Y",         # 15-01-2025
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str.strip(), fmt)
                except ValueError:
                    continue
                    
            return None
        
        except Exception:
            return None


    def _is_thread_after_start_date(self, thread_start_date: str) -> bool:
        """Kiểm tra thread có được tạo sau ngày bắt đầu không"""
        if not self.config.enable_thread_date_filter:
            return True
            
        try:
            thread_dt = self._parse_thread_date(thread_start_date)
            if not thread_dt:
                return True  # Accept nếu không parse được
                
            start_dt = datetime.strptime(self.config.thread_start_date, "%d/%m/%Y")
            return thread_dt >= start_dt
            
        except Exception:
            return True  # Accept nếu có lỗi



    def _collect_posts_from_page(self, soup: BeautifulSoup, thread_id: str, last_post_id: Optional[str] = None) -> tuple:
        posts_list = []
        old_posts_count = 0
        found_last_post = False

        try:
            main_content = soup.select_one('.mainContent')
            if not main_content:
                return (posts_list, old_posts_count, found_last_post)

            message_list = main_content.select_one('.messageList')
            if not message_list:
                return (posts_list, old_posts_count, found_last_post)

            posts = message_list.select('.message')
            posts = list(reversed(posts))

            for post in posts:
                try:
                    post_data = self._extract_post_data(post, thread_id)

                    if post_data and post_data.content.strip():
                        if last_post_id and post_data.id == last_post_id:
                            found_last_post = True
                            logger.debug(f"Found last_post_id {last_post_id}, stopping")
                            break

                        if self.db.post_exists(post_data.id):
                            old_posts_count += 1
                            logger.debug(f"Post {post_data.id} exists, old count: {old_posts_count}")
                            continue
                        else:
                            old_posts_count = 0

                        posts_list.append({
                            "id": post_data.id,
                            "thread_id": post_data.thread_id,
                            "author": post_data.author,
                            "author_link": post_data.author_link,
                            "post_date": post_data.post_date,
                            "content": post_data.content
                        })

                except Exception as e:
                    logger.error(f"Error processing post: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error collecting posts: {e}")

        return (posts_list, old_posts_count, found_last_post)

    def collect_thread_posts(self, url: str, is_new_thread: bool = None) -> int:
        start_time = time.time()
        total_collected = 0
        thread_id = self._extract_thread_id(url)

        if is_new_thread is None:
            thread_exists = self.db.thread_exists_by_link(url)
        else:
            thread_exists = not is_new_thread

        soup = self._fetch_page(url)
        if not soup:
            return 0

        time.sleep(self.config.delay_between_requests)

        total_pages = self._get_total_pages(soup)
        posts_buffer = []
        newest_post_id = None
        last_crawled_post_id = None

        if not thread_exists:
            logger.info(f"[Thread {thread_id}] New thread, crawling forward 1→{total_pages}...")

            for page in range(1, total_pages + 1):
                page_url = f"{url}page-{page}" if page > 1 else url

                soup = self._fetch_page(page_url)
                if not soup:
                    continue

                time.sleep(self.config.delay_between_requests)

                posts_list, _, _ = self._collect_posts_from_page(soup, thread_id, None)

                if posts_list:
                    last_crawled_post_id = posts_list[0]['id']

                posts_buffer.extend(posts_list)

                if len(posts_buffer) >= self.config.batch_size:
                    inserted = self.db.batch_insert_posts(posts_buffer[:self.config.batch_size])
                    total_collected += inserted
                    logger.info(f"[Thread {thread_id}] Batch inserted {inserted}/{self.config.batch_size} posts at page {page}")
                    posts_buffer = posts_buffer[self.config.batch_size:]

                    if last_crawled_post_id:
                        self.db.update_last_post_id(thread_id, last_crawled_post_id)

        else:
            last_post_id = self.db.get_last_post_id(thread_id)
            logger.info(f"[Thread {thread_id}] Existing thread, crawling reverse {total_pages}→1 until last_post_id: {last_post_id}")

            should_stop = False
            for page in range(total_pages, 0, -1):
                if should_stop:
                    break

                page_url = f"{url}page-{page}" if page > 1 else url

                soup = self._fetch_page(page_url)
                if not soup:
                    continue

                time.sleep(self.config.delay_between_requests)

                posts_list, _, found_last = self._collect_posts_from_page(soup, thread_id, last_post_id)

                if not newest_post_id and posts_list:
                    newest_post_id = posts_list[0]['id']

                posts_buffer.extend(posts_list)

                if found_last:
                    logger.info(f"[Thread {thread_id}] Found last_post_id at page {page}, stopping...")
                    should_stop = True
                    break

                if len(posts_buffer) >= self.config.batch_size:
                    inserted = self.db.batch_insert_posts(posts_buffer[:self.config.batch_size])
                    total_collected += inserted
                    logger.info(f"[Thread {thread_id}] Batch inserted {inserted}/{self.config.batch_size} posts at page {page}")
                    posts_buffer = posts_buffer[self.config.batch_size:]

                    if newest_post_id:
                        self.db.update_last_post_id(thread_id, newest_post_id)

        if posts_buffer:
            inserted = self.db.batch_insert_posts(posts_buffer)
            total_collected += inserted
            logger.info(f"[Thread {thread_id}] Final batch inserted {inserted}/{len(posts_buffer)} posts")

            final_post_id = newest_post_id if thread_exists else last_crawled_post_id
            if final_post_id:
                self.db.update_last_post_id(thread_id, final_post_id)

        elapsed = time.time() - start_time
        logger.info(f"[Thread {thread_id}] Collected {total_collected} posts in {elapsed:.1f}s")
        return total_collected

    def _crawl_single_thread(self, thread_info: dict, index: int, total: int) -> dict:
        try:
            thread_data = thread_info["data"]
            link = thread_info["link"]

            is_new_thread = not self.db.thread_exists_by_link(link)
            normalized_thread_id = thread_data.id.replace("thread-", "")

            self.db.insert_f319_list({
                "id": normalized_thread_id,
                "title": thread_data.title,
                "author": thread_data.author,
                "start_date": thread_data.start_date,
                "last_post_date": thread_data.last_post_date,
                "link": thread_data.link,
                "views": thread_data.views,
                "replies": thread_data.replies
            })

            logger.info(f"[{index}/{total}] Crawl thread: {thread_data.title[:50]}...")

            # Lấy last_post_id cũ trước khi crawl
            old_last_post_id = self.db.get_last_post_id(normalized_thread_id) if not is_new_thread else None

            thread_start = time.time()
            posts_count = self.collect_thread_posts(link, is_new_thread=is_new_thread)
            thread_elapsed = time.time() - thread_start

            logger.info(f"Thu thập được {posts_count} posts từ thread")

            # Lấy last_post_id mới sau khi crawl
            new_last_post_id = self.db.get_last_post_id(normalized_thread_id)

            return {
                "title": thread_data.title,
                "posts_count": posts_count,
                "elapsed_time": thread_elapsed,
                "is_new": is_new_thread,
                "old_last_post_id": old_last_post_id,
                "new_last_post_id": new_last_post_id or "N/A"
            }

        except Exception as e:
            logger.error(f"Lỗi crawl thread: {e}")
            return {
                "title": thread_info.get("data", {}).title if hasattr(thread_info.get("data", {}), 'title') else "Unknown",
                "posts_count": 0,
                "elapsed_time": 0,
                "is_new": False,
                "old_last_post_id": None,
                "new_last_post_id": "N/A"
            }

    def collect_today_threads(self, max_pages: int = 10) -> tuple:
        self.start()
        total_collected = 0
        thread_stats = []

        try:
            # Log filter info
            if self.config.enable_thread_date_filter:
                logger.info(f"📅 Thread filter: Only threads from {self.config.thread_start_date} onwards")
            else:
                logger.info("📅 Thread filter disabled: Processing all threads")

            logger.info("Đang load trang (New posts)...")

            possible_urls = [
                "https://f319.com/find-new/posts",
                "https://f319.com/whats-new/posts",
            ]

            base_url = None
            for url in possible_urls:
                try:
                    self.driver.get(url)
                    time.sleep(3)

                    try:
                        page_nav = self.driver.find_element(By.CLASS_NAME, "PageNav")
                        logger.info(f"✓ URL có pagination: {url}")
                        base_url = url
                        break
                    except NoSuchElementException:
                        logger.info(f"✗ URL không có pagination: {url}")
                        continue
                except Exception as e:
                    logger.warning(f"Lỗi khi load {url}: {e}")
                    continue

            if not base_url:
                logger.error("Không tìm thấy trang có pagination!")
                return (0, [])

            logger.info(f"Sử dụng URL: {base_url}")

            for page in range(1, max_pages + 1):
                logger.info(f"Đang xử lý trang {page}")

                if page > 1:
                    try:
                        next_btn = self.driver.find_element(By.XPATH, "//a[@class='text' and contains(text(), 'Tiếp')]")
                        next_url = next_btn.get_attribute('href')

                        if next_url:
                            self.driver.get(next_url)
                            time.sleep(2)
                        else:
                            logger.warning(f"Không lấy được URL trang {page}")
                            break
                    except InvalidSessionIdException:
                        logger.warning(f"Driver crashed at page {page}, restarting...")
                        self.stop()
                        self.start()
                        try:
                            self.driver.get(base_url)
                            time.sleep(3)
                            for p in range(2, page + 1):
                                next_btn = self.driver.find_element(By.XPATH, "//a[@class='text' and contains(text(), 'Tiếp')]")
                                next_url = next_btn.get_attribute('href')
                                if next_url:
                                    self.driver.get(next_url)
                                    time.sleep(2)
                            logger.info(f"Driver restarted, resumed at page {page}")
                        except Exception as retry_error:
                            logger.error(f"Failed to restart driver: {retry_error}")
                            break
                    except NoSuchElementException:
                        logger.warning(f"Không tìm thấy nút next tại trang {page}")
                        break
                    except TimeoutException:
                        logger.error(f"Timeout khi navigate trang {page}")
                        break

                time.sleep(self.config.delay_between_requests)

                try:
                    posts_list = self.driver.find_elements(By.CSS_SELECTOR, '.discussionListItem')
                except NoSuchElementException:
                    logger.warning(f"Không tìm thấy threads trên trang {page}")
                    continue

                if not posts_list:
                    logger.info(f"Không còn threads trên trang {page}")
                    break

                logger.info(f"Tìm thấy {len(posts_list)} threads trên trang {page}")

                threads_to_crawl = []
                filtered_count = 0  # Đếm threads bị lọc
                
                for item in posts_list:
                    try:
                        thread_data = self._extract_thread_data_selenium(item)

                        if thread_data and thread_data.link != "NA":
                            # Kiểm tra ngày tạo thread
                            if not self._is_thread_after_start_date(thread_data.start_date):
                                filtered_count += 1
                                logger.info(f"🚫 FILTERED: '{thread_data.title[:50]}' (Created: {thread_data.start_date})")
                                continue
                                
                            threads_to_crawl.append({
                                "data": thread_data,
                                "link": thread_data.link
                            })
                            logger.info(f"✅ ACCEPTED: '{thread_data.title[:50]}' (Created: {thread_data.start_date})")

                    except Exception as e:
                        logger.error(f"Lỗi extract thread data: {e}")
                        continue

                # Log kết quả filter
                logger.info(f"📊 Page {page}: {len(threads_to_crawl)} threads accepted, {filtered_count} filtered out")
                
                if filtered_count > 0:
                    logger.info(f"🗓️  Filtered {filtered_count} threads older than {self.config.thread_start_date}")
                if len(threads_to_crawl) > 0:
                    logger.info(f"🎯 Processing {len(threads_to_crawl)} threads from {self.config.thread_start_date} onwards")
                
                if not threads_to_crawl:
                    logger.info(f"No threads to crawl on page {page} after date filtering")
                    continue

                logger.info(f"Đã thu thập thông tin {len(threads_to_crawl)} threads, bắt đầu crawl posts...")

                total_threads = len(threads_to_crawl)

                with ThreadPoolExecutor(max_workers=self.config.max_thread_workers) as executor:
                    future_to_thread = {
                        executor.submit(self._crawl_single_thread, thread_info, idx, total_threads): idx
                        for idx, thread_info in enumerate(threads_to_crawl, 1)
                    }

                    for future in as_completed(future_to_thread):
                        thread_idx = future_to_thread[future]
                        try:
                            thread_result = future.result()
                            total_collected += thread_result["posts_count"]
                            thread_stats.append(thread_result)
                        except Exception as e:
                            logger.error(f"Thread {thread_idx} crawl failed: {e}")

                logger.info(f"Hoàn thành trang {page}, tổng posts: {total_collected}")

                self._random_delay()

            # Tổng kết cuối crawler
            total_threads_processed = len(thread_stats)
            
            logger.info("="*60)
            logger.info("📋 THREAD FILTERING SUMMARY")
            logger.info("="*60)
            logger.info(f"📅 Filter date: {self.config.thread_start_date}")
            logger.info(f"✅ Threads processed: {total_threads_processed}")
            logger.info(f"📝 Total posts collected: {total_collected}")
            logger.info("="*60)

        finally:
            self.stop()

        logger.info(f"Hoàn tất! Tổng cộng thu thập: {total_collected} posts")
        return (total_collected, thread_stats)
