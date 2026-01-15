import argparse
import sys
import logging
import time
import os
from datetime import datetime

from database import get_database, Database
from f319_hybrid_crawler import F319HybridCrawler
from f319_full_crawler import F319FullCrawler
from config import CrawlerConfig

# Tạo thư mục logs nếu chưa có
if not os.path.exists('logs'):
    os.makedirs('logs')

# Tạo tên file log với timestamp
log_filename = f"logs/crawl_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_filename, encoding='utf-8')
    ],
    force=True
)
logger = logging.getLogger(__name__)
logger.info(f"Log file created: {log_filename}")


def print_summary(thread_stats: list, total_posts: int, total_elapsed: float):
    """In summary thống kê chi tiết các threads đã crawl"""
    logger.info("\n" + "="*80)
    logger.info("CRAWL SUMMARY - TỔNG KẾT THU THẬP DỮ LIỆU")
    logger.info("="*80)

    # Phân loại threads
    new_threads = [s for s in thread_stats if s.get('is_new', False)]
    updated_threads = [s for s in thread_stats if not s.get('is_new', False)]

    # Hiển thị NEW THREADS
    if new_threads:
        logger.info(f"\n🆕 THREADS MỚI ({len(new_threads)} threads):")
        logger.info("-" * 80)

        for idx, stat in enumerate(new_threads, 1):
            title = stat['title'][:60] + "..." if len(stat['title']) > 60 else stat['title']
            posts = stat['posts_count']
            elapsed = stat['elapsed_time']
            new_last_post_id = stat.get('new_last_post_id', 'N/A')
            mins = int(elapsed // 60)
            secs = int(elapsed % 60)

            logger.info(f"{idx:3}. [NEW] {title}")
            logger.info(f"     Posts: {posts:4} | Thời gian: {mins}m {secs}s | Last Post ID: {new_last_post_id}")

    # Hiển thị UPDATED THREADS
    if updated_threads:
        logger.info(f"\n🔄 THREADS CẬP NHẬT ({len(updated_threads)} threads):")
        logger.info("-" * 80)

        for idx, stat in enumerate(updated_threads, 1):
            title = stat['title'][:60] + "..." if len(stat['title']) > 60 else stat['title']
            posts = stat['posts_count']
            elapsed = stat['elapsed_time']
            old_last_post_id = stat.get('old_last_post_id', 'N/A')
            new_last_post_id = stat.get('new_last_post_id', 'N/A')
            mins = int(elapsed // 60)
            secs = int(elapsed % 60)

            logger.info(f"{idx:3}. [UPDATE] {title}")
            logger.info(f"     Posts mới: {posts:4} | Thời gian: {mins}m {secs}s")
            logger.info(f"     Last Post ID: {old_last_post_id} → {new_last_post_id}")

    # Tổng kết
    logger.info("-" * 80)
    logger.info(f"📊 Tổng số threads: {len(thread_stats)} ({len(new_threads)} mới, {len(updated_threads)} cập nhật)")
    logger.info(f"📝 Tổng số posts mới: {total_posts}")

    total_mins = int(total_elapsed // 60)
    total_secs = int(total_elapsed % 60)
    logger.info(f"⏱️  Tổng thời gian crawl: {total_mins}m {total_secs}s")
    logger.info("="*80 + "\n")


def crawl_hybrid(crawler: F319HybridCrawler, max_pages: int):
    start_time = time.time()
    logger.info(f"Starting hybrid crawl for {max_pages} pages")
    total, thread_stats = crawler.collect_today_threads(max_pages)

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    logger.info(f"Completed in {minutes}m {seconds}s. Total posts: {total}")

    print_summary(thread_stats, total, elapsed)
    return total


def crawl_full(crawler: F319FullCrawler):
    start_time = time.time()
    logger.info("Starting full crawl")
    total, thread_stats = crawler.crawl_all_today_threads()

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    logger.info(f"Completed in {minutes}m {seconds}s. Total posts: {total}")

    print_summary(thread_stats, total, elapsed)
    return total


def show_stats(db: Database):
    stats = db.get_stats()
    print("\n=== Database Statistics ===")
    print(f"Total threads: {stats['total_threads']}")
    print(f"Total posts: {stats['total_posts']}")
    print("===========================\n")


def main():
    parser = argparse.ArgumentParser(
        description='F319.com Forum Crawler',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py hybrid --pages 5
  python main.py full
  python main.py hybrid --headless
  python main.py full --headless
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    hybrid_parser = subparsers.add_parser('hybrid', help='Crawl limited pages (fast)')
    hybrid_parser.add_argument('--pages', type=int, default=5, help='Max pages (default: 5)')
    hybrid_parser.add_argument('--headless', action='store_true', help='Headless mode')

    full_parser = subparsers.add_parser('full', help='Crawl all pages and posts')
    full_parser.add_argument('--headless', action='store_true', help='Headless mode')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    logger.info("Initializing database connection...")
    db = get_database()

    try:
        config = CrawlerConfig()
        if hasattr(args, 'headless') and args.headless:
            config.headless = True

        if args.command == 'hybrid':
            crawler = F319HybridCrawler(db, config)
            crawl_hybrid(crawler, args.pages)
        elif args.command == 'full':
            crawler = F319FullCrawler(db, config)
            crawl_full(crawler)

        show_stats(db)

    finally:
        db.disconnect()
        logger.info("Database connection closed")


if __name__ == "__main__":
    main()
