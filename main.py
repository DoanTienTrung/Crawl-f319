import argparse
import sys
import logging
import time
from datetime import datetime

from database import get_database, Database
from f319_hybrid_crawler import F319HybridCrawler
from f319_full_crawler import F319FullCrawler
from config import CrawlerConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def crawl_hybrid(crawler: F319HybridCrawler, max_pages: int):
    start_time = time.time()
    logger.info(f"Starting HYBRID  crawl for {max_pages} pages")
    total = crawler.collect_today_threads(max_pages)

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    logger.info(f"Hybrid crawl completed in {minutes}m {seconds}s. Total posts collected: {total}")
    return total


def crawl_full(crawler: F319FullCrawler):
    start_time = time.time()
    logger.info("Starting FULL crawl - Crawling ALL pages and ALL posts")
    total = crawler.crawl_all_today_threads()

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    logger.info(f"Full crawl completed in {minutes}m {seconds}s. Total posts collected: {total}")
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
  # Crawl "Hom nay co gi?" with Hybrid (limited pages)
  python main.py hybrid --pages 5

  # Crawl ALL pages and ALL posts (from homepage)
  python main.py full

  # Run in headless mode
  python main.py hybrid --headless
  python main.py full --headless
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    hybrid_parser = subparsers.add_parser('hybrid', help='Crawl "Hom nay co gi?" with Hybrid (limited pages)')
    hybrid_parser.add_argument('--pages', type=int, default=5, help='Max pages to crawl (default: 5)')
    hybrid_parser.add_argument('--headless', action='store_true', help='Run in headless mode')

    full_parser = subparsers.add_parser('full', help='Crawl ALL pages from homepage "Hom nay co gi?" - ALL threads + ALL posts')
    full_parser.add_argument('--headless', action='store_true', help='Run in headless mode')

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
