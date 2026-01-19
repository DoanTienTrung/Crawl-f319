import logging
import signal
import sys
import time
import os
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config import DatabaseConfig, CrawlerConfig, SchedulerConfig
from database import Database
from f319_full_crawler import F319FullCrawler
from f319_hybrid_crawler import F319HybridCrawler

# Tạo thư mục logs nếu chưa có
if not os.path.exists('logs'):
    os.makedirs('logs')

log_filename = f"logs/scheduler_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"

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


class CrawlerScheduler:
    def __init__(self):
        self.db_config = DatabaseConfig()
        self.crawler_config = CrawlerConfig()
        self.scheduler_config = SchedulerConfig()
        
        self.db = Database(self.db_config)
        self.db.connect()
        self.scheduler = BackgroundScheduler(timezone=self.scheduler_config.timezone)
        
        # Graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)

    def run_full_crawler(self):
        """Wrapper function cho full crawler"""
        try:
            logger.info("🚀 Starting Full Crawler job...")
            start_time = time.time()
            
            crawler = F319FullCrawler(self.db, self.crawler_config)
            total_posts, thread_stats = crawler.crawl_all_today_threads()
            
            elapsed = time.time() - start_time
            logger.info(f"✅ Full Crawler completed: {total_posts} posts in {elapsed:.1f}s")
            
            # Log summary
            new_threads = sum(1 for stat in thread_stats if stat.get('is_new', False))
            logger.info(f"📊 Summary: {len(thread_stats)} threads ({new_threads} new)")
            
        except Exception as e:
            logger.error(f"❌ Full Crawler failed: {e}")

    def run_hybrid_crawler(self):
        """Wrapper function cho hybrid crawler"""
        try:
            logger.info("🚀 Starting Hybrid Crawler job...")
            start_time = time.time()
            
            crawler = F319HybridCrawler(self.db, self.crawler_config)
            total_posts, thread_stats = crawler.collect_today_threads(max_pages=10)
            
            elapsed = time.time() - start_time
            logger.info(f"✅ Hybrid Crawler completed: {total_posts} posts in {elapsed:.1f}s")
            
            # Log summary
            new_threads = sum(1 for stat in thread_stats if stat.get('is_new', False))
            logger.info(f"📊 Summary: {len(thread_stats)} threads ({new_threads} new)")
            
        except Exception as e:
            logger.error(f"❌ Hybrid Crawler failed: {e}")

    def run_sequential_crawlers(self):
        """Chạy hybrid trước, sau đó full crawler tuần tự"""
        try:
            logger.info("🚀 Starting Sequential Crawlers (Hybrid → Full)...")
            start_time = time.time()
            
            # 1. Chạy Hybrid Crawler trước
            logger.info("📱 Phase 1: Running Hybrid Crawler...")
            hybrid_start = time.time()
            self.run_hybrid_crawler()
            hybrid_elapsed = time.time() - hybrid_start
            logger.info(f"✅ Hybrid completed in {hybrid_elapsed:.1f}s")
            
            # 2. Nghỉ giữa 2 crawlers
            delay = self.scheduler_config.delay_between_crawlers
            logger.info(f"⏳ Waiting {delay}s before Full Crawler...")
            time.sleep(delay)
            
            # 3. Chạy Full Crawler sau
            logger.info("🔄 Phase 2: Running Full Crawler...")
            full_start = time.time()
            self.run_full_crawler()
            full_elapsed = time.time() - full_start
            logger.info(f"✅ Full completed in {full_elapsed:.1f}s")
            
            # 4. Tổng kết
            total_elapsed = time.time() - start_time
            logger.info(f"🎯 Sequential crawling completed in {total_elapsed:.1f}s")
            
        except Exception as e:
            logger.error(f"❌ Sequential crawlers failed: {e}")

    def setup_jobs(self):
        """Setup scheduled jobs"""
        
        if self.scheduler_config.enable_sequential_crawling:
            # Sequential mode: Hybrid → Full mỗi X giờ
            self.scheduler.add_job(
                func=self.run_sequential_crawlers,
                trigger=IntervalTrigger(
                    hours=self.scheduler_config.sequential_interval_hours,
                    start_date=datetime.now(),
                    timezone=self.scheduler_config.timezone
                ),
                id='sequential_crawlers_job',
                name='Sequential Crawlers (Hybrid → Full)',
                replace_existing=True
            )
            logger.info(f"📅 Sequential crawlers scheduled every {self.scheduler_config.sequential_interval_hours} hours")
            
        else:
            # Individual mode: Separate schedules
            if self.scheduler_config.full_crawler_enabled:
                self.scheduler.add_job(
                    func=self.run_full_crawler,
                    trigger=CronTrigger(
                        hour=self.scheduler_config.full_crawler_hour,
                        minute=self.scheduler_config.full_crawler_minute,
                        timezone=self.scheduler_config.timezone
                    ),
                    id='full_crawler_job',
                    name='Full Crawler Daily Job',
                    replace_existing=True
                )
                logger.info(f"📅 Full Crawler scheduled daily at {self.scheduler_config.full_crawler_hour:02d}:{self.scheduler_config.full_crawler_minute:02d}")

            if self.scheduler_config.hybrid_crawler_enabled:
                self.scheduler.add_job(
                    func=self.run_hybrid_crawler,
                    trigger=IntervalTrigger(
                        hours=self.scheduler_config.hybrid_crawler_interval_hours,
                        start_date=datetime.now(),
                        timezone=self.scheduler_config.timezone
                    ),
                    id='hybrid_crawler_job',
                    name='Hybrid Crawler Interval Job',
                    replace_existing=True
                )
                logger.info(f"⏰ Hybrid Crawler scheduled every {self.scheduler_config.hybrid_crawler_interval_hours} hours")

    def start(self):
        """Start the scheduler"""
        try:
            self.setup_jobs()
            self.scheduler.start()
            logger.info("🎯 Scheduler started successfully")
            
            # Print next run times
            for job in self.scheduler.get_jobs():
                next_run = job.next_run_time
                logger.info(f"📋 {job.name}: next run at {next_run}")
            
            # Run initial crawlers
            if self.scheduler_config.enable_sequential_crawling:
                logger.info("🚀 Running initial sequential crawlers...")
                self.run_sequential_crawlers()
            elif self.scheduler_config.hybrid_crawler_enabled:
                logger.info("🚀 Running initial hybrid crawler...")
                self.run_hybrid_crawler()
                
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            raise

    def stop(self):
        """Stop the scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("🛑 Scheduler stopped")

    def run_manual_test(self):
        """Run manual test of both crawlers"""
        logger.info("🧪 Running manual test...")
        
        print("\n" + "="*50)
        print("Testing Full Crawler...")
        print("="*50)
        self.run_full_crawler()
        
        print("\n" + "="*50)
        print("Testing Hybrid Crawler...")
        print("="*50)
        self.run_hybrid_crawler()
        
        logger.info("✅ Manual test completed")


def main():
    """Main function"""
    scheduler_service = CrawlerScheduler()
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # Manual test mode
        scheduler_service.run_manual_test()
        return
    
    try:
        scheduler_service.start()
        
        logger.info("🔄 Scheduler is running. Press Ctrl+C to stop.")
        
        # Keep the main thread alive
        while True:
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Scheduler error: {e}")
    finally:
        scheduler_service.stop()


if __name__ == "__main__":
    main()
