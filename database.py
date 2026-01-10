import psycopg2
from typing import Optional, Dict, Any
from contextlib import contextmanager
import logging

from config import DatabaseConfig

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig()
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password
            )
            self.conn.autocommit = True
            logger.info(f"Connected to PostgreSQL: {self.config.database}")
        except psycopg2.Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise

    def disconnect(self):
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from PostgreSQL")

    @contextmanager
    def get_cursor(self):
        cursor = self.conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def create_tables(self):
        create_f319_list_sql = """
        CREATE TABLE IF NOT EXISTS f319_list (
            id VARCHAR(255) PRIMARY KEY,
            title TEXT,
            author VARCHAR(255),
            start_date VARCHAR(100),
            last_post_date VARCHAR(100),
            link TEXT UNIQUE,
            views VARCHAR(50),
            replies VARCHAR(50),
            last_post_id VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        # Thêm column nếu chưa tồn tại (cho DB cũ)
        alter_f319_list_sql = """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='f319_list' AND column_name='last_post_id'
            ) THEN
                ALTER TABLE f319_list ADD COLUMN last_post_id VARCHAR(255);
            END IF;
        END $$;
        """

        create_f319_post_sql = """
        CREATE TABLE IF NOT EXISTS f319_post (
            id VARCHAR(255) PRIMARY KEY,
            thread_id VARCHAR(255),
            author VARCHAR(255),
            author_link TEXT,
            post_date BIGINT,
            content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        create_index_sql = """
        CREATE INDEX IF NOT EXISTS idx_f319_post_thread_id ON f319_post(thread_id);
        CREATE INDEX IF NOT EXISTS idx_f319_list_link ON f319_list(link);
        """

        with self.get_cursor() as cursor:
            cursor.execute(create_f319_list_sql)
            cursor.execute(alter_f319_list_sql)
            cursor.execute(create_f319_post_sql)
            cursor.execute(create_index_sql)
            logger.info("Tables created/verified successfully")

    def insert_f319_list(self, data: Dict[str, Any]) -> bool:
        insert_sql = """
        INSERT INTO f319_list (id, title, author, start_date, last_post_date, link, views, replies)
        VALUES (%(id)s, %(title)s, %(author)s, %(start_date)s, %(last_post_date)s, %(link)s, %(views)s, %(replies)s)
        ON CONFLICT (id) DO NOTHING
        """

        try:
            with self.get_cursor() as cursor:
                cursor.execute(insert_sql, data)
                return cursor.rowcount > 0
        except psycopg2.Error as e:
            logger.error(f"Error inserting f319_list: {e}")
            return False

    def insert_f319_post(self, data: Dict[str, Any]) -> bool:
        insert_sql = """
        INSERT INTO f319_post (id, thread_id, author, author_link, post_date, content)
        VALUES (%(id)s, %(thread_id)s, %(author)s, %(author_link)s, %(post_date)s, %(content)s)
        ON CONFLICT (id) DO NOTHING
        """

        try:
            with self.get_cursor() as cursor:
                cursor.execute(insert_sql, data)
                return cursor.rowcount > 0
        except psycopg2.Error as e:
            logger.error(f"Error inserting f319_post: {e}")
            return False

    def thread_has_posts(self, thread_id: str) -> bool:
        query = "SELECT 1 FROM f319_post WHERE thread_id = %s LIMIT 1"
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, (thread_id,))
                return cursor.fetchone() is not None
        except psycopg2.Error as e:
            logger.error(f"Error checking thread_has_posts: {e}")
            return False

    def post_exists(self, post_id: str) -> bool:
        query = "SELECT 1 FROM f319_post WHERE id = %s LIMIT 1"
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, (post_id,))
                return cursor.fetchone() is not None
        except psycopg2.Error as e:
            logger.error(f"Error checking post_exists: {e}")
            return False

    def get_last_post_id(self, thread_id: str) -> Optional[str]:
        """Lấy last_post_id đã lưu của thread"""
        query = "SELECT last_post_id FROM f319_list WHERE id = %s"
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, (thread_id,))
                result = cursor.fetchone()
                return result[0] if result else None
        except psycopg2.Error as e:
            logger.error(f"Error getting last_post_id: {e}")
            return None

    def update_last_post_id(self, thread_id: str, last_post_id: str) -> bool:
        """Update last_post_id sau khi crawl xong thread"""
        query = "UPDATE f319_list SET last_post_id = %s WHERE id = %s"
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, (last_post_id, thread_id))
                return cursor.rowcount > 0
        except psycopg2.Error as e:
            logger.error(f"Error updating last_post_id: {e}")
            return False

    def batch_insert_posts(self, posts_data: list) -> int:
        """Insert nhiều posts cùng lúc (nhanh hơn)"""
        if not posts_data:
            return 0

        insert_sql = """
        INSERT INTO f319_post (id, thread_id, author, author_link, post_date, content)
        VALUES %s
        ON CONFLICT (id) DO NOTHING
        """

        try:
            with self.get_cursor() as cursor:
                # Tạo values string cho executemany
                from psycopg2.extras import execute_values

                values = [
                    (p['id'], p['thread_id'], p['author'], p['author_link'], p['post_date'], p['content'])
                    for p in posts_data
                ]

                execute_values(cursor, insert_sql, values)
                return cursor.rowcount
        except psycopg2.Error as e:
            logger.error(f"Error batch inserting posts: {e}")
            return 0

    def get_stats(self) -> Dict[str, int]:
        with self.get_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM f319_list")
            list_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM f319_post")
            post_count = cursor.fetchone()[0]

            return {
                "total_threads": list_count,
                "total_posts": post_count
            }


def get_database() -> Database:
    db = Database()
    db.connect()
    db.create_tables()
    return db


if __name__ == "__main__":
    db = get_database()
    print(f"Stats: {db.get_stats()}")
    db.disconnect()
