"""
File: database/db_manager.py
Location: telegram_scheduler_bot/database/db_manager.py
Purpose: Universal database connection manager (PostgreSQL/SQLite)
FIXED: Syntax error - missing comma
"""

import os
import sqlite3
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Universal database connection manager
    Automatically detects and handles PostgreSQL or SQLite
    
    Features:
    - Context manager for safe connections
    - Auto-detection of database type
    - Table initialization
    - Database size calculation
    - Dictionary-like row access for both databases
    
    Reusable: YES
    """
    
    def __init__(self, db_path='posts.db'):
        self.db_path = db_path
        self.db_url = os.environ.get('DATABASE_URL')
    
    @contextmanager
    def get_db(self):
        """
        Context manager for database connections
        Automatically detects PostgreSQL or SQLite
        Returns rows as dictionary-like objects
        """
        if self.db_url:
            # PostgreSQL connection with DictCursor
            if self.db_url.startswith('postgres://'):
                self.db_url = self.db_url.replace('postgres://', 'postgresql://', 1)
            
            conn = psycopg2.connect(
                self.db_url,
                connect_timeout=10,
                sslmode='require',  # FIXED: Added comma here
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            conn.autocommit = False
            try:
                yield conn
            finally:
                conn.close()
        else:
            # SQLite connection with Row factory
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            finally:
                conn.close()
    
    def is_postgres(self):
        """Check if using PostgreSQL (True) or SQLite (False)"""
        return self.db_url is not None
    
    def init_database(self):
        """
        Initialize all database tables
        Creates posts, channels, channel_failures, recycle_bin, and recurring_posts tables
        """
        with self.get_db() as conn:
            c = conn.cursor()
            is_pg = self.is_postgres()
            
            if is_pg:
                # PostgreSQL syntax
                c.execute('''
                    CREATE TABLE IF NOT EXISTS posts (
                        id SERIAL PRIMARY KEY,
                        message TEXT,
                        media_type TEXT,
                        media_file_id TEXT,
                        caption TEXT,
                        scheduled_time TIMESTAMP NOT NULL,
                        posted INTEGER DEFAULT 0,
                        total_channels INTEGER DEFAULT 0,
                        successful_posts INTEGER DEFAULT 0,
                        posted_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        batch_id TEXT,
                        paused INTEGER DEFAULT 0
                    )
                ''')
                
                c.execute('''
                    CREATE TABLE IF NOT EXISTS channels (
                        id SERIAL PRIMARY KEY,
                        channel_id TEXT UNIQUE NOT NULL,
                        channel_name TEXT,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        active INTEGER DEFAULT 1,
                        failure_count INTEGER DEFAULT 0,
                        last_success TIMESTAMP,
                        last_failure TIMESTAMP,
                        in_skip_list INTEGER DEFAULT 0
                    )
                ''')
                
                c.execute('''
                    CREATE TABLE IF NOT EXISTS channel_failures (
                        id SERIAL PRIMARY KEY,
                        channel_id TEXT NOT NULL,
                        post_id INTEGER,
                        error_type TEXT,
                        error_message TEXT,
                        failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Recycle bin for deleted channels
                c.execute('''
                    CREATE TABLE IF NOT EXISTS recycle_bin (
                        id SERIAL PRIMARY KEY,
                        channel_id TEXT NOT NULL,
                        channel_name TEXT,
                        deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        failure_count INTEGER DEFAULT 0,
                        last_failure TIMESTAMP
                    )
                ''')
                
                # Batch config for auto-continuous mode
                c.execute('''
                    CREATE TABLE IF NOT EXISTS batch_config (
                        id SERIAL PRIMARY KEY,
                        user_id TEXT NOT NULL,
                        posts_per_batch INTEGER NOT NULL,
                        interval_minutes INTEGER NOT NULL,
                        minute_mark INTEGER DEFAULT 0,
                        active INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Recurring posts table
                c.execute('''
                    CREATE TABLE IF NOT EXISTS recurring_posts (
                        id SERIAL PRIMARY KEY,
                        pattern TEXT NOT NULL,
                        time TEXT NOT NULL,
                        day_of_week INTEGER,
                        day_of_month INTEGER,
                        message TEXT,
                        media_type TEXT,
                        media_file_id TEXT,
                        caption TEXT,
                        active INTEGER DEFAULT 1,
                        last_posted TIMESTAMP,
                        next_scheduled TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
            else:
                # SQLite syntax
                c.execute('''
                    CREATE TABLE IF NOT EXISTS posts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        message TEXT,
                        media_type TEXT,
                        media_file_id TEXT,
                        caption TEXT,
                        scheduled_time TIMESTAMP NOT NULL,
                        posted INTEGER DEFAULT 0,
                        total_channels INTEGER DEFAULT 0,
                        successful_posts INTEGER DEFAULT 0,
                        posted_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        batch_id TEXT,
                        paused INTEGER DEFAULT 0
                    )
                ''')
                
                c.execute('''
                    CREATE TABLE IF NOT EXISTS channels (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        channel_id TEXT UNIQUE NOT NULL,
                        channel_name TEXT,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        active INTEGER DEFAULT 1,
                        failure_count INTEGER DEFAULT 0,
                        last_success TIMESTAMP,
                        last_failure TIMESTAMP,
                        in_skip_list INTEGER DEFAULT 0
                    )
                ''')
                
                c.execute('''
                    CREATE TABLE IF NOT EXISTS channel_failures (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        channel_id TEXT NOT NULL,
                        post_id INTEGER,
                        error_type TEXT,
                        error_message TEXT,
                        failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                c.execute('''
                    CREATE TABLE IF NOT EXISTS recycle_bin (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        channel_id TEXT NOT NULL,
                        channel_name TEXT,
                        deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        failure_count INTEGER DEFAULT 0,
                        last_failure TIMESTAMP
                    )
                ''')
                
                c.execute('''
                    CREATE TABLE IF NOT EXISTS batch_config (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT NOT NULL,
                        posts_per_batch INTEGER NOT NULL,
                        interval_minutes INTEGER NOT NULL,
                        minute_mark INTEGER DEFAULT 0,
                        active INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                c.execute('''
                    CREATE TABLE IF NOT EXISTS recurring_posts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pattern TEXT NOT NULL,
                        time TEXT NOT NULL,
                        day_of_week INTEGER,
                        day_of_month INTEGER,
                        message TEXT,
                        media_type TEXT,
                        media_file_id TEXT,
                        caption TEXT,
                        active INTEGER DEFAULT 1,
                        last_posted TIMESTAMP,
                        next_scheduled TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
            
            # Create indexes for performance
            c.execute('CREATE INDEX IF NOT EXISTS idx_scheduled_posted ON posts(scheduled_time, posted)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_posted_at ON posts(posted_at)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_channel_active ON channels(active)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_channel_skip ON channels(in_skip_list)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_batch_id ON posts(batch_id)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_recycle_deleted ON recycle_bin(deleted_at)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_batch_user ON batch_config(user_id, active)')
            
            conn.commit()
            logger.info(f"✅ Database initialized ({'PostgreSQL' if is_pg else 'SQLite'})")
    
    def get_database_size(self):
        """
        Get database size in MB
        
        Returns:
            float: Database size in megabytes
        """
        with self.get_db() as conn:
            c = conn.cursor()
            
            if self.is_postgres():
                c.execute("SELECT pg_database_size(current_database())")
                result = c.fetchone()
                size_bytes = result['pg_database_size']
            else:
                c.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
                result = c.fetchone()
                size_bytes = result['size']
            
            return size_bytes / 1024 / 1024  # Convert to MB
