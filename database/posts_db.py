"""
File: database/posts_db.py
Location: telegram_scheduler_bot/database/posts_db.py
Purpose: All post database operations
FIXED: PostgreSQL compatibility - all queries return consistent dict format
"""

from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class PostsDB:
    """
    Post database operations
    All CRUD operations for scheduled posts
    FIXED: Consistent dict format for all queries
    """
    
    def __init__(self, db_manager):
        self.db = db_manager
    
    def _ph(self):
        """Placeholder helper for PostgreSQL (%s) vs SQLite (?)"""
        return '%s' if self.db.is_postgres() else '?'
    
    def _row_to_dict(self, row, columns):
        """
        Convert database row to dictionary
        Works with both SQLite (dict-like) and PostgreSQL (tuple)
        FIXED: Automatically converts datetime columns
        """
        if row is None:
            return None
        
        # If already dict-like, convert to regular dict
        if hasattr(row, 'keys'):
            result = dict(row)
        # Convert tuple to dict
        elif isinstance(row, tuple):
            result = {columns[i]: row[i] for i in range(min(len(columns), len(row)))}
        else:
            result = row
        
        # CRITICAL FIX: Ensure datetime fields are datetime objects, not strings
        datetime_fields = ['scheduled_time', 'posted_at', 'created_at', 'last_success', 'last_failure', 'deleted_at', 'added_at', 'failed_at', 'next_scheduled', 'last_posted']
        
        for field in datetime_fields:
            if field in result and result[field] is not None:
                value = result[field]
                # If it's a string, convert to datetime
                if isinstance(value, str):
                    try:
                        result[field] = datetime.fromisoformat(value)
                    except:
                        pass  # Keep as string if conversion fails
                # If already datetime, leave as-is
        
        return result
    
    def _rows_to_dicts(self, rows, columns):
        """Convert list of rows to list of dicts"""
        return [self._row_to_dict(row, columns) for row in rows] if rows else []
    
    def _ensure_datetime(self, value):
        """
        FIXED: Ensure value is a datetime object
        PostgreSQL returns datetime objects, SQLite returns strings
        """
        if value is None:
            return None
        
        if isinstance(value, datetime):
            return value
        
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except:
                return None
        
        return None
    
    def schedule_post(self, scheduled_time_utc, message=None, media_type=None,
                     media_file_id=None, caption=None, batch_id=None, total_channels=0):
        """Schedule a new post"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            c.execute(f'''
                INSERT INTO posts (message, media_type, media_file_id, caption,
                                 scheduled_time, total_channels, batch_id)
                VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
            ''', (message, media_type, media_file_id, caption,
                  scheduled_time_utc.isoformat(), total_channels, batch_id))
            conn.commit()
            
            # Get last inserted ID
            if self.db.is_postgres():
                return c.lastrowid if hasattr(c, 'lastrowid') else None
            else:
                return c.lastrowid
    
    def get_pending_posts(self):
        """Get all pending posts ordered by scheduled time"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM posts WHERE posted = 0 ORDER BY scheduled_time')
            rows = c.fetchall()
            
            # Column names for posts table
            columns = ['id', 'message', 'media_type', 'media_file_id', 'caption',
                      'scheduled_time', 'posted', 'total_channels', 'successful_posts',
                      'posted_at', 'created_at', 'batch_id', 'paused']
            
            return self._rows_to_dicts(rows, columns)
    
    def get_due_posts(self, lookahead_seconds=30):
        """
        Get posts due for sending (with lookahead)
        FIXED: Returns list of dicts, not tuples
        """
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            now_utc = datetime.utcnow()
            check_until = (now_utc + timedelta(seconds=lookahead_seconds)).isoformat()
            
            c.execute(f'''
                SELECT * FROM posts 
                WHERE scheduled_time <= {ph} AND posted = 0 
                ORDER BY scheduled_time LIMIT 200
            ''', (check_until,))
            rows = c.fetchall()
            
            # FIXED: Convert to dict format
            columns = ['id', 'message', 'media_type', 'media_file_id', 'caption',
                      'scheduled_time', 'posted', 'total_channels', 'successful_posts',
                      'posted_at', 'created_at', 'batch_id', 'paused']
            
            return self._rows_to_dicts(rows, columns)
    
    def mark_post_sent(self, post_id, successful_posts):
        """Mark a post as sent"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            c.execute(f'''
                UPDATE posts 
                SET posted = 1, posted_at = {ph}, successful_posts = {ph}
                WHERE id = {ph}
            ''', (datetime.utcnow().isoformat(), successful_posts, post_id))
            conn.commit()
    
    def delete_post(self, post_id):
        """Delete a post by ID"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            c.execute(f'DELETE FROM posts WHERE id = {ph}', (post_id,))
            conn.commit()
            return c.rowcount > 0
    
    def delete_posts_by_numbers(self, numbers):
        """Delete posts by their list numbers"""
        pending = self.get_pending_posts()
        deleted = 0
        
        for num in numbers:
            if 1 <= num <= len(pending):
                post = pending[num - 1]
                if self.delete_post(post['id']):
                    deleted += 1
        
        return deleted
    
    def delete_all_pending(self, confirm=None):
        """Delete all pending posts (requires confirm)"""
        if confirm != 'confirm':
            return -1
        
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM posts WHERE posted = 0')
            deleted = c.rowcount
            conn.commit()
            return deleted
    
    def move_posts(self, post_ids, new_start_time_utc, preserve_intervals=True):
        """Move posts to new time"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            # Get posts to move
            placeholders = ','.join([ph] * len(post_ids))
            c.execute(f'SELECT * FROM posts WHERE id IN ({placeholders}) ORDER BY scheduled_time', post_ids)
            rows = c.fetchall()
            
            columns = ['id', 'message', 'media_type', 'media_file_id', 'caption',
                      'scheduled_time', 'posted', 'total_channels', 'successful_posts',
                      'posted_at', 'created_at', 'batch_id', 'paused']
            posts = self._rows_to_dicts(rows, columns)
            
            if not posts:
                return 0
            
            # Calculate intervals if preserving
            if preserve_intervals and len(posts) > 1:
                first_time = datetime.fromisoformat(posts[0]['scheduled_time'])
                last_time = datetime.fromisoformat(posts[-1]['scheduled_time'])
                total_duration = (last_time - first_time).total_seconds() / 60
                interval = total_duration / (len(posts) - 1) if len(posts) > 1 else 0
            else:
                interval = 0
            
            # Update posts
            moved = 0
            for i, post in enumerate(posts):
                new_time = new_start_time_utc + timedelta(minutes=interval * i)
                c.execute(f'UPDATE posts SET scheduled_time = {ph} WHERE id = {ph}',
                         (new_time.isoformat(), post['id']))
                moved += 1
            
            conn.commit()
            return moved
    
    def move_posts_by_numbers(self, numbers, new_start_time_utc):
        """Move posts by their list numbers"""
        pending = self.get_pending_posts()
        post_ids = []
        
        for num in numbers:
            if 1 <= num <= len(pending):
                post_ids.append(pending[num - 1]['id'])
        
        if not post_ids:
            return 0
        
        return self.move_posts(post_ids, new_start_time_utc, preserve_intervals=True)
    
    def get_posts_by_batch_id(self, batch_id):
        """Get all posts with specific batch_id"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            c.execute(f'SELECT * FROM posts WHERE batch_id = {ph} ORDER BY scheduled_time', (batch_id,))
            rows = c.fetchall()
            
            columns = ['id', 'message', 'media_type', 'media_file_id', 'caption',
                      'scheduled_time', 'posted', 'total_channels', 'successful_posts',
                      'posted_at', 'created_at', 'batch_id', 'paused']
            
            return self._rows_to_dicts(rows, columns)
    
    def get_last_post(self):
        """Get the last scheduled post"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM posts WHERE posted = 0 ORDER BY scheduled_time DESC LIMIT 1')
            row = c.fetchone()
            
            columns = ['id', 'message', 'media_type', 'media_file_id', 'caption',
                      'scheduled_time', 'posted', 'total_channels', 'successful_posts',
                      'posted_at', 'created_at', 'batch_id', 'paused']
            
            return self._row_to_dict(row, columns)
    
    def get_last_batch(self):
        """Get posts from the last batch"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('''
                SELECT DISTINCT batch_id 
                FROM posts 
                WHERE posted = 0 AND batch_id IS NOT NULL 
                ORDER BY scheduled_time DESC LIMIT 1
            ''')
            result = c.fetchone()
            
            if result and result[0]:
                batch_id = result[0]
                c.execute(f'SELECT * FROM posts WHERE batch_id = {self._ph()} ORDER BY scheduled_time',
                         (batch_id,))
                rows = c.fetchall()
                
                columns = ['id', 'message', 'media_type', 'media_file_id', 'caption',
                          'scheduled_time', 'posted', 'total_channels', 'successful_posts',
                          'posted_at', 'created_at', 'batch_id', 'paused']
                
                return self._rows_to_dicts(rows, columns)
        
        return None
    
    def get_next_scheduled_post(self):
        """
        Get time of next scheduled post
        FIXED: Handle both dict-like Row objects and tuples
        """
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT scheduled_time FROM posts WHERE posted = 0 ORDER BY scheduled_time LIMIT 1')
            result = c.fetchone()
            
            if not result:
                return None
            
            # Handle both dict-like (SQLite Row) and tuple (PostgreSQL)
            try:
                # Try dict-like access first (SQLite Row object)
                scheduled_value = result['scheduled_time']
            except (KeyError, TypeError):
                # Fall back to tuple access (PostgreSQL or plain tuple)
                try:
                    scheduled_value = result[0]
                except (IndexError, KeyError, TypeError):
                    logger.error(f"Unable to access scheduled_time from result: {type(result)}, {result}")
                    return None
            
            # If already datetime, return it
            if isinstance(scheduled_value, datetime):
                return scheduled_value
            
            # If string, parse it
            if isinstance(scheduled_value, str):
                try:
                    return datetime.fromisoformat(scheduled_value)
                except Exception as e:
                    logger.error(f"Failed to parse scheduled_time: {scheduled_value}, error: {e}")
                    return None
            
            logger.error(f"Invalid scheduled_time type: {type(scheduled_value)}")
            return None
    
    def cleanup_old_posts(self, minutes_old=30):
        """Delete old posted content"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            cutoff = (datetime.utcnow() - timedelta(minutes=minutes_old)).isoformat()
            
            c.execute(f'SELECT COUNT(*) FROM posts WHERE posted = 1 AND posted_at < {ph}', (cutoff,))
            count_to_delete = c.fetchone()[0]
            
            if count_to_delete > 0:
                c.execute(f'DELETE FROM posts WHERE posted = 1 AND posted_at < {ph}', (cutoff,))
                conn.commit()
                
                if not self.db.is_postgres():
                    c.execute('VACUUM')
                
                logger.info(f"🧹 Cleaned {count_to_delete} old posts")
                return count_to_delete
            return 0
    
    def get_database_stats(self):
        """Get post statistics"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT COUNT(*) FROM posts')
            total_posts = c.fetchone()[0]
            c.execute('SELECT COUNT(*) FROM posts WHERE posted = 0')
            pending_posts = c.fetchone()[0]
            c.execute('SELECT COUNT(*) FROM posts WHERE posted = 1')
            posted_posts = c.fetchone()[0]
            
            return {
                'total': total_posts,
                'pending': pending_posts,
                'posted': posted_posts,
                'db_size_mb': self.db.get_database_size()
            }
    
    def get_overdue_posts(self):
        """Get posts that were scheduled in the past but not sent"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            now_utc = datetime.utcnow().isoformat()
            c.execute(f'SELECT * FROM posts WHERE scheduled_time < {ph} AND posted = 0 ORDER BY scheduled_time',
                     (now_utc,))
            rows = c.fetchall()
            
            columns = ['id', 'message', 'media_type', 'media_file_id', 'caption',
                      'scheduled_time', 'posted', 'total_channels', 'successful_posts',
                      'posted_at', 'created_at', 'batch_id', 'paused']
            
            return self._rows_to_dicts(rows, columns)

