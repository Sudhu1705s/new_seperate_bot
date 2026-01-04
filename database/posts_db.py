"""
File: database/posts_db.py
Location: telegram_scheduler_bot/database/posts_db.py
Purpose: All post database operations
Reusable: Modify for any scheduling system
"""

from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class PostsDB:
    """
    Post database operations
    All CRUD operations for scheduled posts
    """
    
    def __init__(self, db_manager):
        self.db = db_manager
    
    def schedule_post(self, scheduled_time_utc, message=None, media_type=None,
                     media_file_id=None, caption=None, batch_id=None, total_channels=0):
        """Schedule a new post"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('''
                INSERT INTO posts (message, media_type, media_file_id, caption,
                                 scheduled_time, total_channels, batch_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (message, media_type, media_file_id, caption,
                  scheduled_time_utc.isoformat(), total_channels, batch_id))
            conn.commit()
            return c.lastrowid
    
    def get_pending_posts(self):
        """Get all pending posts ordered by scheduled time"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM posts WHERE posted = 0 ORDER BY scheduled_time')
            return c.fetchall()
    
    def get_due_posts(self, lookahead_seconds=30):
        """Get posts due for sending (with lookahead)"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            now_utc = datetime.utcnow()
            check_until = (now_utc + timedelta(seconds=lookahead_seconds)).isoformat()
            
            c.execute('''
                SELECT * FROM posts 
                WHERE scheduled_time <= ? AND posted = 0 
                ORDER BY scheduled_time LIMIT 200
            ''', (check_until,))
            return c.fetchall()
    
    def mark_post_sent(self, post_id, successful_posts):
        """Mark a post as sent"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('''
                UPDATE posts 
                SET posted = 1, posted_at = ?, successful_posts = ? 
                WHERE id = ?
            ''', (datetime.utcnow().isoformat(), successful_posts, post_id))
            conn.commit()
    
    def delete_post(self, post_id):
        """Delete a post by ID"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM posts WHERE id = ?', (post_id,))
            conn.commit()
            return c.rowcount > 0
    
    def delete_posts_by_numbers(self, numbers):
        """Delete posts by their list numbers (IMPROVEMENT #5)"""
        pending = self.get_pending_posts()
        deleted = 0
        
        for num in numbers:
            if 1 <= num <= len(pending):
                post = pending[num - 1]
                if self.delete_post(post['id']):
                    deleted += 1
        
        return deleted
    
    def delete_all_pending(self, confirm=None):
        """
        Delete all pending posts (IMPROVEMENT #5 - requires confirm)
        
        Args:
            confirm: Must be 'confirm' to proceed
        
        Returns:
            int: Number of deleted posts, or -1 if not confirmed
        """
        if confirm != 'confirm':
            return -1
        
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM posts WHERE posted = 0')
            deleted = c.rowcount
            conn.commit()
            return deleted
    
    def move_posts(self, post_ids, new_start_time_utc, preserve_intervals=True):
        """Move posts to new time (IMPROVEMENT #6)"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            
            # Get posts to move
            placeholders = ','.join('?' * len(post_ids))
            c.execute(f'SELECT * FROM posts WHERE id IN ({placeholders}) ORDER BY scheduled_time', post_ids)
            posts = c.fetchall()
            
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
                c.execute('UPDATE posts SET scheduled_time = ? WHERE id = ?',
                         (new_time.isoformat(), post['id']))
                moved += 1
            
            conn.commit()
            return moved
    
    def move_posts_by_numbers(self, numbers, new_start_time_utc):
        """Move posts by their list numbers (IMPROVEMENT #6)"""
        pending = self.get_pending_posts()
        post_ids = []
        
        for num in numbers:
            if 1 <= num <= len(pending):
                post_ids.append(pending[num - 1]['id'])
        
        if not post_ids:
            return 0
        
        return self.move_posts(post_ids, new_start_time_utc, preserve_intervals=True)
    
    def get_posts_by_batch_id(self, batch_id):
        """
        Get all posts with specific batch_id (IMPROVEMENT #9)
        
        Args:
            batch_id: Batch identifier
        
        Returns:
            list: Posts in the batch
        """
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM posts WHERE batch_id = ? ORDER BY scheduled_time', (batch_id,))
            return c.fetchall()
    
    def get_last_post(self):
        """Get the last scheduled post (IMPROVEMENT #12)"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM posts WHERE posted = 0 ORDER BY scheduled_time DESC LIMIT 1')
            return c.fetchone()
    
    def get_last_batch(self):
        """Get posts from the last batch (IMPROVEMENT #12)"""
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
                c.execute('SELECT * FROM posts WHERE batch_id = ? ORDER BY scheduled_time',
                         (result[0],))
                return c.fetchall()
        
        return None
    
    def get_next_scheduled_post(self):
        """Get time of next scheduled post"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT scheduled_time FROM posts WHERE posted = 0 ORDER BY scheduled_time LIMIT 1')
            result = c.fetchone()
            if result:
                return datetime.fromisoformat(result[0])
            return None
    
    def cleanup_old_posts(self, minutes_old=30):
        """Delete old posted content"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            cutoff = (datetime.utcnow() - timedelta(minutes=minutes_old)).isoformat()
            
            c.execute('SELECT COUNT(*) FROM posts WHERE posted = 1 AND posted_at < ?', (cutoff,))
            count_to_delete = c.fetchone()[0]
            
            if count_to_delete > 0:
                c.execute('DELETE FROM posts WHERE posted = 1 AND posted_at < ?', (cutoff,))
                conn.commit()
                
                if not self.db.is_postgres():
                    c.execute('VACUUM')
                
                logger.info(f"🧹 Cleaned {count_to_delete} old posts")
                return count_to_delete
            return 0
    
    def get_database_stats(self):
        """Get post statistics (IMPROVEMENT #16)"""
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
            now_utc = datetime.utcnow().isoformat()
            c.execute('SELECT * FROM posts WHERE scheduled_time < ? AND posted = 0 ORDER BY scheduled_time',
                     (now_utc,))
            return c.fetchall()