"""
File: features/recurring_posts.py
Location: telegram_scheduler_bot/features/recurring_posts.py
Purpose: Recurring post scheduling system (daily, weekly, monthly)
Reusable: YES - Copy for any scheduling bot
"""

from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class RecurringPostsSystem:
    """
    Recurring posts scheduling system
    
    Features:
    - Daily recurring posts (every day at specific time)
    - Weekly recurring posts (every week on specific day)
    - Monthly recurring posts (specific date each month)
    - Auto-schedules next occurrence after posting
    - Pause/resume individual recurring posts
    
    Database structure (add to db_manager.py):
    CREATE TABLE recurring_posts (
        id INTEGER PRIMARY KEY,
        pattern TEXT NOT NULL,  -- 'daily', 'weekly', 'monthly'
        time TEXT NOT NULL,      -- 'HH:MM' format
        day_of_week INTEGER,     -- 0-6 for weekly (Monday=0)
        day_of_month INTEGER,    -- 1-31 for monthly
        message TEXT,
        media_type TEXT,
        media_file_id TEXT,
        caption TEXT,
        active INTEGER DEFAULT 1,
        last_posted TIMESTAMP,
        next_scheduled TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    def __init__(self, db_manager, posts_db, channels_db):
        self.db = db_manager
        self.posts_db = posts_db
        self.channels_db = channels_db
    
    def add_recurring_post(self, pattern, time, message=None, media_type=None,
                          media_file_id=None, caption=None, day_of_week=None,
                          day_of_month=None):
        """
        Add a new recurring post
        
        Args:
            pattern: 'daily', 'weekly', or 'monthly'
            time: 'HH:MM' format (e.g., '09:00', '18:30')
            message: Text message content
            media_type: 'photo', 'video', 'document'
            media_file_id: Telegram file ID
            caption: Media caption
            day_of_week: 0-6 for weekly (0=Monday, 6=Sunday)
            day_of_month: 1-31 for monthly
        
        Returns:
            int: Recurring post ID
        
        Examples:
            # Daily at 9 AM
            add_recurring_post('daily', '09:00', message='Good morning!')
            
            # Weekly on Monday at 6 PM
            add_recurring_post('weekly', '18:00', message='Weekly report', day_of_week=0)
            
            # Monthly on 1st at noon
            add_recurring_post('monthly', '12:00', message='Monthly update', day_of_month=1)
        """
        # Calculate next scheduled time
        next_scheduled = self._calculate_next_occurrence(pattern, time, day_of_week, day_of_month)
        
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('''
                INSERT INTO recurring_posts 
                (pattern, time, day_of_week, day_of_month, message, media_type, 
                 media_file_id, caption, next_scheduled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (pattern, time, day_of_week, day_of_month, message, media_type,
                  media_file_id, caption, next_scheduled.isoformat()))
            conn.commit()
            recurring_id = c.lastrowid
        
        # Schedule first occurrence
        self._schedule_next_post(recurring_id)
        
        logger.info(f"✅ Added recurring post #{recurring_id} ({pattern} at {time})")
        return recurring_id
    
    def _calculate_next_occurrence(self, pattern, time, day_of_week=None, day_of_month=None):
        """
        Calculate next occurrence datetime
        
        Args:
            pattern: 'daily', 'weekly', 'monthly'
            time: 'HH:MM'
            day_of_week: 0-6 for weekly
            day_of_month: 1-31 for monthly
        
        Returns:
            datetime: Next occurrence in UTC
        """
        from config import ist_to_utc, get_ist_now
        
        now_ist = get_ist_now()
        hour, minute = map(int, time.split(':'))
        
        if pattern == 'daily':
            # Next occurrence is today at specified time, or tomorrow if past
            next_ist = now_ist.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if next_ist <= now_ist:
                next_ist += timedelta(days=1)
        
        elif pattern == 'weekly':
            # Next occurrence on specified day of week
            if day_of_week is None:
                raise ValueError("day_of_week required for weekly pattern")
            
            next_ist = now_ist.replace(hour=hour, minute=minute, second=0, microsecond=0)
            days_ahead = (day_of_week - now_ist.weekday()) % 7
            
            if days_ahead == 0 and next_ist <= now_ist:
                days_ahead = 7
            
            next_ist += timedelta(days=days_ahead)
        
        elif pattern == 'monthly':
            # Next occurrence on specified day of month
            if day_of_month is None:
                raise ValueError("day_of_month required for monthly pattern")
            
            next_ist = now_ist.replace(day=day_of_month, hour=hour, minute=minute, 
                                       second=0, microsecond=0)
            
            if next_ist <= now_ist:
                # Move to next month
                if next_ist.month == 12:
                    next_ist = next_ist.replace(year=next_ist.year + 1, month=1)
                else:
                    next_ist = next_ist.replace(month=next_ist.month + 1)
        
        else:
            raise ValueError(f"Invalid pattern: {pattern}")
        
        return ist_to_utc(next_ist)
    
    def _schedule_next_post(self, recurring_id):
        """
        Schedule the next occurrence of a recurring post
        
        Args:
            recurring_id: Recurring post ID
        """
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM recurring_posts WHERE id = ?', (recurring_id,))
            recurring = c.fetchone()
        
        if not recurring or not recurring['active']:
            return
        
        # Create scheduled post
        scheduled_time = datetime.fromisoformat(recurring['next_scheduled'])
        
        post_id = self.posts_db.schedule_post(
            scheduled_time_utc=scheduled_time,
            message=recurring.get('message'),
            media_type=recurring.get('media_type'),
            media_file_id=recurring.get('media_file_id'),
            caption=recurring.get('caption'),
            batch_id=f"recurring_{recurring_id}",
            total_channels=self.channels_db.get_channel_count()
        )
        
        logger.info(f"📅 Scheduled recurring post #{recurring_id} as post #{post_id}")
    
    def process_posted_recurring(self, post_id):
        """
        Process a recurring post after it's been posted
        Calculate and schedule next occurrence
        
        Args:
            post_id: Post ID that was just posted
        
        Should be called from scheduler after successful post send
        """
        # Check if post is from recurring
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT batch_id FROM posts WHERE id = ?', (post_id,))
            result = c.fetchone()
        
        if not result or not result['batch_id']:
            return
        
        batch_id = result['batch_id']
        if not batch_id.startswith('recurring_'):
            return
        
        # Extract recurring ID
        recurring_id = int(batch_id.replace('recurring_', ''))
        
        # Get recurring post details
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM recurring_posts WHERE id = ?', (recurring_id,))
            recurring = c.fetchone()
        
        if not recurring or not recurring['active']:
            return
        
        # Calculate next occurrence
        next_scheduled = self._calculate_next_occurrence(
            recurring['pattern'],
            recurring['time'],
            recurring.get('day_of_week'),
            recurring.get('day_of_month')
        )
        
        # Update recurring post
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('''
                UPDATE recurring_posts 
                SET last_posted = ?, next_scheduled = ?
                WHERE id = ?
            ''', (datetime.utcnow().isoformat(), next_scheduled.isoformat(), recurring_id))
            conn.commit()
        
        # Schedule next post
        self._schedule_next_post(recurring_id)
        
        logger.info(f"🔄 Recurring post #{recurring_id} rescheduled for {next_scheduled}")
    
    def get_all_recurring(self):
        """Get all recurring posts"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM recurring_posts ORDER BY created_at DESC')
            return c.fetchall()
    
    def get_active_recurring(self):
        """Get only active recurring posts"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM recurring_posts WHERE active = 1 ORDER BY next_scheduled')
            return c.fetchall()
    
    def pause_recurring(self, recurring_id):
        """Pause a recurring post"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('UPDATE recurring_posts SET active = 0 WHERE id = ?', (recurring_id,))
            conn.commit()
        logger.info(f"⏸️ Paused recurring post #{recurring_id}")
    
    def resume_recurring(self, recurring_id):
        """Resume a paused recurring post"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('UPDATE recurring_posts SET active = 1 WHERE id = ?', (recurring_id,))
            conn.commit()
        
        # Recalculate next scheduled time
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM recurring_posts WHERE id = ?', (recurring_id,))
            recurring = c.fetchone()
        
        next_scheduled = self._calculate_next_occurrence(
            recurring['pattern'],
            recurring['time'],
            recurring.get('day_of_week'),
            recurring.get('day_of_month')
        )
        
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('UPDATE recurring_posts SET next_scheduled = ? WHERE id = ?',
                     (next_scheduled.isoformat(), recurring_id))
            conn.commit()
        
        self._schedule_next_post(recurring_id)
        logger.info(f"▶️ Resumed recurring post #{recurring_id}")
    
    def delete_recurring(self, recurring_id):
        """Delete a recurring post"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM recurring_posts WHERE id = ?', (recurring_id,))
            conn.commit()
        logger.info(f"🗑️ Deleted recurring post #{recurring_id}")
    
    def get_pattern_description(self, recurring):
        """
        Get human-readable description of recurring pattern
        
        Args:
            recurring: Recurring post dict
        
        Returns:
            str: Description like "Daily at 09:00 IST"
        """
        from config import format_time_display
        
        time = recurring['time']
        pattern = recurring['pattern']
        
        if pattern == 'daily':
            return f"Daily at {time} IST"
        elif pattern == 'weekly':
            days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            day = days[recurring['day_of_week']]
            return f"Every {day} at {time} IST"
        elif pattern == 'monthly':
            return f"Every month on day {recurring['day_of_month']} at {time} IST"
        
        return "Unknown pattern"