"""
File: core/scheduler_core.py
Location: telegram_scheduler_bot/core/scheduler_core.py
Purpose: Main scheduler orchestration class
FIXED: Made recurring posts optional
"""

import asyncio
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class SchedulerCore:
    """
    Main scheduler orchestration class
    
    Coordinates:
    - Database operations (posts_db, channels_db)
    - Rate limiting (rate_limiter)
    - Retry system (retry_system)
    - Parallel sending (sender)
    - Background tasks
    - User sessions
    """
    
    def __init__(self, db_manager, posts_db, channels_db, rate_limiter, retry_system, sender):
        self.db_manager = db_manager
        self.posts_db = posts_db
        self.channels_db = channels_db
        self.rate_limiter = rate_limiter
        self.retry_system = retry_system
        self.sender = sender
        
        # State
        self.user_sessions = {}
        self.emergency_stopped = False
        self.posting_lock = asyncio.Lock()
        
        # FIXED: Initialize recurring system only if feature exists
        try:
            from features.recurring_posts import RecurringPostsSystem
            self.recurring_system = RecurringPostsSystem(db_manager, posts_db, channels_db)
            logger.info("✅ Recurring posts system initialized")
        except ImportError:
            self.recurring_system = None
            logger.warning("⚠️  Recurring posts feature not available")
    
    def datetime_fromisoformat(self, iso_string):
        """Helper to parse ISO format datetime strings"""
        return datetime.fromisoformat(iso_string)
    
    async def process_due_posts(self, bot):
        """
        Check for posts due and send them
        Groups posts by batch_id or time proximity
        """
        if self.emergency_stopped:
            return
        
        async with self.posting_lock:
            posts = self.posts_db.get_due_posts(lookahead_seconds=30)
            
            if not posts:
                return
            
            # Group posts by batch_id or time proximity (within 5 seconds)
            batches = []
            current_batch = []
            last_time = None
            last_batch_id = None
            
            for post in posts:
                scheduled_time = datetime.fromisoformat(post['scheduled_time'])
                batch_id = post.get('batch_id')
                
                if last_time is None:
                    current_batch = [post]
                    last_time = scheduled_time
                    last_batch_id = batch_id
                else:
                    time_diff = abs((scheduled_time - last_time).total_seconds())
                    
                    # Group by batch_id or time proximity
                    if batch_id and batch_id == last_batch_id:
                        current_batch.append(post)
                    elif time_diff <= 5:
                        current_batch.append(post)
                    else:
                        batches.append((last_time, current_batch))
                        current_batch = [post]
                        last_time = scheduled_time
                        last_batch_id = batch_id
            
            if current_batch:
                batches.append((last_time, current_batch))
            
            # Process each batch
            for batch_time, batch_posts in batches:
                if self.emergency_stopped:
                    break
                
                # Wait until exact batch time
                now_utc = datetime.utcnow()
                if batch_time > now_utc:
                    wait_seconds = (batch_time - datetime.utcnow()).total_seconds()
                    if wait_seconds > 0 and wait_seconds <= 30:
                        logger.info(f"⏳ Waiting {wait_seconds:.1f}s for batch of {len(batch_posts)} posts")
                        await asyncio.sleep(wait_seconds)
                
                # Send batch
                logger.info(f"📦 Processing batch of {len(batch_posts)} posts")
                channel_ids = self.channels_db.get_active_channels()
                
                await self.sender.send_batch_to_all_channels(
                    bot=bot,
                    posts=batch_posts,
                    channel_ids=channel_ids,
                    db_manager=self.db_manager,
                    emergency_stopped_flag=lambda: self.emergency_stopped
                )
                
                await asyncio.sleep(1)
    
    async def background_poster(self, bot):
        """
        Background task that continuously checks for due posts
        """
        cleanup_counter = 0
        
        while True:
            try:
                await self.process_due_posts(bot)
                
                next_post_time = self.posts_db.get_next_scheduled_post()
                if next_post_time:
                    time_until_next = (next_post_time - datetime.utcnow()).total_seconds()
                    
                    if time_until_next > 0:
                        sleep_duration = min(max(time_until_next - 2, 1), 15)
                        logger.info(f"⏰ Next post in {time_until_next:.1f}s, sleeping {sleep_duration:.1f}s")
                        await asyncio.sleep(sleep_duration)
                    else:
                        await asyncio.sleep(1)
                else:
                    await asyncio.sleep(10)
                
                # Auto-cleanup old posts
                cleanup_counter += 1
                if cleanup_counter >= 2:
                    self.posts_db.cleanup_old_posts(minutes_old=30)
                    cleanup_counter = 0
                    
            except Exception as e:
                logger.error(f"Background task error: {e}")
            
            await asyncio.sleep(5)
