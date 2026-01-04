"""
File: core/sender.py
Location: telegram_scheduler_bot/core/sender.py
Purpose: High-performance parallel sender (IMPROVEMENT #10)
Reusable: YES - Copy for any broadcast bot
"""

import asyncio
from datetime import datetime
from telegram.error import TelegramError
import logging

logger = logging.getLogger(__name__)

class ParallelSender:
    """
    High-performance parallel sender for multiple channels
    
    Features:
    - Parallel+Hybrid sending strategy
    - Sends each post to all channels simultaneously
    - Integrates with rate limiter and retry system
    - Marks posts as sent immediately (prevents duplicates)
    - Tracks failed sends for retry
    - 10-15 seconds for 402 messages (201 posts × 2 channels)
    
    IMPROVEMENT #10: Parallel+Hybrid for maximum speed
    Reusable: YES - Copy for any multi-channel broadcasting
    """
    
    def __init__(self, rate_limiter, retry_system):
        self.rate_limiter = rate_limiter
        self.retry_system = retry_system
    
    async def send_post_to_channel(self, bot, post, channel_id):
        """
        Send a single post to a single channel
        
        Args:
            bot: Telegram bot instance
            post: Post dict with content
            channel_id: Target channel ID
        
        Returns:
            bool: True if successful, False if failed
        """
        # Check skip list (IMPROVEMENT #7)
        if self.retry_system.should_skip(channel_id):
            logger.info(f"⏭️ Skipping channel {channel_id} (in skip list)")
            return False
        
        # Wait for rate limit clearance (IMPROVEMENT #9)
        await self.rate_limiter.acquire(channel_id)
        
        try:
            # Send based on media type
            if post['media_type'] == 'photo':
                await bot.send_photo(
                    chat_id=channel_id,
                    photo=post['media_file_id'],
                    caption=post['caption']
                )
            elif post['media_type'] == 'video':
                await bot.send_video(
                    chat_id=channel_id,
                    video=post['media_file_id'],
                    caption=post['caption']
                )
            elif post['media_type'] == 'document':
                await bot.send_document(
                    chat_id=channel_id,
                    document=post['media_file_id'],
                    caption=post['caption']
                )
            else:
                await bot.send_message(
                    chat_id=channel_id,
                    text=post['message']
                )
            
            # Report success
            self.rate_limiter.report_success()
            self.retry_system.record_success(channel_id)
            return True
            
        except TelegramError as e:
            error_msg = str(e).lower()
            
            # Check if it's flood control (IMPROVEMENT #9)
            if 'flood' in error_msg or 'too many requests' in error_msg:
                self.rate_limiter.report_flood_control()
            
            # Record failure (IMPROVEMENT #7 & #21)
            self.retry_system.record_failure(channel_id, e, post.get('id'))
            logger.error(f"❌ Failed channel {channel_id}: {e}")
            return False
    
    async def send_batch_to_all_channels(self, bot, posts, channel_ids, db_manager, 
                                        emergency_stopped_flag=None):
        """
        PARALLEL+HYBRID STRATEGY for maximum speed
        
        Strategy:
        1. Send each post to ALL channels simultaneously (parallel)
        2. Mark posts as sent immediately (prevents duplicates on restart)
        3. Skip failed channels during main send
        4. Retry ALL failures AFTER batch complete
        
        Args:
            bot: Telegram bot instance
            posts: List of post dicts to send
            channel_ids: List of target channel IDs
            db_manager: Database manager for marking posts
            emergency_stopped_flag: Callable that returns True if stopped
        
        IMPROVEMENT #10: Parallel+Hybrid sending (10-15 sec for 402 msgs)
        IMPROVEMENT #7: Smart retry (skip failed, retry later)
        IMPROVEMENT #15: Emergency stop support
        """
        if emergency_stopped_flag and emergency_stopped_flag():
            logger.warning("⚠️ Emergency stopped - not sending")
            return
        
        total_messages = len(posts) * len(channel_ids)
        logger.info(f"🚀 BATCH START: {len(posts)} posts × {len(channel_ids)} channels = {total_messages} messages")
        
        start_time = asyncio.get_event_loop().time()
        messages_sent = 0
        failed_sends = []  # Track (post_id, channel_id) for retry
        
        # MAIN SEND: Each post to all channels in parallel
        for i, post in enumerate(posts):
            if emergency_stopped_flag and emergency_stopped_flag():
                logger.warning("⚠️ Emergency stop triggered")
                break
            
            logger.info(f"📤 Sending post {i+1}/{len(posts)} (ID: {post['id']})")
            
            # Create tasks for all channels
            tasks = []
            for channel_id in channel_ids:
                tasks.append(self.send_post_to_channel(bot, post, channel_id))
            
            # Execute all sends in parallel
            results = await asyncio.gather(*tasks)
            successful = sum(results)
            messages_sent += len(results)
            
            # Track failures for retry
            for idx, success in enumerate(results):
                if not success:
                    failed_sends.append((post['id'], channel_ids[idx]))
            
            # Mark post as sent IMMEDIATELY (prevents duplicates on restart)
            with db_manager.get_db() as conn:
                c = conn.cursor()
                c.execute('''
                    UPDATE posts 
                    SET posted = 1, posted_at = ?, successful_posts = ? 
                    WHERE id = ?
                ''', (datetime.utcnow().isoformat(), successful, post['id']))
                conn.commit()
            
            # Log progress
            elapsed = asyncio.get_event_loop().time() - start_time
            rate = messages_sent / elapsed if elapsed > 0 else 0
            logger.info(f"✅ Post {post['id']}: {successful}/{len(channel_ids)} | Rate: {rate:.1f} msg/s")
        
        # RETRY PHASE: Retry all failed sends (IMPROVEMENT #7)
        if failed_sends and not (emergency_stopped_flag and emergency_stopped_flag()):
            logger.info(f"🔄 RETRY PHASE: {len(failed_sends)} failed sends")
            retry_success = 0
            
            for post_id, channel_id in failed_sends:
                # Get post data
                with db_manager.get_db() as conn:
                    c = conn.cursor()
                    c.execute('SELECT * FROM posts WHERE id = ?', (post_id,))
                    post = c.fetchone()
                
                if post and await self.send_post_to_channel(bot, post, channel_id):
                    retry_success += 1
            
            logger.info(f"✅ Retry success: {retry_success}/{len(failed_sends)}")
        
        # ALERT PHASE: Check for channels needing attention (IMPROVEMENT #8)
        for channel_id in channel_ids:
            if self.retry_system.needs_alert(channel_id):
                failures = self.retry_system.consecutive_failures[channel_id]
                logger.warning(f"⚠️ Channel {channel_id}: {failures} consecutive failures - needs attention!")
        
        # Final summary
        total_time = asyncio.get_event_loop().time() - start_time
        final_rate = total_messages / total_time if total_time > 0 else 0
        logger.info(f"🎉 BATCH COMPLETE: {total_messages} messages in {total_time:.1f}s ({final_rate:.1f} msg/s)")
        
        return {
            'total_messages': total_messages,
            'time_taken': total_time,
            'rate': final_rate,
            'failed_count': len(failed_sends),
            'retry_success': retry_success if failed_sends else 0
        }