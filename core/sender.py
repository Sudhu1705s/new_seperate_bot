"""
File: core/sender.py
Location: telegram_scheduler_bot/core/sender.py
Purpose: High-performance parallel sender
FIXED: PostgreSQL compatibility + Interactive failure notifications
"""

import asyncio
from datetime import datetime
from telegram.error import TelegramError
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import logging

logger = logging.getLogger(__name__)

class ParallelSender:
    """
    High-performance parallel sender for multiple channels
    FIXED: Interactive notifications for failed channels
    """
    
    def __init__(self, rate_limiter, retry_system):
        self.rate_limiter = rate_limiter
        self.retry_system = retry_system
        self.admin_notified = {}  # Track which channels we've already notified about
    
    def _ph(self, db_manager):
        """Placeholder helper for PostgreSQL (%s) vs SQLite (?)"""
        return '%s' if db_manager.is_postgres() else '?'
    
    async def _notify_admin_with_actions(self, bot, channel_id, error_message, failure_count):
        """
        Send interactive notification to admin about unreachable channel
        Gives admin options to handle the situation
        """
        from config.settings import ADMIN_ID
        
        # Build notification message
        message = f"🚨 <b>CHANNEL UNREACHABLE</b>\n\n"
        message += f"Channel: <code>{channel_id}</code>\n"
        message += f"Failures: <b>{failure_count}</b>\n"
        message += f"Error: <code>{error_message[:150]}</code>\n\n"
        
        if failure_count >= 3:
            message += "⚠️ <b>Channel added to skip list!</b>\n"
            message += "Posts will NOT be sent to this channel.\n\n"
        
        message += "❓ <b>What do you want to do?</b>"
        
        # Create action buttons
        keyboard = [
            [
                InlineKeyboardButton("🧪 Test Channel", callback_data=f"test_channel:{channel_id}"),
                InlineKeyboardButton("🔄 Retry Now", callback_data=f"retry_channel:{channel_id}")
            ],
            [
                InlineKeyboardButton("🗑️ Delete Permanently", callback_data=f"delete_channel:{channel_id}"),
                InlineKeyboardButton("♻️ Move to Recycle Bin", callback_data=f"recycle_channel:{channel_id}")
            ],
            [
                InlineKeyboardButton("✅ Keep & Resume", callback_data=f"resume_channel:{channel_id}"),
                InlineKeyboardButton("📋 View Failures", callback_data=f"failures:{channel_id}")
            ],
            [
                InlineKeyboardButton("❌ Ignore", callback_data="ignore")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            await bot.send_message(
                chat_id=ADMIN_ID,
                text=message,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            logger.info(f"✅ Admin notified about failed channel {channel_id}")
        except Exception as e:
            logger.error(f"Failed to notify admin: {e}")
    
    async def _notify_first_failure(self, bot, channel_id, error_message):
        """Notify admin immediately when a channel first fails"""
        from config.settings import ADMIN_ID
        
        message = f"⚠️ <b>Channel Failed (First Time)</b>\n\n"
        message += f"Channel: <code>{channel_id}</code>\n"
        message += f"Error: <code>{error_message[:100]}</code>\n\n"
        message += f"💡 Will retry automatically...\n"
        message += f"If this persists, you'll get action options."
        
        try:
            await bot.send_message(
                chat_id=ADMIN_ID,
                text=message,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Failed to notify admin about first failure: {e}")

    async def _notify_second_failure(self, bot, channel_id, error_message):
        """Notify admin on second failure - warning level"""
        from config.settings import ADMIN_ID
        
        message = f"⚠️ <b>Channel Failed AGAIN (2nd Time)</b>\n\n"
        message += f"Channel: <code>{channel_id}</code>\n"
        message += f"Error: <code>{error_message[:100]}</code>\n\n"
        message += f"🔄 Will retry one more time...\n"
        message += f"⚠️ If this fails again, you'll get action options."
        
        try:
            await bot.send_message(
                chat_id=ADMIN_ID,
                text=message,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Failed to notify admin about second failure: {e}")
    
    def _get_post_value(self, post, key, default=None):
        """Safely get value from post (dict or tuple)"""
        if post is None:
            return default
        
        try:
            if isinstance(post, dict):
                return post.get(key, default)
            else:
                # Map common keys to indices
                key_map = {
                    'id': 0, 'message': 1, 'media_type': 2, 'media_file_id': 3,
                    'caption': 4, 'scheduled_time': 5, 'posted': 6,
                    'total_channels': 7, 'successful_posts': 8, 'posted_at': 9,
                    'created_at': 10, 'batch_id': 11, 'paused': 12
                }
                idx = key_map.get(key)
                if idx is not None and len(post) > idx:
                    return post[idx]
                return default
        except Exception as e:
            logger.error(f"Error getting {key} from post: {e}")
            return default
    
    async def send_post_to_channel(self, bot, post, channel_id):
        """Send a single post to a single channel"""
        # Check skip list
        if self.retry_system.should_skip(channel_id):
            logger.info(f"⏭️ Skipping channel {channel_id} (in skip list)")
            return False
        
        # Wait for rate limit clearance
        await self.rate_limiter.acquire(channel_id)
        
        try:
            # Get values safely
            media_type = self._get_post_value(post, 'media_type')
            media_file_id = self._get_post_value(post, 'media_file_id')
            caption = self._get_post_value(post, 'caption')
            message = self._get_post_value(post, 'message')
            
            # Send based on media type
            if media_type == 'photo':
                await bot.send_photo(
                    chat_id=channel_id,
                    photo=media_file_id,
                    caption=caption
                )
            elif media_type == 'video':
                await bot.send_video(
                    chat_id=channel_id,
                    video=media_file_id,
                    caption=caption
                )
            elif media_type == 'document':
                await bot.send_document(
                    chat_id=channel_id,
                    document=media_file_id,
                    caption=caption
                )
            else:
                await bot.send_message(
                    chat_id=channel_id,
                    text=message
                )
            
            # Report success
            self.rate_limiter.report_success()
            self.retry_system.record_success(channel_id)
            
            # Clear notification flag if channel was previously failing
            if channel_id in self.admin_notified:
                del self.admin_notified[channel_id]
            
            return True
            
        except TelegramError as e:
            error_msg = str(e).lower()
            
            # Check if it's flood control
            if 'flood' in error_msg or 'too many requests' in error_msg:
                self.rate_limiter.report_flood_control()
            
            # Record failure
            post_id = self._get_post_value(post, 'id')
            self.retry_system.record_failure(channel_id, e, post_id)
            
            failure_count = self.retry_system.consecutive_failures.get(channel_id, 0)
            
            # Notify admin with actions on first failure OR when reaching threshold
            # Notify admin on EVERY failure with increasing urgency
            if failure_count == 1:
                asyncio.create_task(self._notify_first_failure(bot, channel_id, str(e)))
            elif failure_count == 2:
                asyncio.create_task(self._notify_second_failure(bot, channel_id, str(e)))
            elif failure_count >= 3 and channel_id not in self.admin_notified:
                asyncio.create_task(self._notify_admin_with_actions(bot, channel_id, str(e), failure_count))
                self.admin_notified[channel_id] = failure_count
            
            logger.error(f"❌ Failed channel {channel_id}: {e}")
            return False
    
    async def send_batch_to_all_channels(self, bot, posts, channel_ids, db_manager, 
                                        emergency_stopped_flag=None):
        """PARALLEL+HYBRID STRATEGY for maximum speed"""
        if emergency_stopped_flag and emergency_stopped_flag():
            logger.warning("⚠️ Emergency stopped - not sending")
            return
        
        total_messages = len(posts) * len(channel_ids)
        logger.info(f"🚀 BATCH START: {len(posts)} posts × {len(channel_ids)} channels = {total_messages} messages")
        
        start_time = asyncio.get_event_loop().time()
        messages_sent = 0
        failed_sends = []
        
        # Get placeholder helper
        ph = self._ph(db_manager)
        
        # MAIN SEND: Each post to all channels in parallel
        for i, post in enumerate(posts):
            if emergency_stopped_flag and emergency_stopped_flag():
                logger.warning("⚠️ Emergency stop triggered")
                break
            
            post_id = self._get_post_value(post, 'id')
            logger.info(f"📤 Sending post {i+1}/{len(posts)} (ID: {post_id})")
            
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
                    failed_sends.append((post_id, channel_ids[idx]))
            
            # Mark post as sent with proper placeholders
            with db_manager.get_db() as conn:
                c = conn.cursor()
                c.execute(f'''
                    UPDATE posts 
                    SET posted = 1, posted_at = {ph}, successful_posts = {ph}
                    WHERE id = {ph}
                ''', (datetime.utcnow().isoformat(), successful, post_id))
                conn.commit()
            
            # Log progress
            elapsed = asyncio.get_event_loop().time() - start_time
            rate = messages_sent / elapsed if elapsed > 0 else 0
            logger.info(f"✅ Post {post_id}: {successful}/{len(channel_ids)} | Rate: {rate:.1f} msg/s")
        
        # RETRY PHASE: Retry all failed sends
        retry_success = 0
        if failed_sends and not (emergency_stopped_flag and emergency_stopped_flag()):
            logger.info(f"🔄 RETRY PHASE: {len(failed_sends)} failed sends")
            
            for post_id, channel_id in failed_sends:
                # Get post data
                with db_manager.get_db() as conn:
                    c = conn.cursor()
                    c.execute(f'SELECT * FROM posts WHERE id = {ph}', (post_id,))
                    post = c.fetchone()
                
                if post and await self.send_post_to_channel(bot, post, channel_id):
                    retry_success += 1
            
            logger.info(f"✅ Retry success: {retry_success}/{len(failed_sends)}")
        
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
    async def process_deferred_retries(self, bot, db_manager):
        """
        Process deferred retries for failed channels
        Called when bot is idle
        """
        # Get channels that failed and are ready for retry
        expired_channels = self.retry_system.get_expired_skip_channels()
        
        if not expired_channels:
            return
        
        logger.info(f"🔄 Processing {len(expired_channels)} deferred retries")
        
        # Get recent failed posts
        ph = self._ph(db_manager)
        with db_manager.get_db() as conn:
            c = conn.cursor()
            
            for channel_id in expired_channels:
                # Get last few failed posts for this channel
                c.execute(f'''
                    SELECT DISTINCT post_id FROM channel_failures 
                    WHERE channel_id = {ph}
                    ORDER BY failed_at DESC LIMIT 5
                ''', (channel_id,))
                
                failed_posts = c.fetchall()
                
                for post_row in failed_posts:
                    post_id = post_row[0]
                    
                    # Get post data
                    c.execute(f'SELECT * FROM posts WHERE id = {ph}', (post_id,))
                    post = c.fetchone()
                    
                    if post:
                        # Try to send
                        success = await self.send_post_to_channel(bot, post, channel_id)
                        if success:
                            logger.info(f"✅ Deferred retry success: post {post_id} to {channel_id}")
                        else:
                            logger.info(f"❌ Deferred retry failed: post {post_id} to {channel_id}")
                            break  # Stop retrying this channel



