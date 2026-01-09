"""
File: core/sender.py
Location: telegram_scheduler_bot/core/sender.py
Purpose: Hyper-parallel sender with smart retry deferral
REPLACE YOUR ENTIRE EXISTING sender.py WITH THIS FILE
"""

import asyncio
import time
from datetime import datetime
from telegram.error import TelegramError
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import logging

logger = logging.getLogger(__name__)

# NOTE: This class was previously called ParallelSender
# Just rename it when importing, or keep both names

class HyperParallelSender:
    """
    Ultra-fast parallel sender for 100+ channels
    
    Features:
    - True parallel sending (all tasks at once)
    - Smart retry deferral (only when idle)
    - Interactive admin notifications
    - Skip list filtering before task creation
    
    Performance: 100ch × 30 posts in ~100 seconds
    """
    
    def __init__(self, rate_limiter, retry_system, posts_db=None):
        self.rate_limiter = rate_limiter
        self.retry_system = retry_system
        self.posts_db = posts_db
        self.admin_notified = {}
        self.deferred_retries = []
    
    def _ph(self, db_manager):
        """Placeholder helper for PostgreSQL (%s) vs SQLite (?)"""
        return '%s' if db_manager.is_postgres() else '?'
    
    def _should_defer_retries(self):
        """
        Check if retries should be deferred
        Returns True if there are pending posts within next 60 seconds
        """
        if not self.posts_db:
            return False
        
        try:
            next_post_time = self.posts_db.get_next_scheduled_post()
            
            if next_post_time:
                time_until_next = (next_post_time - datetime.utcnow()).total_seconds()
                
                if time_until_next < 60:
                    logger.debug(f"⏭️ Deferring retries - next post in {time_until_next:.1f}s")
                    return True
            
            return False
        except Exception as e:
            logger.error(f"Error checking pending posts: {e}")
            return False
    
    async def _notify_admin_with_actions(self, bot, channel_id, error_message, failure_count):
        """Send interactive notification to admin"""
        from config.settings import ADMIN_ID
        
        message = f"🚨 <b>CHANNEL UNREACHABLE</b>\n\n"
        message += f"Channel: <code>{channel_id}</code>\n"
        message += f"Failures: <b>{failure_count}</b>\n"
        message += f"Error: <code>{error_message[:150]}</code>\n\n"
        
        if failure_count >= 3:
            message += "⚠️ <b>Channel added to skip list!</b>\n"
            message += "Posts will NOT be sent to this channel.\n\n"
        
        message += "❓ <b>What do you want to do?</b>"
        
        keyboard = [
            [
                InlineKeyboardButton("🧪 Test Channel", callback_data=f"test_channel:{channel_id}"),
                InlineKeyboardButton("🔄 Retry Now", callback_data=f"retry_channel:{channel_id}")
            ],
            [
                InlineKeyboardButton("🗑️ Delete Channel", callback_data=f"delete_channel:{channel_id}"),
                InlineKeyboardButton("✅ Keep & Resume", callback_data=f"resume_channel:{channel_id}")
            ],
            [
                InlineKeyboardButton("♻️ Recycle Bin", callback_data=f"recycle_channel:{channel_id}"),
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
    
    def _get_post_value(self, post, key, default=None):
        """Safely get value from post (dict or tuple)"""
        if post is None:
            return default
        
        try:
            if isinstance(post, dict):
                return post.get(key, default)
            else:
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
        if self.retry_system.should_skip(channel_id):
            return False
        
        await self.rate_limiter.acquire(channel_id)
        
        try:
            media_type = self._get_post_value(post, 'media_type')
            media_file_id = self._get_post_value(post, 'media_file_id')
            caption = self._get_post_value(post, 'caption')
            message = self._get_post_value(post, 'message')
            
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
            
            self.rate_limiter.report_success()
            self.retry_system.record_success(channel_id)
            
            if channel_id in self.admin_notified:
                del self.admin_notified[channel_id]
            
            return True
            
        except TelegramError as e:
            error_msg = str(e).lower()
            
            if 'flood' in error_msg or 'too many requests' in error_msg:
                self.rate_limiter.report_flood_control()
            
            post_id = self._get_post_value(post, 'id')
            self.retry_system.record_failure(channel_id, e, post_id)
            
            failure_count = self.retry_system.consecutive_failures.get(channel_id, 0)
            
            if failure_count == 1:
                asyncio.create_task(self._notify_first_failure(bot, channel_id, str(e)))
            elif failure_count >= 3 and channel_id not in self.admin_notified:
                asyncio.create_task(self._notify_admin_with_actions(bot, channel_id, str(e), failure_count))
                self.admin_notified[channel_id] = failure_count
            
            logger.error(f"❌ Failed channel {channel_id}: {e}")
            return False
    
    async def send_batch_to_all_channels(self, bot, posts, channel_ids, db_manager, 
                                        emergency_stopped_flag=None):
        """Hyper-parallel batch sending with smart retry deferral"""
        if emergency_stopped_flag and emergency_stopped_flag():
            logger.warning("⚠️ Emergency stopped - not sending")
            return
        
        # Reset burst tokens for new batch
        self.rate_limiter.reset_burst()
        
        total_messages = len(posts) * len(channel_ids)
        logger.info(f"🚀 BATCH START: {len(posts)} posts × {len(channel_ids)} channels = {total_messages} messages")
        
        start_time = time.time()
        messages_sent = 0
        failed_sends = []
        
        ph = self._ph(db_manager)
        
        # MAIN SEND: Each post to all channels in parallel
        for i, post in enumerate(posts):
            if emergency_stopped_flag and emergency_stopped_flag():
                logger.warning("⚠️ Emergency stop triggered")
                break
            
            post_id = self._get_post_value(post, 'id')
            logger.info(f"📤 Sending post {i+1}/{len(posts)} (ID: {post_id})")
            
            # Filter out skip-listed channels BEFORE creating tasks
            tasks = []
            active_channels = []
            for channel_id in channel_ids:
                if self.retry_system.should_skip(channel_id):
                    continue
                tasks.append(self.send_post_to_channel(bot, post, channel_id))
                active_channels.append(channel_id)
            
            # Execute all sends in parallel
            results = await asyncio.gather(*tasks)
            successful = sum(results)
            messages_sent += len(results)
            
            # Track failures for retry
            for idx, success in enumerate(results):
                if not success:
                    failed_sends.append((post_id, active_channels[idx]))
            
            # Mark post as sent
            with db_manager.get_db() as conn:
                c = conn.cursor()
                c.execute(f'''
                    UPDATE posts 
                    SET posted = 1, posted_at = {ph}, successful_posts = {ph}
                    WHERE id = {ph}
                ''', (datetime.utcnow().isoformat(), successful, post_id))
                conn.commit()
            
            # Log progress
            elapsed = time.time() - start_time
            rate = messages_sent / elapsed if elapsed > 0 else 0
            skipped_count = len(channel_ids) - len(active_channels)
            if skipped_count > 0:
                logger.info(f"✅ Post {post_id}: {successful}/{len(active_channels)} (skipped {skipped_count}) | Rate: {rate:.1f} msg/s")
            else:
                logger.info(f"✅ Post {post_id}: {successful}/{len(active_channels)} | Rate: {rate:.1f} msg/s")
        
        # SMART RETRY DECISION
        retry_success = 0
        if failed_sends and not (emergency_stopped_flag and emergency_stopped_flag()):
            
            if self._should_defer_retries():
                logger.info(f"⏸️ DEFERRING {len(failed_sends)} retries - pending posts have priority")
                for post_id, channel_id in failed_sends:
                    self.deferred_retries.append({
                        'post_id': post_id,
                        'channel_id': channel_id,
                        'timestamp': datetime.utcnow(),
                        'attempts': 0
                    })
            else:
                logger.info(f"🔄 RETRY PHASE: {len(failed_sends)} failed sends")
                
                for post_id, channel_id in failed_sends:
                    if self.retry_system.should_skip(channel_id):
                        continue
                    
                    with db_manager.get_db() as conn:
                        c = conn.cursor()
                        c.execute(f'SELECT * FROM posts WHERE id = {ph}', (post_id,))
                        post = c.fetchone()
                    
                    if post and await self.send_post_to_channel(bot, post, channel_id):
                        retry_success += 1
                
                logger.info(f"✅ Retry success: {retry_success}/{len(failed_sends)}")
        
        # Final summary
        total_time = time.time() - start_time
        final_rate = total_messages / total_time if total_time > 0 else 0
        logger.info(f"🎉 BATCH COMPLETE: {total_messages} messages in {total_time:.1f}s ({final_rate:.1f} msg/s)")
        
        return {
            'total_messages': total_messages,
            'time_taken': total_time,
            'rate': final_rate,
            'failed_count': len(failed_sends),
            'retry_success': retry_success if failed_sends else 0
        }
    
    async def process_deferred_retries(self, bot, db_manager, max_attempts=3):
        """Process deferred retries when idle"""
        if not self.deferred_retries:
            return 0
        
        if self._should_defer_retries():
            return 0
        
        logger.info(f"🔄 Processing {len(self.deferred_retries)} deferred retries")
        
        retry_success = 0
        retry_failed = 0
        ph = self._ph(db_manager)
        remaining_retries = []
        
        for retry_item in self.deferred_retries:
            post_id = retry_item['post_id']
            channel_id = retry_item['channel_id']
            attempts = retry_item['attempts']
            
            if self.retry_system.should_skip(channel_id):
                remaining_retries.append(retry_item)
                continue
            
            # FIXED: Simpler query without issues
            try:
                with db_manager.get_db() as conn:
                    c = conn.cursor()
                    c.execute(f'SELECT * FROM posts WHERE id = {ph}', (post_id,))
                    post = c.fetchone()
            except Exception as e:
                logger.error(f"Error fetching post {post_id}: {e}")
                continue
            
            if not post:
                continue
            
            success = await self.send_post_to_channel(bot, post, channel_id)
            
            if success:
                retry_success += 1
                logger.info(f"✅ Deferred retry success: {channel_id} for post {post_id}")
            else:
                retry_item['attempts'] += 1
                
                if retry_item['attempts'] >= max_attempts:
                    retry_failed += 1
                    failure_count = self.retry_system.consecutive_failures.get(channel_id, 0)
                    
                    if channel_id not in self.admin_notified:
                        asyncio.create_task(
                            self._notify_admin_with_actions(
                                bot, channel_id, 
                                f"Failed {max_attempts} retry attempts", 
                                failure_count
                            )
                        )
                        self.admin_notified[channel_id] = failure_count
                    
                    logger.error(f"❌ Deferred retry failed {max_attempts} times: {channel_id}")
                else:
                    remaining_retries.append(retry_item)
        
        self.deferred_retries = remaining_retries
        
        logger.info(f"✅ Deferred retries: {retry_success} success, {retry_failed} exhausted, {len(remaining_retries)} remaining")
        return retry_success
