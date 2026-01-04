"""
File: features/backup_system.py
Location: telegram_scheduler_bot/features/backup_system.py
Purpose: Live backup system with auto-updating backup file (IMPROVEMENT #11 & #14)
Reusable: YES - Copy for any bot needing state persistence
"""

import json
import os
from datetime import datetime
from config import format_time_display, utc_now, get_ist_now
import logging

logger = logging.getLogger(__name__)

class LiveBackupSystem:
    """
    Live-updating backup system
    
    Features:
    - Auto-updates backup file in Telegram chat
    - Edits same message if user hasn't sent commands
    - Creates new message if user interrupted
    - Schedules updates (20 min before posts, instant on user action)
    - Creates downloadable JSON backup
    
    IMPROVEMENT #11: Live backup system
    IMPROVEMENT #14: Auto-backup before confirmations
    """
    
    def __init__(self, bot, admin_id):
        self.bot = bot
        self.admin_id = admin_id
        self.last_backup_message_id = None
        self.last_user_message_time = None
        self.last_backup_time = None
        self.emergency_stopped = False
    
    async def create_backup_data(self, scheduler) -> dict:
        """
        Generate complete backup data dictionary
        
        Returns:
            dict: Complete state including channels, posts, settings
        """
        with scheduler.db_manager.get_db() as conn:
            c = conn.cursor()
            
            # Get all channels
            c.execute('SELECT * FROM channels')
            channels = [dict(row) for row in c.fetchall()]
            
            # Get pending posts
            c.execute('SELECT * FROM posts WHERE posted = 0 ORDER BY scheduled_time')
            pending_posts = [dict(row) for row in c.fetchall()]
            
            # Get recent completed posts (last 50)
            c.execute('SELECT * FROM posts WHERE posted = 1 ORDER BY posted_at DESC LIMIT 50')
            completed_posts = [dict(row) for row in c.fetchall()]
        
        return {
            'backup_time_utc': utc_now().isoformat(),
            'backup_time_ist': get_ist_now().isoformat(),
            'version': '2.0',
            'emergency_stopped': self.emergency_stopped,
            'channels': channels,
            'pending_posts': pending_posts,
            'completed_posts': completed_posts,
            'stats': {
                'total_channels': len(channels),
                'active_channels': len([c for c in channels if c['active']]),
                'pending_posts': len(pending_posts),
                'completed_posts': len(completed_posts)
            }
        }
    
    async def send_backup_file(self, scheduler, force_new=False):
        """
        Send or update backup file in Telegram chat
        
        Args:
            scheduler: SchedulerCore instance
            force_new: Force creation of new message (default: False)
        """
        try:
            backup_data = await self.create_backup_data(scheduler)
            json_data = json.dumps(backup_data, indent=2, default=str)
            
            filename = "backup_latest.json"
            
            # Create caption
            caption = (
                f"📎 <b>Live Backup (Auto-Updated)</b>\n\n"
                f"💾 Size: {len(json_data)/1024:.1f} KB\n"
                f"📊 {backup_data['stats']['active_channels']} channels, "
                f"{backup_data['stats']['pending_posts']} pending posts\n"
                f"🔄 {format_time_display(utc_now())}\n"
            )
            
            if self.emergency_stopped:
                caption += "\n🔴 <b>BOT IS STOPPED</b>\n"
            
            caption += "\n💡 Keep this file safe for recovery!"
            
            # Determine if we should send new message or update existing
            should_send_new = force_new or self.last_backup_message_id is None
            
            if not should_send_new and self.last_user_message_time:
                # Check if user sent command after last backup
                if self.last_backup_time and self.last_user_message_time > self.last_backup_time:
                    should_send_new = True
            
            # Write file temporarily
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(json_data)
            
            # Send to Telegram
            msg = await self.bot.send_document(
                chat_id=self.admin_id,
                document=open(filename, 'rb'),
                caption=caption,
                parse_mode='HTML'
            )
            
            # If updating, delete old message
            if self.last_backup_message_id and not should_send_new:
                try:
                    await self.bot.delete_message(
                        chat_id=self.admin_id,
                        message_id=self.last_backup_message_id
                    )
                except:
                    pass
            
            self.last_backup_message_id = msg.message_id
            os.remove(filename)
            self.last_backup_time = utc_now()
            
            logger.info(f"📎 Backup file {'sent' if should_send_new else 'updated'}")
            
        except Exception as e:
            logger.error(f"❌ Backup file error: {e}")
    
    def mark_user_action(self):
        """Mark that user sent a command (for determining when to create new backup message)"""
        self.last_user_message_time = utc_now()
    
    async def schedule_update(self, scheduler, minutes_until_next_post=None):
        """
        Schedule backup update based on context
        
        Args:
            scheduler: SchedulerCore instance
            minutes_until_next_post: Minutes until next scheduled post (optional)
        """
        # Instant update on user action
        if minutes_until_next_post is None:
            await self.send_backup_file(scheduler)
        # Update 20 min before next post
        elif minutes_until_next_post <= 20:
            await self.send_backup_file(scheduler)
    
    async def restore_from_backup(self, scheduler, backup_data: dict):
        """
        Restore channels and posts from backup file
        
        Args:
            scheduler: SchedulerCore instance
            backup_data: Backup dictionary loaded from JSON
        
        Returns:
            tuple: (restored_channels, restored_posts)
        """
        restored_channels = 0
        restored_posts = 0
        
        # Restore channels
        for channel in backup_data.get('channels', []):
            try:
                if channel.get('active'):
                    scheduler.channels_db.add_channel(
                        channel['channel_id'],
                        channel.get('channel_name')
                    )
                    restored_channels += 1
            except Exception as e:
                logger.error(f"Failed to restore channel {channel.get('channel_id')}: {e}")
        
        # Restore pending posts
        for post in backup_data.get('pending_posts', []):
            try:
                scheduler.posts_db.schedule_post(
                    scheduled_time_utc=datetime.fromisoformat(post['scheduled_time']),
                    message=post.get('message'),
                    media_type=post.get('media_type'),
                    media_file_id=post.get('media_file_id'),
                    caption=post.get('caption'),
                    batch_id=post.get('batch_id'),
                    total_channels=post.get('total_channels', 0)
                )
                restored_posts += 1
            except Exception as e:
                logger.error(f"Failed to restore post {post.get('id')}: {e}")
        
        # Restore emergency stop state
        if backup_data.get('emergency_stopped'):
            scheduler.emergency_stopped = True
            self.emergency_stopped = True
        
        logger.info(f"✅ Restored {restored_channels} channels and {restored_posts} posts from backup")
        return restored_channels, restored_posts