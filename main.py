"""
File: main.py
Location: telegram_scheduler_bot/main.py
Purpose: Main entry point for the bot
UPDATED: Uses new AggressiveRateLimiter and HyperParallelSender
"""

import os
import sys
import asyncio
import logging
from telegram.ext import Application
from telegram import Update

# Setup logging
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# UPDATED: Import new classes
from config.settings import BOT_TOKEN, ADMIN_ID, INITIAL_CHANNEL_IDS
from database.db_manager import DatabaseManager
from database.posts_db import PostsDB
from database.channels_db import ChannelsDB
from core.rate_limiter import AggressiveRateLimiter  # CHANGED!
from core.retry_system import SmartRetrySystem
from core.sender import ParallelSender  # CHANGED!
from core.scheduler_core import SchedulerCore
from handlers.command_handlers import register_command_handlers, stats_command, channels_command, list_posts
from handlers.message_handlers import register_message_handlers
from handlers.callback_handlers import register_callback_handlers

async def post_init(application):
    """Initialize background tasks after bot starts"""
    scheduler = application.bot_data['scheduler']
    
    # Start main background poster
    asyncio.create_task(scheduler.background_poster(application.bot))
    logger.info("✅ Background poster started")
    
    # REMOVED: Recurring posts checker (not yet implemented)
    # asyncio.create_task(scheduler.recurring_system.check_and_schedule_recurring(application.bot))
    # logger.info("✅ Recurring posts checker started")

def main():
    """Main entry point"""
    logger.info("="*60)
    logger.info("🚀 TELEGRAM SCHEDULER BOT v2.0 - ULTRA-FAST EDITION")
    logger.info("="*60)
    
    # Initialize database
    db_manager = DatabaseManager()
    db_manager.init_database()
    
    # Initialize database operations
    posts_db = PostsDB(db_manager)
    channels_db = ChannelsDB(db_manager)
    
    # Add initial channels from environment (if any)
    if INITIAL_CHANNEL_IDS:
        for channel_id in INITIAL_CHANNEL_IDS:
            if channel_id:  # Check if not empty
                channels_db.add_channel(channel_id)
        logger.info(f"📢 Loaded {len(INITIAL_CHANNEL_IDS)} channels from environment")
    else:
        logger.info("📢 No initial channels in environment")
    
    # Initialize core systems with NEW classes
    rate_limiter = AggressiveRateLimiter()  # CHANGED!
    retry_system = SmartRetrySystem(skip_duration_minutes=3)  # UPDATED with parameter!
    sender = ParallelSender(rate_limiter, retry_system)  # CHANGED! Added posts_db
    
    # Initialize scheduler core
    scheduler = SchedulerCore(
        db_manager=db_manager,
        posts_db=posts_db,
        channels_db=channels_db,
        rate_limiter=rate_limiter,
        retry_system=retry_system,
        sender=sender
    )
    
    # Create Telegram application
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    
    # Store scheduler in bot_data for access in handlers
    app.bot_data['scheduler'] = scheduler
    
    # Register handlers
    register_command_handlers(app, scheduler)
    register_message_handlers(app, scheduler)
    register_callback_handlers(app, scheduler)  # NEW: Handle button clicks
    
    logger.info("="*60)
    logger.info("✅ TELEGRAM SCHEDULER v2.0 ULTRA-FAST STARTED")
    logger.info(f"📢 Channels: {channels_db.get_channel_count()}")
    logger.info(f"👤 Admin ID: {ADMIN_ID}")
    logger.info(f"🌐 Timezone: UTC storage, IST display")
    logger.info(f"⚡ Rate Limiter: Aggressive (30 msg/sec, burst 50)")
    logger.info(f"🔄 Retry System: Time-based skip (5 min expiry)")
    logger.info(f"🚀 Sender: Hyper-parallel mode")
    logger.info(f"🚀 3 MODES: Bulk, Batch, Auto-Continuous")
    logger.info(f"⚠️ Recurring posts: Not yet implemented")
    logger.info("="*60)
    
    # Start bot
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
