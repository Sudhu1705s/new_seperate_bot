"""
File: main.py
Location: telegram_scheduler_bot/main.py
Purpose: Main entry point for the bot
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

# Import all modules
from config import BOT_TOKEN, ADMIN_ID, INITIAL_CHANNEL_IDS
from database import DatabaseManager, PostsDB, ChannelsDB
from core import AdaptiveRateLimiter, SmartRetrySystem, ParallelSender, SchedulerCore
from handlers import register_all_handlers

async def post_init(application):
    """Initialize background tasks after bot starts"""
    scheduler = application.bot_data['scheduler']
    asyncio.create_task(scheduler.background_poster(application.bot))
    logger.info("✅ Background poster started")

def main():
    """Main entry point"""
    logger.info("="*60)
    logger.info("🚀 TELEGRAM SCHEDULER BOT v2.0")
    logger.info("="*60)
    
    # Initialize database
    db_manager = DatabaseManager()
    db_manager.init_database()
    
    # Initialize database operations
    posts_db = PostsDB(db_manager)
    channels_db = ChannelsDB(db_manager)
    
    # Add initial channels from environment
    for channel_id in INITIAL_CHANNEL_IDS:
        channels_db.add_channel(channel_id)
    
    logger.info(f"📢 Loaded {len(INITIAL_CHANNEL_IDS)} channels from environment")
    
    # Initialize core systems
    rate_limiter = AdaptiveRateLimiter()
    retry_system = SmartRetrySystem()
    sender = ParallelSender(rate_limiter, retry_system)
    
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
    
    # Register all handlers
    register_all_handlers(app, scheduler)
    
    logger.info("="*60)
    logger.info("✅ TELEGRAM SCHEDULER v2.0 STARTED")
    logger.info(f"📢 Channels: {channels_db.get_channel_count()}")
    logger.info(f"👤 Admin ID: {ADMIN_ID}")
    logger.info(f"🌍 Timezone: UTC storage, IST display")
    logger.info(f"🚀 ALL 22 IMPROVEMENTS ACTIVE")
    logger.info(f"📝 3 MODES: Bulk, Batch, Auto-Continuous + Recurring Posts")
    logger.info("="*60)
    
    # Start bot
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()