"""
File: handlers/scheduling_handlers.py
Location: telegram_scheduler_bot/handlers/scheduling_handlers.py
Purpose: Scheduling logic functions for bulk, batch, and auto-continuous modes
"""

from datetime import timedelta
from telegram import Update
from telegram.ext import ContextTypes
from config import format_time_display, utc_to_ist, ist_to_utc
from ui.keyboards import get_mode_keyboard
import logging

logger = logging.getLogger(__name__)

async def schedule_bulk_posts(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """
    Schedule bulk posts with auto-spacing
    
    IMPROVEMENT #1: Zero duration support
    If duration is 0, all posts scheduled at same time with 2-second safety delay
    Otherwise, posts evenly spaced over duration
    """
    user_id = update.effective_user.id
    session = scheduler.user_sessions[user_id]
    
    posts = session.get('posts', [])
    duration_minutes = session['duration_minutes']
    start_utc = session['bulk_start_time_utc']
    num_posts = len(posts)
    
    batch_id = f"bulk_{start_utc.isoformat()}"
    
    # IMPROVEMENT #1: Handle zero duration
    if duration_minutes == 0:
        # All posts at same time (with 2 sec delay for safety)
        logger.info(f"📦 Scheduling {num_posts} posts at same time (zero duration)")
        for i, post in enumerate(posts):
            scheduled_utc = start_utc + timedelta(seconds=i * 2)
            scheduler.posts_db.schedule_post(
                scheduled_time_utc=scheduled_utc,
                message=post.get('message'),
                media_type=post.get('media_type'),
                media_file_id=post.get('media_file_id'),
                caption=post.get('caption'),
                batch_id=batch_id,
                total_channels=scheduler.channels_db.get_channel_count()
            )
    else:
        # Normal spacing
        interval = duration_minutes / num_posts if num_posts > 1 else 0
        logger.info(f"📦 Scheduling {num_posts} posts over {duration_minutes} min (interval: {interval:.1f} min)")
        
        for i, post in enumerate(posts):
            scheduled_utc = start_utc + timedelta(minutes=interval * i)
            scheduler.posts_db.schedule_post(
                scheduled_time_utc=scheduled_utc,
                message=post.get('message'),
                media_type=post.get('media_type'),
                media_file_id=post.get('media_file_id'),
                caption=post.get('caption'),
                batch_id=batch_id,
                total_channels=scheduler.channels_db.get_channel_count()
            )
    
    # Build response
    response = f"✅ <b>BULK SCHEDULED!</b>\n\n"
    response += f"📦 Posts: {num_posts}\n"
    response += f"📢 Channels: {scheduler.channels_db.get_channel_count()}\n"
    response += f"📅 Start: {format_time_display(start_utc)}\n"
    
    if duration_minutes == 0:
        response += f"⚡ All posts at same time (2-sec safety delay)!\n"
    else:
        interval = duration_minutes / num_posts if num_posts > 1 else 0
        response += f"⏱️ Interval: {interval:.1f} min\n"
    
    await update.message.reply_text(
        response,
        reply_markup=get_mode_keyboard(),
        parse_mode='HTML'
    )
    
    # Reset session
    scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
    logger.info(f"✅ Bulk scheduling complete for user {user_id}")

async def schedule_batch_posts(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """
    Schedule batch posts with manual/auto intervals
    
    IMPROVEMENT #2: Supports starting after last post
    IMPROVEMENT #22: Final batch all at once
    """
    user_id = update.effective_user.id
    session = scheduler.user_sessions[user_id]
    
    posts = session.get('posts', [])
    duration_minutes = session['duration_minutes']
    batch_size = session['batch_size']
    start_utc = session['batch_start_time_utc']
    num_posts = len(posts)
    num_batches = (num_posts + batch_size - 1) // batch_size
    batch_interval = duration_minutes / num_batches if num_batches > 1 else 0
    
    batch_id_base = f"batch_{start_utc.isoformat()}"
    
    logger.info(f"🎯 Scheduling {num_posts} posts in {num_batches} batches (size: {batch_size})")
    
    for i, post in enumerate(posts):
        batch_number = i // batch_size
        post_in_batch = i % batch_size
        batch_id = f"{batch_id_base}_b{batch_number}"
        
        # IMPROVEMENT #22: Final batch all at once
        is_final_batch = (batch_number == num_batches - 1)
        posts_in_final_batch = num_posts % batch_size if num_posts % batch_size != 0 else batch_size
        
        if is_final_batch and posts_in_final_batch < batch_size:
            # Final batch with fewer posts - send all at same time
            scheduled_utc = start_utc + timedelta(minutes=batch_interval * batch_number)
            logger.debug(f"Final batch post {i+1}: all at {scheduled_utc}")
        else:
            # Normal batch - 2 sec delay between posts
            scheduled_utc = start_utc + timedelta(
                minutes=batch_interval * batch_number,
                seconds=post_in_batch * 2
            )
            logger.debug(f"Batch {batch_number+1} post {post_in_batch+1}: {scheduled_utc}")
        
        scheduler.posts_db.schedule_post(
            scheduled_time_utc=scheduled_utc,
            message=post.get('message'),
            media_type=post.get('media_type'),
            media_file_id=post.get('media_file_id'),
            caption=post.get('caption'),
            batch_id=batch_id,
            total_channels=scheduler.channels_db.get_channel_count()
        )
    
    # Build response
    start_ist = utc_to_ist(start_utc)
    end_utc = start_utc + timedelta(minutes=duration_minutes)
    end_ist = utc_to_ist(end_utc)
    
    response = f"✅ <b>BATCH SCHEDULED!</b>\n\n"
    response += f"📦 Total Posts: {num_posts}\n"
    response += f"🎯 Batch Size: {batch_size} posts\n"
    response += f"📊 Batches: {num_batches}\n"
    response += f"📢 Channels: {scheduler.channels_db.get_channel_count()}\n"
    response += f"📅 Start: {format_time_display(start_utc)}\n"
    response += f"📅 End: {format_time_display(end_utc)}\n"
    response += f"⏱️ Batch Interval: {batch_interval:.1f} min\n\n"
    
    # Show batch schedule preview
    response += "<b>Batch Schedule:</b>\n"
    for i in range(min(5, num_batches)):
        batch_time_utc = start_utc + timedelta(minutes=batch_interval * i)
        batch_time_ist = utc_to_ist(batch_time_utc)
        batch_start_post = i * batch_size + 1
        batch_end_post = min((i + 1) * batch_size, num_posts)
        posts_in_batch = batch_end_post - batch_start_post + 1
        
        response += f"• Batch #{i+1}: {batch_time_ist.strftime('%H:%M')} IST - "
        response += f"{posts_in_batch} posts (#{batch_start_post}-#{batch_end_post})\n"
    
    if num_batches > 5:
        response += f"\n<i>...and {num_batches - 5} more batches</i>\n"
    
    # Note about final batch if partial
    posts_in_final = num_posts % batch_size
    if posts_in_final != 0 and posts_in_final < batch_size:
        response += f"\n💡 Final batch has {posts_in_final} posts (will send all at once)"
    
    await update.message.reply_text(
        response,
        reply_markup=get_mode_keyboard(),
        parse_mode='HTML'
    )
    
    # Reset session
    scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
    logger.info(f"✅ Batch scheduling complete for user {user_id}")

async def schedule_auto_continuous_posts(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """
    Schedule auto-continuous batch mode (IMPROVEMENT #9)
    
    Automatically schedules posts in batches at fixed intervals
    When new posts added, they continue from next available slot
    
    Features:
    - Fixed batch size
    - Fixed interval between batches
    - Fixed minute mark (:00, :30, :45, etc.)
    - Auto-continues when more posts added
    """
    user_id = update.effective_user.id
    session = scheduler.user_sessions[user_id]
    
    posts = session.get('posts', [])
    batch_size = session['batch_size']
    interval_minutes = session['interval_minutes']
    minute_mark = session.get('minute_mark', 0)  # :00, :30, etc.
    start_utc = session['auto_start_time_utc']
    
    num_posts = len(posts)
    num_batches = (num_posts + batch_size - 1) // batch_size
    
    batch_id_base = f"auto_{start_utc.isoformat()}"
    
    logger.info(f"⏱️ Auto-continuous: {num_posts} posts in {num_batches} batches (size: {batch_size}, interval: {interval_minutes}m)")
    
    for i, post in enumerate(posts):
        batch_number = i // batch_size
        post_in_batch = i % batch_size
        batch_id = f"{batch_id_base}_b{batch_number}"
        
        # Calculate batch time with minute mark
        batch_time_utc = start_utc + timedelta(minutes=interval_minutes * batch_number)
        
        # Adjust to minute mark if specified
        if minute_mark > 0:
            batch_time_utc = batch_time_utc.replace(minute=minute_mark, second=0, microsecond=0)
        
        # Add small delay between posts in same batch
        scheduled_utc = batch_time_utc + timedelta(seconds=post_in_batch * 2)
        
        scheduler.posts_db.schedule_post(
            scheduled_time_utc=scheduled_utc,
            message=post.get('message'),
            media_type=post.get('media_type'),
            media_file_id=post.get('media_file_id'),
            caption=post.get('caption'),
            batch_id=batch_id,
            total_channels=scheduler.channels_db.get_channel_count()
        )
    
    # Build response
    response = f"✅ <b>AUTO-CONTINUOUS SCHEDULED!</b>\n\n"
    response += f"📦 Posts: {num_posts}\n"
    response += f"🎯 Batch Size: {batch_size} posts\n"
    response += f"📊 Batches: {num_batches}\n"
    response += f"⏱️ Interval: Every {interval_minutes} min\n"
    
    if minute_mark > 0:
        response += f"🕐 Minute Mark: :{minute_mark:02d}\n"
    
    response += f"📅 First Batch: {format_time_display(start_utc)}\n"
    response += f"📢 Channels: {scheduler.channels_db.get_channel_count()}\n\n"
    
    # Show first few batches
    response += "<b>Schedule Preview:</b>\n"
    for i in range(min(5, num_batches)):
        batch_time = start_utc + timedelta(minutes=interval_minutes * i)
        if minute_mark > 0:
            batch_time = batch_time.replace(minute=minute_mark, second=0, microsecond=0)
        
        batch_start = i * batch_size + 1
        batch_end = min((i + 1) * batch_size, num_posts)
        
        response += f"• Batch #{i+1}: {format_time_display(batch_time, show_utc=False)} - "
        response += f"Posts #{batch_start}-#{batch_end}\n"
    
    if num_batches > 5:
        response += f"\n<i>...and {num_batches - 5} more batches</i>\n"
    
    response += f"\n💡 Add more posts anytime - they'll auto-schedule!"
    
    await update.message.reply_text(
        response,
        reply_markup=get_mode_keyboard(),
        parse_mode='HTML'
    )
    
    # Reset session
    scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
    logger.info(f"✅ Auto-continuous scheduling complete for user {user_id}")

def register_scheduling_handlers(app, scheduler):
    """
    Register scheduling handlers
    Note: These are called by message_handlers.py, not directly registered
    """
    pass