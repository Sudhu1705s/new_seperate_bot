"""
File: handlers/message_handlers.py
FIXED: Properly handle button clicks by calling commands directly
"""

from telegram import Update
from telegram.ext import MessageHandler, filters, ContextTypes
from datetime import timedelta
from config import ADMIN_ID, format_time_display, utc_now, utc_to_ist, ist_to_utc
from ui.keyboards import (
    get_mode_keyboard,
    get_bulk_collection_keyboard,
    get_confirmation_keyboard,
    get_duration_keyboard,
    get_quick_time_keyboard,
    get_batch_size_keyboard,
    get_start_option_keyboard,
    get_interval_keyboard
)
from utils.helpers import extract_content
from utils.time_parser import parse_user_time_input, calculate_duration_from_end_time
from .scheduling_handlers import schedule_bulk_posts, schedule_batch_posts, schedule_auto_continuous_posts
import logging

logger = logging.getLogger(__name__)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """
    Main message handler for conversation flow
    FIXED: Properly route button clicks to command handlers
    """
    if not update.effective_user:
        return
    
    if update.effective_user.id != ADMIN_ID:
        return
    
    user_id = update.effective_user.id
    
    if user_id not in scheduler.user_sessions:
        scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
    
    session = scheduler.user_sessions[user_id]
    message_text = update.message.text if update.message.text else ""
    
    # FIXED: Handle button presses by calling commands directly
    if "📊 Stats" in message_text:
        from .command_handlers import stats_command
        await stats_command(update, context, scheduler)
        return
    
    if "📢 Channels" in message_text:
        from .command_handlers import channels_command
        await channels_command(update, context, scheduler)
        return
    
    if "📋 View" in message_text and "Pending" in message_text:
        from .command_handlers import list_posts
        await list_posts(update, context, scheduler)
        return
    
    # STEP 1: CHOOSE MODE
    if session['step'] == 'choose_mode':
        
        if "📦 Bulk" in message_text and "Auto-Space" in message_text:
            if scheduler.channels_db.get_channel_count() == 0:
                await update.message.reply_text(
                    "❌ No channels! Add channels first:\n/addchannel -1001234567890",
                    reply_markup=get_mode_keyboard()
                )
                return
            
            session['mode'] = 'bulk'
            session['step'] = 'bulk_get_start_time'
            session['posts'] = []
            
            await update.message.reply_text(
                f"📦 <b>BULK MODE (Auto-Space)</b>\n\n"
                f"🕐 Current: {format_time_display(utc_now())}\n\n"
                f"📅 <b>Step 1:</b> When should FIRST post go out?\n\n"
                f"<b>Examples:</b>\n"
                f"• now - Immediately\n"
                f"• 30m - In 30 minutes\n"
                f"• today 18:00 - Today at 6 PM\n"
                f"• tomorrow 9am - Tomorrow at 9 AM\n"
                f"• 2026-01-31 20:00 - Specific date/time",
                reply_markup=get_quick_time_keyboard(),
                parse_mode='HTML'
            )
            return
        
        elif "🎯 Bulk" in message_text and "Batches" in message_text:
            if scheduler.channels_db.get_channel_count() == 0:
                await update.message.reply_text(
                    "❌ No channels! Add channels first",
                    reply_markup=get_mode_keyboard()
                )
                return
            
            session['mode'] = 'batch'
            session['step'] = 'batch_get_start_option'
            session['posts'] = []
            
            await update.message.reply_text(
                f"🎯 <b>BATCH MODE</b>\n\n"
                f"🕐 Current: {format_time_display(utc_now())}\n\n"
                f"📅 <b>Step 1:</b> When to start?\n\n"
                f"Choose an option:",
                reply_markup=get_start_option_keyboard(),
                parse_mode='HTML'
            )
            return
        
        elif "⏱️ Auto" in message_text and "Continuous" in message_text:
            if scheduler.channels_db.get_channel_count() == 0:
                await update.message.reply_text(
                    "❌ No channels! Add channels first",
                    reply_markup=get_mode_keyboard()
                )
                return
            
            session['mode'] = 'auto'
            session['step'] = 'auto_get_start_option'
            session['posts'] = []
            
            await update.message.reply_text(
                f"⏱️ <b>AUTO-CONTINUOUS MODE</b>\n\n"
                f"🕐 Current: {format_time_display(utc_now())}\n\n"
                f"📅 <b>Step 1:</b> When to start?\n\n"
                f"Choose an option:",
                reply_markup=get_start_option_keyboard(),
                parse_mode='HTML'
            )
            return
        
        elif "❌" in message_text or "cancel" in message_text.lower():
            return
    
    # ============ BULK MODE ============
    # Replace these sections in message_handlers.py:

# ============ BATCH MODE - START OPTION ============
    if session['step'] == 'batch_get_start_option':
        # FIX: Check text more reliably
        if "Specific Time" in message_text or "specific time" in message_text.lower():
            session['step'] = 'batch_get_start_time'
            await update.message.reply_text(
                f"🕐 Current: {format_time_display(utc_now())}\n\n"
                f"📅 When should FIRST batch go out?\n\n"
                f"<b>Examples:</b>\n"
                f"• now\n"
                f"• 30m\n"
                f"• today 18:00\n"
                f"• 2026-01-31 20:00",
                reply_markup=get_quick_time_keyboard(),
                parse_mode='HTML'
            )
        elif "After Last Post" in message_text or "after last" in message_text.lower():
            # Get last scheduled post
            last_post = scheduler.posts_db.get_last_post()
            if not last_post:
                await update.message.reply_text(
                    "❌ No posts scheduled yet! Use specific time instead.",
                    reply_markup=get_start_option_keyboard()
                )
                return
            
            last_time_utc = scheduler.datetime_fromisoformat(last_post['scheduled_time'])
            # Start 5 minutes after last post
            start_utc = last_time_utc + timedelta(minutes=5)
            session['batch_start_time_utc'] = start_utc
            session['step'] = 'batch_get_duration'
            
            await update.message.reply_text(
                f"✅ Start: {format_time_display(start_utc)}\n"
                f"(5 min after last post)\n\n"
                f"⏱️ <b>Step 2:</b> Total duration for ALL batches?\n\n"
                f"• 2h - Over 2 hours\n"
                f"• 6h - Over 6 hours\n"
                f"• 2026-01-31 23:00 - Until this time",
                reply_markup=get_duration_keyboard(),
                parse_mode='HTML'
            )
        return
    
    # ============ AUTO MODE - START OPTION ============
    if session['step'] == 'auto_get_start_option':
        # FIX: Check text more reliably
        if "Specific Time" in message_text or "specific time" in message_text.lower():
            session['step'] = 'auto_get_start_time'
            await update.message.reply_text(
                f"🕐 Current: {format_time_display(utc_now())}\n\n"
                f"📅 When should FIRST batch go out?\n\n"
                f"<b>Examples:</b>\n"
                f"• now\n"
                f"• 30m\n"
                f"• today 20:00\n"
                f"• 2026-01-31 20:00",
                reply_markup=get_quick_time_keyboard(),
                parse_mode='HTML'
            )
        elif "After Last Post" in message_text or "after last" in message_text.lower():
            last_post = scheduler.posts_db.get_last_post()
            if not last_post:
                await update.message.reply_text(
                    "❌ No posts scheduled yet! Use specific time instead.",
                    reply_markup=get_start_option_keyboard()
                )
                return
            
            last_time_utc = scheduler.datetime_fromisoformat(last_post['scheduled_time'])
            start_utc = last_time_utc + timedelta(minutes=5)
            session['auto_start_time_utc'] = start_utc
            session['step'] = 'auto_get_batch_size'
            
            await update.message.reply_text(
                f"✅ Start: {format_time_display(start_utc)}\n"
                f"(5 min after last post)\n\n"
                f"📦 <b>Step 2:</b> Posts per batch?\n\n"
                f"• 10\n"
                f"• 20\n"
                f"• 50",
                reply_markup=get_batch_size_keyboard(),
                parse_mode='HTML'
            )
        return
        
        # [Rest of batch mode code remains the same...]
        # [Truncated for brevity - include full batch and auto mode from original]

def register_message_handlers(app, scheduler):
    """Register message handler"""
    app.add_handler(MessageHandler(
        filters.ALL,
        lambda u, c: handle_message(u, c, scheduler)
    ))

