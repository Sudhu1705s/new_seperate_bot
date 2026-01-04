"""
File: handlers/message_handlers.py
Location: telegram_scheduler_bot/handlers/message_handlers.py
Purpose: Message flow handlers (Bulk, Batch, and Auto-Continuous modes)
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
    3 MODES: Bulk, Batch, and Auto-Continuous
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
    
    # Handle button presses for commands
    if "📊 Stats" in message_text or "stats" in message_text.lower():
        return
    
    if "📢 Channels" in message_text or "channels" in message_text.lower():
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
        
        elif "📋 View" in message_text:
            return
        
        elif "❌" in message_text or "cancel" in message_text.lower():
            return
    
    # ============ BULK MODE ============
    elif session['mode'] == 'bulk':
        
        if "❌" in message_text or "cancel" in message_text.lower():
            user_id = update.effective_user.id
            scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
            await update.message.reply_text("❌ Cancelled", reply_markup=get_mode_keyboard())
            return
        
        if session['step'] == 'bulk_get_start_time':
            try:
                ist_time = parse_user_time_input(message_text)
                utc_time = ist_to_utc(ist_time)
                session['bulk_start_time_utc'] = utc_time
                session['step'] = 'bulk_get_duration'
                
                await update.message.reply_text(
                    f"✅ Start: {format_time_display(utc_time)}\n\n"
                    f"📏 <b>Step 2:</b> How long to space ALL posts?\n\n"
                    f"<b>IMPROVEMENT #1 & #2: Multiple formats!</b>\n"
                    f"• 0m or now - All posts at once\n"
                    f"• 2h - Over 2 hours\n"
                    f"• 6h - Over 6 hours\n"
                    f"• 2026-01-31 23:00 - Until this time",
                    reply_markup=get_duration_keyboard(),
                    parse_mode='HTML'
                )
                
            except ValueError as e:
                await update.message.reply_text(
                    f"❌ {str(e)}",
                    reply_markup=get_quick_time_keyboard()
                )
            return
        
        elif session['step'] == 'bulk_get_duration':
            try:
                start_time_ist = utc_to_ist(session['bulk_start_time_utc'])
                duration_minutes = calculate_duration_from_end_time(start_time_ist, message_text)
                
                session['duration_minutes'] = duration_minutes
                session['step'] = 'bulk_collect_posts'
                
                duration_text = "immediately (all at once)" if duration_minutes == 0 else f"{duration_minutes} minutes"
                
                await update.message.reply_text(
                    f"✅ Duration: {duration_text}\n\n"
                    f"📤 <b>Step 3:</b> Now send/forward all posts\n\n"
                    f"When done, click button:",
                    reply_markup=get_bulk_collection_keyboard(),
                    parse_mode='HTML'
                )
                
            except ValueError as e:
                await update.message.reply_text(
                    f"❌ {str(e)}",
                    reply_markup=get_duration_keyboard()
                )
            return
        
        elif session['step'] == 'bulk_collect_posts':
            
            if "✅ Done" in message_text:
                posts = session.get('posts', [])
                
                if not posts:
                    await update.message.reply_text(
                        "❌ No posts! Send at least one.",
                        reply_markup=get_bulk_collection_keyboard()
                    )
                    return
                
                session['step'] = 'bulk_confirm'
                
                duration_minutes = session['duration_minutes']
                num_posts = len(posts)
                interval = duration_minutes / num_posts if num_posts > 1 and duration_minutes > 0 else 0
                start_utc = session['bulk_start_time_utc']
                start_ist = utc_to_ist(start_utc)
                
                response = f"📋 <b>CONFIRMATION REQUIRED</b>\n\n"
                response += f"📦 Posts: <b>{num_posts}</b>\n"
                response += f"📢 Channels: <b>{scheduler.channels_db.get_channel_count()}</b>\n"
                response += f"📅 Start: {format_time_display(start_utc)}\n"
                
                if duration_minutes == 0:
                    response += f"⚡ <b>All posts at EXACT SAME TIME</b>\n"
                    response += f"(2-second delay between posts for safety)\n"
                else:
                    end_ist = start_ist + timedelta(minutes=duration_minutes)
                    response += f"📅 End: {format_time_display(ist_to_utc(end_ist))}\n"
                    response += f"⏱️ Interval: <b>{interval:.1f} min</b>\n"
                
                response += f"\n⚠️ Click <b>Confirm & Schedule</b> to proceed"
                
                await update.message.reply_text(
                    response,
                    reply_markup=get_confirmation_keyboard(),
                    parse_mode='HTML'
                )
                return
            
            content = extract_content(update.message)
            
            if content:
                session['posts'].append(content)
                count = len(session['posts'])
                await update.message.reply_text(
                    f"✅ Post #{count} added!\n\nTotal: <b>{count}</b>",
                    reply_markup=get_bulk_collection_keyboard(),
                    parse_mode='HTML'
                )
            return
        
        elif session['step'] == 'bulk_confirm':
            if "✅ Confirm" in message_text:
                await schedule_bulk_posts(update, context, scheduler)
                return
            elif "❌" in message_text:
                user_id = update.effective_user.id
                scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
                await update.message.reply_text("❌ Cancelled", reply_markup=get_mode_keyboard())
                return
    
    # ============ BATCH MODE ============
    elif session['mode'] == 'batch':
        
        if "❌" in message_text or "cancel" in message_text.lower():
            user_id = update.effective_user.id
            scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
            await update.message.reply_text("❌ Cancelled", reply_markup=get_mode_keyboard())
            return
        
        if session['step'] == 'batch_get_start_option':
            if "🕐 Specific Time" in message_text:
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
            elif "📅 After Last Post" in message_text:
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
        
        if session['step'] == 'batch_get_start_time':
            try:
                ist_time = parse_user_time_input(message_text)
                utc_time = ist_to_utc(ist_time)
                session['batch_start_time_utc'] = utc_time
                session['step'] = 'batch_get_duration'
                
                await update.message.reply_text(
                    f"✅ Start: {format_time_display(utc_time)}\n\n"
                    f"⏱️ <b>Step 2:</b> Total duration for ALL batches?\n\n"
                    f"• 2h - Over 2 hours\n"
                    f"• 6h - Over 6 hours\n"
                    f"• 2026-01-31 23:00 - Until this time",
                    reply_markup=get_duration_keyboard(),
                    parse_mode='HTML'
                )
                
            except ValueError as e:
                await update.message.reply_text(
                    f"❌ {str(e)}",
                    reply_markup=get_quick_time_keyboard()
                )
            return
        
        elif session['step'] == 'batch_get_duration':
            try:
                start_time_ist = utc_to_ist(session['batch_start_time_utc'])
                duration_minutes = calculate_duration_from_end_time(start_time_ist, message_text)
                
                session['duration_minutes'] = duration_minutes
                session['step'] = 'batch_get_batch_size'
                
                await update.message.reply_text(
                    f"✅ Duration: {duration_minutes} min\n\n"
                    f"📦 <b>Step 3:</b> Posts per batch?\n\n"
                    f"Select or type:\n"
                    f"• 10 - 10 posts per batch\n"
                    f"• 20 - 20 posts per batch\n"
                    f"• 50 - 50 posts per batch",
                    reply_markup=get_batch_size_keyboard(),
                    parse_mode='HTML'
                )
                
            except ValueError as e:
                await update.message.reply_text(
                    f"❌ {str(e)}",
                    reply_markup=get_duration_keyboard()
                )
            return
        
        elif session['step'] == 'batch_get_batch_size':
            try:
                batch_size = int(message_text.strip())
                if batch_size < 1:
                    raise ValueError("Must be at least 1")
                
                session['batch_size'] = batch_size
                session['step'] = 'batch_collect_posts'
                
                await update.message.reply_text(
                    f"✅ Batch size: <b>{batch_size} posts</b>\n\n"
                    f"📤 <b>Step 4:</b> Send/forward all posts\n\n"
                    f"Click button when done:",
                    reply_markup=get_bulk_collection_keyboard(),
                    parse_mode='HTML'
                )
                
            except ValueError:
                await update.message.reply_text(
                    "❌ Invalid! Enter a number (e.g., 10, 20, 30)",
                    reply_markup=get_batch_size_keyboard()
                )
            return
        
        elif session['step'] == 'batch_collect_posts':
            
            if "✅ Done" in message_text:
                posts = session.get('posts', [])
                
                if not posts:
                    await update.message.reply_text(
                        "❌ No posts! Send at least one.",
                        reply_markup=get_bulk_collection_keyboard()
                    )
                    return
                
                session['step'] = 'batch_confirm'
                
                duration_minutes = session['duration_minutes']
                batch_size = session['batch_size']
                num_posts = len(posts)
                num_batches = (num_posts + batch_size - 1) // batch_size
                batch_interval = duration_minutes / num_batches if num_batches > 1 else 0
                start_utc = session['batch_start_time_utc']
                start_ist = utc_to_ist(start_utc)
                
                response = f"📋 <b>CONFIRMATION REQUIRED</b>\n\n"
                response += f"📦 Total Posts: <b>{num_posts}</b>\n"
                response += f"🎯 Batch Size: <b>{batch_size} posts</b>\n"
                response += f"📊 Batches: <b>{num_batches}</b>\n"
                response += f"📢 Channels: <b>{scheduler.channels_db.get_channel_count()}</b>\n"
                response += f"📅 Start: {format_time_display(start_utc)}\n"
                response += f"⏱️ Duration: <b>{duration_minutes} min</b>\n"
                response += f"⏱️ Batch Interval: <b>{batch_interval:.1f} min</b>\n\n"
                response += "<b>Schedule Preview:</b>\n"
                
                for i in range(min(5, num_batches)):
                    batch_utc = start_utc + timedelta(minutes=batch_interval * i)
                    batch_ist = utc_to_ist(batch_utc)
                    batch_start = i * batch_size + 1
                    batch_end = min((i + 1) * batch_size, num_posts)
                    response += f"• Batch #{i+1}: {batch_ist.strftime('%H:%M')} IST - Posts #{batch_start}-{batch_end}\n"
                
                if num_batches > 5:
                    response += f"\n<i>...and {num_batches - 5} more batches</i>\n"
                
                response += f"\n⚠️ Click <b>Confirm & Schedule</b>"
                
                await update.message.reply_text(
                    response,
                    reply_markup=get_confirmation_keyboard(),
                    parse_mode='HTML'
                )
                return
            
            content = extract_content(update.message)
            
            if content:
                session['posts'].append(content)
                count = len(session['posts'])
                await update.message.reply_text(
                    f"✅ Post #{count} added!\n\nTotal: <b>{count}</b>",
                    reply_markup=get_bulk_collection_keyboard(),
                    parse_mode='HTML'
                )
            return
        
        elif session['step'] == 'batch_confirm':
            if "✅ Confirm" in message_text:
                await schedule_batch_posts(update, context, scheduler)
                return
            elif "❌" in message_text:
                user_id = update.effective_user.id
                scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
                await update.message.reply_text("❌ Cancelled", reply_markup=get_mode_keyboard())
                return
    
    # ============ AUTO-CONTINUOUS MODE (IMPROVEMENT #9) ============
    elif session['mode'] == 'auto':
        
        if "❌" in message_text or "cancel" in message_text.lower():
            user_id = update.effective_user.id
            scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
            await update.message.reply_text("❌ Cancelled", reply_markup=get_mode_keyboard())
            return
        
        if session['step'] == 'auto_get_start_option':
            if "🕐 Specific Time" in message_text:
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
            elif "📅 After Last Post" in message_text:
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
        
        if session['step'] == 'auto_get_start_time':
            try:
                ist_time = parse_user_time_input(message_text)
                utc_time = ist_to_utc(ist_time)
                session['auto_start_time_utc'] = utc_time
                session['step'] = 'auto_get_batch_size'
                
                await update.message.reply_text(
                    f"✅ Start: {format_time_display(utc_time)}\n\n"
                    f"📦 <b>Step 2:</b> Posts per batch?\n\n"
                    f"• 10\n"
                    f"• 20\n"
                    f"• 50",
                    reply_markup=get_batch_size_keyboard(),
                    parse_mode='HTML'
                )
                
            except ValueError as e:
                await update.message.reply_text(
                    f"❌ {str(e)}",
                    reply_markup=get_quick_time_keyboard()
                )
            return
        
        elif session['step'] == 'auto_get_batch_size':
            try:
                batch_size = int(message_text.strip())
                if batch_size < 1:
                    raise ValueError("Must be at least 1")
                
                session['batch_size'] = batch_size
                session['step'] = 'auto_get_interval'
                
                await update.message.reply_text(
                    f"✅ Batch size: <b>{batch_size} posts</b>\n\n"
                    f"⏱️ <b>Step 3:</b> Interval between batches?\n\n"
                    f"• 1h - Every hour\n"
                    f"• 30m - Every 30 minutes\n"
                    f"• 2h - Every 2 hours",
                    reply_markup=get_interval_keyboard(),
                    parse_mode='HTML'
                )
                
            except ValueError:
                await update.message.reply_text(
                    "❌ Invalid! Enter a number",
                    reply_markup=get_batch_size_keyboard()
                )
            return
        
        elif session['step'] == 'auto_get_interval':
            try:
                # Parse interval (e.g., "1h", "30m")
                text = message_text.strip().lower()
                if 'h' in text:
                    hours = int(text.replace('h', '').strip())
                    interval_minutes = hours * 60
                elif 'm' in text:
                    interval_minutes = int(text.replace('m', '').strip())
                else:
                    interval_minutes = int(text)
                
                if interval_minutes < 1:
                    raise ValueError("Must be at least 1 minute")
                
                session['interval_minutes'] = interval_minutes
                session['minute_mark'] = 0  # Default: no minute mark
                session['step'] = 'auto_collect_posts'
                
                await update.message.reply_text(
                    f"✅ Interval: <b>Every {interval_minutes} min</b>\n\n"
                    f"📤 <b>Step 4:</b> Send/forward all posts\n\n"
                    f"💡 Posts will auto-schedule at this interval!\n\n"
                    f"Click button when done:",
                    reply_markup=get_bulk_collection_keyboard(),
                    parse_mode='HTML'
                )
                
            except ValueError:
                await update.message.reply_text(
                    "❌ Invalid! Use: 1h, 30m, 2h",
                    reply_markup=get_interval_keyboard()
                )
            return
        
        elif session['step'] == 'auto_collect_posts':
            
            if "✅ Done" in message_text:
                posts = session.get('posts', [])
                
                if not posts:
                    await update.message.reply_text(
                        "❌ No posts! Send at least one.",
                        reply_markup=get_bulk_collection_keyboard()
                    )
                    return
                
                session['step'] = 'auto_confirm'
                
                batch_size = session['batch_size']
                interval_minutes = session['interval_minutes']
                num_posts = len(posts)
                num_batches = (num_posts + batch_size - 1) // batch_size
                start_utc = session['auto_start_time_utc']
                
                response = f"📋 <b>CONFIRMATION REQUIRED</b>\n\n"
                response += f"📦 Posts: <b>{num_posts}</b>\n"
                response += f"🎯 Batch Size: <b>{batch_size} posts</b>\n"
                response += f"📊 Batches: <b>{num_batches}</b>\n"
                response += f"⏱️ Interval: <b>Every {interval_minutes} min</b>\n"
                response += f"📅 First: {format_time_display(start_utc)}\n\n"
                
                response += "<b>First 5 Batches:</b>\n"
                for i in range(min(5, num_batches)):
                    batch_time = start_utc + timedelta(minutes=interval_minutes * i)
                    batch_start = i * batch_size + 1
                    batch_end = min((i + 1) * batch_size, num_posts)
                    response += f"• {format_time_display(batch_time, show_utc=False)} - Posts #{batch_start}-{batch_end}\n"
                
                if num_batches > 5:
                    response += f"\n<i>...and {num_batches - 5} more</i>\n"
                
                response += f"\n⚠️ Click <b>Confirm & Schedule</b>"
                
                await update.message.reply_text(
                    response,
                    reply_markup=get_confirmation_keyboard(),
                    parse_mode='HTML'
                )
                return
            
            content = extract_content(update.message)
            
            if content:
                session['posts'].append(content)
                count = len(session['posts'])
                await update.message.reply_text(
                    f"✅ Post #{count} added!\n\nTotal: <b>{count}</b>",
                    reply_markup=get_bulk_collection_keyboard(),
                    parse_mode='HTML'
                )
            return
        
        elif session['step'] == 'auto_confirm':
            if "✅ Confirm" in message_text:
                await schedule_auto_continuous_posts(update, context, scheduler)
                return
            elif "❌" in message_text:
                user_id = update.effective_user.id
                scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
                await update.message.reply_text("❌ Cancelled", reply_markup=get_mode_keyboard())
                return

def register_message_handlers(app, scheduler):
    """Register message handler"""
    app.add_handler(MessageHandler(
        filters.ALL,
        lambda u, c: handle_message(u, c, scheduler)
    ))