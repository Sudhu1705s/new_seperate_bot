"""
File: handlers/command_handlers.py
Location: telegram_scheduler_bot/handlers/command_handlers.py
Purpose: All command handlers (/start, /stats, /channels, etc.)
"""

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes
from config import format_time_display, utc_now, get_ist_now, ist_to_utc, ADMIN_ID
from ui.keyboards import get_mode_keyboard
from utils.validators import parse_number_range
from utils.time_parser import parse_user_time_input
import logging

logger = logging.getLogger(__name__)

# Command handlers will be simple wrappers that call scheduler methods

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Start command - shows main menu"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    user_id = update.effective_user.id
    scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
    
    stats = scheduler.posts_db.get_database_stats()
    
    status = "🟢 RUNNING" if not scheduler.emergency_stopped else "🔴 STOPPED"
    
    await update.message.reply_text(
        f"🤖 <b>Telegram Scheduler v2.0</b>\n\n"
        f"{status}\n"
        f"🕐 {format_time_display(utc_now())}\n"
        f"📢 Channels: {scheduler.channels_db.get_channel_count()}\n"
        f"📊 Pending: {stats['pending']} | DB: {stats['db_size_mb']:.2f} MB\n\n"
        f"<b>Choose a mode:</b>",
        reply_markup=get_mode_keyboard(),
        parse_mode='HTML'
    )

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Enhanced stats command (IMPROVEMENT #16)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    stats = scheduler.posts_db.get_database_stats()
    health = scheduler.retry_system.get_health_report()
    
    response = "📊 <b>ENHANCED STATISTICS</b>\n\n"
    response += f"🕐 {format_time_display(utc_now())}\n\n"
    response += f"📦 Total Posts: <b>{stats['total']}</b>\n"
    response += f"⏳ Pending: <b>{stats['pending']}</b>\n"
    response += f"✅ Posted: <b>{stats['posted']}</b>\n"
    response += f"💾 Database: <b>{stats['db_size_mb']:.2f} MB</b>\n\n"
    response += f"📢 <b>Channel Health:</b>\n"
    response += f"✅ Healthy: {len(health['healthy'])}\n"
    response += f"⚠️ Warning: {len(health['warning'])}\n"
    response += f"❌ Critical: {len(health['critical'])}\n"
    response += f"🚫 Skip List: {len(health['skip_list'])}\n\n"
    
    if scheduler.emergency_stopped:
        response += "🔴 <b>EMERGENCY STOPPED</b>\n\n"
    
    await update.message.reply_text(response, reply_markup=get_mode_keyboard(), parse_mode='HTML')

async def channels_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Show numbered channel list (IMPROVEMENT #4)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    channels = scheduler.channels_db.get_all_channels()
    
    if not channels:
        await update.message.reply_text(
            "📢 <b>No channels!</b>\n\nUse /addchannel to add channels",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )
        return
    
    response = f"📢 <b>CHANNELS ({len(channels)} total)</b>\n\n"
    
    active_count = 0
    for idx, channel in enumerate(channels, 1):
        if channel['active']:
            name = channel['channel_name'] or "Unnamed"
            response += f"#{idx} ✅ <code>{channel['channel_id']}</code>\n"
            response += f"     📝 {name}\n\n"
            active_count += 1
    
    response += f"<b>Active:</b> {active_count}\n\n"
    response += "<b>Commands:</b>\n"
    response += "• /addchannel [id] [name]\n"
    response += "• /deletechannel 5 (single)\n"
    response += "• /deletechannel 5-10 (range)\n"
    response += "• /deletechannel all confirm\n"
    response += "• /exportchannels\n"
    response += "• /channelhealth\n"
    response += "• /test 5\n"
    
    await update.message.reply_text(response, reply_markup=get_mode_keyboard(), parse_mode='HTML')

async def add_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Add channel command with multi-command support (IMPROVEMENT #3)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not context.args:
        # Check if multi-command paste
        if update.message.text and '\n' in update.message.text:
            added, failed = scheduler.channels_db.add_channels_bulk(update.message.text)
            await update.message.reply_text(
                f"✅ <b>Bulk Import Complete!</b>\n\n"
                f"✅ Added: {added}\n"
                f"❌ Failed: {failed}\n"
                f"📊 Total: {scheduler.channels_db.get_channel_count()} channels",
                reply_markup=get_mode_keyboard(),
                parse_mode='HTML'
            )
            return
        
        await update.message.reply_text(
            "❌ <b>Usage:</b>\n\n"
            "<code>/addchannel -1001234567890 Channel Name</code>\n\n"
            "<b>Or paste multiple:</b>\n"
            "<code>/addchannel -100111 Ch1\n/addchannel -100222 Ch2</code>",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )
        return
    
    channel_id = context.args[0]
    channel_name = " ".join(context.args[1:]) if len(context.args) > 1 else None
    
    if scheduler.channels_db.add_channel(channel_id, channel_name):
        await update.message.reply_text(
            f"✅ <b>Channel Added!</b>\n\n"
            f"📢 ID: <code>{channel_id}</code>\n"
            f"📝 Name: {channel_name or 'Unnamed'}\n"
            f"📊 Total: <b>{scheduler.channels_db.get_channel_count()}</b>",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )

async def remove_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Delete channel by number/range/all (IMPROVEMENT #4)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not context.args:
        await update.message.reply_text(
            "❌ <b>Usage:</b>\n\n"
            "<code>/deletechannel 5</code>\n"
            "<code>/deletechannel 5-10</code>\n"
            "<code>/deletechannel all confirm</code>",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )
        return
    
    arg = context.args[0]
    
    if arg.lower() == 'all':
        if len(context.args) < 2 or context.args[1].lower() != 'confirm':
            await update.message.reply_text(
                f"⚠️ <b>Delete ALL {scheduler.channels_db.get_channel_count()} channels?</b>\n\n"
                f"To confirm:\n<code>/deletechannel all confirm</code>",
                reply_markup=get_mode_keyboard(),
                parse_mode='HTML'
            )
            return
        
        # FIXED: Pass confirm parameter (IMPROVEMENT #4)
        deleted = scheduler.channels_db.remove_all_channels(confirm='confirm')
        await update.message.reply_text(
            f"✅ <b>Deleted {deleted} channels!</b>",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )
        return
    
    try:
        numbers = parse_number_range(arg)
        deleted = scheduler.channels_db.remove_channels_by_numbers(numbers)
        
        await update.message.reply_text(
            f"✅ <b>Deleted {deleted} channels!</b>\n\n"
            f"📊 Remaining: <b>{scheduler.channels_db.get_channel_count()}</b>",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Invalid format: {e}", reply_markup=get_mode_keyboard())

async def export_channels_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Export channels for backup (IMPROVEMENT #3 - uses DB method)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    # FIXED: Use new DB method (IMPROVEMENT #3)
    commands = scheduler.channels_db.export_channels_as_commands()
    
    if not commands:
        await update.message.reply_text("No channels!", reply_markup=get_mode_keyboard())
        return
    
    export_text = "📋 <b>CHANNEL BACKUP</b>\n\n"
    export_text += "Copy and paste to restore:\n\n"
    export_text += "<code>" + "\n".join(commands) + "</code>\n\n"
    export_text += f"📊 Total: {len(commands)} channels"
    
    await update.message.reply_text(export_text, parse_mode='HTML', reply_markup=get_mode_keyboard())

async def channelhealth_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Channel health report (IMPROVEMENT #8)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    health = scheduler.retry_system.get_health_report()
    
    response = "📊 <b>CHANNEL HEALTH REPORT</b>\n\n"
    response += f"✅ Healthy: {len(health['healthy'])} channels\n"
    response += f"⚠️ Warning: {len(health['warning'])} channels\n"
    response += f"❌ Critical: {len(health['critical'])} channels\n"
    response += f"🚫 Skip List: {len(health['skip_list'])} channels\n\n"
    
    if health['critical']:
        response += "<b>Critical Channels:</b>\n"
        for ch in health['critical'][:5]:
            failures = scheduler.retry_system.consecutive_failures.get(ch, 0)
            response += f"• <code>{ch}</code> ({failures} failures)\n"
    
    if health['skip_list']:
        response += "\n<b>Skip List:</b>\n"
        for ch in health['skip_list'][:5]:
            response += f"• <code>{ch}</code>\n"
    
    await update.message.reply_text(response, parse_mode='HTML', reply_markup=get_mode_keyboard())

async def test_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Test single channel (IMPROVEMENT #18)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /test 5", reply_markup=get_mode_keyboard())
        return
    
    try:
        num = int(context.args[0])
        channel_id = scheduler.channels_db.get_channel_by_number(num)
        
        if not channel_id:
            await update.message.reply_text(f"❌ Channel #{num} not found", reply_markup=get_mode_keyboard())
            return
        
        try:
            await context.bot.send_message(
                chat_id=channel_id,
                text=f"🧪 Test message\n{format_time_display(utc_now())}"
            )
            await update.message.reply_text(
                f"✅ Channel #{num} reachable!\n<code>{channel_id}</code>",
                reply_markup=get_mode_keyboard(),
                parse_mode='HTML'
            )
        except Exception as e:
            await update.message.reply_text(
                f"❌ Channel #{num} failed!\n<code>{channel_id}</code>\n\nError: {e}",
                reply_markup=get_mode_keyboard(),
                parse_mode='HTML'
            )
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}", reply_markup=get_mode_keyboard())

async def list_posts(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """List pending posts (IMPROVEMENT #5)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    posts = scheduler.posts_db.get_pending_posts()
    
    if not posts:
        await update.message.reply_text("✅ No pending posts!", reply_markup=get_mode_keyboard())
        return
    
    response = f"📋 <b>Pending Posts ({len(posts)} total)</b>\n\n"
    
    for idx, post in enumerate(posts[:20], 1):
        scheduled_utc = scheduler.datetime_fromisoformat(post['scheduled_time'])
        content = post['message'] or post['caption'] or f"[{post['media_type']}]"
        preview = content[:30] + "..." if len(content) > 30 else content
        
        response += f"#{idx} | {format_time_display(scheduled_utc, show_utc=False)}\n"
        response += f"    {preview}\n\n"
    
    if len(posts) > 20:
        response += f"<i>...and {len(posts) - 20} more</i>\n\n"
    
    response += "<b>Commands:</b>\n"
    response += "• /deletepost 5\n"
    response += "• /deletepost 5-10\n"
    response += "• /movepost 5 20:00\n"
    response += "• /movepost 5-10 20:00"
    
    await update.message.reply_text(response, parse_mode='HTML', reply_markup=get_mode_keyboard())

async def delete_post_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Delete posts by number (IMPROVEMENT #5)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not context.args:
        await update.message.reply_text(
            "Usage:\n/deletepost 5\n/deletepost 5-10\n/deletepost all confirm",
            reply_markup=get_mode_keyboard()
        )
        return
    
    arg = context.args[0]
    
    if arg.lower() == 'all':
        if len(context.args) < 2 or context.args[1].lower() != 'confirm':
            pending_count = len(scheduler.posts_db.get_pending_posts())
            await update.message.reply_text(
                f"⚠️ Delete ALL {pending_count} posts?\n\n/deletepost all confirm",
                reply_markup=get_mode_keyboard()
            )
            return
        
        # FIXED: Pass confirm parameter (IMPROVEMENT #5)
        deleted = scheduler.posts_db.delete_all_pending(confirm='confirm')
        await update.message.reply_text(f"✅ Deleted {deleted} posts!", reply_markup=get_mode_keyboard())
        return
    
    try:
        numbers = parse_number_range(arg)
        deleted = scheduler.posts_db.delete_posts_by_numbers(numbers)
        await update.message.reply_text(f"✅ Deleted {deleted} posts!", reply_markup=get_mode_keyboard())
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}", reply_markup=get_mode_keyboard())

async def movepost_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Move posts (IMPROVEMENT #6)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage:\n/movepost 5 20:00\n/movepost 5-10 tomorrow 9am",
            reply_markup=get_mode_keyboard()
        )
        return
    
    try:
        numbers = parse_number_range(context.args[0])
        time_input = " ".join(context.args[1:])
        
        new_time_ist = parse_user_time_input(time_input)
        new_time_utc = ist_to_utc(new_time_ist)
        
        moved = scheduler.posts_db.move_posts_by_numbers(numbers, new_time_utc)
        
        await update.message.reply_text(
            f"✅ Moved {moved} posts to\n{format_time_display(new_time_utc)}",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}", reply_markup=get_mode_keyboard())

async def lastpost_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Show last post (IMPROVEMENT #12)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    post = scheduler.posts_db.get_last_post()
    
    if not post:
        await update.message.reply_text("No pending posts!", reply_markup=get_mode_keyboard())
        return
    
    scheduled_utc = scheduler.datetime_fromisoformat(post['scheduled_time'])
    content = post['message'] or post['caption'] or f"[{post['media_type']}]"
    
    response = "📋 <b>LAST POST</b>\n\n"
    response += f"📅 {format_time_display(scheduled_utc)}\n"
    response += f"📝 {content[:100]}\n\n"
    response += "💡 Schedule next post after this time"
    
    await update.message.reply_text(response, parse_mode='HTML', reply_markup=get_mode_keyboard())

async def lastpostbatch_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Show last batch (IMPROVEMENT #12)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    batch = scheduler.posts_db.get_last_batch()
    
    if not batch:
        await update.message.reply_text("No batches found!", reply_markup=get_mode_keyboard())
        return
    
    first_time = scheduler.datetime_fromisoformat(batch[0]['scheduled_time'])
    
    response = f"📋 <b>LAST BATCH</b>\n\n"
    response += f"📅 {format_time_display(first_time)}\n"
    response += f"📦 {len(batch)} posts\n\n"
    response += f"💡 Next batch should start after this"
    
    await update.message.reply_text(response, parse_mode='HTML', reply_markup=get_mode_keyboard())

async def stopall_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Emergency stop (IMPROVEMENT #15)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    scheduler.emergency_stopped = True
    
    await update.message.reply_text(
        "🔴 <b>EMERGENCY STOP ACTIVATED</b>\n\n"
        "All posting stopped!\n\nUse /resumeall to resume",
        reply_markup=get_mode_keyboard(),
        parse_mode='HTML'
    )

async def resumeall_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Resume operations (IMPROVEMENT #15)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    scheduler.emergency_stopped = False
    
    await update.message.reply_text(
        "🟢 <b>RESUMED</b>\n\nBot is back online!",
        reply_markup=get_mode_keyboard(),
        parse_mode='HTML'
    )

async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Reset all data (IMPROVEMENT #20)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not context.args or context.args[0].lower() != 'confirm':
        stats = scheduler.posts_db.get_database_stats()
        await update.message.reply_text(
            f"⚠️ <b>RESET ALL DATA?</b>\n\n"
            f"This will delete:\n"
            f"• {scheduler.channels_db.get_channel_count()} channels\n"
            f"• {stats['pending']} pending posts\n\n"
            f"To confirm:\n<code>/reset confirm</code>",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )
        return
    
    # FIXED: Pass confirm parameter (IMPROVEMENT #4 & #5)
    deleted_channels = scheduler.channels_db.remove_all_channels(confirm='confirm')
    deleted_posts = scheduler.posts_db.delete_all_pending(confirm='confirm')
    
    await update.message.reply_text(
        f"✅ <b>RESET COMPLETE</b>\n\n"
        f"Deleted {deleted_channels} channels and {deleted_posts} posts!",
        reply_markup=get_mode_keyboard(),
        parse_mode='HTML'
    )

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Cancel current operation"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    user_id = update.effective_user.id
    scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
    await update.message.reply_text("❌ Cancelled", reply_markup=get_mode_keyboard())

"""
Add these command handlers to command_handlers.py

IMPORTANT: Add after the existing commands
"""

async def recurring_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """
    Create recurring post
    
    Usage:
        /recurring daily 9am Good morning!
        /recurring weekly monday 18:00 Weekly report
        /recurring monthly 1 12:00 Monthly update
    """
    if update.effective_user.id != ADMIN_ID:
        return
    
    if len(context.args) < 3:
        await update.message.reply_text(
            "❌ <b>Usage:</b>\n\n"
            "<code>/recurring daily HH:MM message</code>\n"
            "<code>/recurring weekly DAY HH:MM message</code>\n"
            "<code>/recurring monthly DATE HH:MM message</code>\n\n"
            "<b>Examples:</b>\n"
            "• /recurring daily 9am Good morning!\n"
            "• /recurring weekly monday 18:00 Weekly report\n"
            "• /recurring monthly 1 12:00 Monthly update",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )
        return
    
    pattern = context.args[0].lower()
    
    try:
        if pattern == 'daily':
            time = context.args[1]
            message = " ".join(context.args[2:])
            
            # Parse time
            from utils.time_parser import parse_hour
            hour = parse_hour(time)
            time_formatted = f"{hour:02d}:00"
            
            recurring_id = scheduler.recurring_system.add_recurring_post(
                pattern='daily',
                time=time_formatted,
                message=message
            )
            
            await update.message.reply_text(
                f"✅ <b>Recurring Post Created!</b>\n\n"
                f"📅 ID: #{recurring_id}\n"
                f"🔄 Pattern: Daily at {time_formatted} IST\n"
                f"📝 Message: {message[:50]}...",
                reply_markup=get_mode_keyboard(),
                parse_mode='HTML'
            )
        
        elif pattern == 'weekly':
            day_str = context.args[1].lower()
            time = context.args[2]
            message = " ".join(context.args[3:])
            
            # Parse day of week
            days_map = {
                'monday': 0, 'mon': 0,
                'tuesday': 1, 'tue': 1,
                'wednesday': 2, 'wed': 2,
                'thursday': 3, 'thu': 3,
                'friday': 4, 'fri': 4,
                'saturday': 5, 'sat': 5,
                'sunday': 6, 'sun': 6
            }
            
            if day_str not in days_map:
                raise ValueError("Invalid day! Use: monday, tuesday, etc.")
            
            day_of_week = days_map[day_str]
            
            # Parse time
            from utils.time_parser import parse_hour
            hour = parse_hour(time)
            time_formatted = f"{hour:02d}:00"
            
            recurring_id = scheduler.recurring_system.add_recurring_post(
                pattern='weekly',
                time=time_formatted,
                message=message,
                day_of_week=day_of_week
            )
            
            days_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            
            await update.message.reply_text(
                f"✅ <b>Recurring Post Created!</b>\n\n"
                f"📅 ID: #{recurring_id}\n"
                f"🔄 Pattern: Every {days_names[day_of_week]} at {time_formatted} IST\n"
                f"📝 Message: {message[:50]}...",
                reply_markup=get_mode_keyboard(),
                parse_mode='HTML'
            )
        
        elif pattern == 'monthly':
            day_of_month = int(context.args[1])
            time = context.args[2]
            message = " ".join(context.args[3:])
            
            if not 1 <= day_of_month <= 31:
                raise ValueError("Day must be between 1-31")
            
            # Parse time
            from utils.time_parser import parse_hour
            hour = parse_hour(time)
            time_formatted = f"{hour:02d}:00"
            
            recurring_id = scheduler.recurring_system.add_recurring_post(
                pattern='monthly',
                time=time_formatted,
                message=message,
                day_of_month=day_of_month
            )
            
            await update.message.reply_text(
                f"✅ <b>Recurring Post Created!</b>\n\n"
                f"📅 ID: #{recurring_id}\n"
                f"🔄 Pattern: Every month on day {day_of_month} at {time_formatted} IST\n"
                f"📝 Message: {message[:50]}...",
                reply_markup=get_mode_keyboard(),
                parse_mode='HTML'
            )
        
        else:
            raise ValueError("Pattern must be: daily, weekly, or monthly")
    
    except Exception as e:
        await update.message.reply_text(
            f"❌ Error: {str(e)}",
            reply_markup=get_mode_keyboard()
        )

async def listrecurring_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """List all recurring posts"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    recurring_posts = scheduler.recurring_system.get_all_recurring()
    
    if not recurring_posts:
        await update.message.reply_text(
            "📅 <b>No Recurring Posts</b>\n\n"
            "Create one with:\n<code>/recurring daily 9am Message</code>",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )
        return
    
    response = f"📅 <b>RECURRING POSTS ({len(recurring_posts)} total)</b>\n\n"
    
    for rec in recurring_posts:
        status = "✅ Active" if rec['active'] else "⏸️ Paused"
        pattern_desc = scheduler.recurring_system.get_pattern_description(rec)
        message = rec['message'] or '[Media]'
        preview = message[:30] + "..." if len(message) > 30 else message
        
        response += f"#{rec['id']} {status}\n"
        response += f"🔄 {pattern_desc}\n"
        response += f"📝 {preview}\n\n"
    
    response += "<b>Commands:</b>\n"
    response += "• /pauserecurring 5 - Pause\n"
    response += "• /resumerecurring 5 - Resume\n"
    response += "• /deleterecurring 5 - Delete"
    
    await update.message.reply_text(response, reply_markup=get_mode_keyboard(), parse_mode='HTML')

async def pauserecurring_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Pause recurring post"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /pauserecurring 5", reply_markup=get_mode_keyboard())
        return
    
    try:
        recurring_id = int(context.args[0])
        scheduler.recurring_system.pause_recurring(recurring_id)
        await update.message.reply_text(
            f"⏸️ Paused recurring post #{recurring_id}",
            reply_markup=get_mode_keyboard()
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}", reply_markup=get_mode_keyboard())

async def resumerecurring_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Resume recurring post"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /resumerecurring 5", reply_markup=get_mode_keyboard())
        return
    
    try:
        recurring_id = int(context.args[0])
        scheduler.recurring_system.resume_recurring(recurring_id)
        await update.message.reply_text(
            f"▶️ Resumed recurring post #{recurring_id}",
            reply_markup=get_mode_keyboard()
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}", reply_markup=get_mode_keyboard())

async def deleterecurring_command(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Delete recurring post"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /deleterecurring 5", reply_markup=get_mode_keyboard())
        return
    
    try:
        recurring_id = int(context.args[0])
        scheduler.recurring_system.delete_recurring(recurring_id)
        await update.message.reply_text(
            f"🗑️ Deleted recurring post #{recurring_id}",
            reply_markup=get_mode_keyboard()
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}", reply_markup=get_mode_keyboard())

# ADD TO register_command_handlers() function:
"""

"""

def register_command_handlers(app, scheduler):
    """Register all command handlers"""
    app.add_handler(CommandHandler("start", lambda u, c: start(u, c, scheduler)))
    app.add_handler(CommandHandler("stats", lambda u, c: stats_command(u, c, scheduler)))
    app.add_handler(CommandHandler("channels", lambda u, c: channels_command(u, c, scheduler)))
    app.add_handler(CommandHandler("addchannel", lambda u, c: add_channel_command(u, c, scheduler)))
    app.add_handler(CommandHandler("deletechannel", lambda u, c: remove_channel_command(u, c, scheduler)))
    app.add_handler(CommandHandler("exportchannels", lambda u, c: export_channels_command(u, c, scheduler)))
    app.add_handler(CommandHandler("channelhealth", lambda u, c: channelhealth_command(u, c, scheduler)))
    app.add_handler(CommandHandler("test", lambda u, c: test_channel_command(u, c, scheduler)))
    app.add_handler(CommandHandler("list", lambda u, c: list_posts(u, c, scheduler)))
    app.add_handler(CommandHandler("deletepost", lambda u, c: delete_post_command(u, c, scheduler)))
    app.add_handler(CommandHandler("movepost", lambda u, c: movepost_command(u, c, scheduler)))
    app.add_handler(CommandHandler("lastpost", lambda u, c: lastpost_command(u, c, scheduler)))
    app.add_handler(CommandHandler("lastpostbatch", lambda u, c: lastpostbatch_command(u, c, scheduler)))
    app.add_handler(CommandHandler("stopall", lambda u, c: stopall_command(u, c, scheduler)))
    app.add_handler(CommandHandler("resumeall", lambda u, c: resumeall_command(u, c, scheduler)))
    app.add_handler(CommandHandler("reset", lambda u, c: reset_command(u, c, scheduler)))
    app.add_handler(CommandHandler("cancel", lambda u, c: cancel_command(u, c, scheduler)))
    app.add_handler(CommandHandler("recurring", lambda u, c: recurring_command(u, c, scheduler)))
    app.add_handler(CommandHandler("listrecurring", lambda u, c: listrecurring_command(u, c, scheduler)))
    app.add_handler(CommandHandler("pauserecurring", lambda u, c: pauserecurring_command(u, c, scheduler)))
    app.add_handler(CommandHandler("resumerecurring", lambda u, c: resumerecurring_command(u, c, scheduler)))
    app.add_handler(CommandHandler("deleterecurring", lambda u, c: deleterecurring_command(u, c, scheduler)))