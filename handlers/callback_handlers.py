"""
File: handlers/callback_handlers.py
Location: telegram_scheduler_bot/handlers/callback_handlers.py
Purpose: Handle callback queries from inline buttons
NEW FILE - Handle channel failure action buttons
"""

from telegram import Update
from telegram.ext import CallbackQueryHandler, ContextTypes
from config import ADMIN_ID
import logging

logger = logging.getLogger(__name__)

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE, scheduler):
    """Handle all callback queries from inline buttons"""
    query = update.callback_query
    await query.answer()
    
    if update.effective_user.id != ADMIN_ID:
        return
    
    data = query.data
    
    # Parse callback data
    if data.startswith("test_channel:"):
        channel_id = data.split(":")[1]
        await test_channel_action(query, context, scheduler, channel_id)
    
    elif data.startswith("retry_channel:"):
        channel_id = data.split(":")[1]
        await retry_channel_action(query, context, scheduler, channel_id)
    
    elif data.startswith("delete_channel:"):
        channel_id = data.split(":")[1]
        await delete_channel_action(query, context, scheduler, channel_id)
    
    elif data.startswith("resume_channel:"):
        channel_id = data.split(":")[1]
        await resume_channel_action(query, context, scheduler, channel_id)
    
    elif data.startswith("failures:"):
        channel_id = data.split(":")[1]
        await show_failures_action(query, context, scheduler, channel_id)
    
    elif data == "ignore":
        await query.edit_message_text("✅ Ignored")

async def test_channel_action(query, context, scheduler, channel_id):
    """Test if channel is reachable"""
    await query.edit_message_text(f"🧪 Testing channel <code>{channel_id}</code>...", parse_mode='HTML')
    
    try:
        await context.bot.send_message(
            chat_id=channel_id,
            text=f"🧪 Test message - Bot is working!"
        )
        
        # Success! Remove from skip list
        scheduler.channels_db.mark_channel_in_skip_list(channel_id, False)
        scheduler.retry_system.consecutive_failures[channel_id] = 0
        
        await query.edit_message_text(
            f"✅ <b>Channel Reachable!</b>\n\n"
            f"Channel: <code>{channel_id}</code>\n"
            f"Status: Working normally\n\n"
            f"✅ Removed from skip list\n"
            f"✅ Failure counter reset",
            parse_mode='HTML'
        )
    except Exception as e:
        await query.edit_message_text(
            f"❌ <b>Channel Still Unreachable</b>\n\n"
            f"Channel: <code>{channel_id}</code>\n"
            f"Error: <code>{str(e)[:150]}</code>\n\n"
            f"💡 Try:\n"
            f"• Check if bot is admin in channel\n"
            f"• Verify channel ID is correct\n"
            f"• Re-add bot to channel",
            parse_mode='HTML'
        )

async def retry_channel_action(query, context, scheduler, channel_id):
    """Retry sending to channel by removing from skip list"""
    scheduler.channels_db.mark_channel_in_skip_list(channel_id, False)
    scheduler.retry_system.consecutive_failures[channel_id] = 0
    
    await query.edit_message_text(
        f"🔄 <b>Retry Enabled</b>\n\n"
        f"Channel: <code>{channel_id}</code>\n\n"
        f"✅ Removed from skip list\n"
        f"✅ Failure counter reset\n\n"
        f"Bot will attempt to send to this channel again.",
        parse_mode='HTML'
    )

async def delete_channel_action(query, context, scheduler, channel_id):
    """Delete problematic channel"""
    if scheduler.channels_db.remove_channel(channel_id):
        await query.edit_message_text(
            f"🗑️ <b>Channel Deleted</b>\n\n"
            f"Channel: <code>{channel_id}</code>\n\n"
            f"✅ Removed from database\n"
            f"Posts will no longer be sent here.",
            parse_mode='HTML'
        )
    else:
        await query.edit_message_text(
            f"❌ Failed to delete channel <code>{channel_id}</code>",
            parse_mode='HTML'
        )
async def recycle_channel_action(query, context, scheduler, channel_id):
    """Move channel to recycle bin"""
    if scheduler.channels_db.move_to_recycle_bin(channel_id):
        scheduler.retry_system.remove_from_skip_list(channel_id)
        
        await query.edit_message_text(
            f"♻️ <b>Moved to Recycle Bin</b>\n\n"
            f"Channel: <code>{channel_id}</code>\n\n"
            f"✅ Removed from active channels\n"
            f"✅ Saved in recycle bin\n"
            f"💾 Can be restored later\n\n"
            f"Posts will no longer be sent here.",
            parse_mode='HTML'
        )
    else:
        await query.edit_message_text(
            f"❌ Failed to move channel to recycle bin",
            parse_mode='HTML'
        )
        
async def resume_channel_action(query, context, scheduler, channel_id):
    """Keep channel but remove from skip list"""
    scheduler.channels_db.mark_channel_in_skip_list(channel_id, False)
    scheduler.channels_db.record_channel_success(channel_id)
    
    await query.edit_message_text(
        f"✅ <b>Channel Resumed</b>\n\n"
        f"Channel: <code>{channel_id}</code>\n\n"
        f"✅ Kept in database\n"
        f"✅ Removed from skip list\n"
        f"✅ Failures cleared\n\n"
        f"Bot will continue sending to this channel.",
        parse_mode='HTML'
    )

async def show_failures_action(query, context, scheduler, channel_id):
    """Show detailed failure history"""
    failures = scheduler.channels_db.get_channel_failures(channel_id, limit=5)
    
    message = f"📋 <b>Failure History</b>\n\n"
    message += f"Channel: <code>{channel_id}</code>\n\n"
    
    if not failures:
        message += "No failures recorded."
    else:
        for i, failure in enumerate(failures, 1):
            # Handle both dict and tuple
            if isinstance(failure, dict):
                error_msg = failure.get('error_message', 'Unknown')
                failed_at = failure.get('failed_at', 'Unknown')
            else:
                error_msg = failure[3] if len(failure) > 3 else 'Unknown'
                failed_at = failure[4] if len(failure) > 4 else 'Unknown'
            
            message += f"{i}. {failed_at}\n"
            message += f"   └ <code>{error_msg[:80]}</code>\n\n"
    
    await query.edit_message_text(message, parse_mode='HTML')

def register_callback_handlers(app, scheduler):
    """Register callback query handlers"""
    app.add_handler(CallbackQueryHandler(
        lambda u, c: handle_callback_query(u, c, scheduler)
    ))
