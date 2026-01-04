"""
File: ui/keyboards.py
Location: telegram_scheduler_bot/ui/keyboards.py
Purpose: All keyboard layouts for the bot
Reusable: YES - Copy keyboard patterns for any bot

NOTE: 3 MODES - Bulk, Batch, and Auto-Continuous
"""

from telegram import KeyboardButton, ReplyKeyboardMarkup

def get_mode_keyboard():
    """
    Main mode selection keyboard
    
    3 MODES:
    - Bulk Posts (Auto-Space)
    - Bulk Posts (Batches)
    - Auto-Continuous (IMPROVEMENT #9)
    """
    keyboard = [
        [KeyboardButton("📦 Bulk Posts (Auto-Space)")],
        [KeyboardButton("🎯 Bulk Posts (Batches)")],
        [KeyboardButton("⏱️ Auto-Continuous Batches")],
        [KeyboardButton("📋 View Pending"), KeyboardButton("📊 Stats")],
        [KeyboardButton("📢 Channels"), KeyboardButton("❌ Cancel")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_bulk_collection_keyboard():
    """
    Keyboard shown during post collection phase
    
    Used when user is sending/forwarding posts
    """
    keyboard = [
        [KeyboardButton("✅ Done - Schedule All Posts")],
        [KeyboardButton("❌ Cancel")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_confirmation_keyboard():
    """
    Confirmation keyboard before scheduling
    
    Shows preview and asks user to confirm
    """
    keyboard = [
        [KeyboardButton("✅ Confirm & Schedule")],
        [KeyboardButton("❌ Cancel")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_duration_keyboard():
    """
    Duration selection keyboard
    
    IMPROVEMENT #1: Includes 0m option (all posts at once)
    IMPROVEMENT #2: Supports yyyy-mm-dd hh:mm format in text input
    """
    keyboard = [
        [KeyboardButton("0m"), KeyboardButton("2h"), KeyboardButton("6h")],
        [KeyboardButton("12h"), KeyboardButton("1d"), KeyboardButton("today")],
        [KeyboardButton("❌ Cancel")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_quick_time_keyboard():
    """
    Quick time selection keyboard
    
    Used for start time selection
    IMPROVEMENT #2: User can also type yyyy-mm-dd hh:mm format
    """
    keyboard = [
        [KeyboardButton("now"), KeyboardButton("30m"), KeyboardButton("1h")],
        [KeyboardButton("2h"), KeyboardButton("today 18:00")],
        [KeyboardButton("tomorrow 9am")],
        [KeyboardButton("❌ Cancel")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_batch_size_keyboard():
    """
    Batch size selection keyboard
    
    Used in batch mode to specify posts per batch
    """
    keyboard = [
        [KeyboardButton("10"), KeyboardButton("20"), KeyboardButton("30")],
        [KeyboardButton("50"), KeyboardButton("100")],
        [KeyboardButton("❌ Cancel")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_start_option_keyboard():
    """
    Start option keyboard (IMPROVEMENT #2)
    
    Choose between:
    - Specific time (user enters time)
    - After last post (auto-calculated)
    """
    keyboard = [
        [KeyboardButton("🕐 Specific Time")],
        [KeyboardButton("📅 After Last Post")],
        [KeyboardButton("❌ Cancel")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_interval_keyboard():
    """
    Interval selection keyboard (IMPROVEMENT #9)
    
    Used in auto-continuous mode for batch intervals
    """
    keyboard = [
        [KeyboardButton("30m"), KeyboardButton("1h"), KeyboardButton("2h")],
        [KeyboardButton("3h"), KeyboardButton("6h"), KeyboardButton("12h")],
        [KeyboardButton("❌ Cancel")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)