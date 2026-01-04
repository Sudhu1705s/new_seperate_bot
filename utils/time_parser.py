"""
File: utils/time_parser.py
Location: telegram_scheduler_bot/utils/time_parser.py
Purpose: Time parsing utilities with ZERO duration and END time support
Reusable: YES - Copy for any scheduling system
IMPROVEMENTS: #1 (zero duration), #2 (end time format)
"""

from datetime import datetime, timedelta
import re
from config import get_ist_now, IST

def parse_duration_to_minutes(text):
    """
    Parse duration string to minutes
    
    Args:
        text: Duration string like "30m", "2h", "1d", "0m", "today"
    
    Returns:
        int: Duration in minutes
    
    IMPROVEMENT #1: Supports zero duration (0m, 0, now)
    
    Examples:
        "0m" or "0" or "now" → 0 (all posts at once)
        "30m" → 30
        "2h" → 120
        "1d" → 1440
        "today" → minutes until midnight
    """
    text = text.strip().lower()
    
    # IMPROVEMENT #1: Zero duration support
    if text in ['0m', '0', 'now']:
        return 0
    
    # "today" - duration until midnight
    if text == 'today':
        now = get_ist_now()
        midnight = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
        return int((midnight - now).total_seconds() / 60)
    
    # Standard duration formats
    if text[-1] == 'm':
        return int(text[:-1])
    elif text[-1] == 'h':
        return int(text[:-1]) * 60
    elif text[-1] == 'd':
        return int(text[:-1]) * 1440
    
    raise ValueError("Invalid duration format! Use: 0m, 30m, 2h, 1d, or today")

def parse_hour(text):
    """
    Parse hour from text with am/pm support
    
    Args:
        text: Hour string like "9am", "2pm", "18:00", "14"
    
    Returns:
        int: Hour in 24-hour format (0-23)
    
    Examples:
        "9am" → 9
        "2pm" → 14
        "12am" → 0
        "12pm" → 12
        "18:00" → 18
        "14" → 14
    """
    text = text.strip().lower()
    
    # Handle am/pm format
    if 'am' in text or 'pm' in text:
        hour = int(re.findall(r'\d+', text)[0])
        if 'pm' in text and hour != 12:
            hour += 12
        if 'am' in text and hour == 12:
            hour = 0
        return hour
    
    # Handle HH:MM format
    if ':' in text:
        return int(text.split(':')[0])
    
    # Plain number
    return int(text)

def parse_user_time_input(text):
    """
    Parse user time input to IST datetime
    
    Args:
        text: Time string in various formats
    
    Returns:
        datetime: IST naive datetime
    
    Supported formats:
        - "now", "0m", "0" → current time
        - "30m", "2h", "1d" → relative time
        - "today 18:00", "today 6pm" → today at specific time
        - "tomorrow 9am" → tomorrow at specific time
        - "2026-01-31 20:00" → exact date and time
        - "12/31 20:00" → month/day this year
    
    Examples:
        "now" → current IST time
        "30m" → 30 minutes from now
        "today 18:00" → today at 6 PM IST
        "tomorrow 9am" → tomorrow at 9 AM IST
        "2026-01-31 20:00" → Jan 31, 2026 at 8 PM IST
    """
    text = text.strip().lower()
    now_ist = get_ist_now()
    
    # "now" - immediate
    if text in ['now', '0m', '0']:
        return now_ist
    
    # Duration format (30m, 2h, 1d)
    if text[-1] in ['m', 'h', 'd']:
        if text[-1] == 'm':
            return now_ist + timedelta(minutes=int(text[:-1]))
        elif text[-1] == 'h':
            return now_ist + timedelta(hours=int(text[:-1]))
        elif text[-1] == 'd':
            return now_ist + timedelta(days=int(text[:-1]))
    
    # "tomorrow" keyword
    if text.startswith('tomorrow'):
        tomorrow = now_ist + timedelta(days=1)
        time_part = text.replace('tomorrow', '').strip()
        if time_part:
            hour = parse_hour(time_part)
            return datetime.combine(tomorrow.date(), datetime.min.time()) + timedelta(hours=hour)
        return tomorrow
    
    # "today" keyword
    if text.startswith('today'):
        time_part = text.replace('today', '').strip()
        if time_part:
            hour = parse_hour(time_part)
            return datetime.combine(now_ist.date(), datetime.min.time()) + timedelta(hours=hour)
        return now_ist
    
    # Exact date-time formats
    try:
        # Format: 2026-01-31 20:00
        return datetime.strptime(text, '%Y-%m-%d %H:%M')
    except:
        pass
    
    try:
        # Format: 12/31 20:00 (assumes current year)
        dt = datetime.strptime(text, '%m/%d %H:%M')
        return dt.replace(year=now_ist.year)
    except:
        pass
    
    raise ValueError(
        "Invalid time format!\n"
        "Use: now, 30m, 2h, today 18:00, tomorrow 9am, or 2026-01-31 20:00"
    )

def calculate_duration_from_end_time(start_time_ist, end_input):
    """
    Calculate duration from start time to end time
    
    Args:
        start_time_ist: Start time in IST
        end_input: End time string (can be time format or duration format)
    
    Returns:
        int: Duration in minutes
    
    IMPROVEMENT #2: Supports end time format
    
    Examples:
        start: 2026-01-31 18:00
        end: "2026-01-31 20:00" → 120 minutes
        end: "2h" → 120 minutes
        end: "0m" → 0 minutes (all at once)
    """
    try:
        # Try parsing as end time
        end_time_ist = parse_user_time_input(end_input)
        duration_minutes = int((end_time_ist - start_time_ist).total_seconds() / 60)
        
        if duration_minutes < 0:
            raise ValueError("End time must be after start time!")
        
        return duration_minutes
    except:
        # If not a time, treat as duration
        return parse_duration_to_minutes(end_input)