# =============================================================================
# FILE 5: config/timezone_config.py
# Location: telegram_scheduler_bot/config/timezone_config.py
# Reusable: YES - Modify IST to any timezone
# =============================================================================

"""
Timezone conversion utilities (UTC ↔ IST)
Reusable: YES - Works with any timezone
"""

from datetime import datetime
import pytz

# Timezone Configuration
IST = pytz.timezone('Asia/Kolkata')
UTC = pytz.UTC

def utc_now():
    """Get current UTC time (naive datetime)"""
    return datetime.utcnow()

def ist_to_utc(ist_dt):
    """
    Convert IST naive datetime to UTC naive datetime
    
    Args:
        ist_dt: datetime object in IST (naive or aware)
    
    Returns:
        datetime: UTC naive datetime
    """
    ist_aware = IST.localize(ist_dt) if ist_dt.tzinfo is None else ist_dt
    utc_aware = ist_aware.astimezone(UTC)
    return utc_aware.replace(tzinfo=None)

def utc_to_ist(utc_dt):
    """
    Convert UTC naive datetime to IST naive datetime
    
    Args:
        utc_dt: datetime object in UTC (naive or aware)
    
    Returns:
        datetime: IST naive datetime
    """
    utc_aware = UTC.localize(utc_dt) if utc_dt.tzinfo is None else utc_dt
    ist_aware = utc_aware.astimezone(IST)
    return ist_aware.replace(tzinfo=None)

def get_ist_now():
    """Get current time in IST (naive datetime)"""
    return utc_to_ist(utc_now())

def format_time_display(utc_dt, show_utc=True):
    """
    Format datetime for user display
    
    Args:
        utc_dt: UTC datetime
        show_utc: Whether to show UTC time alongside IST
    
    Returns:
        str: Formatted time string
    """
    ist_dt = utc_to_ist(utc_dt)
    ist_str = ist_dt.strftime('%Y-%m-%d %H:%M IST')
    
    if show_utc:
        utc_str = utc_dt.strftime('(%H:%M UTC)')
        return f"{ist_str} {utc_str}"
    
    return ist_str