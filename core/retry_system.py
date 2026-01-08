"""
File: core/retry_system.py
Location: telegram_scheduler_bot/core/retry_system.py
Purpose: Smart retry system with TIME-BASED skip list
REPLACE YOUR ENTIRE EXISTING retry_system.py WITH THIS FILE
"""

from datetime import datetime, timedelta
from telegram.error import TelegramError
import logging

logger = logging.getLogger(__name__)

class SmartRetrySystem:
    """
    Intelligent retry system with time-based skip list
    
    Features:
    - Skip list expires after N minutes (default 5)
    - Automatic retry when skip expires
    - Smart error classification
    - Failure tracking per channel
    
    NEW: Time-based skip instead of permanent skip
    """
    
    def __init__(self, max_retries=3, alert_threshold=5, skip_duration_minutes=5):
        self.max_retries = max_retries
        self.alert_threshold = alert_threshold
        self.skip_duration_minutes = skip_duration_minutes
        self.skip_list = {}  # {channel_id: timestamp} - TIME-BASED!
        self.failure_history = {}
        self.consecutive_failures = {}
        
        logger.info(f"🔄 SmartRetrySystem initialized: skip_duration={skip_duration_minutes}min")
    
    def classify_error(self, error: TelegramError) -> str:
        """
        Classify error type
        
        Returns:
            'permanent' - Bot kicked, channel deleted
            'rate_limit' - Flood control
            'temporary' - Network issues
        """
        error_msg = str(error).lower()
        
        # Permanent errors
        if any(x in error_msg for x in ['bot was kicked', 'bot was blocked',
                                         'chat not found', 'user is deactivated',
                                         'channel is private', 'bot is not a member',
                                         'forbidden']):
            return 'permanent'
        
        # Rate limit errors
        if any(x in error_msg for x in ['flood', 'too many requests', 'retry after']):
            return 'rate_limit'
        
        # Temporary errors
        return 'temporary'
    
    def record_failure(self, channel_id: str, error: TelegramError, post_id: int = None):
        """Record a failure and add to time-based skip list"""
        error_type = self.classify_error(error)
        
        # Track in history
        if channel_id not in self.failure_history:
            self.failure_history[channel_id] = []
        
        self.failure_history[channel_id].append({
            'type': error_type,
            'msg': str(error),
            'post_id': post_id,
            'time': datetime.utcnow()
        })
        
        # Track consecutive failures (don't count temporary errors)
        if error_type != 'temporary':
            self.consecutive_failures[channel_id] = self.consecutive_failures.get(channel_id, 0) + 1
        
        # Add to skip list with timestamp
        if error_type == 'permanent' or self.consecutive_failures.get(channel_id, 0) >= 1:
            self.skip_list[channel_id] = datetime.utcnow()
            logger.warning(f"⏸️ Channel {channel_id} added to skip list for {self.skip_duration_minutes} min")
    
    def record_success(self, channel_id: str):
        """Record success - reset everything"""
        self.consecutive_failures[channel_id] = 0
        if channel_id in self.skip_list:
            del self.skip_list[channel_id]
            logger.info(f"✅ Channel {channel_id} removed from skip list (success)")
    
    def should_skip(self, channel_id: str) -> bool:
        """
        Check if channel should be skipped (with time expiry)
        Returns True if still in skip period, False if expired
        """
        if channel_id not in self.skip_list:
            return False
        
        # Check if skip period has expired
        skip_time = self.skip_list[channel_id]
        time_elapsed = (datetime.utcnow() - skip_time).total_seconds() / 60
        
        if time_elapsed >= self.skip_duration_minutes:
            # Skip period expired, remove and allow retry
            del self.skip_list[channel_id]
            logger.info(f"⏰ Skip period expired for {channel_id} ({time_elapsed:.1f} min) - will retry")
            return False
        
        # Still in skip period
        return True
    
    def get_skip_time_remaining(self, channel_id: str) -> float:
        """Get minutes remaining in skip period"""
        if channel_id not in self.skip_list:
            return 0.0
        
        skip_time = self.skip_list[channel_id]
        time_elapsed = (datetime.utcnow() - skip_time).total_seconds() / 60
        remaining = self.skip_duration_minutes - time_elapsed
        
        return max(0.0, remaining)
    
    def get_expired_skip_channels(self):
        """
        Get channels whose skip period has expired
        Returns list of channel_ids ready for retry
        """
        expired = []
        now = datetime.utcnow()
        
        for channel_id, skip_time in list(self.skip_list.items()):
            time_elapsed = (now - skip_time).total_seconds() / 60
            if time_elapsed >= self.skip_duration_minutes:
                expired.append(channel_id)
        
        return expired
    
    def get_failed_channels(self):
        """Get list of channels with any failures"""
        return [ch for ch, count in self.consecutive_failures.items() if count > 0]
    
    def needs_alert(self, channel_id: str) -> bool:
        """Check if channel needs alert (reached threshold)"""
        return self.consecutive_failures.get(channel_id, 0) >= self.alert_threshold
    
    def get_health_report(self):
        """Generate health report for all channels"""
        healthy = []
        warning = []
        critical = []
        
        for channel_id, count in self.consecutive_failures.items():
            if count == 0:
                healthy.append(channel_id)
            elif count < self.alert_threshold:
                warning.append(channel_id)
            else:
                critical.append(channel_id)
        
        return {
            'healthy': healthy,
            'warning': warning,
            'critical': critical,
            'skip_list': list(self.skip_list.keys())
        }
    
    def get_failure_details(self, channel_id: str):
        """Get detailed failure history for a channel"""
        return self.failure_history.get(channel_id, [])
    
    def clear_skip_list(self):
        """Clear the skip list (use with caution)"""
        self.skip_list.clear()
        logger.info("🔄 Skip list cleared")
    
    def remove_from_skip_list(self, channel_id: str):
        """Remove specific channel from skip list"""
        if channel_id in self.skip_list:
            del self.skip_list[channel_id]
            logger.info(f"🔄 Channel {channel_id} removed from skip list")
