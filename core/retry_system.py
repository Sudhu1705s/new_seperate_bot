"""
File: core/retry_system.py
Location: telegram_scheduler_bot/core/retry_system.py
Purpose: Smart retry system - SAFER version that doesn't skip working channels
REPLACE WITH THIS IF CHANNELS ARE BEING SKIPPED INCORRECTLY
"""

from datetime import datetime, timedelta
from telegram.error import TelegramError
import logging

logger = logging.getLogger(__name__)

class SmartRetrySystem:
    """
    Intelligent retry system - SAFER VERSION
    
    Changes from aggressive version:
    - Only skip after 3 failures (not 1)
    - Rate limit errors DON'T add to skip list
    - Temporary errors DON'T add to skip list
    - Only PERMANENT errors skip immediately
    """
    
    def __init__(self, max_retries=3, alert_threshold=5, skip_duration_minutes=5):
        self.max_retries = max_retries
        self.alert_threshold = alert_threshold
        self.skip_duration_minutes = skip_duration_minutes
        self.skip_list = {}  # {channel_id: timestamp}
        self.failure_history = {}
        self.consecutive_failures = {}
        
        logger.info(f"🔄 SmartRetrySystem (SAFE MODE) initialized: skip_duration={skip_duration_minutes}min")
    
    def classify_error(self, error: TelegramError) -> str:
        """
        Classify error type
        
        Returns:
            'permanent' - Bot kicked, channel deleted (SKIP IMMEDIATELY)
            'rate_limit' - Flood control (DON'T SKIP)
            'temporary' - Network issues (DON'T SKIP)
        """
        error_msg = str(error).lower()
        
        # Permanent errors - ONLY THESE cause immediate skip
        if any(x in error_msg for x in [
            'bot was kicked',
            'bot was blocked', 
            'chat not found',
            'user is deactivated',
            'bot is not a member',
            'forbidden: bot is not'  # More specific forbidden check
        ]):
            return 'permanent'
        
        # Rate limit errors - DON'T skip for these!
        if any(x in error_msg for x in ['flood', 'too many requests', 'retry after']):
            return 'rate_limit'
        
        # Temporary errors (network, timeout, etc.)
        return 'temporary'
    
    def record_failure(self, channel_id: str, error: TelegramError, post_id: int = None):
        """
        Record a failure
        SAFER: Only skip after 3 consecutive failures OR permanent error
        """
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
        
        # IMPORTANT: Only count permanent errors for consecutive failures
        if error_type == 'permanent':
            self.consecutive_failures[channel_id] = self.consecutive_failures.get(channel_id, 0) + 1
            # Permanent error = immediate skip
            self.skip_list[channel_id] = datetime.utcnow()
            logger.error(f"🚫 Channel {channel_id} PERMANENTLY failed - added to skip list: {error}")
        
        elif error_type == 'rate_limit':
            # Rate limit = don't increment failures, don't skip
            logger.warning(f"⚠️ Channel {channel_id} hit rate limit (NOT counting as failure): {error}")
        
        elif error_type == 'temporary':
            # Temporary error = increment but don't skip yet
            self.consecutive_failures[channel_id] = self.consecutive_failures.get(channel_id, 0) + 1
            failures = self.consecutive_failures[channel_id]
            
            # Only skip after 3 consecutive temporary failures
            if failures >= 3:
                self.skip_list[channel_id] = datetime.utcnow()
                logger.warning(f"⏸️ Channel {channel_id} has {failures} temporary failures - added to skip list for {self.skip_duration_minutes} min")
            else:
                logger.info(f"ℹ️ Channel {channel_id} temporary failure {failures}/3 (not skipping yet): {error}")
    
    def record_success(self, channel_id: str):
        """Record success - reset everything"""
        old_failures = self.consecutive_failures.get(channel_id, 0)
        
        self.consecutive_failures[channel_id] = 0
        
        if channel_id in self.skip_list:
            del self.skip_list[channel_id]
            logger.info(f"✅ Channel {channel_id} SUCCESS - removed from skip list (was {old_failures} failures)")
        elif old_failures > 0:
            logger.info(f"✅ Channel {channel_id} SUCCESS - reset {old_failures} failures")
    
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
            logger.info(f"⏰ Skip period EXPIRED for {channel_id} ({time_elapsed:.1f} min) - will retry")
            # Also reset consecutive failures when skip expires
            self.consecutive_failures[channel_id] = 0
            return False
        
        # Still in skip period
        remaining = self.skip_duration_minutes - time_elapsed
        logger.debug(f"⏭️ Skipping {channel_id} ({remaining:.1f} min remaining)")
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
        """Get channels whose skip period has expired"""
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
        cleared = len(self.skip_list)
        self.skip_list.clear()
        logger.info(f"🔄 Skip list cleared ({cleared} channels)")
    
    def remove_from_skip_list(self, channel_id: str):
        """Remove specific channel from skip list"""
        if channel_id in self.skip_list:
            del self.skip_list[channel_id]
            self.consecutive_failures[channel_id] = 0
            logger.info(f"🔄 Channel {channel_id} manually removed from skip list")
    
    def get_stats(self):
        """Get retry system statistics"""
        return {
            'skip_list_size': len(self.skip_list),
            'channels_with_failures': len([c for c in self.consecutive_failures.values() if c > 0]),
            'total_failures': sum(self.consecutive_failures.values())
        }
