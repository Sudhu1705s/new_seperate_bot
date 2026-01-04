"""
File: core/retry_system.py
Location: telegram_scheduler_bot/core/retry_system.py
Purpose: Smart retry system with error classification (IMPROVEMENT #7 & #21)
Reusable: YES - Works with any multi-channel bot
"""

from datetime import datetime
from telegram.error import TelegramError
import logging

logger = logging.getLogger(__name__)

class SmartRetrySystem:
    """
    Intelligent retry system for failed channels
    
    Features:
    - Skips failed channels during main send (exam strategy)
    - Retries all failures AFTER batch complete
    - Classifies errors (permanent vs temporary vs rate_limit)
    - Tracks failure history per channel
    - Skip list for permanently failed channels
    - Alerts after threshold failures (default: 5)
    
    IMPROVEMENT #7: Skip & retry strategy
    IMPROVEMENT #21: Smart error classification
    Reusable: YES - Copy for any multi-channel bot
    """
    
    def __init__(self, max_retries=3, alert_threshold=5):
        self.max_retries = max_retries
        self.alert_threshold = alert_threshold
        self.skip_list = set()  # Channels to skip (permanent failures)
        self.failure_history = {}  # {channel_id: [error1, error2, ...]}
        self.consecutive_failures = {}  # {channel_id: count}
    
    def classify_error(self, error: TelegramError) -> str:
        """
        Classify error type
        
        Returns:
            'permanent' - Bot kicked, channel deleted (skip immediately)
            'rate_limit' - Flood control (handled by rate limiter)
            'temporary' - Network issues (retry without penalty)
        
        IMPROVEMENT #21: Smart error classification
        """
        error_msg = str(error).lower()
        
        # Permanent errors - bot removed or channel gone
        if any(x in error_msg for x in ['bot was kicked', 'bot was blocked',
                                         'chat not found', 'user is deactivated',
                                         'channel is private']):
            return 'permanent'
        
        # Rate limit errors - handled by rate limiter
        if any(x in error_msg for x in ['flood', 'too many requests', 'retry after']):
            return 'rate_limit'
        
        # Temporary errors - network, timeout, etc.
        return 'temporary'
    
    def record_failure(self, channel_id: str, error: TelegramError, post_id: int = None):
        """
        Record a failure for a channel
        
        Args:
            channel_id: Channel that failed
            error: TelegramError that occurred
            post_id: Post ID that failed (optional)
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
        
        # Track consecutive failures (don't count temporary errors)
        if error_type != 'temporary':
            self.consecutive_failures[channel_id] = self.consecutive_failures.get(channel_id, 0) + 1
        
        # Add to skip list if permanent error
        if error_type == 'permanent':
            self.skip_list.add(channel_id)
            logger.error(f"🚫 Channel {channel_id} marked as permanently failed: {error}")
    
    def record_success(self, channel_id: str):
        """
        Record a successful send - resets failure count
        
        Args:
            channel_id: Channel that succeeded
        """
        self.consecutive_failures[channel_id] = 0
        if channel_id in self.skip_list:
            self.skip_list.remove(channel_id)
            logger.info(f"✅ Channel {channel_id} removed from skip list (success)")
    
    def should_skip(self, channel_id: str) -> bool:
        """
        Check if channel should be skipped during main send
        
        Args:
            channel_id: Channel to check
        
        Returns:
            bool: True if should skip, False if should try
        """
        return channel_id in self.skip_list
    
    def get_failed_channels(self):
        """Get list of channels with any failures"""
        return [ch for ch, count in self.consecutive_failures.items() if count > 0]
    
    def needs_alert(self, channel_id: str) -> bool:
        """
        Check if channel needs alert (reached threshold)
        
        Args:
            channel_id: Channel to check
        
        Returns:
            bool: True if needs alert (≥ threshold failures)
        
        IMPROVEMENT #8: Alert system
        """
        return self.consecutive_failures.get(channel_id, 0) >= self.alert_threshold
    
    def get_health_report(self):
        """
        Generate health report for all channels
        
        Returns:
            dict: {
                'healthy': [list of healthy channel IDs],
                'warning': [list of warning channel IDs],
                'critical': [list of critical channel IDs],
                'skip_list': [list of skipped channel IDs]
            }
        
        IMPROVEMENT #8: Channel health monitoring
        """
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
            'skip_list': list(self.skip_list)
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
            self.skip_list.remove(channel_id)
            logger.info(f"🔄 Channel {channel_id} removed from skip list")