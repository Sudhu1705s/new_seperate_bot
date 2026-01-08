"""
File: core/rate_limiter.py
Location: telegram_scheduler_bot/core/rate_limiter.py
Purpose: Balanced rate limiter - Fast but safe
REPLACE WITH THIS IF AGGRESSIVE VERSION SKIPS TOO MANY CHANNELS
"""

import asyncio
import time
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

class BalancedRateLimiter:
    """
    Balanced rate limiter - Fast but careful
    
    Features:
    - Conservative burst: 20 messages (not 50)
    - Sustained rate: 25 msg/sec (not 30)
    - Better per-channel tracking
    - More gradual slowdown on flood
    
    Performance: Still 5-10x faster than old version, but safer
    """
    
    def __init__(self):
        # Global limits - MORE CONSERVATIVE
        self.global_rate = 25  # msg/sec (safer than 30)
        self.burst_size = 20   # Smaller burst (safer than 50)
        self.burst_available = 20
        
        # Token bucket for sustained rate
        self.tokens = 25.0
        self.max_tokens = 25.0
        self.last_update = time.time()
        
        # Per-channel tracking with STRICT limits
        self.channel_last_send = defaultdict(float)
        self.channel_count_minute = defaultdict(list)
        self.channel_send_count = defaultdict(int)
        
        # Adaptive slowdown - MORE CONSERVATIVE
        self.flood_multiplier = 1.0
        self.last_flood_time = 0
        self.consecutive_successes = 0
        
        self.lock = asyncio.Lock()
        
        logger.info(f"⚡ BalancedRateLimiter initialized: {self.global_rate} msg/sec, burst: {self.burst_size}")
    
    def _refill_tokens(self):
        """Refill token bucket based on time passed"""
        now = time.time()
        elapsed = now - self.last_update
        
        # Add tokens based on rate
        self.tokens = min(
            self.max_tokens,
            self.tokens + (elapsed * self.global_rate * self.flood_multiplier)
        )
        self.last_update = now
    
    def _check_per_channel_limit(self, channel_id):
        """
        Check if sending to this channel would violate per-channel limits
        Returns: (can_send: bool, wait_time: float)
        """
        if not channel_id:
            return True, 0.0
        
        now = time.time()
        
        # Clean old entries (older than 60 seconds)
        self.channel_count_minute[channel_id] = [
            t for t in self.channel_count_minute[channel_id]
            if now - t < 60
        ]
        
        # Check per-channel rate (max 20/min = 1 per 3 seconds)
        count = len(self.channel_count_minute[channel_id])
        
        if count >= 20:
            # Already at limit, calculate wait time
            oldest = self.channel_count_minute[channel_id][0]
            wait_time = 60 - (now - oldest)
            logger.warning(f"⚠️ Channel {channel_id} at limit: {count}/20 messages, wait {wait_time:.1f}s")
            return False, wait_time
        
        if count >= 15:
            # Approaching limit, warn but allow
            logger.debug(f"⚠️ Channel {channel_id}: {count}/20 messages in last minute")
        
        return True, 0.0
    
    async def acquire(self, channel_id=None):
        """
        Acquire permission to send a message
        
        Balanced mode:
        - First 20 messages: Quick burst
        - After 20: Controlled 25 msg/sec
        - Per-channel: Strict 20/min enforcement
        """
        async with self.lock:
            now = time.time()
            
            # Check per-channel limit FIRST
            if channel_id:
                can_send, wait_time = self._check_per_channel_limit(channel_id)
                if not can_send:
                    # Must wait for per-channel limit
                    logger.warning(f"⏳ Per-channel limit hit for {channel_id}, waiting {wait_time:.1f}s")
                    await asyncio.sleep(wait_time)
                    # Re-check after wait
                    can_send, _ = self._check_per_channel_limit(channel_id)
                    if not can_send:
                        logger.error(f"❌ Still can't send to {channel_id} after wait!")
                        return
            
            # BURST MODE: First 20 messages go quickly (but not instantly)
            if self.burst_available > 0:
                self.burst_available -= 1
                # Small delay even in burst mode for safety (0.04s = 25 msg/sec)
                await asyncio.sleep(0.04)
                if self.burst_available % 5 == 0:
                    logger.debug(f"⚡ BURST: {self.burst_available} burst tokens remaining")
            else:
                # SUSTAINED MODE: Token bucket
                self._refill_tokens()
                
                # Wait for token if needed
                if self.tokens < 1.0:
                    wait_time = (1.0 - self.tokens) / (self.global_rate * self.flood_multiplier)
                    await asyncio.sleep(wait_time)
                    self._refill_tokens()
                
                # Consume token
                self.tokens -= 1.0
            
            # Track this send for per-channel limit
            if channel_id:
                self.channel_count_minute[channel_id].append(now)
                self.channel_send_count[channel_id] += 1
    
    def report_flood_control(self):
        """
        Called when Telegram returns flood error
        More aggressive slowdown than before
        """
        old_multiplier = self.flood_multiplier
        self.flood_multiplier = 0.5  # Reduce to 50% (was 70%)
        self.last_flood_time = time.time()
        self.burst_available = 0  # Disable burst
        self.consecutive_successes = 0
        
        logger.error(f"🚨 FLOOD CONTROL! Reducing rate from {self.global_rate * old_multiplier:.1f} to {self.global_rate * 0.5:.1f} msg/sec")
    
    def report_success(self):
        """
        Called on successful send
        Gradually restore rate if flood has passed
        """
        now = time.time()
        self.consecutive_successes += 1
        
        # If 60 seconds since last flood AND 50+ consecutive successes, restore rate
        if now - self.last_flood_time > 60 and self.flood_multiplier < 1.0:
            if self.consecutive_successes >= 50:
                self.flood_multiplier = min(1.0, self.flood_multiplier + 0.1)
                if self.flood_multiplier >= 1.0:
                    logger.info(f"✅ Rate restored to normal: {self.global_rate} msg/sec ({self.consecutive_successes} successes)")
                    self.consecutive_successes = 0
    
    def reset_burst(self):
        """Reset burst tokens (called at start of new batch)"""
        self.burst_available = self.burst_size
        logger.debug(f"🔄 Burst tokens reset: {self.burst_size} available")
    
    def get_stats(self):
        """Get current rate limiter statistics"""
        return {
            'global_rate': self.global_rate * self.flood_multiplier,
            'burst_available': self.burst_available,
            'tokens': self.tokens,
            'flood_multiplier': self.flood_multiplier,
            'consecutive_successes': self.consecutive_successes
        }
