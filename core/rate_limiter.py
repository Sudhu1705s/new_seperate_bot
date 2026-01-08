"""
File: core/rate_limiter.py
Location: telegram_scheduler_bot/core/rate_limiter.py
Purpose: Ultra-fast rate limiter with burst mode
REPLACE YOUR ENTIRE EXISTING rate_limiter.py WITH THIS FILE
"""

import asyncio
import time
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

class AggressiveRateLimiter:
    """
    High-performance rate limiter for 100+ channels
    
    Features:
    - Burst mode: First 50 messages instant
    - Token bucket: 30 msg/sec sustained
    - Per-channel tracking without blocking
    - Adaptive slowdown only on actual flood errors
    
    Performance:
    - 100 channels × 30 posts = ~100 seconds (vs 50 minutes before)
    """
    
    def __init__(self):
        # Global limits
        self.global_rate = 30  # msg/sec (Telegram's actual limit)
        self.burst_size = 50   # First N messages instant
        self.burst_available = 50
        
        # Token bucket for sustained rate
        self.tokens = 30.0
        self.max_tokens = 30.0
        self.last_update = time.time()
        
        # Per-channel tracking (don't block, just warn)
        self.channel_last_send = defaultdict(float)
        self.channel_count_minute = defaultdict(list)
        
        # Adaptive slowdown
        self.flood_multiplier = 1.0  # Start normal
        self.last_flood_time = 0
        
        self.lock = asyncio.Lock()
        
        logger.info(f"⚡ AggressiveRateLimiter initialized: {self.global_rate} msg/sec, burst: {self.burst_size}")
    
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
    
    async def acquire(self, channel_id=None):
        """
        Acquire permission to send a message
        
        Ultra-fast mode:
        - First 50 messages: Instant (0 delay)
        - After 50: Token bucket with 30 msg/sec
        """
        async with self.lock:
            now = time.time()
            
            # BURST MODE: First 50 messages go instantly
            if self.burst_available > 0:
                self.burst_available -= 1
                if self.burst_available % 10 == 0:
                    logger.debug(f"⚡ BURST: {self.burst_available} burst tokens remaining")
                return  # NO DELAY!
            
            # SUSTAINED MODE: Token bucket
            self._refill_tokens()
            
            # Wait for token if needed
            if self.tokens < 1.0:
                wait_time = (1.0 - self.tokens) / (self.global_rate * self.flood_multiplier)
                await asyncio.sleep(wait_time)
                self._refill_tokens()
            
            # Consume token
            self.tokens -= 1.0
            
            # Per-channel tracking (don't block, just log)
            if channel_id:
                # Clean old entries (older than 60 seconds)
                self.channel_count_minute[channel_id] = [
                    t for t in self.channel_count_minute[channel_id]
                    if now - t < 60
                ]
                
                # Add this send
                self.channel_count_minute[channel_id].append(now)
                
                # Warn if approaching per-channel limit (20/min)
                count = len(self.channel_count_minute[channel_id])
                if count >= 18:
                    logger.warning(f"⚠️ Channel {channel_id}: {count}/20 messages in last minute")
    
    def report_flood_control(self):
        """
        Called when Telegram returns flood error
        Temporarily reduce rate by 30%
        """
        self.flood_multiplier = 0.7
        self.last_flood_time = time.time()
        self.burst_available = 0  # Disable burst
        logger.warning(f"⚠️ FLOOD CONTROL! Reducing rate to {self.global_rate * 0.7:.1f} msg/sec")
    
    def report_success(self):
        """
        Called on successful send
        Gradually restore rate if flood has passed
        """
        now = time.time()
        
        # If 60 seconds since last flood, restore normal rate
        if now - self.last_flood_time > 60 and self.flood_multiplier < 1.0:
            old_multiplier = self.flood_multiplier
            self.flood_multiplier = min(1.0, self.flood_multiplier + 0.1)
            if self.flood_multiplier >= 1.0:
                logger.info(f"✅ Rate restored to normal: {self.global_rate} msg/sec")
    
    def reset_burst(self):
        """Reset burst tokens (called at start of new batch)"""
        self.burst_available = self.burst_size
        logger.debug(f"🔄 Burst tokens reset: {self.burst_size} available")
