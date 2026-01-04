"""
File: core/rate_limiter.py
Location: telegram_scheduler_bot/core/rate_limiter.py
Purpose: Adaptive rate limiter with flood control recovery (IMPROVEMENT #9)
Reusable: YES - Works with ANY Telegram bot
"""

import asyncio
import logging

logger = logging.getLogger(__name__)

class AdaptiveRateLimiter:
    """
    Advanced rate limiter with adaptive speed control
    
    Features:
    - Token bucket algorithm for smooth rate limiting
    - Burst allowance for initial fast sending (50 messages)
    - Adaptive rate reduction when flood control detected
    - Auto-recovery to optimal speed after 60 seconds
    - Global rate: 25 msg/sec (up from 22)
    - Per-chat rate: 18 msg/min
    
    IMPROVEMENT #9: Optimized for maximum speed without bans
    Reusable: YES - Copy for any Telegram bot
    """
    
    def __init__(self, global_rate=25, per_chat_rate=18, burst_allowance=50):
        self.base_global_rate = global_rate
        self.per_chat_rate = per_chat_rate
        self.burst_allowance = burst_allowance
        
        # Global rate limiting
        self.global_tokens = burst_allowance  # Start with burst allowance
        self.global_last_update = asyncio.get_event_loop().time()
        self.global_lock = asyncio.Lock()
        
        # Per-chat rate limiting
        self.chat_tokens = {}  # {chat_id: (tokens, last_update)}
        self.chat_locks = {}   # {chat_id: Lock}
        
        # Adaptive control
        self.current_rate = global_rate
        self.flood_detected = False
        self.last_flood_time = None
        self.success_count = 0
    
    async def acquire_global(self):
        """Wait if necessary to respect global rate limit"""
        async with self.global_lock:
            now = asyncio.get_event_loop().time()
            time_passed = now - self.global_last_update
            self.global_last_update = now
            
            # Replenish tokens at current adaptive rate
            self.global_tokens += time_passed * self.current_rate
            if self.global_tokens > self.burst_allowance:
                self.global_tokens = self.burst_allowance
            
            # If no tokens, wait
            if self.global_tokens < 1.0:
                wait_time = (1.0 - self.global_tokens) / self.current_rate
                await asyncio.sleep(wait_time)
                self.global_tokens = 0.0
            else:
                self.global_tokens -= 1.0
    
    async def acquire_chat(self, chat_id):
        """Wait if necessary to respect per-chat rate limit"""
        if chat_id not in self.chat_locks:
            self.chat_locks[chat_id] = asyncio.Lock()
        
        async with self.chat_locks[chat_id]:
            now = asyncio.get_event_loop().time()
            
            if chat_id not in self.chat_tokens:
                self.chat_tokens[chat_id] = (self.per_chat_rate, now)
            
            tokens, last_update = self.chat_tokens[chat_id]
            time_passed = now - last_update
            
            # Replenish tokens (18 per 60 seconds)
            tokens += time_passed * (self.per_chat_rate / 60.0)
            if tokens > self.per_chat_rate:
                tokens = self.per_chat_rate
            
            # If no tokens, wait
            if tokens < 1.0:
                wait_time = (1.0 - tokens) / (self.per_chat_rate / 60.0)
                await asyncio.sleep(wait_time)
                tokens = 0.0
            else:
                tokens -= 1.0
            
            self.chat_tokens[chat_id] = (tokens, asyncio.get_event_loop().time())
    
    async def acquire(self, chat_id):
        """Acquire both global and per-chat tokens"""
        await self.acquire_global()
        await self.acquire_chat(chat_id)
    
    def report_flood_control(self):
        """
        Called when flood control detected
        Reduces rate by 30%, minimum 10 msg/sec
        """
        self.flood_detected = True
        self.last_flood_time = asyncio.get_event_loop().time()
        self.current_rate = max(self.current_rate * 0.7, 10)  # Reduce by 30%
        logger.warning(f"⚠️ Flood control detected! Reducing rate to {self.current_rate:.1f} msg/sec")
    
    def report_success(self):
        """
        Called on successful send
        Gradually recovers rate after 50 successful sends and 60 seconds
        """
        self.success_count += 1
        
        if self.flood_detected and self.success_count >= 50:
            now = asyncio.get_event_loop().time()
            # If 60 seconds passed since last flood, try increasing rate
            if self.last_flood_time and (now - self.last_flood_time) > 60:
                self.current_rate = min(self.current_rate * 1.1, self.base_global_rate)
                self.success_count = 0
                
                if self.current_rate >= self.base_global_rate:
                    self.flood_detected = False
                    logger.info(f"✅ Rate recovered to {self.current_rate:.1f} msg/sec")
    
    def get_status(self):
        """Get current rate limiter status"""
        return {
            'current_rate': self.current_rate,
            'base_rate': self.base_global_rate,
            'flood_detected': self.flood_detected,
            'global_tokens': self.global_tokens
        }