"""
File: handlers/__init__.py
Location: telegram_scheduler_bot/handlers/__init__.py
Purpose: Handlers package initialization
"""

from .command_handlers import register_command_handlers
from .message_handlers import register_message_handlers
from .scheduling_handlers import schedule_bulk_posts, schedule_batch_posts, schedule_auto_continuous_posts

def register_all_handlers(app, scheduler):
    """
    Register all bot handlers
    
    Args:
        app: Telegram Application instance
        scheduler: SchedulerCore instance
    """
    register_command_handlers(app, scheduler)
    register_message_handlers(app, scheduler)

__all__ = [
    'register_all_handlers', 
    'schedule_bulk_posts', 
    'schedule_batch_posts',
    'schedule_auto_continuous_posts'
]