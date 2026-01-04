"""
File: utils/helpers.py
Location: telegram_scheduler_bot/utils/helpers.py
Purpose: Helper utility functions
Reusable: YES - Copy for any Telegram bot
"""

def extract_content(message):
    """
    Extract content from Telegram message
    
    Args:
        message: Telegram message object
    
    Returns:
        dict: Content dictionary with message/media info, or None if no content
    
    Returns structure:
        {
            'message': str (for text messages),
            'media_type': str ('photo', 'video', 'document'),
            'media_file_id': str (Telegram file ID),
            'caption': str (media caption)
        }
    
    Filters out:
        - Commands (starting with /)
        - Button text (menu navigation)
    
    Used for:
        - Collecting posts in bulk/batch mode
        - Extracting content from forwarded messages
    """
    content = {}
    
    # Extract text message (if not a command or button)
    if message.text and not message.text.startswith('/'):
        # Filter out button keywords
        button_keywords = [
            "✅ Done", "❌ Cancel", "✅ Confirm", "📦 Bulk", "🎯 Bulk",
            "📅 Exact", "⏱️ Duration", "📋 View", "📊 Stats", "📢 Channels",
            "Schedule All", "Confirm & Schedule"
        ]
        
        # Only add text if it's not a button press
        if not any(keyword in message.text for keyword in button_keywords):
            content['message'] = message.text
    
    # Extract media
    if message.photo:
        content['media_type'] = 'photo'
        content['media_file_id'] = message.photo[-1].file_id  # Highest resolution
        content['caption'] = message.caption
    elif message.video:
        content['media_type'] = 'video'
        content['media_file_id'] = message.video.file_id
        content['caption'] = message.caption
    elif message.document:
        content['media_type'] = 'document'
        content['media_file_id'] = message.document.file_id
        content['caption'] = message.caption
    
    # Return content only if we extracted something
    return content if content else None