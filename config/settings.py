"""
File: config/settings.py
Purpose: Centralized configuration and constants
Dependencies: os, dotenv
Reusable: YES - Works with any Python project
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# =============================================================================
# TELEGRAM BOT CONFIGURATION
# =============================================================================
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMIN_ID = int(os.environ.get('ADMIN_ID', 0))

# Validate required settings
if not BOT_TOKEN or not ADMIN_ID:
    raise ValueError("❌ BOT_TOKEN and ADMIN_ID must be set in environment variables!")

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================
DATABASE_URL = os.environ.get('DATABASE_URL')
SQLITE_PATH = 'posts.db'  # Fallback for local testing

# =============================================================================
# RATE LIMITING CONFIGURATION (OPTIMIZED)
# =============================================================================
RATE_LIMIT_GLOBAL = int(os.environ.get('RATE_LIMIT_GLOBAL', 25))  # msg/sec (up from 22)
RATE_LIMIT_PER_CHAT = int(os.environ.get('RATE_LIMIT_PER_CHAT', 18))  # msg/min
BURST_ALLOWANCE = 50  # Allow burst of 50 messages

# =============================================================================
# SCHEDULER CONFIGURATION
# =============================================================================
AUTO_CLEANUP_MINUTES = int(os.environ.get('AUTO_CLEANUP_MINUTES', 30))
BATCH_SIZE_DEFAULT = 20
CHECK_INTERVAL_SECONDS = 5  # How often to check for due posts

# =============================================================================
# RETRY SYSTEM CONFIGURATION
# =============================================================================
MAX_RETRY_ATTEMPTS = 3
ALERT_THRESHOLD = 5  # Alert after N consecutive failures

# =============================================================================
# BACKUP SYSTEM CONFIGURATION
# =============================================================================
BACKUP_UPDATE_FREQUENCY = int(os.environ.get('BACKUP_UPDATE_FREQUENCY', 20))  # minutes
BACKUP_INSTANT_ON_USER_ACTION = True
BACKUP_FILE_SIZE_LIMIT_MB = 10  # Skip auto-update if file > this size

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
LOG_LEVEL = 'INFO'
LOG_FILE = 'bot.log'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

# =============================================================================
# CHANNEL CONFIGURATION
# =============================================================================
CHANNEL_IDS_STR = os.environ.get('CHANNEL_IDS', '')
INITIAL_CHANNEL_IDS = [ch.strip() for ch in CHANNEL_IDS_STR.split(',') if ch.strip()]

# =============================================================================
# UI CONFIGURATION
# =============================================================================
POSTS_PER_PAGE = 20
MAX_PREVIEW_LENGTH = 50
SHOW_UTC_TIME = True  # Show both IS