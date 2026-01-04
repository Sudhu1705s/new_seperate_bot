# Telegram Multi-Channel Scheduler Bot v2.0

Complete modular bot with 22+ improvements for high-performance multi-channel broadcasting.

## ✨ Features

### Core Improvements
✅ **Zero Duration Support** - Schedule all posts at exact same time (0m)
✅ **End Time Format** - Use "2026-01-31 20:00" as duration/end time
✅ **Multi-Command Import** - Paste multiple `/addchannel` commands at once
✅ **Numbered Management** - Delete channels/posts by number or range
✅ **Move Posts** - Reschedule posts with `/movepost 6-21 20:00`

### Performance
✅ **Optimized Rate Limiter** - 25 msg/sec (up from 22), adaptive flood control
✅ **Parallel+Hybrid Sending** - 10-15 sec for 402 messages (vs 40+ sec)
✅ **Smart Retry System** - Skip failed channels, retry later (exam strategy)

### Monitoring
✅ **Channel Health** - Track failures, generate reports with `/channelhealth`
✅ **Enhanced Stats** - Detailed analytics with `/stats`
✅ **Last Post Commands** - `/lastpost`, `/lastpostbatch`

### Control
✅ **Emergency Stop** - `/stopall` and `/resumeall` commands
✅ **Batch Mode Improvements** - Manual interval, final batch optimization
✅ **Smart Error Classification** - Permanent vs temporary vs rate limit

### Modes (Only 2)
📦 **Bulk Posts (Auto-Space)** - Evenly distribute posts over time
🎯 **Bulk Posts (Batches)** - Group posts in batches

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your credentials
```

Required variables:
- `BOT_TOKEN` - Your Telegram bot token
- `ADMIN_ID` - Your Telegram user ID
- `DATABASE_URL` - PostgreSQL URL (optional, uses SQLite if not set)

### 3. Run Bot

```bash
python main.py
```

## 📁 File Structure

```
telegram_scheduler_bot/
├── main.py                    # Entry point
├── requirements.txt           # Dependencies
├── .env.example              # Environment template
├── README.md                 # This file
│
├── config/                   # Configuration
│   ├── settings.py          # All settings
│   └── timezone_config.py   # UTC/IST conversion
│
├── database/                 # Database layer
│   ├── db_manager.py        # Connection handler
│   ├── posts_db.py          # Post operations
│   └── channels_db.py       # Channel operations
│
├── core/                     # Core logic
│   ├── rate_limiter.py      # Adaptive rate limiting
│   ├── retry_system.py      # Smart retry
│   ├── sender.py            # Parallel sending
│   └── scheduler_core.py    # Main orchestration
│
├── handlers/                 # Command & message handlers
│   ├── command_handlers.py  # All commands
│   └── message_handlers.py  # Message flow
│
├── ui/                       # User interface
│   └── keyboards.py         # Keyboard layouts
│
└── utils/                    # Utilities
    ├── time_parser.py       # Time parsing
    ├── validators.py        # Input validation
    └── helpers.py           # Helper functions
```

## 🎮 Commands

### Basic Commands
- `/start` - Initialize bot and show main menu
- `/stats` - Show enhanced statistics
- `/channels` - List all channels (numbered)
- `/list` - Show pending posts (numbered)

### Channel Management
- `/addchannel [id] [name]` - Add single channel
- `/addchannel` (paste multiple) - Bulk import channels
- `/deletechannel 5` - Delete channel #5
- `/deletechannel 5-10` - Delete channels 5-10
- `/deletechannel all confirm` - Delete all channels
- `/exportchannels` - Export for backup
- `/channelhealth` - Show health report
- `/test 5` - Test channel #5

### Post Management
- `/deletepost 5` - Delete post #5
- `/deletepost 5-10` - Delete posts 5-10
- `/deletepost all confirm` - Delete all posts
- `/movepost 5 20:00` - Move post #5 to 20:00
- `/movepost 5-10 20:00` - Move posts 5-10 starting from 20:00
- `/lastpost` - Show last scheduled post
- `/lastpostbatch` - Show last batch

### Control
- `/stopall` - Emergency stop all operations
- `/resumeall` - Resume normal operations
- `/reset confirm` - Delete all channels AND posts
- `/cancel` - Cancel current operation

## 🕐 Time Format Examples

All times are in IST (India Standard Time):

### Start Time
- `now` - Immediately
- `30m` - In 30 minutes
- `2h` - In 2 hours
- `today 18:00` - Today at 6 PM
- `tomorrow 9am` - Tomorrow at 9 AM
- `2026-01-31 20:00` - Specific date/time

### Duration
- `0m` - All posts at once (zero duration)
- `2h` - Over 2 hours
- `6h` - Over 6 hours
- `1d` - Over 24 hours
- `today` - Until midnight
- `2026-01-31 23:00` - Until this time (end time format)

## 📊 Performance

### Speed Improvements
- **Before:** 201 posts × 2 channels = 40+ seconds (with 30-sec delays)
- **After:** 201 posts × 2 channels = 10-15 seconds
- **Rate:** ~25 messages/second sustained
- **Burst:** 50 messages instantly

### Rate Limiting
- Global: 25 msg/sec (adaptive)
- Per-chat: 18 msg/min
- Flood control: Auto-reduces 30%, recovers after 60sec

### Retry Strategy
- Skip failed channels during main send
- Retry all failures after batch complete
- Max 3 retry attempts
- Alert after 5 consecutive failures

## 🔧 Advanced Configuration

Edit `config/settings.py` or set environment variables:

```bash
RATE_LIMIT_GLOBAL=25          # Messages per second
RATE_LIMIT_PER_CHAT=18        # Messages per minute per chat
AUTO_CLEANUP_MINUTES=30       # Auto-delete posted content after N minutes
BACKUP_UPDATE_FREQUENCY=20    # Backup update frequency in minutes
```

## 🗄️ Database

Supports both PostgreSQL and SQLite:

### PostgreSQL (Production)
Set `DATABASE_URL` environment variable:
```
postgresql://user:password@host:port/database
```

### SQLite (Development)
Leave `DATABASE_URL` unset, creates `posts.db` locally.

## 🔄 Reusability

Each module is designed to be reusable in other projects:

- **`core/rate_limiter.py`** - Copy for any Telegram bot
- **`core/retry_system.py`** - Copy for any multi-channel bot
- **`core/sender.py`** - Copy for any broadcast bot
- **`database/`** - Copy entire folder for any scheduling system
- **`utils/time_parser.py`** - Copy for any bot needing time input
- **`utils/validators.py`** - Copy for any project needing range parsing

## 🐛 Troubleshooting

### Bot not responding
- Check `BOT_TOKEN` and `ADMIN_ID` in `.env`
- Verify bot has admin rights in channels
- Check logs in `bot.log`

### Flood control errors
- Rate limiter should handle this automatically
- Check logs for "Reducing rate" messages
- Bot will recover after ~60 seconds

### Posts not sending
- Use `/channelhealth` to check channel status
- Use `/test 5` to test specific channel
- Check if channels are in skip list

### Database errors
- PostgreSQL: Verify `DATABASE_URL` is correct
- SQLite: Check file permissions for `posts.db`

## 📝 License

This project is provided as-is for personal use.

## 🤝 Support

For issues or questions, check:
1. Logs in `bot.log`
2. Use `/stats` and `/channelhealth` commands
3. Check environment variables
4. Verify channel permissions

## 🎯 Next Steps

After setup:
1. Add channels with `/addchannel`
2. Test with `/test [channel_number]`
3. Schedule posts using Bulk or Batch mode
4. Monitor with `/stats` and `/channelhealth`
5. Use `/exportchannels` to backup your channels

---

**Version:** 2.0
**Total Files:** 24
**Lines of Code:** ~3500
**All 22 Improvements:** ✅ Active