"""
File: database/channels_db.py
Location: telegram_scheduler_bot/database/channels_db.py
Purpose: All channel database operations
FIXED: KeyError 0 in update_channel_numbers() + PostgreSQL compatibility
"""

from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class ChannelsDB:
    """
    Channel database operations
    All CRUD operations for channels
    FIXED: PostgreSQL compatibility + KeyError fix
    """
    
    FAILURE_THRESHOLD = 3
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.channel_number_map = {}
        self.update_channel_numbers()
    
    def _ph(self):
        """Placeholder helper for PostgreSQL (%s) vs SQLite (?)"""
        return '%s' if self.db.is_postgres() else '?'
    
    def add_channel(self, channel_id, channel_name=None):
        """Add a new channel"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            try:
                c.execute(f'''
                    INSERT INTO channels (channel_id, channel_name, active) 
                    VALUES ({ph}, {ph}, 1)
                ''', (channel_id, channel_name))
                conn.commit()
                self.update_channel_numbers()
                logger.info(f"✅ Added channel: {channel_id}")
                return True
            except:
                # Channel exists, just activate it
                c.execute(f'UPDATE channels SET active = 1 WHERE channel_id = {ph}', (channel_id,))
                conn.commit()
                self.update_channel_numbers()
                return True
    
    def add_channels_bulk(self, commands_text):
        """Add multiple channels from /addchannel commands (IMPROVEMENT #3)"""
        lines = commands_text.strip().split('\n')
        added = 0
        failed = 0
        
        for line in lines:
            line = line.strip()
            if not line.startswith('/addchannel'):
                continue
            
            parts = line.split()
            if len(parts) < 2:
                failed += 1
                continue
            
            channel_id = parts[1]
            channel_name = " ".join(parts[2:]) if len(parts) > 2 else None
            
            try:
                if self.add_channel(channel_id, channel_name):
                    added += 1
                else:
                    failed += 1
            except:
                failed += 1
        
        return added, failed
    
    def remove_channel(self, channel_id):
        """Remove a channel (hard delete - IMPROVEMENT #4)"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            c.execute(f'DELETE FROM channels WHERE channel_id = {ph}', (channel_id,))
            deleted = c.rowcount > 0
            conn.commit()
            if deleted:
                self.update_channel_numbers()
                logger.info(f"🗑️ Removed channel: {channel_id}")
            return deleted
    
    def remove_channels_by_numbers(self, numbers):
        """Remove channels by their list numbers (IMPROVEMENT #4)"""
        deleted = 0
        for num in numbers:
            channel_id = self.get_channel_by_number(num)
            if channel_id and self.remove_channel(channel_id):
                deleted += 1
        return deleted
    
    def remove_all_channels(self, confirm=None):
        """Remove all channels (IMPROVEMENT #4 - requires confirm)"""
        if confirm != 'confirm':
            return -1
        
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM channels')
            deleted = c.rowcount
            conn.commit()
            self.update_channel_numbers()
            return deleted
    
    def move_to_recycle_bin(self, channel_id):
        """Move channel to recycle bin (soft delete - IMPROVEMENT #3)"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            # Get channel info
            c.execute(f'SELECT * FROM channels WHERE channel_id = {ph}', (channel_id,))
            channel = c.fetchone()
            
            if not channel:
                return False
            
            # FIXED: Handle both dict (SQLite) and tuple (PostgreSQL)
            if isinstance(channel, dict):
                ch_id = channel['channel_id']
                ch_name = channel.get('channel_name')
                fail_count = channel.get('failure_count', 0)
                last_fail = channel.get('last_failure')
            else:
                # PostgreSQL returns tuple
                ch_id = channel[0] if len(channel) > 0 else channel_id
                ch_name = channel[1] if len(channel) > 1 else None
                fail_count = channel[5] if len(channel) > 5 else 0
                last_fail = channel[7] if len(channel) > 7 else None
            
            # Move to recycle bin
            c.execute(f'''
                INSERT INTO recycle_bin (channel_id, channel_name, failure_count, last_failure)
                VALUES ({ph}, {ph}, {ph}, {ph})
            ''', (ch_id, ch_name, fail_count, last_fail))
            
            # Delete from channels
            c.execute(f'DELETE FROM channels WHERE channel_id = {ph}', (channel_id,))
            conn.commit()
            self.update_channel_numbers()
            
            logger.info(f"♻️ Moved to recycle bin: {channel_id}")
            return True
    
    def restore_from_recycle_bin(self, channel_id):
        """Restore channel from recycle bin (IMPROVEMENT #3)"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            # Get from recycle bin
            c.execute(f'SELECT * FROM recycle_bin WHERE channel_id = {ph}', (channel_id,))
            channel = c.fetchone()
            
            if not channel:
                return False
            
            # FIXED: Handle both dict and tuple
            if isinstance(channel, dict):
                ch_id = channel['channel_id']
                ch_name = channel.get('channel_name')
            else:
                ch_id = channel[0] if len(channel) > 0 else channel_id
                ch_name = channel[1] if len(channel) > 1 else None
            
            # Restore to channels
            c.execute(f'''
                INSERT INTO channels (channel_id, channel_name, active, failure_count)
                VALUES ({ph}, {ph}, 1, 0)
            ''', (ch_id, ch_name))
            
            # Remove from recycle bin
            c.execute(f'DELETE FROM recycle_bin WHERE channel_id = {ph}', (channel_id,))
            conn.commit()
            self.update_channel_numbers()
            
            logger.info(f"✅ Restored from recycle bin: {channel_id}")
            return True
    
    def get_recycle_bin_channels(self):
        """Get all channels in recycle bin (IMPROVEMENT #3)"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM recycle_bin ORDER BY deleted_at DESC')
            return c.fetchall()
    
    def export_channels_as_commands(self):
        """Export all active channels as /addchannel commands (IMPROVEMENT #3)"""
        channels = self.get_all_channels()
        commands = []
        
        for ch in channels:
            # FIXED: Handle both dict and tuple
            if isinstance(ch, dict):
                ch_id = ch['channel_id']
                ch_name = ch.get('channel_name', '')
                active = ch.get('active', 0)
            else:
                ch_id = ch[0] if len(ch) > 0 else None
                ch_name = ch[1] if len(ch) > 1 else ''
                active = ch[2] if len(ch) > 2 else 0
            
            if active == 1 and ch_id:
                if ch_name:
                    commands.append(f"/addchannel {ch_id} {ch_name}")
                else:
                    commands.append(f"/addchannel {ch_id}")
        
        return commands
    
    def get_all_channels(self):
        """Get all channels"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT channel_id, channel_name, active, added_at FROM channels ORDER BY added_at')
            
            # FIXED: Convert to dict for consistency
            if self.db.is_postgres():
                rows = c.fetchall()
                return [
                    {
                        'channel_id': row[0],
                        'channel_name': row[1],
                        'active': row[2],
                        'added_at': row[3]
                    }
                    for row in rows
                ]
            else:
                return c.fetchall()
    
    def get_active_channels(self):
        """Get only active channels"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT channel_id FROM channels WHERE active = 1 ORDER BY added_at')
            return [row[0] for row in c.fetchall()]
    
    def update_channel_numbers(self):
        """
        FIXED: Create mapping: channel number -> channel ID (IMPROVEMENT #4)
        This method was causing KeyError: 0
        """
        self.channel_number_map = {}
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT channel_id FROM channels WHERE active = 1 ORDER BY added_at')
            rows = c.fetchall()
            
            # FIXED: Proper enumeration for both SQLite and PostgreSQL
            for idx, row in enumerate(rows, 1):
                # row is a tuple in both SQLite and PostgreSQL
                channel_id = row[0]  # First element of tuple
                self.channel_number_map[idx] = channel_id
            
            logger.debug(f"📋 Updated channel numbers: {len(self.channel_number_map)} channels")
    
    def get_channel_by_number(self, number):
        """Get channel ID by its list number (IMPROVEMENT #4)"""
        return self.channel_number_map.get(number)
    
    def get_channel_count(self):
        """Get number of active channels"""
        return len(self.channel_number_map)
    
    def record_channel_failure(self, channel_id, post_id, error_type, error_message):
        """Record a channel failure (IMPROVEMENT #6 & #8)"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            c.execute(f'''
                INSERT INTO channel_failures (channel_id, post_id, error_type, error_message)
                VALUES ({ph}, {ph}, {ph}, {ph})
            ''', (channel_id, post_id, error_type, error_message))
            
            c.execute(f'''
                UPDATE channels 
                SET failure_count = failure_count + 1, last_failure = {ph}
                WHERE channel_id = {ph}
            ''', (datetime.utcnow().isoformat(), channel_id))
            
            # Check if threshold reached
            c.execute(f'SELECT failure_count FROM channels WHERE channel_id = {ph}', (channel_id,))
            result = c.fetchone()
            failure_count = result[0] if result else 0
            
            conn.commit()
            
            # Return True if threshold reached
            return failure_count >= self.FAILURE_THRESHOLD
    
    def record_channel_success(self, channel_id):
        """Record a successful send to channel"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            c.execute(f'''
                UPDATE channels 
                SET failure_count = 0, last_success = {ph}
                WHERE channel_id = {ph}
            ''', (datetime.utcnow().isoformat(), channel_id))
            conn.commit()
    
    def get_channel_failures(self, channel_id, limit=10):
        """Get recent failures for a channel"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            c.execute(f'''
                SELECT * FROM channel_failures 
                WHERE channel_id = {ph}
                ORDER BY failed_at DESC 
                LIMIT {limit}
            ''', (channel_id,))
            return c.fetchall()
    
    def get_channels_with_failures(self):
        """Get channels that have failure counts > 0"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('''
                SELECT channel_id, channel_name, failure_count, last_failure, in_skip_list 
                FROM channels 
                WHERE failure_count > 0 
                ORDER BY failure_count DESC
            ''')
            return c.fetchall()
    
    def mark_channel_in_skip_list(self, channel_id, in_skip_list=True):
        """Mark channel as in skip list (IMPROVEMENT #7)"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            c.execute(f'UPDATE channels SET in_skip_list = {ph} WHERE channel_id = {ph}',
                     (1 if in_skip_list else 0, channel_id))
            conn.commit()
    
    def get_skip_list_channels(self):
        """Get all channels in skip list"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT channel_id, channel_name FROM channels WHERE in_skip_list = 1')
            return c.fetchall()
