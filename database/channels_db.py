"""
File: database/channels_db.py
Location: telegram_scheduler_bot/database/channels_db.py
Purpose: All channel database operations
Reusable: Modify for any multi-channel bot
"""

from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class ChannelsDB:
    """
    Channel database operations
    All CRUD operations for channels
    """
    
    FAILURE_THRESHOLD = 3  # IMPROVEMENT #6: Alert after 3 consecutive failures
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.channel_number_map = {}
        self.update_channel_numbers()
    
    def add_channel(self, channel_id, channel_name=None):
        """Add a new channel"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            try:
                c.execute('''
                    INSERT INTO channels (channel_id, channel_name, active) 
                    VALUES (?, ?, 1)
                ''', (channel_id, channel_name))
                conn.commit()
                self.update_channel_numbers()
                logger.info(f"✅ Added channel: {channel_id}")
                return True
            except:
                # Channel exists, just activate it
                c.execute('UPDATE channels SET active = 1 WHERE channel_id = ?', (channel_id,))
                conn.commit()
                self.update_channel_numbers()
                return True
    
    def add_channels_bulk(self, commands_text):
        """
        Add multiple channels from /addchannel commands (IMPROVEMENT #3)
        Supports multi-line input
        """
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
            c.execute('DELETE FROM channels WHERE channel_id = ?', (channel_id,))
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
        """
        Remove all channels (IMPROVEMENT #4 - requires confirm)
        
        Args:
            confirm: Must be 'confirm' to proceed
        
        Returns:
            int: Number of deleted channels, or -1 if not confirmed
        """
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
        """
        Move channel to recycle bin (soft delete - IMPROVEMENT #3)
        
        Args:
            channel_id: Channel to move
        
        Returns:
            bool: True if successful
        """
        with self.db.get_db() as conn:
            c = conn.cursor()
            
            # Get channel info
            c.execute('SELECT * FROM channels WHERE channel_id = ?', (channel_id,))
            channel = c.fetchone()
            
            if not channel:
                return False
            
            # Move to recycle bin
            c.execute('''
                INSERT INTO recycle_bin (channel_id, channel_name, failure_count, last_failure)
                VALUES (?, ?, ?, ?)
            ''', (channel['channel_id'], channel.get('channel_name'), 
                  channel.get('failure_count', 0), channel.get('last_failure')))
            
            # Delete from channels
            c.execute('DELETE FROM channels WHERE channel_id = ?', (channel_id,))
            conn.commit()
            self.update_channel_numbers()
            
            logger.info(f"♻️ Moved to recycle bin: {channel_id}")
            return True
    
    def restore_from_recycle_bin(self, channel_id):
        """
        Restore channel from recycle bin (IMPROVEMENT #3)
        
        Args:
            channel_id: Channel to restore
        
        Returns:
            bool: True if successful
        """
        with self.db.get_db() as conn:
            c = conn.cursor()
            
            # Get from recycle bin
            c.execute('SELECT * FROM recycle_bin WHERE channel_id = ?', (channel_id,))
            channel = c.fetchone()
            
            if not channel:
                return False
            
            # Restore to channels
            c.execute('''
                INSERT INTO channels (channel_id, channel_name, active, failure_count)
                VALUES (?, ?, 1, 0)
            ''', (channel['channel_id'], channel.get('channel_name')))
            
            # Remove from recycle bin
            c.execute('DELETE FROM recycle_bin WHERE channel_id = ?', (channel_id,))
            conn.commit()
            self.update_channel_numbers()
            
            logger.info(f"✅ Restored from recycle bin: {channel_id}")
            return True
    
    def get_recycle_bin_channels(self):
        """
        Get all channels in recycle bin (IMPROVEMENT #3)
        
        Returns:
            list: Channels in recycle bin
        """
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM recycle_bin ORDER BY deleted_at DESC')
            return c.fetchall()
    
    def export_channels_as_commands(self):
        """
        Export all active channels as /addchannel commands (IMPROVEMENT #3)
        
        Returns:
            list: List of command strings
        """
        channels = self.get_all_channels()
        commands = []
        
        for ch in channels:
            if ch.get('active', 0) == 1:
                channel_id = ch['channel_id']
                channel_name = ch.get('channel_name', '')
                
                if channel_name:
                    commands.append(f"/addchannel {channel_id} {channel_name}")
                else:
                    commands.append(f"/addchannel {channel_id}")
        
        return commands
    
    def get_all_channels(self):
        """Get all channels"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT channel_id, channel_name, active, added_at FROM channels ORDER BY added_at')
            return c.fetchall()
    
    def get_active_channels(self):
        """Get only active channels"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT channel_id FROM channels WHERE active = 1 ORDER BY added_at')
            return [row[0] for row in c.fetchall()]
    
    def update_channel_numbers(self):
        """Create mapping: channel number -> channel ID (IMPROVEMENT #4)"""
        self.channel_number_map = {}
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT channel_id FROM channels WHERE active = 1 ORDER BY added_at')
            for idx, row in enumerate(c.fetchall(), 1):
                self.channel_number_map[idx] = row[0]
    
    def get_channel_by_number(self, number):
        """Get channel ID by its list number (IMPROVEMENT #4)"""
        return self.channel_number_map.get(number)
    
    def get_channel_count(self):
        """Get number of active channels"""
        return len(self.channel_number_map)
    
    def record_channel_failure(self, channel_id, post_id, error_type, error_message):
        """
        Record a channel failure (IMPROVEMENT #6 & #8)
        Checks if threshold reached (3 failures)
        """
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('''
                INSERT INTO channel_failures (channel_id, post_id, error_type, error_message)
                VALUES (?, ?, ?, ?)
            ''', (channel_id, post_id, error_type, error_message))
            
            c.execute('''
                UPDATE channels 
                SET failure_count = failure_count + 1, last_failure = ? 
                WHERE channel_id = ?
            ''', (datetime.utcnow().isoformat(), channel_id))
            
            # Check if threshold reached
            c.execute('SELECT failure_count FROM channels WHERE channel_id = ?', (channel_id,))
            result = c.fetchone()
            failure_count = result[0] if result else 0
            
            conn.commit()
            
            # Return True if threshold reached
            return failure_count >= self.FAILURE_THRESHOLD
    
    def record_channel_success(self, channel_id):
        """Record a successful send to channel"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('''
                UPDATE channels 
                SET failure_count = 0, last_success = ? 
                WHERE channel_id = ?
            ''', (datetime.utcnow().isoformat(), channel_id))
            conn.commit()
    
    def get_channel_failures(self, channel_id, limit=10):
        """Get recent failures for a channel"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('''
                SELECT * FROM channel_failures 
                WHERE channel_id = ? 
                ORDER BY failed_at DESC 
                LIMIT ?
            ''', (channel_id, limit))
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
            c.execute('UPDATE channels SET in_skip_list = ? WHERE channel_id = ?',
                     (1 if in_skip_list else 0, channel_id))
            conn.commit()
    
    def get_skip_list_channels(self):
        """Get all channels in skip list"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT channel_id, channel_name FROM channels WHERE in_skip_list = 1')
            return c.fetchall()