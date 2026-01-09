"""
File: database/channels_db.py
Location: telegram_scheduler_bot/database/channels_db.py
Purpose: All channel database operations
FIXED: KeyError 0 - proper tuple/dict handling + empty result checks
FIXED: get_active_channels() now returns channel IDs correctly
"""

from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class ChannelsDB:
    """
    Channel database operations - FIXED for PostgreSQL compatibility
    """
    
    FAILURE_THRESHOLD = 3
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.channel_number_map = {}
        # FIXED: Don't call update immediately if no channels exist
        try:
            self.update_channel_numbers()
        except Exception as e:
            logger.warning(f"Initial channel number update failed (normal if no channels): {e}")
            self.channel_number_map = {}
    
    def _ph(self):
        """Placeholder helper for PostgreSQL (%s) vs SQLite (?)"""
        return '%s' if self.db.is_postgres() else '?'
    
    def _get_value(self, row, key_or_index):
        """
        FIXED: Universal getter that works for both dict and tuple
        
        Args:
            row: Database row (dict or tuple)
            key_or_index: Column name (str) or index (int)
        
        Returns:
            Value from row or None
        """
        if row is None:
            return None
        
        try:
            if isinstance(row, dict):
                return row.get(key_or_index) if isinstance(key_or_index, str) else None
            else:
                # Tuple or sqlite3.Row
                if isinstance(key_or_index, int):
                    return row[key_or_index] if len(row) > key_or_index else None
                else:
                    # Try dict-style access (sqlite3.Row supports this)
                    try:
                        return row[key_or_index]
                    except (KeyError, TypeError):
                        return None
        except Exception as e:
            logger.error(f"Error getting value {key_or_index} from row: {e}")
            return None
    
    def _extract_channel_id(self, row):
        """
        FIXED: Universal channel_id extractor for both SQLite Row and PostgreSQL tuple
        Handles both index [0] and key ['channel_id'] access
        """
        if row is None:
            return None
        
        # Try dict-like access first (SQLite Row)
        try:
            return row['channel_id']
        except (KeyError, TypeError):
            pass
        
        # Try tuple/index access (PostgreSQL or plain tuple)
        try:
            return row[0]
        except (KeyError, IndexError, TypeError):
            pass
        
        # Last resort: check if it has a get method
        try:
            if hasattr(row, 'get'):
                return row.get('channel_id')
        except:
            pass
        
        logger.error(f"Failed to extract channel_id from row: {type(row)}, {row}")
        return None
    
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
            except Exception as e:
                # Channel exists, just activate it
                try:
                    c.execute(f'UPDATE channels SET active = 1 WHERE channel_id = {ph}', (channel_id,))
                    conn.commit()
                    self.update_channel_numbers()
                    return True
                except Exception as e2:
                    logger.error(f"Failed to add/update channel {channel_id}: {e2}")
                    return False
    
    def add_channels_bulk(self, commands_text):
        """Add multiple channels from text"""
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
            
            if self.add_channel(channel_id, channel_name):
                added += 1
            else:
                failed += 1
        
        return added, failed
    
    def remove_channel(self, channel_id):
        """Remove a channel"""
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
        """Remove channels by their list numbers"""
        deleted = 0
        for num in numbers:
            channel_id = self.get_channel_by_number(num)
            if channel_id and self.remove_channel(channel_id):
                deleted += 1
        return deleted
    
    def remove_all_channels(self, confirm=None):
        """Remove all channels (requires confirm)"""
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
        """Move channel to recycle bin"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            c.execute(f'SELECT * FROM channels WHERE channel_id = {ph}', (channel_id,))
            channel = c.fetchone()
            
            if not channel:
                return False
            
            # Use universal getter
            ch_id = self._get_value(channel, 'channel_id') or self._get_value(channel, 1) or channel_id
            ch_name = self._get_value(channel, 'channel_name') or self._get_value(channel, 2)
            fail_count = self._get_value(channel, 'failure_count') or self._get_value(channel, 5) or 0
            last_fail = self._get_value(channel, 'last_failure') or self._get_value(channel, 7)
            
            c.execute(f'''
                INSERT INTO recycle_bin (channel_id, channel_name, failure_count, last_failure)
                VALUES ({ph}, {ph}, {ph}, {ph})
            ''', (ch_id, ch_name, fail_count, last_fail))
            
            c.execute(f'DELETE FROM channels WHERE channel_id = {ph}', (channel_id,))
            conn.commit()
            self.update_channel_numbers()
            
            logger.info(f"♻️ Moved to recycle bin: {channel_id}")
            return True
    
    def restore_from_recycle_bin(self, channel_id):
        """Restore channel from recycle bin"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            c.execute(f'SELECT * FROM recycle_bin WHERE channel_id = {ph}', (channel_id,))
            channel = c.fetchone()
            
            if not channel:
                return False
            
            ch_id = self._get_value(channel, 'channel_id') or self._get_value(channel, 1) or channel_id
            ch_name = self._get_value(channel, 'channel_name') or self._get_value(channel, 2)
            
            c.execute(f'''
                INSERT INTO channels (channel_id, channel_name, active, failure_count)
                VALUES ({ph}, {ph}, 1, 0)
            ''', (ch_id, ch_name))
            
            c.execute(f'DELETE FROM recycle_bin WHERE channel_id = {ph}', (channel_id,))
            conn.commit()
            self.update_channel_numbers()
            
            logger.info(f"✅ Restored from recycle bin: {channel_id}")
            return True
    
    def get_recycle_bin_channels(self):
        """Get all channels in recycle bin"""
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM recycle_bin ORDER BY deleted_at DESC')
            return c.fetchall()
    
    def export_channels_as_commands(self):
        """Export all active channels as /addchannel commands"""
        channels = self.get_all_channels()
        commands = []
        
        for ch in channels:
            ch_id = self._get_value(ch, 'channel_id') or self._get_value(ch, 0)
            ch_name = self._get_value(ch, 'channel_name') or self._get_value(ch, 1) or ''
            active = self._get_value(ch, 'active') or self._get_value(ch, 2) or 0
            
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
            return c.fetchall()
    
    def get_active_channels(self):
        """
        Get only active channels
        FIXED: Universal handling for both SQLite Row and PostgreSQL tuple
        """
        with self.db.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT channel_id FROM channels WHERE active = 1 ORDER BY added_at')
            rows = c.fetchall()
            
            if not rows:
                logger.debug("No active channels found")
                return []
            
            # FIXED: Use universal extractor instead of direct [0] access
            channel_ids = []
            for row in rows:
                channel_id = self._extract_channel_id(row)
                if channel_id:
                    channel_ids.append(channel_id)
            
            logger.debug(f"📡 Found {len(channel_ids)} active channels")
            return channel_ids
    
    def update_channel_numbers(self):
        """
        FIXED: Create mapping: channel number -> channel ID
        This was causing KeyError: 0
        """
        self.channel_number_map = {}
        
        try:
            with self.db.get_db() as conn:
                c = conn.cursor()
                c.execute('SELECT channel_id FROM channels WHERE active = 1 ORDER BY added_at')
                rows = c.fetchall()
                
                # FIXED: Check if rows exist before iterating
                if not rows:
                    logger.debug("No active channels found")
                    return
                
                # FIXED: Safely enumerate rows using universal extractor
                for idx, row in enumerate(rows, 1):
                    if row is None:
                        continue
                    
                    channel_id = self._extract_channel_id(row)
                    if channel_id:
                        self.channel_number_map[idx] = channel_id
                
                logger.debug(f"📋 Updated channel numbers: {len(self.channel_number_map)} channels")
        
        except Exception as e:
            logger.error(f"Error updating channel numbers: {e}")
            self.channel_number_map = {}
    
    def get_channel_by_number(self, number):
        """Get channel ID by its list number"""
        return self.channel_number_map.get(number)
    
    def get_channel_count(self):
        """Get number of active channels"""
        return len(self.channel_number_map)
    
    def record_channel_failure(self, channel_id, post_id, error_type, error_message):
        """Record a channel failure"""
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
            
            c.execute(f'SELECT failure_count FROM channels WHERE channel_id = {ph}', (channel_id,))
            result = c.fetchone()
            
            # FIXED: Use universal extractor
            failure_count = self._extract_channel_id(result) if result else 0
            
            conn.commit()
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
    
    """
    File: database/channels_db.py
    Location: telegram_scheduler_bot/database/channels_db.py
    Purpose: All channel database operations
    FIXED: PostgreSQL DISTINCT + ORDER BY compatibility
    REPLACE the get_channel_failures method in your existing file
    """
    
    # Find this method in your channels_db.py and replace it:
    
    def get_channel_failures(self, channel_id, limit=10):
        """
        Get recent failures for a channel
        FIXED: PostgreSQL compatibility with DISTINCT
        """
        with self.db.get_db() as conn:
            c = conn.cursor()
            ph = self._ph()
            
            # FIXED: Don't use DISTINCT, or if using it, include ORDER BY column in SELECT
            c.execute(f'''
                SELECT * FROM channel_failures 
                WHERE channel_id = {ph}
                ORDER BY failed_at DESC 
                LIMIT {limit}
            ''', (channel_id,))
            return c.fetchall()
    
    
    # Also check if you have this method - if it uses DISTINCT + ORDER BY, fix it too:
    
    def get_last_batch(self):
        """
        Get posts from the last batch
        FIXED: PostgreSQL DISTINCT + ORDER BY compatibility
        """
        with self.db.get_db() as conn:
            c = conn.cursor()
            
            # FIXED: Include order by column in SELECT when using DISTINCT
            c.execute('''
                SELECT DISTINCT batch_id, scheduled_time
                FROM posts 
                WHERE posted = 0 AND batch_id IS NOT NULL 
                ORDER BY scheduled_time DESC 
                LIMIT 1
            ''')
            result = c.fetchone()
            
            if result:
                batch_id = self._fetchone_value(c, column_index=0) if hasattr(self, '_fetchone_value') else result[0]
                
                if batch_id:
                    c.execute(f'SELECT * FROM posts WHERE batch_id = {self._ph()} ORDER BY scheduled_time',
                             (batch_id,))
                    rows = c.fetchall()
                    
                    columns = ['id', 'message', 'media_type', 'media_file_id', 'caption',
                              'scheduled_time', 'posted', 'total_channels', 'successful_posts',
                              'posted_at', 'created_at', 'batch_id', 'paused']
                    
                    if hasattr(self, '_rows_to_dicts'):
                        return self._rows_to_dicts(rows, columns)
                    else:
                        return rows
            
            return None
    
    def mark_channel_in_skip_list(self, channel_id, in_skip_list=True):
        """Mark channel as in skip list"""
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

