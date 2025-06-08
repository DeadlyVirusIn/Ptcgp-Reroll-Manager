import asyncio
import discord
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
from dataclasses import dataclass
import sqlite3

@dataclass
class ExpirationInfo:
    gp_id: int
    message_id: int
    name: str
    expiration_date: datetime
    time_until_expiration: timedelta
    current_state: str
    thread_id: Optional[int] = None

class ExpirationManager:
    def __init__(self, db_manager, bot_instance):
        self.db = db_manager
        self.bot = bot_instance
        self.logger = logging.getLogger(__name__)
        self.check_interval = 300  # 5 minutes
        self.warning_threshold_hours = 6  # Warn when 6 hours remaining
        self.is_running = False

    async def start_expiration_monitoring(self):
        """Start the background task for monitoring expirations"""
        if self.is_running:
            self.logger.warning("Expiration monitoring is already running")
            return
        
        self.is_running = True
        self.logger.info("Starting expiration monitoring system")
        
        while self.is_running:
            try:
                await self._check_expirations()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                self.logger.error(f"Error in expiration monitoring loop: {e}")
                await asyncio.sleep(60)  # Wait a minute before retrying

    async def stop_expiration_monitoring(self):
        """Stop the background monitoring"""
        self.is_running = False
        self.logger.info("Stopped expiration monitoring system")

    async def cleanup_old_expiration_data(self, days_to_keep: int = 7):
        """Clean up old expiration warning data"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            with self.db.lock:
                conn = sqlite3.connect(self.db.db_path)
                cursor = conn.cursor()
                
                # Clean up old expiration warnings
                cursor.execute('''
                    DELETE FROM expiration_warnings 
                    WHERE warned_at < ?
                ''', (cutoff_date,))
                
                deleted_count = cursor.rowcount
                conn.commit()
                conn.close()
            
            self.logger.info(f"Cleaned up {deleted_count} old expiration warnings")
            
        except Exception as e:
            self.logger.error(f"Error cleaning up expiration data: {e}")

    async def _check_expirations(self):
        """Check for expired and soon-to-expire god packs"""
        try:
            # Get expired god packs
            expired_gps = self.db.get_expired_godpacks()
            
            if expired_gps:
                self.logger.info(f"Found {len(expired_gps)} expired god packs")
                await self._process_expired_godpacks(expired_gps)
            
            # Check for god packs expiring soon
            expiring_soon = await self._get_expiring_soon()
            
            if expiring_soon:
                self.logger.info(f"Found {len(expiring_soon)} god packs expiring soon")
                await self._send_expiration_warnings(expiring_soon)
                
        except Exception as e:
            self.logger.error(f"Error checking expirations: {e}")

    async def _process_expired_godpacks(self, expired_gps: List):
        """Process expired god packs - update state and archive threads"""
        from database_manager import GPState
        
        for gp in expired_gps:
            try:
                # Determine new state based on current state
                if gp.state == GPState.ALIVE:
                    new_state = GPState.EXPIRED
                else:
                    new_state = GPState.DEAD
                
                # Update database
                success = self.db.update_godpack_state(gp.id, new_state)
                
                if success:
                    self.logger.info(f"Updated GP {gp.id} ({gp.name}) from {gp.state.value} to {new_state.value}")
                    
                    # Archive associated Discord thread
                    await self._archive_godpack_thread(gp)
                    
                    # Send notification to relevant channels
                    await self._send_expiration_notification(gp, new_state)
                
            except Exception as e:
                self.logger.error(f"Error processing expired GP {gp.id}: {e}")

    async def _archive_godpack_thread(self, gp):
        """Archive the Discord thread associated with a god pack"""
        try:
            # Find the thread by searching all guilds
            for guild in self.bot.guilds:
                # Look for forum channels
                for channel in guild.channels:
                    if isinstance(channel, discord.ForumChannel):
                        # Search through threads
                        for thread in channel.threads:
                            if await self._is_godpack_thread(thread, gp.message_id):
                                if not thread.archived:
                                    await self._archive_thread_with_retry(thread)
                                    self.logger.info(f"Archived thread for GP {gp.id}: {thread.name}")
                                return
                        
                        # Also check archived threads
                        async for thread in channel.archived_threads(limit=100):
                            if await self._is_godpack_thread(thread, gp.message_id):
                                # Already archived, no need to do anything
                                return
                                
        except Exception as e:
            self.logger.error(f"Error archiving thread for GP {gp.id}: {e}")

    async def _is_godpack_thread(self, thread: discord.Thread, message_id: int) -> bool:
        """Check if a thread belongs to a specific god pack"""
        try:
            # Check if the thread's starting message mentions the god pack ID
            starter_message = await thread.fetch_message(thread.id)
            return f"GodPack ID: {message_id}" in starter_message.content or str(message_id) in thread.name
        except:
            return False

    async def _archive_thread_with_retry(self, thread: discord.Thread, max_retries: int = 3):
        """Archive a thread with retry logic"""
        for attempt in range(max_retries):
            try:
                await thread.edit(archived=True, reason="God pack expired")
                return
            except discord.HTTPException as e:
                if e.status == 429:  # Rate limited
                    retry_after = int(e.response.headers.get("Retry-After", 5))
                    self.logger.warning(f"Rate limited archiving thread {thread.name}, retrying after {retry_after}s")
                    await asyncio.sleep(retry_after)
                else:
                    self.logger.error(f"HTTP error archiving thread {thread.name}: {e}")
                    break
            except Exception as e:
                self.logger.error(f"Error archiving thread {thread.name} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff

    async def _get_expiring_soon(self) -> List[ExpirationInfo]:
        """Get god packs that will expire within the warning threshold"""
        with self.db.lock:
            conn = sqlite3.connect(self.db.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            warning_time = datetime.now() + timedelta(hours=self.warning_threshold_hours)
            
            cursor.execute('''
                SELECT * FROM godpacks 
                WHERE expiration_date BETWEEN CURRENT_TIMESTAMP AND ?
                AND state IN ('TESTING', 'ALIVE')
                AND id NOT IN (
                    SELECT gp_id FROM expiration_warnings 
                    WHERE warned_at > datetime('now', '-1 day')
                )
            ''', (warning_time,))
            
            rows = cursor.fetchall()
            conn.close()
        
        expiring_soon = []
        for row in rows:
            exp_date = datetime.fromisoformat(row['expiration_date'])
            time_until = exp_date - datetime.now()
            
            expiring_soon.append(ExpirationInfo(
                gp_id=row['id'],
                message_id=row['message_id'],
                name=row['name'],
                expiration_date=exp_date,
                time_until_expiration=time_until,
                current_state=row['state']
            ))
        
        return expiring_soon

    async def _send_expiration_warnings(self, expiring_gps: List[ExpirationInfo]):
        """Send warnings for god packs expiring soon"""
        for exp_info in expiring_gps:
            try:
                # Find relevant channels to send warnings
                for guild in self.bot.guilds:
                    # Look for god pack channels or general channels
                    warning_channels = []
                    
                    for channel in guild.text_channels:
                        if any(keyword in channel.name.lower() for keyword in ['godpack', 'gp', 'alert', 'notification']):
                            warning_channels.append(channel)
                    
                    # Send warning to first suitable channel found
                    if warning_channels:
                        await self._send_warning_message(warning_channels[0], exp_info)
                        self._record_warning_sent(exp_info.gp_id)
                        break
                        
            except Exception as e:
                self.logger.error(f"Error sending expiration warning for GP {exp_info.gp_id}: {e}")

    async def _send_warning_message(self, channel: discord.TextChannel, exp_info: ExpirationInfo):
        """Send warning message to a channel"""
        hours_remaining = exp_info.time_until_expiration.total_seconds() / 3600
        
        embed = discord.Embed(
            title="⚠️ God Pack Expiring Soon",
            description=f"**{exp_info.name}** will expire in approximately **{hours_remaining:.1f} hours**",
            color=discord.Color.orange(),
            timestamp=datetime.now()
        )
        
        embed.add_field(
            name="Expiration Time",
            value=f"<t:{int(exp_info.expiration_date.timestamp())}:F>",
            inline=False
        )
        
        embed.add_field(
            name="Current State",
            value=exp_info.current_state,
            inline=True
        )
        
        embed.add_field(
            name="Time Remaining",
            value=f"{hours_remaining:.1f} hours",
            inline=True
        )
        
        embed.set_footer(text=f"GP ID: {exp_info.gp_id}")
        
        await channel.send(embed=embed)

    async def _send_expiration_notification(self, gp, new_state):
        """Send notification when a god pack expires"""
        for guild in self.bot.guilds:
            for channel in guild.text_channels:
                if any(keyword in channel.name.lower() for keyword in ['godpack', 'gp', 'log', 'notification']):
                    embed = discord.Embed(
                        title="📅 God Pack Expired",
                        description=f"**{gp.name}** has expired and been marked as **{new_state.value}**",
                        color=discord.Color.red() if str(new_state) == "GPState.DEAD" else discord.Color.dark_orange(),
                        timestamp=datetime.now()
                    )
                    
                    embed.add_field(
                        name="Pack Details",
                        value=f"Friend Code: {gp.friend_code}\nPack Number: {gp.pack_number}",
                        inline=False
                    )
                    
                    embed.add_field(
                        name="Expired At",
                        value=f"<t:{int(gp.expiration_date.timestamp())}:F>",
                        inline=True
                    )
                    
                    embed.set_footer(text=f"GP ID: {gp.id}")
                    
                    await channel.send(embed=embed)
                    break

    def _record_warning_sent(self, gp_id: int):
        """Record that a warning was sent for a god pack"""
        with self.db.lock:
            conn = sqlite3.connect(self.db.db_path)
            cursor = conn.cursor()
            
            # Create warnings table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS expiration_warnings (
                    gp_id INTEGER,
                    warned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (gp_id, warned_at)
                )
            ''')
            
            cursor.execute('''
                INSERT INTO expiration_warnings (gp_id) VALUES (?)
            ''', (gp_id,))
            
            conn.commit()
            conn.close()

    async def get_expiration_summary(self, days_ahead: int = 3) -> Dict:
        """Get a summary of upcoming expirations"""
        with self.db.lock:
            conn = sqlite3.connect(self.db.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            future_date = datetime.now() + timedelta(days=days_ahead)
            
            cursor.execute('''
                SELECT 
                    state,
                    COUNT(*) as count,
                    MIN(expiration_date) as earliest_expiration,
                    MAX(expiration_date) as latest_expiration
                FROM godpacks 
                WHERE expiration_date BETWEEN CURRENT_TIMESTAMP AND ?
                AND state NOT IN ('EXPIRED', 'DEAD', 'INVALID')
                GROUP BY state
            ''', (future_date,))
            
            state_summary = cursor.fetchall()
            
            # Get detailed list
            cursor.execute('''
                SELECT id, message_id, name, friend_code, state, expiration_date
                FROM godpacks 
                WHERE expiration_date BETWEEN CURRENT_TIMESTAMP AND ?
                AND state NOT IN ('EXPIRED', 'DEAD', 'INVALID')
                ORDER BY expiration_date ASC
            ''', (future_date,))
            
            detailed_list = cursor.fetchall()
            conn.close()
        
        summary = {
            'total_expiring': sum(row['count'] for row in state_summary),
            'by_state': {row['state']: row['count'] for row in state_summary},
            'earliest_expiration': min([datetime.fromisoformat(row['earliest_expiration']) 
                                      for row in state_summary]) if state_summary else None,
            'detailed_list': [
                {
                    'id': row['id'],
                    'name': row['name'],
                    'state': row['state'],
                    'expiration_date': datetime.fromisoformat(row['expiration_date']),
                    'hours_remaining': (datetime.fromisoformat(row['expiration_date']) - datetime.now()).total_seconds() / 3600
                }
                for row in detailed_list
            ]
        }
        
        return summary

    async def extend_expiration(self, gp_id: int, hours: int) -> bool:
        """Manually extend the expiration time of a god pack"""
        try:
            godpack = self.db.get_godpack(gp_id=gp_id)
            if not godpack:
                return False
            
            new_expiration = godpack.expiration_date + timedelta(hours=hours)
            
            with self.db.lock:
                conn = sqlite3.connect(self.db.db_path)
                cursor = conn.cursor()
                
                cursor.execute('''
                    UPDATE godpacks 
                    SET expiration_date = ?, updated_at = CURRENT_TIMESTAMP 
                    WHERE id = ?
                ''', (new_expiration, gp_id))
                
                success = cursor.rowcount > 0
                conn.commit()
                conn.close()
            
            if success:
                self.logger.info(f"Extended expiration for GP {gp_id} by {hours} hours")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error extending expiration for GP {gp_id}: {e}")
            return False

    async def force_expire_godpack(self, gp_id: int, reason: str = "Manual expiration") -> bool:
        """Manually expire a god pack immediately"""
        try:
            from database_manager import GPState
            
            godpack = self.db.get_godpack(gp_id=gp_id)
            if not godpack:
                return False
            
            # Determine new state
            new_state = GPState.EXPIRED if godpack.state == GPState.ALIVE else GPState.DEAD
            
            # Update state
            success = self.db.update_godpack_state(gp_id, new_state)
            
            if success:
                # Archive thread
                await self._archive_godpack_thread(godpack)
                
                # Send notification
                await self._send_manual_expiration_notification(godpack, new_state, reason)
                
                self.logger.info(f"Manually expired GP {gp_id} ({godpack.name}): {reason}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error manually expiring GP {gp_id}: {e}")
            return False

    async def _send_manual_expiration_notification(self, gp, new_state, reason: str):
        """Send notification for manual expiration"""
        for guild in self.bot.guilds:
            for channel in guild.text_channels:
                if any(keyword in channel.name.lower() for keyword in ['godpack', 'gp', 'log']):
                    embed = discord.Embed(
                        title="🔧 God Pack Manually Expired",
                        description=f"**{gp.name}** has been manually expired and marked as **{new_state.value}**",
                        color=discord.Color.blue(),
                        timestamp=datetime.now()
                    )
                    
                    embed.add_field(
                        name="Reason",
                        value=reason,
                        inline=False
                    )
                    
                    embed.add_field(
                        name="Pack Details",
                        value=f"Friend Code: {gp.friend_code}\nPack Number: {gp.pack_number}",
                        inline=False
                    )
                    
                    embed.set_footer(text=f"GP ID: {gp.id}")
                    
                    await channel.send(embed=embed)
                    break

    def get_monitoring_status(self) -> Dict:
        """Get the current status of the expiration monitoring system"""
        return {
            'is_running': self.is_running,
            'check_interval_seconds': self.check_interval,
            'warning_threshold_hours': self.warning_threshold_hours,
            'last_check': datetime.now().isoformat()
        }