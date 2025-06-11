import discord
from discord.ext import commands
from discord import app_commands
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple, Union
import logging
from functools import wraps
import time
from collections import defaultdict

try:
    import config
    COMMAND_COOLDOWN = getattr(config, 'COMMAND_COOLDOWN_SECONDS', 2)
    MAX_PROBABILITY_CALCULATIONS_PER_MINUTE = getattr(config, 'MAX_PROBABILITY_CALCULATIONS_PER_MINUTE', 60)
    MAX_PLOT_GENERATIONS_PER_HOUR = getattr(config, 'MAX_PLOT_GENERATIONS_PER_HOUR', 20)
    MAX_GLOBAL_REQUESTS_PER_MINUTE = getattr(config, 'MAX_GLOBAL_REQUESTS_PER_MINUTE', 500)
    MAX_USER_COMMANDS_PER_5MIN = getattr(config, 'MAX_USER_COMMANDS_PER_5MIN', 100)
    MAX_HEAVY_COMMANDS_PER_HOUR = getattr(config, 'MAX_HEAVY_COMMANDS_PER_HOUR', 20)
    MAX_ADMIN_COMMANDS_PER_HOUR = getattr(config, 'MAX_ADMIN_COMMANDS_PER_HOUR', 50)
except ImportError:
    COMMAND_COOLDOWN = 2
    MAX_PROBABILITY_CALCULATIONS_PER_MINUTE = 60
    MAX_PLOT_GENERATIONS_PER_HOUR = 20
    MAX_GLOBAL_REQUESTS_PER_MINUTE = 500
    MAX_USER_COMMANDS_PER_5MIN = 100
    MAX_HEAVY_COMMANDS_PER_HOUR = 20
    MAX_ADMIN_COMMANDS_PER_HOUR = 50

class EnhancedRateLimiter:
    """Advanced rate limiter with global limits, user tracking, and abuse prevention"""
    
    def __init__(self):
        self._buckets = {}
        self._user_violations = defaultdict(int)
        self._global_stats = {
            'total_requests': 0,
            'rejected_requests': 0,
            'unique_users': set(),
            'peak_requests_per_minute': 0,
            'last_reset': time.time()
        }
        self._lock = asyncio.Lock()
        
        # Global rate limits
        self._global_limits = {
            'requests_per_minute': MAX_GLOBAL_REQUESTS_PER_MINUTE,
            'user_commands_per_5min': MAX_USER_COMMANDS_PER_5MIN,
            'heavy_commands_per_hour': MAX_HEAVY_COMMANDS_PER_HOUR,
            'admin_commands_per_hour': MAX_ADMIN_COMMANDS_PER_HOUR,
            'global_user_limit_per_5min': 150  # New: absolute global limit per user
        }
        
        # Command classifications
        self._heavy_commands = {
            'plot_user', 'plot_server', 'plot_godpacks', 'plot_probability',
            'sync_sheets', 'system_status', 'create_backup'
        }
        
        self._admin_commands = {
            'force_expire', 'extend_expiration', 'system_status', 'sync_sheets',
            'create_backup', 'list_backups', 'rate_limit_stats', 'reset_user_rate_limits'
        }
    
    async def check_rate_limit(self, user_id: int, command_type: str, 
                              max_uses: int, time_window: int) -> Tuple[bool, float, Optional[str]]:
        """
        Enhanced rate limit check with global limits and abuse detection
        Returns: (is_allowed, retry_after, reason)
        """
        async with self._lock:
            now = time.time()
            
            # Update global stats
            self._global_stats['total_requests'] += 1
            self._global_stats['unique_users'].add(user_id)
            
            # Check global server limit
            global_allowed, global_retry = await self._check_global_limit(now)
            if not global_allowed:
                self._global_stats['rejected_requests'] += 1
                return False, global_retry, "Server rate limit exceeded"
            
            # Check global user limit (NEW)
            global_user_allowed, global_user_retry = await self._check_global_user_limit(user_id, now)
            if not global_user_allowed:
                self._user_violations[user_id] += 1
                return False, global_user_retry, "Global user rate limit exceeded"
            
            # Check user global limit
            user_global_allowed, user_global_retry = await self._check_user_global_limit(user_id, now)
            if not user_global_allowed:
                self._user_violations[user_id] += 1
                return False, user_global_retry, "User global rate limit exceeded"
            
            # Check command-specific heavy limits
            if command_type in self._heavy_commands:
                heavy_allowed, heavy_retry = await self._check_heavy_command_limit(user_id, now)
                if not heavy_allowed:
                    return False, heavy_retry, "Heavy command rate limit exceeded"
            
            # Check admin command limits
            if command_type in self._admin_commands:
                admin_allowed, admin_retry = await self._check_admin_command_limit(user_id, now)
                if not admin_allowed:
                    return False, admin_retry, "Admin command rate limit exceeded"
            
            # Check abuse patterns
            if await self._check_abuse_pattern(user_id, now):
                return False, 300.0, "Abuse pattern detected - temporary cooldown"
            
            # Standard bucket-based rate limiting
            bucket_key = f"{user_id}:{command_type}"
            
            if bucket_key not in self._buckets:
                self._buckets[bucket_key] = []
            
            # Clean old entries
            self._buckets[bucket_key] = [
                timestamp for timestamp in self._buckets[bucket_key]
                if now - timestamp < time_window
            ]
            
            # Check rate limit
            if len(self._buckets[bucket_key]) >= max_uses:
                oldest = min(self._buckets[bucket_key])
                retry_after = time_window - (now - oldest)
                return False, retry_after, f"Command rate limit exceeded"
            
            # Add current request
            self._buckets[bucket_key].append(now)
            return True, 0.0, None

    async def _check_global_limit(self, now: float) -> Tuple[bool, float]:
        """Check server-wide rate limit"""
        # Clean up old global requests
        minute_ago = now - 60
        
        # Count requests in last minute (simplified - in production, use more sophisticated tracking)
        recent_requests = sum(1 for bucket in self._buckets.values() 
                            for timestamp in bucket if timestamp > minute_ago)
        
        if recent_requests >= self._global_limits['requests_per_minute']:
            return False, 60.0
        
        return True, 0.0
    
    async def _check_global_user_limit(self, user_id: int, now: float) -> Tuple[bool, float]:
        """Check absolute global user limit across ALL command types"""
        user_buckets = [bucket for key, bucket in self._buckets.items() 
                       if key.startswith(f"{user_id}:")]
        
        # Count ALL user requests in last 5 minutes
        five_minutes_ago = now - 300
        total_user_requests = sum(1 for bucket in user_buckets 
                                for timestamp in bucket if timestamp > five_minutes_ago)
        
        if total_user_requests >= self._global_limits['global_user_limit_per_5min']:
            oldest_request = min((timestamp for bucket in user_buckets 
                                for timestamp in bucket if timestamp > five_minutes_ago), 
                               default=now)
            retry_after = 300 - (now - oldest_request)
            return False, retry_after
        
        return True, 0.0
    
    async def _check_user_global_limit(self, user_id: int, now: float) -> Tuple[bool, float]:
        """Check user's global command rate limit"""
        user_buckets = [bucket for key, bucket in self._buckets.items() 
                       if key.startswith(f"{user_id}:")]
        
        # Count user requests in last 5 minutes
        five_minutes_ago = now - 300
        user_requests = sum(1 for bucket in user_buckets 
                          for timestamp in bucket if timestamp > five_minutes_ago)
        
        if user_requests >= self._global_limits['user_commands_per_5min']:
            oldest_request = min((timestamp for bucket in user_buckets 
                                for timestamp in bucket if timestamp > five_minutes_ago), 
                               default=now)
            retry_after = 300 - (now - oldest_request)
            return False, retry_after
        
        return True, 0.0
    
    async def _check_heavy_command_limit(self, user_id: int, now: float) -> Tuple[bool, float]:
        """Check heavy command rate limit"""
        heavy_buckets = [bucket for key, bucket in self._buckets.items() 
                        if key.startswith(f"{user_id}:") and 
                        any(cmd in key for cmd in self._heavy_commands)]
        
        # Count heavy commands in last hour
        hour_ago = now - 3600
        heavy_requests = sum(1 for bucket in heavy_buckets 
                           for timestamp in bucket if timestamp > hour_ago)
        
        if heavy_requests >= self._global_limits['heavy_commands_per_hour']:
            oldest_request = min((timestamp for bucket in heavy_buckets 
                                for timestamp in bucket if timestamp > hour_ago), 
                               default=now)
            retry_after = 3600 - (now - oldest_request)
            return False, retry_after
        
        return True, 0.0
    
    async def _check_admin_command_limit(self, user_id: int, now: float) -> Tuple[bool, float]:
        """Check admin command rate limit"""
        admin_buckets = [bucket for key, bucket in self._buckets.items() 
                        if key.startswith(f"{user_id}:") and 
                        any(cmd in key for cmd in self._admin_commands)]
        
        # Count admin commands in last hour
        hour_ago = now - 3600
        admin_requests = sum(1 for bucket in admin_buckets 
                           for timestamp in bucket if timestamp > hour_ago)
        
        if admin_requests >= self._global_limits['admin_commands_per_hour']:
            oldest_request = min((timestamp for bucket in admin_buckets 
                                for timestamp in bucket if timestamp > hour_ago), 
                               default=now)
            retry_after = 3600 - (now - oldest_request)
            return False, retry_after
        
        return True, 0.0
    
    async def _check_abuse_pattern(self, user_id: int, now: float) -> bool:
        """Detect potential abuse patterns"""
        # Check violation count
        if self._user_violations[user_id] >= 5:
            return True
        
        # Check rapid-fire requests (more than 10 requests in 10 seconds)
        user_buckets = [bucket for key, bucket in self._buckets.items() 
                       if key.startswith(f"{user_id}:")]
        
        ten_seconds_ago = now - 10
        recent_requests = sum(1 for bucket in user_buckets 
                            for timestamp in bucket if timestamp > ten_seconds_ago)
        
        if recent_requests > 10:
            self._user_violations[user_id] += 1
            return True
        
        return False

    async def get_user_command_stats(self, user_id: int) -> Dict:
        """Get detailed command usage stats for a specific user"""
        now = time.time()
        user_buckets = {key: bucket for key, bucket in self._buckets.items() 
                       if key.startswith(f"{user_id}:")}
        
        stats = {
            'total_commands_last_hour': 0,
            'total_commands_last_5min': 0,
            'heavy_commands_last_hour': 0,
            'admin_commands_last_hour': 0,
            'command_breakdown': {},
            'violations': self._user_violations[user_id]
        }
        
        hour_ago = now - 3600
        five_minutes_ago = now - 300
        
        for key, bucket in user_buckets.items():
            command = key.split(':', 1)[1]
            
            # Count by time periods
            last_hour = sum(1 for ts in bucket if ts > hour_ago)
            last_5min = sum(1 for ts in bucket if ts > five_minutes_ago)
            
            stats['total_commands_last_hour'] += last_hour
            stats['total_commands_last_5min'] += last_5min
            
            if command in self._heavy_commands:
                stats['heavy_commands_last_hour'] += last_hour
            
            if command in self._admin_commands:
                stats['admin_commands_last_hour'] += last_hour
            
            stats['command_breakdown'][command] = {
                'last_hour': last_hour,
                'last_5min': last_5min,
                'total_in_bucket': len(bucket)
            }
        
        return stats
    
    def get_rate_limit_stats(self) -> Dict:
        """Get rate limiting statistics"""
        now = time.time()
        uptime = now - self._global_stats['last_reset']
        
        return {
            'total_requests': self._global_stats['total_requests'],
            'rejected_requests': self._global_stats['rejected_requests'],
            'rejection_rate': (self._global_stats['rejected_requests'] / 
                             max(1, self._global_stats['total_requests'])) * 100,
            'unique_users': len(self._global_stats['unique_users']),
            'uptime_hours': uptime / 3600,
            'requests_per_hour': self._global_stats['total_requests'] / max(1, uptime / 3600),
            'active_buckets': len(self._buckets),
            'users_with_violations': len(self._user_violations),
            'global_limits': self._global_limits
        }
    
    def reset_user_violations(self, user_id: int):
        """Reset violations for a user (admin function)"""
        if user_id in self._user_violations:
            del self._user_violations[user_id]
    
    def cleanup_expired_buckets(self):
        """Clean up expired rate limit buckets"""
        now = time.time()
        expired_keys = []
        
        for key, bucket in self._buckets.items():
            # Remove timestamps older than 1 hour
            bucket[:] = [ts for ts in bucket if now - ts < 3600]
            if not bucket:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self._buckets[key]

# Enhanced rate limiter instance
enhanced_rate_limiter = EnhancedRateLimiter()

def enhanced_rate_limit(command_type: str, max_uses: int = 1, time_window: int = 60):
    """Enhanced rate limiting decorator with global limits and abuse detection"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, interaction_or_ctx, *args, **kwargs):
            # Determine if this is an interaction (slash) or context (prefix)
            if hasattr(interaction_or_ctx, 'response'):
                # This is an interaction (slash command)
                user_id = interaction_or_ctx.user.id
                is_interaction = True
            else:
                # This is a context (prefix command)
                user_id = interaction_or_ctx.author.id
                is_interaction = False
            
            # Check enhanced rate limit
            allowed, retry_after, reason = await enhanced_rate_limiter.check_rate_limit(
                user_id, command_type, max_uses, time_window
            )
            
            if not allowed:
                embed = discord.Embed(
                    title="⏳ Rate Limited",
                    description=f"**Reason:** {reason}\n**Try again in:** {retry_after:.1f} seconds",
                    color=discord.Color.orange()
                )
                
                # Add helpful information
                if "global" in reason.lower():
                    embed.add_field(
                        name="ℹ️ Info",
                        value="Server is experiencing high load. Please try again later.",
                        inline=False
                    )
                elif "abuse" in reason.lower():
                    embed.add_field(
                        name="⚠️ Warning",
                        value="Suspicious activity detected. Contact an admin if this persists.",
                        inline=False
                    )
                
                if is_interaction:
                    await interaction_or_ctx.response.send_message(embed=embed, ephemeral=True)
                else:
                    await interaction_or_ctx.reply(embed=embed)
                return
            
            # Execute command
            return await func(self, interaction_or_ctx, *args, **kwargs)
        return wrapper
    return decorator

class EnhancedBotCommands(commands.Cog):
    def __init__(self, bot, db_manager):
        self.bot = bot
        self.db = db_manager
        self.logger = logging.getLogger(__name__)
        
        # Lazy-loaded components
        self._probability_calc = None
        self._analytics = None
        self._plotting = None
        self._expiration_manager = None
        self._sheets_integration = None
        
        # Command usage tracking for analytics
        self._command_usage = {}
    
    async def cog_before_invoke(self, ctx):
        """Track command usage before each invocation"""
        # Handle both interaction (slash) and context (prefix) commands
        if hasattr(ctx, 'interaction') and ctx.interaction:
            # Slash command
            command_name = ctx.interaction.command.name if ctx.interaction.command else "unknown"
            user_id = ctx.interaction.user.id
        else:
            # Prefix command
            command_name = ctx.command.name if ctx.command else "unknown"
            user_id = ctx.author.id
        
        if command_name not in self._command_usage:
            self._command_usage[command_name] = {
                'count': 0,
                'last_used': None,
                'users': set()
            }
        
        self._command_usage[command_name]['count'] += 1
        self._command_usage[command_name]['last_used'] = datetime.now()
        self._command_usage[command_name]['users'].add(user_id)
    
    async def cog_after_invoke(self, ctx):
        """Log successful command execution"""
        # Handle both interaction (slash) and context (prefix) commands
        if hasattr(ctx, 'interaction') and ctx.interaction:
            # Slash command
            command_name = ctx.interaction.command.name if ctx.interaction.command else "unknown"
            user = ctx.interaction.user
        else:
            # Prefix command
            command_name = ctx.command.name if ctx.command else "unknown"
            user = ctx.author
        
        self.logger.info(f"Command {command_name} executed by {user} ({user.id})")
    
    async def cog_command_error(self, ctx, error: commands.CommandError):
        """Handle command errors"""
        if isinstance(error, commands.CommandOnCooldown):
            # Handle both interaction and context
            if hasattr(ctx, 'interaction') and ctx.interaction:
                await ctx.interaction.response.send_message(
                    f"⏳ Command on cooldown. Try again in {error.retry_after:.1f} seconds.",
                    ephemeral=True
                )
            else:
                await ctx.reply(
                    f"⏳ Command on cooldown. Try again in {error.retry_after:.1f} seconds."
                )
        else:
            command_name = getattr(ctx.command, 'name', 'unknown') if hasattr(ctx, 'command') else 'unknown'
            self.logger.error(f"Command error in {command_name}: {error}")
            
            error_msg = "❌ An error occurred while processing your command."
            
            if hasattr(ctx, 'interaction') and ctx.interaction:
                if not ctx.interaction.response.is_done():
                    await ctx.interaction.response.send_message(error_msg, ephemeral=True)
            else:
                await ctx.reply(error_msg)

    # Lazy-loaded properties
    
    @property
    def probability_calc(self):
        """Lazy load ProbabilityCalculator"""
        if self._probability_calc is None:
            try:
                from probability_calculator import ProbabilityCalculator
                self._probability_calc = ProbabilityCalculator(self.db)
            except ImportError:
                self.logger.warning("ProbabilityCalculator not available")
                return None
        return self._probability_calc

    @property
    def analytics(self):
        """Lazy load HeartbeatAnalytics"""
        if self._analytics is None:
            try:
                from heartbeat_analytics import HeartbeatAnalytics
                self._analytics = HeartbeatAnalytics(self.db)
            except ImportError:
                self.logger.warning("HeartbeatAnalytics not available")
                return None
        return self._analytics

    @property
    def plotting(self):
        """Lazy load enhanced plotting system"""
        if self._plotting is None:
            try:
                from plotting_system import create_plotting_system
                self._plotting = create_plotting_system(self.db)
            except ImportError:
                self.logger.warning("Plotting system not available")
                return None
        return self._plotting

    @property
    def expiration_manager(self):
        """Lazy load ExpirationManager"""
        if self._expiration_manager is None:
            try:
                from expiration_manager import ExpirationManager
                self._expiration_manager = ExpirationManager(self.db, self.bot)
            except ImportError:
                self.logger.warning("ExpirationManager not available")
                return None
        return self._expiration_manager

    @property
    def sheets_integration(self):
        """Lazy load GoogleSheetsIntegration"""
        if self._sheets_integration is None:
            try:
                from google_sheets_integration import GoogleSheetsIntegration
                self._sheets_integration = GoogleSheetsIntegration(self.db)
            except ImportError:
                self.logger.warning("GoogleSheetsIntegration not available")
                return None
        return self._sheets_integration

    # ========================================================================================
    # STATUS COMMANDS - BOTH PREFIX AND SLASH VERSIONS
    # ========================================================================================
    
    # Shared status update logic
    async def _update_user_status(self, user_id: int, display_name: str, status: str, is_interaction: bool = True):
        """Shared logic for updating user status"""
        if not self.db or not hasattr(self.db, 'update_user_status'):
            return None, "❌ Enhanced system not available."
        
        success = self.db.update_user_status(user_id, status)
        
        status_colors = {
            'active': discord.Color.green(),
            'inactive': discord.Color.orange(),
            'farm': discord.Color.blue(),
            'leech': discord.Color.purple()
        }
        
        status_descriptions = {
            'active': {
                'title': "✅ Status Updated",
                'desc': "You are now marked as **Active**",
                'info': "🎮 Active Status",
                'details': "You'll receive priority for:\n• God pack notifications\n• Reroll coordination\n• Event participation"
            },
            'inactive': {
                'title': "✅ Status Updated",
                'desc': "You are now marked as **Inactive**",
                'info': "😴 Inactive Status",
                'details': "You'll receive reduced notifications and won't be included in active user counts."
            },
            'farm': {
                'title': "✅ Status Updated",
                'desc': "You are now marked as **Farm**",
                'info': "🚜 Farm Status",
                'details': "You're focused on farming and grinding. You'll receive:\n• Farming tips and strategies\n• Resource optimization alerts\n• Efficiency notifications"
            },
            'leech': {
                'title': "✅ Status Updated",
                'desc': "You are now marked as **Leech**",
                'info': "🔄 Leech Status",
                'details': "You're looking for reroll opportunities. You'll receive:\n• Reroll notifications\n• God pack sharing opportunities\n• Account coordination alerts"
            }
        }
        
        if success:
            embed = discord.Embed(
                title=status_descriptions[status]['title'],
                description=status_descriptions[status]['desc'],
                color=status_colors[status]
            )
            
            embed.add_field(
                name=status_descriptions[status]['info'],
                value=status_descriptions[status]['details'],
                inline=False
            )
        else:
            # User doesn't exist, create them
            self.db.add_user(user_id, display_name=display_name)
            self.db.update_user_status(user_id, status)
            
            embed = discord.Embed(
                title=status_descriptions[status]['title'],
                description=f"{status_descriptions[status]['desc']}\n\n*New user profile created!*",
                color=status_colors[status]
            )
            
            if status == 'active':
                embed.add_field(
                    name="🆕 Getting Started",
                    value=f"Consider setting your player ID with `{'/' if is_interaction else '!'}setplayerid <your_id>` for full functionality.",
                    inline=False
                )
        
        if status == 'inactive':
            embed.add_field(
                name="🔄 Return Anytime",
                value=f"Use `{'/' if is_interaction else '!'}active` when you're ready to participate again!",
                inline=False
            )
        
        return embed, None

    # ACTIVE STATUS COMMANDS
    @app_commands.command(name="active", description="Set your status to active")
    @enhanced_rate_limit("status", 5, 60)
    async def set_active_slash(self, interaction: discord.Interaction):
        """Set yourself as active (slash command version)"""
        try:
            embed, error = await self._update_user_status(
                interaction.user.id, interaction.user.display_name, 'active', True
            )
            
            if error:
                await interaction.response.send_message(error, ephemeral=True)
            else:
                await interaction.response.send_message(embed=embed, ephemeral=True)
                self.logger.info(f"User {interaction.user} set to active via slash command")
                
        except Exception as e:
            self.logger.error(f"Error in active slash command: {e}")
            await interaction.response.send_message("❌ Error updating status.", ephemeral=True)
    
    @commands.command(name='active')
    @commands.cooldown(1, COMMAND_COOLDOWN, commands.BucketType.user)
    async def set_active_prefix(self, ctx):
        """Set yourself as active (prefix command version)"""
        try:
            embed, error = await self._update_user_status(
                ctx.author.id, ctx.author.display_name, 'active', False
            )
            
            if error:
                await ctx.reply(error)
            else:
                await ctx.reply(embed=embed)
                self.logger.info(f"User {ctx.author} set to active via prefix command")
                
        except Exception as e:
            self.logger.error(f"Error in active prefix command: {e}")
            await ctx.reply("❌ Error updating status.")
    
    # INACTIVE STATUS COMMANDS
    @app_commands.command(name="inactive", description="Set your status to inactive")
    @enhanced_rate_limit("status", 5, 60)
    async def set_inactive_slash(self, interaction: discord.Interaction):
        """Set yourself as inactive (slash command version)"""
        try:
            embed, error = await self._update_user_status(
                interaction.user.id, interaction.user.display_name, 'inactive', True
            )
            
            if error:
                await interaction.response.send_message(error, ephemeral=True)
            else:
                await interaction.response.send_message(embed=embed, ephemeral=True)
                self.logger.info(f"User {interaction.user} set to inactive via slash command")
                
        except Exception as e:
            self.logger.error(f"Error in inactive slash command: {e}")
            await interaction.response.send_message("❌ Error updating status.", ephemeral=True)
    
    @commands.command(name='inactive')
    @commands.cooldown(1, COMMAND_COOLDOWN, commands.BucketType.user)
    async def set_inactive_prefix(self, ctx):
        """Set yourself as inactive (prefix command version)"""
        try:
            embed, error = await self._update_user_status(
                ctx.author.id, ctx.author.display_name, 'inactive', False
            )
            
            if error:
                await ctx.reply(error)
            else:
                await ctx.reply(embed=embed)
                self.logger.info(f"User {ctx.author} set to inactive via prefix command")
                
        except Exception as e:
            self.logger.error(f"Error in inactive prefix command: {e}")
            await ctx.reply("❌ Error updating status.")

    # FARM STATUS COMMANDS
    @app_commands.command(name="farm", description="Set your status to farm")
    @enhanced_rate_limit("status", 5, 60)
    async def set_farm_slash(self, interaction: discord.Interaction):
        """Set yourself as farm status (slash command version)"""
        try:
            embed, error = await self._update_user_status(
                interaction.user.id, interaction.user.display_name, 'farm', True
            )
            
            if error:
                await interaction.response.send_message(error, ephemeral=True)
            else:
                await interaction.response.send_message(embed=embed, ephemeral=True)
                self.logger.info(f"User {interaction.user} set to farm via slash command")
                
        except Exception as e:
            self.logger.error(f"Error in farm slash command: {e}")
            await interaction.response.send_message("❌ Error updating status.", ephemeral=True)
    
    @commands.command(name='farm')
    @commands.cooldown(1, COMMAND_COOLDOWN, commands.BucketType.user)
    async def set_farm_prefix(self, ctx):
        """Set yourself as farm status (prefix command version)"""
        try:
            embed, error = await self._update_user_status(
                ctx.author.id, ctx.author.display_name, 'farm', False
            )
            
            if error:
                await ctx.reply(error)
            else:
                await ctx.reply(embed=embed)
                self.logger.info(f"User {ctx.author} set to farm via prefix command")
                
        except Exception as e:
            self.logger.error(f"Error in farm prefix command: {e}")
            await ctx.reply("❌ Error updating status.")
    
    # LEECH STATUS COMMANDS
    @app_commands.command(name="leech", description="Set your status to leech")
    @enhanced_rate_limit("status", 5, 60)
    async def set_leech_slash(self, interaction: discord.Interaction):
        """Set yourself as leech status (slash command version)"""
        try:
            embed, error = await self._update_user_status(
                interaction.user.id, interaction.user.display_name, 'leech', True
            )
            
            if error:
                await interaction.response.send_message(error, ephemeral=True)
            else:
                await interaction.response.send_message(embed=embed, ephemeral=True)
                self.logger.info(f"User {interaction.user} set to leech via slash command")
                
        except Exception as e:
            self.logger.error(f"Error in leech slash command: {e}")
            await interaction.response.send_message("❌ Error updating status.", ephemeral=True)
    
    @commands.command(name='leech')
    @commands.cooldown(1, COMMAND_COOLDOWN, commands.BucketType.user)
    async def set_leech_prefix(self, ctx):
        """Set yourself as leech status (prefix command version)"""
        try:
            embed, error = await self._update_user_status(
                ctx.author.id, ctx.author.display_name, 'leech', False
            )
            
            if error:
                await ctx.reply(error)
            else:
                await ctx.reply(embed=embed)
                self.logger.info(f"User {ctx.author} set to leech via prefix command")
                
        except Exception as e:
            self.logger.error(f"Error in leech prefix command: {e}")
            await ctx.reply("❌ Error updating status.")

    # ========================================================================================
    # PLAYER ID COMMANDS - BOTH PREFIX AND SLASH VERSIONS
    # ========================================================================================
    
    async def _set_player_id_logic(self, user_id: int, display_name: str, player_id: str, is_interaction: bool = True):
        """Shared logic for setting player ID"""
        if not self.db or not hasattr(self.db, 'add_user'):
            return None, "❌ Enhanced system not available."
        
        # Basic validation
        if not player_id or len(player_id.strip()) == 0:
            return None, "❌ Player ID cannot be empty."
            
        player_id = player_id.strip()
        
        # Validate player ID format if configured
        try:
            if hasattr(config, 'PLAYER_ID_PATTERN'):
                import re
                if not re.match(config.PLAYER_ID_PATTERN, player_id):
                    return None, f"❌ Player ID format is invalid. Expected format: {getattr(config, 'PLAYER_ID_FORMAT_DESCRIPTION', 'Valid player ID')}"
        except:
            pass  # Skip validation if pattern check fails
        
        # Check if player ID is already taken
        if hasattr(self.db, 'get_user_by_player_id'):
            existing_user = self.db.get_user_by_player_id(player_id)
            if existing_user and existing_user.get('discord_id') != user_id:
                return None, "❌ This player ID is already registered to another user."
        
        # Add or update user
        self.db.add_user(user_id, player_id=player_id, display_name=display_name)
        
        embed = discord.Embed(
            title="✅ Player ID Updated",
            description=f"Your player ID has been set to: **{player_id}**",
            color=discord.Color.green()
        )
        
        embed.add_field(
            name="📱 What's Next?",
            value=f"You can now use enhanced features like:\n• `{'/' if is_interaction else '!'}active` - Set your status\n• Track your god pack progress\n• Access detailed analytics",
            inline=False
        )
        
        return embed, None
    
    @app_commands.command(name="setplayerid", description="Set your player ID")
    @app_commands.describe(player_id="Your player ID")
    @enhanced_rate_limit("status", 5, 60)
    async def set_player_id_slash(self, interaction: discord.Interaction, player_id: str):
        """Set your player ID (slash command version)"""
        try:
            embed, error = await self._set_player_id_logic(
                interaction.user.id, interaction.user.display_name, player_id, True
            )
            
            if error:
                await interaction.response.send_message(error, ephemeral=True)
            else:
                await interaction.response.send_message(embed=embed, ephemeral=True)
                self.logger.info(f"Player ID set for {interaction.user}: {player_id}")
                
        except Exception as e:
            self.logger.error(f"Error setting player ID via slash command: {e}")
            await interaction.response.send_message("❌ Error setting player ID.", ephemeral=True)
    
    @commands.command(name='setplayerid')
    @commands.cooldown(1, COMMAND_COOLDOWN, commands.BucketType.user)
    async def set_player_id_prefix(self, ctx, *, player_id: str):
        """Set your player ID (prefix command version)"""
        try:
            embed, error = await self._set_player_id_logic(
                ctx.author.id, ctx.author.display_name, player_id, False
            )
            
            if error:
                await ctx.reply(error)
            else:
                await ctx.reply(embed=embed)
                self.logger.info(f"Player ID set for {ctx.author}: {player_id}")
                
        except Exception as e:
            self.logger.error(f"Error setting player ID via prefix command: {e}")
            await ctx.reply("❌ Error setting player ID.")

    # ========================================================================================
    # MY STATUS COMMANDS - BOTH PREFIX AND SLASH VERSIONS
    # ========================================================================================
    
    async def _get_user_status_logic(self, user_id: int, display_name: str, avatar_url: str, is_interaction: bool = True):
        """Shared logic for getting user status"""
        if not self.db or not hasattr(self.db, 'get_user'):
            return None, "❌ Enhanced system not available."
        
        user_data = self.db.get_user(user_id)
        
        if not user_data:
            embed = discord.Embed(
                title="❌ Profile Not Found",
                description="You don't have a profile yet!",
                color=discord.Color.red()
            )
            
            embed.add_field(
                name="🚀 Get Started",
                value=f"Use `{'/' if is_interaction else '!'}setplayerid <your_id>` to create your profile\nOr use `{'/' if is_interaction else '!'}active` to start with basic setup",
                inline=False
            )
            
            return embed, None
        
        # Status color mapping
        status_colors = {
            'active': discord.Color.green(),
            'inactive': discord.Color.orange(),
            'farm': discord.Color.blue(),
            'leech': discord.Color.purple()
        }
        
        status = user_data.get('status', 'unknown')
        embed = discord.Embed(
            title="📱 Your Status Profile",
            color=status_colors.get(status, discord.Color.grey()),
            timestamp=datetime.now()
        )
        
        embed.set_thumbnail(url=avatar_url)
        
        # Basic info
        embed.add_field(
            name="🆔 Player ID",
            value=user_data.get('player_id', 'Not set'),
            inline=True
        )
        
        embed.add_field(
            name="📊 Status",
            value=status.title(),
            inline=True
        )
        
        embed.add_field(
            name="📅 Registered",
            value=user_data.get('created_at', 'Unknown'),
            inline=True
        )
        
        # Activity info
        if 'last_activity' in user_data:
            embed.add_field(
                name="⏰ Last Activity",
                value=user_data['last_activity'],
                inline=True
            )
        
        # Statistics if available
        if hasattr(self.db, 'get_user_statistics'):
            try:
                stats = self.db.get_user_statistics(user_id)
                if stats and stats.get('user_info'):
                    user_info = stats['user_info']
                    embed.add_field(
                        name="📈 Statistics",
                        value=f"Total Packs: {user_info.get('total_packs', 0)}\nGod Packs: {user_info.get('total_gps', 0)}",
                        inline=True
                    )
            except Exception as e:
                self.logger.debug(f"Could not get user stats: {e}")
        
        # Status descriptions
        status_descriptions = {
            'active': "🟢 You'll receive all notifications and priority access to features.",
            'inactive': "🟠 You'll receive minimal notifications and reduced feature access.",
            'farm': "🔵 You're focused on farming and will receive farming-related updates.",
            'leech': "🟣 You're looking for reroll opportunities and account coordination."
        }
        
        if status in status_descriptions:
            embed.add_field(
                name="ℹ️ Status Info",
                value=status_descriptions[status],
                inline=False
            )
        
        embed.set_footer(text=f"User ID: {user_id}")
        return embed, None
    
    @app_commands.command(name="mystatus", description="Show your current status and profile")
    @enhanced_rate_limit("query", 5, 60)
    async def my_status_slash(self, interaction: discord.Interaction):
        """Show your current status and profile info (slash command version)"""
        try:
            embed, error = await self._get_user_status_logic(
                interaction.user.id, interaction.user.display_name, 
                interaction.user.display_avatar.url, True
            )
            
            if error:
                await interaction.response.send_message(error, ephemeral=True)
            else:
                await interaction.response.send_message(embed=embed, ephemeral=True)
                
        except Exception as e:
            self.logger.error(f"Error in mystatus slash command: {e}")
            await interaction.response.send_message("❌ Error retrieving status information.", ephemeral=True)
    
    @commands.command(name='mystatus')
    @commands.cooldown(1, COMMAND_COOLDOWN, commands.BucketType.user)
    async def my_status_prefix(self, ctx):
        """Show your current status and profile info (prefix command version)"""
        try:
            embed, error = await self._get_user_status_logic(
                ctx.author.id, ctx.author.display_name, 
                ctx.author.display_avatar.url, False
            )
            
            if error:
                await ctx.reply(error)
            else:
                await ctx.reply(embed=embed)
                
        except Exception as e:
            self.logger.error(f"Error in mystatus prefix command: {e}")
            await ctx.reply("❌ Error retrieving status information.")

    # ========================================================================================
    # PROBABILITY COMMANDS - BOTH PREFIX AND SLASH VERSIONS
    # ========================================================================================
    
    async def _calculate_probability_logic(self, gp_id: int):
        """Shared logic for probability calculation"""
        if not self.probability_calc:
            return None, "❌ Probability calculator not available."
        
        # Import required enums locally
        try:
            from database_manager import GPState, TestType
        except ImportError:
            return None, "❌ Database manager not available."
        
        try:
            result = self.probability_calc.calculate_godpack_probability(gp_id, force_recalculate=True)
            summary = self.probability_calc.get_probability_summary(gp_id)
            
            embed = discord.Embed(
                title=f"🎯 Probability Analysis - {summary['godpack'].name}",
                color=self._get_probability_color(result.probability_alive),
                timestamp=datetime.now()
            )
            
            embed.add_field(
                name="📊 Probability Alive",
                value=f"**{result.probability_alive:.1f}%**",
                inline=True
            )
            
            embed.add_field(
                name="🎯 Confidence Level",
                value=f"**{result.confidence_level:.1f}%**",
                inline=True
            )
            
            embed.add_field(
                name="🧪 Total Tests",
                value=f"**{result.total_tests}**",
                inline=True
            )
            
            embed.add_field(
                name="❌ Miss Tests",
                value=f"{result.miss_tests}",
                inline=True
            )
            
            embed.add_field(
                name="👻 No-Show Tests",
                value=f"{result.noshow_tests}",
                inline=True
            )
            
            embed.add_field(
                name="💡 Recommendation",
                value=summary['recommendation'],
                inline=False
            )
            
            # Add member breakdown if available
            if summary['member_details']:
                member_text = []
                for member_id, details in list(summary['member_details'].items())[:5]:
                    member_text.append(f"**{details['name']}**: {details['probability']:.1f}% ({len(details['tests'])} tests)")
                
                if member_text:
                    embed.add_field(
                        name="👥 Member Breakdown",
                        value="\n".join(member_text),
                        inline=False
                    )
            
            embed.set_footer(text=f"GP ID: {gp_id} | Last calculated: {result.last_calculated.strftime('%H:%M:%S')}")
            
            return embed, None
            
        except Exception as e:
            self.logger.error(f"Error calculating probability for GP {gp_id}: {e}")
            return None, "❌ Error calculating probability. Please check the GP ID."
    
    @app_commands.command(name="probability", description="Calculate probability for a god pack")
    @app_commands.describe(gp_id="The ID of the god pack to check")
    @enhanced_rate_limit("probability", MAX_PROBABILITY_CALCULATIONS_PER_MINUTE, 60)
    async def probability_slash(self, interaction: discord.Interaction, gp_id: int):
        """Calculate and display the probability that a god pack is alive"""
        try:
            await interaction.response.defer()
            
            embed, error = await self._calculate_probability_logic(gp_id)
            
            if error:
                await interaction.followup.send(error)
            else:
                await interaction.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in probability slash command: {e}")
            await interaction.followup.send("❌ Error calculating probability.")
    
    @commands.command(name='probability')
    @commands.cooldown(1, COMMAND_COOLDOWN, commands.BucketType.user)
    async def probability_prefix(self, ctx, gp_id: int):
        """Calculate and display the probability that a god pack is alive"""
        try:
            embed, error = await self._calculate_probability_logic(gp_id)
            
            if error:
                await ctx.reply(error)
            else:
                await ctx.reply(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in probability prefix command: {e}")
            await ctx.reply("❌ Error calculating probability.")

    # ========================================================================================
    # TEST COMMANDS - BOTH PREFIX AND SLASH VERSIONS
    # ========================================================================================
    
    async def _add_miss_test_logic(self, user_id: int, gp_id: int):
        """Shared logic for adding miss test"""
        if not self.probability_calc:
            return None, "❌ Probability calculator not available."
        
        try:
            from database_manager import TestType
        except ImportError:
            return None, "❌ Database manager not available."
        
        try:
            result = self.probability_calc.add_test_and_calculate(user_id, gp_id, TestType.MISS)
            
            embed = discord.Embed(
                title="❌ Miss Test Added",
                description=f"Updated probability: **{result.probability_alive:.1f}%**",
                color=discord.Color.red(),
                timestamp=datetime.now()
            )
            
            embed.add_field(
                name="Impact",
                value=f"Total tests: {result.total_tests}\nConfidence: {result.confidence_level:.1f}%",
                inline=False
            )
            
            embed.set_footer(text=f"GP ID: {gp_id}")
            
            return embed, None
            
        except Exception as e:
            self.logger.error(f"Error adding miss test for GP {gp_id}: {e}")
            return None, "❌ Error adding miss test."
    
    @app_commands.command(name="miss", description="Add a miss test for a god pack")
    @app_commands.describe(gp_id="The ID of the god pack that was missed")
    @enhanced_rate_limit("test", 10, 60)
    async def miss_slash(self, interaction: discord.Interaction, gp_id: int):
        """Add a miss test and calculate updated probability"""
        try:
            await interaction.response.defer()
            
            embed, error = await self._add_miss_test_logic(interaction.user.id, gp_id)
            
            if error:
                await interaction.followup.send(error)
            else:
                await interaction.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in miss slash command: {e}")
            await interaction.followup.send("❌ Error adding miss test.")
    
    @commands.command(name='miss')
    @commands.cooldown(1, COMMAND_COOLDOWN, commands.BucketType.user)
    async def miss_prefix(self, ctx, gp_id: int):
        """Add a miss test and calculate updated probability"""
        try:
            embed, error = await self._add_miss_test_logic(ctx.author.id, gp_id)
            
            if error:
                await ctx.reply(error)
            else:
                await ctx.reply(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in miss prefix command: {e}")
            await ctx.reply("❌ Error adding miss test.")
    
    async def _add_noshow_test_logic(self, user_id: int, gp_id: int, open_slots: int, number_friends: int):
        """Shared logic for adding no-show test"""
        # Validate inputs
        if open_slots < 0 or open_slots > 3:
            return None, "❌ Open slots must be between 0 and 3."
        
        if number_friends < 1:
            return None, "❌ Number of friends must be at least 1."
        
        if not self.probability_calc:
            return None, "❌ Probability calculator not available."
        
        try:
            from database_manager import TestType
        except ImportError:
            return None, "❌ Database manager not available."
        
        try:
            # Calculate no-show probability first
            noshow_prob = self.probability_calc.calculate_noshow_probability(open_slots, number_friends)
            
            result = self.probability_calc.add_test_and_calculate(
                user_id, gp_id, TestType.NOSHOW, open_slots, number_friends
            )
            
            embed = discord.Embed(
                title="👻 No-Show Test Added",
                description=f"Updated probability: **{result.probability_alive:.1f}%**",
                color=discord.Color.orange(),
                timestamp=datetime.now()
            )
            
            embed.add_field(
                name="Test Details",
                value=f"Open slots: {open_slots}\nFriends: {number_friends}\nNo-show impact: {noshow_prob*100:.1f}%",
                inline=True
            )
            
            embed.add_field(
                name="Overall Impact",
                value=f"Total tests: {result.total_tests}\nConfidence: {result.confidence_level:.1f}%",
                inline=True
            )
            
            embed.set_footer(text=f"GP ID: {gp_id}")
            
            return embed, None
            
        except Exception as e:
            self.logger.error(f"Error adding no-show test for GP {gp_id}: {e}")
            return None, "❌ Error adding no-show test."
    
    @app_commands.command(name="noshow", description="Add a no-show test with slot/friend data")
    @app_commands.describe(
        gp_id="The ID of the god pack",
        open_slots="Number of open friend slots",
        number_friends="Number of friends with the god pack"
    )
    @enhanced_rate_limit("test", 10, 60)
    async def noshow_slash(self, interaction: discord.Interaction, gp_id: int, open_slots: int, number_friends: int):
        """Add a no-show test with detailed probability calculation"""
        try:
            await interaction.response.defer()
            
            embed, error = await self._add_noshow_test_logic(
                interaction.user.id, gp_id, open_slots, number_friends
            )
            
            if error:
                await interaction.followup.send(error)
            else:
                await interaction.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in noshow slash command: {e}")
            await interaction.followup.send("❌ Error adding no-show test.")
    
    @commands.command(name='noshow')
    @commands.cooldown(1, COMMAND_COOLDOWN, commands.BucketType.user)
    async def noshow_prefix(self, ctx, gp_id: int, open_slots: int, number_friends: int):
        """Add a no-show test with detailed probability calculation"""
        try:
            embed, error = await self._add_noshow_test_logic(
                ctx.author.id, gp_id, open_slots, number_friends
            )
            
            if error:
                await ctx.reply(error)
            else:
                await ctx.reply(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in noshow prefix command: {e}")
            await ctx.reply("❌ Error adding no-show test.")

    # ========================================================================================
    # PLOTTING COMMANDS - BOTH PREFIX AND SLASH VERSIONS
    # ========================================================================================
    
    async def _plot_user_logic(self, user_id: int, display_name: str, days: int):
        """Shared logic for plotting user data"""
        if not self.plotting:
            return None, "❌ Plotting system not available.", None
        
        try:
            result = await self.plotting.plot_user_timeline(user_id, days)
            
            # Check if result is a file or text
            if isinstance(result, discord.File):
                embed = discord.Embed(
                    title=f"📈 User Activity - {display_name}",
                    description=f"Activity timeline for the last {days} days",
                    color=discord.Color.blue(),
                    timestamp=datetime.now()
                )
                
                if result.filename.endswith('.png'):
                    embed.set_image(url="attachment://" + result.filename)
                
                return embed, None, result
            else:
                # Text response
                return None, f"```\n{result}\n```", None
                
        except Exception as e:
            self.logger.error(f"Error plotting user data: {e}")
            return None, "❌ Error generating user plot.", None
    
    @app_commands.command(name="plot_user", description="Generate activity plot for a user")
    @app_commands.describe(
        user="The user to plot data for",
        days="Number of days to plot"
    )
    @enhanced_rate_limit("plot_user", MAX_PLOT_GENERATIONS_PER_HOUR, 3600)
    async def plot_user_slash(self, interaction: discord.Interaction,
                       user: Optional[discord.Member] = None,
                       days: app_commands.Range[int, 1, 30] = 7):
        """Generate user activity plot or text summary"""
        try:
            await interaction.response.defer()
            
            target_user = user or interaction.user
            embed, error_text, file = await self._plot_user_logic(target_user.id, target_user.display_name, days)
            
            if embed and file:
                embed.set_footer(text=f"Requested by {interaction.user}")
                await interaction.followup.send(embed=embed, file=file)
            elif error_text:
                await interaction.followup.send(error_text)
            
        except Exception as e:
            self.logger.error(f"Error in plot_user slash command: {e}")
            await interaction.followup.send("❌ Error generating user plot.")
    
    @commands.command(name='plot_user')
    @commands.cooldown(1, COMMAND_COOLDOWN * 3, commands.BucketType.user)  # Longer cooldown for plots
    async def plot_user_prefix(self, ctx, user: Optional[discord.Member] = None, days: int = 7):
        """Generate user activity plot or text summary"""
        try:
            if days < 1 or days > 30:
                await ctx.reply("❌ Days must be between 1 and 30.")
                return
            
            target_user = user or ctx.author
            embed, error_text, file = await self._plot_user_logic(target_user.id, target_user.display_name, days)
            
            if embed and file:
                embed.set_footer(text=f"Requested by {ctx.author}")
                await ctx.reply(embed=embed, file=file)
            elif error_text:
                await ctx.reply(error_text)
            
        except Exception as e:
            self.logger.error(f"Error in plot_user prefix command: {e}")
            await ctx.reply("❌ Error generating user plot.")
    
    @app_commands.command(name="plot_status", description="Check plotting system status")
    @enhanced_rate_limit("query", 5, 60)
    async def plot_status_slash(self, interaction: discord.Interaction):
        """Check plotting system status and capabilities"""
        try:
            await interaction.response.defer()
            
            if not self.plotting:
                await interaction.followup.send("❌ Plotting system not available.")
                return
            
            # Get system status
            if hasattr(self.plotting, 'get_system_status'):
                status = await self.plotting.get_system_status()
            else:
                status = {'plotting_available': False, 'fallback_active': True}
            
            embed = discord.Embed(
                title="📊 Plotting System Status",
                color=discord.Color.green() if status.get('plotting_available') else discord.Color.orange(),
                timestamp=datetime.now()
            )
            
            embed.add_field(
                name="🎨 Full Plotting",
                value="✅ Available" if status.get('plotting_available') else "❌ Unavailable",
                inline=True
            )
            
            embed.add_field(
                name="📝 Text Fallback",
                value="✅ Active" if status.get('fallback_active') else "⭕ Inactive",
                inline=True
            )
            
            embed.add_field(
                name="💾 Cache",
                value="✅ Enabled" if status.get('cache_enabled') else "❌ Disabled",
                inline=True
            )
            
            if status.get('missing_packages'):
                embed.add_field(
                    name="📦 Missing Packages",
                    value=", ".join(status['missing_packages']),
                    inline=False
                )
            
            if status.get('plotting_available'):
                embed.add_field(
                    name="ℹ️ Info",
                    value="Full matplotlib plotting available with charts and graphs.",
                    inline=False
                )
            else:
                embed.add_field(
                    name="ℹ️ Info", 
                    value="Using text-based charts as fallback. Install matplotlib, seaborn, and numpy for full plotting.",
                    inline=False
                )
            
            embed.set_footer(text=f"Requested by {interaction.user}")
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in plot_status slash command: {e}")
            await interaction.followup.send("❌ Error checking plotting status.")
    
    @commands.command(name='plot_status')
    @commands.cooldown(1, COMMAND_COOLDOWN, commands.BucketType.user)
    async def plot_status_prefix(self, ctx):
        """Check plotting system status and capabilities"""
        try:
            if not self.plotting:
                await ctx.reply("❌ Plotting system not available.")
                return
            
            # Get system status
            if hasattr(self.plotting, 'get_system_status'):
                status = await self.plotting.get_system_status()
            else:
                status = {'plotting_available': False, 'fallback_active': True}
            
            embed = discord.Embed(
                title="📊 Plotting System Status",
                color=discord.Color.green() if status.get('plotting_available') else discord.Color.orange(),
                timestamp=datetime.now()
            )
            
            embed.add_field(
                name="🎨 Full Plotting",
                value="✅ Available" if status.get('plotting_available') else "❌ Unavailable",
                inline=True
            )
            
            embed.add_field(
                name="📝 Text Fallback",
                value="✅ Active" if status.get('fallback_active') else "⭕ Inactive",
                inline=True
            )
            
            if status.get('plotting_available'):
                embed.add_field(
                    name="ℹ️ Info",
                    value="Full matplotlib plotting available.",
                    inline=False
                )
            else:
                embed.add_field(
                    name="ℹ️ Info", 
                    value="Using text-based fallback mode.",
                    inline=False
                )
            
            embed.set_footer(text=f"Requested by {ctx.author}")
            
            await ctx.reply(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in plot_status prefix command: {e}")
            await ctx.reply("❌ Error checking plotting status.")

    # ========================================================================================
    # BACKUP MANAGEMENT COMMANDS - BOTH PREFIX AND SLASH VERSIONS
    # ========================================================================================
    
    async def _create_backup_logic(self, user_id: int, description: str = "Manual backup"):
        """Shared logic for creating backups"""
        if not self.db or not hasattr(self.db, 'backup_manager'):
            return None, "❌ Backup system not available."
        
        try:
            from database_manager import BackupType
        except ImportError:
            return None, "❌ Backup system not available."
        
        try:
            backup_path = self.db.backup_manager.create_backup(
                backup_type=BackupType.MANUAL,
                description=description
            )
            
            if backup_path:
                backup_info = self.db.backup_manager.get_backup_info(backup_path)
                size_mb = backup_info['size_bytes'] / (1024 * 1024)
                
                embed = discord.Embed(
                    title="✅ Backup Created",
                    description=f"Database backup created successfully",
                    color=discord.Color.green(),
                    timestamp=datetime.now()
                )
                embed.add_field(name="Size", value=f"{size_mb:.2f} MB", inline=True)
                embed.add_field(name="Records", value=f"{sum(backup_info['record_counts'].values()):,}", inline=True)
                
                return embed, None
            else:
                return None, "❌ Failed to create backup."
                
        except Exception as e:
            self.logger.error(f"Error creating backup: {e}")
            return None, "❌ Error creating backup."
    
    @app_commands.command(name="create_backup", description="Create manual database backup (Admin only)")
    @app_commands.describe(description="Description for the backup")
    @app_commands.default_permissions(manage_guild=True)
    @enhanced_rate_limit("create_backup", 5, 300)
    async def create_backup_slash(self, interaction: discord.Interaction, description: str = "Manual backup"):
        """Create a manual database backup"""
        try:
            if not interaction.user.guild_permissions.manage_guild:
                await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
                return
            
            await interaction.response.defer()
            
            embed, error = await self._create_backup_logic(interaction.user.id, description)
            
            if error:
                await interaction.followup.send(error)
            else:
                embed.set_footer(text=f"Created by {interaction.user}")
                await interaction.followup.send(embed=embed)
                
        except Exception as e:
            self.logger.error(f"Error in create_backup slash command: {e}")
            await interaction.followup.send("❌ Error creating backup.")
    
    @commands.command(name='create_backup')
    @commands.has_permissions(manage_guild=True)
    @commands.cooldown(1, COMMAND_COOLDOWN * 5, commands.BucketType.user)
    async def create_backup_prefix(self, ctx, *, description: str = "Manual backup"):
        """Create a manual database backup"""
        try:
            embed, error = await self._create_backup_logic(ctx.author.id, description)
            
            if error:
                await ctx.reply(error)
            else:
                embed.set_footer(text=f"Created by {ctx.author}")
                await ctx.reply(embed=embed)
                
        except Exception as e:
            self.logger.error(f"Error in create_backup prefix command: {e}")
            await ctx.reply("❌ Error creating backup.")
    
    async def _list_backups_logic(self):
        """Shared logic for listing backups"""
        if not self.db or not hasattr(self.db, 'list_backups'):
            return None, "❌ Backup system not available."
        
        try:
            backups = self.db.list_backups()
            
            if not backups:
                return None, "📦 No backups available."
            
            embed = discord.Embed(
                title="📦 Database Backups",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )
            
            for i, backup in enumerate(backups[:10]):  # Show first 10
                timestamp = datetime.fromisoformat(backup['timestamp'])
                size_mb = backup['size_bytes'] / (1024 * 1024)
                
                embed.add_field(
                    name=f"{backup['type'].title()} - {timestamp.strftime('%Y-%m-%d %H:%M')}",
                    value=f"Size: {size_mb:.2f} MB\nRecords: {sum(backup['record_counts'].values()):,}",
                    inline=True
                )
            
            if len(backups) > 10:
                embed.set_footer(text=f"Showing 10 of {len(backups)} backups")
            
            return embed, None
            
        except Exception as e:
            self.logger.error(f"Error listing backups: {e}")
            return None, "❌ Error listing backups."
    
    @app_commands.command(name="list_backups", description="List available backups (Admin only)")
    @app_commands.default_permissions(manage_guild=True)
    @enhanced_rate_limit("query", 10, 60)
    async def list_backups_slash(self, interaction: discord.Interaction):
        """List all available database backups"""
        try:
            if not interaction.user.guild_permissions.manage_guild:
                await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
                return
            
            await interaction.response.defer()
            
            embed, error = await self._list_backups_logic()
            
            if error:
                await interaction.followup.send(error)
            else:
                embed.set_footer(text=f"Requested by {interaction.user}")
                await interaction.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in list_backups slash command: {e}")
            await interaction.followup.send("❌ Error listing backups.")
    
    @commands.command(name='list_backups')
    @commands.has_permissions(manage_guild=True)
    @commands.cooldown(1, COMMAND_COOLDOWN, commands.BucketType.user)
    async def list_backups_prefix(self, ctx):
        """List all available database backups"""
        try:
            embed, error = await self._list_backups_logic()
            
            if error:
                await ctx.reply(error)
            else:
                embed.set_footer(text=f"Requested by {ctx.author}")
                await ctx.reply(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in list_backups prefix command: {e}")
            await ctx.reply("❌ Error listing backups.")

    # ========================================================================================
    # HELP COMMANDS - BOTH PREFIX AND SLASH VERSIONS
    # ========================================================================================
    
    async def _help_logic(self, is_interaction: bool = True):
        """Shared logic for help command"""
        embed = discord.Embed(
            title="📚 PTCGP Bot Help",
            description="Here are the available commands:",
            color=discord.Color.blue(),
            timestamp=datetime.now()
        )
        
        command_prefix = "/" if is_interaction else "!"
        
        # Status Commands
        embed.add_field(
            name="🔄 Status Commands",
            value=(
                f"`{command_prefix}active` - Set status to active\n"
                f"`{command_prefix}inactive` - Set status to inactive\n"
                f"`{command_prefix}farm` - Set status to farm\n"
                f"`{command_prefix}leech` - Set status to leech\n"
                f"`{command_prefix}mystatus` - Show your status profile\n"
                f"`{command_prefix}setplayerid <id>` - Set your player ID"
            ),
            inline=False
        )
        
        # Probability Commands
        embed.add_field(
            name="🎯 Probability Commands",
            value=(
                f"`{command_prefix}probability <gp_id>` - Calculate god pack probability\n"
                f"`{command_prefix}miss <gp_id>` - Add a miss test\n"
                f"`{command_prefix}noshow <gp_id> <slots> <friends>` - Add a no-show test"
            ),
            inline=False
        )
        
        # Plotting Commands
        embed.add_field(
            name="📈 Plotting Commands",
            value=(
                f"`{command_prefix}plot_user [user] [days]` - Generate user activity plot\n"
                f"`{command_prefix}plot_status` - Check plotting system status"
            ),
            inline=False
        )
        
        # Admin Commands
        embed.add_field(
            name="🔧 Admin Commands",
            value=(
                f"`{command_prefix}create_backup [description]` - Create manual backup\n"
                f"`{command_prefix}list_backups` - List available backups\n"
                f"`{command_prefix}rate_limit_stats` - View rate limit stats\n"
                f"`{command_prefix}system_status` - Check system health"
            ),
            inline=False
        )
        
        embed.add_field(
            name="ℹ️ Notes",
            value=(
                "• Commands work with both `!` (prefix) and `/` (slash) formats\n"
                "• Admin commands require Manage Server permission\n"
                "• Rate limits protect server performance\n"
                "• Global rate limit: 150 commands per 5 minutes per user"
            ),
            inline=False
        )
        
        if is_interaction:
            embed.set_footer(text="Use !help for prefix commands or /help for slash commands")
        else:
            embed.set_footer(text="Use /help for slash commands or !help for prefix commands")
        
        return embed
    
    @app_commands.command(name="help", description="Show help for bot commands")
    @enhanced_rate_limit("help", 5, 60)
    async def help_slash(self, interaction: discord.Interaction):
        """Show help information for bot commands"""
        try:
            embed = await self._help_logic(True)
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            self.logger.error(f"Error in help slash command: {e}")
            await interaction.response.send_message("❌ Error displaying help.", ephemeral=True)
    
    @commands.command(name='help')
    @commands.cooldown(1, COMMAND_COOLDOWN, commands.BucketType.user)
    async def help_prefix(self, ctx):
        """Show help information for bot commands"""
        try:
            embed = await self._help_logic(False)
            await ctx.reply(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in help prefix command: {e}")
            await ctx.reply("❌ Error displaying help.")

    # ========================================================================================
    # RATE LIMITING MANAGEMENT COMMANDS - SLASH ONLY (ADMIN)
    # ========================================================================================
    
    @app_commands.command(name="rate_limit_stats", description="View rate limiting statistics (Admin only)")
    @app_commands.default_permissions(manage_guild=True)
    @enhanced_rate_limit("rate_limit_stats", 5, 60)
    async def rate_limit_stats(self, interaction: discord.Interaction):
        """View rate limiting statistics"""
        try:
            if not interaction.user.guild_permissions.manage_guild:
                await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
                return
            
            await interaction.response.defer()
            
            stats = enhanced_rate_limiter.get_rate_limit_stats()
            
            embed = discord.Embed(
                title="📊 Rate Limiting Statistics",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )
            
            embed.add_field(
                name="📈 Request Stats",
                value=(
                    f"Total Requests: **{stats['total_requests']:,}**\n"
                    f"Rejected: **{stats['rejected_requests']:,}**\n"
                    f"Rejection Rate: **{stats['rejection_rate']:.2f}%**"
                ),
                inline=True
            )
            
            embed.add_field(
                name="👥 User Stats",
                value=(
                    f"Unique Users: **{stats['unique_users']:,}**\n"
                    f"Users w/ Violations: **{stats['users_with_violations']:,}**\n"
                    f"Active Buckets: **{stats['active_buckets']:,}**"
                ),
                inline=True
            )
            
            embed.add_field(
                name="⏱️ Performance",
                value=(
                    f"Uptime: **{stats['uptime_hours']:.1f}h**\n"
                    f"Requests/Hour: **{stats['requests_per_hour']:.1f}**"
                ),
                inline=True
            )
            
            # Add global limits information
            limits = stats['global_limits']
            embed.add_field(
                name="⚙️ Current Limits",
                value=(
                    f"Server: {limits['requests_per_minute']}/min\n"
                    f"User Global: {limits['global_user_limit_per_5min']}/5min\n"
                    f"Heavy Commands: {limits['heavy_commands_per_hour']}/hour\n"
                    f"Admin Commands: {limits['admin_commands_per_hour']}/hour"
                ),
                inline=False
            )
            
            embed.set_footer(text=f"Requested by {interaction.user}")
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in rate_limit_stats command: {e}")
            await interaction.followup.send("❌ Error retrieving rate limit statistics.")
    
    @app_commands.command(name="user_rate_stats", description="View rate limit stats for a specific user (Admin only)")
    @app_commands.describe(user="User to check rate limit stats for")
    @app_commands.default_permissions(manage_guild=True)
    @enhanced_rate_limit("query", 10, 60)
    async def user_rate_stats(self, interaction: discord.Interaction, user: discord.Member):
        """View detailed rate limiting statistics for a specific user"""
        try:
            if not interaction.user.guild_permissions.manage_guild:
                await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
                return
            
            await interaction.response.defer()
            
            stats = await enhanced_rate_limiter.get_user_command_stats(user.id)
            
            embed = discord.Embed(
                title=f"📊 Rate Limit Stats - {user.display_name}",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )
            
            embed.add_field(
                name="📈 Usage Summary",
                value=(
                    f"Last Hour: **{stats['total_commands_last_hour']}** commands\n"
                    f"Last 5 Min: **{stats['total_commands_last_5min']}** commands\n"
                    f"Violations: **{stats['violations']}**"
                ),
                inline=True
            )
            
            embed.add_field(
                name="⚡ Command Types",
                value=(
                    f"Heavy (last hour): **{stats['heavy_commands_last_hour']}**\n"
                    f"Admin (last hour): **{stats['admin_commands_last_hour']}**"
                ),
                inline=True
            )
            
            # Show top command breakdown
            if stats['command_breakdown']:
                top_commands = sorted(
                    stats['command_breakdown'].items(), 
                    key=lambda x: x[1]['last_hour'], 
                    reverse=True
                )[:5]
                
                breakdown_text = []
                for cmd, data in top_commands:
                    if data['last_hour'] > 0:
                        breakdown_text.append(f"**{cmd}**: {data['last_hour']} (last hour)")
                
                if breakdown_text:
                    embed.add_field(
                        name="🔍 Command Breakdown",
                        value="\n".join(breakdown_text),
                        inline=False
                    )
            
            embed.set_footer(text=f"Requested by {interaction.user}")
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in user_rate_stats command: {e}")
            await interaction.followup.send("❌ Error retrieving user rate limit statistics.")
    
    @app_commands.command(name="reset_user_rate_limits", description="Reset rate limits for a user (Admin only)")
    @app_commands.describe(user="User to reset rate limits for")
    @app_commands.default_permissions(manage_guild=True)
    @enhanced_rate_limit("reset_user_rate_limits", 5, 300)
    async def reset_user_rate_limits(self, interaction: discord.Interaction, user: discord.Member):
        """Reset rate limits for a specific user"""
        try:
            if not interaction.user.guild_permissions.manage_guild:
                await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
                return
            
            enhanced_rate_limiter.reset_user_violations(user.id)
            
            embed = discord.Embed(
                title="✅ Rate Limits Reset",
                description=f"Rate limit violations cleared for {user.mention}",
                color=discord.Color.green(),
                timestamp=datetime.now()
            )
            
            embed.set_footer(text=f"Reset by {interaction.user}")
            
            await interaction.response.send_message(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in reset_user_rate_limits command: {e}")
            await interaction.response.send_message("❌ Error resetting rate limits.", ephemeral=True)

    # ========================================================================================
    # SYSTEM STATUS COMMANDS - SLASH ONLY (ADMIN)
    # ========================================================================================
    
    @app_commands.command(name="system_status", description="Check system health (Admin only)")
    @app_commands.default_permissions(manage_guild=True)
    @enhanced_rate_limit("system_status", 10, 60)
    async def system_status(self, interaction: discord.Interaction):
        """Check system health and status"""
        try:
            # Check permissions
            if not interaction.user.guild_permissions.manage_guild:
                await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
                return
            
            await interaction.response.defer()
            
            # Gather system information
            embed = discord.Embed(
                title="🔧 System Status",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )
            
            # Database status
            if self.db and hasattr(self.db, 'get_database_info'):
                try:
                    db_info = self.db.get_database_info()
                    embed.add_field(
                        name="💾 Database",
                        value=f"Size: {db_info.get('size_mb', 0)} MB\nRecords: {db_info.get('total_records', 0):,}\nIntegrity: {'✅' if db_info.get('integrity_check') else '❌'}",
                        inline=True
                    )
                except Exception as e:
                    embed.add_field(
                        name="💾 Database",
                        value="❌ Error getting info",
                        inline=True
                    )
            else:
                embed.add_field(
                    name="💾 Database",
                    value="❌ Not Available",
                    inline=True
                )
            
            # Component status
            components = {
                "Probability Calculator": self.probability_calc is not None,
                "Analytics": self.analytics is not None,
                "Plotting": self.plotting is not None,
                "Expiration Manager": self.expiration_manager is not None,
                "Sheets Integration": self.sheets_integration is not None
            }
            
            status_text = "\n".join([f"{name}: {'✅' if status else '❌'}" for name, status in components.items()])
            embed.add_field(
                name="📦 Components",
                value=status_text,
                inline=True
            )
            
            # Rate limiter status
            try:
                rate_stats = enhanced_rate_limiter.get_rate_limit_stats()
                active_limits = len(enhanced_rate_limiter._buckets)
                embed.add_field(
                    name="🚦 Rate Limiter",
                    value=f"Active buckets: {active_limits}\nRejection rate: {rate_stats.get('rejection_rate', 0):.1f}%",
                    inline=True
                )
            except Exception:
                embed.add_field(
                    name="🚦 Rate Limiter",
                    value="❌ Error getting stats",
                    inline=True
                )
            
            # Bot permissions check
            guild = interaction.guild
            bot_member = guild.get_member(self.bot.user.id)
            if bot_member:
                perms = bot_member.guild_permissions
                critical_perms = ['send_messages', 'embed_links', 'attach_files', 'manage_threads']
                missing_perms = [perm for perm in critical_perms if not getattr(perms, perm, False)]
                
                embed.add_field(
                    name="🔐 Permissions",
                    value=f"Critical perms: {'✅' if not missing_perms else '❌'}\nMissing: {', '.join(missing_perms) if missing_perms else 'None'}",
                    inline=True
                )
            
            embed.set_footer(text=f"Requested by {interaction.user}")
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in system_status command: {e}")
            await interaction.followup.send("❌ Error checking system status.")
    
    # ========================================================================================
    # UTILITY METHODS
    # ========================================================================================
    
    def _get_probability_color(self, probability: float) -> discord.Color:
        """Get color based on probability value"""
        if probability >= 80:
            return discord.Color.green()
        elif probability >= 50:
            return discord.Color.orange()
        elif probability >= 20:
            return discord.Color.red()
        else:
            return discord.Color.dark_red()
    
    async def get_command_usage_stats(self) -> Dict:
        """Get command usage statistics"""
        return {
            cmd: {
                'count': data['count'],
                'unique_users': len(data['users']),
                'last_used': data['last_used'].isoformat() if data['last_used'] else None
            }
            for cmd, data in self._command_usage.items()
        }
    
    async def check_global_rate_limit(self, user_id: int) -> Tuple[bool, str]:
        """Check if user has hit global rate limit across all commands"""
        allowed, retry_after, reason = await enhanced_rate_limiter.check_rate_limit(
            user_id, "global", 150, 300  # 150 commands per 5 minutes
        )
        return allowed, reason or ""
    
    async def cleanup(self):
        """Cleanup resources when cog is unloaded"""
        if self._plotting and hasattr(self._plotting, 'cleanup'):
            await self._plotting.cleanup()
        
        # Cleanup rate limiter
        enhanced_rate_limiter.cleanup_expired_buckets()
        
        self.logger.info("Enhanced bot commands cleaned up")

async def setup(bot, db_manager=None):
    """Setup function for loading this cog"""
    if db_manager is None:
        # Try to get database manager from bot
        db_manager = getattr(bot, 'db_manager', None)
        if db_manager is None:
            print("WARNING: No database manager provided to enhanced_bot_commands")
    
    cog = EnhancedBotCommands(bot, db_manager)
    await bot.add_cog(cog)
    print(f"✅ Enhanced bot commands loaded with {len(cog.get_app_commands())} app commands")