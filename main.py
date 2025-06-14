﻿import discord
from discord.ext import commands, tasks
from discord import app_commands
import asyncio
import datetime
import logging
import os
import sys
import traceback
import re
from typing import Optional, List

# Set matplotlib backend for headless environments
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt

# Import configuration and utilities from both systems
import config

# FIXED: Safe imports with comprehensive error handling
XML_LEGACY_SUPPORT = False
try:
    from xml_manager import (
        get_active_users as xml_get_active_users,
        get_all_users as xml_get_all_users,
        does_user_profile_exists,
        set_user_attrib_value,
        get_user_attrib_value
    )
    XML_LEGACY_SUPPORT = True
    logging.getLogger(__name__).info("XML legacy support enabled")
except ImportError as e:
    logging.getLogger(__name__).warning(f"XML legacy support not available: {e}")

# FIXED: Enhanced features availability flags
ENHANCED_FEATURES_AVAILABLE = True
DATABASE_MANAGER_AVAILABLE = False
PROBABILITY_CALC_AVAILABLE = False
ANALYTICS_AVAILABLE = False
PLOTTING_AVAILABLE = False
EXPIRATION_MANAGER_AVAILABLE = False
SHEETS_INTEGRATION_AVAILABLE = False
ENHANCED_COMMANDS_AVAILABLE = False

# Original bot imports with error handling
CORE_UTILS_AVAILABLE = False
try:
    from core_utils import (
        # Core functions
        get_guild, get_member_by_id, get_active_users, get_all_users,
        send_enhanced_stats, create_detailed_user_stats, create_timeline_stats_with_visualization,
        create_user_activity_chart, generate_server_report, health_check,
        
        # User management
        set_user_pack_preference, get_user_pack_preferences, does_user_profile_exists,
        set_user_attrib_value, get_user_attrib_value, cleanup_inactive_users,
        
        # Data management
        backup_user_data, restore_user_data, validate_user_data_integrity,
        log_user_activity, emergency_shutdown,
        
        # Legacy compatibility
        send_stats_legacy, get_channel_by_id
    )
    CORE_UTILS_AVAILABLE = True
    print("✅ Core utils loaded successfully")
except ImportError as e:
    logging.getLogger(__name__).error(f"Failed to import core_utils: {e}")
    print(f"❌ Core utils import failed: {e}")
    # Create dummy functions to prevent crashes
    async def get_active_users(*args, **kwargs): return []
    async def get_all_users(*args, **kwargs): return []
    async def send_enhanced_stats(*args, **kwargs): pass
    async def create_detailed_user_stats(*args, **kwargs): return None
    async def create_timeline_stats_with_visualization(*args, **kwargs): return None
    async def create_user_activity_chart(*args, **kwargs): return None
    async def generate_server_report(*args, **kwargs): return None
    async def health_check(*args, **kwargs): return {"core": False}
    async def set_user_pack_preference(*args, **kwargs): return False
    async def get_user_pack_preferences(*args, **kwargs): return {}
    async def cleanup_inactive_users(*args, **kwargs): pass
    async def backup_user_data(*args, **kwargs): return False
    async def restore_user_data(*args, **kwargs): return False
    def validate_user_data_integrity(*args, **kwargs): return False
    async def log_user_activity(*args, **kwargs): pass
    async def emergency_shutdown(*args, **kwargs): pass
    async def send_stats_legacy(*args, **kwargs): pass
    def get_channel_by_id(*args, **kwargs): return None

UTILS_AVAILABLE = False
try:
    from utils import (
        format_number_to_k, format_minutes_to_days, round_to_one_decimal,
        send_channel_message, bulk_delete_messages
    )
    UTILS_AVAILABLE = True
    print("✅ Utils loaded successfully")
except ImportError as e:
    logging.getLogger(__name__).warning(f"Some utils functions not available: {e}")
    print(f"⚠️  Utils not available: {e}")
    # Create dummy functions
    def format_number_to_k(num): return str(num)
    def format_minutes_to_days(minutes): return f"{minutes}m"
    def round_to_one_decimal(num): return round(num, 1)
    async def send_channel_message(*args, **kwargs): pass
    async def bulk_delete_messages(*args, **kwargs): pass

MISS_SENTENCES_AVAILABLE = False
try:
    from miss_sentences import find_emoji
    MISS_SENTENCES_AVAILABLE = True
    print("✅ Miss sentences loaded successfully")
except ImportError as e:
    logging.getLogger(__name__).warning(f"miss_sentences module not available: {e}")
    print(f"⚠️  Miss sentences not available: {e}")
    def find_emoji(*args, **kwargs): return ""

# FIXED: Enhanced components lazy loading with proper availability tracking
def get_database_manager():
    """Lazy import to prevent circular dependencies"""
    global DATABASE_MANAGER_AVAILABLE
    try:
        from database_manager import DatabaseManager, GPState, TestType
        DATABASE_MANAGER_AVAILABLE = True
        return DatabaseManager, GPState, TestType
    except ImportError as e:
        logging.getLogger(__name__).error(f"Failed to import database_manager: {e}")
        DATABASE_MANAGER_AVAILABLE = False
        return None, None, None

def get_enhanced_components():
    """Lazy import enhanced components with individual availability tracking"""
    global PROBABILITY_CALC_AVAILABLE, ANALYTICS_AVAILABLE, PLOTTING_AVAILABLE, ENHANCED_FEATURES_AVAILABLE
    global EXPIRATION_MANAGER_AVAILABLE, SHEETS_INTEGRATION_AVAILABLE, ENHANCED_COMMANDS_AVAILABLE
    
    components = {}
    
    try:
        from probability_calculator import ProbabilityCalculator
        components['ProbabilityCalculator'] = ProbabilityCalculator
        PROBABILITY_CALC_AVAILABLE = True
    except ImportError as e:
        logging.getLogger(__name__).warning(f"ProbabilityCalculator not available: {e}")
        PROBABILITY_CALC_AVAILABLE = False
    
    try:
        from heartbeat_analytics import HeartbeatAnalytics
        components['HeartbeatAnalytics'] = HeartbeatAnalytics
        ANALYTICS_AVAILABLE = True
    except ImportError as e:
        logging.getLogger(__name__).warning(f"HeartbeatAnalytics not available: {e}")
        ANALYTICS_AVAILABLE = False
    
    try:
        from plotting_system import PlottingSystem
        components['PlottingSystem'] = PlottingSystem
        PLOTTING_AVAILABLE = True
    except ImportError as e:
        logging.getLogger(__name__).warning(f"PlottingSystem not available: {e}")
        PLOTTING_AVAILABLE = False
    
    try:
        from expiration_manager import ExpirationManager
        components['ExpirationManager'] = ExpirationManager
        EXPIRATION_MANAGER_AVAILABLE = True
    except ImportError as e:
        logging.getLogger(__name__).warning(f"ExpirationManager not available: {e}")
        EXPIRATION_MANAGER_AVAILABLE = False
    
    try:
        from google_sheets_integration import GoogleSheetsIntegration
        components['GoogleSheetsIntegration'] = GoogleSheetsIntegration
        SHEETS_INTEGRATION_AVAILABLE = True
    except ImportError as e:
        logging.getLogger(__name__).warning(f"GoogleSheetsIntegration not available: {e}")
        SHEETS_INTEGRATION_AVAILABLE = False
    
    try:
        from enhanced_bot_commands import EnhancedBotCommands
        components['EnhancedBotCommands'] = EnhancedBotCommands
        ENHANCED_COMMANDS_AVAILABLE = True
    except ImportError as e:
        logging.getLogger(__name__).warning(f"EnhancedBotCommands not available: {e}")
        ENHANCED_COMMANDS_AVAILABLE = False
    
    # Update global availability flag
    ENHANCED_FEATURES_AVAILABLE = any([
        PROBABILITY_CALC_AVAILABLE, ANALYTICS_AVAILABLE, PLOTTING_AVAILABLE,
        EXPIRATION_MANAGER_AVAILABLE, SHEETS_INTEGRATION_AVAILABLE, ENHANCED_COMMANDS_AVAILABLE
    ])
    
    return components

# FIXED: Enhanced configuration validation
def validate_critical_config():
    """Validate critical configuration on startup with comprehensive checks"""
    errors = []
    warnings = []
    
    # Check required configuration
    if not hasattr(config, 'token') or not config.token:
        errors.append("Discord token is required")
    
    if not hasattr(config, 'guild_id') or not config.guild_id:
        errors.append("Guild ID is required")
    
    # Validate channel IDs are numeric or empty
    channel_fields = [
        'channel_id_commands', 'channel_id_user_stats', 
        'channel_id_heartbeat', 'channel_id_webhook',
        'stats_channel_id'  # Alternative naming
    ]
    
    for field in channel_fields:
        if hasattr(config, field):
            value = getattr(config, field)
            if value and value != "" and not str(value).isdigit():
                errors.append(f"{field} must be a valid Discord channel ID or empty string")
            elif value == "":
                warnings.append(f"{field} is not configured - some features may not work")
    
    # Check Discord.py version
    try:
        import discord
        version_parts = discord.__version__.split('.')
        major, minor = int(version_parts[0]), int(version_parts[1])
        if major < 2 or (major == 2 and minor < 3):
            errors.append("Discord.py version 2.3.0+ required")
    except Exception as e:
        errors.append(f"Discord.py import error: {e}")
    
    # Check database path configuration
    if hasattr(config, 'DATABASE_PATH'):
        db_path = getattr(config, 'DATABASE_PATH')
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir, exist_ok=True)
                warnings.append(f"Created database directory: {db_dir}")
            except Exception as e:
                errors.append(f"Cannot create database directory {db_dir}: {e}")
    
    # Check if bot has required intents
    try:
        if hasattr(config, 'required_intents'):
            for intent in config.required_intents:
                if not hasattr(discord.Intents, intent):
                    warnings.append(f"Unknown intent specified: {intent}")
    except Exception:
        pass
    
    # Validate command prefix
    if hasattr(config, 'command_prefix'):
        prefix = getattr(config, 'command_prefix')
        if not prefix or len(prefix) > 5:
            warnings.append("Command prefix should be 1-5 characters")
    
    if errors:
        raise ValueError(f"Critical configuration errors: {'; '.join(errors)}")
    
    if warnings:
        logger = logging.getLogger(__name__)
        for warning in warnings:
            logger.warning(f"Config warning: {warning}")
    
    return True

# FIXED: Enhanced logging setup with error handling
def setup_logging():
    """Setup comprehensive logging system with fallback options"""
    try:
        # Create logs directory with proper error handling
        try:
            os.makedirs('logs', exist_ok=True)
        except PermissionError:
            # Try alternative directory
            try:
                os.makedirs(os.path.expanduser('~/ptcgp_bot_logs'), exist_ok=True)
                log_dir = os.path.expanduser('~/ptcgp_bot_logs')
            except Exception:
                # Fall back to current directory
                log_dir = '.'
        except Exception:
            log_dir = '.'
        else:
            log_dir = 'logs'
        
        # Configure logging
        log_level = getattr(config, 'LOG_LEVEL', 'INFO').upper()
        if log_level not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            log_level = 'INFO'
            
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        # Create handlers with error handling
        handlers = []
        
        # File handler with rotation
        try:
            log_file_path = os.path.join(log_dir, 'bot.log')
            file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
            file_handler.setFormatter(logging.Formatter(log_format))
            handlers.append(file_handler)
        except Exception as e:
            print(f"Warning: Could not create file handler: {e}")
        
        # Console handler
        try:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(logging.Formatter(log_format))
            handlers.append(console_handler)
        except Exception as e:
            print(f"Warning: Could not create console handler: {e}")
        
        # Root logger configuration
        if handlers:
            logging.basicConfig(
                level=getattr(logging, log_level, logging.INFO),
                format=log_format,
                handlers=handlers,
                force=True  # Override any existing configuration
            )
        else:
            # Minimal fallback configuration
            logging.basicConfig(
                level=logging.INFO,
                format=log_format
            )
        
        # Set specific logger levels
        logging.getLogger('discord').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        
        return True
        
    except Exception as e:
        print(f"Failed to setup logging: {e}")
        # Minimal fallback
        logging.basicConfig(level=logging.INFO)
        return False

# Initialize logger after setup
logger = logging.getLogger(__name__)

def get_stats_channel(bot):
    """Get the stats channel from config with fallback options"""
    try:
        # Try multiple possible channel ID configurations
        channel_id = None
        possible_configs = [
            'channel_id_user_stats', 'stats_channel_id', 'channel_id_commands'
        ]
        
        for config_name in possible_configs:
            if hasattr(config, config_name):
                value = getattr(config, config_name)
                if value and str(value).isdigit():
                    channel_id = int(value)
                    break
        
        if channel_id:
            channel = bot.get_channel(channel_id)
            if channel:
                return channel
            else:
                logger.warning(f"Channel {channel_id} not found or bot has no access")
        
        # Fallback: try to find any channel the bot can write to
        for guild in bot.guilds:
            for channel in guild.text_channels:
                if channel.permissions_for(guild.me).send_messages:
                    logger.info(f"Using fallback channel: {channel.name} in {guild.name}")
                    return channel
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting stats channel: {e}")
        return None

class UnifiedPTCGPBot(commands.Bot):
    """Unified bot combining reroll management and god pack tracking functionality"""
    
    def __init__(self):
        # Set up intents for both systems
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.guilds = True
        intents.reactions = True
        
        super().__init__(
            command_prefix=getattr(config, 'command_prefix', '!'),
            intents=intents,
            help_command=None,  # Disable default help command
            case_insensitive=True
        )
        
        # FIXED: Initialize enhanced systems with safe defaults
        self.db_manager = None
        self.probability_calc = None
        self.analytics = None
        self.plotting = None
        self.expiration_manager = None
        self.sheets_integration = None
        
        # Track component availability
        self.components_available = {
            'database_manager': False,
            'probability_calc': False,
            'analytics': False,
            'plotting': False,
            'expiration_manager': False,
            'sheets_integration': False,
            'enhanced_commands': False
        }
        
        # Global variables for tracking (original features)
        self.stats_task_running = False
        self.last_stats_time = None
        self.start_time = datetime.datetime.now()
        
        # System health tracking
        self.initialization_errors = []
        self.initialization_warnings = []
        
        logger.info("Unified PTCGP Reroll Bot initialized")

    async def setup_hook(self):
        """FIXED: Set up the bot when it starts with comprehensive error handling"""
        try:
            logger.info("Starting bot setup...")
            
            # Initialize enhanced systems first
            await self.initialize_enhanced_systems()
            
            # Add the enhanced commands cog if available
            if self.db_manager:
                try:
                    from enhanced_bot_commands import EnhancedBotCommands
                    enhanced_cog = EnhancedBotCommands(self, self.db_manager)
                    await self.add_cog(enhanced_cog)
                    self.components_available['enhanced_commands'] = True
                    logger.info("✅ Enhanced bot commands loaded successfully")
                    print("✅ Enhanced commands cog loaded")
                except ImportError as e:
                    logger.warning(f"Enhanced commands not available: {e}")
                    print(f"❌ Enhanced commands not available: {e}")
                except Exception as e:
                    logger.error(f"Failed to load enhanced commands: {e}")
                    self.initialization_errors.append(f"Enhanced commands: {e}")
                    print(f"❌ Failed to load enhanced commands: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                logger.warning("Database manager not available - enhanced commands not loaded")
                print("❌ Enhanced commands not loaded - no database manager")
            
            # Start background tasks from both systems
            try:
                if hasattr(self, 'cleanup_task'):
                    self.cleanup_task.start()
                    logger.info("Cleanup task started")
            except Exception as e:
                logger.error(f"Failed to start cleanup task: {e}")
                self.initialization_errors.append(f"Cleanup task: {e}")
            
            try:
                if hasattr(self, 'daily_sync_task'):
                    self.daily_sync_task.start()
                    logger.info("Daily sync task started")
            except Exception as e:
                logger.error(f"Failed to start daily sync task: {e}")
                self.initialization_errors.append(f"Daily sync task: {e}")
            
            # Start expiration monitoring (enhanced system)
            if self.expiration_manager:
                try:
                    asyncio.create_task(self.expiration_manager.start_expiration_monitoring())
                    logger.info("Expiration monitoring started")
                except Exception as e:
                    logger.error(f"Failed to start expiration monitoring: {e}")
                    self.initialization_errors.append(f"Expiration monitoring: {e}")
            
            logger.info("Bot setup completed")
            
            # Log initialization summary
            if self.initialization_errors:
                logger.warning(f"Setup completed with {len(self.initialization_errors)} errors")
                for error in self.initialization_errors:
                    logger.warning(f"  - {error}")
            
            if self.initialization_warnings:
                logger.info(f"Setup completed with {len(self.initialization_warnings)} warnings")
                for warning in self.initialization_warnings:
                    logger.info(f"  - {warning}")
            
        except Exception as e:
            logger.error(f"Critical error in setup_hook: {e}")
            traceback.print_exc()

    async def on_ready(self):
        """FIXED: Bot startup event with enhanced error handling"""
        try:
            print(f'✅ {self.user.name} has connected to Discord!')
            print(f'🔗 Bot ID: {self.user.id}')
            print(f'🌐 Connected to {len(self.guilds)} guild(s)')
            print(f'🎯 Command prefix: {self.command_prefix}')
            
            # Log guild information
            for guild in self.guilds:
                logger.info(f"Connected to guild: {guild.name} (ID: {guild.id})")
                print(f"📋 Guild: {guild.name} (ID: {guild.id})")
            
            # Start original system background tasks with error handling
            try:
                if not self.stats_sender.is_running():
                    self.stats_sender.start()
                    print('📊 Stats sender task started')
            except Exception as e:
                logger.error(f"Failed to start stats sender: {e}")
                print('❌ Stats sender task failed to start')
            
            try:
                if not self.user_cleanup.is_running():
                    self.user_cleanup.start()
                    print('🧹 User cleanup task started')
            except Exception as e:
                logger.error(f"Failed to start user cleanup: {e}")
                print('❌ User cleanup task failed to start')
            
            # Start enhanced system data backup if configured
            if hasattr(config, 'enable_auto_backup') and config.enable_auto_backup:
                try:
                    if hasattr(self, 'data_backup') and not self.data_backup.is_running():
                        self.data_backup.start()
                        print('💾 Data backup task started')
                except Exception as e:
                    logger.error(f"Failed to start data backup: {e}")
                    print('❌ Data backup task failed to start')
            
            # Perform health check on startup
            try:
                if CORE_UTILS_AVAILABLE:
                    health_status = await health_check()
                    healthy_count = sum(health_status.values())
                    total_count = len(health_status)
                    print(f'🏥 Health check: {healthy_count}/{total_count} systems OK')
                else:
                    print('🏥 Health check: Core utils not available')
            except Exception as e:
                logger.warning(f"Health check failed: {e}")
                print('❌ Health check failed')
            
            # Sync slash commands for enhanced features
            try:
                synced = await self.tree.sync()
                logger.info(f"Synced {len(synced)} slash command(s)")
                if synced:
                    print(f'🔄 Synced {len(synced)} slash commands')
                    for cmd in synced:
                        print(f"  - /{cmd.name}: {cmd.description}")
                else:
                    logger.info("No slash commands to sync")
                    print("ℹ️  No slash commands to sync")
            except Exception as e:
                logger.error(f"Failed to sync commands: {e}")
                print(f'❌ Failed to sync slash commands: {e}')
                import traceback
                traceback.print_exc()
            
            # Print component availability summary
            available_components = [name for name, available in self.components_available.items() if available]
            print(f'🎯 Available components: {len(available_components)}/{len(self.components_available)}')
            for component in available_components:
                print(f"  ✅ {component}")
            
            unavailable_components = [name for name, available in self.components_available.items() if not available]
            if unavailable_components:
                print(f'❌ Unavailable components:')
                for component in unavailable_components:
                    print(f"  ❌ {component}")
            
            if self.initialization_errors:
                print(f'⚠️  Initialization completed with {len(self.initialization_errors)} errors - check logs')
            
            print("\n🤖 Bot is ready! Try these commands:")
            print("   !help - Show available commands")
            print("   !test - Test basic functionality") 
            print("   !ping - Check bot latency")
            print("   /help - Try slash commands")
            print("   /active - Test enhanced features")
            print(f"\n📢 Watching for commands with prefix: {self.command_prefix}")
            
        except Exception as e:
            logger.error(f"Error in on_ready: {e}")
            traceback.print_exc()

    async def on_message(self, message):
        """FIXED: Process messages for heartbeats, god pack detection, and commands with error handling"""
        try:
            if message.author.bot:
                return
            
            # Debug logging for commands (can be disabled later)
            if message.content.startswith(self.command_prefix):
                logger.debug(f"Command received: {message.content} from {message.author} in {message.guild}")
                print(f"🔍 Command: {message.content} from {message.author.display_name}")
            
            # Process commands first - this is critical
            await self.process_commands(message)
            
            # Enhanced heartbeat processing (only if enhanced features available)
            if self.components_available['database_manager']:
                try:
                    if await self._is_heartbeat_message(message):
                        await self._process_heartbeat(message)
                        return
                except Exception as e:
                    logger.error(f"Error processing heartbeat: {e}")
            
            # Enhanced god pack detection (only if enhanced features available)
            if self.components_available['database_manager']:
                try:
                    if await self._is_godpack_message(message):
                        await self._process_godpack(message)
                        return
                except Exception as e:
                    logger.error(f"Error processing god pack: {e}")
                    
        except Exception as e:
            logger.error(f"Error in on_message: {e}")
            traceback.print_exc()

    async def on_command_error(self, ctx, error):
        """FIXED: Enhanced global error handler for commands with detailed debugging"""
        try:
            # Log the error for debugging
            logger.error(f"Command error in {ctx.command}: {error}")
            
            if isinstance(error, commands.CommandNotFound):
                # More helpful command not found message
                attempted_command = ctx.message.content.split()[0]
                await ctx.reply(f"❌ Command `{attempted_command}` not found. Use `{self.command_prefix}help` to see available commands.", delete_after=10)
                
                # Suggest similar commands
                available_commands = [cmd.name for cmd in self.commands if not cmd.hidden]
                suggestions = [cmd for cmd in available_commands if attempted_command.lower().replace(self.command_prefix, '') in cmd.lower()]
                if suggestions:
                    await ctx.send(f"💡 Did you mean: {', '.join([f'`{self.command_prefix}{cmd}`' for cmd in suggestions[:3]])}?", delete_after=10)
                    
            elif isinstance(error, commands.MissingPermissions):
                await ctx.reply("❌ You don't have permission to use this command.", delete_after=10)
            elif isinstance(error, commands.BadArgument):
                await ctx.reply("❌ Invalid argument provided. Please check the command syntax.", delete_after=10)
            elif isinstance(error, commands.MissingRequiredArgument):
                await ctx.reply(f"❌ Missing required argument: `{error.param}`. Use `{self.command_prefix}help` for syntax.", delete_after=10)
            elif isinstance(error, commands.CommandOnCooldown):
                await ctx.reply(f"❌ Command is on cooldown. Try again in {error.retry_after:.2f} seconds.", delete_after=10)
            elif isinstance(error, commands.BotMissingPermissions):
                perms = ', '.join(error.missing_permissions)
                await ctx.reply(f"❌ I need the following permissions: {perms}", delete_after=10)
            elif isinstance(error, commands.NoPrivateMessage):
                await ctx.reply("❌ This command cannot be used in private messages.", delete_after=10)
            elif isinstance(error, discord.Forbidden):
                await ctx.reply("❌ I don't have permission to perform this action.", delete_after=10)
            elif isinstance(error, discord.HTTPException):
                await ctx.reply("❌ A network error occurred. Please try again.", delete_after=10)
            else:
                # Log unexpected errors but don't expose details to users
                error_id = abs(hash(str(error))) % 10000
                logger.error(f"Unexpected error in command {ctx.command} (ID: {error_id}): {error}")
                traceback.print_exception(type(error), error, error.__traceback__)
                
                if hasattr(config, 'debug_mode') and config.debug_mode:
                    await ctx.reply(f"❌ Unexpected error (ID: {error_id}): {str(error)[:100]}")
                else:
                    await ctx.reply(f"❌ An unexpected error occurred (ID: {error_id}). Please contact an admin if this persists.")
                    
                # Print to console for immediate debugging
                print(f"❌ Command Error (ID: {error_id}): {error}")
                
        except Exception as e:
            logger.error(f"Error in error handler: {e}")
            # Last resort - try to send a simple message
            try:
                await ctx.send("❌ An error occurred.")
            except:
                pass

    # ========================================================================================
    # ESSENTIAL COMMANDS (ALWAYS WORK) - FIXED VERSION
    # ========================================================================================

    # ========================================================================================
    # ESSENTIAL COMMANDS (ALWAYS WORK) - FIXED VERSION
    # ========================================================================================
    
    @commands.command(name="test")
    async def test_command(self, ctx):
        """Test if the bot is responding to commands"""
        try:
            embed = discord.Embed(
                title="🧪 Bot Test Results",
                color=0x00ff00,
                timestamp=datetime.datetime.now()
            )
            
            embed.add_field(name="✅ Status", value="Bot is online and responding!", inline=False)
            embed.add_field(name="📡 Latency", value=f"{round(self.latency * 1000)}ms", inline=True)
            embed.add_field(name="🔧 Prefix", value=f"`{self.command_prefix}`", inline=True)
            embed.add_field(name="🏠 Guild", value=ctx.guild.name, inline=True)
            
            # Show component status
            available = sum(self.components_available.values())
            total = len(self.components_available)
            embed.add_field(name="🎯 Components", value=f"{available}/{total} available", inline=True)
            
            # Show command count
            cmd_count = len([cmd for cmd in self.commands if not cmd.hidden])
            embed.add_field(name="📋 Commands", value=f"{cmd_count} loaded", inline=True)
            
            embed.set_footer(text=f"Bot ID: {self.user.id}")
            
            await ctx.reply(embed=embed)
            logger.info(f"Test command executed by {ctx.author} in {ctx.guild.name}")
            
        except Exception as e:
            logger.error(f"Error in test command: {e}")
            await ctx.reply("✅ Bot is working! (Simple response due to error in embed)")

    @commands.command(name="ping")
    async def ping_command(self, ctx):
        """Check bot latency and connection status"""
        try:
            latency = round(self.latency * 1000)
            
            if latency < 100:
                status = "🟢 Excellent"
                color = 0x00ff00
            elif latency < 200:
                status = "🟡 Good"
                color = 0xffff00
            else:
                status = "🔴 Poor"
                color = 0xff0000
            
            embed = discord.Embed(
                title="🏓 Pong!",
                description=f"**Latency:** {latency}ms\n**Status:** {status}",
                color=color,
                timestamp=datetime.datetime.now()
            )
            
            await ctx.reply(embed=embed)
            
        except Exception as e:
            logger.error(f"Error in ping command: {e}")
            await ctx.reply(f"🏓 Pong! Latency: {round(self.latency * 1000)}ms")

    @commands.command(name="help")
    async def help_command(self, ctx):
        """FIXED: Display comprehensive help information with availability checks"""
        try:
            embed = discord.Embed(
                title="🤖 Unified PTCGP Bot Commands",
                description="Complete command list for reroll management and god pack tracking",
                color=0x3498db
            )
            
            # Essential commands (always available)
            embed.add_field(
                name="🧪 Essential Commands",
                value="`!test` - Test bot functionality\n"
                      "`!ping` - Check bot latency\n"
                      "`!help` - Show this help message\n"
                      "`!status` - Show detailed bot status\n"
                      "`!sync` - Force sync slash commands (Admin)",
                inline=False
            )
            
            # Statistics commands (if core utils work)
            if CORE_UTILS_AVAILABLE:
                embed.add_field(
                    name="📊 Statistics Commands",
                    value="`!stats` - Show current statistics\n"
                          "`!mystats` - Show your detailed statistics\n"
                          "`!timeline [days]` - Show activity timeline\n"
                          "`!report [days]` - Generate server report\n"
                          "`!refresh` - Refresh your statistics",
                    inline=False
                )
            
            # User management commands
            if CORE_UTILS_AVAILABLE:
                embed.add_field(
                    name="👤 User Management",
                    value="`!setpack <pack_name>` - Set preferred pack\n"
                          "`!mypack` - Show your pack preferences\n"
                          "`!activity [user]` - Show user activity chart\n"
                          "`!userstats [user]` - Show user statistics",
                    inline=False
                )
            
            # Enhanced system commands (only show if available)
            if self.components_available['enhanced_commands']:
                embed.add_field(
                    name="🔄 Enhanced Commands (Prefix & Slash)",
                    value="`!active` or `/active` - Set status to active\n"
                          "`!inactive` or `/inactive` - Set status to inactive\n"
                          "`!farm` or `/farm` - Set status to farm\n"
                          "`!leech` or `/leech` - Set status to leech\n"
                          "`!mystatus` or `/mystatus` - Show your profile\n"
                          "`!setplayerid <id>` or `/setplayerid <id>` - Set player ID",
                    inline=False
                )
                
                embed.add_field(
                    name="🌟 God Pack Commands",
                    value="`!probability <gp_id>` or `/probability <gp_id>` - Check GP probability\n"
                          "`!miss <gp_id>` or `/miss <gp_id>` - Add miss test\n"
                          "`!noshow <gp_id> <slots> <friends>` or `/noshow` - Add no-show test",
                    inline=False
                )
            
            # Utility commands
            embed.add_field(
                name="🔧 Utility Commands",
                value="`!status` - Show bot status\n"
                      "`!leaderboard` - Show leaderboards",
                inline=False
            )
            
            # Admin commands (only show if user has permissions)
            if (ctx.author.guild_permissions.administrator or 
                any(role.id in getattr(config, 'admin_role_ids', []) for role in ctx.author.roles)):
                
                admin_commands = []
                if CORE_UTILS_AVAILABLE:
                    admin_commands.extend([
                        "`!forcestats` - Force send statistics",
                        "`!cleanup` - Clean up inactive users",
                        "`!backup` - Create data backup",
                        "`!health` - System health check",
                        "`!emergency` - Emergency shutdown"
                    ])
                
                if self.components_available['enhanced_commands']:
                    admin_commands.extend([
                        "`/system_status` - Enhanced system status",
                        "`/create_backup` - Create database backup",
                        "`/rate_limit_stats` - View rate limiting stats"
                    ])
                
                if admin_commands:
                    embed.add_field(
                        name="🔧 Admin Commands",
                        value="\n".join(admin_commands),
                        inline=False
                    )
            
            # System status indicators
            status_indicators = []
            if CORE_UTILS_AVAILABLE:
                status_indicators.append("✅ Core System")
            else:
                status_indicators.append("❌ Core System")
                
            if self.components_available['enhanced_commands']:
                status_indicators.append("✅ Enhanced System")
            else:
                status_indicators.append("❌ Enhanced System")
            
            embed.add_field(
                name="📡 System Status",
                value=" | ".join(status_indicators),
                inline=False
            )
            
            embed.set_footer(text=f"Prefix: {self.command_prefix} | Use /help for slash commands")
            await ctx.reply(embed=embed)
            
        except Exception as e:
            logger.error(f"Error in help command: {e}")
            # Fallback simple help if embed fails
            help_text = f"""🤖 **PTCGP Bot Help**

**Essential Commands:**
`!test` - Test bot functionality
`!ping` - Check latency  
`!help` - This help message
`!status` - Bot status

**Prefix:** `{self.command_prefix}`
**Bot ID:** {self.user.id}

Try `!test` to verify everything is working!"""
            await ctx.reply(help_text)
    @commands.command(name='sync')
    @commands.has_permissions(administrator=True)
    async def sync_commands(self, ctx):
        """Force sync slash commands (Admin only)"""
        try:
            await ctx.send("🔄 Syncing slash commands...")
            
            # Get current commands
            current_commands = self.tree.get_commands()
            print(f"Commands before sync: {[cmd.name for cmd in current_commands]}")
            
            # Clear and re-sync
            self.tree.clear_commands()
            synced = await self.tree.sync()
            
            embed = discord.Embed(
                title="✅ Commands Synced",
                description=f"Successfully synced {len(synced)} slash commands",
                color=discord.Color.green()
            )
            
            if synced:
                command_list = "\n".join([f"• /{cmd.name}" for cmd in synced])
                embed.add_field(
                    name="📋 Synced Commands",
                    value=command_list,
                    inline=False
                )
            
            # Debug info
            embed.add_field(
                name="🔧 Debug Info",
                value=f"Cogs loaded: {list(self.cogs.keys())}\nDB Manager: {'✅' if self.db_manager else '❌'}",
                inline=False
            )
            
            await ctx.send(embed=embed)
            logger.info(f"Commands manually synced by {ctx.author}: {[cmd.name for cmd in synced]}")
            
        except Exception as e:
            logger.error(f"Error syncing commands: {e}")
            await ctx.send(f"❌ Error syncing commands: {e}")

    @commands.command(name="status")
    async def status_command(self, ctx):
        """Show comprehensive bot status"""
        try:
            uptime = datetime.datetime.now() - self.start_time
            
            embed = discord.Embed(
                title="🤖 Bot Status Dashboard",
                color=0x2ecc71,
                timestamp=datetime.datetime.now()
            )
            
            # Basic info
            embed.add_field(
                name="⏱️ Uptime",
                value=f"{uptime.days}d {uptime.seconds//3600}h {(uptime.seconds//60)%60}m",
                inline=True
            )
            
            embed.add_field(
                name="📡 Latency",
                value=f"{round(self.latency * 1000)}ms",
                inline=True
            )
            
            embed.add_field(
                name="🏠 Guilds",
                value=str(len(self.guilds)),
                inline=True
            )
            
            # System status
            core_status = "✅ Available" if CORE_UTILS_AVAILABLE else "❌ Not Available"
            enhanced_status = f"✅ {sum(self.components_available.values())}/{len(self.components_available)}" if any(self.components_available.values()) else "❌ Not Available"
            
            embed.add_field(
                name="🔧 Core System",
                value=core_status,
                inline=True
            )
            
            embed.add_field(
                name="🚀 Enhanced System",
                value=enhanced_status,
                inline=True
            )
            
            # Commands and tasks
            cmd_count = len([cmd for cmd in self.commands if not cmd.hidden])
            slash_count = len(self.tree.get_commands())
            embed.add_field(
                name="📋 Commands",
                value=f"Prefix: {cmd_count}\nSlash: {slash_count}",
                inline=True
            )
            
            # Database status
            if self.db_manager:
                try:
                    db_status = "✅ Connected" if self.db_manager.test_connection() else "❌ Error"
                except:
                    db_status = "❓ Unknown"
            else:
                db_status = "❌ Not Available"
                
            embed.add_field(
                name="🗄️ Database",
                value=db_status,
                inline=True
            )
            
            # Component details
            if any(self.components_available.values()):
                component_status = []
                for component, available in self.components_available.items():
                    status_icon = "✅" if available else "❌"
                    component_status.append(f"{status_icon} {component.replace('_', ' ').title()}")
                
                embed.add_field(
                    name="🎯 Component Status",
                    value="\n".join(component_status),
                    inline=False
                )
            
            embed.set_footer(text=f"Bot ID: {self.user.id}")
            await ctx.reply(embed=embed)
            
        except Exception as e:
            logger.error(f"Error in status command: {e}")
            # Simple fallback
            uptime = datetime.datetime.now() - self.start_time
            await ctx.reply(f"🤖 Bot Status: Online\n⏱️ Uptime: {uptime.days}d {uptime.seconds//3600}h\n📡 Latency: {round(self.latency * 1000)}ms")

    # ========================================================================================
    # CORE STATISTICS COMMANDS (FROM ORIGINAL SYSTEM)
    # ========================================================================================
    
    @commands.command(name='stats')
    @commands.cooldown(1, getattr(config, 'COMMAND_COOLDOWN_SECONDS', 2), commands.BucketType.user)
    async def stats_command(self, ctx):
        """Display server statistics"""
        if not CORE_UTILS_AVAILABLE:
            await ctx.send("❌ Core statistics system not available.")
            return
            
        try:
            await send_enhanced_stats(self, manual_trigger=True)
            
        except Exception as e:
            logger.error(f"Error in stats command: {e}")
            await ctx.send("❌ Error retrieving statistics.")

    @commands.command(name='mystats')
    @commands.cooldown(1, getattr(config, 'COMMAND_COOLDOWN_SECONDS', 2), commands.BucketType.user)
    async def my_stats_command(self, ctx):
        """Show your detailed statistics"""
        if not CORE_UTILS_AVAILABLE:
            await ctx.send("❌ Core statistics system not available.")
            return
            
        try:
            stats_embed = await create_detailed_user_stats(self, str(ctx.author.id))
            if stats_embed:
                await ctx.send(embed=stats_embed)
            else:
                await ctx.send("❌ No statistics found for your account. Start using the bot to generate data!")
                
        except Exception as e:
            logger.error(f"Error in mystats command: {e}")
            await ctx.send("❌ Error retrieving your statistics.")

    @commands.command(name='timeline')
    @commands.cooldown(1, getattr(config, 'COMMAND_COOLDOWN_SECONDS', 5), commands.BucketType.user)
    async def timeline_command(self, ctx, days: int = 7):
        """Show activity timeline for specified days (default: 7)"""
        if not CORE_UTILS_AVAILABLE:
            await ctx.send("❌ Core statistics system not available.")
            return
            
        try:
            # Validate days parameter
            if days < 1 or days > 365:
                await ctx.send("❌ Days must be between 1 and 365.")
                return
            
            timeline_embed = await create_timeline_stats_with_visualization(self, days)
            if timeline_embed:
                await ctx.send(embed=timeline_embed)
            else:
                await ctx.send(f"❌ No timeline data found for the last {days} days.")
                
        except Exception as e:
            logger.error(f"Error in timeline command: {e}")
            await ctx.send("❌ Error generating timeline.")

    @commands.command(name='activity')
    @commands.cooldown(1, getattr(config, 'COMMAND_COOLDOWN_SECONDS', 5), commands.BucketType.user)
    async def activity_command(self, ctx, user: discord.Member = None):
        """Show user activity chart"""
        if not CORE_UTILS_AVAILABLE:
            await ctx.send("❌ Core statistics system not available.")
            return
            
        target_user = user if user else ctx.author
        
        try:
            chart_buffer = await create_user_activity_chart(str(target_user.id), 7)
            if chart_buffer:
                file = discord.File(chart_buffer, filename=f"activity_{target_user.id}.png")
                embed = discord.Embed(
                    title=f"📊 Activity Chart - {target_user.display_name}",
                    color=discord.Color.blue(),
                    timestamp=datetime.datetime.now()
                )
                embed.set_image(url=f"attachment://activity_{target_user.id}.png")
                await ctx.send(embed=embed, file=file)
            else:
                await ctx.send(f"❌ No activity data found for {target_user.display_name}.")
                
        except Exception as e:
            logger.error(f"Error in activity command: {e}")
            await ctx.send("❌ Error generating activity chart.")

    @commands.command(name='report')
    @commands.cooldown(1, getattr(config, 'COMMAND_COOLDOWN_SECONDS', 10), commands.BucketType.user)
    async def report_command(self, ctx, days: int = 30):
        """Generate comprehensive server report"""
        if not CORE_UTILS_AVAILABLE:
            await ctx.send("❌ Core statistics system not available.")
            return
            
        try:
            # Validate days parameter
            if days < 1 or days > 365:
                await ctx.send("❌ Days must be between 1 and 365.")
                return
            
            report_embed = await generate_server_report(self, days)
            if report_embed:
                await ctx.send(embed=report_embed)
            else:
                await ctx.send(f"❌ No report data available for the last {days} days.")
                
        except Exception as e:
            logger.error(f"Error in report command: {e}")
            await ctx.send("❌ Error generating report.")

    @commands.command(name='refresh')
    @commands.cooldown(1, getattr(config, 'COMMAND_COOLDOWN_SECONDS', 5), commands.BucketType.user)
    async def refresh_command(self, ctx):
        """Refresh your statistics cache"""
        if not CORE_UTILS_AVAILABLE:
            await ctx.send("❌ Core statistics system not available.")
            return
            
        try:
            # Log user activity
            await log_user_activity(str(ctx.author.id), ctx.author.display_name, 'refresh_requested')
            
            embed = discord.Embed(
                title="🔄 Statistics Refreshed",
                description="Your statistics cache has been refreshed. Use `!mystats` to see updated data.",
                color=discord.Color.green()
            )
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error in refresh command: {e}")
            await ctx.send("❌ Error refreshing statistics.")
    @commands.command(name='leaderboard')
    @commands.cooldown(1, getattr(config, 'COMMAND_COOLDOWN_SECONDS', 5), commands.BucketType.user)
    async def leaderboard_command(self, ctx, category: str = "activity"):
        """Show leaderboards"""
        try:
            # Check if we have enhanced leaderboard functionality
            if self.components_available['analytics'] and self.analytics:
                try:
                    # Try to get real leaderboard data
                    leaderboard_data = await self.analytics.get_leaderboard(category)
                    if leaderboard_data:
                        embed = discord.Embed(
                            title=f"🏆 {category.title()} Leaderboard",
                            color=discord.Color.gold(),
                            timestamp=datetime.datetime.now()
                        )
                        
                        for i, (user_id, score) in enumerate(leaderboard_data[:10], 1):
                            user = self.get_user(user_id)
                            username = user.display_name if user else f"User {user_id}"
                            
                            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                            embed.add_field(
                                name=f"{medal} {username}",
                                value=f"Score: {score}",
                                inline=False
                            )
                        
                        await ctx.send(embed=embed)
                        return
                except Exception as e:
                    logger.error(f"Error getting enhanced leaderboard: {e}")
            
            # Fallback leaderboard
            embed = discord.Embed(
                title="🏆 Leaderboards",
                description="Leaderboard system is being developed with enhanced analytics.",
                color=discord.Color.gold()
            )
            
            embed.add_field(
                name="📊 Planned Categories",
                value="• Activity Score\n• God Packs Found\n• Streak Length\n• Contribution Score",
                inline=False
            )
            
            if CORE_UTILS_AVAILABLE:
                embed.add_field(
                    name="📈 Available Data",
                    value="Basic statistics are available. Enhanced leaderboards coming soon!",
                    inline=False
                )
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error in leaderboard command: {e}")
            await ctx.send("❌ Error displaying leaderboard.")

    @commands.command(name='serverstats')
    @commands.cooldown(1, getattr(config, 'COMMAND_COOLDOWN_SECONDS', 5), commands.BucketType.user)
    async def server_stats_command(self, ctx):
        """Show detailed server statistics"""
        if not CORE_UTILS_AVAILABLE:
            await ctx.send("❌ Core statistics system not available.")
            return
            
        try:
            embed = discord.Embed(
                title="📊 Server Statistics",
                color=discord.Color.blue(),
                timestamp=datetime.datetime.now()
            )
            
            # Basic Discord server stats
            embed.add_field(
                name="👥 Members",
                value=f"Total: {ctx.guild.member_count}\nOnline: {sum(1 for m in ctx.guild.members if m.status != discord.Status.offline)}",
                inline=True
            )
            
            embed.add_field(
                name="📝 Channels",
                value=f"Text: {len(ctx.guild.text_channels)}\nVoice: {len(ctx.guild.voice_channels)}",
                inline=True
            )
            
            embed.add_field(
                name="🎭 Roles",
                value=str(len(ctx.guild.roles)),
                inline=True
            )
            
            # Bot-specific stats if available
            try:
                active_users = await get_active_users()
                all_users = await get_all_users()
                
                embed.add_field(
                    name="🎮 PTCGP Users",
                    value=f"Active: {len(active_users)}\nTotal: {len(all_users)}",
                    inline=True
                )
            except Exception as e:
                logger.debug(f"Could not get user stats: {e}")
            
            # Enhanced stats if database available
            if self.components_available['database_manager'] and self.db_manager:
                try:
                    user_count = self.db_manager.get_user_count() if hasattr(self.db_manager, 'get_user_count') else 0
                    gp_count = self.db_manager.get_godpack_count() if hasattr(self.db_manager, 'get_godpack_count') else 0
                    
                    embed.add_field(
                        name="🗄️ Database",
                        value=f"Users: {user_count}\nGod Packs: {gp_count}",
                        inline=True
                    )
                except Exception as e:
                    logger.debug(f"Could not get database stats: {e}")
            
            embed.set_footer(text=f"Server ID: {ctx.guild.id}")
            await ctx.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error in serverstats command: {e}")
            await ctx.send("❌ Error retrieving server statistics.")

    # ========================================================================================
    # USER MANAGEMENT COMMANDS (FROM ORIGINAL SYSTEM)
    # ========================================================================================
    
    @commands.command(name='setpack')
    @commands.cooldown(1, getattr(config, 'COMMAND_COOLDOWN_SECONDS', 2), commands.BucketType.user)
    async def set_pack_command(self, ctx, *, pack_name: str):
        """Set your preferred pack"""
        if not CORE_UTILS_AVAILABLE:
            await ctx.send("❌ Core user management system not available.")
            return
            
        try:
            # Validate pack name
            valid_packs = getattr(config, 'VALID_PACK_NAMES', [
                'Charizard ex', 'Pikachu ex', 'Mewtwo ex'
            ])
            
            # Find matching pack (case insensitive)
            matched_pack = None
            pack_name_lower = pack_name.lower()
            
            for valid_pack in valid_packs:
                if pack_name_lower in valid_pack.lower() or valid_pack.lower() in pack_name_lower:
                    matched_pack = valid_pack
                    break
            
            if not matched_pack:
                # Create a nice embed showing valid packs
                embed = discord.Embed(
                    title="❌ Invalid Pack Name",
                    description="Please choose from the following valid packs:",
                    color=discord.Color.red()
                )
                
                for i, pack in enumerate(valid_packs, 1):
                    embed.add_field(
                        name=f"{i}. {pack}",
                        value="Available",
                        inline=True
                    )
                
                embed.set_footer(text=f"Example: {ctx.prefix}setpack {valid_packs[0]}")
                await ctx.send(embed=embed)
                return
            
            success = await set_user_pack_preference(str(ctx.author.id), ctx.author.display_name, matched_pack)
            
            if success:
                embed = discord.Embed(
                    title="✅ Pack Preference Set",
                    description=f"Your preferred pack has been set to: **{matched_pack}**",
                    color=discord.Color.green()
                )
                
                # Add pack emoji if available
                pack_emojis = {
                    'Charizard ex': '🔥',
                    'Pikachu ex': '⚡',
                    'Mewtwo ex': '🔮'
                }
                
                if matched_pack in pack_emojis:
                    embed.description = f"{pack_emojis[matched_pack]} {embed.description}"
                
                await ctx.send(embed=embed)
                logger.info(f"Pack preference set for {ctx.author}: {matched_pack}")
            else:
                await ctx.send("❌ Error setting pack preference. Please try again.")
                
        except Exception as e:
            logger.error(f"Error in setpack command: {e}")
            await ctx.send("❌ Error setting pack preference.")

    @commands.command(name='mypack')
    @commands.cooldown(1, getattr(config, 'COMMAND_COOLDOWN_SECONDS', 2), commands.BucketType.user)
    async def my_pack_command(self, ctx):
        """Show your pack preferences"""
        if not CORE_UTILS_AVAILABLE:
            await ctx.send("❌ Core user management system not available.")
            return
            
        try:
            preferences = await get_user_pack_preferences(str(ctx.author.id))
            
            if preferences:
                embed = discord.Embed(
                    title="📦 Your Pack Preferences",
                    color=discord.Color.blue(),
                    timestamp=datetime.datetime.now()
                )
                
                # Pack emojis for visual appeal
                pack_emojis = {
                    'Charizard ex': '🔥',
                    'Pikachu ex': '⚡',
                    'Mewtwo ex': '🔮'
                }
                
                selected_pack = preferences.get('selected_pack', 'None')
                pack_stats = preferences.get('pack_statistics', {})
                
                embed.add_field(
                    name="Selected Pack",
                    value=f"{pack_emojis.get(selected_pack, '📦')} {selected_pack}",
                    inline=False
                )
                
                if pack_stats:
                    stats_text = ""
                    for pack, count in pack_stats.items():
                        emoji = pack_emojis.get(pack, '📦')
                        stats_text += f"{emoji} {pack}: {count} packs\n"
                    
                    embed.add_field(
                        name="Pack Statistics",
                        value=stats_text,
                        inline=False
                    )
                
                embed.set_footer(text=f"User ID: {ctx.author.id}")
                await ctx.send(embed=embed)
            else:
                embed = discord.Embed(
                    title="📦 No Pack Preferences Found",
                    description=f"You haven't set any pack preferences yet!\nUse `{ctx.prefix}setpack <pack_name>` to set your preference.",
                    color=discord.Color.orange()
                )
                
                # Show available packs
                valid_packs = getattr(config, 'VALID_PACK_NAMES', [
                    'Charizard ex', 'Pikachu ex', 'Mewtwo ex'
                ])
                
                embed.add_field(
                    name="Available Packs",
                    value="\n".join([f"• {pack}" for pack in valid_packs]),
                    inline=False
                )
                
                await ctx.send(embed=embed)
                
        except Exception as e:
            logger.error(f"Error in mypack command: {e}")
            await ctx.send("❌ Error retrieving pack preferences.")

    @commands.command(name='userstats')
    @commands.cooldown(1, getattr(config, 'COMMAND_COOLDOWN_SECONDS', 2), commands.BucketType.user)
    async def user_stats_command(self, ctx, user: discord.Member = None):
        """Show statistics for a specific user"""
        if not CORE_UTILS_AVAILABLE:
            await ctx.send("❌ Core statistics system not available.")
            return
            
        target_user = user if user else ctx.author
        
        try:
            embed = await create_detailed_user_stats(self, str(target_user.id))
            if embed:
                # Add user avatar and additional info
                embed.set_thumbnail(url=target_user.display_avatar.url)
                embed.set_footer(text=f"User ID: {target_user.id}")
                await ctx.send(embed=embed)
            else:
                embed = discord.Embed(
                    title="❌ No Statistics Found",
                    description=f"No statistics found for {target_user.display_name}.",
                    color=discord.Color.red()
                )
                
                if target_user == ctx.author:
                    embed.add_field(
                        name="Get Started",
                        value=f"Start using the bot commands to generate statistics!\nTry `{ctx.prefix}setpack` to set your pack preference.",
                        inline=False
                    )
                
                await ctx.send(embed=embed)
                
        except Exception as e:
            logger.error(f"Error in userstats command: {e}")
            await ctx.send("❌ Error retrieving user statistics.")

    # ========================================================================================
    # INITIALIZATION AND ENHANCED SYSTEM SETUP
    # ========================================================================================
    
    async def initialize_enhanced_systems(self):
        """FIXED: Initialize all enhanced systems with comprehensive error handling"""
        try:
            logger.info("Initializing enhanced systems...")
            
            # Create required directories with proper error handling
            required_dirs = ['data', 'logs', 'backups', 'plots', 'exports', 'exports/analytics']
            
            # Add custom directories from config if available
            if hasattr(config, 'REQUIRED_DIRECTORIES'):
                required_dirs.extend(config.REQUIRED_DIRECTORIES)
            
            for directory in required_dirs:
                try:
                    os.makedirs(directory, exist_ok=True)
                    logger.debug(f"Directory ensured: {directory}")
                except PermissionError:
                    self.initialization_warnings.append(f"Permission denied creating directory: {directory}")
                    logger.warning(f"Permission denied creating directory: {directory}")
                except Exception as e:
                    self.initialization_warnings.append(f"Failed to create directory {directory}: {e}")
                    logger.warning(f"Failed to create directory {directory}: {e}")
            
            # Initialize database manager with error handling
            try:
                DatabaseManager, GPState, TestType = get_database_manager()
                if DatabaseManager and DATABASE_MANAGER_AVAILABLE:
                    self.db_manager = DatabaseManager()
                    self.components_available['database_manager'] = True
                    logger.info("✅ Database manager initialized")
                else:
                    self.initialization_warnings.append("Database manager not available")
                    logger.warning("⚠️ Database manager not available")
            except Exception as e:
                self.initialization_errors.append(f"Database manager initialization: {e}")
                logger.error(f"❌ Database manager initialization failed: {e}")
            
            # Initialize enhanced components if database is available
            if self.db_manager:
                await self._initialize_enhanced_components()
            else:
                logger.warning("Skipping enhanced components initialization - no database manager")
            
            # Run XML migration if needed and enabled
            await self._handle_xml_migration()
            
            # Initialize Google Sheets if enabled
            await self._initialize_google_sheets()
            
            # Log initialization summary
            available_count = sum(self.components_available.values())
            total_count = len(self.components_available)
            logger.info(f"🎉 Enhanced systems initialization completed: {available_count}/{total_count} components available")
            
            if self.initialization_errors:
                logger.warning(f"Initialization completed with {len(self.initialization_errors)} errors")
            
        except Exception as e:
            self.initialization_errors.append(f"Critical initialization error: {e}")
            logger.error(f"❌ Critical error initializing enhanced systems: {e}")
            traceback.print_exc()

    async def _initialize_enhanced_components(self):
        """Initialize individual enhanced components with error handling"""
        enhanced_components = get_enhanced_components()
        
        # Initialize Probability Calculator
        if 'ProbabilityCalculator' in enhanced_components and PROBABILITY_CALC_AVAILABLE:
            try:
                self.probability_calc = enhanced_components['ProbabilityCalculator'](self.db_manager)
                self.components_available['probability_calc'] = True
                logger.info("✅ Probability calculator initialized")
            except Exception as e:
                self.initialization_errors.append(f"Probability calculator: {e}")
                logger.error(f"❌ Probability calculator initialization failed: {e}")
        
        # Initialize Heartbeat Analytics
        if 'HeartbeatAnalytics' in enhanced_components and ANALYTICS_AVAILABLE:
            try:
                self.analytics = enhanced_components['HeartbeatAnalytics'](self.db_manager)
                self.components_available['analytics'] = True
                logger.info("✅ Heartbeat analytics initialized")
            except Exception as e:
                self.initialization_errors.append(f"Heartbeat analytics: {e}")
                logger.error(f"❌ Heartbeat analytics initialization failed: {e}")
        
        # Initialize Plotting System
        if 'PlottingSystem' in enhanced_components and PLOTTING_AVAILABLE:
            try:
                self.plotting = enhanced_components['PlottingSystem'](self.db_manager)
                self.components_available['plotting'] = True
                logger.info("✅ Plotting system initialized")
            except Exception as e:
                self.initialization_errors.append(f"Plotting system: {e}")
                logger.error(f"❌ Plotting system initialization failed: {e}")
        
        # Initialize Expiration Manager
        if 'ExpirationManager' in enhanced_components and EXPIRATION_MANAGER_AVAILABLE:
            try:
                self.expiration_manager = enhanced_components['ExpirationManager'](self.db_manager, self)
                self.components_available['expiration_manager'] = True
                logger.info("✅ Expiration manager initialized")
            except Exception as e:
                self.initialization_errors.append(f"Expiration manager: {e}")
                logger.error(f"❌ Expiration manager initialization failed: {e}")
        
        # Initialize Google Sheets Integration
        if 'GoogleSheetsIntegration' in enhanced_components and SHEETS_INTEGRATION_AVAILABLE:
            try:
                self.sheets_integration = enhanced_components['GoogleSheetsIntegration'](self.db_manager)
                self.components_available['sheets_integration'] = True
                logger.info("✅ Google Sheets integration initialized")
            except Exception as e:
                self.initialization_errors.append(f"Google Sheets integration: {e}")
                logger.error(f"❌ Google Sheets integration initialization failed: {e}")

    async def _handle_xml_migration(self):
        """Handle XML data migration with error handling"""
        try:
            if (hasattr(config, 'AUTO_MIGRATE_XML_DATA') and 
                config.AUTO_MIGRATE_XML_DATA and 
                self.db_manager and 
                XML_LEGACY_SUPPORT):
                
                try:
                    from migration import migrate_xml_to_sqlite
                    success = migrate_xml_to_sqlite(self.db_manager)
                    if success:
                        logger.info("✅ XML data migration completed")
                    else:
                        self.initialization_warnings.append("XML migration failed or no data to migrate")
                        logger.warning("⚠️ XML migration failed or no data to migrate")
                except ImportError:
                    self.initialization_warnings.append("Migration module not available")
                    logger.warning("⚠️ Migration module not available")
                except Exception as e:
                    self.initialization_errors.append(f"XML migration: {e}")
                    logger.error(f"❌ XML migration error: {e}")
            
        except Exception as e:
            logger.error(f"Error handling XML migration: {e}")

    async def _initialize_google_sheets(self):
        """Initialize Google Sheets integration with error handling"""
        try:
            if (hasattr(config, 'ENABLE_SHEETS_INTEGRATION') and 
                config.ENABLE_SHEETS_INTEGRATION and 
                self.sheets_integration):
                
                try:
                    guild_id = int(config.guild_id)
                    spreadsheet_name = f"PTCGP Bot Data - {config.guild_id}"
                    
                    if hasattr(self.sheets_integration, 'setup_guild_spreadsheet'):
                        await self.sheets_integration.setup_guild_spreadsheet(guild_id, spreadsheet_name)
                        logger.info("✅ Google Sheets integration setup completed")
                except ValueError:
                    self.initialization_errors.append("Invalid guild_id for Google Sheets setup")
                    logger.error("❌ Invalid guild_id for Google Sheets setup")
                except Exception as e:
                    self.initialization_warnings.append(f"Google Sheets setup failed: {e}")
                    logger.warning(f"⚠️ Google Sheets setup failed: {e}")
            
        except Exception as e:
            logger.error(f"Error initializing Google Sheets: {e}")

    # ========================================================================================
    # MESSAGE PROCESSING HELPER METHODS
    # ========================================================================================
    
    async def _is_heartbeat_message(self, message) -> bool:
        """Check if message is a heartbeat with enhanced detection"""
        try:
            content = message.content
            lines = content.split('\n')
            
            # Enhanced heartbeat detection
            if len(lines) >= 4:
                return (
                    'Online:' in lines[1] and 
                    'Offline:' in lines[2] and 
                    'Time:' in lines[3] and 
                    'Packs:' in lines[3]
                )
            return False
        except Exception as e:
            logger.error(f"Error checking heartbeat message: {e}")
            return False

    async def _process_heartbeat(self, message):
        """FIXED: Process heartbeat message with enhanced tracking and error handling"""
        if not self.db_manager:
            return
            
        try:
            lines = message.content.split('\n')
            
            # Extract discord ID (enhanced parsing)
            user_line = lines[0].strip()
            discord_id = None
            
            if user_line.isdigit():
                discord_id = int(user_line)
            else:
                # Try to extract from mention or username
                member = await self._resolve_user(message.guild, user_line)
                discord_id = member.id if member else None
            
            if not discord_id:
                logger.warning(f"Could not resolve user from heartbeat: {user_line}")
                return
            
            # Enhanced parsing of heartbeat data
            online_instances = self._extract_instances(lines[1], 'Online:')
            offline_instances = self._extract_instances(lines[2], 'Offline:')
            main_on = 'Main' in lines[1]
            
            # Extract time and packs with better error handling
            time_packs_line = lines[3]
            time_match = re.search(r'Time: (\d+)m', time_packs_line)
            packs_match = re.search(r'Packs: (\d+)', time_packs_line)
            
            time_minutes = int(time_match.group(1)) if time_match else 0
            packs = int(packs_match.group(1)) if packs_match else 0
            
            # Extract selected packs if available
            selected_packs = []
            if len(lines) > 4 and 'Select:' in lines[4]:
                try:
                    selected_str = lines[4].split('Select:')[1].strip()
                    selected_packs = [pack.strip() for pack in selected_str.split(',') if pack.strip()]
                except Exception as e:
                    logger.warning(f"Error parsing selected packs: {e}")
            
            # Store in database
            if hasattr(self.db_manager, 'add_heartbeat'):
                success = self.db_manager.add_heartbeat(
                    message.id, discord_id, message.created_at,
                    online_instances, offline_instances, time_minutes,
                    packs, main_on, selected_packs
                )
                
                if success:
                    # Update user status to active
                    if hasattr(self.db_manager, 'update_user_status'):
                        self.db_manager.update_user_status(discord_id, 'active')
                    logger.info(f"Processed heartbeat for user {discord_id}")
                    
                    # Auto-sync to sheets if enabled
                    if (self.sheets_integration and 
                        hasattr(self.sheets_integration, 'sync_enabled') and 
                        getattr(self.sheets_integration, 'sync_enabled', False)):
                        asyncio.create_task(self._auto_sync_user_data(message.guild.id, discord_id))
                else:
                    logger.warning(f"Failed to store heartbeat for user {discord_id}")
            
        except Exception as e:
            logger.error(f"Error processing heartbeat: {e}")
            traceback.print_exc()

    async def _is_godpack_message(self, message) -> bool:
        """Enhanced god pack detection with error handling"""
        try:
            content = message.content.lower()
            keywords = [
                'god pack found', 'godpack found', 'gp found',
                'rare pack found', 'special pack found'
            ]
            
            return any(keyword in content for keyword in keywords) and len(message.attachments) > 0
        except Exception as e:
            logger.error(f"Error checking god pack message: {e}")
            return False

    async def _process_godpack(self, message):
        """FIXED: Process god pack message with enhanced tracking and error handling"""
        if not self.db_manager:
            return
            
        try:
            # Enhanced god pack parsing
            content = message.content
            lines = content.split('\n')
            
            # Extract user name and friend code
            if len(lines) >= 2:
                name_fc_line = lines[1]
                name, friend_code = self._extract_name_and_fc(name_fc_line)
            else:
                name = "Unknown"
                friend_code = "Unknown"
            
            # Extract pack number and ratio
            pack_number = self._extract_pack_number(content)
            ratio = self._extract_ratio(content)
            
            # Determine state based on content
            DatabaseManager, GPState, TestType = get_database_manager()
            if GPState:
                if 'invalid' in content.lower():
                    state = GPState.INVALID
                else:
                    state = GPState.TESTING
            else:
                state = None
            
            # Get screenshot URL
            screenshot_url = str(message.attachments[0].url) if message.attachments else ""
            
            # Store in database
            if hasattr(self.db_manager, 'add_godpack'):
                gp_id = self.db_manager.add_godpack(
                    message.id, message.created_at, pack_number,
                    name, friend_code, state, screenshot_url, ratio
                )
                
                if gp_id:
                    logger.info(f"Processed god pack {gp_id}: {name}")
                    
                    # Create forum thread if configured
                    try:
                        await self._create_gp_thread(message.guild, gp_id)
                    except Exception as e:
                        logger.warning(f"Failed to create GP thread: {e}")
                    
                    # Auto-sync to sheets
                    if (self.sheets_integration and 
                        hasattr(self.sheets_integration, 'sync_enabled') and 
                        getattr(self.sheets_integration, 'sync_enabled', False)):
                        asyncio.create_task(self._auto_sync_gp_data(message.guild.id))
                else:
                    logger.warning(f"Failed to store god pack: {name}")
            
        except Exception as e:
            logger.error(f"Error processing god pack: {e}")
            traceback.print_exc()

    # ========================================================================================
    # UTILITY HELPER METHODS
    # ========================================================================================
    
    def _extract_instances(self, line: str, prefix: str) -> int:
        """Extract number of instances from a line with error handling"""
        try:
            if prefix not in line:
                return 0
                
            after_prefix = line.split(prefix)[1]
            # Count comma-separated items
            items = [item.strip() for item in after_prefix.split(',') if item.strip()]
            # Filter for actual instance identifiers (numbers or main)
            valid_items = []
            for item in items:
                item_clean = item.lower().strip()
                if item_clean == 'main' or item_clean.replace('.', '').isdigit():
                    valid_items.append(item)
            return len(valid_items)
        except Exception as e:
            logger.error(f"Error extracting instances from '{line}' with prefix '{prefix}': {e}")
            return 0

    def _extract_name_and_fc(self, line: str) -> tuple:
        """Extract name and friend code from line with enhanced error handling"""
        try:
            # Pattern: Name (FriendCode) or Name FriendCode
            import re
            
            # Try parentheses format first
            match = re.match(r'^(.*?)\s*\((.*?)\)', line.strip())
            if match:
                name = match.group(1).strip()
                fc = match.group(2).strip()
                return (name, fc) if name and fc else ("Unknown", "Unknown")
            
            # Try space-separated format
            parts = line.strip().split()
            if len(parts) >= 2:
                # Last part is likely friend code if it's numeric
                if parts[-1].isdigit() and len(parts[-1]) >= 9:
                    name = ' '.join(parts[:-1])
                    fc = parts[-1]
                    return (name, fc) if name else ("Unknown", fc)
            
            # Fallback - use the whole line as name
            return (line.strip() if line.strip() else "Unknown", "Unknown")
            
        except Exception as e:
            logger.error(f"Error extracting name and FC from '{line}': {e}")
            return ("Unknown", "Unknown")

    def _extract_pack_number(self, content: str) -> int:
        """Extract pack number from content with error handling"""
        try:
            import re
            
            # Look for patterns like "3 packs", "[3P]", "3P", etc.
            patterns = [
                r'(\d+)\s*packs?',
                r'\[(\d+)P\]',
                r'(\d+)P\b',
                r'Pack:?\s*(\d+)',
                r'(\d+)\s*pack'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    try:
                        number = int(match.group(1))
                        return max(1, number)  # Ensure at least 1
                    except (ValueError, IndexError):
                        continue
            
            return 1  # Default to 1 pack
            
        except Exception as e:
            logger.error(f"Error extracting pack number from content: {e}")
            return 1

    def _extract_ratio(self, content: str) -> int:
        """Extract ratio from content with error handling"""
        try:
            import re
            
            # Look for patterns like [2/5], (3/5), 4/5, etc.
            patterns = [
                r'\[(\d+)/5\]',
                r'\((\d+)/5\)',
                r'(\d+)/5',
                r'ratio:?\s*(\d+)'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    try:
                        ratio = int(match.group(1))
                        return max(0, min(5, ratio))  # Clamp between 0-5
                    except (ValueError, IndexError):
                        continue
            
            return -1  # Unknown ratio
            
        except Exception as e:
            logger.error(f"Error extracting ratio from content: {e}")
            return -1

    async def _resolve_user(self, guild, user_identifier: str):
        """Resolve user from various identifier formats with error handling"""
        try:
            if not guild or not user_identifier:
                return None
                
            user_identifier = user_identifier.strip()
            
            # Try direct ID
            if user_identifier.isdigit():
                user_id = int(user_identifier)
                return guild.get_member(user_id)
            
            # Try mention format
            if user_identifier.startswith('<@') and user_identifier.endswith('>'):
                user_id_str = user_identifier.strip('<@!>')
                if user_id_str.isdigit():
                    user_id = int(user_id_str)
                    return guild.get_member(user_id)
            
            # Try by display name (case insensitive)
            for member in guild.members:
                if member.display_name.lower() == user_identifier.lower():
                    return member
            
            # Try by username (case insensitive)
            for member in guild.members:
                if member.name.lower() == user_identifier.lower():
                    return member
            
            return None
            
        except Exception as e:
            logger.error(f"Error resolving user '{user_identifier}': {e}")
            return None

    async def _create_gp_thread(self, guild, gp_id: int):
        """Create forum thread for god pack with comprehensive error handling"""
        if not self.db_manager:
            return
            
        try:
            # Find god pack forum channel
            forum_channel = None
            for channel in guild.channels:
                if (isinstance(channel, discord.ForumChannel) and 
                    any(keyword in channel.name.lower() for keyword in ['godpack', 'gp', 'god pack'])):
                    forum_channel = channel
                    break
            
            if not forum_channel:
                logger.debug("No god pack forum channel found")
                return
            
            # Check bot permissions
            if not forum_channel.permissions_for(guild.me).create_public_threads:
                logger.warning(f"No permission to create threads in {forum_channel.name}")
                return
            
            if hasattr(self.db_manager, 'get_godpack'):
                godpack = self.db_manager.get_godpack(gp_id=gp_id)
                if not godpack:
                    logger.warning(f"God pack {gp_id} not found in database")
                    return
                
                # Create thread name with safe fallbacks
                state_name = getattr(godpack.state, 'value', 'UNKNOWN') if godpack.state else 'UNKNOWN'
                name = godpack.name if godpack.name else 'Unknown'
                pack_num = godpack.pack_number if godpack.pack_number else 1
                
                thread_name = f"[{state_name}] {name} ({pack_num}P)"
                
                # Create embed
                embed = discord.Embed(
                    title=f"God Pack - {name}",
                    color=discord.Color.orange(),
                    timestamp=datetime.datetime.now()
                )
                
                embed.add_field(name="Friend Code", value=godpack.friend_code or "Unknown", inline=True)
                embed.add_field(name="Pack Number", value=str(pack_num), inline=True)
                
                ratio_display = f"{godpack.ratio}/5" if godpack.ratio > 0 else "Unknown"
                embed.add_field(name="Ratio", value=ratio_display, inline=True)
                embed.add_field(name="GP ID", value=str(gp_id), inline=False)
                
                if godpack.screenshot_url:
                    embed.set_image(url=godpack.screenshot_url)
                
                # Create thread with initial message
                content = f"**God Pack ID**: {gp_id}\n\nUse `/probability {gp_id}` to see current probability."
                
                thread = await forum_channel.create_thread(
                    name=thread_name[:100],  # Discord limit
                    content=content,
                    embed=embed
                )
                
                # Add wishlist reaction with error handling
                try:
                    await thread.message.add_reaction("⭐")
                except Exception as e:
                    logger.warning(f"Failed to add reaction to GP thread: {e}")
                
                logger.info(f"Created thread for GP {gp_id}: {thread_name}")
            
        except discord.Forbidden:
            logger.warning("Missing permissions to create god pack thread")
        except discord.HTTPException as e:
            logger.error(f"Discord HTTP error creating GP thread: {e}")
        except Exception as e:
            logger.error(f"Error creating GP thread: {e}")

    async def _auto_sync_user_data(self, guild_id: int, discord_id: int):
        """Auto-sync user data to sheets with error handling"""
        if not self.sheets_integration:
            return
            
        try:
            await asyncio.sleep(5)  # Small delay to batch updates
            if hasattr(self.sheets_integration, 'sync_godpacks_to_sheet'):
                await self.sheets_integration.sync_godpacks_to_sheet(guild_id)
                logger.debug(f"Auto-synced user data for guild {guild_id}")
        except Exception as e:
            logger.error(f"Error auto-syncing user data: {e}")

    async def _auto_sync_gp_data(self, guild_id: int):
        """Auto-sync god pack data to sheets with error handling"""
        if not self.sheets_integration:
            return
            
        try:
            await asyncio.sleep(5)  # Small delay to batch updates
            if hasattr(self.sheets_integration, 'sync_godpacks_to_sheet'):
                await self.sheets_integration.sync_godpacks_to_sheet(guild_id)
                logger.debug(f"Auto-synced GP data for guild {guild_id}")
        except Exception as e:
            logger.error(f"Error auto-syncing GP data: {e}")

    # ========================================================================================
    # BACKGROUND TASKS (combining both systems) - with enhanced error handling
    # ========================================================================================
    
    @tasks.loop(minutes=getattr(config, 'stats_interval_minutes', 30))
    async def stats_sender(self):
        """FIXED: Background task to send statistics periodically with error handling"""
        if self.stats_task_running:
            logger.warning("Stats task already running, skipping...")
            return
        
        if not CORE_UTILS_AVAILABLE:
            logger.warning("Core utils not available, skipping stats")
            return
        
        try:
            self.stats_task_running = True
            await send_enhanced_stats(self, manual_trigger=False)
            self.last_stats_time = datetime.datetime.now()
            logger.info(f"Automated stats sent at {self.last_stats_time.strftime('%H:%M:%S')}")
        except Exception as e:
            logger.error(f"Error in stats sender task: {e}")
            traceback.print_exc()
        finally:
            self.stats_task_running = False

    @tasks.loop(hours=24)  # Run daily
    async def user_cleanup(self):
        """FIXED: Background task to clean up inactive users with error handling"""
        if not CORE_UTILS_AVAILABLE:
            logger.warning("Core utils not available, skipping user cleanup")
            return
            
        try:
            await cleanup_inactive_users()
            logger.info("Daily user cleanup completed")
        except Exception as e:
            logger.error(f"Error in user cleanup task: {e}")
            traceback.print_exc()

    @tasks.loop(hours=6)  # Run every 6 hours
    async def data_backup(self):
        """FIXED: Background task to backup user data with error handling"""
        if not CORE_UTILS_AVAILABLE:
            logger.warning("Core utils not available, skipping data backup")
            return
            
        try:
            success = await backup_user_data()
            if success:
                logger.info("Automated data backup completed")
            else:
                logger.warning("Data backup returned false")
        except Exception as e:
            logger.error(f"Error in data backup task: {e}")
            traceback.print_exc()

    @tasks.loop(hours=6)
    async def cleanup_task(self):
        """FIXED: Periodic cleanup task with enhanced error handling"""
        try:
            logger.info("Running periodic cleanup...")
            
            if self.db_manager:
                try:
                    # Clean up old data
                    if hasattr(self.db_manager, 'cleanup_old_data'):
                        self.db_manager.cleanup_old_data(days_to_keep=30)
                        logger.info("Database cleanup completed")
                except Exception as e:
                    logger.error(f"Database cleanup error: {e}")
                
                # Cache heartbeat runs for analytics
                if self.analytics:
                    try:
                        if hasattr(self.analytics, 'cache_run_data'):
                            self.analytics.cache_run_data()
                            logger.info("Analytics cache updated")
                    except Exception as e:
                        logger.error(f"Analytics cache error: {e}")
            
            # Clean up expiration warnings
            if self.expiration_manager:
                try:
                    if hasattr(self.expiration_manager, 'cleanup_old_expiration_data'):
                        await self.expiration_manager.cleanup_old_expiration_data(days_to_keep=7)
                        logger.info("Expiration data cleanup completed")
                except Exception as e:
                    logger.error(f"Expiration cleanup error: {e}")
            
            logger.info("Cleanup task completed successfully")
            
        except Exception as e:
            logger.error(f"Critical error in cleanup task: {e}")
            traceback.print_exc()

    @tasks.loop(hours=24)
    async def daily_sync_task(self):
        """FIXED: Daily sync task for Google Sheets with enhanced error handling"""
        try:
            if not self.sheets_integration:
                logger.debug("Sheets integration not available, skipping daily sync")
                return
                
            if not hasattr(self.sheets_integration, 'sync_enabled') or not self.sheets_integration.sync_enabled:
                logger.debug("Sheets sync not enabled, skipping daily sync")
                return
            
            logger.info("Running daily Google Sheets sync...")
            
            for guild in self.guilds:
                try:
                    # Full sync for each guild
                    if hasattr(self.sheets_integration, 'full_sync'):
                        results = await self.sheets_integration.full_sync(guild.id)
                        success_count = sum(results.values()) if results else 0
                        total_count = len(results) if results else 0
                        logger.info(f"Daily sync for {guild.name}: {success_count}/{total_count} successful")
                    
                    # Update daily statistics
                    try:
                        if hasattr(self.sheets_integration, 'update_daily_statistics'):
                            await self.sheets_integration.update_daily_statistics(guild.id)
                            logger.info(f"Daily statistics updated for {guild.name}")
                    except Exception as e:
                        logger.error(f"Failed to update daily statistics for {guild.name}: {e}")
                    
                except Exception as e:
                    logger.error(f"Error in daily sync for guild {guild.id} ({guild.name}): {e}")
                    traceback.print_exc()
            
            logger.info("Daily sync task completed")
            
        except Exception as e:
            logger.error(f"Critical error in daily sync task: {e}")
            traceback.print_exc()

# FIXED: Error handlers for background tasks
    @stats_sender.error
    async def stats_sender_error(self, error):
        """Handle stats sender task errors"""
        logger.error(f"Stats sender task error: {error}")
        self.stats_task_running = False
        
        # Try to restart the task
        try:
            await asyncio.sleep(60)  # Wait 1 minute before restart
            if not self.stats_sender.is_running():
                self.stats_sender.restart()
                logger.info("Stats sender task restarted after error")
        except Exception as e:
            logger.error(f"Failed to restart stats sender: {e}")

    @user_cleanup.error
    async def user_cleanup_error(self, error):
        """Handle user cleanup task errors"""
        logger.error(f"User cleanup task error: {error}")
        
        # Try to restart the task
        try:
            await asyncio.sleep(300)  # Wait 5 minutes before restart
            if not self.user_cleanup.is_running():
                self.user_cleanup.restart()
                logger.info("User cleanup task restarted after error")
        except Exception as e:
            logger.error(f"Failed to restart user cleanup: {e}")

    @cleanup_task.error
    async def cleanup_task_error(self, error):
        """Handle cleanup task errors"""
        logger.error(f"Cleanup task error: {error}")
        
        # Try to restart the task
        try:
            await asyncio.sleep(300)  # Wait 5 minutes before restart
            if not self.cleanup_task.is_running():
                self.cleanup_task.restart()
                logger.info("Cleanup task restarted after error")
        except Exception as e:
            logger.error(f"Failed to restart cleanup task: {e}")

    @daily_sync_task.error
    async def daily_sync_task_error(self, error):
        """Handle daily sync task errors"""
        logger.error(f"Daily sync task error: {error}")
        
        # Try to restart the task
        try:
            await asyncio.sleep(3600)  # Wait 1 hour before restart
            if not self.daily_sync_task.is_running():
                self.daily_sync_task.restart()
                logger.info("Daily sync task restarted after error")
        except Exception as e:
            logger.error(f"Failed to restart daily sync task: {e}")

    @data_backup.error
    async def data_backup_error(self, error):
        """Handle data backup task errors"""
        logger.error(f"Data backup task error: {error}")
        
        # Try to restart the task
        try:
            await asyncio.sleep(3600)  # Wait 1 hour before restart
            if hasattr(self, 'data_backup') and not self.data_backup.is_running():
                self.data_backup.restart()
                logger.info("Data backup task restarted after error")
        except Exception as e:
            logger.error(f"Failed to restart data backup task: {e}")

    # ========================================================================================
    # TASK BEFORE LOOP HANDLERS
    # ========================================================================================
    
    @stats_sender.before_loop
    async def before_stats_sender(self):
        """Wait for bot to be ready before starting stats sender"""
        await self.wait_until_ready()
        logger.info("Stats sender task waiting for bot ready...")

    @user_cleanup.before_loop
    async def before_user_cleanup(self):
        """Wait for bot to be ready before starting user cleanup"""
        await self.wait_until_ready()
        logger.info("User cleanup task waiting for bot ready...")

    @cleanup_task.before_loop
    async def before_cleanup_task(self):
        """Wait for bot to be ready before starting cleanup task"""
        await self.wait_until_ready()
        logger.info("Enhanced cleanup task waiting for bot ready...")

    @daily_sync_task.before_loop
    async def before_daily_sync_task(self):
        """Wait for bot to be ready before starting daily sync"""
        await self.wait_until_ready()
        logger.info("Daily sync task waiting for bot ready...")

    @data_backup.before_loop
    async def before_data_backup(self):
        """Wait for bot to be ready before starting data backup"""
        await self.wait_until_ready()
        logger.info("Data backup task waiting for bot ready...")

    # ========================================================================================
    # UTILITY METHODS FOR BOT STATUS AND MANAGEMENT
    # ========================================================================================
    
    def get_component_status(self) -> dict:
        """Get current status of all bot components"""
        return {
            'core_utils_available': CORE_UTILS_AVAILABLE,
            'xml_legacy_support': XML_LEGACY_SUPPORT,
            'enhanced_features_available': ENHANCED_FEATURES_AVAILABLE,
            'components': dict(self.components_available),
            'initialization_errors': len(self.initialization_errors),
            'initialization_warnings': len(self.initialization_warnings),
            'uptime_seconds': (datetime.datetime.now() - self.start_time).total_seconds(),
            'last_stats_time': self.last_stats_time.isoformat() if self.last_stats_time else None
        }

    async def safe_send_message(self, channel, content=None, embed=None, file=None):
        """Safely send a message with error handling"""
        if not channel:
            logger.warning("Attempted to send message to None channel")
            return None
            
        try:
            return await channel.send(content=content, embed=embed, file=file)
        except discord.Forbidden:
            logger.warning(f"No permission to send message in {channel}")
            return None
        except discord.HTTPException as e:
            logger.error(f"HTTP error sending message: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error sending message: {e}")
            return None

    def is_component_available(self, component_name: str) -> bool:
        """Check if a specific component is available"""
        return self.components_available.get(component_name, False)

    async def graceful_restart_task(self, task_name: str, delay: int = 300):
        """Gracefully restart a failed background task"""
        try:
            logger.info(f"Attempting to restart task: {task_name}")
            await asyncio.sleep(delay)
            
            if hasattr(self, task_name):
                task = getattr(self, task_name)
                if hasattr(task, 'restart'):
                    task.restart()
                    logger.info(f"Successfully restarted task: {task_name}")
                else:
                    logger.warning(f"Task {task_name} does not support restart")
            else:
                logger.warning(f"Task {task_name} not found")
                
        except Exception as e:
            logger.error(f"Error restarting task {task_name}: {e}")

    def log_system_state(self):
        """Log current system state for debugging"""
        try:
            status = self.get_component_status()
            logger.info(f"System state: {status}")
        except Exception as e:
            logger.error(f"Error logging system state: {e}")

    async def health_check_endpoint(self):
        """Simple health check for monitoring"""
        try:
            status = {
                'bot_status': 'online',
                'database': self.db_manager.test_connection() if self.db_manager and hasattr(self.db_manager, 'test_connection') else False,
                'uptime_seconds': (datetime.datetime.now() - self.start_time).total_seconds(),
                'guilds_connected': len(self.guilds),
                'enhanced_features': sum(self.components_available.values())
            }
            return status
        except Exception as e:
            return {'error': str(e)}

    # ========================================================================================
    # SHUTDOWN AND CLEANUP
    # ========================================================================================
    
    async def close(self):
        """FIXED: Clean shutdown for both systems with comprehensive cleanup"""
        logger.info("Unified bot shutting down...")
        
        try:
            # Stop enhanced system components
            if self.expiration_manager:
                try:
                    if hasattr(self.expiration_manager, 'stop_expiration_monitoring'):
                        await self.expiration_manager.stop_expiration_monitoring()
                        logger.info("Expiration manager stopped")
                except Exception as e:
                    logger.error(f"Error stopping expiration manager: {e}")
            
            # Stop all background tasks
            tasks_to_cancel = []
            
            # Add core tasks
            if hasattr(self, 'stats_sender'):
                tasks_to_cancel.append(self.stats_sender)
            if hasattr(self, 'user_cleanup'):
                tasks_to_cancel.append(self.user_cleanup)
            if hasattr(self, 'cleanup_task'):
                tasks_to_cancel.append(self.cleanup_task)
            if hasattr(self, 'daily_sync_task'):
                tasks_to_cancel.append(self.daily_sync_task)
            if hasattr(self, 'data_backup'):
                tasks_to_cancel.append(self.data_backup)
            
            # Cancel all tasks gracefully
            for task in tasks_to_cancel:
                if task and hasattr(task, 'is_running') and task.is_running():
                    try:
                        task.cancel()
                        logger.info(f"Cancelled task: {task}")
                    except Exception as e:
                        logger.error(f"Error cancelling task {task}: {e}")
            
            # Wait for tasks to complete cancellation
            if tasks_to_cancel:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*[task for task in tasks_to_cancel if task], return_exceptions=True),
                        timeout=10.0
                    )
                except asyncio.TimeoutError:
                    logger.warning("Some tasks did not cancel within timeout")
                except Exception as e:
                    logger.error(f"Error waiting for task cancellation: {e}")
            
            # Close database connections
            if self.db_manager and hasattr(self.db_manager, 'close'):
                try:
                    self.db_manager.close()
                    logger.info("Database connections closed")
                except Exception as e:
                    logger.error(f"Error closing database: {e}")
            
            # Final cleanup
            logger.info("Bot shutdown cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during bot shutdown: {e}")
        
        # Call parent close method
        try:
            await super().close()
        except Exception as e:
            logger.error(f"Error in parent close method: {e}")

# End of UnifiedPTCGPBot class

# ========================================================================================
# MODULE-LEVEL FUNCTIONS (outside the class - NO INDENTATION)
# ========================================================================================

def validate_startup_requirements():
    """FIXED: Validate all startup requirements with comprehensive checks"""
    errors = []
    warnings = []
    
    # Check Python version
    import sys
    if sys.version_info < (3, 8):
        errors.append("Python 3.8+ is required")
    
    # Check Python modules
    required_modules = [
        'discord', 'matplotlib', 'numpy', 'pandas', 
        'sqlite3', 'asyncio', 'datetime', 'logging', 're'
    ]
    
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            if module in ['numpy', 'pandas', 'matplotlib']:
                warnings.append(f"Optional module '{module}' not installed - some features may be limited")
            else:
                errors.append(f"Required module '{module}' not installed")
    
    # Check file structure
    required_files = ['config.py']
    for file in required_files:
        if not os.path.exists(file):
            errors.append(f"Required file '{file}' not found")
    
    # Check config
    try:
        import config
        if not hasattr(config, 'token') or not config.token:
            errors.append("Discord token not configured in config.py")
        if not hasattr(config, 'guild_id') or not config.guild_id:
            errors.append("Guild ID not configured in config.py")
        
        # Check if guild_id is valid
        if hasattr(config, 'guild_id') and config.guild_id:
            if not str(config.guild_id).isdigit():
                errors.append("Guild ID must be a valid Discord guild ID (numeric)")
                
    except ImportError:
        errors.append("Config file not found or contains syntax errors")
    except Exception as e:
        errors.append(f"Config file error: {e}")
    
    # Check write permissions for required directories
    test_dirs = ['data', 'logs', 'backups']
    for directory in test_dirs:
        try:
            os.makedirs(directory, exist_ok=True)
            # Test write permission
            test_file = os.path.join(directory, 'test_write.tmp')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)  # Clean up test file
        except PermissionError:
            warnings.append(f"No write permission for directory: {directory}")
        except Exception as e:
            warnings.append(f"Cannot access directory {directory}: {e}")

    # Optional modules check
    optional_modules = [
        'database_manager', 'heartbeat_analytics', 'plotting_system',
        'expiration_manager', 'google_sheets_integration', 'enhanced_bot_commands',
        'core_utils', 'utils', 'xml_manager'
    ]

    for module in optional_modules:
        try:
            __import__(module)
        except ImportError:
            warnings.append(f"Optional module '{module}' not available - some features will be disabled")

    return len(errors) == 0, errors, warnings

async def initialize_unified_bot():
    """FIXED: Initialize unified bot data and settings with comprehensive error handling"""
    try:
        logger.info("Starting unified bot initialization...")
        
        # Validate configuration first
        try:
            validate_critical_config()
            logger.info("✅ Configuration validation passed")
        except ValueError as e:
            logger.error(f"❌ Configuration validation failed: {e}")
            raise
        
        # Create necessary directories with error handling
        required_dirs = [
            'data', 'logs', 'backups', 'plots', 'exports', 'exports/analytics'
        ]
        
        # Add any additional directories from config
        try:
            if hasattr(config, 'REQUIRED_DIRECTORIES'):
                required_dirs.extend(config.REQUIRED_DIRECTORIES)
        except Exception as e:
            logger.warning(f"Error reading REQUIRED_DIRECTORIES from config: {e}")
        
        for directory in required_dirs:
            try:
                os.makedirs(directory, exist_ok=True)
                logger.debug(f"Directory ensured: {directory}")
            except PermissionError:
                logger.warning(f"Permission denied creating directory: {directory}")
            except Exception as e:
                logger.warning(f"Failed to create directory {directory}: {e}")
        
        # Test database path if configured
        try:
            if hasattr(config, 'DATABASE_PATH'):
                db_path = config.DATABASE_PATH
                db_dir = os.path.dirname(db_path)
                if db_dir and not os.path.exists(db_dir):
                    os.makedirs(db_dir, exist_ok=True)
                    logger.info(f"Created database directory: {db_dir}")
        except Exception as e:
            logger.warning(f"Database path setup warning: {e}")
        
        logger.info('✅ Unified bot initialization completed')
        
    except Exception as e:
        logger.error(f'❌ Critical error during bot initialization: {e}')
        raise

def main():
    """FIXED: Main function to run the unified bot with comprehensive error handling"""
    # Setup logging first
    if not setup_logging():
        print("Failed to setup logging, continuing with basic logging...")
        logging.basicConfig(level=logging.INFO)
    
    logger = logging.getLogger(__name__)
    logger.info("Starting PTCGP Unified Bot...")
    
    # Validate startup requirements
    try:
        is_valid, errors, warnings = validate_startup_requirements()
        
        if errors:
            logger.error("Startup validation failed:")
            for error in errors:
                logger.error(f"  - {error}")
            logger.error("Please fix these issues before starting the bot.")
            return 1
        
        if warnings:
            logger.warning("Startup warnings:")
            for warning in warnings:
                logger.warning(f"  - {warning}")
    except Exception as e:
        logger.error(f"Error during startup validation: {e}")
        return 1
    
    # Get token with proper error handling
    TOKEN = None
    try:
        TOKEN = getattr(config, 'token', None) or os.getenv('DISCORD_TOKEN')
        if not TOKEN:
            logger.error("Discord token not found in config.py 'token' variable or DISCORD_TOKEN environment variable")
            return 1
        
        # Basic token validation
        if len(TOKEN) < 50:  # Discord tokens are typically much longer
            logger.error("Discord token appears to be invalid (too short)")
            return 1
            
    except Exception as e:
        logger.error(f"Error getting Discord token: {e}")
        return 1
    
    # Create and run unified bot
    bot = None
    exit_code = 0
    
    try:
        logger.info("Creating bot instance...")
        print("🤖 Creating bot instance...")
        bot = UnifiedPTCGPBot()
        
        # Initialize bot
        logger.info("Initializing bot systems...")
        print("🔧 Initializing bot systems...")
        asyncio.run(initialize_unified_bot())
        
        # Run the bot
        logger.info("🚀 Starting unified PTCGP bot...")
        print("🚀 Bot is starting up...")
        print("=" * 50)
        bot.run(TOKEN)
        
    except KeyboardInterrupt:
        logger.info("🛑 Bot shutdown requested by user (Ctrl+C)")
        print("🛑 Bot shutdown requested by user")
        exit_code = 0
    except discord.LoginFailure:
        logger.error("❌ Failed to login - check your Discord token")
        print("❌ Failed to login - check your Discord token")
        exit_code = 1
    except discord.PrivilegedIntentsRequired:
        logger.error("❌ Bot requires privileged intents - enable them in Discord Developer Portal")
        print("❌ Bot requires privileged intents - enable them in Discord Developer Portal")
        exit_code = 1
    except discord.HTTPException as e:
        logger.error(f"❌ Discord HTTP error: {e}")
        print(f"❌ Discord HTTP error: {e}")
        exit_code = 1
    except ConnectionError as e:
        logger.error(f"❌ Connection error: {e}")
        print(f"❌ Connection error - check your internet connection")
        exit_code = 1
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        traceback.print_exc()
        print(f"❌ Fatal error: {e}")
        exit_code = 1
    finally:
        logger.info("👋 Unified bot shutdown sequence initiated")
        print("👋 Shutting down...")
        
        # Clean shutdown
        if bot:
            try:
                asyncio.run(bot.close())
                logger.info("✅ Bot cleanup completed")
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")
        
        logger.info("🔚 Bot shutdown complete")
        print("🔚 Bot shutdown complete")
        
    return exit_code

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
