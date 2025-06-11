import os
from typing import Dict, List, Any, Optional, Tuple
import json
import logging
from pathlib import Path
import discord

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# ENVIRONMENT VARIABLES AND SECURITY
# =============================================================================

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    logger.warning("python-dotenv not installed. Using system environment variables only.")

# =============================================================================
# DISCORD CONFIGURATION - SECURE
# =============================================================================

# SECURITY FIX: Use environment variables for sensitive data
token = os.getenv('DISCORD_TOKEN', '')
guild_id_str = os.getenv('DISCORD_GUILD_ID', '0')

# FIXED: Safe conversion of guild_id with error handling
try:
    guild_id = int(guild_id_str) if guild_id_str.isdigit() else 0
except (ValueError, TypeError):
    guild_id = 0
    logger.error(f"Invalid DISCORD_GUILD_ID: {guild_id_str}")

# Validate token is set
if not token:
    logger.error("DISCORD_TOKEN environment variable not set!")
    logger.info("Please set DISCORD_TOKEN in your environment or .env file")

# =============================================================================
# DISCORD PERMISSIONS VALIDATION
# =============================================================================

# Required bot permissions for proper operation
REQUIRED_BOT_PERMISSIONS = [
    'send_messages',
    'embed_links', 
    'attach_files',
    'read_message_history',
    'add_reactions',
    'manage_threads',
    'create_public_threads',
    'view_channel',
    'use_slash_commands',
    'manage_messages'
]

# Optional but recommended permissions
RECOMMENDED_BOT_PERMISSIONS = [
    'manage_webhooks',
    'manage_channels',
    'kick_members',
    'ban_members'
]

def validate_bot_permissions(bot_permissions: discord.Permissions) -> Tuple[bool, List[str], List[str]]:
    """Validate bot permissions against required and recommended lists"""
    missing_required = []
    missing_recommended = []
    
    for perm in REQUIRED_BOT_PERMISSIONS:
        if not getattr(bot_permissions, perm, False):
            missing_required.append(perm)
    
    for perm in RECOMMENDED_BOT_PERMISSIONS:
        if not getattr(bot_permissions, perm, False):
            missing_recommended.append(perm)
    
    return len(missing_required) == 0, missing_required, missing_recommended

async def validate_guild_setup(bot, guild_id: int) -> Dict[str, Any]:
    """Comprehensive guild setup validation"""
    validation_results = {
        'permissions': {'valid': False, 'missing_required': [], 'missing_recommended': []},
        'channels': {'valid': False, 'missing_channels': [], 'inaccessible_channels': []},
        'roles': {'valid': False, 'missing_admin_roles': []},
        'features': {'valid': True, 'disabled_features': []},
        'overall_valid': False
    }
    
    try:
        guild = bot.get_guild(guild_id)
        if not guild:
            validation_results['error'] = f"Bot not in guild {guild_id}"
            return validation_results
        
        bot_member = guild.get_member(bot.user.id)
        if not bot_member:
            validation_results['error'] = "Bot member not found in guild"
            return validation_results
        
        # Validate permissions
        bot_permissions = bot_member.guild_permissions
        perm_valid, missing_req, missing_rec = validate_bot_permissions(bot_permissions)
        validation_results['permissions'] = {
            'valid': perm_valid,
            'missing_required': missing_req,
            'missing_recommended': missing_rec
        }
        
        # Validate channels
        required_channels = {
            'commands': channel_id_commands,
            'user_stats': channel_id_user_stats,
            'heartbeat': channel_id_heartbeat,
            'webhook': channel_id_webhook
        }
        
        missing_channels = []
        inaccessible_channels = []
        
        for channel_name, channel_id in required_channels.items():
            if not channel_id or channel_id == 0:
                missing_channels.append(channel_name)
                continue
                
            channel = guild.get_channel(channel_id)
            if not channel:
                missing_channels.append(f"{channel_name} (ID: {channel_id})")
                continue
            
            channel_perms = channel.permissions_for(bot_member)
            required_channel_perms = ['view_channel', 'send_messages', 'embed_links']
            
            for perm in required_channel_perms:
                if not getattr(channel_perms, perm, False):
                    inaccessible_channels.append(f"{channel_name} (missing {perm})")
                    break
        
        validation_results['channels'] = {
            'valid': len(missing_channels) == 0 and len(inaccessible_channels) == 0,
            'missing_channels': missing_channels,
            'inaccessible_channels': inaccessible_channels
        }
        
        # Validate admin roles
        missing_admin_roles = []
        if admin_role_ids:
            for role_id in admin_role_ids:
                role = guild.get_role(role_id)
                if not role:
                    missing_admin_roles.append(str(role_id))
        
        validation_results['roles'] = {
            'valid': len(missing_admin_roles) == 0,
            'missing_admin_roles': missing_admin_roles
        }
        
        # Validate features
        disabled_features = []
        if not FEATURES.get('google_sheets_integration', False):
            disabled_features.append("Google Sheets integration")
        if not FEATURES.get('plotting_system', False):
            disabled_features.append("Plotting system")
        if not create_threads_for_god_packs and not bot_permissions.manage_threads:
            disabled_features.append("Thread creation (no permission)")
        if not upload_gist and not github_token:
            disabled_features.append("GitHub Gist uploads")
        
        validation_results['features'] = {
            'valid': True,
            'disabled_features': disabled_features
        }
        
        # Overall validation
        validation_results['overall_valid'] = (
            validation_results['permissions']['valid'] and
            validation_results['channels']['valid'] and
            validation_results['roles']['valid']
        )
        
    except Exception as e:
        validation_results['error'] = f"Validation error: {str(e)}"
        logger.error(f"Guild validation error: {e}")
    
    return validation_results

def log_validation_results(results: Dict[str, Any]):
    """Log validation results in a readable format"""
    if 'error' in results:
        logger.critical(f"🚨 Guild validation failed: {results['error']}")
        return
    
    if results['overall_valid']:
        logger.info("✅ Guild validation passed successfully")
    else:
        logger.critical("🚨 Guild validation failed!")
    
    perm_results = results['permissions']
    if perm_results['missing_required']:
        logger.critical(f"❌ Missing required permissions: {', '.join(perm_results['missing_required'])}")
    if perm_results['missing_recommended']:
        logger.warning(f"⚠️ Missing recommended permissions: {', '.join(perm_results['missing_recommended'])}")
    
    channel_results = results['channels']
    if channel_results['missing_channels']:
        logger.critical(f"❌ Missing channels: {', '.join(channel_results['missing_channels'])}")
    if channel_results['inaccessible_channels']:
        logger.critical(f"❌ Inaccessible channels: {', '.join(channel_results['inaccessible_channels'])}")
    
    role_results = results['roles']
    if role_results['missing_admin_roles']:
        logger.warning(f"⚠️ Missing admin roles: {', '.join(role_results['missing_admin_roles'])}")
    
    feature_results = results['features']
    if feature_results['disabled_features']:
        logger.info(f"ℹ️ Disabled features: {', '.join(feature_results['disabled_features'])}")

def create_permission_invite_url(client_id: str) -> str:
    """Generate Discord invite URL with all required permissions"""
    permissions = discord.Permissions()
    for perm_name in REQUIRED_BOT_PERMISSIONS + RECOMMENDED_BOT_PERMISSIONS:
        if hasattr(permissions, perm_name):
            setattr(permissions, perm_name, True)
    
    invite_url = f"https://discord.com/api/oauth2/authorize?client_id={client_id}&permissions={permissions.value}&scope=bot%20applications.commands"
    return invite_url

# =============================================================================
# UNIFIED DATABASE CONFIGURATION
# =============================================================================

DATA_DIR = Path(os.getenv('BOT_DATA_DIR', 'data'))
DATA_DIR.mkdir(exist_ok=True)

DATABASE_PATH = DATA_DIR / 'ptcgp_unified.db'
database_path = str(DATABASE_PATH)
db_path = database_path

# =============================================================================
# SECURE API KEYS AND TOKENS
# =============================================================================

github_token = os.getenv('GITHUB_TOKEN', '')
gist_id = os.getenv('GITHUB_GIST_ID', '')
pastebin_api_key = os.getenv('PASTEBIN_API_KEY', '')
pastebin_user_key = os.getenv('PASTEBIN_USER_KEY', '')

git_token = github_token
git_gist_id = gist_id
git_gist_group_name = os.getenv('GIT_GIST_GROUP_NAME', 'ptcgp_bot')
git_gist_gp_name = os.getenv('GIT_GIST_GP_NAME', 'godpacks')

# =============================================================================
# DISCORD CHANNEL IDS - SECURE CONFIGURATION
# =============================================================================

def safe_int_conversion(value: str, default: int = 0) -> int:
    """Safely convert string to int with error handling"""
    if not value or value.strip() == '':
        return default
    
    # Remove any non-numeric characters except digits
    cleaned = ''.join(c for c in str(value) if c.isdigit())
    
    if not cleaned:
        return default
        
    try:
        return int(cleaned)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert '{value}' to integer, using default {default}")
        return default

def load_channel_config():
    """Load channel configuration from environment or JSON file"""
    channels = {
        'channel_id_commands': safe_int_conversion(os.getenv('CHANNEL_ID_COMMANDS', '0')),
        'channel_id_user_stats': safe_int_conversion(os.getenv('CHANNEL_ID_USER_STATS', '0')),
        'channel_id_heartbeat': safe_int_conversion(os.getenv('CHANNEL_ID_HEARTBEAT', '0')),
        'channel_id_webhook': safe_int_conversion(os.getenv('CHANNEL_ID_WEBHOOK', '0')),
    }
    
    config_file = DATA_DIR / 'channels.json'
    if config_file.exists() and any(v == 0 for v in channels.values()):
        try:
            with open(config_file, 'r') as f:
                file_config = json.load(f)
                for key, value in file_config.items():
                    if channels.get(key, 0) == 0:
                        channels[key] = safe_int_conversion(str(value))
        except Exception as e:
            logger.error(f"Error loading channel config: {e}")
    
    return channels

channel_config = load_channel_config()

channel_id_commands = channel_config.get('channel_id_commands', 0)
channel_id_user_stats = channel_config.get('channel_id_user_stats', 0)
channel_id_heartbeat = channel_config.get('channel_id_heartbeat', 0)
channel_id_webhook = channel_config.get('channel_id_webhook', 0)

stats_channel_id = channel_id_user_stats

# Optional forum channels - using safe conversion
channel_id_mewtwo_verification_forum = safe_int_conversion(os.getenv('CHANNEL_MEWTWO_FORUM', '0'))
channel_id_charizard_verification_forum = safe_int_conversion(os.getenv('CHANNEL_CHARIZARD_FORUM', '0'))
channel_id_pikachu_verification_forum = safe_int_conversion(os.getenv('CHANNEL_PIKACHU_FORUM', '0'))
channel_id_mew_verification_forum = safe_int_conversion(os.getenv('CHANNEL_MEW_FORUM', '0'))
channel_id_dialga_verification_forum = safe_int_conversion(os.getenv('CHANNEL_DIALGA_FORUM', '0'))
channel_id_palkia_verification_forum = safe_int_conversion(os.getenv('CHANNEL_PALKIA_FORUM', '0'))
channel_id_arceus_verification_forum = safe_int_conversion(os.getenv('CHANNEL_ARCEUS_FORUM', '0'))
channel_id_shining_verification_forum = safe_int_conversion(os.getenv('CHANNEL_SHINING_FORUM', '0'))
channel_id_solgaleo_verification_forum = safe_int_conversion(os.getenv('CHANNEL_SOLGALEO_FORUM', '0'))
channel_id_lunala_verification_forum = safe_int_conversion(os.getenv('CHANNEL_LUNALA_FORUM', '0'))
channel_id_buzzwole_verification_forum = safe_int_conversion(os.getenv('CHANNEL_BUZZWOLE_FORUM', '0'))
channel_id_2star_verification_forum = safe_int_conversion(os.getenv('CHANNEL_2STAR_FORUM', '0'))

channel_id_anticheat = safe_int_conversion(os.getenv('CHANNEL_ANTICHEAT', '0'))
channel_id_gp_tracking_list = safe_int_conversion(os.getenv('CHANNEL_GP_TRACKING', '0'))
channel_id_notifications = safe_int_conversion(os.getenv('CHANNEL_NOTIFICATIONS', '0'))

channel_id_analytics_reports = safe_int_conversion(os.getenv('CHANNEL_ANALYTICS', '0')) or channel_id_user_stats
channel_id_expiration_warnings = safe_int_conversion(os.getenv('CHANNEL_EXPIRATION', '0')) or channel_id_notifications
channel_id_probability_logs = safe_int_conversion(os.getenv('CHANNEL_PROBABILITY', '0')) or channel_id_commands
channel_id_system_status = safe_int_conversion(os.getenv('CHANNEL_SYSTEM', '0')) or channel_id_commands

# =============================================================================
# BOT SETTINGS - CONFIGURABLE
# =============================================================================

command_prefix = os.getenv('BOT_PREFIX', '!')

# FIXED: Safe admin role IDs parsing with comprehensive error handling
def parse_admin_role_ids():
    """Safely parse admin role IDs from environment variable"""
    admin_roles_str = os.getenv('ADMIN_ROLE_IDS', '').strip()
    
    if not admin_roles_str:
        return []
    
    role_ids = []
    for role_str in admin_roles_str.split(','):
        role_str = role_str.strip()
        if not role_str:
            continue
            
        # Skip placeholder values
        if role_str.lower() in ['role_id1', 'role_id2', 'role_id_here', 'your_role_id']:
            continue
            
        # Try to convert to int
        try:
            if role_str.isdigit():
                role_ids.append(int(role_str))
            else:
                logger.warning(f"Skipping invalid admin role ID: '{role_str}' (not a number)")
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse admin role ID '{role_str}': {e}")
    
    return role_ids

admin_role_ids = parse_admin_role_ids()
user_cleanup_days = safe_int_conversion(os.getenv('USER_CLEANUP_DAYS', '30'), 30)

enable_auto_backup = os.getenv('ENABLE_AUTO_BACKUP', 'true').lower() == 'true'
BACKUP_INTERVAL_HOURS = safe_int_conversion(os.getenv('BACKUP_INTERVAL_HOURS', '24'), 24)
BACKUP_DIR = DATA_DIR / 'backups'
BACKUP_DIR.mkdir(exist_ok=True)
DATABASE_BACKUP_PATH = str(BACKUP_DIR / 'database')

MAX_BACKUP_COUNT = safe_int_conversion(os.getenv('MAX_BACKUP_COUNT', '50'), 50)
COMPRESS_BACKUPS = os.getenv('COMPRESS_BACKUPS', 'true').lower() == 'true'
VERIFY_BACKUPS = os.getenv('VERIFY_BACKUPS', 'true').lower() == 'true'

upload_gist = os.getenv('UPLOAD_GIST', 'true').lower() == 'true'
backup_to_pastebin = os.getenv('BACKUP_PASTEBIN', 'false').lower() == 'true'
upload_logs = os.getenv('UPLOAD_LOGS', 'false').lower() == 'true'

stats_interval_minutes = safe_int_conversion(os.getenv('STATS_INTERVAL', '30'), 30)

# =============================================================================
# GOOGLE SHEETS INTEGRATION
# =============================================================================

GOOGLE_CREDENTIALS_FILE = os.getenv('GOOGLE_CREDENTIALS_FILE', 'credentials.json')
ENABLE_SHEETS_INTEGRATION = (
    os.getenv('ENABLE_SHEETS', 'false').lower() == 'true' and
    os.path.exists(GOOGLE_CREDENTIALS_FILE)
)

AUTO_SYNC_INTERVAL_MINUTES = safe_int_conversion(os.getenv('SHEETS_SYNC_INTERVAL', '30'), 30)
SPREADSHEET_NAME = os.getenv('SPREADSHEET_NAME', f"PTCGP Reroll Data - Guild {guild_id}")

SHEETS_SYNC_ON_STARTUP = ENABLE_SHEETS_INTEGRATION
SHEETS_SYNC_ON_GP_FOUND = ENABLE_SHEETS_INTEGRATION
SHEETS_SYNC_ON_USER_UPDATE = False
SHEETS_DAILY_BACKUP = ENABLE_SHEETS_INTEGRATION

# =============================================================================
# PACK FILTERS AND ROLE CONFIGURATION
# =============================================================================

enable_role_based_filters = os.getenv('ENABLE_ROLE_FILTERS', 'true').lower() == 'true'

def load_pack_filters():
    """Load pack filter configuration"""
    filters_file = DATA_DIR / 'pack_filters.json'
    
    if filters_file.exists():
        try:
            with open(filters_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading pack filters: {e}")
    
    return {
        "genetic_apex": {
            "required_roles": ["Genetic Apex"],
            "filters": {
                "minimum_cards": {"rare": 1},
                "rarity_requirements": {"epic": False},
                "special_conditions": {}
            },
            "priority": 1
        },
        "mythical_island": {
            "required_roles": ["Mythical Island"],
            "filters": {
                "minimum_cards": {"rare": 1},
                "rarity_requirements": {"legendary": False},
                "special_conditions": {}
            },
            "priority": 2
        }
    }

pack_filters = load_pack_filters()

# =============================================================================
# PERFORMANCE AND LIMITS - All using safe conversion
# =============================================================================

DB_CONNECTION_POOL_SIZE = safe_int_conversion(os.getenv('DB_POOL_SIZE', '5'), 5)
DB_QUERY_TIMEOUT_SECONDS = safe_int_conversion(os.getenv('DB_TIMEOUT', '30'), 30)
ENABLE_DATABASE_WAL_MODE = os.getenv('DB_WAL_MODE', 'true').lower() == 'true'
DATA_RETENTION_DAYS = safe_int_conversion(os.getenv('DATA_RETENTION_DAYS', '30'), 30)

MAX_CONCURRENT_OPERATIONS = safe_int_conversion(os.getenv('MAX_CONCURRENT_OPS', '5'), 5)
MAX_PROBABILITY_CALCULATIONS_PER_MINUTE = safe_int_conversion(os.getenv('MAX_PROB_CALC_PER_MIN', '60'), 60)
MAX_SHEET_UPDATES_PER_HOUR = safe_int_conversion(os.getenv('MAX_SHEET_UPDATES_PER_HOUR', '100'), 100)
MAX_PLOT_GENERATIONS_PER_HOUR = safe_int_conversion(os.getenv('MAX_PLOTS_PER_HOUR', '20'), 20)

MAX_GLOBAL_REQUESTS_PER_MINUTE = safe_int_conversion(os.getenv('MAX_GLOBAL_REQUESTS_PER_MINUTE', '500'), 500)
MAX_USER_COMMANDS_PER_5MIN = safe_int_conversion(os.getenv('MAX_USER_COMMANDS_PER_5MIN', '100'), 100)
MAX_HEAVY_COMMANDS_PER_HOUR = safe_int_conversion(os.getenv('MAX_HEAVY_COMMANDS_PER_HOUR', '20'), 20)
MAX_ADMIN_COMMANDS_PER_HOUR = safe_int_conversion(os.getenv('MAX_ADMIN_COMMANDS_PER_HOUR', '50'), 50)

MAX_MEMORY_USAGE_MB = safe_int_conversion(os.getenv('MAX_MEMORY_MB', '1024'), 1024)
CLEANUP_INTERVAL_HOURS = safe_int_conversion(os.getenv('CLEANUP_INTERVAL', '6'), 6)
ENABLE_MEMORY_MONITORING = os.getenv('ENABLE_MEMORY_MONITOR', 'true').lower() == 'true'

COMMAND_COOLDOWN_SECONDS = safe_int_conversion(os.getenv('COMMAND_COOLDOWN', '2'), 2)

# =============================================================================
# PROBABILITY AND ANALYTICS - Using safe conversion for numeric values
# =============================================================================

PROBABILITY_CACHE_TIMEOUT_MINUTES = safe_int_conversion(os.getenv('PROB_CACHE_TIMEOUT', '5'), 5)
PROBABILITY_CONFIDENCE_THRESHOLD = float(os.getenv('PROB_CONFIDENCE_THRESHOLD', '30'))
ENABLE_BAYESIAN_UPDATES = os.getenv('ENABLE_BAYESIAN', 'true').lower() == 'true'

DEFAULT_MIN_FRIENDS_ASSUMPTION = safe_int_conversion(os.getenv('DEFAULT_MIN_FRIENDS', '6'), 6)
PROBABILITY_RECALC_ON_TEST = os.getenv('PROB_RECALC_ON_TEST', 'true').lower() == 'true'
ENABLE_MEMBER_PROBABILITY_BREAKDOWN = os.getenv('ENABLE_PROB_BREAKDOWN', 'true').lower() == 'true'

MISS_TEST_WEIGHT = float(os.getenv('MISS_TEST_WEIGHT', '1.0'))
NOSHOW_TEST_WEIGHT = float(os.getenv('NOSHOW_TEST_WEIGHT', '0.7'))

# =============================================================================
# HEARTBEAT AND MONITORING - Using safe conversion
# =============================================================================

heartbeat_rate = safe_int_conversion(os.getenv('HEARTBEAT_RATE', '30'), 30)
HEARTBEAT_GAP_THRESHOLD_MINUTES = safe_int_conversion(os.getenv('HEARTBEAT_GAP_THRESHOLD', '60'), 60)
ANOMALY_DETECTION_ENABLED = os.getenv('ANOMALY_DETECTION', 'true').lower() == 'true'
ANOMALY_DETECTION_SENSITIVITY = float(os.getenv('ANOMALY_SENSITIVITY', '2.0'))

TRACK_USER_EFFICIENCY = os.getenv('TRACK_EFFICIENCY', 'true').lower() == 'true'
TRACK_CONSISTENCY_SCORES = os.getenv('TRACK_CONSISTENCY', 'true').lower() == 'true'
ENABLE_PERFORMANCE_COMPARISONS = os.getenv('ENABLE_COMPARISONS', 'true').lower() == 'true'

MIN_RUN_DURATION_MINUTES = safe_int_conversion(os.getenv('MIN_RUN_DURATION', '30'), 30)
MAX_RUN_GAP_MINUTES = safe_int_conversion(os.getenv('MAX_RUN_GAP', '60'), 60)

# =============================================================================
# EXPIRATION MANAGEMENT - Using safe conversion
# =============================================================================

EXPIRATION_CHECK_INTERVAL_MINUTES = safe_int_conversion(os.getenv('EXPIRATION_CHECK_INTERVAL', '5'), 5)
EXPIRATION_WARNING_THRESHOLD_HOURS = safe_int_conversion(os.getenv('EXPIRATION_WARNING_HOURS', '6'), 6)
AUTO_ARCHIVE_EXPIRED_THREADS = os.getenv('AUTO_ARCHIVE_EXPIRED', 'true').lower() == 'true'

SEND_EXPIRATION_WARNINGS = os.getenv('SEND_EXPIRATION_WARNINGS', 'true').lower() == 'true'
WARNING_HOURS_BEFORE_EXPIRATION = [24, 6, 1]
ENABLE_EXPIRATION_NOTIFICATIONS = os.getenv('ENABLE_EXPIRATION_NOTIFS', 'true').lower() == 'true'

ARCHIVE_DEAD_THREADS = os.getenv('ARCHIVE_DEAD_THREADS', 'true').lower() == 'true'
ARCHIVE_EXPIRED_THREADS = os.getenv('ARCHIVE_EXPIRED_THREADS', 'true').lower() == 'true'
ARCHIVE_INVALID_THREADS = os.getenv('ARCHIVE_INVALID_THREADS', 'false').lower() == 'true'

# =============================================================================
# PLOTTING CONFIGURATION - Using safe conversion
# =============================================================================

PLOT_STYLE = os.getenv('PLOT_STYLE', 'seaborn-v0_8')
PLOT_DPI = safe_int_conversion(os.getenv('PLOT_DPI', '150'), 150)
MAX_PLOT_HISTORY_DAYS = safe_int_conversion(os.getenv('MAX_PLOT_DAYS', '30'), 30)

MAX_CONCURRENT_PLOTS = safe_int_conversion(os.getenv('MAX_CONCURRENT_PLOTS', '3'), 3)
PLOT_CACHE_DURATION_MINUTES = safe_int_conversion(os.getenv('PLOT_CACHE_DURATION', '10'), 10)
AUTO_GENERATE_DAILY_PLOTS = os.getenv('AUTO_GENERATE_PLOTS', 'true').lower() == 'true'

PLOT_COLOR_SCHEME = os.getenv('PLOT_COLOR_SCHEME', 'husl')
PLOT_FIGURE_SIZE = (15, 10)
ENABLE_INTERACTIVE_PLOTS = os.getenv('ENABLE_INTERACTIVE_PLOTS', 'false').lower() == 'true'

PLOT_OUTPUT_PATH = str(DATA_DIR / 'plots')
Path(PLOT_OUTPUT_PATH).mkdir(exist_ok=True)

ENABLE_PLOTTING_FALLBACK = os.getenv('ENABLE_PLOTTING_FALLBACK', 'true').lower() == 'true'
TEXT_CHART_WIDTH = safe_int_conversion(os.getenv('TEXT_CHART_WIDTH', '40'), 40)
TEXT_CHART_HEIGHT = safe_int_conversion(os.getenv('TEXT_CHART_HEIGHT', '10'), 10)

# =============================================================================
# THREAD MANAGEMENT
# =============================================================================

create_threads_for_god_packs = os.getenv('CREATE_GP_THREADS', 'true').lower() == 'true'
create_threads_for_tradeable_cards = os.getenv('CREATE_TRADEABLE_THREADS', 'true').lower() == 'true'
create_threads_for_double_stars = os.getenv('CREATE_2STAR_THREADS', 'true').lower() == 'true'

AUTO_ADD_PROBABILITY_TO_THREADS = os.getenv('AUTO_ADD_PROB_TO_THREADS', 'true').lower() == 'true'
AUTO_ADD_EXPIRATION_TO_THREADS = os.getenv('AUTO_ADD_EXPIRATION_TO_THREADS', 'true').lower() == 'true'
ENABLE_THREAD_ANALYTICS = os.getenv('ENABLE_THREAD_ANALYTICS', 'true').lower() == 'true'

log_pack_finds_to_channel = os.getenv('LOG_PACK_FINDS', 'true').lower() == 'true'
pack_finds_log_channel_id = safe_int_conversion(os.getenv('PACK_FINDS_CHANNEL', '0'))

AUTO_ARCHIVE_AFTER_EXPIRATION = os.getenv('AUTO_ARCHIVE_AFTER_EXP', 'true').lower() == 'true'
AUTO_UPDATE_THREAD_TITLES = os.getenv('AUTO_UPDATE_THREAD_TITLES', 'true').lower() == 'true'
THREAD_UPDATE_INTERVAL_MINUTES = safe_int_conversion(os.getenv('THREAD_UPDATE_INTERVAL', '30'), 30)

# =============================================================================
# ANTI-CHEAT AND RULES - Using safe conversion
# =============================================================================

anti_cheat = os.getenv('ENABLE_ANTICHEAT', 'true').lower() == 'true'
ADVANCED_ANOMALY_DETECTION = os.getenv('ADVANCED_ANOMALY_DETECTION', 'true').lower() == 'true'
ANOMALY_ALERT_THRESHOLD = float(os.getenv('ANOMALY_ALERT_THRESHOLD', '3.0'))
LOG_ANOMALIES_TO_DATABASE = os.getenv('LOG_ANOMALIES_TO_DB', 'true').lower() == 'true'
anti_cheat_rate = safe_int_conversion(os.getenv('ANTICHEAT_RATE', '3'), 3)

can_people_add_others = os.getenv('CAN_ADD_OTHERS', 'true').lower() == 'true'
can_people_remove_others = os.getenv('CAN_REMOVE_OTHERS', 'true').lower() == 'true'

ENABLE_ADVANCED_PERMISSIONS = os.getenv('ENABLE_ADV_PERMISSIONS', 'true').lower() == 'true'
ADMIN_ONLY_COMMANDS = ["force_expire", "extend_expiration", "system_status"]
MODERATOR_COMMANDS = ["probability", "analytics", "plotting"]

# =============================================================================
# AUTO KICK SETTINGS - Using safe conversion
# =============================================================================

auto_kick = os.getenv('ENABLE_AUTO_KICK', 'true').lower() == 'true'
refresh_interval = safe_int_conversion(os.getenv('REFRESH_INTERVAL', '30'), 30)
inactive_time = safe_int_conversion(os.getenv('INACTIVE_TIME', '61'), 61)
inactive_instance_count = safe_int_conversion(os.getenv('INACTIVE_INSTANCE_COUNT', '1'), 1)
inactive_pack_per_min_count = safe_int_conversion(os.getenv('INACTIVE_PPM_COUNT', '1'), 1)
inactive_if_main_offline = os.getenv('INACTIVE_IF_MAIN_OFFLINE', 'true').lower() == 'true'

USE_ANALYTICS_FOR_KICK_DECISIONS = os.getenv('USE_ANALYTICS_FOR_KICK', 'true').lower() == 'true'
KICK_GRACE_PERIOD_HOURS = safe_int_conversion(os.getenv('KICK_GRACE_PERIOD', '24'), 24)
ENABLE_KICK_APPEALS = os.getenv('ENABLE_KICK_APPEALS', 'true').lower() == 'true'
AUTO_KICK_ANOMALY_THRESHOLD = float(os.getenv('AUTO_KICK_ANOMALY_THRESHOLD', '5.0'))

# =============================================================================
# LEECHING SETTINGS - Using safe conversion
# =============================================================================

can_people_leech = os.getenv('CAN_PEOPLE_LEECH', 'true').lower() == 'true'
leech_perm_gp_count = safe_int_conversion(os.getenv('LEECH_PERM_GP_COUNT', '20'), 20)
leech_perm_pack_count = safe_int_conversion(os.getenv('LEECH_PERM_PACK_COUNT', '50000'), 50000)

TRACK_LEECH_PERFORMANCE = os.getenv('TRACK_LEECH_PERFORMANCE', 'true').lower() == 'true'
LEECH_EFFICIENCY_THRESHOLD = float(os.getenv('LEECH_EFFICIENCY_THRESHOLD', '0.5'))
AUTO_DEMOTE_INACTIVE_LEECHERS = os.getenv('AUTO_DEMOTE_LEECHERS', 'true').lower() == 'true'

# =============================================================================
# GP STATS AND TRACKING - Using safe conversion
# =============================================================================

reset_server_data_frequently = os.getenv('RESET_SERVER_DATA', 'true').lower() == 'true'
reset_server_data_time = safe_int_conversion(os.getenv('RESET_SERVER_TIME', '240'), 240)
output_user_data_on_git_gist = os.getenv('OUTPUT_TO_GIST', 'true').lower() == 'true'

ENABLE_ADVANCED_GP_ANALYTICS = os.getenv('ENABLE_ADV_GP_ANALYTICS', 'true').lower() == 'true'
GP_SUCCESS_RATE_TRACKING = os.getenv('GP_SUCCESS_TRACKING', 'true').lower() == 'true'
GENERATE_GP_REPORTS = os.getenv('GENERATE_GP_REPORTS', 'true').lower() == 'true'
GP_REPORT_INTERVAL_HOURS = safe_int_conversion(os.getenv('GP_REPORT_INTERVAL', '24'), 24)

gp_tracking_update_interval = safe_int_conversion(os.getenv('GP_TRACKING_INTERVAL', '30'), 30)
gp_tracking_use_cron_schedule = os.getenv('GP_TRACKING_USE_CRON', 'true').lower() == 'true'

include_tradeable_cards_in_tracking = os.getenv('TRACK_TRADEABLE', 'false').lower() == 'true'
include_double_stars_in_tracking = os.getenv('TRACK_2STARS', 'true').lower() == 'true'
include_god_packs_in_tracking = os.getenv('TRACK_GODPACKS', 'true').lower() == 'true'

tradeable_card_tracking_label = os.getenv('TRADEABLE_LABEL', 'Special Cards')
double_star_tracking_label = os.getenv('2STAR_LABEL', 'Double Stars')
god_pack_tracking_label = os.getenv('GP_LABEL', 'God Packs')

notifications_enabled = os.getenv('NOTIFICATIONS_ENABLED', 'true').lower() == 'true'

INCLUDE_PROBABILITY_IN_TRACKING = os.getenv('INCLUDE_PROB_IN_TRACKING', 'true').lower() == 'true'
INCLUDE_EXPIRATION_IN_TRACKING = os.getenv('INCLUDE_EXP_IN_TRACKING', 'true').lower() == 'true'
AUTO_SORT_BY_PROBABILITY = os.getenv('AUTO_SORT_BY_PROB', 'true').lower() == 'true'
TRACKING_LIST_MAX_ENTRIES = safe_int_conversion(os.getenv('TRACKING_MAX_ENTRIES', '50'), 50)

# =============================================================================
# FEATURE FLAGS
# =============================================================================

FEATURES = {
    'advanced_probability': os.getenv('FEAT_ADV_PROBABILITY', 'true').lower() == 'true',
    'heartbeat_analytics': os.getenv('FEAT_HB_ANALYTICS', 'true').lower() == 'true',
    'plotting_system': os.getenv('FEAT_PLOTTING', 'true').lower() == 'true',
    'expiration_tracking': os.getenv('FEAT_EXPIRATION', 'true').lower() == 'true',
    'google_sheets_integration': ENABLE_SHEETS_INTEGRATION,
    'anomaly_detection': os.getenv('FEAT_ANOMALY', 'true').lower() == 'true',
    'auto_archiving': os.getenv('FEAT_AUTO_ARCHIVE', 'true').lower() == 'true',
    'performance_monitoring': os.getenv('FEAT_PERF_MONITOR', 'true').lower() == 'true',
    'advanced_statistics': os.getenv('FEAT_ADV_STATS', 'true').lower() == 'true',
    'user_comparisons': os.getenv('FEAT_USER_COMPARE', 'true').lower() == 'true',
    'automated_reporting': os.getenv('FEAT_AUTO_REPORT', 'false').lower() == 'true',
    'predictive_analytics': os.getenv('FEAT_PREDICTIVE', 'false').lower() == 'true',
    'machine_learning_insights': os.getenv('FEAT_ML', 'false').lower() == 'true',
}

# =============================================================================
# ELIGIBLE IDS AND FILTERING - Using safe conversion
# =============================================================================

safe_eligible_ids_filtering = os.getenv('SAFE_ELIGIBLE_FILTERING', 'true').lower() == 'true'
add_double_star_to_vip_ids_txt = os.getenv('ADD_2STAR_TO_VIP', 'true').lower() == 'true'

USE_ANALYTICS_FOR_FILTERING = os.getenv('USE_ANALYTICS_FILTERING', 'true').lower() == 'true'
FILTER_BY_PROBABILITY = os.getenv('FILTER_BY_PROBABILITY', 'true').lower() == 'true'
MINIMUM_PROBABILITY_THRESHOLD = float(os.getenv('MIN_PROB_THRESHOLD', '20.0'))

force_skip_min_2stars = safe_int_conversion(os.getenv('FORCE_SKIP_MIN_2STARS', '2'), 2)
force_skip_min_packs = safe_int_conversion(os.getenv('FORCE_SKIP_MIN_PACKS', '2'), 2)

USE_PROBABILITY_FOR_SKIP = os.getenv('USE_PROB_FOR_SKIP', 'true').lower() == 'true'
SKIP_LOW_PROBABILITY_THRESHOLD = float(os.getenv('SKIP_LOW_PROB_THRESHOLD', '10.0'))
SKIP_BASED_ON_USER_HISTORY = os.getenv('SKIP_USER_HISTORY', 'true').lower() == 'true'

# =============================================================================
# TIME SETTINGS - Using safe conversion
# =============================================================================

auto_close_live_post_time = safe_int_conversion(os.getenv('AUTO_CLOSE_LIVE_TIME', '96'), 96)
auto_close_not_live_post_time = safe_int_conversion(os.getenv('AUTO_CLOSE_NOTLIVE_TIME', '36'), 36)
backup_user_datas_time = safe_int_conversion(os.getenv('BACKUP_USER_TIME', '30'), 30)
delay_msg_delete_state = safe_int_conversion(os.getenv('MSG_DELETE_DELAY', '10'), 10)

DYNAMIC_CLOSE_TIMES = os.getenv('DYNAMIC_CLOSE_TIMES', 'true').lower() == 'true'
HIGH_PROBABILITY_EXTEND_HOURS = safe_int_conversion(os.getenv('HIGH_PROB_EXTEND_HOURS', '24'), 24)
LOW_PROBABILITY_REDUCE_HOURS = safe_int_conversion(os.getenv('LOW_PROB_REDUCE_HOURS', '12'), 12)

# =============================================================================
# DISPLAY SETTINGS
# =============================================================================

english_language = os.getenv('ENGLISH_LANGUAGE', 'true').lower() == 'true'
show_per_person_live = os.getenv('SHOW_PER_PERSON_LIVE', 'true').lower() == 'true'

SHOW_PROBABILITY_IN_STATS = os.getenv('SHOW_PROB_IN_STATS', 'true').lower() == 'true'
SHOW_EFFICIENCY_SCORES = os.getenv('SHOW_EFFICIENCY', 'true').lower() == 'true'
SHOW_ANALYTICS_SUMMARY = os.getenv('SHOW_ANALYTICS', 'true').lower() == 'true'
USE_ENHANCED_FORMATTING = os.getenv('USE_ENH_FORMATTING', 'true').lower() == 'true'

ENABLE_EMBEDDED_CHARTS = os.getenv('ENABLE_EMB_CHARTS', 'false').lower() == 'true'
CHART_UPDATE_FREQUENCY = os.getenv('CHART_UPDATE_FREQ', 'daily')
DEFAULT_CHART_THEME = os.getenv('DEFAULT_CHART_THEME', 'dark')

# =============================================================================
# OTHER SETTINGS - Using safe conversion where applicable
# =============================================================================

miss_before_dead = [4, 6, 8, 10, 12]
miss_not_liked_multiplier = [0.5, 0.5, 0.5, 0.75, 0.85, 1]

USE_PROBABILITY_FOR_MISS_CALC = os.getenv('USE_PROB_FOR_MISS', 'true').lower() == 'true'
DYNAMIC_MISS_THRESHOLDS = os.getenv('DYNAMIC_MISS_THRESH', 'true').lower() == 'true'
CONFIDENCE_MULTIPLIER = float(os.getenv('CONFIDENCE_MULT', '0.8'))

min_2stars = safe_int_conversion(os.getenv('MIN_2STARS', '0'), 0)
group_packs_type = safe_int_conversion(os.getenv('GROUP_PACKS_TYPE', '5'), 5)

TRACK_GROUP_PERFORMANCE = os.getenv('TRACK_GROUP_PERF', 'true').lower() == 'true'
COMPARE_TO_HISTORICAL_DATA = os.getenv('COMPARE_HISTORICAL', 'true').lower() == 'true'
GENERATE_GROUP_INSIGHTS = os.getenv('GEN_GROUP_INSIGHTS', 'true').lower() == 'true'

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_TO_FILE = os.getenv('LOG_TO_FILE', 'true').lower() == 'true'
LOG_FILE_PATH = str(DATA_DIR / 'logs' / 'ptcgp_bot.log')
MAX_LOG_FILE_SIZE_MB = safe_int_conversion(os.getenv('MAX_LOG_SIZE_MB', '50'), 50)
LOG_BACKUP_COUNT = safe_int_conversion(os.getenv('LOG_BACKUP_COUNT', '5'), 5)

LOG_ANALYTICS_EVENTS = os.getenv('LOG_ANALYTICS', 'true').lower() == 'true'
LOG_PROBABILITY_CALCULATIONS = os.getenv('LOG_PROB_CALC', 'true').lower() == 'true'
LOG_PERFORMANCE_METRICS = os.getenv('LOG_PERF_METRICS', 'true').lower() == 'true'
ENABLE_STRUCTURED_LOGGING = os.getenv('ENABLE_STRUCT_LOGGING', 'true').lower() == 'true'

LOG_DIR = DATA_DIR / 'logs'
LOG_DIR.mkdir(exist_ok=True)

# =============================================================================
# AESTHETICS AND EMOJIS
# =============================================================================

text_verified_logo = os.getenv('EMOJI_VERIFIED', '✅')
text_liked_logo = os.getenv('EMOJI_LIKED', '🔥')
text_waiting_logo = os.getenv('EMOJI_WAITING', '⌛')
text_not_liked_logo = os.getenv('EMOJI_NOT_LIKED', '🥶')
text_dead_logo = os.getenv('EMOJI_DEAD', '💀')

text_high_probability_logo = os.getenv('EMOJI_HIGH_PROB', '🟢')
text_medium_probability_logo = os.getenv('EMOJI_MED_PROB', '🟡')
text_low_probability_logo = os.getenv('EMOJI_LOW_PROB', '🔴')
text_expired_logo = os.getenv('EMOJI_EXPIRED', '📅')
text_analytics_logo = os.getenv('EMOJI_ANALYTICS', '📊')

leaderboard_best_farm1_custom_emoji_name = os.getenv('EMOJI_BEST_FARM1', 'Chadge')
leaderboard_best_farm2_custom_emoji_name = os.getenv('EMOJI_BEST_FARM2', 'PeepoLove')
leaderboard_best_farm3_custom_emoji_name = os.getenv('EMOJI_BEST_FARM3', 'PeepoShy')
leaderboard_best_farm_length = safe_int_conversion(os.getenv('LEADERBOARD_FARM_LENGTH', '6'), 6)

leaderboard_best_verifier1_custom_emoji_name = os.getenv('EMOJI_BEST_VER1', 'Wicked')
leaderboard_best_verifier2_custom_emoji_name = os.getenv('EMOJI_BEST_VER2', 'PeepoSunglasses')
leaderboard_best_verifier3_custom_emoji_name = os.getenv('EMOJI_BEST_VER3', 'PeepoHappy')

leaderboard_worst_verifier1_custom_emoji_name = os.getenv('EMOJI_WORST_VER1', 'Bedge')
leaderboard_worst_verifier2_custom_emoji_name = os.getenv('EMOJI_WORST_VER2', 'PeepoClown')
leaderboard_worst_verifier3_custom_emoji_name = os.getenv('EMOJI_WORST_VER3', 'DinkDonk')

ga_mewtwo_custom_emoji_name = os.getenv('EMOJI_MEWTWO', 'mewtwo')
ga_charizard_custom_emoji_name = os.getenv('EMOJI_CHARIZARD', 'charizard')
ga_pikachu_custom_emoji_name = os.getenv('EMOJI_PIKACHU', 'pikachu')
mi_mew_custom_emoji_name = os.getenv('EMOJI_MEW', 'mew')
sts_dialga_custom_emoji_name = os.getenv('EMOJI_DIALGA', 'dialga')
sts_palkia_custom_emoji_name = os.getenv('EMOJI_PALKIA', 'palkia')
tl_arceus_custom_emoji_name = os.getenv('EMOJI_ARCEUS', 'arceus')
sr_giratina_custom_emoji_name = os.getenv('EMOJI_GIRATINA', 'lucario_shiny')
sm_solgaleo_custom_emoji_name = os.getenv('EMOJI_SOLGALEO', 'solgaleo')
sm_lunala_custom_emoji_name = os.getenv('EMOJI_LUNALA', 'lunala')
sv_buzzwole_custom_emoji_name = os.getenv('EMOJI_BUZZWOLE', 'buzzwole')

# =============================================================================
# PATH CONFIGURATIONS
# =============================================================================

path_users_data = str(DATA_DIR / 'UserData.xml')
path_server_data = str(DATA_DIR / 'ServerData.xml')

ANALYTICS_EXPORT_PATH = str(DATA_DIR / 'exports' / 'analytics')

required_dirs = [
    DATA_DIR,
    DATA_DIR / 'backups',
    DATA_DIR / 'backups' / 'database',
    DATA_DIR / 'plots',
    DATA_DIR / 'exports',
    DATA_DIR / 'exports' / 'analytics',
    DATA_DIR / 'logs'
]

for directory in required_dirs:
    directory.mkdir(exist_ok=True, parents=True)

# =============================================================================
# XML ATTRIBUTE CONFIGURATIONS (LEGACY)
# =============================================================================

attrib_pocket_id = "PocketID"
attrib_prefix = "Prefix"
attrib_user_state = "UserState"
attrib_active_state = "ActiveState"
attrib_average_instances = "AverageInstances"
attrib_hb_instances = "HBInstances"
attrib_real_instances = "RealInstances"
attrib_session_time = "SessionTime"
attrib_total_packs_opened = "TotalPacksOpened"
attrib_total_packs_farm = "TotalPacksFarm"
attrib_total_average_instances = "TotalAverageInstances"
attrib_total_average_ppm = "TotalAveragePPM"
attrib_total_hb_tick = "TotalHBTick"
attrib_session_packs_opened = "SessionPacksOpened"
attrib_diff_packs_since_last_hb = "DiffPacksSinceLastHB"
attrib_diff_time_since_last_hb = "DiffTimeSinceLastHB"
attrib_packs_per_min = "PacksPerMin"
attrib_god_pack_found = "GodPackFound"
attrib_god_pack_live = "GodPackLive"
attrib_last_active_time = "LastActiveTime"
attrib_last_heartbeat_time = "LastHeartbeatTime"
attrib_total_time = "TotalTime"
attrib_total_time_farm = "TotalTimeFarm"
attrib_total_miss = "TotalMiss"
attrib_anticheat_user_count = "AntiCheatUserCount"
attrib_subsystems = "Subsystems"
attrib_subsystem = "Subsystem"
attrib_eligible_gps = "eligibleGPs"
attrib_eligible_gp = "eligibleGP"
attrib_live_gps = "liveGPs"
attrib_live_gp = "liveGP"
attrib_ineligible_gps = "ineligibleGPs"
attrib_ineligible_gp = "ineligibleGP"
attrib_selected_pack = "SelectedPack"
attrib_rolling_type = "RollingType"
attrib_display_name = "DisplayName"

# =============================================================================
# AUTO MIGRATION SETTINGS
# =============================================================================

AUTO_MIGRATE_XML_DATA = os.getenv('AUTO_MIGRATE_XML', 'false').lower() == 'true'
PRESERVE_XML_FILES = os.getenv('PRESERVE_XML', 'true').lower() == 'true'

# =============================================================================
# ENHANCED CONFIGURATION VALIDATION
# =============================================================================

def validate_config():
    """Enhanced configuration validation with permission checks"""
    errors = []
    warnings = []
    
    if not token:
        errors.append("Discord token is required - set DISCORD_TOKEN environment variable")
    if not guild_id or guild_id == 0:
        errors.append("Guild ID is required - set DISCORD_GUILD_ID environment variable")
    
    required_channels = {
        'channel_id_commands': 'Commands channel (CHANNEL_ID_COMMANDS)',
        'channel_id_heartbeat': 'Heartbeat channel (CHANNEL_ID_HEARTBEAT)', 
        'channel_id_webhook': 'Webhook channel (CHANNEL_ID_WEBHOOK)',
        'channel_id_user_stats': 'User stats channel (CHANNEL_ID_USER_STATS)'
    }
    
    for channel_var, description in required_channels.items():
        value = globals().get(channel_var)
        if not value or value == 0:
            errors.append(f"{description} is required - configure {channel_var}")
    
    if create_threads_for_god_packs:
        warnings.append("Thread creation enabled - ensure bot has 'manage_threads' and 'create_public_threads' permissions")
    
    if auto_kick:
        warnings.append("Auto-kick enabled - ensure bot has 'kick_members' permission")
    
    try:
        if not DATABASE_PATH.parent.exists():
            warnings.append(f"Database directory {DATABASE_PATH.parent} does not exist (will be created)")
        
        import shutil
        free_space_gb = shutil.disk_usage(DATABASE_PATH.parent).free / (1024**3)
        if free_space_gb < 1:
            warnings.append(f"Low disk space: {free_space_gb:.1f}GB available")
            
    except Exception as e:
        warnings.append(f"Database path validation failed: {e}")
    
    if FEATURES.get('google_sheets_integration'):
        if not os.path.exists(GOOGLE_CREDENTIALS_FILE):
            errors.append(f"Google Sheets integration enabled but credentials file '{GOOGLE_CREDENTIALS_FILE}' not found")
        else:
            try:
                with open(GOOGLE_CREDENTIALS_FILE, 'r') as f:
                    creds = json.load(f)
                    if 'type' not in creds or creds['type'] != 'service_account':
                        warnings.append("Google credentials file may not be a service account")
            except Exception as e:
                warnings.append(f"Error validating Google credentials: {e}")
    
    if FEATURES.get('plotting_system'):
        try:
            import matplotlib
            import numpy
            import seaborn
        except ImportError as e:
            warnings.append(f"Plotting system enabled but required packages missing: {e}")
    
    if MAX_MEMORY_USAGE_MB < 256:
        warnings.append("MAX_MEMORY_USAGE_MB is very low (< 256MB), may cause performance issues")
    
    if PLOT_DPI > 300:
        warnings.append("PLOT_DPI is very high (> 300), may cause memory issues")
    
    if MAX_PROBABILITY_CALCULATIONS_PER_MINUTE > 100:
        warnings.append("Very high probability calculation limit may impact performance")
    
    return {
        'errors': errors,
        'warnings': warnings,
        'valid': len(errors) == 0,
        'total_issues': len(errors) + len(warnings),
        'permission_validation_available': True
    }

def get_configuration_summary():
    """Get a summary of current configuration"""
    return {
        'database_path': str(DATABASE_PATH),
        'features_enabled': sum(FEATURES.values()),
        'total_features': len(FEATURES),
        'required_channels_configured': sum(1 for ch in [
            channel_id_commands, channel_id_user_stats, 
            channel_id_heartbeat, channel_id_webhook
        ] if ch and ch != 0),
        'guild_configured': bool(guild_id and guild_id != 0),
        'token_configured': bool(token)
    }

def create_example_env_file():
    """Create an example .env file with all configuration options"""
    env_example = """# Discord Bot Configuration
DISCORD_TOKEN=your_bot_token_here
DISCORD_GUILD_ID=your_guild_id_here

# Channel IDs (required) - Replace with actual channel IDs
CHANNEL_ID_COMMANDS=123456789012345678
CHANNEL_ID_USER_STATS=123456789012345678
CHANNEL_ID_HEARTBEAT=123456789012345678
CHANNEL_ID_WEBHOOK=123456789012345678

# Optional Channel IDs
CHANNEL_ID_ANTICHEAT=
CHANNEL_ID_GP_TRACKING=
CHANNEL_ID_NOTIFICATIONS=

# API Keys (optional)
GITHUB_TOKEN=
GITHUB_GIST_ID=
PASTEBIN_API_KEY=

# Google Sheets (optional)
GOOGLE_CREDENTIALS_FILE=credentials.json
ENABLE_SHEETS=false

# Bot Settings
BOT_PREFIX=!
ADMIN_ROLE_IDS=123456789012345678,987654321098765432

# Performance Settings
DB_POOL_SIZE=5
MAX_MEMORY_MB=1024
COMMAND_COOLDOWN=2

# Enhanced Rate Limiting
MAX_GLOBAL_REQUESTS_PER_MINUTE=500
MAX_USER_COMMANDS_PER_5MIN=100
MAX_HEAVY_COMMANDS_PER_HOUR=20
MAX_ADMIN_COMMANDS_PER_HOUR=50

# Backup Settings
MAX_BACKUP_COUNT=50
COMPRESS_BACKUPS=true
VERIFY_BACKUPS=true

# Feature Flags
FEAT_ADV_PROBABILITY=true
FEAT_PLOTTING=true
FEAT_EXPIRATION=true

# Plotting Fallback
ENABLE_PLOTTING_FALLBACK=true
TEXT_CHART_WIDTH=40
TEXT_CHART_HEIGHT=10
"""
    
    env_example_path = DATA_DIR.parent / '.env.example'
    with open(env_example_path, 'w') as f:
        f.write(env_example)
    
    logger.info(f"Created example environment file at {env_example_path}")

# =============================================================================
# STARTUP VALIDATION
# =============================================================================

_validation_result = validate_config()

if not _validation_result['valid']:
    logger.critical("🚨 CRITICAL CONFIGURATION ERRORS FOUND:")
    for error in _validation_result['errors']:
        logger.critical(f"  ❌ {error}")
    logger.critical("\n⚠️  Bot will not start until these errors are fixed!")
    
    if not (DATA_DIR.parent / '.env').exists() and not (DATA_DIR.parent / '.env.example').exists():
        create_example_env_file()
        logger.info("📝 Created .env.example file - copy to .env and fill in your values")

if _validation_result['warnings']:
    logger.warning("⚠️  Configuration warnings:")
    for warning in _validation_result['warnings']:
        logger.warning(f"  ⚠️  {warning}")

if _validation_result['valid']:
    logger.info("✅ Configuration validation passed")
    summary = get_configuration_summary()
    logger.info(f"📊 Configuration summary: {summary['features_enabled']}/{summary['total_features']} features enabled, "
                f"{summary['required_channels_configured']}/4 required channels configured")

# =============================================================================
# VALID PACK NAMES CONFIGURATION
# =============================================================================

VALID_PACK_NAMES = [
    'Charizard ex', 'Pikachu ex', 'Mewtwo ex'
]

logger.info("✅ Enhanced secure config.py loaded successfully")