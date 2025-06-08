import os
from typing import Dict, List

# =============================================================================
# DISCORD CONFIGURATION - CRITICAL: MUST BE CONFIGURED
# =============================================================================

# Your app Token, if you don't know it you can reset it here: https://discord.com/developers/applications > Your App > Bot > Reset Token
token = ""  # REQUIRED: Add your Discord bot token here

# With Discord developer mode on, right-click your server and "Copy Server ID"
guild_id = ""  # REQUIRED: Add your Discord server ID here

# =============================================================================
# UNIFIED DATABASE CONFIGURATION - CRITICAL FIX
# =============================================================================

# FIXED: Unified database path - all systems will use this single database
DATABASE_PATH = os.path.join('data', 'ptcgp_unified.db')

# Aliases for backward compatibility
database_path = DATABASE_PATH
db_path = DATABASE_PATH

# =============================================================================
# CRITICAL VARIABLES (REQUIRED FOR BOT TO FUNCTION)
# =============================================================================

# Command prefix (referenced by main.py)
command_prefix = "!"

# Token aliases for compatibility
discord_token = token

# Admin role IDs for permission checking
admin_role_ids = []  # Add your admin role IDs here: [123456789, 987654321]

# User cleanup settings
user_cleanup_days = 30

# Auto backup settings
enable_auto_backup = True

# Upload settings
upload_gist = True
backup_to_pastebin = False
upload_logs = False

# GitHub aliases
github_token = ""  # Set to your GitHub token
gist_id = ""  # Set to your gist ID

# Stats interval for background tasks
stats_interval_minutes = 30

# =============================================================================
# CHANNEL IDs - CRITICAL: MUST BE CONFIGURED FOR BOT TO WORK
# =============================================================================
# IMPORTANT: You MUST configure these channel IDs for the bot to work properly
# For all channel_id below, right-click a channel in your Discord server and "Copy Channel ID" with developer mode on

# REQUIRED CHANNELS - Bot will not function without these
channel_id_commands = ""  # REQUIRED: Commands channel ID (where bot commands are used)
channel_id_user_stats = ""  # REQUIRED: Stats channel ID (where statistics are posted)
channel_id_heartbeat = ""  # REQUIRED: Heartbeat channel ID (where heartbeat webhooks are sent)
channel_id_webhook = ""  # REQUIRED: Webhook channel ID (where pack webhooks are sent)

# Stats channel alias for backward compatibility
stats_channel_id = channel_id_user_stats

# Pack specific forum channels - Each one must be a forum channel (OPTIONAL)
channel_id_mewtwo_verification_forum = ""
channel_id_charizard_verification_forum = ""
channel_id_pikachu_verification_forum = ""
channel_id_mew_verification_forum = ""
channel_id_dialga_verification_forum = ""
channel_id_palkia_verification_forum = ""
channel_id_arceus_verification_forum = ""
channel_id_shining_verification_forum = ""
channel_id_solgaleo_verification_forum = ""
channel_id_lunala_verification_forum = ""
channel_id_buzzwole_verification_forum = ""

# THE ID OF THE DISCORD CHANNEL - Where Double 2 Star validation threads will be created ⚠️ IT MUST BE A FORUM CHANNEL
channel_id_2star_verification_forum = ""

# Additional channels (OPTIONAL)
channel_id_anticheat = ""  # Where the AntiCheat pseudonyms are sent for analysis
channel_id_gp_tracking_list = ""  # Where the GP tracking list will be posted
channel_id_notifications = ""  # Where notifications for new GP/tradeable cards will be sent

# =============================================================================
# NEW: ENHANCED ANALYTICS CHANNELS (OPTIONAL)
# =============================================================================
channel_id_analytics_reports = ""  # Leave empty to use channel_id_user_stats
channel_id_expiration_warnings = ""  # Leave empty to use channel_id_notifications
channel_id_probability_logs = ""  # Leave empty to use channel_id_commands
channel_id_system_status = ""  # Leave empty to use channel_id_commands

# =============================================================================
# PACK FILTERS FOR ROLE-BASED FILTERING
# =============================================================================
enable_role_based_filters = True

pack_filters = {
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

# =============================================================================
# ENHANCED DATABASE CONFIGURATION
# =============================================================================
BACKUP_INTERVAL_HOURS = 24
DATA_RETENTION_DAYS = 30

# Auto-migration from XML to SQLite
AUTO_MIGRATE_XML_DATA = True
PRESERVE_XML_FILES = True

# Database performance settings
DB_CONNECTION_POOL_SIZE = 5
DB_QUERY_TIMEOUT_SECONDS = 30
ENABLE_DATABASE_WAL_MODE = True

# =============================================================================
# GOOGLE SHEETS INTEGRATION (OPTIONAL)
# =============================================================================
GOOGLE_CREDENTIALS_FILE = "credentials.json"
ENABLE_SHEETS_INTEGRATION = True
AUTO_SYNC_INTERVAL_MINUTES = 30

SHEETS_SYNC_ON_STARTUP = True
SHEETS_SYNC_ON_GP_FOUND = True
SHEETS_SYNC_ON_USER_UPDATE = True
SHEETS_DAILY_BACKUP = True

SPREADSHEET_NAME = f"PTCGP Reroll Data - Guild {guild_id}"

# =============================================================================
# ADVANCED PROBABILITY SYSTEM
# =============================================================================
PROBABILITY_CACHE_TIMEOUT_MINUTES = 5
PROBABILITY_CONFIDENCE_THRESHOLD = 30
ENABLE_BAYESIAN_UPDATES = True

DEFAULT_MIN_FRIENDS_ASSUMPTION = 6
PROBABILITY_RECALC_ON_TEST = True
ENABLE_MEMBER_PROBABILITY_BREAKDOWN = True

MISS_TEST_WEIGHT = 1.0
NOSHOW_TEST_WEIGHT = 0.7

# =============================================================================
# EXPIRATION MANAGEMENT
# =============================================================================
EXPIRATION_CHECK_INTERVAL_MINUTES = 5
EXPIRATION_WARNING_THRESHOLD_HOURS = 6
AUTO_ARCHIVE_EXPIRED_THREADS = True

SEND_EXPIRATION_WARNINGS = True
WARNING_HOURS_BEFORE_EXPIRATION = [24, 6, 1]
ENABLE_EXPIRATION_NOTIFICATIONS = True

ARCHIVE_DEAD_THREADS = True
ARCHIVE_EXPIRED_THREADS = True
ARCHIVE_INVALID_THREADS = False

# =============================================================================
# HEARTBEAT ANALYTICS
# =============================================================================
HEARTBEAT_GAP_THRESHOLD_MINUTES = 60
ANOMALY_DETECTION_ENABLED = True
ANOMALY_DETECTION_SENSITIVITY = 2.0

TRACK_USER_EFFICIENCY = True
TRACK_CONSISTENCY_SCORES = True
ENABLE_PERFORMANCE_COMPARISONS = True

MIN_RUN_DURATION_MINUTES = 30
MAX_RUN_GAP_MINUTES = 60

# =============================================================================
# PLOTTING SYSTEM
# =============================================================================
PLOT_STYLE = "seaborn-v0_8"
PLOT_DPI = 150
MAX_PLOT_HISTORY_DAYS = 30

MAX_CONCURRENT_PLOTS = 3
PLOT_CACHE_DURATION_MINUTES = 10
AUTO_GENERATE_DAILY_PLOTS = True

PLOT_COLOR_SCHEME = "husl"
PLOT_FIGURE_SIZE = (15, 10)
ENABLE_INTERACTIVE_PLOTS = False

# =============================================================================
# GITHUB/GIST SETTINGS
# =============================================================================
git_token = ""
git_gist_id = ""
git_gist_group_name = ""
git_gist_gp_name = ""

GIST_AUTO_UPDATE_ANALYTICS = True
GIST_BACKUP_INTERVAL_HOURS = 6
ENABLE_GIST_STATISTICS = True

# =============================================================================
# PERFORMANCE LIMITS
# =============================================================================
MAX_CONCURRENT_OPERATIONS = 5
MAX_PROBABILITY_CALCULATIONS_PER_MINUTE = 60
MAX_SHEET_UPDATES_PER_HOUR = 100
MAX_PLOT_GENERATIONS_PER_HOUR = 20

MAX_MEMORY_USAGE_MB = 1024
CLEANUP_INTERVAL_HOURS = 6
ENABLE_MEMORY_MONITORING = True

# =============================================================================
# THREAD CREATION SETTINGS
# =============================================================================
create_threads_for_god_packs = True
create_threads_for_tradeable_cards = True
create_threads_for_double_stars = True

AUTO_ADD_PROBABILITY_TO_THREADS = True
AUTO_ADD_EXPIRATION_TO_THREADS = True
ENABLE_THREAD_ANALYTICS = True

log_pack_finds_to_channel = True
pack_finds_log_channel_id = ""

AUTO_ARCHIVE_AFTER_EXPIRATION = True
AUTO_UPDATE_THREAD_TITLES = True
THREAD_UPDATE_INTERVAL_MINUTES = 30

# =============================================================================
# RULES AND ANTI-CHEAT
# =============================================================================
anti_cheat = True
ADVANCED_ANOMALY_DETECTION = True
ANOMALY_ALERT_THRESHOLD = 3.0
LOG_ANOMALIES_TO_DATABASE = True

can_people_add_others = True
can_people_remove_others = True

ENABLE_ADVANCED_PERMISSIONS = True
ADMIN_ONLY_COMMANDS = ["force_expire", "extend_expiration", "system_status"]
MODERATOR_COMMANDS = ["probability", "analytics", "plotting"]

# =============================================================================
# AUTO KICK SETTINGS
# =============================================================================
auto_kick = True
refresh_interval = 30
inactive_time = 61
inactive_instance_count = 1
inactive_pack_per_min_count = 1
inactive_if_main_offline = True

USE_ANALYTICS_FOR_KICK_DECISIONS = True
KICK_GRACE_PERIOD_HOURS = 24
ENABLE_KICK_APPEALS = True
AUTO_KICK_ANOMALY_THRESHOLD = 5.0

# =============================================================================
# LEECHING SETTINGS
# =============================================================================
can_people_leech = True
leech_perm_gp_count = 20
leech_perm_pack_count = 50000

TRACK_LEECH_PERFORMANCE = True
LEECH_EFFICIENCY_THRESHOLD = 0.5
AUTO_DEMOTE_INACTIVE_LEECHERS = True

# =============================================================================
# GP STATS
# =============================================================================
reset_server_data_frequently = True
reset_server_data_time = 240
output_user_data_on_git_gist = True

ENABLE_ADVANCED_GP_ANALYTICS = True
GP_SUCCESS_RATE_TRACKING = True
GENERATE_GP_REPORTS = True
GP_REPORT_INTERVAL_HOURS = 24

# =============================================================================
# GP TRACKING
# =============================================================================
gp_tracking_update_interval = 30
gp_tracking_use_cron_schedule = True

include_tradeable_cards_in_tracking = False
include_double_stars_in_tracking = True
include_god_packs_in_tracking = True

tradeable_card_tracking_label = "Special Cards"
double_star_tracking_label = "Double Stars"
god_pack_tracking_label = "God Packs"

notifications_enabled = True

INCLUDE_PROBABILITY_IN_TRACKING = True
INCLUDE_EXPIRATION_IN_TRACKING = True
AUTO_SORT_BY_PROBABILITY = True
TRACKING_LIST_MAX_ENTRIES = 50

# =============================================================================
# FEATURE FLAGS
# =============================================================================
FEATURES = {
    'advanced_probability': True,
    'heartbeat_analytics': True,
    'plotting_system': True,
    'expiration_tracking': True,
    'google_sheets_integration': True,
    'anomaly_detection': True,
    'auto_archiving': True,
    'performance_monitoring': True,
    'advanced_statistics': True,
    'user_comparisons': True,
    'automated_reporting': True,
    'predictive_analytics': False,  # Experimental
    'machine_learning_insights': False,  # Experimental
}

# =============================================================================
# ELIGIBLE IDs
# =============================================================================
safe_eligible_ids_filtering = True
add_double_star_to_vip_ids_txt = True

USE_ANALYTICS_FOR_FILTERING = True
FILTER_BY_PROBABILITY = True
MINIMUM_PROBABILITY_THRESHOLD = 20.0

# =============================================================================
# FORCE SKIP
# =============================================================================
force_skip_min_2stars = 2
force_skip_min_packs = 2

USE_PROBABILITY_FOR_SKIP = True
SKIP_LOW_PROBABILITY_THRESHOLD = 10.0
SKIP_BASED_ON_USER_HISTORY = True

# =============================================================================
# TIME SETTINGS
# =============================================================================
auto_close_live_post_time = 96  # hours
auto_close_not_live_post_time = 36  # hours
heartbeat_rate = 30  # minutes
anti_cheat_rate = 3  # minutes
backup_user_datas_time = 30  # minutes
delay_msg_delete_state = 10  # seconds

DYNAMIC_CLOSE_TIMES = True
HIGH_PROBABILITY_EXTEND_HOURS = 24
LOW_PROBABILITY_REDUCE_HOURS = 12

# =============================================================================
# DISPLAY SETTINGS
# =============================================================================
english_language = True
show_per_person_live = True

SHOW_PROBABILITY_IN_STATS = True
SHOW_EFFICIENCY_SCORES = True
SHOW_ANALYTICS_SUMMARY = True
USE_ENHANCED_FORMATTING = True

ENABLE_EMBEDDED_CHARTS = True
CHART_UPDATE_FREQUENCY = "daily"
DEFAULT_CHART_THEME = "dark"

# =============================================================================
# OTHER SETTINGS
# =============================================================================
miss_before_dead = [4, 6, 8, 10, 12]
miss_not_liked_multiplier = [0.5, 0.5, 0.5, 0.75, 0.85, 1]

USE_PROBABILITY_FOR_MISS_CALC = True
DYNAMIC_MISS_THRESHOLDS = True
CONFIDENCE_MULTIPLIER = 0.8

min_2stars = 0
group_packs_type = 5

TRACK_GROUP_PERFORMANCE = True
COMPARE_TO_HISTORICAL_DATA = True
GENERATE_GROUP_INSIGHTS = True

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
LOG_LEVEL = "INFO"
LOG_TO_FILE = True
LOG_FILE_PATH = "logs/ptcgp_bot.log"
MAX_LOG_FILE_SIZE_MB = 50
LOG_BACKUP_COUNT = 5

LOG_ANALYTICS_EVENTS = True
LOG_PROBABILITY_CALCULATIONS = True
LOG_PERFORMANCE_METRICS = True
ENABLE_STRUCTURED_LOGGING = True

# =============================================================================
# AESTHETICS
# =============================================================================
text_verified_logo = "✅"
text_liked_logo = "🔥"
text_waiting_logo = "⌛"
text_not_liked_logo = "🥶"
text_dead_logo = "💀"

text_high_probability_logo = "🟢"
text_medium_probability_logo = "🟡"
text_low_probability_logo = "🔴"
text_expired_logo = "📅"
text_analytics_logo = "📊"

leaderboard_best_farm1_custom_emoji_name = "Chadge"
leaderboard_best_farm2_custom_emoji_name = "PeepoLove"
leaderboard_best_farm3_custom_emoji_name = "PeepoShy"
leaderboard_best_farm_length = 6

leaderboard_best_verifier1_custom_emoji_name = "Wicked"
leaderboard_best_verifier2_custom_emoji_name = "PeepoSunglasses"
leaderboard_best_verifier3_custom_emoji_name = "PeepoHappy"

leaderboard_worst_verifier1_custom_emoji_name = "Bedge"
leaderboard_worst_verifier2_custom_emoji_name = "PeepoClown"
leaderboard_worst_verifier3_custom_emoji_name = "DinkDonk"

ga_mewtwo_custom_emoji_name = "mewtwo"
ga_charizard_custom_emoji_name = "charizard"
ga_pikachu_custom_emoji_name = "pikachu"
mi_mew_custom_emoji_name = "mew"
sts_dialga_custom_emoji_name = "dialga"
sts_palkia_custom_emoji_name = "palkia"
tl_arceus_custom_emoji_name = "arceus"
sr_giratina_custom_emoji_name = "lucario_shiny"
sm_solgaleo_custom_emoji_name = "solgaleo"
sm_lunala_custom_emoji_name = "lunala"
sv_buzzwole_custom_emoji_name = "buzzwole"

# =============================================================================
# PATH CONFIGURATIONS
# =============================================================================
path_users_data = os.path.join('data', 'UserData.xml')
path_server_data = os.path.join('data', 'ServerData.xml')

DATABASE_BACKUP_PATH = os.path.join('backups', 'database')
PLOT_OUTPUT_PATH = os.path.join('plots')
ANALYTICS_EXPORT_PATH = os.path.join('exports', 'analytics')
LOG_PATH = os.path.join('logs')

# Create directories if they don't exist
required_dirs = ['data', 'backups', 'backups/database', 'plots', 'exports', 'exports/analytics', 'logs']
for directory in required_dirs:
    os.makedirs(directory, exist_ok=True)

# =============================================================================
# XML ATTRIBUTE CONFIGURATIONS (PRESERVED FOR MIGRATION)
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
# COMMAND COOLDOWN SETTINGS
# =============================================================================
COMMAND_COOLDOWN_SECONDS = 2

# =============================================================================
# REQUIRED DIRECTORIES
# =============================================================================
REQUIRED_DIRECTORIES = [
    'data', 'logs', 'backups', 'plots', 'exports', 'exports/analytics'
]

# =============================================================================
# CONFIGURATION VALIDATION
# =============================================================================
def validate_config():
    """Validate configuration settings and return detailed results"""
    errors = []
    warnings = []
    
    # Critical settings validation
    if not token:
        errors.append("Discord token is required - add your bot token to 'token' variable")
    if not guild_id:
        errors.append("Guild ID is required - add your Discord server ID to 'guild_id' variable")
    
    # Required channel validation
    required_channels = {
        'channel_id_commands': 'Commands channel ID',
        'channel_id_heartbeat': 'Heartbeat channel ID', 
        'channel_id_webhook': 'Webhook channel ID',
        'channel_id_user_stats': 'User stats channel ID'
    }
    
    for channel_var, description in required_channels.items():
        if not globals().get(channel_var):
            errors.append(f"{description} is required - configure {channel_var}")
    
    # Channel ID format validation
    all_channels = [
        'channel_id_commands', 'channel_id_user_stats', 'channel_id_heartbeat',
        'channel_id_webhook', 'channel_id_anticheat', 'channel_id_gp_tracking_list',
        'channel_id_notifications'
    ]
    
    for channel in all_channels:
        value = globals().get(channel)
        if value and not str(value).isdigit():
            warnings.append(f"{channel} should be a numeric Discord channel ID")
    
    # Feature dependency validation
    if FEATURES.get('google_sheets_integration') and not GOOGLE_CREDENTIALS_FILE:
        warnings.append("Google Sheets integration enabled but no credentials file specified")
    
    if FEATURES.get('plotting_system') and MAX_CONCURRENT_PLOTS < 1:
        errors.append("MAX_CONCURRENT_PLOTS must be at least 1")
    
    # Performance validation
    if MAX_MEMORY_USAGE_MB < 256:
        warnings.append("MAX_MEMORY_USAGE_MB is very low (< 256MB), may cause performance issues")
    
    # Database path validation
    try:
        db_dir = os.path.dirname(DATABASE_PATH)
        if not os.path.exists(db_dir):
            warnings.append(f"Database directory {db_dir} does not exist (will be created)")
    except Exception as e:
        warnings.append(f"Database path validation failed: {e}")
    
    return {
        'errors': errors,
        'warnings': warnings,
        'valid': len(errors) == 0,
        'total_issues': len(errors) + len(warnings)
    }

def get_configuration_summary():
    """Get a summary of current configuration"""
    return {
        'database_path': DATABASE_PATH,
        'features_enabled': sum(FEATURES.values()),
        'total_features': len(FEATURES),
        'required_channels_configured': sum(1 for ch in [
            channel_id_commands, channel_id_user_stats, 
            channel_id_heartbeat, channel_id_webhook
        ] if ch),
        'guild_configured': bool(guild_id),
        'token_configured': bool(token)
    }

# =============================================================================
# STARTUP VALIDATION
# =============================================================================

# Run validation on import
_validation_result = validate_config()

if not _validation_result['valid']:
    print("🚨 CRITICAL CONFIGURATION ERRORS FOUND:")
    for error in _validation_result['errors']:
        print(f"  ❌ {error}")
    print("\n⚠️  Bot will not start until these errors are fixed!")

if _validation_result['warnings']:
    print("⚠️  Configuration warnings:")
    for warning in _validation_result['warnings']:
        print(f"  ⚠️  {warning}")

if _validation_result['valid']:
    print("✅ Basic configuration validation passed")
    summary = get_configuration_summary()
    print(f"📊 Configuration summary: {summary['features_enabled']}/{summary['total_features']} features enabled, "
          f"{summary['required_channels_configured']}/4 required channels configured")

print("✅ Enhanced config.py loaded successfully")