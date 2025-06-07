import os

# Your app Token, if you don't know it you can reset it here: https://discord.com/developers/applications > Your App > Bot > Reset Token
token = ""
# With Discord developer mode on, right-click your server and "Copy Server ID"
guild_id = ""

# =========================================== CHANNEL IDs ===========================================
# For all channel_id below, right-click a channel in your Discord server and "Copy Server ID" with developer mode on

# THE ID OF THE DISCORD CHANNEL - Where ID list and AutoKick alerts are sent
channel_id_commands = ""
# THE ID OF THE DISCORD CHANNEL - Where statistics of users will be sent
channel_id_user_stats = ""

# Pack specific forum channels - Each one must be a forum channel
channel_id_mewtwo_verification_forum = ""  # Add your Mewtwo channel ID here
channel_id_charizard_verification_forum = ""  # Add your Charizard channel ID here
channel_id_pikachu_verification_forum = ""  # Add your Pikachu channel ID here
channel_id_mew_verification_forum = ""  # Add your Mew channel ID here
channel_id_dialga_verification_forum = ""  # Add your Dialga channel ID here
channel_id_palkia_verification_forum = ""  # Add your Palkia channel ID here
channel_id_arceus_verification_forum = ""  # Add your Arceus channel ID here
channel_id_shining_verification_forum = ""  # Add your Shining channel ID here
channel_id_solgaleo_verification_forum = ""  # Solgaleo channel ID - NEW
channel_id_lunala_verification_forum = ""  # Lunala channel ID - NEW
channel_id_buzzwole_verification_forum = ""  # Buzzwole channel ID - NEW

# THE ID OF THE DISCORD CHANNEL - Where Double 2 Star validation threads will be created ⚠️ IT MUST BE A FORUM CHANNEL
channel_id_2star_verification_forum = ""

# THE ID OF THE DISCORD CHANNEL - Where the Packs Webhooks is linked, better to be a separate channel from heartbeat webhook
channel_id_webhook = ""
# THE ID OF THE DISCORD CHANNEL - Where the Heartbeat Webhooks is linked, better to be a separate channel from packs webhook
channel_id_heartbeat = ""
# THE ID OF THE DISCORD CHANNEL - Where the AntiCheat pseudonyms are sent for analysis
channel_id_anticheat = ""
# THE ID OF THE DISCORD CHANNEL - Where the GP tracking list will be posted
channel_id_gp_tracking_list = ""
# THE ID OF THE DISCORD CHANNEL - Where notifications for new GP/tradeable cards will be sent
channel_id_notifications = ""  # You can use the same as channel_id_commands or create a new channel

# =========================================== GITHUB/GIST SETTINGS ===========================================
# Create a new fine-grained token for your GitHub account, and make sure to only check to read/write your Gists: https://github.com/settings/tokens
git_token = ""
# Then, create a GitGist: https://gist.github.com/ and get its ID (the numbers in the URL).
git_gist_id = ""
# And the GitGist Name based on the name you gave it
git_gist_group_name = ""
# And the GitGist Name based on the name you gave it
git_gist_gp_name = ""

# =========================================== THREAD CREATION SETTINGS ===========================================
# Control whether threads are automatically created for different pack types
create_threads_for_god_packs = True  # Set to False to disable thread creation for God Packs
create_threads_for_tradeable_cards = True  # Set to False to disable thread creation for tradeable cards (Full Art, Rainbow, etc.)
create_threads_for_double_stars = True  # Set to False to disable thread creation for Double 2-Star packs

# If threads are disabled, you can still choose to log the findings to a specific channel
log_pack_finds_to_channel = True  # Set to False to completely disable any pack find logging
pack_finds_log_channel_id = ""  # Channel where pack finds will be logged if threads are disabled (can be same as webhook channel)

# =========================================== RULES ===========================================
# Choose if you want the AntiCheat to be enabled or not, if yes then fill "channel_id_anticheat" above
anti_cheat = True
# If you want your group to be able to add other people than themselves using /active @user 
can_people_add_others = True
# If you want your group to be able to remove other people than themselves using /inactive @user 
can_people_remove_others = True
# If you want to output selected Packs in ids.txt, see Hoytdj patch note: https://github.com/hoytdj/PTCGPB/releases/tag/v1.5.4
enable_role_based_filters = True

# =========================================== AUTO KICK ===========================================
# Setting this to true will enable auto kick and kick players based on the other factors below
auto_kick = True
# Every X minutes divided by 2 it will alternate between sending user stats and checking for inactive people
# Example with a value of 10: 5min:InactivityCheck, 10min:UserStats, 15min:InactivityCheck, 20min:UserStats, etc...
refresh_interval = 30
# After how many minutes the user will be considered inactive (keep in mind that heartbeats are sent every 30min by default)
inactive_time = 61
# At which number of instances users will be kicked, for a value of 1, users with 2 instances and above won't be kicked (Main is not counted as an instance)
inactive_instance_count = 1
# At which number of instances users will be kicked, for a value of 1, users below 1 pack per min will be kicked)
inactive_pack_per_min_count = 1
# Kick instantly if it detects that Main is On AND Offline ⚠️ At this time there are false positives where Main could be considered offline but it has no issue in reality
inactive_if_main_offline = True

# =========================================== LEECHING ===========================================
# Decide whether or not people can leech
can_people_leech = True
# Decide after how many GPs found people can be able to leech
leech_perm_gp_count = 20
# Decide after how many packs opened people can be able to leech
leech_perm_pack_count = 50000

# =========================================== GP STATS ===========================================
# Decide if you want your Server's Stats (GP stats) to be reset every 4 hours which could prevent some duplicated stuff in ServerData.xml 
reset_server_data_frequently = True
# Decide how frequently you want to Reset GP Stats, default is 4 hours (240min)
reset_server_data_time = 240
# 🔴 I highly recommend you leave the next one disabled, it can cause random crashes if running on low-end servers
# Upload UserData.xml to GitGist, "reset_server_data_frequently" also needs to be true for it to work
output_user_data_on_git_gist = True

# =========================================== GP TRACKING ===========================================
# How often to update the GP tracking list (in minutes)
gp_tracking_update_interval = 30
# Determine if updates should use cron schedule or interval-based updates
# true = use node-schedule with cron, false = use setInterval
gp_tracking_use_cron_schedule = True

# NEW: Control what appears in the GP tracking list
include_tradeable_cards_in_tracking = False  # Set to True to include tradeable cards in GP tracking list
include_double_stars_in_tracking = True  # Set to True to include double star packs in GP tracking list
include_god_packs_in_tracking = True  # Set to True to include actual God Packs in GP tracking list

# Optional: Custom labels for different card types in tracking
tradeable_card_tracking_label = "Special Cards"  # Label for tradeable cards section
double_star_tracking_label = "Double Stars"  # Label for double star section  
god_pack_tracking_label = "God Packs"  # Label for god pack section

# Enable or disable notifications for new findings
notifications_enabled = True  # Set to False to disable notifications

# =========================================== ELIGIBLE IDs ===========================================
# If some people in your group are running Min2Stars: 2 and some others 3, that flags all the GPs as 5/5 in the list to avoid auto removal bot from kicking 2/5 for those who are at Min2Stars: 3
safe_eligible_ids_filtering = True  # true = all flagged as 5/5
add_double_star_to_vip_ids_txt = True  # true = add double star pack account usernames to vip ids txt for GP Test Mode

# =========================================== FORCE SKIP ===========================================
# Allows you to bypass GP based on Packs Amount, Example: force_skip_min_2stars 2 & force_skip_min_packs 2 will 
# - not send to verification forum all GP [3P][2/5] [4P][2/5] [5P][2/5] and below 
# - send to verification forum all GP [1P][2/5] [2P][2/5] and above
force_skip_min_2stars = 2
force_skip_min_packs = 2

# =========================================== OTHER TIME SETTINGS ===========================================

# Decide after how much time you want the verification posts to automatically close, it'll be the time from the post creation, not the last activity
# Age of post before closing the post ⚠️ Closed Posts will be removed from the Eligible GPs / VIP IDs list
auto_close_live_post_time = 96  # hours
auto_close_not_live_post_time = 36  # hours
# No need to modify it except if you specifically changed it in the script
heartbeat_rate = 30  # minutes
# No need to modify it except if you specifically changed it in the script
anti_cheat_rate = 3  # minutes
# Decide how frequently you want to Backup UserData, default is 30min
backup_user_datas_time = 30  # minutes
# Delete some messages after X seconds (/active /inactive /refresh /forcerefresh) 0 = no delete
delay_msg_delete_state = 10  # seconds

# =========================================== DISPLAY SETTINGS ===========================================
# Choose language
english_language = True
# Do you want to show GP Lives per User in Stats
show_per_person_live = True

# =========================================== OTHER SETTINGS ===========================================

# Number of /miss needed before a post is marked as dead, here it means 1pack=4miss, 2packs=6miss, 3packs=8miss, etc...
miss_before_dead = [4, 6, 8, 10, 12]
# Multiply the Miss required when a post is flagged as NotLiked (ex: with a value of 0.5 a post with 8 miss required will switch to 4 miss)
miss_not_liked_multiplier = [0.5, 0.5, 0.5, 0.75, 0.85, 1]  # Based on two stars Amount, ex: x0.85 for a [4/5]

# The average Min2Stars of the group on Arturo's bot, used to calculate the Potential Lives GP
min_2stars = 0  # can be a floating number ex: 2.5
# What does your group run, it is used for AntiCheat
group_packs_type = 5  # 5 for 5 packs, 3 for 3 packs

# =========================================== AESTHETICS ===========================================
# Icons of GP Validation
text_verified_logo = "✅"
text_liked_logo = "🔥"
text_waiting_logo = "⌛"
text_not_liked_logo = "🥶"
text_dead_logo = "💀"

leaderboard_best_farm1_custom_emoji_name = "Chadge"  # 🌟 if not found
leaderboard_best_farm2_custom_emoji_name = "PeepoLove"  # ⭐️ if not found
leaderboard_best_farm3_custom_emoji_name = "PeepoShy"  # ✨ if not found
leaderboard_best_farm_length = 6  # Number of People showing in "Best Farmers"

leaderboard_best_verifier1_custom_emoji_name = "Wicked"  # 🥇 if not found
leaderboard_best_verifier2_custom_emoji_name = "PeepoSunglasses"  # 🥈 if not found
leaderboard_best_verifier3_custom_emoji_name = "PeepoHappy"  # 🥉 if not found

leaderboard_worst_verifier1_custom_emoji_name = "Bedge"  # 😈 if not found
leaderboard_worst_verifier2_custom_emoji_name = "PeepoClown"  # 👿 if not found
leaderboard_worst_verifier3_custom_emoji_name = "DinkDonk"  # 💀 if not found /!\ This is the worst one, it should be at the top but that helps for readability 

ga_mewtwo_custom_emoji_name = "mewtwo"  # 🧠 if not found, alternative: 🟣
ga_charizard_custom_emoji_name = "charizard"  # 🔥 if not found, alternative: 🟠
ga_pikachu_custom_emoji_name = "pikachu"  # ⚡️ if not found, alternative: 🟡
mi_mew_custom_emoji_name = "mew"  # 🏝️ if not found, alternative: 🟢
sts_dialga_custom_emoji_name = "dialga"  # 🕒 if not found, alternative: 🟦
sts_palkia_custom_emoji_name = "palkia"  # 🌌 if not found, alternative: 🟪
tl_arceus_custom_emoji_name = "arceus"  # 💡 if not found, alternative: 🟨
sr_giratina_custom_emoji_name = "lucario_shiny"  # ✨ if not found
sm_solgaleo_custom_emoji_name = "solgaleo"  # ☀️ if not found
sm_lunala_custom_emoji_name = "lunala"  # 🌙 if not found
sv_buzzwole_custom_emoji_name = "buzzwole"  # 💪 if not found

# =========================================== PATH CONFIGURATIONS ===========================================
# Path configurations for data files
path_users_data = os.path.join('data', 'UserData.xml')
path_server_data = os.path.join('data', 'ServerData.xml')

# =========================================== XML ATTRIBUTE CONFIGURATIONS ===========================================
# XML attribute configurations - Use the exact same case as in the original XML
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