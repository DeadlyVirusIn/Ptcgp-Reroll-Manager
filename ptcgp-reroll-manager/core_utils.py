import discord
import asyncio
import math
import re
import os
import json
import datetime
import xml.etree.ElementTree as ET
from typing import List, Dict, Tuple, Any, Optional, Union
import sqlite3
import matplotlib.pyplot as plt
import numpy as np
from io import BytesIO

# Import configuration
import config

# Import utilities
from utils import (
    localize, format_number_to_k, format_minutes_to_days, sum_int_array, 
    sum_float_array, round_to_one_decimal, round_to_two_decimals, 
    count_digits, extract_numbers, is_numbers, convert_min_to_ms, convert_ms_to_min,
    split_multi, replace_last_occurrence, replace_miss_count, replace_miss_needed,
    send_received_message, send_channel_message, bulk_delete_messages, color_text,
    add_text_bar, format_number_with_spaces, get_random_string_from_array,
    get_oldest_message, wait, replace_any_logo_with, normalize_ocr,
    get_lasts_anti_cheat_messages
)

# Import miss messages
from miss_sentences import find_emoji

# Import upload utilities
from upload_utils import update_gist

# Setup XML path constants
path_users_data = os.path.join('data', 'UserData.xml')
path_server_data = os.path.join('data', 'ServerData.xml')

async def get_guild(bot) -> discord.Guild:
    """Get the guild object for the configured guild ID."""
    return await bot.fetch_guild(config.guild_id)

async def get_member_by_id(bot, member_id: str) -> Optional[discord.Member]:
    """Get a guild member by their ID."""
    if not member_id or not member_id.strip():
        return None
        
    try:
        guild = await get_guild(bot)
        return await guild.fetch_member(clean_string(member_id))
    except discord.errors.NotFound:
        print(f"⚠️ Member with ID {member_id} not found")
        return None
    except Exception as e:
        print(f"❌ Error fetching member {member_id}: {e}")
        return None
def clean_string(text: str) -> str:
    """Remove non-alphanumeric characters from a string."""
    if not text:
        return ""
    return re.sub(r'[^a-zA-Z0-9]', '', str(text))

def check_file_exists(path: str) -> bool:
    """Check if a file exists."""
    return os.path.isfile(path)

def check_file_exists_or_create(path: str, root_element: str = "root") -> bool:
    """Check if a file exists, create it if not."""
    if os.path.isfile(path):
        return True
        
    # Create directory if it doesn't exist
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
        
    # Create XML file with root element
    root = ET.Element(root_element)
    tree = ET.ElementTree(root)
    
    with open(path, 'wb') as f:
        tree.write(f, encoding='utf-8', xml_declaration=True)
        
    return True

async def read_file_async(path: str) -> str:
    """Read a file asynchronously."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"❌ Error reading file {path}: {e}")
        return ""

def write_file(path: str, content: str) -> bool:
    """Write content to a file."""
    try:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    except Exception as e:
        print(f"❌ Error writing to file {path}: {e}")
        return False

def backup_file(path: str) -> bool:
    """Create a backup of a file."""
    if not os.path.isfile(path):
        return False
        
    backup_path = f"{path}.bak"
    try:
        with open(path, 'r', encoding='utf-8') as src:
            with open(backup_path, 'w', encoding='utf-8') as dst:
                dst.write(src.read())
        return True
    except Exception as e:
        print(f"❌ Error backing up file {path}: {e}")
        return False
async def does_user_profile_exists(user_id: str, username: str) -> bool:
    """Check if a user profile exists in UserData.xml."""
    if not check_file_exists(path_users_data):
        return False
        
    try:
        tree = ET.parse(path_users_data)
        root = tree.getroot()
        
        for user in root.findall('user'):
            if user.get('id') == user_id:
                return True
                
        # User not found, create profile
        new_user = ET.SubElement(root, 'user')
        new_user.set('id', user_id)
        new_user.set('username', username)
        
        tree.write(path_users_data, encoding='utf-8', xml_declaration=True)
        return True
    except Exception as e:
        print(f"❌ Error checking user profile: {e}")
        return False

async def set_user_attrib_value(user_id: str, username: str, attribute: str, value: Any) -> bool:
    """Set an attribute value for a user."""
    await does_user_profile_exists(user_id, username)
    
    try:
        tree = ET.parse(path_users_data)
        root = tree.getroot()
        
        # Find the user
        user = None
        for u in root.findall('user'):
            if u.get('id') == user_id:
                user = u
                break
                
        if user is None:
            return False
            
        # Set the attribute
        user.set(attribute, str(value))
        tree.write(path_users_data, encoding='utf-8', xml_declaration=True)
        return True
    except Exception as e:
        print(f"❌ Error setting user attribute: {e}")
        return False

async def get_user_attrib_value(user_id: str, attribute: str, default_value: Any = None) -> Any:
    """Get an attribute value for a user."""
    if not check_file_exists(path_users_data):
        return default_value
        
    try:
        tree = ET.parse(path_users_data)
        root = tree.getroot()
        
        for user in root.findall('user'):
            if user.get('id') == user_id:
                value = user.get(attribute)
                return value if value is not None else default_value
                
        return default_value
    except Exception as e:
        print(f"❌ Error getting user attribute: {e}")
        return default_value
async def get_active_users(active_only: bool = True, include_farm: bool = False) -> List[ET.Element]:
    """Get active users from UserData.xml."""
    if not check_file_exists(path_users_data):
        return []
        
    try:
        tree = ET.parse(path_users_data)
        root = tree.getroot()
        
        active_users = []
        for user in root.findall('user'):
            user_state = user.get('user_state', 'inactive')
            
            if active_only:
                if user_state == 'active':
                    active_users.append(user)
                elif include_farm and user_state == 'farm':
                    active_users.append(user)
            else:
                active_users.append(user)
                
        return active_users
    except Exception as e:
        print(f"❌ Error getting active users: {e}")
        return []

async def get_active_ids() -> str:
    """Get active user IDs formatted for ids.txt."""
    active_users = await get_active_users(True, False)
    
    id_list = []
    for user in active_users:
        pocket_id = user.get('pocket_id')
        if pocket_id:
            selected_pack = user.get('selected_pack', '')
            
            # Format the entry based on whether role-based filters are enabled
            if config.enable_role_based_filters and selected_pack:
                id_list.append(f"{pocket_id}/{selected_pack}")
            else:
                id_list.append(pocket_id)
    
    return '\n'.join(id_list)

async def get_all_users() -> List[ET.Element]:
    """Get all users from UserData.xml."""
    return await get_active_users(False, True)

def get_username_from_user(user: ET.Element) -> str:
    """Get username from user element."""
    return user.get('username', 'Unknown')

def get_id_from_user(user: ET.Element) -> str:
    """Get ID from user element."""
    return user.get('id', '0')

def get_attrib_value_from_user(user: ET.Element, attribute: str, default_value: Any = None) -> Any:
    """Get attribute value from user element."""
    value = user.get(attribute)
    return value if value is not None else default_value

def get_time_from_gp(gp_element: Any) -> datetime.datetime:
    """Get time from GP element."""
    time_str = gp_element.get('time', None)
    if time_str:
        try:
            return datetime.datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        except:
            pass
    return datetime.datetime.now()
async def refresh_user_active_state(user: ET.Element) -> Tuple[str, float]:
    """Refresh user active state and return status and inactive time."""
    current_time = datetime.datetime.now()
    
    last_hb_time_str = get_attrib_value_from_user(user, 'last_heartbeat_time', 0)
    try:
        if last_hb_time_str == 0:
            return 'inactive', 0
            
        last_hb_time = datetime.datetime.fromisoformat(last_hb_time_str.replace('Z', '+00:00'))
        inactive_minutes = (current_time - last_hb_time).total_seconds() / 60
        
        if inactive_minutes <= float(config.heartbeat_rate + 1):
            return 'active', 0
        elif inactive_minutes <= float(config.inactive_time):
            return 'waiting', inactive_minutes
        else:
            return 'inactive', inactive_minutes
    except Exception as e:
        print(f"❌ Error refreshing user active state: {e}")
        return 'inactive', 0

async def refresh_user_real_instances(user: ET.Element, active_state: str) -> int:
    """Refresh and return user's real instance count."""
    if active_state != 'active':
        return 0
        
    hb_instances = int(get_attrib_value_from_user(user, 'hb_instances', 0))
    
    # Get instances from subsystems
    subsystem_instances = 0
    subsystems = user.findall('subsystem')
    for subsystem in subsystems:
        subsystem_hb_time_str = subsystem.get('last_heartbeat_time', '0')
        try:
            subsystem_hb_time = datetime.datetime.fromisoformat(subsystem_hb_time_str.replace('Z', '+00:00'))
            current_time = datetime.datetime.now()
            diff_minutes = (current_time - subsystem_hb_time).total_seconds() / 60
            
            if diff_minutes <= float(config.heartbeat_rate + 1):
                subsystem_instances += int(subsystem.get('hb_instances', 0))
        except:
            pass
    
    total_instances = hb_instances + subsystem_instances
    
    # Update real_instances in user data
    user_id = get_id_from_user(user)
    username = get_username_from_user(user)
    await set_user_attrib_value(user_id, username, 'real_instances', total_instances)
    
    return total_instances
async def get_server_data_gps(gp_type: str) -> List[Any]:
    """Get GP data from ServerData.xml."""
    if not check_file_exists(path_server_data):
        return []
        
    try:
        tree = ET.parse(path_server_data)
        root = tree.getroot()
        
        # Find the GP element
        gp_parent = root.find(gp_type)
        if gp_parent is None:
            return []
            
        gp_items = []
        gp_single_type = gp_type.rstrip('s')  # Remove plural 's'
        
        for gp in gp_parent.findall(gp_single_type):
            gp_items.append(gp)
            
        return gp_items
    except Exception as e:
        print(f"❌ Error getting server data GPs: {e}")
        return []

async def add_server_gp(gp_type: str, forum_post):
    """Add a GP to ServerData.xml."""
    if not check_file_exists(path_server_data):
        check_file_exists_or_create(path_server_data, "root")
        
    try:
        tree = ET.parse(path_server_data)
        root = tree.getroot()
        
        # Find or create the GP parent element
        gp_parent = root.find(gp_type + 's')  # Add plural 's'
        if gp_parent is None:
            gp_parent = ET.SubElement(root, gp_type + 's')
            
        # Create a new GP element
        new_gp = ET.SubElement(gp_parent, gp_type)
        new_gp.set('time', datetime.datetime.now().isoformat())
        new_gp.set('name', forum_post.name)
        new_gp.text = str(forum_post.id)
        
        tree.write(path_server_data, encoding='utf-8', xml_declaration=True)
    except Exception as e:
        print(f"❌ Error adding server GP: {e}")

async def get_pack_specific_channel(pack_booster_type: str) -> str:
    """Get the channel ID for a specific pack type."""
    print(f"Finding channel for pack type: \"{pack_booster_type}\"")
    
    # Handle empty input
    if not pack_booster_type:
        print("No pack type provided, defaulting to Mewtwo channel")
        return config.channel_id_mewtwo_verification_forum
    
    # Clean up the input
    pack_type = pack_booster_type.strip().upper()
    print(f"Normalized pack type: \"{pack_type}\"")
    
    # Direct mapping based on includes
    if "MEWTWO" in pack_type:
        print("Routing to Mewtwo channel")
        return config.channel_id_mewtwo_verification_forum
    elif "CHARIZARD" in pack_type:
        print("Routing to Charizard channel")
        return config.channel_id_charizard_verification_forum
    elif "PIKACHU" in pack_type:
        print("Routing to Pikachu channel")
        return config.channel_id_pikachu_verification_forum
    elif "MEW" in pack_type and "MEWTWO" not in pack_type:
        print("Routing to Mew channel")
        return config.channel_id_mew_verification_forum
    elif "DIALGA" in pack_type:
        print("Routing to Dialga channel")
        return config.channel_id_dialga_verification_forum
    elif "PALKIA" in pack_type:
        print("Routing to Palkia channel")
        return config.channel_id_palkia_verification_forum
    elif "ARCEUS" in pack_type:
        print("Routing to Arceus channel")
        return config.channel_id_arceus_verification_forum
    elif "SHINING" in pack_type:
        print("Routing to Shining channel")
        return config.channel_id_shining_verification_forum
    elif "SOLGALEO" in pack_type:
        print("Routing to Solgaleo channel")
        return config.channel_id_solgaleo_verification_forum
    elif "LUNALA" in pack_type:
        print("Routing to Lunala channel")
        return config.channel_id_lunala_verification_forum
    else:
        print(f"No specific match for \"{pack_type}\", defaulting to Mewtwo channel")
        return config.channel_id_mewtwo_verification_forum
async def get_users_stats(users: List[ET.Element], members: List[discord.Member], is_anti_cheat_on: bool = False) -> List[str]:
    """Get formatted stats for users."""
    current_time = datetime.datetime.now()
    users_stats = []
    
    # Collect all data for sorting and additional calculations
    user_data_array = []
    
    for user in users:
        try:
            user_id = get_id_from_user(user)
            username = get_username_from_user(user)
            visible_username = username
            
            # Match Discord display name
            member = next((m for m in members if m.name == username), None)
            if member:
                visible_username = member.display_name
                
            # Gather basic user data
            user_state = get_attrib_value_from_user(user, 'user_state', 'inactive')
            user_active_state = await refresh_user_active_state(user)
            active_state = user_active_state[0]
            inactive_time = user_active_state[1]
            
            # Gather system stats - this part might need adaptation based on your XML structure
            # For this example, we'll assume subsystems are child elements of user
            instances_subsystems = []
            session_time_subsystems = []
            session_packs_subsystems = []
            last_hb_time_subsystems = []
            diff_packs_since_last_hb_subsystems = []
            
            for subsystem in user.findall('subsystem'):
                instances_subsystems.append(int(subsystem.get('hb_instances', 0)))
                session_time_subsystems.append(float(subsystem.get('session_time', 0)))
                session_packs_subsystems.append(float(subsystem.get('session_packs_opened', 0)))
                last_hb_time_subsystems.append(subsystem.get('last_heartbeat_time', '0'))
                diff_packs_since_last_hb_subsystems.append(float(subsystem.get('diff_packs_since_last_hb', 0)))
                
            # Calculate aggregated stats
            session_packs_subsystems = 0
            total_packs_since_last_hb_subsystems = 0
            total_packs_subsystems = 0
            total_diff_packs_since_last_hb_subsystems = 0
            bigger_session_time_subsystems = 0
            active_subsystems_count = 0
            
            for i, last_hb_time_str in enumerate(last_hb_time_subsystems):
                try:
                    last_hb_time = datetime.datetime.fromisoformat(last_hb_time_str.replace('Z', '+00:00'))
                    diff_hb_subsystem = (current_time - last_hb_time).total_seconds() / 60
                    
                    if diff_hb_subsystem < float(config.heartbeat_rate + 1):
                        bigger_session_time_subsystems = max(bigger_session_time_subsystems, session_time_subsystems[i])
                        session_packs_subsystems += float(session_packs_subsystems[i])
                        total_diff_packs_since_last_hb_subsystems += float(diff_packs_since_last_hb_subsystems[i])
                        active_subsystems_count += 1
                except:
                    pass
                    
                total_packs_subsystems += float(session_packs_subsystems[i] if i < len(session_packs_subsystems) else 0)
                
            # Get or calculate additional user stats
            instances = await refresh_user_real_instances(user, active_state)
            session_time = float(get_attrib_value_from_user(user, 'session_time', 0))
            session_time = round_to_one_decimal(max(session_time, bigger_session_time_subsystems))
            
            session_pack_f = float(get_attrib_value_from_user(user, 'session_packs_opened', 0)) + session_packs_subsystems
            
            # Calculate packs/min and packs/hour
            diff_packs_since_last_hb = float(get_attrib_value_from_user(user, 'diff_packs_since_last_hb', 0)) + total_diff_packs_since_last_hb_subsystems
            diff_time_since_last_hb = float(get_attrib_value_from_user(user, 'diff_time_since_last_hb', config.heartbeat_rate))
            avg_pack_min = round_to_one_decimal(diff_packs_since_last_hb / diff_time_since_last_hb)
            
            if math.isnan(avg_pack_min) or user_state == "leech":
                avg_pack_min = 0
                
            avg_pack_hour = round_to_one_decimal(avg_pack_min * 60)
            
            # Update packsPerMin in user data
            await set_user_attrib_value(user_id, username, 'packs_per_min', avg_pack_min)
            
            # Get GP stats
            total_pack = int(get_attrib_value_from_user(user, 'total_packs_opened', 0))
            session_pack_i = int(get_attrib_value_from_user(user, 'session_packs_opened', 0)) + int(total_packs_subsystems)
            total_god_pack = int(get_attrib_value_from_user(user, 'god_pack_found', 0))
            
            avg_god_pack = round_to_one_decimal((total_pack + session_pack_i) / total_god_pack if total_god_pack >= 1 else (total_pack + session_pack_i))
            gp_live = int(get_attrib_value_from_user(user, 'god_pack_live', 0))
            
            # Get miss stats
            total_miss = int(get_attrib_value_from_user(user, 'total_miss', 0))
            total_time = float(get_attrib_value_from_user(user, 'total_time', 0))
            miss_per_24_hour = round_to_one_decimal((float(total_miss) / (total_time / 60)) * 24 if total_time > 0 else 0)
            
            # Calculate time since last GP - this would need actual tracking in your system
            time_since_last_gp = None
            if total_god_pack > 0:
                # This is just a placeholder - implement actual tracking
                import random
                days_since_last_gp = random.randint(0, 30)
                time_since_last_gp = f"{days_since_last_gp}d"
                
            # Test data - placeholders
            id_no_show_tests = 0
            all_no_show_tests = 0
            all_miss_tests = 0
            
            # Last HB time calculation
            last_hb_time_str = get_attrib_value_from_user(user, 'last_heartbeat_time', '0')
            try:
                last_hb_time = datetime.datetime.fromisoformat(last_hb_time_str.replace('Z', '+00:00'))
                minutes_since_last_hb = round_to_one_decimal((current_time - last_hb_time).total_seconds() / 60)
            except:
                minutes_since_last_hb = 0
                
            # Selected packs
            selected_packs = get_attrib_value_from_user(user, 'selected_pack', '')
            
            # Store all user data
            user_data_array.append({
                'id': user_id,
                'username': username,
                'visible_username': visible_username,
                'user_state': user_state,
                'active_state': active_state,
                'inactive_time': inactive_time,
                'instances': instances,
                'session_time': session_time,
                'session_pack_f': session_pack_f,
                'avg_pack_min': avg_pack_min,
                'avg_pack_hour': avg_pack_hour,
                'total_pack': total_pack,
                'session_pack_i': session_pack_i,
                'total_god_pack': total_god_pack,
                'avg_god_pack': avg_god_pack,
                'gp_live': gp_live,
                'total_miss': total_miss,
                'total_time': total_time,
                'miss_per_24_hour': miss_per_24_hour,
                'minutes_since_last_hb': minutes_since_last_hb,
                'active_subsystems_count': active_subsystems_count,
                'selected_packs': selected_packs,
                'id_no_show_tests': id_no_show_tests,
                'all_no_show_tests': all_no_show_tests,
                'all_miss_tests': all_miss_tests,
                'time_since_last_gp': time_since_last_gp
            })
        except Exception as e:
            print(f"❌ Error processing stats for user {get_id_from_user(user)}: {e}")
# Continue from the previous function (get_users_stats)
    
    # Sort users by active state and then by packs per minute
    user_data_array.sort(key=lambda x: (
        0 if x['active_state'] == 'active' else 1,
        {'active': 0, 'farm': 1, 'leech': 2, 'inactive': 3}.get(x['user_state'], 4),
        -x['avg_pack_min']
    ))
    
    # Format each user's stats
    for user_data in user_data_array:
        user_output = "```ansi\n"
        
        # Format username with status color and timestamp
        username_with_time = user_data['visible_username']
        if user_data['active_state'] == 'active':
            last_hb_hours = int(user_data['minutes_since_last_hb'] // 60)
            last_hb_minutes = int(user_data['minutes_since_last_hb'] % 60)
            if last_hb_hours > 0 or last_hb_minutes > 0:
                username_with_time += f"[{last_hb_hours}h{last_hb_minutes}m]"
                
        # Color based on state
        if user_data['user_state'] == 'active':
            if user_data['active_state'] == 'active':
                user_output += color_text(username_with_time, "green")
            elif user_data['active_state'] == 'waiting':
                user_output += color_text(username_with_time, "yellow") + " - started"
            else:  # inactive
                if user_data['minutes_since_last_hb'] == 0 or math.isnan(user_data['minutes_since_last_hb']):
                    user_output += color_text(username_with_time, "red") + f" - {color_text('Heartbeat issue', 'red')}"
                else:
                    inactive_time_rounded = round(float(user_data['inactive_time']))
                    user_output += color_text(username_with_time, "red") + f" - inactive for {color_text(inactive_time_rounded, 'red')}mn"
        elif user_data['user_state'] == 'farm':
            user_output += color_text(username_with_time, "cyan")
        elif user_data['user_state'] == 'leech':
            user_output += color_text(username_with_time, "pink")
            
        # Display instances
        subsystem_text = f"({user_data['instances']}/{user_data['active_subsystems_count']})" if user_data['active_subsystems_count'] > 0 else ''
        user_output += f" | {color_text(f'{user_data['instances']} {subsystem_text} Instances', 'gray')}\n"
        
        # Session stats with packs per hour
        user_output += f"Session: {color_text(user_data['session_time'], 'cyan')}({color_text(user_data['avg_pack_hour'], 'cyan')}) pph"
        user_output += f" running {color_text(f'{user_data['session_time']}m', 'gray')} w/ {color_text(f'{user_data['session_pack_f']} packs', 'gray')} in last {color_text('24H', 'gray')}\n"
        
        # Miss tests stats
        user_output += f"ID Miss Tests: {color_text(user_data['id_no_show_tests'], 'cyan')} ID Noshow Tests: {color_text(user_data['all_no_show_tests'], 'cyan')} All Miss Tests: {color_text(user_data['all_miss_tests'], 'cyan')}\n"
        
        # Pack stats
        user_output += f"Packs: {color_text(user_data['total_pack'], 'cyan')} GP: {color_text(user_data['total_god_pack'], 'cyan')} Alive GP: {color_text(user_data['gp_live'], 'cyan')}\n"
        
        # Time since last GP
        if user_data['time_since_last_gp']:
            user_output += f"Time since last GP: {color_text(user_data['time_since_last_gp'], 'cyan')}\n"
            
        user_output += "```"
        users_stats.append(user_output)
        
    return users_stats
async def create_enhanced_stats_embed(active_users: List[ET.Element], all_users: List[ET.Element]) -> List[discord.Embed]:
    """Create enhanced statistics embeds."""
    current_time = datetime.datetime.now()
    
    # Calculate active users statistics
    active_users = await get_active_users(True, False)  # Refresh users
    
    # Get real instances for active users
    active_instances = []
    for user in active_users:
        active_state = (await refresh_user_active_state(user))[0]
        instances = await refresh_user_real_instances(user, active_state)
        active_instances.append(instances)
    
    instances_amount = sum(active_instances)
    avg_instances = round_to_one_decimal(instances_amount / len(active_users) if active_users else 0)
    
    # Get packs per minute stats
    global_packs_per_min = []
    for user in active_users:
        ppm = float(get_attrib_value_from_user(user, 'packs_per_min', 0))
        global_packs_per_min.append(ppm)
    
    accumulated_packs_per_min = sum(global_packs_per_min)
    avg_packs_per_min = round_to_one_decimal(accumulated_packs_per_min / len(active_users) if active_users else 0)
    
    # Calculate packs per hour
    total_packs_per_hour = round_to_one_decimal(accumulated_packs_per_min * 60)
    
    # Calculate session times
    session_times = []
    for user in active_users:
        session_time = float(get_attrib_value_from_user(user, 'session_time', 0))
        session_times.append(session_time)
    
    total_session_time = sum(session_times)
    avg_session_time = round_to_one_decimal(total_session_time / len(active_users) if active_users else 0)
    
    # Calculate total time online over past 24h
    last_24_hours = current_time - datetime.timedelta(days=1)
    total_online_time = 0
    total_packs_last_24h = 0
    
    for user in active_users:
        last_hb_time_str = get_attrib_value_from_user(user, 'last_heartbeat_time', '0')
        try:
            last_hb_time = datetime.datetime.fromisoformat(last_hb_time_str.replace('Z', '+00:00'))
            if last_hb_time > last_24_hours:
                session_time = float(get_attrib_value_from_user(user, 'session_time', 0))
                total_online_time += session_time
                
                # Count packs in last 24h
                diff_packs = float(get_attrib_value_from_user(user, 'diff_packs_since_last_hb', 0))
                total_packs_last_24h += diff_packs
        except:
            pass
    
    # Convert minutes to hours
    total_online_hours = round_to_one_decimal(total_online_time / 60)
    
    # Total server stats
    total_server_packs = sum([int(get_attrib_value_from_user(user, 'total_packs_opened', 0)) for user in all_users])
    total_server_time = sum([float(get_attrib_value_from_user(user, 'total_time', 0)) for user in all_users])
    
    # Calculate GP statistics
    eligible_gps = await get_server_data_gps('eligible_gps')
    ineligible_gps = await get_server_data_gps('ineligible_gps')
    live_gps = await get_server_data_gps('live_gps')
    
    eligible_gp_count = 0
    ineligible_gp_count = 0
    live_gp_count = 0
    week_eligible_gp_count = 0
    week_live_gp_count = 0
    today_eligible_gp_count = 0
    today_live_gp_count = 0
    
    total_gp_count = 0
    potential_live_gp_count = 0
    
    week_luck = 0
    total_luck = 0
    today_luck = 0
    
    one_week_ago = current_time - datetime.timedelta(days=7)
    today_start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
    
    if eligible_gps:
        eligible_gp_count = len(eligible_gps)
        
        for eligible_gp in eligible_gps:
            gp_time = get_time_from_gp(eligible_gp)
            if gp_time > one_week_ago:
                week_eligible_gp_count += 1
            if gp_time > today_start:
                today_eligible_gp_count += 1
                
        if ineligible_gps:
            ineligible_gp_count = len(ineligible_gps)
            total_gp_count = eligible_gp_count + ineligible_gp_count
            
            if live_gps:
                live_gp_count = len(live_gps)
                
                for live_gp in live_gps:
                    gp_time = get_time_from_gp(live_gp)
                    if gp_time > one_week_ago:
                        week_live_gp_count += 1
                    if gp_time > today_start:
                        today_live_gp_count += 1
                        
                if week_eligible_gp_count > 0:
                    week_luck = round_to_one_decimal(week_live_gp_count / week_eligible_gp_count * 100)
                    
                if today_eligible_gp_count > 0:
                    today_luck = round_to_one_decimal(today_live_gp_count / today_eligible_gp_count * 100)
                    
                if live_gp_count > 0:
                    total_luck = round_to_one_decimal(live_gp_count / eligible_gp_count * 100)
                    
                if not math.isnan(total_luck) and total_luck > 0 and total_gp_count > 0:
                    potential_eligible_gp_count = eligible_gp_count + (ineligible_gp_count * config.min_2stars * 0.1)
                    potential_live_gp_count = round(potential_eligible_gp_count * (total_luck / 100))
    
    # Create embeds
    stats_embed1 = discord.Embed(title="Summary", color=0xf02f7e)
    stats_embed1.add_field(name="👥 Rerollers", value=f"{len(active_users)}", inline=True)
    stats_embed1.add_field(name="🔄 Instances", value=f"{instances_amount}", inline=True)
    stats_embed1.add_field(name="📊 Avg Inst/User", value=f"{avg_instances}", inline=True)
    
    stats_embed1.add_field(name="🔥 Pack/Min", value=f"{round_to_one_decimal(accumulated_packs_per_min)}", inline=True)
    stats_embed1.add_field(name="🔥 Pack/Hour", value=f"{total_packs_per_hour}", inline=True)
    stats_embed1.add_field(name="📊 Avg PPM/User", value=f"{avg_packs_per_min}", inline=True)
    
    stats_embed1.add_field(name="📊 Avg Session", value=f"{avg_session_time}mn", inline=True)
    stats_embed1.add_field(name="🕒 Online 24h", value=f"{total_online_hours}h", inline=True)
    stats_embed1.add_field(name="📦 Packs 24h", value=f"{format_number_to_k(total_packs_last_24h)}", inline=True)
    
    stats_embed1.add_field(name="🃏 Total Packs", value=f"{format_number_to_k(total_server_packs)}", inline=True)
    stats_embed1.add_field(name="🕓 Total Time", value=f"{format_minutes_to_days(total_server_time)}", inline=True)
    
    stats_embed2 = discord.Embed(title="GodPack Stats", color=0xf02f7e)
    stats_embed2.add_field(name="✅ Today Live", value=f"{today_live_gp_count}/{today_eligible_gp_count}", inline=True)
    stats_embed2.add_field(name="🍀 Today Luck", value=f"{today_luck}%", inline=True)
    stats_embed2.add_field(name="\u200B", value="\u200B", inline=True)
    
    stats_embed2.add_field(name="✅ Week Live", value=f"{week_live_gp_count}/{week_eligible_gp_count}", inline=True)
    stats_embed2.add_field(name="🍀 Week Luck", value=f"{week_luck}%", inline=True)
    stats_embed2.add_field(name="\u200B", value="\u200B", inline=True)
    
    stats_embed2.add_field(name="✅ Total Live", value=f"{live_gp_count}/{eligible_gp_count}", inline=True)
    stats_embed2.add_field(name="🍀 Total Luck", value=f"{total_luck}%", inline=True)
    stats_embed2.add_field(name="\u200B", value="\u200B", inline=True)
    
    stats_embed2.add_field(name="☑️ Potential Live", value=f"{potential_live_gp_count}", inline=True)
    stats_embed2.add_field(name="📊 Total GP", value=f"{total_gp_count}", inline=True)
    
    return [stats_embed1, stats_embed2]
async def get_enhanced_selected_packs_embed_text(bot, active_users: List[ET.Element]) -> str:
    """Get formatted text for selected packs embed."""
    pack_counter = {
        'GA_Mewtwo': 0,
        'GA_Charizard': 0,
        'GA_Pikachu': 0,
        'MI_Mew': 0,
        'STS_Dialga': 0,
        'STS_Palkia': 0,
        'TL_Arceus': 0,
        'SR_Giratina': 0,
        'SM_Solgaleo': 0,
        'SM_Lunala': 0
    }
    
    # Track which users are rolling each pack type
    pack_rollers = {
        'GA_Mewtwo': [],
        'GA_Charizard': [],
        'GA_Pikachu': [],
        'MI_Mew': [],
        'STS_Dialga': [],
        'STS_Palkia': [],
        'TL_Arceus': [],
        'SR_Giratina': [],
        'SM_Solgaleo': [],
        'SM_Lunala': []
    }
    
    # Process each user's pack selection
    for user in active_users:
        user_id = get_id_from_user(user)
        username = get_username_from_user(user)
        
        # Get display name
        member = await get_member_by_id(bot, user_id)
        display_name = member.display_name if member else username
        
        # Helper function to add user to pack counter
        def add_user_to_pack(pack_type, instances, user_name):
            pack_counter[pack_type] += instances
            pack_rollers[pack_type].append({
                'name': user_name,
                'instances': instances
            })
            
        # Process main user packs
        selected_packs = get_attrib_value_from_user(user, 'selected_pack', '')
        hb_instances = float(get_attrib_value_from_user(user, 'hb_instances', 0))
        
        # Calculate instances per pack type
        different_packs_amount = max(len(selected_packs.split(',')) if ',' in selected_packs else 1, 1)
        instances_per_pack = hb_instances / different_packs_amount
        
        # Check each pack type
        if 'MEWTWO' in selected_packs.upper():
            add_user_to_pack('GA_Mewtwo', instances_per_pack, display_name)
        if 'CHARIZARD' in selected_packs.upper():
            add_user_to_pack('GA_Charizard', instances_per_pack, display_name)
        if 'PIKACHU' in selected_packs.upper():
            add_user_to_pack('GA_Pikachu', instances_per_pack, display_name)
        if 'MEW' in selected_packs.upper():
            add_user_to_pack('MI_Mew', instances_per_pack, display_name)
        if 'DIALGA' in selected_packs.upper():
            add_user_to_pack('STS_Dialga', instances_per_pack, display_name)
        if 'PALKIA' in selected_packs.upper():
            add_user_to_pack('STS_Palkia', instances_per_pack, display_name)
        if 'ARCEUS' in selected_packs.upper():
            add_user_to_pack('TL_Arceus', instances_per_pack, display_name)
        if 'SHINING' in selected_packs.upper():
            add_user_to_pack('SR_Giratina', instances_per_pack, display_name)
        if 'SOLGALEO' in selected_packs.upper():
            add_user_to_pack('SM_Solgaleo', instances_per_pack, display_name)
        if 'LUNALA' in selected_packs.upper():
            add_user_to_pack('SM_Lunala', instances_per_pack, display_name)
            
        # Process subsystems
        for subsystem in user.findall('subsystem'):
            selected_packs_subsystems = subsystem.get('selected_pack', '')
            hb_instances_subsystems = float(subsystem.get('hb_instances', 0))
            
            # Calculate instances per pack type for subsystem
            different_packs_amount_sub = max(len(selected_packs_subsystems.split(',')) if ',' in selected_packs_subsystems else 1, 1)
            instances_per_pack_sub = hb_instances_subsystems / different_packs_amount_sub
            
            # Check each pack type for subsystem
            if 'MEWTWO' in selected_packs_subsystems.upper():
                add_user_to_pack('GA_Mewtwo', instances_per_pack_sub, f"{display_name} (sub)")
            if 'CHARIZARD' in selected_packs_subsystems.upper():
                add_user_to_pack('GA_Charizard', instances_per_pack_sub, f"{display_name} (sub)")
            if 'PIKACHU' in selected_packs_subsystems.upper():
                add_user_to_pack('GA_Pikachu', instances_per_pack_sub, f"{display_name} (sub)")
            if 'MEW' in selected_packs_subsystems.upper():
                add_user_to_pack('MI_Mew', instances_per_pack_sub, f"{display_name} (sub)")
            if 'DIALGA' in selected_packs_subsystems.upper():
                add_user_to_pack('STS_Dialga', instances_per_pack_sub, f"{display_name} (sub)")
            if 'PALKIA' in selected_packs_subsystems.upper():
                add_user_to_pack('STS_Palkia', instances_per_pack_sub, f"{display_name} (sub)")
            if 'ARCEUS' in selected_packs_subsystems.upper():
                add_user_to_pack('TL_Arceus', instances_per_pack_sub, f"{display_name} (sub)")
            if 'SHINING' in selected_packs_subsystems.upper():
                add_user_to_pack('SR_Giratina', instances_per_pack_sub, f"{display_name} (sub)")
            if 'SOLGALEO' in selected_packs_subsystems.upper():
                add_user_to_pack('SM_Solgaleo', instances_per_pack_sub, f"{display_name} (sub)")
            if 'LUNALA' in selected_packs_subsystems.upper():
                add_user_to_pack('SM_Lunala', instances_per_pack_sub, f"{display_name} (sub)")
    
    # Round all pack counts
    for key in pack_counter:
        pack_counter[key] = round_to_one_decimal(pack_counter[key])
        
    # Sort rollers by instances
    for key in pack_rollers:
        pack_rollers[key].sort(key=lambda x: x['instances'], reverse=True)
        
    # Get emojis
    emoji_GA_Mewtwo = find_emoji(bot, config.ga_mewtwo_custom_emoji_name, "🧠")
    emoji_GA_Charizard = find_emoji(bot, config.ga_charizard_custom_emoji_name, "🔥")
    emoji_GA_Pikachu = find_emoji(bot, config.ga_pikachu_custom_emoji_name, "⚡️")
    emoji_MI_Mew = find_emoji(bot, config.mi_mew_custom_emoji_name, "🏝️")
    emoji_STS_Dialga = find_emoji(bot, config.sts_dialga_custom_emoji_name, "🕒")
    emoji_STS_Palkia = find_emoji(bot, config.sts_palkia_custom_emoji_name, "🌌")
    emoji_TL_Arceus = find_emoji(bot, config.tl_arceus_custom_emoji_name, "💡")
    emoji_SR_Giratina = find_emoji(bot, config.sr_giratina_custom_emoji_name, "✨")
    emoji_SM_Solgaleo = find_emoji(bot, config.sm_solgaleo_custom_emoji_name, "☀️")
    emoji_SM_Lunala = find_emoji(bot, config.sm_lunala_custom_emoji_name, "🌙")
    
    # Create detailed description
    pack_details = ""
    
    # First generation packs
    if pack_counter['GA_Mewtwo'] > 0:
        pack_details += f"{emoji_GA_Mewtwo} **Mewtwo:** {pack_counter['GA_Mewtwo']} instances\n"
        if pack_rollers['GA_Mewtwo']:
            top_rollers = pack_rollers['GA_Mewtwo'][:3]
            roller_text = '\n'.join([f"  • {roller['name']}: {round_to_one_decimal(roller['instances'])}" for roller in top_rollers])
            pack_details += f"{roller_text}\n\n"
        else:
            pack_details += "\n"
    
    if pack_counter['GA_Charizard'] > 0:
        pack_details += f"{emoji_GA_Charizard} **Charizard:** {pack_counter['GA_Charizard']} instances\n"
        if pack_rollers['GA_Charizard']:
            top_rollers = pack_rollers['GA_Charizard'][:3]
            roller_text = '\n'.join([f"  • {roller['name']}: {round_to_one_decimal(roller['instances'])}" for roller in top_rollers])
            pack_details += f"{roller_text}\n\n"
        else:
            pack_details += "\n"
    
    if pack_counter['GA_Pikachu'] > 0:
        pack_details += f"{emoji_GA_Pikachu} **Pikachu:** {pack_counter['GA_Pikachu']} instances\n"
        if pack_rollers['GA_Pikachu']:
            top_rollers = pack_rollers['GA_Pikachu'][:3]
            roller_text = '\n'.join([f"  • {roller['name']}: {round_to_one_decimal(roller['instances'])}" for roller in top_rollers])
            pack_details += f"{roller_text}\n\n"
        else:
            pack_details += "\n"
            
    if pack_counter['MI_Mew'] > 0:
        pack_details += f"{emoji_MI_Mew} **Mew:** {pack_counter['MI_Mew']} instances\n"
        if pack_rollers['MI_Mew']:
            top_rollers = pack_rollers['MI_Mew'][:3]
            roller_text = '\n'.join([f"  • {roller['name']}: {round_to_one_decimal(roller['instances'])}" for roller in top_rollers])
            pack_details += f"{roller_text}\n\n"
        else:
            pack_details += "\n"
# Continue from the previous function (get_enhanced_selected_packs_embed_text)
    
    # Second generation packs
    if pack_counter['STS_Dialga'] > 0:
        pack_details += f"{emoji_STS_Dialga} **Dialga:** {pack_counter['STS_Dialga']} instances\n"
        if pack_rollers['STS_Dialga']:
            top_rollers = pack_rollers['STS_Dialga'][:3]
            roller_text = '\n'.join([f"  • {roller['name']}: {round_to_one_decimal(roller['instances'])}" for roller in top_rollers])
            pack_details += f"{roller_text}\n\n"
        else:
            pack_details += "\n"
    
    if pack_counter['STS_Palkia'] > 0:
        pack_details += f"{emoji_STS_Palkia} **Palkia:** {pack_counter['STS_Palkia']} instances\n"
        if pack_rollers['STS_Palkia']:
            top_rollers = pack_rollers['STS_Palkia'][:3]
            roller_text = '\n'.join([f"  • {roller['name']}: {round_to_one_decimal(roller['instances'])}" for roller in top_rollers])
            pack_details += f"{roller_text}\n\n"
        else:
            pack_details += "\n"
    
    if pack_counter['TL_Arceus'] > 0:
        pack_details += f"{emoji_TL_Arceus} **Arceus:** {pack_counter['TL_Arceus']} instances\n"
        if pack_rollers['TL_Arceus']:
            top_rollers = pack_rollers['TL_Arceus'][:3]
            roller_text = '\n'.join([f"  • {roller['name']}: {round_to_one_decimal(roller['instances'])}" for roller in top_rollers])
            pack_details += f"{roller_text}\n\n"
        else:
            pack_details += "\n"
    
    if pack_counter['SR_Giratina'] > 0:
        pack_details += f"{emoji_SR_Giratina} **Shining:** {pack_counter['SR_Giratina']} instances\n"
        if pack_rollers['SR_Giratina']:
            top_rollers = pack_rollers['SR_Giratina'][:3]
            roller_text = '\n'.join([f"  • {roller['name']}: {round_to_one_decimal(roller['instances'])}" for roller in top_rollers])
            pack_details += f"{roller_text}\n\n"
        else:
            pack_details += "\n"
    
    # Solgaleo and Lunala packs
    if pack_counter['SM_Solgaleo'] > 0:
        pack_details += f"{emoji_SM_Solgaleo} **Solgaleo:** {pack_counter['SM_Solgaleo']} instances\n"
        if pack_rollers['SM_Solgaleo']:
            top_rollers = pack_rollers['SM_Solgaleo'][:3]
            roller_text = '\n'.join([f"  • {roller['name']}: {round_to_one_decimal(roller['instances'])}" for roller in top_rollers])
            pack_details += f"{roller_text}\n\n"
        else:
            pack_details += "\n"
    
    if pack_counter['SM_Lunala'] > 0:
        pack_details += f"{emoji_SM_Lunala} **Lunala:** {pack_counter['SM_Lunala']} instances\n"
        if pack_rollers['SM_Lunala']:
            top_rollers = pack_rollers['SM_Lunala'][:3]
            roller_text = '\n'.join([f"  • {roller['name']}: {round_to_one_decimal(roller['instances'])}" for roller in top_rollers])
            pack_details += f"{roller_text}\n\n"
        else:
            pack_details += "\n"
    
    # No packs selected message
    if not pack_details:
        pack_details = "No packs currently selected by any active users."
    
    return pack_details
async def create_timeline_stats(bot, days: int = 7) -> discord.Embed:
    """Create timeline statistics embed."""
    # Calculate date range
    current_date = datetime.datetime.now()
    start_date = current_date - datetime.timedelta(days=days)
    
    # Prepare data structure for daily stats
    daily_stats = {}
    for i in range(days + 1):
        date = start_date + datetime.timedelta(days=i)
        date_key = date.strftime('%Y-%m-%d')  # YYYY-MM-DD format
        
        daily_stats[date_key] = {
            'active_users': 0,
            'total_instances': 0,
            'total_packs': 0,
            'gps_found': 0
        }
    
    # Get all users
    all_users = await get_all_users()
    
    # Process each user's data
    for user in all_users:
        user_id = get_id_from_user(user)
        
        # Get heartbeat history - this would need to be implemented in your system
        # Here we're just using the current values as placeholders
        session_packs = float(get_attrib_value_from_user(user, 'session_packs_opened', 0))
        last_hb_time_str = get_attrib_value_from_user(user, 'last_heartbeat_time', '0')
        instances = int(get_attrib_value_from_user(user, 'hb_instances', 0))
        
        # Skip users with no heartbeat
        if last_hb_time_str == '0':
            continue
            
        # Process heartbeat data
        try:
            last_hb_time = datetime.datetime.fromisoformat(last_hb_time_str.replace('Z', '+00:00'))
            
            # Skip entries outside date range
            if last_hb_time < start_date or last_hb_time > current_date:
                continue
                
            date_key = last_hb_time.strftime('%Y-%m-%d')
            
            # Skip if date not in range
            if date_key not in daily_stats:
                continue
                
            # Update stats for this day
            daily_stats[date_key]['active_users'] += 1
            daily_stats[date_key]['total_instances'] += instances
            daily_stats[date_key]['total_packs'] += session_packs
        except Exception as e:
            print(f"❌ Error processing timeline stats for user {user_id}: {e}")
    
    # Get godpack data
    live_gps = await get_server_data_gps('live_gps')
    
    # Count GPs by date
    if live_gps:
        for gp in live_gps:
            gp_time = get_time_from_gp(gp)
            if gp_time >= start_date and gp_time <= current_date:
                date_key = gp_time.strftime('%Y-%m-%d')
                if date_key in daily_stats:
                    daily_stats[date_key]['gps_found'] += 1
    
    # Create the embed
    timeline_embed = discord.Embed(
        title=f"Activity Timeline (Last {days} Days)",
        description="Daily activity statistics for the server",
        color=0x4b7bec
    )
    
    # Format the stats for each day
    days_array = sorted(daily_stats.keys())  # Sort dates chronologically
    
    # Group stats by time period
    timeline_text = ''
    days_to_show = min(days, 7)  # Limit to 7 days
    
    for i in range(len(days_array) - days_to_show, len(days_array)):
        date_key = days_array[i]
        stats = daily_stats[date_key]
        
        # Format date
        date = datetime.datetime.strptime(date_key, '%Y-%m-%d')
        formatted_date = date.strftime('%b %d, %a')  # e.g., "May 18, Sun"
        
        # Create activity indicators
        user_indicator = '👥' * min(stats['active_users'] // 5 + 1, 5)
        instance_indicator = '🖥️' * min(stats['total_instances'] // 20 + 1, 5)
        pack_indicator = '📦' * min(stats['total_packs'] // 5000 + 1, 5)
        gp_indicator = '✨' * min(stats['gps_found'], 5) if stats['gps_found'] > 0 else '❌'
        
        timeline_text += f"**{formatted_date}**\n"
        timeline_text += f"Users: {stats['active_users']} {user_indicator}\n"
        timeline_text += f"Instances: {stats['total_instances']} {instance_indicator}\n"
        timeline_text += f"Packs: {format_number_to_k(stats['total_packs'])} {pack_indicator}\n"
        timeline_text += f"GPs: {stats['gps_found']} {gp_indicator}\n\n"
    
# Calculate weekly summary
    total_weekly_users = max([stats['active_users'] for stats in daily_stats.values()])
    total_weekly_instances = sum([stats['total_instances'] for stats in daily_stats.values()])
    total_weekly_packs = sum([stats['total_packs'] for stats in daily_stats.values()])
    total_weekly_gps = sum([stats['gps_found'] for stats in daily_stats.values()])
    
    # Calculate averages
    avg_daily_instances = round_to_one_decimal(total_weekly_instances / days)
    avg_daily_packs = round_to_one_decimal(total_weekly_packs / days)
    
    # Set description and fields
    timeline_embed.description = timeline_text
    timeline_embed.add_field(name='📊 Period Summary', value=f"{days} Days")
    timeline_embed.add_field(name='👥 Max Users', value=f"{total_weekly_users}", inline=True)
    timeline_embed.add_field(name='🖥️ Avg Instances/Day', value=f"{avg_daily_instances}", inline=True)
    timeline_embed.add_field(name='📦 Avg Packs/Day', value=f"{format_number_to_k(avg_daily_packs)}", inline=True)
    timeline_embed.add_field(name='✨ Total GPs Found', value=f"{total_weekly_gps}", inline=True)
    
    return timeline_embed
async def send_stats(bot):
    """Send stats to the designated channel."""
    print("📝 Updating Stats...")
    
    try:
        guild = await get_guild(bot)
        
        # Clear previous messages in stats channel
        stats_channel = guild.get_channel(int(config.channel_id_user_stats))
        if not stats_channel:
            print(f"Warning: Stats channel {config.channel_id_user_stats} not found")
            return
            
        try:
            await bulk_delete_messages(stats_channel, 50)
        except Exception as e:
            print(f"Channel {stats_channel} does not support message purging: {e}")
        
        # Fetch guild members - fixed for async generator
        try:
            members = []
            async for member in guild.fetch_members():
                members.append(member)
        except Exception as e:
            print(f"Error fetching members: {e}")
            # Fallback to cached members if fetch_members fails
            members = guild.members
        
        # Get active and all users
        active_users = await get_active_users(True, True)
        all_users = await get_all_users()
        
        # Exit if no active users
        if not active_users:
            await stats_channel.send(content="No active rerollers found. Use `/active` to add yourself to the list.")
            return
        
        # Check AntiCheat status
        is_anti_cheat_on = False
        anti_cheat_verifier = ""
        
        if config.anti_cheat:
            try:
                recent_anti_cheat_messages = await get_lasts_anti_cheat_messages(bot)
                
                if len(recent_anti_cheat_messages.get('messages', [])) == math.floor(30 / config.anti_cheat_rate):
                    is_anti_cheat_on = True
                    member_anti_cheat_verifier = await get_member_by_id(bot, recent_anti_cheat_messages.get('prefix', ''))
                    
                    if not member_anti_cheat_verifier:
                        anti_cheat_verifier = "Unknown"
                        print(f"❗️ AntiCheat Verifier ID {recent_anti_cheat_messages.get('prefix', '')} is not registered on this server")
                    else:
                        anti_cheat_verifier = member_anti_cheat_verifier.display_name
            except Exception as e:
                print(f"Error checking AntiCheat status: {e}")
        
        # Get stats for active users
        active_users_infos = await get_users_stats(active_users, members, is_anti_cheat_on)
        
        # Send channel headers
        text_server_stats = localize("Stats Serveur", "Server Stats")
        text_user_stats = localize("Stats Rerollers Actifs", "Active Rerollers Stats")
        
        # Send SERVER STATS header
        server_stats_header = f"## {text_server_stats}:"
        await stats_channel.send(content=server_stats_header)
        
        # Send enhanced stats embeds
        stats_embeds = await create_enhanced_stats_embed(active_users, all_users)
        
        for embed in stats_embeds:
            await stats_channel.send(embed=embed)
            await asyncio.sleep(1.5)
        
        # Send SERVER RULES
        server_state = "```ansi\n"
        
        if config.anti_cheat:
            server_state += f"🛡️ Anti-Cheat : {color_text('ON', 'green') + color_text(f' Verified by {anti_cheat_verifier}', 'gray') if is_anti_cheat_on else color_text('OFF', 'red')}\n"
        
        server_state += f"💤 Auto Kick : {color_text('ON', 'green') if config.auto_kick else color_text('OFF', 'red')}\n"
        server_state += f"🩸 Leeching : {color_text('ON', 'green') if config.can_people_leech else color_text('OFF', 'red')}\n"
        server_state += "```"
        
        await stats_channel.send(content=server_state)
        await asyncio.sleep(1.5)
        
        # Send SELECTED PACKS
        pack_details_text = await get_enhanced_selected_packs_embed_text(bot, active_users)
        
        embed_selected_packs = discord.Embed(
            title="Instances / Selected Packs",
            description=pack_details_text,
            color=0xf02f7e
        )
        
        await stats_channel.send(embed=embed_selected_packs)
        await asyncio.sleep(1.5)
        
        # Send USER STATS
        await stats_channel.send(content=f"## {text_user_stats}:")
        
        # Send user stats in batches
        for user_info in active_users_infos:
            await stats_channel.send(content=user_info)
            await asyncio.sleep(1.5)
        
        # Send LEADERBOARDS if enough users
        if len(all_users) > 5:
            # Create and send miss rate and farming leaderboards
            leaderboards = await create_leaderboards(bot, all_users)
            miss_leaderboard, farm_leaderboard = leaderboards
            
            if miss_leaderboard:
                best_miss_embed, worst_miss_embed = miss_leaderboard
                await stats_channel.send(embed=best_miss_embed)
                await stats_channel.send(embed=worst_miss_embed)
            
            if farm_leaderboard:
                await stats_channel.send(embed=farm_leaderboard)
        
        print("☑️📝 Done updating Stats")
    except Exception as e:
        print(f"Error sending stats: {e}")
        import traceback
        traceback.print_exc()
async def create_leaderboards(bot, all_users: List[ET.Element]) -> Tuple[Optional[List[discord.Embed]], Optional[discord.Embed]]:
    """Create leaderboard embeds for miss rates and farming."""
    try:
        # Prepare arrays for miss rate and farming stats
        miss_count_array = []
        farm_info_array = []
        
        # Process all users
        for user in all_users:
            user_id = get_id_from_user(user)
            username = get_username_from_user(user)
            
            member = await get_member_by_id(bot, user_id)
            display_name = member.display_name if member else username
            
            # Process miss rate stats
            total_miss = int(get_attrib_value_from_user(user, 'total_miss', 0))
            total_time = float(get_attrib_value_from_user(user, 'total_time', 0))
            session_time = float(get_attrib_value_from_user(user, 'session_time', 0))
            total_time_hour = (total_time + session_time) / 60
            
            miss_per_24_hour = round_to_one_decimal((float(total_miss) / total_time_hour) * 24 if total_time_hour > 0 else 0)
            if math.isnan(miss_per_24_hour) or miss_per_24_hour == float('inf'):
                miss_per_24_hour = 0
            
            # Only include users with miss data
            if total_miss > 0 and total_time > 0:
                miss_count_array.append({'user': display_name, 'value': miss_per_24_hour})
            
            # Process farming stats
            total_time_farm = float(get_attrib_value_from_user(user, 'total_time_farm', 0))
            total_packs_farm = int(get_attrib_value_from_user(user, 'total_packs_farm', 0))
            
            # Only include users with farming data
            if total_time_farm > 0:
                farm_info_array.append({
                    'user': display_name,
                    'packs': total_packs_farm,
                    'time': total_time_farm,
                    'ppm': round_to_one_decimal(total_packs_farm / total_time_farm if total_time_farm > 0 else 0)
                })
        
        best_miss_embed = None
        worst_miss_embed = None
        farm_embed = None
        
        # Create farming leaderboard
        if len(farm_info_array) >= config.leaderboard_best_farm_length:
            # Sort by farming time (highest first)
            farm_info_array.sort(key=lambda x: x['time'], reverse=True)
            
            best_farmers_text = ""
            
            for i in range(min(config.leaderboard_best_farm_length, len(farm_info_array))):
                emoji_best_farm = None
                if i == 0:
                    emoji_best_farm = find_emoji(bot, config.leaderboard_best_farm1_custom_emoji_name, "🌟")
                elif i == 1:
                    emoji_best_farm = find_emoji(bot, config.leaderboard_best_farm2_custom_emoji_name, "⭐️")
                else:
                    emoji_best_farm = find_emoji(bot, config.leaderboard_best_farm3_custom_emoji_name, "✨")
                
                farmer = farm_info_array[i]
                best_farmers_text += f"{emoji_best_farm} **{farmer['user']}** - {round_to_one_decimal(farmer['time']/60)}h with {farmer['packs']} packs ({farmer['ppm']} ppm)\n\n"
            
            farm_embed = discord.Embed(
                title="Best Farmers",
                description=best_farmers_text,
                color=0x39d1bf
            )
        
        # Create miss rate leaderboards
        if len(miss_count_array) >= 6:
            emoji_best_verifier1 = find_emoji(bot, config.leaderboard_best_verifier1_custom_emoji_name, "🥇")
            emoji_best_verifier2 = find_emoji(bot, config.leaderboard_best_verifier2_custom_emoji_name, "🥈")
            emoji_best_verifier3 = find_emoji(bot, config.leaderboard_best_verifier3_custom_emoji_name, "🥉")
            
            emoji_worst_verifier1 = find_emoji(bot, config.leaderboard_worst_verifier1_custom_emoji_name, "😈")
            emoji_worst_verifier2 = find_emoji(bot, config.leaderboard_worst_verifier2_custom_emoji_name, "👿")
            emoji_worst_verifier3 = find_emoji(bot, config.leaderboard_worst_verifier3_custom_emoji_name, "💀")
            
            # Sort by highest miss rate first (worst verifiers)
            miss_count_array.sort(key=lambda x: x['value'], reverse=True)
            worst_miss_counts_text = f"""
{emoji_worst_verifier3} **{miss_count_array[0]['user']}** - {miss_count_array[0]['value']} miss / 24h

{emoji_worst_verifier2} **{miss_count_array[1]['user']}** - {miss_count_array[1]['value']} miss / 24h

{emoji_worst_verifier1} **{miss_count_array[2]['user']}** - {miss_count_array[2]['value']} miss / 24h
            """
            
            # Sort by lowest miss rate first (best verifiers)
            miss_count_array.sort(key=lambda x: x['value'])
            best_miss_counts_text = f"""
{emoji_best_verifier1} **{miss_count_array[0]['user']}** - {miss_count_array[0]['value']} miss / 24h

{emoji_best_verifier2} **{miss_count_array[1]['user']}** - {miss_count_array[1]['value']} miss / 24h

{emoji_best_verifier3} **{miss_count_array[2]['user']}** - {miss_count_array[2]['value']} miss / 24h
            """
            
            best_miss_embed = discord.Embed(
                title="Best Verifiers",
                description=best_miss_counts_text,
                color=0x5cd139
            )
            
            worst_miss_embed = discord.Embed(
                title="Bottom Verifiers",
                description=worst_miss_counts_text,
                color=0xd13939
            )
        
        return [
            [best_miss_embed, worst_miss_embed] if best_miss_embed and worst_miss_embed else None,
            farm_embed
        ]
    except Exception as e:
        print(f"❌ Error creating leaderboards: {e}")
        return [None, None]
async def send_ids(bot, update_server: bool = True):
    """Send active user IDs to the designated channel."""
    active_pocket_ids = await get_active_ids()
    
    text_content_of = localize("Contenu de IDs.txt", "Content of IDs.txt")
    text_active_pocket_ids = f"*{text_content_of} :*\n```\n{active_pocket_ids}\n```"
    
    # Send instances and IDs
    await send_channel_message(bot, config.channel_id_commands, text_active_pocket_ids, config.delay_msg_delete_state)
    
    if update_server:
        await update_gist(active_pocket_ids)

async def update_gp_tracking_list(bot):
    """Update the GP tracking list in the designated channel."""
    print("📝 Updating GP Tracking List...")
    
    guild = await get_guild(bot)
    tracking_channel = guild.get_channel(int(config.channel_id_gp_tracking_list))
    
    if not tracking_channel:
        print(f"⚠️ Warning: GP Tracking channel {config.channel_id_gp_tracking_list} not found")
        return
    
    # Clear previous tracking messages
    await bulk_delete_messages(tracking_channel, 10)
    
    # Prepare the message
    tracking_message = "✅ **Alive Packs** ✅\n"
    
    # Process each forum channel for GPs
    pack_forums = [
        config.channel_id_mewtwo_verification_forum,
        config.channel_id_charizard_verification_forum,
        config.channel_id_pikachu_verification_forum,
        config.channel_id_mew_verification_forum,
        config.channel_id_dialga_verification_forum,
        config.channel_id_palkia_verification_forum,
        config.channel_id_arceus_verification_forum,
        config.channel_id_shining_verification_forum,
        config.channel_id_solgaleo_verification_forum,
        config.channel_id_lunala_verification_forum
    ]
    
    # Track all GPs
    alive_gps = []
    testing_gps = []
    
    # Process all verification forums
    for forum_id in pack_forums:
        if not forum_id:
            continue
        
        try:
            forum = bot.get_channel(int(forum_id))
            if not forum:
                continue
            
            # Fetch active threads
            active_threads = await forum.archived_threads().flatten()
            
            # Process each thread
            for thread in active_threads:
                # Skip dead GPs
                if config.text_dead_logo in thread.name:
                    continue
                
                # Extract thread info
                clean_name = replace_any_logo_with(thread.name, "").strip()
                
                # Get pack type from forum name
                pack_type = ""
                if forum_id == config.channel_id_mewtwo_verification_forum:
                    pack_type = "Mewtwo"
                elif forum_id == config.channel_id_charizard_verification_forum:
                    pack_type = "Charizard"
                elif forum_id == config.channel_id_pikachu_verification_forum:
                    pack_type = "Pikachu"
                elif forum_id == config.channel_id_mew_verification_forum:
                    pack_type = "Mew"
                elif forum_id == config.channel_id_dialga_verification_forum:
                    pack_type = "Dialga"
                elif forum_id == config.channel_id_palkia_verification_forum:
                    pack_type = "Palkia"
                elif forum_id == config.channel_id_arceus_verification_forum:
                    pack_type = "Arceus"
                elif forum_id == config.channel_id_shining_verification_forum:
                    pack_type = "Shining"
                elif forum_id == config.channel_id_solgaleo_verification_forum:
                    pack_type = "Solgaleo"
                elif forum_id == config.channel_id_lunala_verification_forum:
                    pack_type = "Lunala"
                
                # Format display string
                name_parts = clean_name.split(' ')
                account_name = name_parts[0]
                
                # Extract and format the remaining parts
                remaining_info = clean_name[len(account_name):].strip()
                
                # Add [PackType] if not already in the name
                formatted_name = f"{account_name} {remaining_info}"
                if f"[{pack_type}]" not in formatted_name:
                    formatted_name = f"{account_name} {remaining_info}[{pack_type}]"
                
                # If the name doesn't end with [GP], add it for god packs
                if "god pack" in thread.name.lower() and not formatted_name.endswith("[GP]"):
                    formatted_name += "[GP]"
                
                # Check if alive or testing
                if config.text_verified_logo in thread.name:
                    alive_gps.append({
                        'name': formatted_name,
                        'thread_id': thread.id
                    })
                else:
                    testing_gps.append({
                        'name': formatted_name,
                        'thread_id': thread.id
                    })
        except Exception as e:
            print(f"⚠️ Error processing forum {forum_id}: {e}")
    
    # Sort alphabetically by account name
    alive_gps.sort(key=lambda x: x['name'])
    testing_gps.sort(key=lambda x: x['name'])
    
    # Format alive GPs
    for gp in alive_gps:
        tracking_message += f"**[`[Alive]`](https://discord.com/channels/{config.guild_id}/{gp['thread_id']}) {gp['name']}**\n"
    
    if not alive_gps:
        tracking_message += "No alive GPs currently tracked.\n"
    
    # Add testing packs section
    tracking_message += "\n🍀 **Testing Packs** 🍀\n"
    
    # Format testing GPs
    for gp in testing_gps:
        tracking_message += f"**[`[Testing]`](https://discord.com/channels/{config.guild_id}/{gp['thread_id']}) {gp['name']}**\n"
    
    if not testing_gps:
        tracking_message += "No testing GPs currently tracked.\n"
    
    # Send the tracking message
    await tracking_channel.send(content=tracking_message)
    
    print("✅ GP Tracking List updated successfully")
async def update_inactive_gps(bot):
    """Mark inactive GPs as dead and close their threads."""
    text_start = "🔍 Checking Inactive GPs..."
    text_done = "☑️🔍 Finished checking Inactive GPs"
    print(text_start)
    
    # Get all pack-specific forums
    pack_forums = [
        config.channel_id_mewtwo_verification_forum,
        config.channel_id_charizard_verification_forum,
        config.channel_id_pikachu_verification_forum,
        config.channel_id_mew_verification_forum,
        config.channel_id_dialga_verification_forum,
        config.channel_id_palkia_verification_forum,
        config.channel_id_arceus_verification_forum,
        config.channel_id_shining_verification_forum,
        config.channel_id_solgaleo_verification_forum,
        config.channel_id_lunala_verification_forum
    ]
    
    removed_thread_count = 0
    
    # Process each forum channel
    for forum_id in pack_forums:
        if not forum_id:
            continue
        
        try:
            forum = bot.get_channel(int(forum_id))
            if not forum:
                print(f"⚠️ Warning: Forum channel {forum_id} not found")
                continue
            
            # Get active threads
            active_threads = await forum.archived_threads().flatten()
            
            for thread in active_threads:
                # Calculate thread age in hours
                thread_age_hours = (datetime.datetime.now(datetime.timezone.utc) - thread.created_at).total_seconds() / 3600
                
                # Check if thread is older than closure thresholds
                should_close = False
                
                if config.text_verified_logo in thread.name and thread_age_hours > config.auto_close_live_post_time:
                    should_close = True
                elif config.text_verified_logo not in thread.name and thread_age_hours > config.auto_close_not_live_post_time:
                    should_close = True
                elif config.text_dead_logo in thread.name:
                    should_close = True
                
                if should_close:
                    # Mark as dead if not already dead or verified
                    if config.text_dead_logo not in thread.name and config.text_verified_logo not in thread.name:
                        new_thread_name = replace_any_logo_with(thread.name, config.text_dead_logo)
                        await thread.edit(name=new_thread_name)
                        await asyncio.sleep(1)
                    
                    # Close the thread
                    await thread.archive()
                    print(f"🔒 Closed thread: {thread.name} (ID: {thread.id})")
                    
                    removed_thread_count += 1
        except Exception as e:
            print(f"⚠️ Warning: Error processing forum {forum_id}: {e}")
    
    # Also check double star forum
    if config.channel_id_2star_verification_forum:
        try:
            forum = bot.get_channel(int(config.channel_id_2star_verification_forum))
            if forum:
                active_threads = await forum.archived_threads().flatten()
                
                for thread in active_threads:
                    # Calculate thread age in hours
                    thread_age_hours = (datetime.datetime.now(datetime.timezone.utc) - thread.created_at).total_seconds() / 3600
                    
                    # Check if thread is older than closure thresholds
                    should_close = False
                    
                    if config.text_verified_logo in thread.name and thread_age_hours > config.auto_close_live_post_time:
                        should_close = True
                    elif config.text_verified_logo not in thread.name and thread_age_hours > config.auto_close_not_live_post_time:
                        should_close = True
                    elif config.text_dead_logo in thread.name:
                        should_close = True
                    
                    if should_close:
                        # Mark as dead if not already dead or verified
                        if config.text_dead_logo not in thread.name and config.text_verified_logo not in thread.name:
                            new_thread_name = replace_any_logo_with(thread.name, config.text_dead_logo)
                            await thread.edit(name=new_thread_name)
                            await asyncio.sleep(1)
                        
                        # Close the thread
                        await thread.archive()
                        print(f"🔒 Closed thread: {thread.name} (ID: {thread.id})")
                        
                        removed_thread_count += 1
        except Exception as e:
            print(f"⚠️ Warning: Error processing double star forum: {e}")
    
    print(text_done)
    
    if removed_thread_count > 0:
        await update_eligible_ids(bot)
        # Update GP tracking list after archiving inactive GPs
        await update_gp_tracking_list(bot)
async def update_eligible_ids(bot):
    """Update the eligible IDs list based on forum threads."""
    text_start = "📜 Updating Eligible IDs..."
    text_done = "☑️📜 Finished updating Eligible IDs"
    print(text_start)
    
    # Get all pack-specific forums
    pack_forums = [
        config.channel_id_mewtwo_verification_forum,
        config.channel_id_charizard_verification_forum,
        config.channel_id_pikachu_verification_forum,
        config.channel_id_mew_verification_forum,
        config.channel_id_dialga_verification_forum,
        config.channel_id_palkia_verification_forum,
        config.channel_id_arceus_verification_forum,
        config.channel_id_shining_verification_forum,
        config.channel_id_solgaleo_verification_forum,
        config.channel_id_lunala_verification_forum
    ]
    
    id_list = ""
    
    # Process each forum channel
    for forum_id in pack_forums:
        if not forum_id:
            continue
        
        try:
            forum = bot.get_channel(int(forum_id))
            if not forum:
                print(f"⚠️ Warning: Forum channel {forum_id} not found")
                continue
            
            # Get active threads
            active_threads = await forum.archived_threads().flatten()
            
            for thread in active_threads:
                # Check if post contains any validation logo
                if (config.text_not_liked_logo in thread.name or
                    config.text_waiting_logo in thread.name or
                    config.text_liked_logo in thread.name or
                    config.text_verified_logo in thread.name):
                    
                    initial_message = await get_oldest_message(thread)
                    if not initial_message or not initial_message.content:
                        continue
                    
                    content_split = initial_message.content.split('\n')
                    
                    clean_thread_name = replace_any_logo_with(thread.name, "")
                    gp_pocket_name = clean_thread_name.split(" ")[1] if len(clean_thread_name.split(" ")) > 1 else "Unknown"
                    
                    gp_two_star_count = "5/5"  # Default
                    if not config.safe_eligible_ids_filtering:
                        gp_two_star_count_array = re.findall(r'\[(\d+\/\d+)\]', clean_thread_name)
                        gp_two_star_count = gp_two_star_count_array[0] if gp_two_star_count_array else "5/5"
                    
                    gp_pocket_id_line = next((line for line in content_split if 'ID:' in line), None)
                    
                    if gp_pocket_id_line:
                        id_list += f"{gp_pocket_id_line.replace('ID:', '').strip()} | {gp_pocket_name} | {gp_two_star_count}\n"
            
            # Also check archived threads
            try:
                archived_threads = await forum.archived_threads().flatten()
                
                for thread in archived_threads:
                    # Check if post contains verified logo
                    if config.text_verified_logo in thread.name:
                        
                        initial_message = await get_oldest_message(thread)
                        if not initial_message or not initial_message.content:
                            continue
                        
                        content_split = initial_message.content.split('\n')
                        
                        clean_thread_name = replace_any_logo_with(thread.name, "")
                        gp_pocket_name = clean_thread_name.split(" ")[1] if len(clean_thread_name.split(" ")) > 1 else "Unknown"
                        
                        gp_two_star_count = "5/5"  # Default
                        if not config.safe_eligible_ids_filtering:
                            gp_two_star_count_array = re.findall(r'\[(\d+\/\d+)\]', clean_thread_name)
                            gp_two_star_count = gp_two_star_count_array[0] if gp_two_star_count_array else "5/5"
                        
                        gp_pocket_id_line = next((line for line in content_split if 'ID:' in line), None)
                        
                        if gp_pocket_id_line:
                            id_list += f"{gp_pocket_id_line.replace('ID:', '').strip()} | {gp_pocket_name} | {gp_two_star_count}\n"
            except Exception as e:
                print(f"⚠️ Warning: Error fetching archived threads from {forum_id}: {e}")
        except Exception as e:
            print(f"⚠️ Warning: Error processing forum {forum_id}: {e}")
    
    # Process double star threads
    if config.channel_id_2star_verification_forum and config.add_double_star_to_vip_ids_txt:
        try:
            double_star_forum = bot.get_channel(int(config.channel_id_2star_verification_forum))
            if double_star_forum:
                double_star_threads = await double_star_forum.archived_threads().flatten()
                
                for thread in double_star_threads:
                    # Check if post contains any validation logo
                    if (config.text_not_liked_logo in thread.name or
                        config.text_waiting_logo in thread.name or
                        config.text_liked_logo in thread.name or
                        config.text_verified_logo in thread.name):
                        
                        initial_message = await get_oldest_message(thread)
                        if not initial_message or not initial_message.content:
                            continue
                        
                        content_split = initial_message.content.split('\n')
                        
                        clean_double_star_thread_name = replace_any_logo_with(thread.name, "")
                        double_star_pocket_name = clean_double_star_thread_name.split(" ")[1] if len(clean_double_star_thread_name.split(" ")) > 1 else "Unknown"
                        double_star_count = "5/5"  # Default
                        
                        if not config.safe_eligible_ids_filtering:
                            double_star_count_array = re.findall(r'\[(\d+\/\d+)\]', clean_double_star_thread_name)
                            double_star_count = double_star_count_array[0] if double_star_count_array else "5/5"
                        
                        double_star_pocket_id_line = next((line for line in content_split if 'ID:' in line), None)
                        
                        if double_star_pocket_id_line:
                            id_list += f"{double_star_pocket_id_line.replace('ID:', '').strip()} | {double_star_pocket_name} | {double_star_count}\n"
        except Exception as e:
            print(f"⚠️ Warning: Error processing double star forum: {e}")
    
    print(text_done)
    
    # Update the gist with the compiled list
    await update_gist(id_list, config.git_gist_gp_name)

async def mark_as_dead(bot, interaction, optional_text=""):
    """Mark a godpack as dead."""
    text_mark_as_dead = localize("Godpack marqué comme mort", "Godpack marked as dud")
    text_already_dead = localize("Il est déjà mort et enterré... tu veux vraiment en remettre une couche ?", "It's already dead and buried...")
    
    thread = bot.get_channel(interaction.channel_id)
    
    if config.text_dead_logo in thread.name:
        await send_received_message(text_already_dead, interaction)
    else:
        new_thread_name = replace_any_logo_with(thread.name, config.text_dead_logo)
        
        await thread.edit(name=new_thread_name)
        
        await send_received_message(optional_text + config.text_dead_logo + " " + text_mark_as_dead, interaction)
        
        await update_eligible_ids(bot)
        
        # Update GP tracking list after marking as dead
        await update_gp_tracking_list(bot)

async def set_user_state(bot, user, state, interaction=None):
    """Set a user's state (active, inactive, farm, leech)."""
    text_missing_friend_code = localize("Le Player ID est nécéssaire avant quoi que ce soit", "The Player ID is needed before anything")
    
    user_id = str(user.id)
    username = user.name
    user_display_name = user.display_name
    
    if await does_user_profile_exists(user_id, username):
        if await get_user_attrib_value(user_id, 'pocket_id') is None:
            return await send_received_message(text_missing_friend_code, interaction, config.delay_msg_delete_state)
    else:
        return await send_received_message(text_missing_friend_code, interaction, config.delay_msg_delete_state)
    
    is_player_active = await get_user_attrib_value(user_id, 'user_state')
    
    if state == "active":
        text_already_in = localize("est déjà présent dans la liste des rerollers actifs", "is already in the active rerollers pool")
        
        # Skip if player already active
        if is_player_active != "active":
            print(f"➕ Added {username}")
            await set_user_attrib_value(user_id, username, 'user_state', "active")
            await set_user_attrib_value(user_id, username, 'last_active_time', datetime.datetime.now().isoformat())
            await send_received_message(f"```ansi\n{color_text('+ ' + user_display_name, 'green')} as active\n```", interaction, 0)
            # Send the list of IDs
            await send_ids(bot)
            return
        else:
            await send_received_message(f"**<@{user_id}>** " + text_already_in, interaction, config.delay_msg_delete_state)
            return
    
    elif state == "inactive":
        text_already_out = localize("est déjà absent de la liste des rerollers actifs", "is already out of the active rerollers pool")
        
        if is_player_active != "inactive":
            print(f"➖ Removed {username}")
            await set_user_attrib_value(user_id, username, 'user_state', "inactive")
            await send_received_message(f"```ansi\n{color_text('- ' + user_display_name, 'red')} as inactive\n```", interaction, 0)
            # Send the list of IDs
            await send_ids(bot)
            return
        else:
            await send_received_message(f"**<@{user_id}>** " + text_already_out, interaction, config.delay_msg_delete_state)
            return
    
    elif state == "farm":
        text_already_out = localize("est déjà listé comme farmer", "is already listed as farmer")
        
        if is_player_active != "farm":
            print(f"⚡️ Farm {username}")
            await set_user_attrib_value(user_id, username, 'user_state', "farm")
            await send_received_message(f"```ansi\n{color_text('+ ' + user_display_name, 'cyan')} as farmer\n```", interaction, 0)
            # Send the list of IDs
            await send_ids(bot)
            return
        else:
            await send_received_message(f"**<@{user_id}>** " + text_already_out, interaction, config.delay_msg_delete_state)
            return
    
    elif state == "leech":
        if not config.can_people_leech:
            text_no_leech = localize("Le leech est désactivé sur ce serveur", "Leeching is disabled on this server")
            await send_received_message(text_no_leech, interaction, config.delay_msg_delete_state)
            return
        
        text_no_req_gp = localize("ne peut pas leech car il a moins de", "can't leech because he got less than")
        text_no_req_packs = localize("et moins de", "and less than")
        gp_gp_count = int(await get_user_attrib_value(user_id, 'god_pack_found', 0))
        gp_pack_count = int(await get_user_attrib_value(user_id, 'total_packs_opened', 0))
        
        if gp_gp_count < config.leech_perm_gp_count and gp_pack_count < config.leech_perm_pack_count:
            await send_received_message(f"**<@{user_id}>** {text_no_req_gp} {config.leech_perm_gp_count}gp {text_no_req_packs} {config.leech_perm_pack_count}packs", interaction, config.delay_msg_delete_state)
            return
        
        text_already_out = localize("est déjà listé comme leecher", "is already listed as leecher")
        
        if is_player_active != "leech":
            print(f"🩸 Leech {username}")
            await set_user_attrib_value(user_id, username, 'user_state', "leech")
            await send_received_message(f"```ansi\n{color_text('+ ' + user_display_name, 'pink')} as leecher\n```", interaction, 0)
            # Send the list of IDs
            await send_ids(bot)
            return
        else:
            await send_received_message(f"**<@{user_id}>** " + text_already_out, interaction, config.delay_msg_delete_state)
            return
    
    else:
        await send_received_message(f"Failed to update the state of user **<@{user_id}>** to {state}", interaction, config.delay_msg_delete_state)
        return
async def update_server_data(bot, startup=False):
    """Update and reset the server's GP stats data."""
    server_data_exist = check_file_exists(path_server_data)
    
    # Only check if file is <4h at startup
    if server_data_exist and startup:
        file_modification_date = datetime.datetime.fromtimestamp(os.path.getmtime(path_server_data))
        date_limit = datetime.datetime.now() - datetime.timedelta(minutes=config.reset_server_data_time)
        
        # If file modified less than X minutes ago, return
        if file_modification_date > date_limit:
            print("⏭️ Skipping GP stats reset, already fresh")
            return
    
    if not server_data_exist or config.reset_server_data_frequently:
        text_start = "🔄 Analyze & Reset all GP stats to ServerData.xml..."
        text_done = "☑️🔄 Finished Analyze & Reset all GP stats"
        print(text_start)
        
        # Default XML Structure
        root = ET.Element("root")
        
        # Create GP elements
        live_gps = ET.SubElement(root, "live_gps")
        eligible_gps = ET.SubElement(root, "eligible_gps")
        ineligible_gps = ET.SubElement(root, "ineligible_gps")
        
        # Get all pack-specific forums
        pack_forums = [
            config.channel_id_mewtwo_verification_forum,
            config.channel_id_charizard_verification_forum,
            config.channel_id_pikachu_verification_forum,
            config.channel_id_mew_verification_forum,
            config.channel_id_dialga_verification_forum,
            config.channel_id_palkia_verification_forum,
            config.channel_id_arceus_verification_forum,
            config.channel_id_shining_verification_forum,
            config.channel_id_solgaleo_verification_forum,
            config.channel_id_lunala_verification_forum
        ]
        
        # Process each forum channel
        for forum_id in pack_forums:
            if not forum_id:
                continue
            
            try:
                forum_channel = bot.get_channel(int(forum_id))
                if not forum_channel:
                    print(f"⚠️ Warning: Forum channel {forum_id} not found")
                    continue
                
                active_threads = await forum_channel.archived_threads().flatten()
                archived_threads = []
                
                # Fetch all archived threads (paginate if needed)
                before = None
                has_more = True
                
                while has_more:
                    try:
                        fetched = await forum_channel.archived_threads(limit=100, before=before)
                        fetched_threads = fetched.threads
                        archived_threads.extend(fetched_threads)
                        has_more = len(fetched_threads) == 100
                        if has_more and fetched_threads:
                            before = fetched_threads[-1].id
                    except Exception as e:
                        print(f"⚠️ Warning: Error fetching archived threads from {forum_id}: {e}")
                        has_more = False
                
                all_threads = active_threads + archived_threads
                
                # Process all threads in this forum
                for thread in all_threads:
                    # Check if post name contains no logo, in that case skip post
                    if not any(logo in thread.name for logo in [
                            config.text_verified_logo,
                            config.text_not_liked_logo,
                            config.text_waiting_logo,
                            config.text_liked_logo,
                            config.text_dead_logo
                        ]):
                        continue
                    
                    # Add to eligible GPs
                    eligible_gp = ET.SubElement(eligible_gps, "eligible_gp")
                    eligible_gp.set("time", thread.created_at.isoformat())
                    eligible_gp.set("name", thread.name)
                    eligible_gp.text = str(thread.id)
                    
                    # If verified, also add to live GPs
                    if config.text_verified_logo in thread.name:
                        live_gp = ET.SubElement(live_gps, "live_gp")
                        live_gp.set("time", thread.created_at.isoformat())
                        live_gp.set("name", thread.name)
                        live_gp.text = str(thread.id)
            except Exception as e:
                print(f"⚠️ Warning: Error processing forum {forum_id}: {e}")
        
        # Also check double star forum
        if config.channel_id_2star_verification_forum:
            try:
                forum_channel = bot.get_channel(int(config.channel_id_2star_verification_forum))
                if forum_channel:
                    active_threads = await forum_channel.archived_threads().flatten()
                    archived_threads = []
                    
                    # Fetch all archived threads (paginate if needed)
                    before = None
                    has_more = True
                    
                    while has_more:
                        try:
                            fetched = await forum_channel.archived_threads(limit=100, before=before)
                            fetched_threads = fetched.threads
                            archived_threads.extend(fetched_threads)
                            has_more = len(fetched_threads) == 100
                            if has_more and fetched_threads:
                                before = fetched_threads[-1].id
                        except Exception as e:
                            print(f"⚠️ Warning: Error fetching archived threads from 2Star forum: {e}")
                            has_more = False
                    
                    all_threads = active_threads + archived_threads
                    
                    # Process all threads
                    for thread in all_threads:
                        # Check if post name contains no logo, in that case skip post
                        if not any(logo in thread.name for logo in [
                                config.text_verified_logo,
                                config.text_not_liked_logo,
                                config.text_waiting_logo,
                                config.text_liked_logo,
                                config.text_dead_logo
                            ]):
                            continue
                        
                        # Add to eligible GPs
                        eligible_gp = ET.SubElement(eligible_gps, "eligible_gp")
                        eligible_gp.set("time", thread.created_at.isoformat())
                        eligible_gp.set("name", thread.name)
                        eligible_gp.text = str(thread.id)
                        
                        # If verified, also add to live GPs
                        if config.text_verified_logo in thread.name:
                            live_gp = ET.SubElement(live_gps, "live_gp")
                            live_gp.set("time", thread.created_at.isoformat())
                            live_gp.set("name", thread.name)
                            live_gp.text = str(thread.id)
            except Exception as e:
                print(f"⚠️ Warning: Error processing 2Star forum: {e}")
        
        # Get all ineligible posts in Webhook channel
        try:
            webhook_channel = bot.get_channel(int(config.channel_id_webhook))
            
            last_message_id = None
            fetch_more = True
            
            while fetch_more:
                options = {"limit": 100}
                if last_message_id:
                    options["before"] = last_message_id
                
                messages = await webhook_channel.history(**options).flatten()
                
                if not messages:
                    break
                
                for message in messages:
                    if message.author.bot and "invalid" in message.content.lower():
                        ineligible_gp = ET.SubElement(ineligible_gps, "ineligible_gp")
                        ineligible_gp.set("time", message.created_at.isoformat())
                        ineligible_gp.set("name", message.content)
                        ineligible_gp.text = str(message.id)
                
                # Update the last message ID for the next batch
                last_message_id = messages[-1].id
                
                # Stop fetching if fewer than 100 messages are returned
                fetch_more = len(messages) == 100
        except Exception as e:
            print(f"⚠️ Warning: Error processing webhook channel: {e}")
        
        # Write the XML to file
        tree = ET.ElementTree(root)
        tree.write(path_server_data, encoding="utf-8", xml_declaration=True)
        
        print(text_done)
        
        # Send Users Data to GitGist
        if config.output_user_data_on_git_gist:
            try:
                with open(path_users_data, 'r', encoding='utf-8') as f:
                    data = f.read()
                await update_gist(data, "UsersData")
            except Exception as e:
                print(f"❌ ERROR trying to read file at {path_users_data}: {e}")
        
        await update_user_data_gp_live(bot)
        
        # Update GP tracking list
        await update_gp_tracking_list(bot)

async def update_anti_cheat(bot):
    """Update AntiCheat stats for users based on recent messages."""
    try:
        recent_anti_cheat_messages = (await get_lasts_anti_cheat_messages(bot)).get('messages', [])
        
        if len(recent_anti_cheat_messages) == math.floor(30 / config.anti_cheat_rate):
            text_start = "🛡️ AntiCheat Analyzing..."
            text_done = "☑️🛡️ Finished AntiCheat Analyzing"
            print(text_start)
            
            array_usernames = ' '.join([msg.content for msg in recent_anti_cheat_messages]).split(',')
            
            all_users = await get_active_users()
            for user in all_users:
                user_id = get_id_from_user(user)
                member = await get_member_by_id(bot, user_id)
                
                # Skip if member does not exist
                if not member:
                    print(f"❗️ User {user_id} is not registered on this server")
                    continue
                
                username = member.name
                user_prefix = await get_user_attrib_value(user_id, 'prefix', "NoPrefixFound")
                anti_cheat_user_count = 0
                anti_cheat_user_names = []
                
                for username_check in array_usernames:
                    normalized_user_prefix = normalize_ocr(user_prefix).upper()
                    normalized_username = normalize_ocr(username_check).upper()
                    
                    if normalized_username.startswith(normalized_user_prefix):
                        anti_cheat_user_count += 1
                        anti_cheat_user_names.append(username_check)
                
                await set_user_attrib_value(user_id, username, 'anti_cheat_user_count', anti_cheat_user_count)
                # Debug Usernames
                # if anti_cheat_user_names:
                #     await send_channel_message(bot, "XXXXXXXXXXXXXXXXXXX", username + "\n" + ', '.join(anti_cheat_user_names))
            
            print(text_done)
        else:
            print("🛡️🚫 AntiCheat is OFF")
    except Exception as e:
        print(f'❌ ERROR - Trying to Analyze for AntiCheat:\n{e}')
async def update_user_data_gp_live(bot):
    """Update users' GPLive count based on server data."""
    text_start = "🟢 Updating GPLive UserData..."
    text_done = "☑️🟢 Finished updating GPLive UserData"
    print(text_start)
    
    # Reset all user GP live counts
    all_users = await get_all_users()
    for user in all_users:
        user_id = get_id_from_user(user)
        username = get_username_from_user(user)
        await set_user_attrib_value(user_id, username, 'god_pack_live', 0)
    
    try:
        live_gps = await get_server_data_gps('live_gps')
        
        # Create an array of GP data
        live_gp_array = []
        for live_gp in live_gps:
            live_gp_array.append({
                'time': live_gp.get('time'),
                'name': live_gp.get('name'),
                'id': live_gp.text
            })
        
        # Get all pack-specific forums
        pack_forums = [
            config.channel_id_mewtwo_verification_forum,
            config.channel_id_charizard_verification_forum,
            config.channel_id_pikachu_verification_forum,
            config.channel_id_mew_verification_forum,
            config.channel_id_dialga_verification_forum,
            config.channel_id_palkia_verification_forum,
            config.channel_id_arceus_verification_forum,
            config.channel_id_shining_verification_forum,
            config.channel_id_solgaleo_verification_forum,
            config.channel_id_lunala_verification_forum,
            config.channel_id_2star_verification_forum
        ]
        
        # Process each live GP
        for live_gp in live_gp_array:
            # Try to find this thread in any forum
            thread_found = False
            
            for forum_id in pack_forums:
                if not forum_id:
                    continue
                
                try:
                    verification_channel = bot.get_channel(int(forum_id))
                    if not verification_channel:
                        continue
                    
                    try:
                        thread = await verification_channel.fetch_thread(int(live_gp['id']))
                        if thread:
                            # Found the thread, update user's GPLive count
                            await add_user_data_gp_live(bot, thread)
                            thread_found = True
                            break
                    except Exception:
                        # Thread not in this forum, continue searching
                        pass
                except Exception as e:
                    print(f"⚠️ Warning: Error accessing forum {forum_id}: {e}")
            
            if not thread_found:
                print(f"⚠️ Warning: Could not find thread {live_gp['id']} in any forum channel")
        
        print(text_done)
    except Exception as e:
        print(f"❌ ERROR - Failed to update UserData GPLive\n{e}")

async def add_user_data_gp_live(bot, thread):
    """Increment the GP live count for a user based on a thread."""
    initial_message = await get_oldest_message(thread)
    if not initial_message or not initial_message.content:
        return
    
    # Extract owner ID from the first message
    owner_id_parts = re.findall(r'<@(\d+)>', initial_message.content)
    if not owner_id_parts:
        print(f"❗️ Failed to extract owner ID from thread {thread.id}")
        return
    
    owner_id = owner_id_parts[0]
    
    member = await get_member_by_id(bot, owner_id)
    if not member:
        print(f"❗️ Failed to update UserData GPLive of thread ID: {thread.id}\nFor more info, check the ID in ServerData.xml")
        return
    
    gp_live = int(await get_user_attrib_value(owner_id, 'god_pack_live', 0))
    await set_user_attrib_value(owner_id, member.name, 'god_pack_live', gp_live + 1)

def extract_gp_info(message):
    """Extract godpack information from a webhook message."""
    print(f"----- Extracting GP info from: {message.content[:100]}... -----")
    
    # Define regex patterns
    regex_owner_id = r'<@(\d+)>'
    regex_account_name = r'^(\S+)'
    regex_account_id = r'\((\d+)\)'
    regex_two_star_ratio = r'\[(\d)\/\d\]'
    regex_pack_amount = r'\[(\d+)P\]'
    regex_pack_booster_type = r'\[\d+P\]\s*([^\]]+)\s*\]'
    
    print("Applying regex patterns...")
    
    # Extract the data using regex
    owner_id_match = re.search(regex_owner_id, message.content)
    
    # For account name, need to get second line
    message_lines = message.content.split('\n')
    account_name_match = re.search(regex_account_name, message_lines[1] if len(message_lines) > 1 else '')
    
    account_id_match = re.search(regex_account_id, message.content)
    two_star_ratio_match = re.search(regex_two_star_ratio, message.content)
    pack_amount_match = re.search(regex_pack_amount, message.content)
    pack_booster_type_match = re.search(regex_pack_booster_type, message.content)
    
    # Log the results
    print(f"- Owner ID match: {owner_id_match[0] if owner_id_match else 'NO MATCH'}")
    print(f"- Account Name match: {account_name_match[0] if account_name_match else 'NO MATCH'}")
    print(f"- Account ID match: {account_id_match[0] if account_id_match else 'NO MATCH'}")
    print(f"- Two Star Ratio match: {two_star_ratio_match[0] if two_star_ratio_match else 'NO MATCH'}")
    print(f"- Pack Amount match: {pack_amount_match[0] if pack_amount_match else 'NO MATCH'}")
    print(f"- Pack Booster Type match: {pack_booster_type_match[0] if pack_booster_type_match else 'NO MATCH'}")
    
    # Get the values or defaults
    owner_id = owner_id_match[1] if owner_id_match else "0000000000000000"
    account_name = account_name_match[1] if account_name_match else "NoAccountName"
    account_id = account_id_match[1] if account_id_match else "0000000000000000"
    two_star_ratio = two_star_ratio_match[1] if two_star_ratio_match else "0"
    pack_amount = pack_amount_match[1] if pack_amount_match else "0"
    pack_booster_type = pack_booster_type_match[1] if pack_booster_type_match else "NoPackBoosterType"
    
    print(f"----- Extracted GP info - Name: {account_name}, ID: {account_id}, Ratio: {two_star_ratio}, Amount: {pack_amount}, Type: {pack_booster_type} -----")
    
    return {
        'owner_id': owner_id,
        'account_name': account_name,
        'account_id': account_id,
        'two_star_ratio': two_star_ratio,
        'pack_amount': pack_amount,
        'pack_booster_type': pack_booster_type
    }

def extract_double_star_info(message):
    """Extract double star pack information from a webhook message."""
    try:
        # Define regex patterns
        regex_owner_id = r'<@(\d+)>'
        regex_account_name = r'found by (\S+)'
        regex_account_id = r'\((\d+)\)'
        regex_pack_amount = r'\((\d+) packs'
        
        # Extract the data using regex
        owner_id_match = re.search(regex_owner_id, message.content)
        account_name_match = re.search(regex_account_name, message.content)
        account_id_match = re.search(regex_account_id, message.content)
        pack_amount_match = re.search(regex_pack_amount, message.content)
        
        # Get the values or defaults
        owner_id = owner_id_match[1] if owner_id_match else "0000000000000000"
        account_name = account_name_match[1] if account_name_match else "NoAccountName"
        account_id = account_id_match[1] if account_id_match else "0000000000000000"
        pack_amount = pack_amount_match[1] if pack_amount_match else "0"
        
        print(f"Extracted info - OwnerID: {owner_id}, AccountName: {account_name}, AccountID: {account_id}, PackAmount: {pack_amount}")
        
        return {
            'owner_id': owner_id,
            'account_name': account_name,
            'account_id': account_id,
            'pack_amount': pack_amount
        }
    except Exception as e:
        print(f"❌ ERROR - Failed to extract double star info for message: {message.content}\n{e}")
        return {
            'owner_id': "0000000000000000",
            'account_name': "NoAccountName",
            'account_id': "0000000000000000",
            'pack_amount': "0"
        }
async def create_forum_post(bot, message, channel_id, pack_name, title_name, owner_id, account_id, pack_amount, pack_booster_type=""):
    """Create a forum post for a godpack or double star pack."""
    try:
        print(f"Creating forum post for {pack_name} ({pack_booster_type}) in channel {channel_id}")
        guild = await get_guild(bot)
        
        # Verify channel exists
        forum = bot.get_channel(int(channel_id))
        if not forum:
            print(f"Error: Channel {channel_id} not found!")
            return
        print(f"Found forum channel: {forum.name}")
        
        # Prepare message content
        text_verification_redirect = localize("Verification ici :", "Verification link here :")
        text_found_by = localize(f"{pack_name} trouvé par", f"{pack_name} found by")
        text_command_tooltip = localize(
            "Écrivez **/miss** si un autre est apparu ou que vous ne l'avez pas\n**/verified** ou **/dead** pour changer l'état du post",
            "Write **/miss** if another one appeared or you didn't see it\n**/verified** or **/dead** to change post state"
        )
        text_eligible = localize("**Éligibles :**", "**Eligible :**")
        
# Get member
        member = await get_member_by_id(bot, owner_id)
        if not member:
            print(f"Error: Member with ID {owner_id} not found on server")
            return
        owner_username = member.name
        
        # Increment God Pack counter for actual God Packs
        if pack_name == "GodPack":
            god_pack_found = int(await get_user_attrib_value(owner_id, 'god_pack_found', 0))
            await set_user_attrib_value(owner_id, owner_username, 'god_pack_found', god_pack_found + 1)
        
        # Get image URL
        image_url = message.attachments[0].url if message.attachments else None
        
        # Get all active users for tagging
        active_users = await get_active_users(False, True)
        active_user_ids = [get_id_from_user(user) for user in active_users]
        tag_active_usernames = ''.join([f"<@{user_id}>" for user_id in active_user_ids])
        
        print(f"Creating webhook thread for {pack_name}")
        # Create thread in webhook channel
        try:
            thread = await message.create_thread(
                name=text_verification_redirect,
                auto_archive_duration=60  # Auto-archive after 1 hour
            )
            
            # First line - who found it
            text_foundby_line = f"{text_found_by} **<@{owner_id}>**\n"
            
            # Normalize pack amount
            pack_amount = extract_numbers(str(pack_amount))
            pack_amount = max(min(int(pack_amount[0]) if pack_amount else 1, 5), 1)
            
            # Second line - miss counter (only for GodPacks)
            text_miss_line = ""
            if pack_name == "GodPack":
                miss_limit = config.miss_before_dead[pack_amount - 1]
                text_miss = f"## [ 0 miss / {miss_limit} ]"
                text_miss_line = f"{text_miss}\n\n"
            
            # Third line - eligible users
            text_eligible_line = f"{text_eligible} {tag_active_usernames}\n\n"
            
            # Fourth line - metadata
            text_metadata_line = f"Source: {message.jump_url}\nID:{account_id}\n{image_url or ''}\n\n"
            
            # Create appropriate title based on pack type
            post_name = f"{config.text_waiting_logo} {title_name}"
            
            # For non-GodPack types, append the pack type in brackets
            if pack_name != "GodPack":
                post_name += f" [{pack_name}]"
            
            print(f"Creating forum thread with name: {post_name}")
            
            try:
                # Create thread in the forum
                forum_post = await forum.create_thread(
                    name=post_name,
                    content=text_foundby_line + text_miss_line + text_eligible_line + text_metadata_line + text_command_tooltip,
                    auto_archive_duration=1440  # Auto-archive after 24 hours if no activity
                )
                
                print(f"Forum thread created successfully with ID: {forum_post.thread.id}")
                
                # Post forum link in webhook thread and lock it
                await thread.send(f"{text_verification_redirect} {forum_post.thread.jump_url}")
                await thread.edit(locked=True)
                
                # Post the account ID message
                await forum_post.thread.send(
                    content=f"{account_id} is the id of the account\n-# You can copy paste this message in PocketTCG to look for this account"
                )
                
                # Check if account ID is valid
                if account_id == "0000000000000000":
                    text_incorrect_id = localize(
                        "L'ID du compte est incorrect :\n- Injecter le compte pour retrouver l'ID\n- Reposter le GP dans le webhook avec l'ID entre parenthèse\n- Faites /removegpfound @LaPersonneQuiLaDrop\n- Supprimer ce post",
                        "The account ID is incorrect:\n- Inject the account to find the ID\n- Repost the GP in the webhook with the ID in parentheses\n- Do /removegpfound @UserThatDroppedIt\n- Delete this post"
                    )
                    await forum_post.thread.send(content=f"# ⚠️ {text_incorrect_id}")
                
                await asyncio.sleep(1)
                
                # Only update eligible IDs for GodPacks
                if pack_name == "GodPack":
                    await update_eligible_ids(bot)
                    await add_server_gp('eligible_gp', forum_post.thread)
                
                # Update GP tracking list
                await update_gp_tracking_list(bot)
                
                print("Forum post creation completed successfully")
                
            except Exception as e:
                print(f"Error creating forum thread: {str(e)}")
                import traceback
                traceback.print_exc()
                
        except Exception as e:
            print(f"Error creating webhook thread: {str(e)}")
            import traceback
            traceback.print_exc()
            
    except Exception as e:
        print(f"Critical error creating {pack_name} forum post: {str(e)}")
        import traceback
        traceback.print_exc()
        print(f"Failed to create {pack_name} Forum Thread for Account {account_id} owned by <@{owner_id}>")
async def send_status_header(bot):
    """Send the status header with buttons to the commands channel."""
    print("📝 Updating Status Header...")
    
    try:
        guild = await get_guild(bot)
        commands_channel_id = int(config.channel_id_commands)
        commands_channel = bot.get_channel(commands_channel_id)
        
        # Check if channel exists
        if not commands_channel:
            print(f"WARNING: Error: Could not find channel with ID: {commands_channel_id}")
            try:
                commands_channel = await bot.fetch_channel(commands_channel_id)
                if not commands_channel:
                    print(f"❌ Error: Channel with ID {commands_channel_id} does not exist or bot cannot access it")
                    return
            except Exception as e:
                print(f"❌ Error fetching channel: {e}")
                return
        
        # Get recent messages
        print("🔍 Looking for previous status header messages...")
        
        try:
            messages = await commands_channel.history(limit=10).flatten()
            
            # Filter for bot messages that have our status header characteristics
            status_messages = [
                msg for msg in messages
                if msg.author.id == bot.user.id
                and msg.embeds
                and len(msg.embeds) > 0
                and msg.embeds[0].title
                and "Click to change Status" in msg.embeds[0].title
            ]
            
            if status_messages:
                print(f"🗑️ Found {len(status_messages)} status messages to delete")
                
                # Delete each message individually
                for message in status_messages:
                    try:
                        await message.delete()
                        print(f"✅ Deleted status message with ID: {message.id}")
                    except Exception as e:
                        print(f"❌ Failed to delete message {message.id}: {str(e)}")
                    
                    # Add a small delay to avoid rate limits
                    await asyncio.sleep(0.5)
            else:
                print("ℹ️ No previous status messages found to delete")
        except Exception as e:
            print(f"❌ Error finding/deleting status messages: {str(e)}")
            # Continue execution even if message deletion fails
        
        # Create and send the new status header
        header_description = f"""
    ```ansi
    {color_text("Active", "green")} - ✅Friend Requests{" ✅Auto Kickable" if config.auto_kick else ""}
    {color_text("Inactive", "red")} - ❌Friend Requests
    {color_text("Farm / No Main", "cyan")} - ❌Friend Requests{" ❌Auto Kickable" if config.auto_kick else ""}
    {color_text("Switch to this for others when verifying / playing on Main / Low Instances amount due to high computer usage", "gray")}
    {f'{color_text("Leech / Only Main", "pink")} - ✅Friend Requests{" ❌Auto Kickable" if config.auto_kick else ""}' if config.can_people_leech else ""}
    ```"""

        embed_status_change = discord.Embed(
            title="__**Click to change Status**__ - *Similar to /active /inactive /farm " + ("/leech" if config.can_people_leech else "") + "*",
            description=header_description,
            color=0xf02f7e
        )
        
        # Create buttons
        button_active = discord.ui.Button(
            style=discord.ButtonStyle.success,
            label="Active",
            emoji="✅",
            custom_id="active"
        )
        
        button_farm = discord.ui.Button(
            style=discord.ButtonStyle.primary,
            label="Farm",
            emoji="⚡",
            custom_id="farm"
        )
        
        button_leech = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="Leech",
            emoji="🩸",
            custom_id="leech",
            disabled=not config.can_people_leech
        )
        
        button_inactive = discord.ui.Button(
            style=discord.ButtonStyle.danger,
            label="Inactive",
            emoji="💀",
            custom_id="inactive"
        )
        
        button_refresh_stats = discord.ui.Button(
            style=discord.ButtonStyle.primary,
            label="Refresh Stats",
            emoji="🔄",
            custom_id="refreshUserStats"
        )
        
        # Create view and add buttons
        view = discord.ui.View()
        view.add_item(button_active)
        view.add_item(button_inactive)
        view.add_item(button_farm)
        view.add_item(button_leech)
        view.add_item(button_refresh_stats)
        
        # Send the message with buttons
        try:
            await commands_channel.send(embed=embed_status_change, view=view)
            print("☑️📝 Done updating Status Header")
        except Exception as e:
            print(f"❌ Error sending status header: {str(e)}")
    except Exception as e:
        print(f"❌ Critical error in send_status_header: {str(e)}")
        import traceback
        traceback.print_exc()
async def inactivity_check(bot):
    """Check and kick inactive users."""
    print("👀 Checking inactivity...")
    
    guild = await get_guild(bot)
    inactive_count = 0
    
    active_users = await get_active_users()
    # Exit if 0 active users
    if not active_users:
        return
    
    for user in active_users:
        # Check user active state
        user_active_state = await refresh_user_active_state(user)
        active_state = user_active_state[0]
        inactive_time_minutes = user_active_state[1]
        
        # Check user instances count
        user_instances = await refresh_user_real_instances(user, active_state)
        
        # Check user pack per min & session time
        user_pack_per_min = float(get_attrib_value_from_user(user, 'packs_per_min', 10))
        session_time = float(get_attrib_value_from_user(user, 'session_time', 0))
        
        # Check time since last active
        last_active_time_str = get_attrib_value_from_user(user, 'last_active_time', '0')
        try:
            last_active_time = datetime.datetime.fromisoformat(last_active_time_str.replace('Z', '+00:00'))
            current_time = datetime.datetime.now(datetime.timezone.utc)
            diff_active_time = (current_time - last_active_time).total_seconds() / 60
        except:
            diff_active_time = 0
        
        # Check if kickable
        text_have_been_kicked = ""
        should_kick = False
        
        if active_state == "inactive":
            text_have_been_kicked = localize(
                f"a été kick des rerollers actifs pour inactivité depuis plus de {config.inactive_time}mn",
                f"has been kicked out of active rerollers for inactivity for more than {config.inactive_time}mn"
            )
            print(f"✖️ Kicked {get_username_from_user(user)} - inactivity for more than {config.inactive_time}mn")
            should_kick = True
        elif diff_active_time > float(config.heartbeat_rate) + 1 and session_time > float(config.heartbeat_rate) + 1:
            if user_instances <= int(config.inactive_instance_count):
                text_have_been_kicked = localize(
                    f"a été kick des rerollers actifs car il a {user_instances} instances en cours",
                    f"has been kicked out of active rerollers for inactivity because he had {user_instances} instances running"
                )
                print(f"✖️ Kicked {get_username_from_user(user)} - {user_instances} instances running")
                should_kick = True
            elif user_pack_per_min < float(config.inactive_pack_per_min_count) and user_pack_per_min > 0:
                text_have_been_kicked = localize(
                    f"a été kick des rerollers actifs pour avoir fait {user_pack_per_min} packs/mn",
                    f"has been kicked out of active rerollers for inactivity because made {user_pack_per_min} packs/mn"
                )
                print(f"✖️ Kicked {get_username_from_user(user)} - made {user_pack_per_min} packs/mn")
                should_kick = True
        
        # Kick the user if necessary
        if should_kick:
            user_id = get_id_from_user(user)
            username = get_username_from_user(user)
            await set_user_attrib_value(user_id, username, 'user_state', "inactive")
            await guild.get_channel(int(config.channel_id_commands)).send(
                content=f"<@{user_id}> {text_have_been_kicked}"
            )
            inactive_count += 1
    
    if inactive_count >= 1:
        await send_ids(bot)
# Export all necessary functions
__all__ = [
    'get_guild',
    'get_member_by_id',
    'get_users_stats',
    'send_stats',
    'send_ids',
    'send_status_header',
    'inactivity_check',
    'extract_gp_info',
    'extract_double_star_info',
    'create_forum_post',
    'mark_as_dead',
    'update_eligible_ids',
    'update_inactive_gps',
    'set_user_state',
    'update_server_data',
    'update_anti_cheat',
    'update_user_data_gp_live',
    'add_user_data_gp_live',
    'get_pack_specific_channel',
    'update_gp_tracking_list',
    'create_enhanced_stats_embed',
    'get_enhanced_selected_packs_embed_text',
    'create_timeline_stats',
    'create_leaderboards',
    'check_file_exists',
    'check_file_exists_or_create',
    'set_user_attrib_value',
    'get_user_attrib_value',
    'get_active_users',
    'get_all_users',
    'get_active_ids'
]