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
async def get_pack_routing_info(user_id: str, selected_pack: str = None) -> Dict[str, Any]:
    """Get pack routing information for a user based on their roles and selected pack."""
    try:
        # If role-based filtering is disabled, return empty routing
        if not config.enable_role_based_filters:
            return {'filters': [], 'routing_active': False}
            
        # Get user from bot to check roles
        bot = None  # This should be passed as parameter in real implementation
        member = await get_member_by_id(bot, user_id) if bot else None
        
        if not member:
            return {'filters': [], 'routing_active': False}
            
        # Check user roles against pack filters
        user_roles = [role.name.lower() for role in member.roles]
        active_filters = []
        
        # Check each configured pack filter
        for pack_name, pack_config in config.pack_filters.items():
            required_roles = [role.lower() for role in pack_config.get('required_roles', [])]
            
            # If user has any of the required roles for this pack
            if any(role in user_roles for role in required_roles):
                # If no specific pack is selected, or if this is the selected pack
                if not selected_pack or pack_name.lower() == selected_pack.lower():
                    active_filters.append({
                        'pack_name': pack_name,
                        'filters': pack_config.get('filters', {}),
                        'priority': pack_config.get('priority', 1)
                    })
        
        # Sort by priority (higher priority first)
        active_filters.sort(key=lambda x: x['priority'], reverse=True)
        
        return {
            'filters': active_filters,
            'routing_active': len(active_filters) > 0,
            'user_roles': user_roles
        }
        
    except Exception as e:
        print(f"❌ Error getting pack routing info: {e}")
        return {'filters': [], 'routing_active': False}

async def apply_pack_filters(detection_data: Dict[str, Any], routing_info: Dict[str, Any]) -> Dict[str, Any]:
    """Apply pack filters to detection data based on routing information."""
    try:
        if not routing_info.get('routing_active', False):
            return detection_data
            
        filtered_data = detection_data.copy()
        
        for filter_config in routing_info['filters']:
            pack_filters = filter_config.get('filters', {})
            
            # Apply minimum card filters
            min_filters = pack_filters.get('minimum_cards', {})
            for card_type, min_count in min_filters.items():
                current_count = filtered_data.get(f'{card_type}_count', 0)
                if current_count < min_count:
                    filtered_data[f'{card_type}_filtered'] = True
                    
            # Apply rarity filters
            rarity_filters = pack_filters.get('rarity_requirements', {})
            for rarity, required in rarity_filters.items():
                if required and not filtered_data.get(f'has_{rarity}', False):
                    filtered_data[f'{rarity}_filtered'] = True
                    
            # Apply special conditions
            special_filters = pack_filters.get('special_conditions', {})
            for condition, required in special_filters.items():
                if required and not filtered_data.get(condition, False):
                    filtered_data[f'{condition}_filtered'] = True
        
        return filtered_data
        
    except Exception as e:
        print(f"❌ Error applying pack filters: {e}")
        return detection_data

async def enhanced_pack_detection(image_data: Any, user_id: str, selected_pack: str = None) -> Dict[str, Any]:
    """Enhanced pack detection with role-based filtering and routing."""
    try:
        # Get routing information for the user
        routing_info = await get_pack_routing_info(user_id, selected_pack)
        
        # Perform base pack detection (this would call your existing detection logic)
        base_detection = await perform_base_pack_detection(image_data)
        
        # Apply pack-specific filters
        filtered_detection = await apply_pack_filters(base_detection, routing_info)
        
        # Add routing metadata
        filtered_detection['routing_info'] = routing_info
        filtered_detection['pack_routing_applied'] = routing_info.get('routing_active', False)
        
        return filtered_detection
        
    except Exception as e:
        print(f"❌ Error in enhanced pack detection: {e}")
        return {'error': str(e), 'routing_info': {'filters': [], 'routing_active': False}}

async def perform_base_pack_detection(image_data: Any) -> Dict[str, Any]:
    """Perform base pack detection logic (placeholder for your existing detection)."""
    # This should contain your existing pack detection logic
    # For now, returning a basic structure
    return {
        'cards_detected': 0,
        'rare_count': 0,
        'epic_count': 0,
        'legendary_count': 0,
        'has_rare': False,
        'has_epic': False,
        'has_legendary': False,
        'pack_value': 0,
        'detection_confidence': 0.0
    }

async def update_user_pack_stats(user_id: str, username: str, detection_result: Dict[str, Any]):
    """Update user pack statistics based on detection results."""
    try:
        # Update basic pack counts
        total_packs = int(await get_user_attrib_value(user_id, 'total_packs_opened', 0)) + 1
        await set_user_attrib_value(user_id, username, 'total_packs_opened', total_packs)
        
        session_packs = float(await get_user_attrib_value(user_id, 'session_packs_opened', 0)) + 1
        await set_user_attrib_value(user_id, username, 'session_packs_opened', session_packs)
        
        # Update card counts if detected
        if detection_result.get('cards_detected', 0) > 0:
            rare_count = int(await get_user_attrib_value(user_id, 'total_rare_cards', 0))
            epic_count = int(await get_user_attrib_value(user_id, 'total_epic_cards', 0))
            legendary_count = int(await get_user_attrib_value(user_id, 'total_legendary_cards', 0))
            
            rare_count += detection_result.get('rare_count', 0)
            epic_count += detection_result.get('epic_count', 0)
            legendary_count += detection_result.get('legendary_count', 0)
            
            await set_user_attrib_value(user_id, username, 'total_rare_cards', rare_count)
            await set_user_attrib_value(user_id, username, 'total_epic_cards', epic_count)
            await set_user_attrib_value(user_id, username, 'total_legendary_cards', legendary_count)
        
        # Update pack-specific stats if routing was applied
        if detection_result.get('pack_routing_applied', False):
            routing_info = detection_result.get('routing_info', {})
            active_filters = routing_info.get('filters', [])
            
            for filter_config in active_filters:
                pack_name = filter_config.get('pack_name', 'unknown')
                pack_attr = f'{pack_name.lower()}_packs_opened'
                
                pack_count = int(await get_user_attrib_value(user_id, pack_attr, 0)) + 1
                await set_user_attrib_value(user_id, username, pack_attr, pack_count)
        
    except Exception as e:
        print(f"❌ Error updating user pack stats: {e}")

async def get_user_pack_preferences(user_id: str) -> Dict[str, Any]:
    """Get user's pack preferences and filtering settings."""
    try:
        selected_pack = await get_user_attrib_value(user_id, 'selected_pack', '')
        filter_enabled = await get_user_attrib_value(user_id, 'pack_filter_enabled', 'true')
        
        # Get user's pack-specific statistics
        pack_stats = {}
        if config.enable_role_based_filters:
            for pack_name in config.pack_filters.keys():
                pack_attr = f'{pack_name.lower()}_packs_opened'
                pack_count = await get_user_attrib_value(user_id, pack_attr, 0)
                pack_stats[pack_name] = int(pack_count)
        
        return {
            'selected_pack': selected_pack,
            'filter_enabled': filter_enabled.lower() == 'true',
            'pack_statistics': pack_stats,
            'routing_available': config.enable_role_based_filters
        }
        
    except Exception as e:
        print(f"❌ Error getting user pack preferences: {e}")
        return {
            'selected_pack': '',
            'filter_enabled': True,
            'pack_statistics': {},
            'routing_available': False
        }

async def set_user_pack_preference(user_id: str, username: str, pack_name: str) -> bool:
    """Set user's preferred pack for filtering."""
    try:
        if not config.enable_role_based_filters:
            return False
            
        # Validate pack name exists in configuration
        if pack_name and pack_name not in config.pack_filters:
            return False
            
        await set_user_attrib_value(user_id, username, 'selected_pack', pack_name)
        return True
        
    except Exception as e:
        print(f"❌ Error setting user pack preference: {e}")
        return False

def calculate_pack_efficiency(detection_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate pack opening efficiency metrics."""
    try:
        if not detection_results:
            return {'efficiency': 0, 'avg_value': 0, 'rare_rate': 0}
            
        total_packs = len(detection_results)
        total_value = sum(result.get('pack_value', 0) for result in detection_results)
        rare_packs = sum(1 for result in detection_results if result.get('has_rare', False))
        epic_packs = sum(1 for result in detection_results if result.get('has_epic', False))
        legendary_packs = sum(1 for result in detection_results if result.get('has_legendary', False))
        
        avg_value = total_value / total_packs if total_packs > 0 else 0
        rare_rate = (rare_packs / total_packs * 100) if total_packs > 0 else 0
        epic_rate = (epic_packs / total_packs * 100) if total_packs > 0 else 0
        legendary_rate = (legendary_packs / total_packs * 100) if total_packs > 0 else 0
        
        # Calculate efficiency score (weighted combination of factors)
        efficiency = (
            (rare_rate * 0.3) + 
            (epic_rate * 0.4) + 
            (legendary_rate * 0.3) + 
            (min(avg_value / 1000, 100) * 0.2)  # Normalize value component
        )
        
        return {
            'efficiency': round_to_one_decimal(efficiency),
            'avg_value': round_to_one_decimal(avg_value),
            'rare_rate': round_to_one_decimal(rare_rate),
            'epic_rate': round_to_one_decimal(epic_rate),
            'legendary_rate': round_to_one_decimal(legendary_rate),
            'total_packs': total_packs
        }
        
    except Exception as e:
        print(f"❌ Error calculating pack efficiency: {e}")
        return {'efficiency': 0, 'avg_value': 0, 'rare_rate': 0}
async def create_enhanced_stats_embed(active_users: List[ET.Element], all_users: List[ET.Element]) -> List[discord.Embed]:
    """Create enhanced statistics embeds with comprehensive data."""
    current_time = datetime.datetime.now()
    
    # Calculate active users statistics
    active_users = await get_active_users(True, False)  # Refresh users
    
    # Get real instances for active users
    active_instances = []
    global_packs_per_min = []
    session_times = []
    
    for user in active_users:
        active_state = (await refresh_user_active_state(user))[0]
        instances = await refresh_user_real_instances(user, active_state)
        active_instances.append(instances)
        
        ppm = float(get_attrib_value_from_user(user, 'packs_per_min', 0))
        global_packs_per_min.append(ppm)
        
        session_time = float(get_attrib_value_from_user(user, 'session_time', 0))
        session_times.append(session_time)
    
    instances_amount = sum(active_instances)
    avg_instances = round_to_one_decimal(instances_amount / len(active_users) if active_users else 0)
    
    accumulated_packs_per_min = sum(global_packs_per_min)
    avg_packs_per_min = round_to_one_decimal(accumulated_packs_per_min / len(active_users) if active_users else 0)
    
    # Calculate packs per hour
    total_packs_per_hour = round_to_one_decimal(accumulated_packs_per_min * 60)
    
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
    eligible_gps = await get_server_data_gps('eligible_gp')
    ineligible_gps = await get_server_data_gps('ineligible_gp')
    live_gps = await get_server_data_gps('live_gp')
    
    eligible_gp_count = len(eligible_gps) if eligible_gps else 0
    ineligible_gp_count = len(ineligible_gps) if ineligible_gps else 0
    live_gp_count = len(live_gps) if live_gps else 0
    
    week_eligible_gp_count = 0
    week_live_gp_count = 0
    today_eligible_gp_count = 0
    today_live_gp_count = 0
    
    total_gp_count = eligible_gp_count + ineligible_gp_count
    potential_live_gp_count = 0
    
    week_luck = 0
    total_luck = 0
    today_luck = 0
    
    one_week_ago = current_time - datetime.timedelta(days=7)
    today_start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Calculate time-based GP stats
    if eligible_gps:
        for eligible_gp in eligible_gps:
            gp_time = get_time_from_gp(eligible_gp)
            if gp_time > one_week_ago:
                week_eligible_gp_count += 1
            if gp_time > today_start:
                today_eligible_gp_count += 1
                
        if live_gps:
            for live_gp in live_gps:
                gp_time = get_time_from_gp(live_gp)
                if gp_time > one_week_ago:
                    week_live_gp_count += 1
                if gp_time > today_start:
                    today_live_gp_count += 1
                    
            # Calculate luck percentages
            if week_eligible_gp_count > 0:
                week_luck = round_to_one_decimal(week_live_gp_count / week_eligible_gp_count * 100)
                
            if today_eligible_gp_count > 0:
                today_luck = round_to_one_decimal(today_live_gp_count / today_eligible_gp_count * 100)
                
            if eligible_gp_count > 0:
                total_luck = round_to_one_decimal(live_gp_count / eligible_gp_count * 100)
                
            # Calculate potential live GPs
            if not math.isnan(total_luck) and total_luck > 0 and total_gp_count > 0:
                potential_eligible_gp_count = eligible_gp_count + (ineligible_gp_count * config.min_2stars * 0.1)
                potential_live_gp_count = round(potential_eligible_gp_count * (total_luck / 100))
    
    # Create first embed with basic stats
    stats_embed1 = discord.Embed(title="Summary", color=0xf02f7e)
    
    # First row
    stats_embed1.add_field(name="👥 Rerollers", value=f"{len(active_users)}", inline=True)
    stats_embed1.add_field(name="🔄 Instances", value=f"{instances_amount}", inline=True)
    stats_embed1.add_field(name="📊 Avg Inst/User", value=f"{avg_instances}", inline=True)
    
    # Second row
    stats_embed1.add_field(name="🔥 Pack/Min", value=f"{round_to_one_decimal(accumulated_packs_per_min)}", inline=True)
    stats_embed1.add_field(name="🔥 Pack/Hour", value=f"{total_packs_per_hour}", inline=True)
    stats_embed1.add_field(name="📊 Avg PPM/User", value=f"{avg_packs_per_min}", inline=True)
    
    # Third row
    stats_embed1.add_field(name="📊 Avg Session", value=f"{avg_session_time}mn", inline=True)
    stats_embed1.add_field(name="🕒 Online 24h", value=f"{total_online_hours}h", inline=True)
    stats_embed1.add_field(name="📦 Packs 24h", value=f"{format_number_to_k(total_packs_last_24h)}", inline=True)
    
    # Fourth row
    stats_embed1.add_field(name="🃏 Total Packs", value=f"{format_number_to_k(total_server_packs)}", inline=True)
    stats_embed1.add_field(name="🕓 Total Time", value=f"{format_minutes_to_days(total_server_time)}", inline=True)
    stats_embed1.add_field(name="\u200B", value="\u200B", inline=True)  # Empty field for alignment
    
    # Second embed with godpack stats
    stats_embed2 = discord.Embed(title="GodPack Stats", color=0xf02f7e)
    
    # GP Stats rows
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
    stats_embed2.add_field(name="\u200B", value="\u200B", inline=True)
    
    return [stats_embed1, stats_embed2]

async def create_timeline_stats_with_visualization(bot, days: int = 7) -> discord.Embed:
    """Create timeline statistics with visual indicators."""
    # Calculate date range
    current_date = datetime.datetime.now()
    start_date = current_date - datetime.timedelta(days=days)
    
    # Prepare data structure for daily stats
    daily_stats = {}
    for i in range(days + 1):
        date = start_date + datetime.timedelta(days=i)
        date_key = date.strftime('%Y-%m-%d')
        
        daily_stats[date_key] = {
            'active_users': 0,
            'total_instances': 0,
            'total_packs': 0,
            'gps_found': 0
        }
    
    # Get all users to analyze their data
    all_users = await get_all_users()
    
    # Process each user's data (simplified for timeline)
    for user in all_users:
        user_id = get_id_from_user(user)
        
        # Get basic user stats for the timeline
        last_hb_time_str = get_attrib_value_from_user(user, 'last_heartbeat_time', '0')
        if last_hb_time_str == '0':
            continue
            
        try:
            last_hb_time = datetime.datetime.fromisoformat(last_hb_time_str.replace('Z', '+00:00'))
            
            # Skip entries outside date range
            if last_hb_time < start_date or last_hb_time > current_date:
                continue
                
            date_key = last_hb_time.strftime('%Y-%m-%d')
            
            if date_key not in daily_stats:
                continue
                
            # Update stats for this day
            daily_stats[date_key]['active_users'] += 1
            
            instances = int(get_attrib_value_from_user(user, 'hb_instances', 0))
            daily_stats[date_key]['total_instances'] += instances
            
            session_packs = float(get_attrib_value_from_user(user, 'session_packs_opened', 0))
            daily_stats[date_key]['total_packs'] += session_packs
            
        except Exception as e:
            continue
    
    # Get godpack data from server data
    live_gps = await get_server_data_gps('live_gp')
    
    # Count GPs by date
    if live_gps:
        for gp in live_gps:
            gp_time = get_time_from_gp(gp)
            if gp_time >= start_date and gp_time <= current_date:
                date_key = gp_time.strftime('%Y-%m-%d')
                if date_key in daily_stats:
                    daily_stats[date_key]['gps_found'] += 1
    
    # Create the embed with timeline stats
    timeline_embed = discord.Embed(
        title=f"Activity Timeline (Last {days} Days)",
        description="Daily activity statistics for the server",
        color=0x4b7bec
    )
    
    # Format the stats for each day
    days_array = sorted(daily_stats.keys())
    
    # Create timeline text with visual indicators
    timeline_text = ''
    days_to_show = min(days, 7)  # Limit to 7 days to avoid too long messages
    
    for i in range(len(days_array) - days_to_show, len(days_array)):
        date_key = days_array[i]
        stats = daily_stats[date_key]
        
        # Format date to be more readable
        date = datetime.datetime.strptime(date_key, '%Y-%m-%d')
        formatted_date = date.strftime('%b %d, %a')
        
        # Create activity indicators based on stats
        user_indicator = '👥' * min(stats['active_users'] // 5 + 1, 5)
        instance_indicator = '🖥️' * min(stats['total_instances'] // 20 + 1, 5)
        pack_indicator = '📦' * min(int(stats['total_packs']) // 5000 + 1, 5)
        gp_indicator = '✨' * min(stats['gps_found'], 5) if stats['gps_found'] > 0 else '❌'
        
        timeline_text += f"**{formatted_date}**\n"
        timeline_text += f"Users: {stats['active_users']} {user_indicator}\n"
        timeline_text += f"Instances: {stats['total_instances']} {instance_indicator}\n"
        timeline_text += f"Packs: {format_number_to_k(stats['total_packs'])} {pack_indicator}\n"
        timeline_text += f"GPs: {stats['gps_found']} {gp_indicator}\n\n"
    
    # Add fields for weekly summary
    total_weekly_users = max([stats['active_users'] for stats in daily_stats.values()] + [0])
    total_weekly_instances = sum([stats['total_instances'] for stats in daily_stats.values()])
    total_weekly_packs = sum([stats['total_packs'] for stats in daily_stats.values()])
    total_weekly_gps = sum([stats['gps_found'] for stats in daily_stats.values()])
    
    # Calculate averages
    avg_daily_instances = round_to_one_decimal(total_weekly_instances / days if days > 0 else 0)
    avg_daily_packs = round_to_one_decimal(total_weekly_packs / days if days > 0 else 0)
    
    timeline_embed.description = timeline_text
    timeline_embed.add_field(name='📊 Period Summary', value=f"{days} Days", inline=False)
    timeline_embed.add_field(name='👥 Max Users', value=f"{total_weekly_users}", inline=True)
    timeline_embed.add_field(name='🖥️ Avg Instances/Day', value=f"{avg_daily_instances}", inline=True)
    timeline_embed.add_field(name='📦 Avg Packs/Day', value=f"{format_number_to_k(avg_daily_packs)}", inline=True)
    timeline_embed.add_field(name='✨ Total GPs Found', value=f"{total_weekly_gps}", inline=True)
    timeline_embed.add_field(name="\u200B", value="\u200B", inline=True)  # Empty field for alignment
    
    return timeline_embed

async def create_user_activity_chart(user_id: str, days: int = 7) -> Optional[BytesIO]:
    """Create an activity chart for a specific user."""
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from datetime import datetime, timedelta
        
        # Get user data
        user_data = []
        current_date = datetime.now()
        start_date = current_date - timedelta(days=days)
        
        # This would need to be implemented based on your data storage
        # For now, creating a placeholder implementation
        
        # Create the plot
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
        
        # Sample data - replace with actual user activity data
        dates = [start_date + timedelta(days=x) for x in range(days)]
        packs_per_day = [100 + (x * 10) for x in range(days)]  # Sample data
        instances_per_day = [5 + (x % 3) for x in range(days)]  # Sample data
        
        # Plot packs opened
        ax1.plot(dates, packs_per_day, 'b-o', linewidth=2, markersize=6)
        ax1.set_title('Daily Packs Opened', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Packs', fontsize=12)
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        
        # Plot instances used
        ax2.bar(dates, instances_per_day, color='orange', alpha=0.7)
        ax2.set_title('Daily Instances Used', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Instances', fontsize=12)
        ax2.set_xlabel('Date', fontsize=12)
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        
        plt.tight_layout()
        
        # Save to BytesIO
        buffer = BytesIO()
        plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
        buffer.seek(0)
        plt.close()
        
        return buffer
        
    except Exception as e:
        print(f"❌ Error creating user activity chart: {e}")
        return None
async def create_comprehensive_leaderboards(bot, all_users: List[ET.Element]) -> Tuple[Optional[List[discord.Embed]], Optional[discord.Embed]]:
    """Create comprehensive leaderboard embeds for miss rates and farming with enhanced data."""
    try:
        # Prepare arrays for miss rate and farming stats
        miss_count_array = []
        farm_info_array = []
        
        # Process all users to calculate their stats
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
            
            # Only include users with substantial data
            if total_miss > 0 and total_time > 60:  # At least 1 hour of data
                miss_count_array.append({
                    'user': display_name, 
                    'value': miss_per_24_hour,
                    'total_miss': total_miss,
                    'total_hours': round_to_one_decimal(total_time_hour)
                })
            
            # Process farming stats
            total_time_farm = float(get_attrib_value_from_user(user, 'total_time_farm', 0))
            total_packs_farm = int(get_attrib_value_from_user(user, 'total_packs_farm', 0))
            
            # Only include users with substantial farming data
            if total_time_farm > 30:  # At least 30 minutes of farming
                farm_info_array.append({
                    'user': display_name,
                    'packs': total_packs_farm,
                    'time': total_time_farm,
                    'ppm': round_to_one_decimal(total_packs_farm / total_time_farm if total_time_farm > 0 else 0),
                    'hours': round_to_one_decimal(total_time_farm / 60)
                })
        
        best_miss_embed = None
        worst_miss_embed = None
        farm_embed = None
        
        # Create farming leaderboard if there are enough entries
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
                best_farmers_text += f"{emoji_best_farm} **{farmer['user']}** - {farmer['hours']}h with {farmer['packs']} packs ({farmer['ppm']} ppm)\n\n"
            
            farm_embed = discord.Embed(
                title="Best Farmers",
                description=best_farmers_text,
                color=0x39d1bf
            )
        
        # Create miss rate leaderboards if there are enough entries
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
*({miss_count_array[0]['total_miss']} misses over {miss_count_array[0]['total_hours']}h)*

{emoji_worst_verifier2} **{miss_count_array[1]['user']}** - {miss_count_array[1]['value']} miss / 24h
*({miss_count_array[1]['total_miss']} misses over {miss_count_array[1]['total_hours']}h)*

{emoji_worst_verifier1} **{miss_count_array[2]['user']}** - {miss_count_array[2]['value']} miss / 24h
*({miss_count_array[2]['total_miss']} misses over {miss_count_array[2]['total_hours']}h)*
            """
            
            # Sort by lowest miss rate first (best verifiers)
            miss_count_array.sort(key=lambda x: x['value'])
            best_miss_counts_text = f"""
{emoji_best_verifier1} **{miss_count_array[0]['user']}** - {miss_count_array[0]['value']} miss / 24h
*({miss_count_array[0]['total_miss']} misses over {miss_count_array[0]['total_hours']}h)*

{emoji_best_verifier2} **{miss_count_array[1]['user']}** - {miss_count_array[1]['value']} miss / 24h
*({miss_count_array[1]['total_miss']} misses over {miss_count_array[1]['total_hours']}h)*

{emoji_best_verifier3} **{miss_count_array[2]['user']}** - {miss_count_array[2]['value']} miss / 24h
*({miss_count_array[2]['total_miss']} misses over {miss_count_array[2]['total_hours']}h)*
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
        print(f"❌ Error creating comprehensive leaderboards: {e}")
        return [None, None]

async def create_pack_efficiency_leaderboard(bot, all_users: List[ET.Element]) -> Optional[discord.Embed]:
    """Create a leaderboard based on pack opening efficiency."""
    try:
        efficiency_array = []
        
        for user in all_users:
            user_id = get_id_from_user(user)
            username = get_username_from_user(user)
            
            member = await get_member_by_id(bot, user_id)
            display_name = member.display_name if member else username
            
            # Get pack statistics
            total_packs = int(get_attrib_value_from_user(user, 'total_packs_opened', 0))
            rare_cards = int(get_attrib_value_from_user(user, 'total_rare_cards', 0))
            epic_cards = int(get_attrib_value_from_user(user, 'total_epic_cards', 0))
            legendary_cards = int(get_attrib_value_from_user(user, 'total_legendary_cards', 0))
            
            # Only include users with substantial pack data
            if total_packs >= 100:  # At least 100 packs opened
                rare_rate = (rare_cards / total_packs * 100) if total_packs > 0 else 0
                epic_rate = (epic_cards / total_packs * 100) if total_packs > 0 else 0
                legendary_rate = (legendary_cards / total_packs * 100) if total_packs > 0 else 0
                
                # Calculate efficiency score
                efficiency_score = (rare_rate * 0.3) + (epic_rate * 0.4) + (legendary_rate * 0.3)
                
                efficiency_array.append({
                    'user': display_name,
                    'efficiency': round_to_one_decimal(efficiency_score),
                    'total_packs': total_packs,
                    'rare_rate': round_to_one_decimal(rare_rate),
                    'epic_rate': round_to_one_decimal(epic_rate),
                    'legendary_rate': round_to_one_decimal(legendary_rate)
                })
        
        if len(efficiency_array) >= 3:
            # Sort by efficiency (highest first)
            efficiency_array.sort(key=lambda x: x['efficiency'], reverse=True)
            
            efficiency_text = ""
            for i in range(min(5, len(efficiency_array))):
                entry = efficiency_array[i]
                if i == 0:
                    emoji = "🏆"
                elif i == 1:
                    emoji = "🥈"
                elif i == 2:
                    emoji = "🥉"
                else:
                    emoji = "📊"
                
                efficiency_text += f"{emoji} **{entry['user']}** - {entry['efficiency']}% efficiency\n"
                efficiency_text += f"*{format_number_to_k(entry['total_packs'])} packs • R:{entry['rare_rate']}% E:{entry['epic_rate']}% L:{entry['legendary_rate']}%*\n\n"
            
            return discord.Embed(
                title="Pack Efficiency Leaders",
                description=efficiency_text,
                color=0x9c59d1
            )
        
        return None
        
    except Exception as e:
        print(f"❌ Error creating pack efficiency leaderboard: {e}")
        return None

async def create_detailed_user_stats(bot, user_id: str) -> Optional[discord.Embed]:
    """Create detailed statistics embed for a specific user."""
    try:
        # Get user data
        all_users = await get_all_users()
        user_data = None
        
        for user in all_users:
            if get_id_from_user(user) == user_id:
                user_data = user
                break
        
        if not user_data:
            return None
        
        member = await get_member_by_id(bot, user_id)
        username = get_username_from_user(user_data)
        display_name = member.display_name if member else username
        
        # Get basic stats
        total_packs = int(get_attrib_value_from_user(user_data, 'total_packs_opened', 0))
        total_time = float(get_attrib_value_from_user(user_data, 'total_time', 0))
        session_time = float(get_attrib_value_from_user(user_data, 'session_time', 0))
        total_miss = int(get_attrib_value_from_user(user_data, 'total_miss', 0))
        
        # Calculate derived stats
        total_hours = round_to_one_decimal((total_time + session_time) / 60)
        packs_per_hour = round_to_one_decimal(total_packs / total_hours if total_hours > 0 else 0)
        miss_rate = round_to_one_decimal((total_miss / total_hours * 24) if total_hours > 0 else 0)
        
        # Get card stats
        rare_cards = int(get_attrib_value_from_user(user_data, 'total_rare_cards', 0))
        epic_cards = int(get_attrib_value_from_user(user_data, 'total_epic_cards', 0))
        legendary_cards = int(get_attrib_value_from_user(user_data, 'total_legendary_cards', 0))
        
        # Calculate rates
        rare_rate = round_to_one_decimal((rare_cards / total_packs * 100) if total_packs > 0 else 0)
        epic_rate = round_to_one_decimal((epic_cards / total_packs * 100) if total_packs > 0 else 0)
        legendary_rate = round_to_one_decimal((legendary_cards / total_packs * 100) if total_packs > 0 else 0)
        
        # Get current session info
        user_state = get_attrib_value_from_user(user_data, 'user_state', 'inactive')
        session_packs = int(get_attrib_value_from_user(user_data, 'session_packs_opened', 0))
        current_instances = int(get_attrib_value_from_user(user_data, 'real_instances', 0))
        
        # Create embed
        stats_embed = discord.Embed(
            title=f"📊 Detailed Stats - {display_name}",
            color=0x3498db
        )
        
        # Overall Statistics
        stats_embed.add_field(
            name="📈 Overall Statistics",
            value=f"**Total Packs:** {format_number_to_k(total_packs)}\n"
                  f"**Total Time:** {format_minutes_to_days(total_time + session_time)}\n"
                  f"**Packs/Hour:** {packs_per_hour}\n"
                  f"**Miss Rate:** {miss_rate}/24h",
            inline=True
        )
        
        # Card Statistics
        stats_embed.add_field(
            name="🃏 Card Statistics",
            value=f"**Rare:** {rare_cards} ({rare_rate}%)\n"
                  f"**Epic:** {epic_cards} ({epic_rate}%)\n"
                  f"**Legendary:** {legendary_cards} ({legendary_rate}%)\n"
                  f"**Total Cards:** {rare_cards + epic_cards + legendary_cards}",
            inline=True
        )
        
        # Current Session
        session_status = "🟢 Active" if user_state == "active" else "🔴 Inactive"
        stats_embed.add_field(
            name="⚡ Current Session",
            value=f"**Status:** {session_status}\n"
                  f"**Session Packs:** {session_packs}\n"
                  f"**Instances:** {current_instances}\n"
                  f"**Session Time:** {round_to_one_decimal(session_time)}mn",
            inline=True
        )
        
        # Add pack preferences if role-based filtering is enabled
        if config.enable_role_based_filters:
            pack_prefs = await get_user_pack_preferences(user_id)
            selected_pack = pack_prefs.get('selected_pack', 'None')
            pack_stats_text = f"**Selected Pack:** {selected_pack or 'None'}\n"
            
            for pack_name, count in pack_prefs.get('pack_statistics', {}).items():
                if count > 0:
                    pack_stats_text += f"**{pack_name}:** {count} packs\n"
            
            stats_embed.add_field(
                name="🎯 Pack Preferences",
                value=pack_stats_text,
                inline=False
            )
        
        return stats_embed
        
    except Exception as e:
        print(f"❌ Error creating detailed user stats: {e}")
        return None

async def send_enhanced_stats(bot, manual_trigger: bool = False):
    """Send enhanced statistics with all new features."""
    try:
        # Get stats channel
        stats_channel = bot.get_channel(config.stats_channel_id)
        if not stats_channel:
            print(f"❌ Stats channel not found: {config.stats_channel_id}")
            return
        
        # Get all users data
        active_users = await get_active_users(True, False)
        all_users = await get_all_users()
        
        if not active_users and not manual_trigger:
            print("ℹ️ No active users, skipping stats")
            return
        
        # Create and send enhanced statistics embeds
        stats_embeds = await create_enhanced_stats_embed(active_users, all_users)
        
        for embed in stats_embeds:
            await stats_channel.send(embed=embed)
            await asyncio.sleep(1.5)  # Rate limiting
        
        # Send timeline stats (last 7 days)
        timeline_embed = await create_timeline_stats_with_visualization(bot, 7)
        await stats_channel.send(embed=timeline_embed)
        await asyncio.sleep(1.5)
        
        # Send leaderboards if enough users
        if len(all_users) > 5:
            # Create and send enhanced miss rate and farming leaderboards
            leaderboards = await create_comprehensive_leaderboards(bot, all_users)
            miss_leaderboard, farm_leaderboard = leaderboards
            
            if miss_leaderboard:
                best_miss_embed, worst_miss_embed = miss_leaderboard
                await stats_channel.send(embed=best_miss_embed)
                await asyncio.sleep(1.5)
                await stats_channel.send(embed=worst_miss_embed)
                await asyncio.sleep(1.5)
            
            if farm_leaderboard:
                await stats_channel.send(embed=farm_leaderboard)
                await asyncio.sleep(1.5)
            
            # Send pack efficiency leaderboard
            efficiency_embed = await create_pack_efficiency_leaderboard(bot, all_users)
            if efficiency_embed:
                await stats_channel.send(embed=efficiency_embed)
                await asyncio.sleep(1.5)
        
        # Update IDs file
        ids_content = await get_active_ids()
        if write_file('ids.txt', ids_content):
            print("✅ Updated ids.txt")
        
        # Upload to gist if configured
        if hasattr(config, 'upload_gist') and config.upload_gist:
            await update_gist()
        
        print(f"✅ Enhanced stats sent successfully")
        
    except Exception as e:
        print(f"❌ Error sending enhanced stats: {e}")

async def generate_server_report(bot, days: int = 30) -> Optional[discord.Embed]:
    """Generate a comprehensive server activity report."""
    try:
        current_time = datetime.datetime.now()
        start_time = current_time - datetime.timedelta(days=days)
        
        all_users = await get_all_users()
        
        # Calculate server-wide statistics
        total_users = len(all_users)
        active_users_count = len(await get_active_users(True, False))
        
        total_server_packs = sum([int(get_attrib_value_from_user(user, 'total_packs_opened', 0)) for user in all_users])
        total_server_time = sum([float(get_attrib_value_from_user(user, 'total_time', 0)) for user in all_users])
        total_server_hours = round_to_one_decimal(total_server_time / 60)
        
        # Calculate average statistics
        avg_packs_per_user = round_to_one_decimal(total_server_packs / total_users if total_users > 0 else 0)
        avg_hours_per_user = round_to_one_decimal(total_server_hours / total_users if total_users > 0 else 0)
        
        # Get GP statistics
        live_gps = await get_server_data_gps('live_gp')
        eligible_gps = await get_server_data_gps('eligible_gp')
        
        total_live_gps = len(live_gps) if live_gps else 0
        total_eligible_gps = len(eligible_gps) if eligible_gps else 0
        
        overall_luck = round_to_one_decimal((total_live_gps / total_eligible_gps * 100) if total_eligible_gps > 0 else 0)
        
        # Create comprehensive report embed
        report_embed = discord.Embed(
            title=f"📋 Server Activity Report ({days} Days)",
            description=f"Comprehensive analysis from {start_time.strftime('%b %d, %Y')} to {current_time.strftime('%b %d, %Y')}",
            color=0x2ecc71
        )
        
        # Server Overview
        report_embed.add_field(
            name="🌐 Server Overview",
            value=f"**Total Users:** {total_users}\n"
                  f"**Active Users:** {active_users_count}\n"
                  f"**Activity Rate:** {round_to_one_decimal((active_users_count / total_users * 100) if total_users > 0 else 0)}%",
            inline=True
        )
        
        # Pack Statistics
        report_embed.add_field(
            name="📦 Pack Statistics",
            value=f"**Total Packs:** {format_number_to_k(total_server_packs)}\n"
                  f"**Avg/User:** {format_number_to_k(avg_packs_per_user)}\n"
                  f"**Server Hours:** {format_number_to_k(total_server_hours)}h",
            inline=True
        )
        
        # GodPack Performance
        report_embed.add_field(
            name="✨ GodPack Performance",
            value=f"**Live GPs:** {total_live_gps}\n"
                  f"**Eligible GPs:** {total_eligible_gps}\n"
                  f"**Success Rate:** {overall_luck}%",
            inline=True
        )
        
        # Performance Metrics
        server_packs_per_hour = round_to_one_decimal(total_server_packs / total_server_hours if total_server_hours > 0 else 0)
        report_embed.add_field(
            name="⚡ Performance Metrics",
            value=f"**Server PPH:** {server_packs_per_hour}\n"
                  f"**Avg Hours/User:** {avg_hours_per_user}h\n"
                  f"**Efficiency:** {round_to_one_decimal(server_packs_per_hour / active_users_count if active_users_count > 0 else 0)} PPH/User",
            inline=False
        )
        
        return report_embed
        
    except Exception as e:
        print(f"❌ Error generating server report: {e}")
        return None
async def get_channel_by_id(bot, channel_id: int) -> Optional[discord.TextChannel]:
    """Get a channel by its ID."""
    try:
        return bot.get_channel(channel_id)
    except Exception as e:
        print(f"❌ Error getting channel {channel_id}: {e}")
        return None

async def send_stats_legacy(bot, manual_trigger: bool = False):
    """Legacy stats function for backward compatibility."""
    # This calls the new enhanced stats function
    await send_enhanced_stats(bot, manual_trigger)

def create_basic_embed(title: str, description: str = "", color: int = 0x3498db) -> discord.Embed:
    """Create a basic embed with standard formatting."""
    embed = discord.Embed(title=title, description=description, color=color)
    return embed

async def log_user_activity(user_id: str, username: str, activity_type: str, details: str = ""):
    """Log user activity for debugging and analytics."""
    try:
        timestamp = datetime.datetime.now().isoformat()
        log_entry = f"[{timestamp}] User {username} ({user_id}): {activity_type}"
        if details:
            log_entry += f" - {details}"
        
        # Write to log file or database
        log_path = os.path.join('logs', 'user_activity.log')
        
        # Create logs directory if it doesn't exist
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(log_entry + '\n')
            
    except Exception as e:
        print(f"❌ Error logging user activity: {e}")

async def cleanup_inactive_users():
    """Clean up data for users who haven't been active in a long time."""
    try:
        current_time = datetime.datetime.now()
        cleanup_threshold = current_time - datetime.timedelta(days=config.user_cleanup_days if hasattr(config, 'user_cleanup_days') else 30)
        
        all_users = await get_all_users()
        users_to_clean = []
        
        for user in all_users:
            last_hb_time_str = get_attrib_value_from_user(user, 'last_heartbeat_time', '0')
            if last_hb_time_str == '0':
                continue
                
            try:
                last_hb_time = datetime.datetime.fromisoformat(last_hb_time_str.replace('Z', '+00:00'))
                if last_hb_time < cleanup_threshold:
                    users_to_clean.append(user)
            except:
                continue
        
        # Reset session data for inactive users
        for user in users_to_clean:
            user_id = get_id_from_user(user)
            username = get_username_from_user(user)
            
            await set_user_attrib_value(user_id, username, 'session_time', 0)
            await set_user_attrib_value(user_id, username, 'session_packs_opened', 0)
            await set_user_attrib_value(user_id, username, 'user_state', 'inactive')
            
        print(f"✅ Cleaned up {len(users_to_clean)} inactive users")
        
    except Exception as e:
        print(f"❌ Error cleaning up inactive users: {e}")

async def backup_user_data():
    """Create a backup of user data."""
    try:
        if not check_file_exists(path_users_data):
            return False
            
        backup_dir = os.path.join('backups', datetime.datetime.now().strftime('%Y-%m-%d'))
        os.makedirs(backup_dir, exist_ok=True)
        
        backup_path = os.path.join(backup_dir, f"UserData_{datetime.datetime.now().strftime('%H-%M-%S')}.xml")
        
        with open(path_users_data, 'r', encoding='utf-8') as src:
            with open(backup_path, 'w', encoding='utf-8') as dst:
                dst.write(src.read())
                
        print(f"✅ User data backed up to {backup_path}")
        return True
        
    except Exception as e:
        print(f"❌ Error backing up user data: {e}")
        return False

async def restore_user_data(backup_path: str) -> bool:
    """Restore user data from a backup."""
    try:
        if not os.path.isfile(backup_path):
            print(f"❌ Backup file not found: {backup_path}")
            return False
            
        # Create a backup of current data before restoring
        await backup_user_data()
        
        with open(backup_path, 'r', encoding='utf-8') as src:
            with open(path_users_data, 'w', encoding='utf-8') as dst:
                dst.write(src.read())
                
        print(f"✅ User data restored from {backup_path}")
        return True
        
    except Exception as e:
        print(f"❌ Error restoring user data: {e}")
        return False

def validate_user_data_integrity() -> bool:
    """Validate the integrity of user data XML."""
    try:
        if not check_file_exists(path_users_data):
            return False
            
        tree = ET.parse(path_users_data)
        root = tree.getroot()
        
        # Check for duplicate user IDs
        user_ids = []
        for user in root.findall('user'):
            user_id = user.get('id')
            if user_id in user_ids:
                print(f"⚠️ Duplicate user ID found: {user_id}")
                return False
            user_ids.append(user_id)
            
        # Check for required attributes
        required_attrs = ['id', 'username']
        for user in root.findall('user'):
            for attr in required_attrs:
                if user.get(attr) is None:
                    print(f"⚠️ Missing required attribute '{attr}' for user")
                    return False
                    
        print("✅ User data integrity check passed")
        return True
        
    except Exception as e:
        print(f"❌ Error validating user data integrity: {e}")
        return False

async def migrate_user_data(old_version: str, new_version: str) -> bool:
    """Migrate user data between versions."""
    try:
        print(f"🔄 Migrating user data from {old_version} to {new_version}")
        
        # Create backup before migration
        await backup_user_data()
        
        # Version-specific migration logic would go here
        if old_version == "1.0" and new_version == "2.0":
            # Example migration: add new attributes
            all_users = await get_all_users()
            for user in all_users:
                user_id = get_id_from_user(user)
                username = get_username_from_user(user)
                
                # Add new attributes with default values
                if not get_attrib_value_from_user(user, 'total_rare_cards'):
                    await set_user_attrib_value(user_id, username, 'total_rare_cards', 0)
                if not get_attrib_value_from_user(user, 'total_epic_cards'):
                    await set_user_attrib_value(user_id, username, 'total_epic_cards', 0)
                if not get_attrib_value_from_user(user, 'total_legendary_cards'):
                    await set_user_attrib_value(user_id, username, 'total_legendary_cards', 0)
        
        print(f"✅ Migration from {old_version} to {new_version} completed")
        return True
        
    except Exception as e:
        print(f"❌ Error migrating user data: {e}")
        return False

def get_system_stats() -> Dict[str, Any]:
    """Get system performance statistics."""
    try:
        import psutil
        
        # Get memory usage
        memory = psutil.virtual_memory()
        
        # Get CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Get disk usage
        disk = psutil.disk_usage('/')
        
        return {
            'memory_used': memory.used,
            'memory_total': memory.total,
            'memory_percent': memory.percent,
            'cpu_percent': cpu_percent,
            'disk_used': disk.used,
            'disk_total': disk.total,
            'disk_percent': (disk.used / disk.total) * 100
        }
        
    except ImportError:
        # psutil not available, return basic stats
        return {
            'memory_used': 0,
            'memory_total': 0,
            'memory_percent': 0,
            'cpu_percent': 0,
            'disk_used': 0,
            'disk_total': 0,
            'disk_percent': 0
        }
    except Exception as e:
        print(f"❌ Error getting system stats: {e}")
        return {}

async def health_check() -> Dict[str, bool]:
    """Perform a health check of all system components."""
    health_status = {
        'user_data_accessible': False,
        'server_data_accessible': False,
        'user_data_valid': False,
        'stats_channel_accessible': False,
        'system_performance_ok': False
    }
    
    try:
        # Check user data accessibility
        health_status['user_data_accessible'] = check_file_exists(path_users_data)
        
        # Check server data accessibility
        health_status['server_data_accessible'] = check_file_exists(path_server_data)
        
        # Check user data validity
        health_status['user_data_valid'] = validate_user_data_integrity()
        
        # Check system performance
        sys_stats = get_system_stats()
        health_status['system_performance_ok'] = (
            sys_stats.get('memory_percent', 0) < 90 and 
            sys_stats.get('cpu_percent', 0) < 90 and
            sys_stats.get('disk_percent', 0) < 90
        )
        
        return health_status
        
    except Exception as e:
        print(f"❌ Error performing health check: {e}")
        return health_status

async def emergency_shutdown(bot, reason: str = "Emergency shutdown"):
    """Perform emergency shutdown procedures."""
    try:
        print(f"🚨 Emergency shutdown initiated: {reason}")
        
        # Backup data
        await backup_user_data()
        
        # Save current active IDs
        ids_content = await get_active_ids()
        emergency_ids_path = f"emergency_ids_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        write_file(emergency_ids_path, ids_content)
        
        # Set all users to inactive
        active_users = await get_active_users(True, True)
        for user in active_users:
            user_id = get_id_from_user(user)
            username = get_username_from_user(user)
            await set_user_attrib_value(user_id, username, 'user_state', 'inactive')
        
        # Send notification if possible
        try:
            if hasattr(config, 'admin_channel_id'):
                admin_channel = bot.get_channel(config.admin_channel_id)
                if admin_channel:
                    await admin_channel.send(f"🚨 Emergency shutdown: {reason}")
        except:
            pass
            
        print("✅ Emergency shutdown procedures completed")
        
    except Exception as e:
        print(f"❌ Error during emergency shutdown: {e}")

# Utility functions for statistics calculations
def calculate_percentile(values: List[float], percentile: float) -> float:
    """Calculate the nth percentile of a list of values."""
    if not values:
        return 0
    
    sorted_values = sorted(values)
    index = (percentile / 100) * (len(sorted_values) - 1)
    
    if index.is_integer():
        return sorted_values[int(index)]
    else:
        lower = sorted_values[int(index)]
        upper = sorted_values[int(index) + 1]
        return lower + (upper - lower) * (index - int(index))

def calculate_standard_deviation(values: List[float]) -> float:
    """Calculate standard deviation of a list of values."""
    if len(values) < 2:
        return 0
    
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
    return math.sqrt(variance)

def calculate_trend(values: List[float]) -> str:
    """Calculate trend direction from a list of values."""
    if len(values) < 2:
        return "stable"
    
    # Simple linear regression to determine trend
    n = len(values)
    x_values = list(range(n))
    
    x_mean = sum(x_values) / n
    y_mean = sum(values) / n
    
    numerator = sum((x_values[i] - x_mean) * (values[i] - y_mean) for i in range(n))
    denominator = sum((x - x_mean) ** 2 for x in x_values)
    
    if denominator == 0:
        return "stable"
    
    slope = numerator / denominator
    
    if slope > 0.1:
        return "increasing"
    elif slope < -0.1:
        return "decreasing"
    else:
        return "stable"

# Export functions for external use
__all__ = [
    'get_guild', 'get_member_by_id', 'clean_string', 'check_file_exists', 'check_file_exists_or_create',
    'read_file_async', 'write_file', 'backup_file', 'does_user_profile_exists', 'set_user_attrib_value',
    'get_user_attrib_value', 'get_active_users', 'get_active_ids', 'get_all_users', 'get_username_from_user',
    'get_id_from_user', 'get_attrib_value_from_user', 'get_time_from_gp', 'refresh_user_active_state',
    'refresh_user_real_instances', 'get_server_data_gps', 'add_server_gp', 'get_pack_routing_info',
    'apply_pack_filters', 'enhanced_pack_detection', 'perform_base_pack_detection', 'update_user_pack_stats',
    'get_user_pack_preferences', 'set_user_pack_preference', 'calculate_pack_efficiency',
    'create_enhanced_stats_embed', 'create_timeline_stats_with_visualization', 'create_user_activity_chart',
    'create_comprehensive_leaderboards', 'create_pack_efficiency_leaderboard', 'create_detailed_user_stats',
    'send_enhanced_stats', 'generate_server_report', 'get_channel_by_id', 'send_stats_legacy',
    'create_basic_embed', 'log_user_activity', 'cleanup_inactive_users', 'backup_user_data',
    'restore_user_data', 'validate_user_data_integrity', 'migrate_user_data', 'get_system_stats',
    'health_check', 'emergency_shutdown', 'calculate_percentile', 'calculate_standard_deviation',
    'calculate_trend'
]

print("✅ Enhanced core_utils.py loaded successfully with all features")