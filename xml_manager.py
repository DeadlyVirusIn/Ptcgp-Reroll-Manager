import os
import xml.etree.ElementTree as ET
import datetime
import threading
from typing import List, Dict, Any, Optional, Union, Tuple
import logging

# Import the path constants
from config import (
    path_users_data,
    path_server_data,
    attrib_pocket_id,
    attrib_prefix,
    attrib_user_state,
    attrib_active_state,
    attrib_average_instances,
    attrib_hb_instances,
    attrib_real_instances,
    attrib_session_time,
    attrib_total_packs_opened,
    attrib_total_packs_farm,
    attrib_total_average_instances,
    attrib_total_average_ppm,
    attrib_total_hb_tick,
    attrib_session_packs_opened,
    attrib_diff_packs_since_last_hb,
    attrib_diff_time_since_last_hb,
    attrib_packs_per_min,
    attrib_god_pack_found,
    attrib_god_pack_live,
    attrib_last_active_time,
    attrib_last_heartbeat_time,
    attrib_total_time,
    attrib_total_time_farm,
    attrib_total_miss,
    attrib_anticheat_user_count,
    attrib_subsystems,
    attrib_subsystem,
    attrib_eligible_gps,
    attrib_eligible_gp,
    attrib_live_gps,
    attrib_live_gp,
    attrib_ineligible_gps,
    attrib_ineligible_gp,
    attrib_selected_pack,
    attrib_rolling_type
)

# FIXED: Thread safety improvements - Use context managers for safer locking
users_data_lock = threading.RLock()  # Use RLock for reentrant locking
server_data_lock = threading.RLock()  # Use RLock for reentrant locking

logger = logging.getLogger(__name__)

def clean_string(text: str) -> str:
    """Remove non-alphanumeric characters from a string."""
    if not text:
        return ""
    return ''.join(c for c in str(text) if c.isalnum())

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
    
    try:
        with open(path, 'wb') as f:
            tree.write(f, encoding='utf-8', xml_declaration=True)
        return True
    except Exception as e:
        logger.error(f"Error creating XML file {path}: {e}")
        return False

async def read_file_async(path: str) -> str:
    """Read a file asynchronously."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading file {path}: {e}")
        return ""

def write_file(path: str, content: str) -> bool:
    """Write content to a file."""
    try:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    except Exception as e:
        logger.error(f"Error writing to file {path}: {e}")
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
        logger.error(f"Error backing up file {path}: {e}")
        return False

async def does_user_profile_exists(user_id: str, username: str) -> bool:
    """Check if a user profile exists in UserData.xml, create if not."""
    try:
        with users_data_lock:
            if not check_file_exists(path_users_data):
                check_file_exists_or_create(path_users_data, "Users")
                
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
        logger.error(f"Error checking user profile: {e}")
        return False

async def set_user_attrib_value(user_id: str, username: str, attribute: str, value: Any) -> bool:
    """Set an attribute value for a user."""
    try:
        with users_data_lock:
            await does_user_profile_exists(user_id, username)
            
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
        logger.error(f"Error setting user attribute: {e}")
        return False

async def get_user_attrib_value(user_id: str, attribute: str, default_value: Any = None) -> Any:
    """Get an attribute value for a user."""
    try:
        with users_data_lock:
            if not check_file_exists(path_users_data):
                return default_value
                
            tree = ET.parse(path_users_data)
            root = tree.getroot()
            
            for user in root.findall('user'):
                if user.get('id') == user_id:
                    value = user.get(attribute)
                    return value if value is not None else default_value
                    
            return default_value
    except Exception as e:
        logger.error(f"Error getting user attribute: {e}")
        return default_value

async def set_all_users_attrib_value(attribute: str, value: Any) -> bool:
    """Set an attribute value for all users."""
    try:
        with users_data_lock:
            if not check_file_exists(path_users_data):
                return False
                
            tree = ET.parse(path_users_data)
            root = tree.getroot()
            
            for user in root.findall('user'):
                user.set(attribute, str(value))
                
            tree.write(path_users_data, encoding='utf-8', xml_declaration=True)
            return True
    except Exception as e:
        logger.error(f"Error setting attribute for all users: {e}")
        return False

async def set_user_subsystem_attrib_value(user_id: str, username: str, subsystem_name: str, attribute: str, value: Any) -> bool:
    """Set an attribute value for a user's subsystem."""
    try:
        with users_data_lock:
            await does_user_profile_exists(user_id, username)
            
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
            
            # Find subsystems element
            subsystems = user.find(attrib_subsystems)
            if subsystems is None:
                subsystems = ET.SubElement(user, attrib_subsystems)
            
            # Find specific subsystem
            subsystem = None
            for s in subsystems.findall(attrib_subsystem):
                if s.get('name') == subsystem_name:
                    subsystem = s
                    break
            
            # Create subsystem if not found
            if subsystem is None:
                subsystem = ET.SubElement(subsystems, attrib_subsystem)
                subsystem.set('name', subsystem_name)
            
            # Set attribute
            subsystem.set(attribute, str(value))
            
            tree.write(path_users_data, encoding='utf-8', xml_declaration=True)
            return True
    except Exception as e:
        logger.error(f"Error setting user subsystem attribute: {e}")
        return False

async def get_user_subsystem_attrib_value(user_id: str, subsystem_name: str, attribute: str, default_value: Any = None) -> Any:
    """Get an attribute value for a user's subsystem."""
    try:
        with users_data_lock:
            if not check_file_exists(path_users_data):
                return default_value
                
            tree = ET.parse(path_users_data)
            root = tree.getroot()
            
            # Find the user
            for user in root.findall('user'):
                if user.get('id') == user_id:
                    # Find subsystems element
                    subsystems = user.find(attrib_subsystems)
                    if subsystems is None:
                        return default_value
                    
                    # Find specific subsystem
                    for subsystem in subsystems.findall(attrib_subsystem):
                        if subsystem.get('name') == subsystem_name:
                            value = subsystem.get(attribute)
                            return value if value is not None else default_value
                    
                    return default_value
            
            return default_value
    except Exception as e:
        logger.error(f"Error getting user subsystem attribute: {e}")
        return default_value

async def get_user_subsystems(user_element: ET.Element) -> List[ET.Element]:
    """Get all subsystems for a user."""
    subsystems_elem = user_element.find(attrib_subsystems)
    if subsystems_elem is not None:
        return subsystems_elem.findall(attrib_subsystem)
    return []

async def get_user_active_subsystems(user_element: ET.Element) -> List[ET.Element]:
    """Get active subsystems for a user."""
    current_time = datetime.datetime.now()
    active_subsystems = []
    
    subsystems = await get_user_subsystems(user_element)
    for subsystem in subsystems:
        last_hb_time_str = subsystem.get(attrib_last_heartbeat_time, '0')
        try:
            last_hb_time = datetime.datetime.fromisoformat(last_hb_time_str.replace('Z', '+00:00'))
            diff_min = (current_time - last_hb_time).total_seconds() / 60
            
            if diff_min < float(30 + 1):  # Using heartbeat_rate + 1
                active_subsystems.append(subsystem)
        except:
            pass
            
    return active_subsystems

async def get_active_users(active_only: bool = True, include_farm: bool = False) -> List[ET.Element]:
    """Get active users from UserData.xml."""
    try:
        with users_data_lock:
            if not check_file_exists(path_users_data):
                return []
                
            tree = ET.parse(path_users_data)
            root = tree.getroot()
            
            active_users = []
            for user in root.findall('user'):
                user_state = user.get(attrib_user_state, 'inactive')
                
                if active_only:
                    if user_state == 'active':
                        active_users.append(user)
                    elif include_farm and user_state == 'farm':
                        active_users.append(user)
                else:
                    active_users.append(user)
                    
            return active_users
    except Exception as e:
        logger.error(f"Error getting active users: {e}")
        return []

async def get_active_ids() -> str:
    """Get active user IDs formatted for ids.txt."""
    from config import enable_role_based_filters
    
    try:
        with users_data_lock:
            active_users = await get_active_users(True, False)
            
            id_list = []
            for user in active_users:
                pocket_id = user.get(attrib_pocket_id)
                if pocket_id:
                    selected_pack = user.get(attrib_selected_pack, '')
                    
                    # Format the entry based on whether role-based filters are enabled
                    if enable_role_based_filters and selected_pack:
                        id_list.append(f"{pocket_id}/{selected_pack}")
                    else:
                        id_list.append(pocket_id)
            
            return '\n'.join(id_list)
    except Exception as e:
        logger.error(f"Error getting active IDs: {e}")
        return ""

async def get_all_users() -> List[ET.Element]:
    """Get all users from UserData.xml."""
    return await get_active_users(False, True)

def get_username_from_users(users: List[ET.Element]) -> List[str]:
    """Get list of usernames from user elements."""
    return [user.get('username', 'Unknown') for user in users]

def get_username_from_user(user: ET.Element) -> str:
    """Get username from user element."""
    return user.get('username', 'Unknown')

def get_id_from_users(users: List[ET.Element]) -> List[str]:
    """Get list of IDs from user elements."""
    return [user.get('id', '0') for user in users]

def get_id_from_user(user: ET.Element) -> str:
    """Get ID from user element."""
    return user.get('id', '0')

def get_time_from_gp(gp_element: ET.Element) -> datetime.datetime:
    """Get time from GP element."""
    time_str = gp_element.get('time', None)
    if time_str:
        try:
            return datetime.datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        except:
            pass
    return datetime.datetime.now()

def get_attrib_value_from_users(users: List[ET.Element], attribute: str, default_value: Any = None) -> List[Any]:
    """Get attribute values from user elements."""
    return [get_attrib_value_from_user(user, attribute, default_value) for user in users]

def get_attrib_value_from_user(user: ET.Element, attribute: str, default_value: Any = None) -> Any:
    """Get attribute value from user element."""
    value = user.get(attribute)
    return value if value is not None else default_value

def get_attrib_value_from_user_subsystems(user: ET.Element, attribute: str, default_value: Any = None) -> List[Any]:
    """Get attribute values from user's subsystems."""
    subsystems_elem = user.find(attrib_subsystems)
    if subsystems_elem is None:
        return []
        
    values = []
    for subsystem in subsystems_elem.findall(attrib_subsystem):
        value = subsystem.get(attribute)
        values.append(value if value is not None else default_value)
        
    return values

async def refresh_user_active_state(user: ET.Element) -> Tuple[str, float]:
    """Refresh user active state and return status and inactive time."""
    current_time = datetime.datetime.now()
    
    last_hb_time_str = get_attrib_value_from_user(user, attrib_last_heartbeat_time, 0)
    try:
        if last_hb_time_str == 0:
            return 'inactive', 0
            
        last_hb_time = datetime.datetime.fromisoformat(last_hb_time_str.replace('Z', '+00:00'))
        inactive_minutes = (current_time - last_hb_time).total_seconds() / 60
        
        if inactive_minutes <= float(30 + 1):  # Using heartbeat_rate + 1
            return 'active', 0
        elif inactive_minutes <= float(61):  # Using inactive_time
            return 'waiting', inactive_minutes
        else:
            return 'inactive', inactive_minutes
    except Exception as e:
        logger.error(f"Error refreshing user active state: {e}")
        return 'inactive', 0

async def refresh_user_real_instances(user: ET.Element, active_state: str) -> int:
    """Refresh and return user's real instance count."""
    if active_state != 'active':
        return 0
        
    hb_instances = int(get_attrib_value_from_user(user, attrib_hb_instances, 0))
    
    # Get instances from subsystems
    subsystem_instances = 0
    subsystems = user.find(attrib_subsystems)
    if subsystems is not None:
        for subsystem in subsystems.findall(attrib_subsystem):
            subsystem_hb_time_str = subsystem.get(attrib_last_heartbeat_time, '0')
            try:
                subsystem_hb_time = datetime.datetime.fromisoformat(subsystem_hb_time_str.replace('Z', '+00:00'))
                current_time = datetime.datetime.now()
                diff_minutes = (current_time - subsystem_hb_time).total_seconds() / 60
                
                if diff_minutes <= float(30 + 1):  # Using heartbeat_rate + 1
                    subsystem_instances += int(subsystem.get(attrib_hb_instances, 0))
            except:
                pass
    
    total_instances = hb_instances + subsystem_instances
    
    # Update real_instances in user data
    user_id = get_id_from_user(user)
    username = get_username_from_user(user)
    await set_user_attrib_value(user_id, username, attrib_real_instances, total_instances)
    
    return total_instances

async def add_server_gp(gp_type: str, forum_post) -> bool:
    """Add a GP to ServerData.xml."""
    try:
        with server_data_lock:
            if not check_file_exists(path_server_data):
                check_file_exists_or_create(path_server_data, "root")
                
            tree = ET.parse(path_server_data)
            root = tree.getroot()
            
            # Find or create the GP parent element
            gp_parent_type = gp_type + 's'  # Add plural 's'
            gp_parent = root.find(gp_parent_type)
            if gp_parent is None:
                gp_parent = ET.SubElement(root, gp_parent_type)
                
            # Create a new GP element
            new_gp = ET.SubElement(gp_parent, gp_type)
            new_gp.set('time', datetime.datetime.now().isoformat())
            new_gp.set('name', forum_post.name)
            new_gp.text = str(forum_post.id)
            
            tree.write(path_server_data, encoding='utf-8', xml_declaration=True)
            return True
    except Exception as e:
        logger.error(f"Error adding server GP: {e}")
        return False

async def get_server_data_gps(gp_type: str) -> List[ET.Element]:
    """Get GP data from ServerData.xml."""
    try:
        with server_data_lock:
            if not check_file_exists(path_server_data):
                return []
                
            tree = ET.parse(path_server_data)
            root = tree.getroot()
            
            # Find the GP element
            gp_parent = root.find(gp_type)
            if gp_parent is None:
                return []
                
            gp_items = []
            gp_single_type = gp_type[:-1] if gp_type.endswith('s') else gp_type  # Remove plural 's'
            
            for gp in gp_parent.findall(gp_single_type):
                gp_items.append(gp)
                
            return gp_items
    except Exception as e:
        logger.error(f"Error getting server data GPs: {e}")
        return []