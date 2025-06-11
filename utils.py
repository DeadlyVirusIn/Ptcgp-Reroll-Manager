import discord
import asyncio
import re
import math
import random
import datetime
from typing import List, Dict, Any, Optional, Union
import os
import json

# LOCALIZATION AND FORMATTING UTILITIES

def localize(text: str, **kwargs) -> str:
    """Localize text with parameter substitution."""
    try:
        # Simple parameter substitution
        for key, value in kwargs.items():
            text = text.replace(f"{{{key}}}", str(value))
        return text
    except Exception as e:
        print(f"❌ Error localizing text: {e}")
        return text

def format_number_to_k(number: Union[int, float]) -> str:
    """Format large numbers to K/M notation."""
    try:
        num = float(number)
        if num >= 1000000:
            return f"{num/1000000:.1f}M"
        elif num >= 1000:
            return f"{num/1000:.1f}K"
        else:
            return str(int(num))
    except (ValueError, TypeError):
        return "0"

def format_number_with_spaces(number: Union[int, float]) -> str:
    """Format numbers with space separators (e.g., 1 234 567)."""
    try:
        return f"{int(number):,}".replace(',', ' ')
    except (ValueError, TypeError):
        return "0"

def format_minutes_to_days(minutes: Union[int, float]) -> str:
    """Convert minutes to human-readable format (days, hours, minutes)."""
    try:
        total_minutes = int(minutes)
        if total_minutes < 60:
            return f"{total_minutes}m"
        elif total_minutes < 1440:  # Less than 24 hours
            hours = total_minutes // 60
            mins = total_minutes % 60
            return f"{hours}h {mins}m" if mins > 0 else f"{hours}h"
        else:  # 24+ hours
            days = total_minutes // 1440
            remaining_minutes = total_minutes % 1440
            hours = remaining_minutes // 60
            mins = remaining_minutes % 60
            
            result = f"{days}d"
            if hours > 0:
                result += f" {hours}h"
            if mins > 0 and days < 7:  # Only show minutes if less than a week
                result += f" {mins}m"
            return result
    except (ValueError, TypeError):
        return "0m"

def format_time_duration(seconds: Union[int, float]) -> str:
    """Format seconds into human-readable duration."""
    try:
        total_seconds = int(seconds)
        if total_seconds < 60:
            return f"{total_seconds}s"
        elif total_seconds < 3600:
            minutes = total_seconds // 60
            secs = total_seconds % 60
            return f"{minutes}m {secs}s" if secs > 0 else f"{minutes}m"
        else:
            hours = total_seconds // 3600
            remaining = total_seconds % 3600
            minutes = remaining // 60
            return f"{hours}h {minutes}m" if minutes > 0 else f"{hours}h"
    except (ValueError, TypeError):
        return "0s"

# MATHEMATICAL UTILITIES

def round_to_one_decimal(number: Union[int, float]) -> float:
    """Round number to one decimal place."""
    try:
        return round(float(number), 1)
    except (ValueError, TypeError):
        return 0.0

def round_to_two_decimals(number: Union[int, float]) -> float:
    """Round number to two decimal places."""
    try:
        return round(float(number), 2)
    except (ValueError, TypeError):
        return 0.0

def sum_int_array(array: List[Union[int, str]]) -> int:
    """Sum an array of integers."""
    try:
        return sum(int(x) for x in array if str(x).isdigit())
    except (ValueError, TypeError):
        return 0

def sum_float_array(array: List[Union[float, str]]) -> float:
    """Sum an array of floats."""
    try:
        total = 0.0
        for x in array:
            try:
                total += float(x)
            except (ValueError, TypeError):
                continue
        return total
    except:
        return 0.0

def calculate_average(numbers: List[Union[int, float]]) -> float:
    """Calculate average of a list of numbers."""
    try:
        if not numbers:
            return 0.0
        valid_numbers = [float(x) for x in numbers if x is not None]
        return sum(valid_numbers) / len(valid_numbers) if valid_numbers else 0.0
    except:
        return 0.0

def calculate_percentage(part: Union[int, float], total: Union[int, float]) -> float:
    """Calculate percentage with safe division."""
    try:
        if total == 0:
            return 0.0
        return round_to_one_decimal((float(part) / float(total)) * 100)
    except:
        return 0.0

# STRING UTILITIES

def count_digits(text: str) -> int:
    """Count number of digits in a string."""
    try:
        return sum(1 for char in str(text) if char.isdigit())
    except:
        return 0

def extract_numbers(text: str) -> List[int]:
    """Extract all numbers from a string."""
    try:
        return [int(match) for match in re.findall(r'\d+', str(text))]
    except:
        return []

def extract_floats(text: str) -> List[float]:
    """Extract all floating point numbers from a string."""
    try:
        return [float(match) for match in re.findall(r'\d+\.?\d*', str(text))]
    except:
        return []

def is_numbers(text: str) -> bool:
    """Check if string contains only numbers."""
    try:
        return str(text).replace('.', '').replace('-', '').isdigit()
    except:
        return False

def clean_string(text: str) -> str:
    """Clean string by removing special characters."""
    try:
        return re.sub(r'[^a-zA-Z0-9\s]', '', str(text))
    except:
        return ""

def normalize_string(text: str) -> str:
    """Normalize string to lowercase and remove extra spaces."""
    try:
        return ' '.join(str(text).lower().split())
    except:
        return ""

def split_multi(text: str, delimiters: List[str]) -> List[str]:
    """Split string by multiple delimiters."""
    try:
        pattern = '|'.join(map(re.escape, delimiters))
        return [part.strip() for part in re.split(pattern, str(text)) if part.strip()]
    except:
        return [str(text)]

def replace_last_occurrence(text: str, old: str, new: str) -> str:
    """Replace the last occurrence of a substring."""
    try:
        text_str = str(text)
        old_str = str(old)
        new_str = str(new)
        
        index = text_str.rfind(old_str)
        if index == -1:
            return text_str
        return text_str[:index] + new_str + text_str[index + len(old_str):]
    except:
        return str(text)

def replace_miss_count(text: str, count: int) -> str:
    """Replace miss count placeholder in text."""
    try:
        return str(text).replace('{miss_count}', str(count))
    except:
        return str(text)

def replace_miss_needed(text: str, needed: int) -> str:
    """Replace miss needed placeholder in text."""
    try:
        return str(text).replace('{miss_needed}', str(needed))
    except:
        return str(text)

def get_random_string_from_array(array: List[str]) -> str:
    """Get random string from array."""
    try:
        if not array:
            return ""
        return random.choice(array)
    except:
        return ""

# TIME UTILITIES

def convert_min_to_ms(minutes: Union[int, float]) -> int:
    """Convert minutes to milliseconds."""
    try:
        return int(float(minutes) * 60 * 1000)
    except:
        return 0

def convert_ms_to_min(milliseconds: Union[int, float]) -> float:
    """Convert milliseconds to minutes."""
    try:
        return float(milliseconds) / (60 * 1000)
    except:
        return 0.0

def convert_seconds_to_minutes(seconds: Union[int, float]) -> float:
    """Convert seconds to minutes."""
    try:
        return float(seconds) / 60
    except:
        return 0.0

def get_timestamp() -> str:
    """Get current timestamp as ISO string."""
    return datetime.datetime.now().isoformat()

def format_timestamp(timestamp: str, format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format timestamp string."""
    try:
        dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        return dt.strftime(format_str)
    except:
        return timestamp

def time_ago(timestamp: str) -> str:
    """Get human-readable time ago string."""
    try:
        dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        now = datetime.datetime.now(dt.tzinfo)
        diff = now - dt
        
        if diff.days > 0:
            return f"{diff.days}d ago"
        elif diff.seconds > 3600:
            hours = diff.seconds // 3600
            return f"{hours}h ago"
        elif diff.seconds > 60:
            minutes = diff.seconds // 60
            return f"{minutes}m ago"
        else:
            return "just now"
    except:
        return "unknown"

# DISCORD UTILITIES

async def send_received_message(channel, content: str) -> Optional[discord.Message]:
    """Send a message to a channel."""
    try:
        return await channel.send(content)
    except discord.errors.Forbidden:
        print(f"❌ No permission to send message to channel {channel.id}")
        return None
    except discord.errors.HTTPException as e:
        print(f"❌ HTTP error sending message: {e}")
        return None
    except Exception as e:
        print(f"❌ Error sending message: {e}")
        return None

async def send_channel_message(channel, content: str = None, embed: discord.Embed = None, file: discord.File = None) -> Optional[discord.Message]:
    """Send a message with optional embed and file to a channel."""
    try:
        return await channel.send(content=content, embed=embed, file=file)
    except discord.errors.Forbidden:
        print(f"❌ No permission to send message to channel {channel.id}")
        return None
    except discord.errors.HTTPException as e:
        print(f"❌ HTTP error sending message: {e}")
        return None
    except Exception as e:
        print(f"❌ Error sending message: {e}")
        return None

async def send_dm_message(user, content: str = None, embed: discord.Embed = None) -> Optional[discord.Message]:
    """Send a direct message to a user."""
    try:
        return await user.send(content=content, embed=embed)
    except discord.errors.Forbidden:
        print(f"❌ Cannot send DM to user {user.id}")
        return None
    except discord.errors.HTTPException as e:
        print(f"❌ HTTP error sending DM: {e}")
        return None
    except Exception as e:
        print(f"❌ Error sending DM: {e}")
        return None

async def bulk_delete_messages(channel, limit: int = 100) -> int:
    """Bulk delete messages from a channel."""
    try:
        deleted = await channel.purge(limit=limit)
        return len(deleted)
    except discord.errors.Forbidden:
        print(f"❌ No permission to delete messages in channel {channel.id}")
        return 0
    except discord.errors.HTTPException as e:
        print(f"❌ HTTP error deleting messages: {e}")
        return 0
    except Exception as e:
        print(f"❌ Error deleting messages: {e}")
        return 0

async def get_oldest_message(channel, limit: int = 100) -> Optional[discord.Message]:
    """Get the oldest message from a channel."""
    try:
        messages = []
        async for message in channel.history(limit=limit, oldest_first=True):
            messages.append(message)
        return messages[0] if messages else None
    except Exception as e:
        print(f"❌ Error getting oldest message: {e}")
        return None

async def get_user_by_id(bot, user_id: int) -> Optional[discord.User]:
    """Get a user by their ID."""
    try:
        return await bot.fetch_user(user_id)
    except discord.errors.NotFound:
        print(f"❌ User {user_id} not found")
        return None
    except Exception as e:
        print(f"❌ Error fetching user {user_id}: {e}")
        return None

# TEXT PROCESSING UTILITIES

def color_text(text: str, color_code: str) -> str:
    """Add color codes to text for terminal output."""
    try:
        colors = {
            'red': '\033[91m',
            'green': '\033[92m',
            'yellow': '\033[93m',
            'blue': '\033[94m',
            'purple': '\033[95m',
            'cyan': '\033[96m',
            'white': '\033[97m',
            'reset': '\033[0m'
        }
        return f"{colors.get(color_code, '')}{text}{colors['reset']}"
    except:
        return str(text)

def add_text_bar(text: str, char: str = "=", length: int = 50) -> str:
    """Add a text bar above and below text."""
    try:
        bar = char * length
        return f"{bar}\n{text}\n{bar}"
    except:
        return str(text)

def create_progress_bar(current: int, total: int, length: int = 20) -> str:
    """Create a text-based progress bar."""
    try:
        if total == 0:
            return "[" + " " * length + "] 0%"
        
        progress = min(current / total, 1.0)
        filled_length = int(length * progress)
        bar = "█" * filled_length + "░" * (length - filled_length)
        percentage = int(progress * 100)
        
        return f"[{bar}] {percentage}%"
    except:
        return "[" + " " * length + "] 0%"

def truncate_text(text: str, max_length: int, suffix: str = "...") -> str:
    """Truncate text to specified length with suffix."""
    try:
        text_str = str(text)
        if len(text_str) <= max_length:
            return text_str
        return text_str[:max_length - len(suffix)] + suffix
    except:
        return str(text)

def replace_any_logo_with(text: str, logo_replacements: Dict[str, str]) -> str:
    """Replace any logos/patterns in text with specified replacements."""
    try:
        result = str(text)
        for pattern, replacement in logo_replacements.items():
            result = result.replace(pattern, replacement)
        return result
    except:
        return str(text)

def normalize_ocr(text: str) -> str:
    """Normalize OCR text by fixing common recognition errors."""
    try:
        # Common OCR fixes
        replacements = {
            '0': 'O',  # Zero to letter O
            '1': 'I',  # One to letter I
            '5': 'S',  # Five to letter S
            '8': 'B',  # Eight to letter B
        }
        
        result = str(text).upper()
        for old, new in replacements.items():
            result = result.replace(old, new)
            
        # Remove extra spaces
        result = ' '.join(result.split())
        
        return result
    except:
        return str(text)

# FILE AND DATA UTILITIES

def load_json_file(file_path: str) -> Dict[str, Any]:
    """Load JSON file safely."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ JSON file not found: {file_path}")
        return {}
    except json.JSONDecodeError as e:
        print(f"❌ Error decoding JSON file {file_path}: {e}")
        return {}
    except Exception as e:
        print(f"❌ Error loading JSON file {file_path}: {e}")
        return {}

def save_json_file(file_path: str, data: Dict[str, Any]) -> bool:
    """Save data to JSON file safely."""
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"❌ Error saving JSON file {file_path}: {e}")
        return False

def ensure_directory_exists(directory_path: str) -> bool:
    """Ensure a directory exists, create if it doesn't."""
    try:
        os.makedirs(directory_path, exist_ok=True)
        return True
    except Exception as e:
        print(f"❌ Error creating directory {directory_path}: {e}")
        return False

# VALIDATION UTILITIES

def validate_discord_id(discord_id: str) -> bool:
    """Validate Discord ID format."""
    try:
        return discord_id.isdigit() and len(discord_id) >= 17 and len(discord_id) <= 19
    except:
        return False

def validate_email(email: str) -> bool:
    """Basic email validation."""
    try:
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, str(email)))
    except:
        return False

def validate_url(url: str) -> bool:
    """Basic URL validation."""
    try:
        pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        return bool(re.match(pattern, str(url)))
    except:
        return False

def sanitize_filename(filename: str) -> str:
    """Sanitize filename by removing invalid characters."""
    try:
        # Remove invalid characters for filename
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', str(filename))
        # Remove extra spaces and dots
        sanitized = re.sub(r'\.+', '.', sanitized)
        sanitized = re.sub(r'\s+', ' ', sanitized).strip()
        # Limit length
        return sanitized[:255] if sanitized else "unnamed"
    except:
        return "unnamed"

# ASYNC UTILITIES

async def wait(seconds: Union[int, float]):
    """Async wait function."""
    try:
        await asyncio.sleep(float(seconds))
    except:
        pass

async def run_with_timeout(coro, timeout_seconds: float):
    """Run coroutine with timeout."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout_seconds)
    except asyncio.TimeoutError:
        print(f"❌ Operation timed out after {timeout_seconds} seconds")
        return None
    except Exception as e:
        print(f"❌ Error in timed operation: {e}")
        return None

# ANTI-CHEAT UTILITIES

def get_lasts_anti_cheat_messages(messages: List[str], count: int = 5) -> List[str]:
    """Get the last N anti-cheat messages."""
    try:
        return messages[-count:] if len(messages) >= count else messages
    except:
        return []

def detect_spam_pattern(messages: List[str], threshold: int = 3) -> bool:
    """Detect if messages show spam pattern."""
    try:
        if len(messages) < threshold:
            return False
            
        # Check for identical messages
        recent_messages = messages[-threshold:]
        unique_messages = set(recent_messages)
        
        # If most messages are the same, it's likely spam
        return len(unique_messages) <= threshold // 2
    except:
        return False

def calculate_message_similarity(msg1: str, msg2: str) -> float:
    """Calculate similarity between two messages (0-1)."""
    try:
        msg1_clean = normalize_string(msg1)
        msg2_clean = normalize_string(msg2)
        
        if not msg1_clean or not msg2_clean:
            return 0.0
            
        # Simple character-based similarity
        common_chars = sum(1 for c in msg1_clean if c in msg2_clean)
        total_chars = max(len(msg1_clean), len(msg2_clean))
        
        return common_chars / total_chars if total_chars > 0 else 0.0
    except:
        return 0.0

# RATE LIMITING UTILITIES

class RateLimiter:
    """Simple rate limiter for bot operations."""
    
    def __init__(self):
        self.requests = {}
    
    def is_rate_limited(self, key: str, limit: int, window_seconds: int) -> bool:
        """Check if a key is rate limited."""
        try:
            now = datetime.datetime.now()
            
            if key not in self.requests:
                self.requests[key] = []
            
            # Clean old requests
            self.requests[key] = [
                req_time for req_time in self.requests[key]
                if (now - req_time).total_seconds() < window_seconds
            ]
            
            # Check if limit exceeded
            if len(self.requests[key]) >= limit:
                return True
            
            # Add current request
            self.requests[key].append(now)
            return False
        except:
            return False

# Create global rate limiter instance
rate_limiter = RateLimiter()

# EXPORT ALL UTILITY FUNCTIONS
__all__ = [
    # Formatting
    'localize', 'format_number_to_k', 'format_number_with_spaces', 'format_minutes_to_days',
    'format_time_duration', 'format_timestamp', 'time_ago',
    
    # Math
    'round_to_one_decimal', 'round_to_two_decimals', 'sum_int_array', 'sum_float_array',
    'calculate_average', 'calculate_percentage',
    
    # String processing
    'count_digits', 'extract_numbers', 'extract_floats', 'is_numbers', 'clean_string',
    'normalize_string', 'split_multi', 'replace_last_occurrence', 'replace_miss_count',
    'replace_miss_needed', 'get_random_string_from_array', 'truncate_text',
    'replace_any_logo_with', 'normalize_ocr',
    
    # Time
    'convert_min_to_ms', 'convert_ms_to_min', 'convert_seconds_to_minutes', 'get_timestamp',
    
    # Discord
    'send_received_message', 'send_channel_message', 'send_dm_message', 'bulk_delete_messages',
    'get_oldest_message', 'get_user_by_id',
    
    # Text processing
    'color_text', 'add_text_bar', 'create_progress_bar',
    
    # File operations
    'load_json_file', 'save_json_file', 'ensure_directory_exists',
    
    # Validation
    'validate_discord_id', 'validate_email', 'validate_url', 'sanitize_filename',
    
    # Async
    'wait', 'run_with_timeout',
    
    # Anti-cheat
    'get_lasts_anti_cheat_messages', 'detect_spam_pattern', 'calculate_message_similarity',
    
    # Rate limiting
    'RateLimiter', 'rate_limiter'
]

print("✅ Enhanced utils.py loaded successfully")