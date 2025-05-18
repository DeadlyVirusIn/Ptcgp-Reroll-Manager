import datetime
import re
import asyncio
import math
import os
from typing import List, Optional, Any, Dict, Tuple, Union

# Import config variables
import config

# ============================= UTILITY FUNCTIONS =============================

def localize(fr: str, en: str) -> str:
    """Return localized text based on language setting."""
    if config.english_language:
        return en
    else:
        return fr

def format_number_to_k(num: Union[int, float]) -> str:
    """Format number to K or M format if large enough."""
    num = float(num)  # Convert to float for safety
    if num >= 1000000:
        return f"{num / 1000000:.1f}M"
    elif num >= 1000:
        return f"{num / 1000:.1f}k"
    else:
        return str(int(num))

def format_minutes_to_days(minutes: Union[int, float]) -> str:
    """Format minutes to days."""
    minutes = float(minutes)  # Convert to float for safety
    days = minutes / (24 * 60)  # 1 day = 24 hours * 60 minutes
    return f"{days:.1f} days"

def sum_int_array(array_numbers: List[Union[int, str]]) -> int:
    """Sum an array of integers, filtering out undefined values."""
    return sum([int(x) for x in array_numbers if x is not None and str(x).strip()])

def sum_float_array(array_numbers: List[Union[float, str]]) -> float:
    """Sum an array of floats."""
    return sum([float(x) for x in array_numbers if x is not None and str(x).strip()])

def round_to_one_decimal(num: float) -> float:
    """Round a number to one decimal place."""
    return round(num * 10) / 10

def round_to_two_decimals(num: float) -> float:
    """Round a number to two decimal places."""
    return round(num * 100) / 100

def count_digits(string: str) -> int:
    """Count the number of digits in a string."""
    return len([char for char in string if char.isdigit()])

def extract_numbers(string: str) -> List[int]:
    """Extract numbers from a string."""
    if not string:
        return [0]
    numbers = [int(n) for n in re.findall(r'\d+', string)]
    return numbers if numbers else [0]

def extract_two_star_amount(input_string: str) -> int:
    """Extract two star amount from a string like [3/5]."""
    match = re.search(r'\[(\d+)\/5\]', input_string)
    if match and match.group(1):
        return int(match.group(1))
    return 5

def replace_last_occurrence(string: str, search: str, replace: str) -> str:
    """Replace the last occurrence of a substring."""
    return string[::-1].replace(search[::-1], replace[::-1], 1)[::-1]

def replace_miss_count(string: str, new_count: int) -> str:
    """Replace miss count in format [ X miss / Y ]."""
    pattern = r'(\[ )(\d+)( miss / \d+ \])'
    return re.sub(pattern, f'\\1{new_count}\\3', string)

def replace_miss_needed(string: str, new_count: int) -> str:
    """Replace miss needed in format [ X miss / Y ]."""
    pattern = r'(\[ \d+ miss / )(\d+)( \])'
    return re.sub(pattern, f'\\1{new_count}\\3', string)

def is_numbers(input_string: str) -> bool:
    """Check if a string contains only numbers."""
    return input_string.isdigit()

def convert_min_to_ms(minutes: float) -> int:
    """Convert minutes to milliseconds."""
    return int(minutes * 60000)

def convert_ms_to_min(milliseconds: float) -> float:
    """Convert milliseconds to minutes."""
    return milliseconds / 60000

def split_multi(string: str, tokens: List[str]) -> List[str]:
    """Split a string by multiple tokens."""
    temp_char = tokens[0]  # Use the first token as a temporary join character
    for i in range(1, len(tokens)):
        string = string.replace(tokens[i], temp_char)
    return string.split(temp_char)

async def send_received_message(content: str, interaction=None, 
                               timeout: int = 0, bot=None, channel_id: str = None) -> None:
    """Send a message and optionally delete it after a timeout."""
    if channel_id is None:
        channel_id = config.channel_id_commands
        
    if interaction:
        try:
            if hasattr(interaction, 'followup') and callable(getattr(interaction, 'followup').send):
                message = await interaction.followup.send(content=content)
            elif hasattr(interaction, 'response') and callable(getattr(interaction, 'response').send_message):
                await interaction.response.send_message(content=content, ephemeral=True)
                return
            else:
                # Fallback to replying to the interaction
                message = await interaction.channel.send(content=content)
        except Exception as e:
            print(f"Error sending received message: {e}")
            return
        
        if float(timeout) > 0:
            await asyncio.sleep(float(timeout))
            try:
                await message.delete()
            except Exception:
                print('❗️ Tried to delete nonexistent message')
    else:
        if bot:
            await send_channel_message(bot, channel_id, content, timeout)

async def send_channel_message(bot, channel_id: str, content: str, timeout: int = 0) -> None:
    """Send a message to a channel and optionally delete it after a timeout."""
    channel = bot.get_channel(int(channel_id))
    if not channel:
        print(f"❌ Channel with ID {channel_id} not found")
        return
        
    message = await channel.send(content=content)
    
    if timeout > 0:
        await asyncio.sleep(timeout)
        try:
            await message.delete()
        except Exception:
            print('❗️ Tried to delete nonexistent message')

async def bulk_delete_messages(channel, number_of_messages: int = 100) -> None:
    """Bulk delete messages from a channel."""
    if not hasattr(channel, 'purge') or not callable(channel.purge):
        print(f"Channel {channel} does not support message purging")
        return
        
    try:
        total_deleted = 0
        
        while total_deleted < number_of_messages:
            remaining = number_of_messages - total_deleted
            limit = min(remaining, 100)
            
            messages = await channel.history(limit=limit).flatten()
            
            # Filter out pinned messages
            messages_to_delete = [message for message in messages if not message.pinned]
            
            if not messages_to_delete:
                break
                
            deleted = await channel.purge(limit=limit, check=lambda m: not m.pinned)
            total_deleted += len(deleted)
            
    except Exception as e:
        print(f'❌ ERROR deleting messages: {e}')

def color_text(text: str, color: str) -> str:
    """Add ANSI color to text for console output."""
    if color == "gray":
        return f"\033[2;30m{text}\033[0m"
    elif color == "red":
        return f"\033[2;31m{text}\033[0m"
    elif color == "green":
        return f"\033[2;32m{text}\033[0m"
    elif color == "yellow":
        return f"\033[2;33m{text}\033[0m"
    elif color == "blue":
        return f"\033[2;34m{text}\033[0m"
    elif color == "pink":
        return f"\033[2;35m{text}\033[0m"
    elif color == "cyan":
        return f"\033[2;36m{text}\033[0m"
    else:
        return text

def add_text_bar(text: str, target_length: int, color: bool = True) -> str:
    """Add a bar with spacing to make text align to target length."""
    current_length = len(text)
    spaces_needed = max(target_length - current_length - 1, 0)
    spaces = ' ' * spaces_needed
    bar = color_text('|', "gray") if color else '|'
    return text + spaces + bar

def format_number_with_spaces(number: Union[int, float], total_length: int) -> str:
    """Format a number with spaces to reach a specific length."""
    number_str = str(number)
    current_length = len(number_str.replace('.', ''))
    spaces_needed = max(total_length - current_length, 0)
    formatted_str = number_str + '⠀' * spaces_needed  # Using Unicode Braille Pattern Blank
    return formatted_str

def get_random_string_from_array(array: List[str]) -> str:
    """Get a random string from an array."""
    if not array:
        raise ValueError("Array should not be empty")
    import random
    return random.choice(array)

async def get_oldest_message(channel) -> Optional[Any]:
    """Get the oldest message in a channel."""
    try:
        if hasattr(channel, 'history') and callable(channel.history):
            history = await channel.history(limit=1, oldest_first=True).flatten()
            return history[0] if history else None
        return None
    except Exception as e:
        print(f'❌ ERROR TRYING TO ACCESS OLDER MESSAGE: {e}')
        return None

async def wait(seconds: float) -> None:
    """Asynchronous wait function."""
    await asyncio.sleep(seconds)

def replace_any_logo_with(text: str, new_logo: str) -> str:
    """Replace any logo with a new one."""
    edited_text = (text.replace(config.text_dead_logo, new_logo)
                      .replace(config.text_not_liked_logo, new_logo)
                      .replace(config.text_waiting_logo, new_logo)
                      .replace(config.text_liked_logo, new_logo)
                      .replace(config.text_verified_logo, new_logo))
    return edited_text

def normalize_ocr(string: str) -> str:
    """Replace characters that look the same for OCR comparison."""
    replacements = {'D': 'O', 'B': 'O', '0': 'O', '1': 'I', 'l': 'I'}
    return ''.join([replacements.get(char, char) for char in string.upper()])

async def get_lasts_anti_cheat_messages(bot) -> Dict[str, Any]:
    """Get the most recent AntiCheat messages that share a prefix."""
    try:
        channel_anti_cheat = bot.get_channel(int(config.channel_id_anticheat))
        if not channel_anti_cheat:
            return {"prefix": "", "messages": []}
            
        messages = await channel_anti_cheat.history(limit=100).flatten()
        
        anti_cheat_time_threshold = 30 + config.anti_cheat_rate
        
        # Check if messages are less than threshold minutes ago
        threshold_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=anti_cheat_time_threshold)
        recent_messages = [msg for msg in messages if msg.created_at > threshold_time]
        
        # Find the messages that start with the same numeric sequence
        messages_by_prefix = {}
        
        for msg in recent_messages:
            # Extract the initial numeric sequence
            match = re.match(r'^\d+', msg.content)
            if match:
                prefix = match.group(0)
                
                # Group messages by their prefix
                if prefix not in messages_by_prefix:
                    messages_by_prefix[prefix] = []
                messages_by_prefix[prefix].append(msg)
        
        # Find the prefix with the highest total length of string
        max_length_prefix = ''
        max_length = 0
        
        for prefix, prefix_messages in messages_by_prefix.items():
            total_length = sum(len(msg.content) for msg in prefix_messages)
            if total_length > max_length:
                max_length = total_length
                max_length_prefix = prefix
        
        result_messages = messages_by_prefix.get(max_length_prefix, [])
        if result_messages:
            result_messages = result_messages[:int(30 / config.anti_cheat_rate)]
        
        return {
            "prefix": max_length_prefix,
            "messages": result_messages
        }
    except Exception as e:
        print(f"❌ ERROR getting AntiCheat messages: {e}")
        return {"prefix": "", "messages": []}

def update_average(current_average: float, count: int, new_value: float) -> float:
    """Update a running average with a new value."""
    if count <= 1:
        return new_value
    new_average = (current_average * (count - 1) + new_value) / count
    return new_average