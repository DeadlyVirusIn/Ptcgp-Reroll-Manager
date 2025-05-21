# GP Test Utilities for managing godpack test data
import json
import datetime
import logging
import os
import sqlite3
import math
from typing import Optional, Dict, List, Any

logger = logging.getLogger("bot")

# Database file
DB_FILE = 'gpp_test.db'

# Enum for test states
class TestState:
    MISS = "MISS"
    NOSHOW = "NOSHOW"

# Dictionary to cache database connections
db_connections = {}

async def get_db_connection(guild_id: str) -> sqlite3.Connection:
    """Get a database connection for a specific guild."""
    if guild_id in db_connections:
        return db_connections[guild_id]
    
    # Create database directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Connect to the database
    db_path = os.path.join('data', DB_FILE)
    conn = sqlite3.connect(db_path)
    
    # Create table if it doesn't exist
    table_name = f"gpp_test_{guild_id}"
    conn.execute(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        discord_id TEXT,
        timestamp TEXT,
        gp_id TEXT,
        name TEXT,
        open_slots INTEGER DEFAULT(-1),
        number_friends INTEGER DEFAULT(-1),
        PRIMARY KEY (discord_id, timestamp, gp_id)
    )''')
    
    db_connections[guild_id] = conn
    return conn

def factorial(n: int) -> int:
    """Calculate factorial."""
    if n == 0 or n == 1:
        return 1
    result = 1
    for i in range(2, n + 1):
        result *= i
    return result

def combinations(n: int, k: int) -> int:
    """Calculate combinations (n choose k)."""
    # Handle edge cases
    if k < 0 or k > n:
        return 0
    if k == 0 or k == n:
        return 1
    
    # Optimize calculation for large numbers
    if k > n - k:
        k = n - k
    
    # Calculate n choose k
    result = 1
    for i in range(1, k + 1):
        result *= (n - (k - i))
        result /= i
    
    return round(result)

def compute_chance_noshow_as_dud(open_slots: int, number_friends: int) -> float:
    """Compute the probability that a NoShow counts as a dud."""
    # Ensure minimum value for number_friends
    if number_friends < 6:
        number_friends = 6
    
    # Handle edge cases
    if open_slots < 0 or number_friends < 0:
        return 1.0
    if open_slots >= number_friends:
        return 1.0
    if number_friends - (4 - open_slots) - 1 < open_slots:
        return 1.0
    
    # Main calculation
    try:
        numerator = combinations(number_friends - (4 - open_slots) - 1, open_slots)
        denominator = combinations(number_friends - (4 - open_slots), open_slots)
        return 1.0 - (numerator / denominator)
    except Exception as e:
        logger.error(f'Error in combinatorial calculation: {e}')
        return 1.0  # Safe default

async def compute_prob(guild_id: str, godpack_id: str) -> float:
    """Calculate probability of godpack being alive based on tests."""
    db = await get_db_connection(guild_id)
    table_name = f"gpp_test_{guild_id}"
    
    cursor = db.cursor()
    cursor.execute(f"SELECT * FROM {table_name} WHERE gp_id = ?", (godpack_id,))
    all_tests = cursor.fetchall()
    
    # Convert to dictionary for easier processing
    tests = []
    for test in all_tests:
        tests.append({
            "discord_id": test[0],
            "timestamp": test[1],
            "gp_id": test[2],
            "name": test[3],
            "open_slots": test[4],
            "number_friends": test[5]
        })
    
    # Initialize probability calculation
    prob_alive = 1.0
    member_base_chance = {}
    
    # Process each test
    for test in tests:
        # Initialize base chance if this is the first test for this user
        if test["discord_id"] not in member_base_chance:
            # Using default value of 5 for pack_number
            member_base_chance[test["discord_id"]] = 5
        
        # If user's chance is already 0, the whole GP probability is 0
        if member_base_chance[test["discord_id"]] <= 0:
            return 0.0
        
        # Determine number of duds based on test type
        number_duds = 0
        if test["name"] == TestState.MISS:
            number_duds = 1.0
        elif test["name"] == TestState.NOSHOW:
            number_duds = compute_chance_noshow_as_dud(test["open_slots"], test["number_friends"])
        
        # Update probability
        prob_alive = prob_alive * \
            max(member_base_chance[test["discord_id"]] - number_duds, 0.0) / \
            member_base_chance[test["discord_id"]]
        
        # Update member's remaining chance
        member_base_chance[test["discord_id"]] = member_base_chance[test["discord_id"]] - number_duds
    
    logger.info(f"Computed {prob_alive} chance of being alive with individual probabilities {member_base_chance}")
    return prob_alive * 100.0

async def add_noshow(guild_id: str, godpack_id: str, user_id: str, open_slots: int, number_friends: int) -> float:
    """Add a NoShow test for a godpack."""
    db = await get_db_connection(guild_id)
    table_name = f"gpp_test_{guild_id}"
    timestamp = datetime.datetime.now().isoformat()
    
    # Insert NoShow record
    cursor = db.cursor()
    cursor.execute(
        f"INSERT OR IGNORE INTO {table_name} (discord_id, timestamp, gp_id, name, open_slots, number_friends) VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, timestamp, godpack_id, TestState.NOSHOW, open_slots, number_friends)
    )
    db.commit()
    
    # Compute and return updated probability
    return await compute_prob(guild_id, godpack_id)

async def reset_test(guild_id: str, godpack_id: str, user_id: str) -> float:
    """Reset all tests for a user on a specific godpack."""
    db = await get_db_connection(guild_id)
    table_name = f"gpp_test_{guild_id}"
    
    # Delete all tests for this user and godpack
    cursor = db.cursor()
    cursor.execute(
        f"DELETE FROM {table_name} WHERE gp_id = ? AND discord_id = ?",
        (godpack_id, user_id)
    )
    db.commit()
    
    # Compute and return updated probability
    return await compute_prob(guild_id, godpack_id)

async def add_miss(guild_id: str, godpack_id: str, user_id: str) -> float:
    """Add a Miss test for a godpack."""
    db = await get_db_connection(guild_id)
    table_name = f"gpp_test_{guild_id}"
    timestamp = datetime.datetime.now().isoformat()
    
    # Insert Miss record
    cursor = db.cursor()
    cursor.execute(
        f"INSERT OR IGNORE INTO {table_name} (discord_id, timestamp, gp_id, name) VALUES (?, ?, ?, ?)",
        (user_id, timestamp, godpack_id, TestState.MISS)
    )
    db.commit()
    
    # Compute and return updated probability
    return await compute_prob(guild_id, godpack_id)

async def get_tests_for_godpack(guild_id: str, godpack_id: str) -> List[Dict[str, Any]]:
    """Get all tests for a specific godpack."""
    db = await get_db_connection(guild_id)
    table_name = f"gpp_test_{guild_id}"
    
    cursor = db.cursor()
    cursor.execute(f"SELECT * FROM {table_name} WHERE gp_id = ?", (godpack_id,))
    tests = cursor.fetchall()
    
    # Convert to list of dictionaries
    result = []
    for test in tests:
        result.append({
            "discord_id": test[0],
            "timestamp": test[1],
            "gp_id": test[2],
            "name": test[3],
            "open_slots": test[4],
            "number_friends": test[5]
        })
    
    return result

async def get_test_summary(guild_id: str, godpack_id: str) -> str:
    """Get a human-readable summary of tests for a godpack."""
    tests = await get_tests_for_godpack(guild_id, godpack_id)
    
    if not tests:
        return "No tests recorded for this godpack."
    
    summary = f"**Test Summary for GodPack {godpack_id}:**\n\n"
    
    # Group tests by user
    tests_by_user = {}
    for test in tests:
        if test["discord_id"] not in tests_by_user:
            tests_by_user[test["discord_id"]] = []
        tests_by_user[test["discord_id"]].append(test)
    
    # Generate summary for each user
    for user_id, user_tests in tests_by_user.items():
        summary += f"**User <@{user_id}>:**\n"
        for test in user_tests:
            timestamp = datetime.datetime.fromisoformat(test["timestamp"].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M')
            
            if test["name"] == TestState.MISS:
                summary += f"  - MISS at {timestamp}\n"
            elif test["name"] == TestState.NOSHOW:
                dud_chance = compute_chance_noshow_as_dud(test["open_slots"], test["number_friends"])
                summary += f"  - NOSHOW at {timestamp} (Slots: {test['open_slots']}, Friends: {test['number_friends']}, Dud Chance: {(dud_chance * 100):.1f}%)\n"
        summary += '\n'
    
    # Calculate and add overall probability
    prob = await compute_prob(guild_id, godpack_id)
    summary += f"\n**Overall probability: {prob:.1f}%**"
    
    return summary

def extract_godpack_id_from_message(message) -> Optional[str]:
    """Extract godpack ID from a message."""
    if not message or not message.content:
        return None
    
    # Try to find "account: 123456789" in the message
    import re
    match = re.search(r'account: (\d+)', message.content, re.IGNORECASE)
    if match and match.group(1):
        return match.group(1)
    
    # Look for alternative patterns
    alt_match = re.search(r'ID:?\s*(\d+)', message.content, re.IGNORECASE)
    if alt_match and alt_match.group(1):
        return alt_match.group(1)
    
    # If nothing else works, use the message ID as a fallback
    return str(message.id)