# db_setup.py
import sqlite3
import os
import logging

logger = logging.getLogger("bot")

def initialize_database():
    """Initialize the database for godpack tests."""
    try:
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        # Connect to the database
        db_path = os.path.join('data', 'gpp_test.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create a template table for godpack tests
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS gpp_test_template (
            discord_id TEXT,
            timestamp TEXT,
            gp_id TEXT,
            name TEXT,
            open_slots INTEGER DEFAULT(-1),
            number_friends INTEGER DEFAULT(-1),
            PRIMARY KEY (discord_id, timestamp, gp_id)
        )''')
        
        # Commit changes and close connection
        conn.commit()
        conn.close()
        
        logger.info("Database initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        return False

def create_guild_tables():
    """Create tables for all guilds the bot is a member of."""
    try:
        # Connect to the database
        db_path = os.path.join('data', 'gpp_test.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get the list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        # Get existing guild tables
        guild_tables = [table[0] for table in tables if table[0].startswith("gpp_test_")]
        
        # Get guild IDs from bot
        # This is a placeholder - you'll need to implement a way to get all guild IDs
        guild_ids = []  # Replace with actual guild IDs
        
        # Create tables for any missing guilds
        for guild_id in guild_ids:
            table_name = f"gpp_test_{guild_id}"
            if table_name not in guild_tables:
                cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    discord_id TEXT,
                    timestamp TEXT,
                    gp_id TEXT,
                    name TEXT,
                    open_slots INTEGER DEFAULT(-1),
                    number_friends INTEGER DEFAULT(-1),
                    PRIMARY KEY (discord_id, timestamp, gp_id)
                )''')
                logger.info(f"Created table for guild: {guild_id}")
        
        # Commit changes and close connection
        conn.commit()
        conn.close()
        
        logger.info("Guild tables created successfully")
        return True
    except Exception as e:
        logger.error(f"Error creating guild tables: {e}")
        return False

def ensure_guild_table(guild_id: str):
    """Ensure a table exists for the specified guild."""
    try:
        # Connect to the database
        db_path = os.path.join('data', 'gpp_test.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        table_name = f"gpp_test_{guild_id}"
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            discord_id TEXT,
            timestamp TEXT,
            gp_id TEXT,
            name TEXT,
            open_slots INTEGER DEFAULT(-1),
            number_friends INTEGER DEFAULT(-1),
            PRIMARY KEY (discord_id, timestamp, gp_id)
        )''')
        
        # Commit changes and close connection
        conn.commit()
        conn.close()
        
        logger.info(f"Ensured table exists for guild: {guild_id}")
        return True
    except Exception as e:
        logger.error(f"Error ensuring guild table: {e}")
        return False

if __name__ == "__main__":
    # Set up basic logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize the database
    initialize_database()