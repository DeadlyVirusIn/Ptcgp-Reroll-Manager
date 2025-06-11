import sqlite3
import os
import logging
import sys
from typing import List, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def ensure_data_directory() -> bool:
    """Ensure data directory exists"""
    try:
        os.makedirs('data', exist_ok=True)
        logger.info("Data directory ensured")
        return True
    except Exception as e:
        logger.error(f"Failed to create data directory: {e}")
        return False

def test_database_access() -> bool:
    """Test if we can create and access database"""
    try:
        db_path = os.path.join('data', 'test_access.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.close()
        
        # Clean up test database
        if os.path.exists(db_path):
            os.remove(db_path)
            
        logger.info("Database access test successful")
        return True
    except Exception as e:
        logger.error(f"Database access test failed: {e}")
        return False

def initialize_database() -> bool:
    """Initialize the main database for godpack tests and bot data"""
    try:
        if not ensure_data_directory():
            return False
            
        if not test_database_access():
            return False
        
        # Connect to the main database
        db_path = os.path.join('data', 'gpp_test.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Enable foreign keys
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # Create a template table for godpack tests
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS gpp_test_template (
            discord_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            gp_id TEXT NOT NULL,
            name TEXT NOT NULL,
            open_slots INTEGER DEFAULT(-1),
            number_friends INTEGER DEFAULT(-1),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (discord_id, timestamp, gp_id)
        )''')
        
        # Create indexes for better performance
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_gpp_test_template_discord_id 
        ON gpp_test_template(discord_id)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_gpp_test_template_gp_id 
        ON gpp_test_template(gp_id)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_gpp_test_template_timestamp 
        ON gpp_test_template(timestamp)
        ''')
        
        # Commit changes and close connection
        conn.commit()
        conn.close()
        
        logger.info("Database initialized successfully")
        return True
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error during database initialization: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during database initialization: {e}")
        return False

def create_guild_table(guild_id: str) -> bool:
    """Create a table for a specific guild"""
    try:
        # Validate guild_id
        if not guild_id or not guild_id.isdigit():
            logger.error(f"Invalid guild ID: {guild_id}")
            return False
        
        # Connect to the database
        db_path = os.path.join('data', 'gpp_test.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Enable foreign keys
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # Create table for specific guild
        table_name = f"gpp_test_{guild_id}"
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            discord_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            gp_id TEXT NOT NULL,
            name TEXT NOT NULL,
            open_slots INTEGER DEFAULT(-1),
            number_friends INTEGER DEFAULT(-1),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (discord_id, timestamp, gp_id)
        )''')
        
        # Create indexes for the guild table
        cursor.execute(f'''
        CREATE INDEX IF NOT EXISTS idx_{table_name}_discord_id 
        ON {table_name}(discord_id)
        ''')
        
        cursor.execute(f'''
        CREATE INDEX IF NOT EXISTS idx_{table_name}_gp_id 
        ON {table_name}(gp_id)
        ''')
        
        cursor.execute(f'''
        CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp 
        ON {table_name}(timestamp)
        ''')
        
        # Commit changes and close connection
        conn.commit()
        conn.close()
        
        logger.info(f"Created table for guild: {guild_id}")
        return True
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error creating guild table for {guild_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error creating guild table for {guild_id}: {e}")
        return False

def ensure_guild_table(guild_id: str) -> bool:
    """Ensure a table exists for the specified guild"""
    try:
        # Validate guild_id
        if not guild_id or not guild_id.isdigit():
            logger.error(f"Invalid guild ID: {guild_id}")
            return False
        
        return create_guild_table(guild_id)
        
    except Exception as e:
        logger.error(f"Error ensuring guild table for {guild_id}: {e}")
        return False

def get_existing_guild_tables() -> List[str]:
    """Get list of existing guild tables"""
    try:
        db_path = os.path.join('data', 'gpp_test.db')
        if not os.path.exists(db_path):
            return []
            
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all table names that match guild pattern
        cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name LIKE 'gpp_test_%'
        """)
        
        tables = cursor.fetchall()
        conn.close()
        
        # Extract guild IDs from table names
        guild_ids = []
        for table in tables:
            table_name = table[0]
            if table_name.startswith('gpp_test_') and table_name != 'gpp_test_template':
                guild_id = table_name.replace('gpp_test_', '')
                if guild_id.isdigit():
                    guild_ids.append(guild_id)
        
        return guild_ids
        
    except Exception as e:
        logger.error(f"Error getting existing guild tables: {e}")
        return []

def validate_database_structure() -> Tuple[bool, List[str]]:
    """Validate database structure and return status with issues"""
    issues = []
    
    try:
        db_path = os.path.join('data', 'gpp_test.db')
        if not os.path.exists(db_path):
            issues.append("Database file does not exist")
            return False, issues
            
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if template table exists
        cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='gpp_test_template'
        """)
        
        if not cursor.fetchone():
            issues.append("Template table 'gpp_test_template' does not exist")
        
        # Check template table structure
        cursor.execute("PRAGMA table_info(gpp_test_template)")
        columns = cursor.fetchall()
        
        required_columns = {
            'discord_id', 'timestamp', 'gp_id', 'name', 
            'open_slots', 'number_friends', 'created_at'
        }
        
        existing_columns = {column[1] for column in columns}
        missing_columns = required_columns - existing_columns
        
        if missing_columns:
            issues.append(f"Missing columns in template table: {missing_columns}")
        
        conn.close()
        
        return len(issues) == 0, issues
        
    except Exception as e:
        issues.append(f"Database validation error: {e}")
        return False, issues

def main():
    """Main function for database setup"""
    print("🗄️ PTCGP Database Setup")
    print("=" * 30)
    
    # Initialize the database
    if not initialize_database():
        print("❌ Database initialization failed")
        sys.exit(1)
    
    # Validate database structure
    valid, issues = validate_database_structure()
    
    if not valid:
        print("❌ Database validation failed:")
        for issue in issues:
            print(f"   - {issue}")
        sys.exit(1)
    
    # Show existing guild tables
    existing_guilds = get_existing_guild_tables()
    if existing_guilds:
        print(f"✅ Found existing guild tables for: {', '.join(existing_guilds)}")
    else:
        print("ℹ️ No existing guild tables found")
    
    print("✅ Database setup completed successfully")
    print("\nNext steps:")
    print("1. Configure your bot token and guild ID in config.py")
    print("2. Run: python startup_migration.py")
    print("3. Start the bot: python main.py")

if __name__ == "__main__":
    main()