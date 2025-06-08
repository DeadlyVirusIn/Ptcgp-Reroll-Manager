#!/usr/bin/env python3
"""
Simple migration script to convert XML data to SQLite
Run this before starting the enhanced bot for the first time
"""

import sys
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    print("🔄 Starting XML to SQLite migration...")
    
    # Check if XML files exist
    xml_files = ['UserData.xml', 'ServerData.xml']
    found_files = [f for f in xml_files if Path(f).exists()]
    
    if not found_files:
        print("⚠️ No XML files found. If this is a fresh installation, no migration needed.")
        return
    
    print(f"📁 Found XML files: {', '.join(found_files)}")
    
    try:
        # Import the database manager and migration function
        from database_manager import DatabaseManager
        from migration import migrate_xml_to_sqlite
        
        # Initialize database
        print("📊 Initializing new SQLite database...")
        db_manager = DatabaseManager()
        
        # Run migration
        print("🔄 Migrating XML data...")
        success = migrate_xml_to_sqlite(db_manager)
        
        if success:
            print("✅ Migration completed successfully!")
            print("\n📋 Next steps:")
            print("1. Your old XML files are preserved (you can back them up)")
            print("2. Start the enhanced bot: python updated_main_bot.py")
            print("3. All your existing data is now in the SQLite database")
        else:
            print("❌ Migration failed. Check the logs for details.")
            sys.exit(1)
            
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure all the enhanced bot files are in the same directory.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Migration error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()