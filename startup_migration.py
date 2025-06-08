#!/usr/bin/env python3
"""
STARTUP MIGRATION SCRIPT
Run this FIRST before starting your enhanced bot!
This will migrate your existing XML data to the new SQLite system.
"""

import os
import sys
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_requirements():
    """Check if all required files exist"""
    required_files = [
        'config.py',
        'database_manager.py',
        'migration.py',
        'main.py'
    ]
    
    missing_files = []
    for file in required_files:
        if not Path(file).exists():
            missing_files.append(file)
    
    if missing_files:
        logger.error(f"Missing required files: {missing_files}")
        return False
    
    logger.info("✅ All required files present")
    return True

def run_migration():
    """Run the XML to SQLite migration"""
    try:
        # Check for existing XML files
        xml_files = ['UserData.xml', 'ServerData.xml']
        data_dir = Path('data')
        
        xml_found = []
        if data_dir.exists():
            xml_found.extend([f for f in xml_files if (data_dir / f).exists()])
        xml_found.extend([f for f in xml_files if Path(f).exists()])
        
        if not xml_found:
            logger.info("📁 No XML files found - this appears to be a fresh installation")
            logger.info("🎉 You can start the bot directly with: python main.py")
            return True
        
        logger.info(f"📁 Found XML files: {xml_found}")
        
        # Import and run migration
        from database_manager import DatabaseManager
        from migration import migrate_xml_to_sqlite
        
        logger.info("🗄️ Initializing new SQLite database...")
        db_manager = DatabaseManager()
        
        logger.info("🔄 Starting XML to SQLite migration...")
        success = migrate_xml_to_sqlite(db_manager)
        
        if success:
            logger.info("✅ Migration completed successfully!")
            logger.info("")
            logger.info("📋 Next steps:")
            logger.info("1. Your XML files have been preserved")
            logger.info("2. All data is now in the SQLite database")
            logger.info("3. Start the enhanced bot: python main.py")
            logger.info("4. Check the logs for any issues")
            return True
        else:
            logger.error("❌ Migration failed!")
            return False
            
    except ImportError as e:
        logger.error(f"❌ Import error: {e}")
        logger.error("Make sure all the enhanced bot files are in the same directory")
        return False
    except Exception as e:
        logger.error(f"❌ Migration error: {e}")
        return False

def check_config():
    """Check if config.py has required settings"""
    try:
        import config
        
        required_settings = ['token', 'guild_id']
        missing_settings = []
        
        for setting in required_settings:
            if not hasattr(config, setting) or not getattr(config, setting):
                missing_settings.append(setting)
        
        if missing_settings:
            logger.warning(f"⚠️ Missing required config settings: {missing_settings}")
            logger.warning("Please update config.py with your Discord token and guild ID")
            return False
        
        logger.info("✅ Config validation passed")
        return True
        
    except ImportError:
        logger.error("❌ Cannot import config.py")
        return False
    except Exception as e:
        logger.error(f"❌ Config validation error: {e}")
        return False

def create_directories():
    """Create necessary directories"""
    directories = ['data', 'logs', 'backups', 'plots', 'exports']
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        logger.info(f"📁 Created directory: {directory}")

def main():
    """Main migration function"""
    logger.info("🚀 PTCGP Enhanced Bot - Startup Migration")
    logger.info("=" * 50)
    
    # Step 1: Check requirements
    if not check_requirements():
        sys.exit(1)
    
    # Step 2: Create directories
    create_directories()
    
    # Step 3: Check config
    if not check_config():
        logger.error("❌ Please fix config.py and run this script again")
        sys.exit(1)
    
    # Step 4: Run migration
    if not run_migration():
        sys.exit(1)
    
    logger.info("")
    logger.info("🎉 Startup migration completed successfully!")
    logger.info("You can now start your enhanced bot with: python main.py")

if __name__ == "__main__":
    main()