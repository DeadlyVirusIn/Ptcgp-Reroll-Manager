import os
import sys
import logging
from typing import List, Tuple, Dict

def validate_python_modules() -> Tuple[List[str], List[str]]:
    """Validate required Python modules"""
    required_modules = [
        'discord', 'matplotlib', 'numpy', 'pandas', 
        'sqlite3', 'asyncio', 'datetime', 'threading',
        'json', 'xml.etree.ElementTree', 'aiohttp'
    ]
    
    optional_modules = [
        'gspread', 'oauth2client', 'seaborn', 'psutil'
    ]
    
    missing_required = []
    missing_optional = []
    
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing_required.append(module)
    
    for module in optional_modules:
        try:
            __import__(module)
        except ImportError:
            missing_optional.append(module)
    
    return missing_required, missing_optional

def validate_file_structure() -> List[str]:
    """Validate required files exist"""
    required_files = [
        'config.py',
        'database_manager.py',
        'main.py',
        'enhanced_bot_commands.py'
    ]
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    return missing_files

def validate_directories() -> List[str]:
    """Validate and create required directories"""
    required_dirs = [
        'data',
        'logs', 
        'backups',
        'backups/database',
        'plots',
        'exports',
        'exports/analytics'
    ]
    
    created_dirs = []
    for directory in required_dirs:
        if not os.path.exists(directory):
            try:
                os.makedirs(directory, exist_ok=True)
                created_dirs.append(directory)
            except Exception as e:
                logging.error(f"Failed to create directory {directory}: {e}")
    
    return created_dirs

def validate_configuration() -> Tuple[List[str], List[str]]:
    """Validate configuration settings"""
    errors = []
    warnings = []
    
    try:
        import config
        
        # Check critical settings
        if not hasattr(config, 'token') or not config.token:
            errors.append("Discord token not configured")
        
        if not hasattr(config, 'guild_id') or not config.guild_id:
            errors.append("Guild ID not configured")
        
        # Check channel IDs
        channel_fields = [
            'channel_id_commands', 'channel_id_user_stats', 
            'channel_id_heartbeat', 'channel_id_webhook'
        ]
        
        for field in channel_fields:
            if hasattr(config, field):
                value = getattr(config, field)
                if value and not str(value).isdigit():
                    warnings.append(f"{field} should be a valid Discord channel ID")
            else:
                warnings.append(f"{field} not configured")
        
        # Check optional but recommended settings
        optional_settings = [
            'command_prefix', 'stats_interval_minutes', 'database_path'
        ]
        
        for setting in optional_settings:
            if not hasattr(config, setting):
                warnings.append(f"Optional setting '{setting}' not configured")
                
    except ImportError:
        errors.append("config.py file not found or has syntax errors")
    except Exception as e:
        errors.append(f"Configuration validation error: {e}")
    
    return errors, warnings

def validate_database_access() -> List[str]:
    """Validate database can be created and accessed"""
    issues = []
    
    try:
        import sqlite3
        
        # Test database creation in data directory
        test_db_path = os.path.join('data', 'test_connection.db')
        
        # Test connection
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()
        
        # Test table creation
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY,
                test_field TEXT
            )
        ''')
        
        # Test insert
        cursor.execute('INSERT INTO test_table (test_field) VALUES (?)', ('test',))
        
        # Test select
        cursor.execute('SELECT * FROM test_table')
        results = cursor.fetchall()
        
        conn.commit()
        conn.close()
        
        # Clean up test database
        if os.path.exists(test_db_path):
            os.remove(test_db_path)
            
    except sqlite3.Error as e:
        issues.append(f"SQLite database error: {e}")
    except Exception as e:
        issues.append(f"Database access error: {e}")
    
    return issues

def validate_google_sheets_setup() -> Tuple[bool, List[str]]:
    """Validate Google Sheets integration setup"""
    issues = []
    
    # Check if credentials file exists
    if not os.path.exists('credentials.json'):
        return False, ["credentials.json not found (Google Sheets integration will be disabled)"]
    
    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        
        # Test credentials loading
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets"
        ]
        
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            'credentials.json', scope
        )
        
        # Don't actually connect, just validate credentials format
        return True, []
        
    except ImportError:
        issues.append("gspread or oauth2client not installed")
    except Exception as e:
        issues.append(f"Google Sheets credentials error: {e}")
    
    return False, issues

def validate_startup_requirements() -> bool:
    """Main validation function"""
    print("🔍 Validating PTCGP Bot startup requirements...")
    print("=" * 50)
    
    all_good = True
    
    # 1. Validate Python modules
    missing_required, missing_optional = validate_python_modules()
    
    if missing_required:
        print("❌ Missing required Python modules:")
        for module in missing_required:
            print(f"   - {module}")
        print("   Install with: pip install -r requirements.txt")
        all_good = False
    else:
        print("✅ All required Python modules available")
    
    if missing_optional:
        print("⚠️ Missing optional Python modules:")
        for module in missing_optional:
            print(f"   - {module}")
        print("   Some features may be limited")
    
    # 2. Validate file structure
    missing_files = validate_file_structure()
    if missing_files:
        print("❌ Missing required files:")
        for file in missing_files:
            print(f"   - {file}")
        all_good = False
    else:
        print("✅ All required files present")
    
    # 3. Validate directories
    created_dirs = validate_directories()
    if created_dirs:
        print("📁 Created missing directories:")
        for directory in created_dirs:
            print(f"   - {directory}")
    print("✅ Directory structure validated")
    
    # 4. Validate configuration
    config_errors, config_warnings = validate_configuration()
    
    if config_errors:
        print("❌ Configuration errors:")
        for error in config_errors:
            print(f"   - {error}")
        all_good = False
    else:
        print("✅ Configuration validated")
    
    if config_warnings:
        print("⚠️ Configuration warnings:")
        for warning in config_warnings:
            print(f"   - {warning}")
    
    # 5. Validate database access
    db_issues = validate_database_access()
    if db_issues:
        print("❌ Database access issues:")
        for issue in db_issues:
            print(f"   - {issue}")
        all_good = False
    else:
        print("✅ Database access validated")
    
    # 6. Validate Google Sheets (optional)
    sheets_available, sheets_issues = validate_google_sheets_setup()
    if sheets_available:
        print("✅ Google Sheets integration available")
    else:
        print("ℹ️ Google Sheets integration not available:")
        for issue in sheets_issues:
            print(f"   - {issue}")
    
    print("=" * 50)
    
    if all_good:
        print("🎉 All validations passed! Bot is ready to start.")
        print("Run: python main.py")
        return True
    else:
        print("❌ Validation failed. Please fix the issues above.")
        return False

if __name__ == "__main__":
    success = validate_startup_requirements()
    sys.exit(0 if success else 1)