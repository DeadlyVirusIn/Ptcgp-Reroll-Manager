import os
import sys

def apply_quick_fixes():
    """Apply essential fixes for bot startup"""
    print("🔧 Applying quick fixes for PTCGP Bot...")
    
    # Set matplotlib backend before any other imports
    try:
        import matplotlib
        matplotlib.use('Agg')  # Use non-interactive backend for headless servers
        print("✅ Matplotlib backend configured")
    except ImportError:
        print("⚠️ Matplotlib not installed - install with: pip install matplotlib")
    
    # Create required directories
    directories = [
        'data',
        'logs', 
        'backups',
        'backups/database',
        'plots',
        'exports',
        'exports/analytics'
    ]
    
    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
            print(f"✅ Created directory: {directory}")
        except Exception as e:
            print(f"❌ Failed to create directory {directory}: {e}")
    
    # Check for required configuration
    try:
        import config
        issues = []
        
        if not hasattr(config, 'token') or not config.token:
            issues.append("Discord token not configured in config.py")
        
        if not hasattr(config, 'guild_id') or not config.guild_id:
            issues.append("Guild ID not configured in config.py")
            
        if issues:
            print("⚠️ Configuration issues found:")
            for issue in issues:
                print(f"   - {issue}")
        else:
            print("✅ Basic configuration validated")
            
    except ImportError:
        print("❌ config.py not found - please ensure it exists")
    
    # Check for credentials.json (optional)
    if os.path.exists('credentials.json'):
        print("✅ Google Sheets credentials found")
    else:
        print("ℹ️ Google Sheets credentials.json not found (optional)")
    
    # Validate database can be created
    try:
        import sqlite3
        test_db = os.path.join('data', 'test.db')
        conn = sqlite3.connect(test_db)
        conn.close()
        os.remove(test_db)
        print("✅ Database access validated")
    except Exception as e:
        print(f"❌ Database access issue: {e}")
    
    print("\n🎉 Quick fixes completed!")
    print("You can now run: python startup_migration.py")
    print("Then run: python main.py")

if __name__ == "__main__":
    apply_quick_fixes()