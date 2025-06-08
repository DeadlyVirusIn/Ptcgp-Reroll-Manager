import os
import sqlite3
import shutil
from datetime import datetime

def backup_and_reset():
    """Backup current data and reset to clean state"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Backup existing data
    if os.path.exists('data'):
        shutil.copytree('data', f'data_backup_{timestamp}')
        print(f"✅ Backed up data to data_backup_{timestamp}")
    
    # Reset database
    db_path = os.path.join('data', 'ptcgp_unified.db')
    if os.path.exists(db_path):
        shutil.move(db_path, f'data/ptcgp_unified_backup_{timestamp}.db')
        print(f"✅ Backed up database")
    
    # Initialize fresh database
    from database_manager import DatabaseManager
    db = DatabaseManager()
    print("✅ Initialized fresh database")

if __name__ == "__main__":
    backup_and_reset()