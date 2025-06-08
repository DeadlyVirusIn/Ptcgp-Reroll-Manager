import sqlite3
import json
import threading
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from enum import Enum
import logging

# CRITICAL FIX: Import unified database path from config
try:
    import config
    DB_PATH = getattr(config, 'DATABASE_PATH', os.path.join('data', 'ptcgp_unified.db'))
except ImportError:
    DB_PATH = os.path.join('data', 'ptcgp_unified.db')

class GPState(Enum):
    TESTING = "TESTING"
    ALIVE = "ALIVE"
    DEAD = "DEAD"
    INVALID = "INVALID"
    EXPIRED = "EXPIRED"

class TestType(Enum):
    MISS = "MISS"
    NOSHOW = "NOSHOW"

@dataclass
class GodPack:
    id: int
    message_id: int
    timestamp: datetime
    pack_number: int
    name: str
    friend_code: str
    state: GPState
    screenshot_url: str
    ratio: int = -1
    expiration_date: Optional[datetime] = None

@dataclass
class HeartBeat:
    message_id: int
    timestamp: datetime
    discord_id: int
    instances_online: int
    instances_offline: int
    time: int
    packs: int
    main_on: bool
    selected_packs: List[str] = None

@dataclass
class TestResult:
    discord_id: int
    timestamp: datetime
    gp_id: int
    test_type: TestType
    open_slots: int = -1
    number_friends: int = -1

class DatabaseManager:
    def __init__(self, db_path: str = None):
        # CRITICAL FIX: Use unified database path
        self.db_path = db_path or DB_PATH
        self.lock = threading.RLock()
        self.logger = logging.getLogger(__name__)
        self._connection_pool = []
        self._pool_size = 5
        
        # Ensure data directory exists
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        self._initialize_database()

    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection with proper error handling and pooling"""
        try:
            # Create database directory if it doesn't exist
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
            
            conn = sqlite3.connect(
                self.db_path, 
                timeout=30.0,  # 30 second timeout
                check_same_thread=False
            )
            
            # Enable foreign keys and WAL mode for better performance
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA cache_size = 10000")
            
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"Database connection error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected database connection error: {e}")
            raise

    def _execute_query(self, query: str, params: tuple = None, fetch_results: bool = False):
        """Safely execute query with proper error handling and connection management"""
        with self.lock:
            conn = None
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                if fetch_results:
                    results = cursor.fetchall()
                else:
                    results = cursor.rowcount
                
                conn.commit()
                return results
                
            except sqlite3.IntegrityError as e:
                if conn:
                    conn.rollback()
                self.logger.error(f"Database integrity error: {e}")
                raise
            except sqlite3.OperationalError as e:
                if conn:
                    conn.rollback()
                self.logger.error(f"Database operational error: {e}")
                raise
            except Exception as e:
                if conn:
                    conn.rollback()
                self.logger.error(f"Database query error: {e}")
                raise
            finally:
                if conn:
                    conn.close()

    def _initialize_database(self):
        """Initialize database with all necessary tables and proper error handling"""
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                # Users table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        discord_id INTEGER PRIMARY KEY,
                        player_id TEXT,
                        display_name TEXT,
                        prefix TEXT,
                        status TEXT DEFAULT 'inactive',
                        average_instances INTEGER DEFAULT 0,
                        total_packs INTEGER DEFAULT 0,
                        total_gps INTEGER DEFAULT 0,
                        last_heartbeat TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # God Packs table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS godpacks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        message_id INTEGER UNIQUE NOT NULL,
                        timestamp TIMESTAMP NOT NULL,
                        pack_number INTEGER NOT NULL,
                        name TEXT NOT NULL,
                        friend_code TEXT NOT NULL,
                        state TEXT NOT NULL,
                        screenshot_url TEXT,
                        ratio INTEGER DEFAULT -1,
                        expiration_date TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Heartbeats table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS heartbeats (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        message_id INTEGER UNIQUE NOT NULL,
                        discord_id INTEGER NOT NULL,
                        timestamp TIMESTAMP NOT NULL,
                        instances_online INTEGER NOT NULL DEFAULT 0,
                        instances_offline INTEGER NOT NULL DEFAULT 0,
                        time INTEGER NOT NULL DEFAULT 0,
                        packs INTEGER NOT NULL DEFAULT 0,
                        main_on BOOLEAN NOT NULL DEFAULT 0,
                        selected_packs TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (discord_id) REFERENCES users (discord_id) ON DELETE CASCADE
                    )
                ''')
                
                # Test Results table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS test_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        discord_id INTEGER NOT NULL,
                        timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        gp_id INTEGER NOT NULL,
                        test_type TEXT NOT NULL,
                        open_slots INTEGER DEFAULT -1,
                        number_friends INTEGER DEFAULT -1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (discord_id) REFERENCES users (discord_id) ON DELETE CASCADE,
                        FOREIGN KEY (gp_id) REFERENCES godpacks (id) ON DELETE CASCADE
                    )
                ''')
                
                # GP Statistics table for caching probability calculations
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS gp_statistics (
                        gp_id INTEGER PRIMARY KEY,
                        probability_alive REAL DEFAULT 100.0,
                        total_tests INTEGER DEFAULT 0,
                        miss_tests INTEGER DEFAULT 0,
                        noshow_tests INTEGER DEFAULT 0,
                        confidence_level REAL DEFAULT 0.0,
                        last_calculated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (gp_id) REFERENCES godpacks (id) ON DELETE CASCADE
                    )
                ''')
                
                # Heartbeat Analytics table for run tracking
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS heartbeat_runs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        discord_id INTEGER NOT NULL,
                        start_time TIMESTAMP NOT NULL,
                        end_time TIMESTAMP NOT NULL,
                        start_packs INTEGER NOT NULL DEFAULT 0,
                        end_packs INTEGER NOT NULL DEFAULT 0,
                        average_instances REAL NOT NULL DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (discord_id) REFERENCES users (discord_id) ON DELETE CASCADE
                    )
                ''')
                
                # Expiration warnings table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS expiration_warnings (
                        gp_id INTEGER NOT NULL,
                        warned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (gp_id, warned_at),
                        FOREIGN KEY (gp_id) REFERENCES godpacks (id) ON DELETE CASCADE
                    )
                ''')
                
                # Create indexes for better performance
                self._create_indexes(cursor)
                
                conn.commit()
                conn.close()
                
                self.logger.info(f"Database initialized successfully at {self.db_path}")
                
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
            raise

    def _create_indexes(self, cursor):
        """Create database indexes for better performance"""
        try:
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)",
                "CREATE INDEX IF NOT EXISTS idx_users_last_heartbeat ON users(last_heartbeat)",
                "CREATE INDEX IF NOT EXISTS idx_godpacks_state ON godpacks(state)",
                "CREATE INDEX IF NOT EXISTS idx_godpacks_timestamp ON godpacks(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_godpacks_expiration ON godpacks(expiration_date)",
                "CREATE INDEX IF NOT EXISTS idx_heartbeats_discord_id ON heartbeats(discord_id)",
                "CREATE INDEX IF NOT EXISTS idx_heartbeats_timestamp ON heartbeats(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_test_results_gp_id ON test_results(gp_id)",
                "CREATE INDEX IF NOT EXISTS idx_test_results_discord_id ON test_results(discord_id)",
                "CREATE INDEX IF NOT EXISTS idx_heartbeat_runs_discord_id ON heartbeat_runs(discord_id)",
                "CREATE INDEX IF NOT EXISTS idx_heartbeat_runs_start_time ON heartbeat_runs(start_time)"
            ]
            
            for index_sql in indexes:
                cursor.execute(index_sql)
                
        except Exception as e:
            self.logger.error(f"Error creating indexes: {e}")

    def add_user(self, discord_id: int, player_id: str = None, display_name: str = None, 
                 prefix: str = None) -> bool:
        """Add or update a user with proper error handling"""
        try:
            self._execute_query('''
                INSERT OR REPLACE INTO users 
                (discord_id, player_id, display_name, prefix, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (discord_id, player_id, display_name, prefix))
            
            self.logger.debug(f"Added/updated user: {discord_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding user {discord_id}: {e}")
            return False

    def get_user(self, discord_id: int) -> Optional[Dict]:
        """Get user information with error handling"""
        try:
            with self.lock:
                conn = self._get_connection()
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('SELECT * FROM users WHERE discord_id = ?', (discord_id,))
                row = cursor.fetchone()
                conn.close()
                
                return dict(row) if row else None
                
        except Exception as e:
            self.logger.error(f"Error getting user {discord_id}: {e}")
            return None

    def get_all_users(self) -> List[Dict]:
        """Get all users with error handling"""
        try:
            with self.lock:
                conn = self._get_connection()
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('SELECT * FROM users ORDER BY display_name')
                rows = cursor.fetchall()
                conn.close()
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            self.logger.error(f"Error getting all users: {e}")
            return []

    def update_user_status(self, discord_id: int, status: str) -> bool:
        """Update user status with validation"""
        try:
            valid_statuses = ['active', 'inactive', 'farm', 'leech']
            if status not in valid_statuses:
                self.logger.error(f"Invalid status: {status}")
                return False
            
            affected_rows = self._execute_query('''
                UPDATE users 
                SET status = ?, updated_at = CURRENT_TIMESTAMP 
                WHERE discord_id = ?
            ''', (status, discord_id))
            
            success = affected_rows > 0
            if success:
                self.logger.debug(f"Updated user {discord_id} status to {status}")
            else:
                self.logger.warning(f"User {discord_id} not found for status update")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error updating user status {discord_id}: {e}")
            return False

    def update_user_stats(self, discord_id: int, **kwargs) -> bool:
        """Update user statistics with validation"""
        if not kwargs:
            return False
            
        try:
            # Build dynamic update query safely
            valid_fields = {'average_instances', 'total_packs', 'total_gps', 'last_heartbeat'}
            set_clauses = []
            values = []
            
            for key, value in kwargs.items():
                if key in valid_fields:
                    set_clauses.append(f"{key} = ?")
                    values.append(value)
            
            if not set_clauses:
                self.logger.warning(f"No valid fields to update for user {discord_id}")
                return False
            
            set_clauses.append("updated_at = CURRENT_TIMESTAMP")
            values.append(discord_id)
            
            query = f"UPDATE users SET {', '.join(set_clauses)} WHERE discord_id = ?"
            affected_rows = self._execute_query(query, tuple(values))
            
            success = affected_rows > 0
            if success:
                self.logger.debug(f"Updated stats for user {discord_id}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error updating user stats {discord_id}: {e}")
            return False

    def delete_user(self, discord_id: int) -> bool:
        """Delete a user and all related data"""
        try:
            affected_rows = self._execute_query(
                'DELETE FROM users WHERE discord_id = ?', 
                (discord_id,)
            )
            
            success = affected_rows > 0
            if success:
                self.logger.info(f"Deleted user {discord_id} and all related data")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error deleting user {discord_id}: {e}")
            return False

    def add_godpack(self, message_id: int, timestamp: datetime, pack_number: int,
                   name: str, friend_code: str, state: GPState, screenshot_url: str,
                   ratio: int = -1) -> Optional[int]:
        """Add a new god pack with automatic expiration calculation"""
        try:
            # Calculate expiration date (3 days from 6 AM reset time)
            reset_time = timestamp.replace(hour=6, minute=0, second=0, microsecond=0)
            if timestamp < reset_time:
                expiration_date = reset_time + timedelta(days=3)
            else:
                expiration_date = reset_time + timedelta(days=4)
            
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO godpacks 
                    (message_id, timestamp, pack_number, name, friend_code, state, 
                     screenshot_url, ratio, expiration_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (message_id, timestamp, pack_number, name, friend_code, 
                      state.value, screenshot_url, ratio, expiration_date))
                
                gp_id = cursor.lastrowid
                
                # Initialize statistics
                cursor.execute('''
                    INSERT INTO gp_statistics (gp_id) VALUES (?)
                ''', (gp_id,))
                
                conn.commit()
                conn.close()
                
                self.logger.info(f"Added godpack {gp_id}: {name}")
                return gp_id
                
        except sqlite3.IntegrityError as e:
            self.logger.error(f"Godpack with message_id {message_id} already exists: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error adding godpack: {e}")
            return None

    def get_godpack(self, gp_id: int = None, message_id: int = None) -> Optional[GodPack]:
        """Get god pack by ID or message ID with error handling"""
        try:
            with self.lock:
                conn = self._get_connection()
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                if gp_id:
                    cursor.execute('SELECT * FROM godpacks WHERE id = ?', (gp_id,))
                elif message_id:
                    cursor.execute('SELECT * FROM godpacks WHERE message_id = ?', (message_id,))
                else:
                    return None
                
                row = cursor.fetchone()
                conn.close()
                
                if row:
                    return GodPack(
                        id=row['id'],
                        message_id=row['message_id'],
                        timestamp=datetime.fromisoformat(row['timestamp']),
                        pack_number=row['pack_number'],
                        name=row['name'],
                        friend_code=row['friend_code'],
                        state=GPState(row['state']),
                        screenshot_url=row['screenshot_url'],
                        ratio=row['ratio'],
                        expiration_date=datetime.fromisoformat(row['expiration_date']) if row['expiration_date'] else None
                    )
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting godpack: {e}")
            return None

    def get_all_godpacks(self, state: GPState = None, limit: int = None) -> List[GodPack]:
        """Get all god packs, optionally filtered by state"""
        try:
            with self.lock:
                conn = self._get_connection()
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                if state:
                    query = 'SELECT * FROM godpacks WHERE state = ? ORDER BY timestamp DESC'
                    params = (state.value,)
                else:
                    query = 'SELECT * FROM godpacks ORDER BY timestamp DESC'
                    params = ()
                
                if limit:
                    query += f' LIMIT {int(limit)}'
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                conn.close()
                
                godpacks = []
                for row in rows:
                    try:
                        godpacks.append(GodPack(
                            id=row['id'],
                            message_id=row['message_id'],
                            timestamp=datetime.fromisoformat(row['timestamp']),
                            pack_number=row['pack_number'],
                            name=row['name'],
                            friend_code=row['friend_code'],
                            state=GPState(row['state']),
                            screenshot_url=row['screenshot_url'],
                            ratio=row['ratio'],
                            expiration_date=datetime.fromisoformat(row['expiration_date']) if row['expiration_date'] else None
                        ))
                    except Exception as e:
                        self.logger.error(f"Error parsing godpack row: {e}")
                        continue
                
                return godpacks
                
        except Exception as e:
            self.logger.error(f"Error getting all godpacks: {e}")
            return []

    def update_godpack_state(self, gp_id: int, state: GPState) -> bool:
        """Update god pack state with validation"""
        try:
            affected_rows = self._execute_query('''
                UPDATE godpacks 
                SET state = ?, updated_at = CURRENT_TIMESTAMP 
                WHERE id = ?
            ''', (state.value, gp_id))
            
            success = affected_rows > 0
            if success:
                self.logger.info(f"Updated godpack {gp_id} state to {state.value}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error updating godpack state {gp_id}: {e}")
            return False

    def update_godpack_ratio(self, gp_id: int, ratio: int) -> bool:
        """Update god pack ratio with validation"""
        try:
            if ratio < -1 or ratio > 5:
                self.logger.error(f"Invalid ratio: {ratio}")
                return False
            
            affected_rows = self._execute_query('''
                UPDATE godpacks 
                SET ratio = ?, updated_at = CURRENT_TIMESTAMP 
                WHERE id = ?
            ''', (ratio, gp_id))
            
            success = affected_rows > 0
            if success:
                self.logger.debug(f"Updated godpack {gp_id} ratio to {ratio}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error updating godpack ratio {gp_id}: {e}")
            return False

    def get_expired_godpacks(self) -> List[GodPack]:
        """Get all expired god packs that haven't been marked as expired"""
        try:
            with self.lock:
                conn = self._get_connection()
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM godpacks 
                    WHERE expiration_date < CURRENT_TIMESTAMP 
                    AND state NOT IN ('EXPIRED', 'DEAD', 'INVALID')
                    ORDER BY expiration_date ASC
                ''')
                
                rows = cursor.fetchall()
                conn.close()
                
                godpacks = []
                for row in rows:
                    try:
                        godpacks.append(GodPack(
                            id=row['id'],
                            message_id=row['message_id'],
                            timestamp=datetime.fromisoformat(row['timestamp']),
                            pack_number=row['pack_number'],
                            name=row['name'],
                            friend_code=row['friend_code'],
                            state=GPState(row['state']),
                            screenshot_url=row['screenshot_url'],
                            ratio=row['ratio'],
                            expiration_date=datetime.fromisoformat(row['expiration_date']) if row['expiration_date'] else None
                        ))
                    except Exception as e:
                        self.logger.error(f"Error parsing expired godpack row: {e}")
                        continue
                
                return godpacks
                
        except Exception as e:
            self.logger.error(f"Error getting expired godpacks: {e}")
            return []

    def delete_godpack(self, gp_id: int) -> bool:
        """Delete a god pack and all related data with CASCADE"""
        try:
            affected_rows = self._execute_query(
                'DELETE FROM godpacks WHERE id = ?', 
                (gp_id,)
            )
            
            success = affected_rows > 0
            if success:
                self.logger.info(f"Deleted godpack {gp_id} and all related data")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error deleting godpack {gp_id}: {e}")
            return False

    def add_heartbeat(self, message_id: int, discord_id: int, timestamp: datetime,
                     instances_online: int, instances_offline: int, time: int,
                     packs: int, main_on: bool, selected_packs: List[str] = None) -> bool:
        """Add a heartbeat entry with validation"""
        try:
            # Validate inputs
            if instances_online < 0 or instances_offline < 0:
                self.logger.error("Invalid instance counts")
                return False
            
            if time < 0 or packs < 0:
                self.logger.error("Invalid time or packs values")
                return False
            
            selected_packs_str = json.dumps(selected_packs) if selected_packs else None
            
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO heartbeats 
                    (message_id, discord_id, timestamp, instances_online, instances_offline,
                     time, packs, main_on, selected_packs)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (message_id, discord_id, timestamp, instances_online, instances_offline,
                      time, packs, main_on, selected_packs_str))
                
                # Update user's last heartbeat and total packs
                cursor.execute('''
                    UPDATE users 
                    SET last_heartbeat = ?, total_packs = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE discord_id = ?
                ''', (timestamp, packs, discord_id))
                
                # If user doesn't exist, create them
                if cursor.rowcount == 0:
                    cursor.execute('''
                        INSERT OR IGNORE INTO users (discord_id, last_heartbeat, total_packs)
                        VALUES (?, ?, ?)
                    ''', (discord_id, timestamp, packs))
                
                conn.commit()
                conn.close()
                
                self.logger.debug(f"Added heartbeat for user {discord_id}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error adding heartbeat: {e}")
            return False

    def get_heartbeat(self, message_id: int = None, discord_id: int = None, latest: bool = False) -> Optional[HeartBeat]:
        """Get heartbeat by message ID, or latest for a user"""
        try:
            with self.lock:
                conn = self._get_connection()
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                if message_id:
                    cursor.execute('SELECT * FROM heartbeats WHERE message_id = ?', (message_id,))
                elif discord_id and latest:
                    cursor.execute('''
                        SELECT * FROM heartbeats 
                        WHERE discord_id = ? 
                        ORDER BY timestamp DESC 
                        LIMIT 1
                    ''', (discord_id,))
                else:
                    return None
                
                row = cursor.fetchone()
                conn.close()
                
                if row:
                    selected_packs = json.loads(row['selected_packs']) if row['selected_packs'] else []
                    return HeartBeat(
                        message_id=row['message_id'],
                        timestamp=datetime.fromisoformat(row['timestamp']),
                        discord_id=row['discord_id'],
                        instances_online=row['instances_online'],
                        instances_offline=row['instances_offline'],
                        time=row['time'],
                        packs=row['packs'],
                        main_on=bool(row['main_on']),
                        selected_packs=selected_packs
                    )
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting heartbeat: {e}")
            return None

    def add_test_result(self, discord_id: int, gp_id: int, test_type: TestType,
                       open_slots: int = -1, number_friends: int = -1) -> bool:
        """Add a test result with validation"""
        try:
            # Validate inputs
            if test_type == TestType.NOSHOW:
                if open_slots < 0 or number_friends < 0:
                    self.logger.error("No-show tests require valid open_slots and number_friends")
                    return False
            
            self._execute_query('''
                INSERT INTO test_results 
                (discord_id, gp_id, test_type, open_slots, number_friends)
                VALUES (?, ?, ?, ?, ?)
            ''', (discord_id, gp_id, test_type.value, open_slots, number_friends))
            
            self.logger.debug(f"Added {test_type.value} test for GP {gp_id} by user {discord_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding test result: {e}")
            return False

    def get_test_results(self, gp_id: int) -> List[TestResult]:
        """Get all test results for a god pack"""
        try:
            with self.lock:
                conn = self._get_connection()
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM test_results 
                    WHERE gp_id = ? 
                    ORDER BY timestamp ASC
                ''', (gp_id,))
                
                rows = cursor.fetchall()
                conn.close()
                
                results = []
                for row in rows:
                    try:
                        results.append(TestResult(
                            discord_id=row['discord_id'],
                            timestamp=datetime.fromisoformat(row['timestamp']),
                            gp_id=row['gp_id'],
                            test_type=TestType(row['test_type']),
                            open_slots=row['open_slots'],
                            number_friends=row['number_friends']
                        ))
                    except Exception as e:
                        self.logger.error(f"Error parsing test result row: {e}")
                        continue
                
                return results
                
        except Exception as e:
            self.logger.error(f"Error getting test results for GP {gp_id}: {e}")
            return []

    def get_heartbeats_for_user(self, discord_id: int, days_back: int = 7) -> List[HeartBeat]:
        """Get heartbeats for a user within specified days"""
        try:
            with self.lock:
                conn = self._get_connection()
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                since_date = datetime.now() - timedelta(days=days_back)
                
                cursor.execute('''
                    SELECT * FROM heartbeats 
                    WHERE discord_id = ? AND timestamp >= ?
                    ORDER BY timestamp ASC
                ''', (discord_id, since_date))
                
                rows = cursor.fetchall()
                conn.close()
                
                heartbeats = []
                for row in rows:
                    try:
                        selected_packs = json.loads(row['selected_packs']) if row['selected_packs'] else []
                        heartbeats.append(HeartBeat(
                            message_id=row['message_id'],
                            timestamp=datetime.fromisoformat(row['timestamp']),
                            discord_id=row['discord_id'],
                            instances_online=row['instances_online'],
                            instances_offline=row['instances_offline'],
                            time=row['time'],
                            packs=row['packs'],
                            main_on=bool(row['main_on']),
                            selected_packs=selected_packs
                        ))
                    except Exception as e:
                        self.logger.error(f"Error parsing heartbeat row: {e}")
                        continue
                
                return heartbeats
                
        except Exception as e:
            self.logger.error(f"Error getting heartbeats for user {discord_id}: {e}")
            return []

    def get_active_users(self, minutes_back: int = 60) -> List[Dict]:
        """Get users who have sent heartbeats recently"""
        try:
            with self.lock:
                conn = self._get_connection()
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                since_time = datetime.now() - timedelta(minutes=minutes_back)
                
                cursor.execute('''
                    SELECT DISTINCT u.* FROM users u
                    JOIN heartbeats h ON u.discord_id = h.discord_id
                    WHERE h.timestamp >= ?
                    ORDER BY u.display_name
                ''', (since_time,))
                
                rows = cursor.fetchall()
                conn.close()
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            self.logger.error(f"Error getting active users: {e}")
            return []

    def cleanup_old_data(self, days_to_keep: int = 30):
        """Clean up old heartbeat data and other temporary data"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # Clean up old heartbeats
            deleted_heartbeats = self._execute_query(
                'DELETE FROM heartbeats WHERE timestamp < ?', 
                (cutoff_date,)
            )
            
            # Clean up old test results
            deleted_tests = self._execute_query(
                'DELETE FROM test_results WHERE timestamp < ?', 
                (cutoff_date,)
            )
            
            # Clean up old heartbeat runs
            deleted_runs = self._execute_query(
                'DELETE FROM heartbeat_runs WHERE end_time < ?', 
                (cutoff_date,)
            )
            
            # Clean up old expiration warnings
            deleted_warnings = self._execute_query(
                'DELETE FROM expiration_warnings WHERE warned_at < ?', 
                (cutoff_date,)
            )
            
            self.logger.info(f"Cleaned up {deleted_heartbeats} heartbeats, {deleted_tests} test results, "
                           f"{deleted_runs} runs, and {deleted_warnings} warnings")
            
            return deleted_heartbeats, deleted_tests, deleted_runs, deleted_warnings
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
            return 0, 0, 0, 0

    def backup_database(self, backup_path: str) -> bool:
        """Create a backup of the database"""
        try:
            # Ensure backup directory exists
            backup_dir = os.path.dirname(backup_path)
            if backup_dir and not os.path.exists(backup_dir):
                os.makedirs(backup_dir, exist_ok=True)
            
            with self.lock:
                source = self._get_connection()
                backup = sqlite3.connect(backup_path)
                source.backup(backup)
                backup.close()
                source.close()
            
            self.logger.info(f"Database backed up to {backup_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error backing up database: {e}")
            return False

    def get_database_info(self) -> Dict:
        """Get comprehensive information about the database"""
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                # Get table sizes
                tables = ['users', 'godpacks', 'heartbeats', 'test_results', 
                         'gp_statistics', 'heartbeat_runs', 'expiration_warnings']
                table_info = {}
                
                for table in tables:
                    try:
                        cursor.execute(f'SELECT COUNT(*) FROM {table}')
                        count = cursor.fetchone()[0]
                        table_info[table] = count
                    except Exception as e:
                        self.logger.error(f"Error getting count for table {table}: {e}")
                        table_info[table] = 0
                
                # Get database file size
                cursor.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]
                cursor.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]
                db_size = page_count * page_size
                
                # Get database integrity check
                cursor.execute("PRAGMA integrity_check")
                integrity_result = cursor.fetchone()[0]
                
                conn.close()
                
                return {
                    'database_path': self.db_path,
                    'size_bytes': db_size,
                    'size_mb': round(db_size / (1024 * 1024), 2),
                    'tables': table_info,
                    'total_records': sum(table_info.values()),
                    'integrity_check': integrity_result == 'ok',
                    'wal_mode': True  # We enable WAL mode
                }
                
        except Exception as e:
            self.logger.error(f"Error getting database info: {e}")
            return {'error': str(e)}

    def vacuum_database(self) -> bool:
        """Vacuum the database to reclaim space and optimize performance"""
        try:
            with self.lock:
                conn = self._get_connection()
                conn.execute("VACUUM")
                conn.close()
                
            self.logger.info("Database vacuumed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error vacuuming database: {e}")
            return False

    def test_connection(self) -> bool:
        """Test database connection and basic operations"""
        try:
            with self.lock:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                # Test basic query
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                
                # Test table access
                cursor.execute("SELECT COUNT(*) FROM users")
                cursor.fetchone()
                
                conn.close()
                
                return result[0] == 1
                
        except Exception as e:
            self.logger.error(f"Database connection test failed: {e}")
            return False