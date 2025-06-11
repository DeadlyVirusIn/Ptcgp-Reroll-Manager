import sqlite3
import json
import threading
import queue
import os
import time
import shutil
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Generator, Tuple
from dataclasses import dataclass
from enum import Enum
from contextlib import contextmanager
import logging
import atexit

# Import unified database path from config
try:
    import config
    DB_PATH = getattr(config, 'DATABASE_PATH', os.path.join('data', 'ptcgp_unified.db'))
    POOL_SIZE = getattr(config, 'DB_CONNECTION_POOL_SIZE', 5)
    QUERY_TIMEOUT = getattr(config, 'DB_QUERY_TIMEOUT_SECONDS', 30)
    AUTO_BACKUP_ENABLED = getattr(config, 'AUTO_BACKUP_ENABLED', True)
    BACKUP_RETENTION_DAYS = getattr(config, 'BACKUP_RETENTION_DAYS', 30)
    MAX_BACKUP_COUNT = getattr(config, 'MAX_BACKUP_COUNT', 50)
except ImportError:
    DB_PATH = os.path.join('data', 'ptcgp_unified.db')
    POOL_SIZE = 5
    QUERY_TIMEOUT = 30
    AUTO_BACKUP_ENABLED = True
    BACKUP_RETENTION_DAYS = 30
    MAX_BACKUP_COUNT = 50

class GPState(Enum):
    TESTING = "TESTING"
    ALIVE = "ALIVE"
    DEAD = "DEAD"
    INVALID = "INVALID"
    EXPIRED = "EXPIRED"

class TestType(Enum):
    MISS = "MISS"
    NOSHOW = "NOSHOW"

class BackupType(Enum):
    MANUAL = "MANUAL"
    AUTOMATIC = "AUTOMATIC"
    SCHEMA_CHANGE = "SCHEMA_CHANGE"
    MIGRATION = "MIGRATION"
    SCHEDULED = "SCHEDULED"
    EMERGENCY = "EMERGENCY"

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

@dataclass
class User:
    discord_id: int
    player_id: Optional[str] = None
    display_name: Optional[str] = None
    prefix: Optional[str] = None
    status: str = 'inactive'
    average_instances: int = 0
    total_packs: int = 0
    total_gps: int = 0
    last_heartbeat: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class HeartbeatRun:
    id: int
    discord_id: int
    start_time: datetime
    end_time: datetime
    start_packs: int
    end_packs: int
    average_instances: float
    created_at: Optional[datetime] = None

@dataclass
class GPStatistics:
    gp_id: int
    probability_alive: float = 100.0
    total_tests: int = 0
    miss_tests: int = 0
    noshow_tests: int = 0
    confidence_level: float = 0.0
    last_calculated: Optional[datetime] = None
class BackupManager:
    """Enhanced backup management system with comprehensive features"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.backup_dir = os.path.join(os.path.dirname(db_path), 'backups')
        self.logger = logging.getLogger(__name__)
        
        # Ensure backup directory exists
        os.makedirs(self.backup_dir, exist_ok=True)
        
        # Create subdirectories for different backup types
        for backup_type in BackupType:
            type_dir = os.path.join(self.backup_dir, backup_type.value.lower())
            os.makedirs(type_dir, exist_ok=True)
    
    def create_backup(self, backup_type: BackupType = BackupType.MANUAL, 
                     description: str = None, compress: bool = True) -> Optional[str]:
        """Create a database backup with comprehensive metadata"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"ptcgp_backup_{backup_type.value.lower()}_{timestamp}.db"
            type_dir = os.path.join(self.backup_dir, backup_type.value.lower())
            backup_path = os.path.join(type_dir, backup_filename)
            
            # Ensure the backup won't exceed storage limits
            if not self._check_storage_limits():
                self.logger.warning("Storage limits reached, cleaning up old backups")
                self.cleanup_old_backups(BACKUP_RETENTION_DAYS // 2)
            
            # Create the backup
            start_time = time.time()
            shutil.copy2(self.db_path, backup_path)
            backup_time = time.time() - start_time
            
            # Get file size
            backup_size = os.path.getsize(backup_path)
            
            # Create comprehensive metadata
            metadata = {
                'timestamp': datetime.now().isoformat(),
                'type': backup_type.value,
                'description': description or f"{backup_type.value} backup",
                'original_db_path': self.db_path,
                'backup_path': backup_path,
                'size_bytes': backup_size,
                'size_mb': round(backup_size / (1024 * 1024), 2),
                'backup_duration_seconds': round(backup_time, 2),
                'compressed': compress,
                'schema_version': self._get_schema_version(backup_path),
                'integrity_verified': self._verify_backup_integrity(backup_path)
            }
            
            # Get record counts
            try:
                metadata['record_counts'] = self._get_record_counts(backup_path)
                metadata['total_records'] = sum(metadata['record_counts'].values())
            except Exception as e:
                self.logger.warning(f"Could not get record counts for backup: {e}")
                metadata['record_counts'] = {}
                metadata['total_records'] = 0
            
            # Save metadata
            metadata_path = backup_path + '.meta'
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            # Optionally compress the backup
            if compress and backup_size > 10 * 1024 * 1024:  # Compress if > 10MB
                compressed_path = self._compress_backup(backup_path)
                if compressed_path:
                    os.remove(backup_path)
                    backup_path = compressed_path
                    metadata['backup_path'] = backup_path
                    metadata['compressed'] = True
                    # Update metadata file
                    with open(metadata_path, 'w') as f:
                        json.dump(metadata, f, indent=2)
            
            self.logger.info(f"Created {backup_type.value} backup: {backup_path} ({metadata['size_mb']} MB)")
            return backup_path
            
        except Exception as e:
            self.logger.error(f"Error creating backup: {e}")
            return None
    
    def _check_storage_limits(self) -> bool:
        """Check if we're within storage limits"""
        try:
            backup_count = len(self.list_backups())
            if backup_count >= MAX_BACKUP_COUNT:
                return False
            
            # Check total backup directory size
            total_size = 0
            for root, dirs, files in os.walk(self.backup_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.path.exists(file_path):
                        total_size += os.path.getsize(file_path)
            
            # Limit to 1GB total backup storage
            max_size = 1024 * 1024 * 1024  # 1GB
            return total_size < max_size
            
        except Exception as e:
            self.logger.warning(f"Could not check storage limits: {e}")
            return True
    
    def _compress_backup(self, backup_path: str) -> Optional[str]:
        """Compress a backup file using gzip"""
        try:
            import gzip
            compressed_path = backup_path + '.gz'
            
            with open(backup_path, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            return compressed_path
            
        except ImportError:
            self.logger.warning("gzip not available for compression")
            return None
        except Exception as e:
            self.logger.error(f"Error compressing backup: {e}")
            return None
    
    def _verify_backup_integrity(self, backup_path: str) -> bool:
        """Verify backup file integrity"""
        try:
            conn = sqlite3.connect(backup_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()[0]
            conn.close()
            return result == "ok"
        except Exception as e:
            self.logger.warning(f"Could not verify backup integrity: {e}")
            return False
    
    def _get_schema_version(self, db_path: str) -> int:
        """Get schema version from database"""
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(version) FROM schema_version")
            result = cursor.fetchone()
            conn.close()
            return result[0] if result and result[0] is not None else 0
        except:
            return 0
    
    def _get_record_counts(self, db_path: str) -> Dict[str, int]:
        """Get record counts from a database file"""
        counts = {}
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            tables = ['users', 'godpacks', 'heartbeats', 'test_results', 
                     'gp_statistics', 'heartbeat_runs', 'expiration_warnings', 'schema_version']
            
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    counts[table] = cursor.fetchone()[0]
                except sqlite3.Error:
                    counts[table] = 0
            
            conn.close()
            return counts
            
        except Exception as e:
            self.logger.error(f"Error getting record counts: {e}")
            return {}
    
    def list_backups(self, backup_type: BackupType = None, limit: int = None) -> List[Dict]:
        """List all available backups with optional filtering"""
        backups = []
        try:
            if not os.path.exists(self.backup_dir):
                return backups
            
            # Search in all subdirectories or specific type directory
            search_dirs = []
            if backup_type:
                type_dir = os.path.join(self.backup_dir, backup_type.value.lower())
                if os.path.exists(type_dir):
                    search_dirs.append(type_dir)
            else:
                # Search all type directories
                for bt in BackupType:
                    type_dir = os.path.join(self.backup_dir, bt.value.lower())
                    if os.path.exists(type_dir):
                        search_dirs.append(type_dir)
                # Also search root backup directory for legacy backups
                search_dirs.append(self.backup_dir)
            
            for search_dir in search_dirs:
                for filename in os.listdir(search_dir):
                    if filename.endswith('.db') or filename.endswith('.db.gz'):
                        backup_path = os.path.join(search_dir, filename)
                        metadata_path = backup_path + '.meta'
                        
                        if os.path.exists(metadata_path):
                            try:
                                with open(metadata_path, 'r') as f:
                                    metadata = json.load(f)
                                backups.append(metadata)
                            except Exception as e:
                                self.logger.warning(f"Could not read metadata for {filename}: {e}")
                                # Create basic metadata from file
                                backups.append(self._create_legacy_metadata(backup_path))
                        else:
                            # Create basic metadata for legacy backups
                            backups.append(self._create_legacy_metadata(backup_path))
            
            # Sort by timestamp, newest first
            backups.sort(key=lambda x: x['timestamp'], reverse=True)
            
            # Apply limit if specified
            if limit:
                backups = backups[:limit]
            
            return backups
            
        except Exception as e:
            self.logger.error(f"Error listing backups: {e}")
            return []
    
    def _create_legacy_metadata(self, backup_path: str) -> Dict:
        """Create metadata for legacy backup files"""
        try:
            stat = os.stat(backup_path)
            return {
                'timestamp': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                'type': 'LEGACY',
                'description': 'Legacy backup (no metadata)',
                'backup_path': backup_path,
                'size_bytes': stat.st_size,
                'size_mb': round(stat.st_size / (1024 * 1024), 2),
                'record_counts': self._get_record_counts(backup_path),
                'integrity_verified': False,
                'compressed': backup_path.endswith('.gz')
            }
        except Exception as e:
            self.logger.error(f"Error creating legacy metadata: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'type': 'UNKNOWN',
                'description': 'Unknown backup',
                'backup_path': backup_path,
                'size_bytes': 0,
                'size_mb': 0,
                'record_counts': {},
                'integrity_verified': False,
                'compressed': False
            }
    
    def get_backup_info(self, backup_path: str) -> Optional[Dict]:
        """Get detailed information about a specific backup"""
        try:
            metadata_path = backup_path + '.meta'
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                
                # Add current file info in case file was moved/modified
                if os.path.exists(backup_path):
                    stat = os.stat(backup_path)
                    metadata['current_size_bytes'] = stat.st_size
                    metadata['current_size_mb'] = round(stat.st_size / (1024 * 1024), 2)
                    metadata['file_exists'] = True
                else:
                    metadata['file_exists'] = False
                
                return metadata
            else:
                # Generate basic info for legacy backups
                return self._create_legacy_metadata(backup_path)
                
        except Exception as e:
            self.logger.error(f"Error getting backup info: {e}")
            return None
    
    def restore_backup(self, backup_path: str, create_pre_restore_backup: bool = True) -> bool:
        """Restore database from backup with safety measures"""
        try:
            if not os.path.exists(backup_path):
                self.logger.error(f"Backup file not found: {backup_path}")
                return False
            
            # Verify backup integrity before restoring
            backup_valid = True
            if backup_path.endswith('.gz'):
                # For compressed backups, we'll extract and verify
                temp_path = backup_path.replace('.gz', '.temp')
                try:
                    import gzip
                    with gzip.open(backup_path, 'rb') as f_in:
                        with open(temp_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    backup_valid = self._verify_backup_integrity(temp_path)
                    if backup_valid:
                        backup_path = temp_path
                    else:
                        os.remove(temp_path)
                except Exception as e:
                    self.logger.error(f"Error extracting compressed backup: {e}")
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                    return False
            else:
                backup_valid = self._verify_backup_integrity(backup_path)
            
            if not backup_valid:
                self.logger.error("Backup file integrity check failed")
                return False
            
            # Create a backup of current database before restoring
            if create_pre_restore_backup:
                current_backup = self.create_backup(
                    BackupType.EMERGENCY, 
                    f"Pre-restore backup - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
                if not current_backup:
                    self.logger.warning("Could not create pre-restore backup, continuing anyway")
            
            # Restore the backup
            shutil.copy2(backup_path, self.db_path)
            
            # Clean up temporary file if it was created
            if backup_path.endswith('.temp'):
                os.remove(backup_path)
            
            self.logger.info(f"Database restored from backup: {backup_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error restoring backup: {e}")
            return False
    
    def delete_backup(self, backup_path: str) -> bool:
        """Delete a backup file and its metadata"""
        try:
            if os.path.exists(backup_path):
                os.remove(backup_path)
            
            metadata_path = backup_path + '.meta'
            if os.path.exists(metadata_path):
                os.remove(metadata_path)
            
            self.logger.info(f"Deleted backup: {backup_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error deleting backup: {e}")
            return False
    
    def cleanup_old_backups(self, retention_days: int = None, max_count: int = None) -> int:
        """Clean up old backup files with multiple criteria"""
        if retention_days is None:
            retention_days = BACKUP_RETENTION_DAYS
        if max_count is None:
            max_count = MAX_BACKUP_COUNT
        
        deleted_count = 0
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        try:
            backups = self.list_backups()
            
            # Sort by timestamp, oldest first for deletion
            backups.sort(key=lambda x: x['timestamp'])
            
            for backup in backups:
                should_delete = False
                backup_path = backup.get('backup_path', '')
                backup_date = datetime.fromisoformat(backup['timestamp'])
                
                # Don't auto-delete manual backups unless they're really old
                if backup.get('type') == 'MANUAL':
                    manual_cutoff = datetime.now() - timedelta(days=retention_days * 2)
                    should_delete = backup_date < manual_cutoff
                else:
                    # Delete based on age
                    should_delete = backup_date < cutoff_date
                
                # Also delete if we have too many backups (keep manual ones)
                if not should_delete and len(backups) - deleted_count > max_count:
                    if backup.get('type') != 'MANUAL':
                        should_delete = True
                
                if should_delete and os.path.exists(backup_path):
                    try:
                        self.delete_backup(backup_path)
                        deleted_count += 1
                        self.logger.info(f"Deleted old backup: {os.path.basename(backup_path)}")
                    except Exception as e:
                        self.logger.warning(f"Could not delete backup {backup_path}: {e}")
            
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old backups: {e}")
            return 0
    
    def get_backup_statistics(self) -> Dict:
        """Get comprehensive backup statistics"""
        try:
            backups = self.list_backups()
            
            stats = {
                'total_backups': len(backups),
                'total_size_bytes': 0,
                'total_size_mb': 0,
                'backup_types': {},
                'oldest_backup': None,
                'newest_backup': None,
                'average_size_mb': 0,
                'compressed_count': 0,
                'integrity_verified_count': 0
            }
            
            if not backups:
                return stats
            
            for backup in backups:
                # Size statistics
                size_bytes = backup.get('size_bytes', 0)
                stats['total_size_bytes'] += size_bytes
                
                # Type statistics
                backup_type = backup.get('type', 'UNKNOWN')
                if backup_type not in stats['backup_types']:
                    stats['backup_types'][backup_type] = {'count': 0, 'size_bytes': 0}
                stats['backup_types'][backup_type]['count'] += 1
                stats['backup_types'][backup_type]['size_bytes'] += size_bytes
                
                # Compression and integrity
                if backup.get('compressed', False):
                    stats['compressed_count'] += 1
                if backup.get('integrity_verified', False):
                    stats['integrity_verified_count'] += 1
            
            # Calculate derived statistics
            stats['total_size_mb'] = round(stats['total_size_bytes'] / (1024 * 1024), 2)
            stats['average_size_mb'] = round(stats['total_size_mb'] / len(backups), 2)
            
            # Oldest and newest
            sorted_backups = sorted(backups, key=lambda x: x['timestamp'])
            stats['oldest_backup'] = sorted_backups[0]['timestamp']
            stats['newest_backup'] = sorted_backups[-1]['timestamp']
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting backup statistics: {e}")
            return {}
class ConnectionPool:
    """Thread-safe connection pool for SQLite with enhanced monitoring"""
    
    def __init__(self, db_path: str, pool_size: int = 5, timeout: float = 30.0):
        self.db_path = db_path
        self.pool_size = pool_size
        self.timeout = timeout
        self._pool = queue.Queue(maxsize=pool_size)
        self._all_connections = set()
        self._active_connections = 0
        self._total_connections_created = 0
        self._failed_connections = 0
        self._lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
        
        # Connection statistics
        self._connection_stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'pool_exhausted_count': 0,
            'dead_connections_detected': 0
        }
        
        # Initialize the pool
        self._initialize_pool()
        
        # Register cleanup on exit
        atexit.register(self.close_all)
    
    def _initialize_pool(self):
        """Initialize connection pool with pre-created connections"""
        for _ in range(self.pool_size):
            conn = self._create_connection()
            if conn:
                self._pool.put(conn)
    
    def _create_connection(self) -> Optional[sqlite3.Connection]:
        """Create a new database connection with optimal settings"""
        try:
            # Ensure directory exists
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
            
            # Create connection
            conn = sqlite3.connect(
                self.db_path,
                timeout=self.timeout,
                isolation_level='DEFERRED',
                check_same_thread=True
            )
            
            # Configure for optimal performance
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA cache_size = 10000")
            conn.execute("PRAGMA temp_store = MEMORY")
            conn.execute("PRAGMA mmap_size = 268435456")  # 256MB
            conn.execute("PRAGMA optimize")
            
            # Track connection
            self._all_connections.add(conn)
            self._total_connections_created += 1
            
            return conn
            
        except sqlite3.Error as e:
            self.logger.error(f"Failed to create database connection: {e}")
            self._failed_connections += 1
            return None
    
    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Get a connection from the pool using context manager"""
        conn = None
        with self._lock:
            self._connection_stats['total_requests'] += 1
        
        try:
            # Try to get from pool
            try:
                conn = self._pool.get(timeout=self.timeout)
            except queue.Empty:
                # Pool exhausted, create new connection
                with self._lock:
                    self._connection_stats['pool_exhausted_count'] += 1
                self.logger.warning("Connection pool exhausted, creating new connection")
                conn = self._create_connection()
                if not conn:
                    raise sqlite3.Error("Failed to create connection")
            
            # Test connection is alive
            try:
                conn.execute("SELECT 1")
            except sqlite3.Error:
                # Connection dead, create new one
                with self._lock:
                    self._connection_stats['dead_connections_detected'] += 1
                self.logger.warning("Dead connection detected, creating new one")
                try:
                    conn.close()
                except:
                    pass
                conn = self._create_connection()
                if not conn:
                    raise sqlite3.Error("Failed to create replacement connection")
            
            with self._lock:
                self._active_connections += 1
                self._connection_stats['successful_requests'] += 1
            
            yield conn
            
        except Exception as e:
            with self._lock:
                self._connection_stats['failed_requests'] += 1
            self.logger.error(f"Connection error: {e}")
            raise
        finally:
            # Return connection to pool
            if conn:
                try:
                    with self._lock:
                        self._active_connections -= 1
                    self._pool.put_nowait(conn)
                except queue.Full:
                    # Pool full, close connection
                    try:
                        conn.close()
                    except:
                        pass
    
    def get_pool_statistics(self) -> Dict:
        """Get connection pool statistics"""
        with self._lock:
            return {
                'pool_size': self.pool_size,
                'active_connections': self._active_connections,
                'available_connections': self._pool.qsize(),
                'total_connections_created': self._total_connections_created,
                'failed_connections': self._failed_connections,
                'connection_stats': self._connection_stats.copy(),
                'pool_utilization': (self.pool_size - self._pool.qsize()) / self.pool_size * 100
            }
    
    def health_check(self) -> bool:
        """Perform health check on the connection pool"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                return cursor.fetchone()[0] == 1
        except Exception as e:
            self.logger.error(f"Connection pool health check failed: {e}")
            return False
    
    def close_all(self):
        """Close all connections in the pool"""
        with self._lock:
            # Close pooled connections
            while not self._pool.empty():
                try:
                    conn = self._pool.get_nowait()
                    conn.close()
                except:
                    pass
            
            # Close tracked connections - FIXED VERSION
            connections_to_close = list(self._all_connections)  # Create a copy
            for conn in connections_to_close:
                try:
                    conn.close()
                except:
                    pass
            
            self._all_connections.clear()  # Clear the set
            self._active_connections = 0
class DatabaseManager:
    """Enhanced database manager with comprehensive features"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or DB_PATH
        self.logger = logging.getLogger(__name__)
        
        # Thread-safe connection pool
        self._pool = ConnectionPool(self.db_path, POOL_SIZE, QUERY_TIMEOUT)
        
        # Thread-local storage for transactions
        self._local = threading.local()
        
        # Global lock for schema changes
        self._schema_lock = threading.RLock()
        
        # Initialize backup manager
        self.backup_manager = BackupManager(self.db_path)
        
        # Performance monitoring
        self._query_stats = {
            'total_queries': 0,
            'failed_queries': 0,
            'slow_queries': 0,
            'transaction_count': 0,
            'rollback_count': 0
        }
        self._query_lock = threading.Lock()
        
        # Initialize database
        self._initialize_database()
        
        # Create initial backup if enabled
        if AUTO_BACKUP_ENABLED:
            self.backup_manager.create_backup(BackupType.AUTOMATIC, "Initial startup backup")
    
    def _backup_before_schema_change(self) -> Optional[str]:
        """Create automatic backup before schema modifications"""
        try:
            backup_path = self.backup_manager.create_backup(
                BackupType.SCHEMA_CHANGE,
                f"Pre-schema change backup - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            if backup_path:
                self.logger.info(f"Created schema change backup: {backup_path}")
            return backup_path
        except Exception as e:
            self.logger.error(f"Failed to create schema change backup: {e}")
            return None
    
    @contextmanager
    def transaction(self):
        """Context manager for database transactions with monitoring"""
        with self._pool.get_connection() as conn:
            # Store connection in thread-local storage
            self._local.conn = conn
            self._local.in_transaction = True
            
            with self._query_lock:
                self._query_stats['transaction_count'] += 1
            
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                with self._query_lock:
                    self._query_stats['rollback_count'] += 1
                raise
            finally:
                self._local.conn = None
                self._local.in_transaction = False
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get current connection (from transaction or pool)"""
        # Check if we're in a transaction
        if hasattr(self._local, 'in_transaction') and self._local.in_transaction:
            return self._local.conn
        
        # Otherwise, this should be called within a context manager
        raise RuntimeError("Database operation must be within a transaction or connection context")
    
    def _execute_query(self, query: str, params: tuple = None, fetch_results: bool = False):
        """Execute a query with proper connection management and monitoring"""
        start_time = time.time()
        
        with self._query_lock:
            self._query_stats['total_queries'] += 1
        
        try:
            if hasattr(self._local, 'in_transaction') and self._local.in_transaction:
                # We're in a transaction, use the transaction connection
                conn = self._local.conn
                cursor = conn.cursor()
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                if fetch_results:
                    return cursor.fetchall()
                return cursor.rowcount
            else:
                # Not in a transaction, use a pooled connection
                with self._pool.get_connection() as conn:
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
        except Exception as e:
            with self._query_lock:
                self._query_stats['failed_queries'] += 1
            self.logger.error(f"Query failed: {query[:100]}... Error: {e}")
            raise
        finally:
            # Monitor slow queries
            execution_time = time.time() - start_time
            if execution_time > 1.0:  # Queries taking more than 1 second
                with self._query_lock:
                    self._query_stats['slow_queries'] += 1
                self.logger.warning(f"Slow query detected ({execution_time:.2f}s): {query[:100]}...")
    
    # System Event Logging
    def _log_system_event(self, event_type: str, event_data: Dict = None, 
                         user_id: int = None, severity: str = 'INFO'):
        """Log a system event for audit purposes"""
        try:
            event_data_str = json.dumps(event_data) if event_data else None
            
            self._execute_query('''
                INSERT INTO system_events (event_type, event_data, user_id, severity)
                VALUES (?, ?, ?, ?)
            ''', (event_type, event_data_str, user_id, severity))
            
        except Exception as e:
            self.logger.error(f"Error logging system event: {e}")
    def _initialize_database(self):
        """Initialize database with all necessary tables"""
        with self._schema_lock:
            # Check if we need to create backup before schema changes
            schema_backup_created = False
            
            with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                
                # Check if this is a new database or schema update
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                existing_tables = {row[0] for row in cursor.fetchall()}
                
                if existing_tables and not schema_backup_created:
                    self._backup_before_schema_change()
                    schema_backup_created = True
                
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
                
                # GP Statistics table
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
                
                # Heartbeat Analytics table
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
                
                # Schema version tracking table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS schema_version (
                        version INTEGER PRIMARY KEY,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        description TEXT,
                        backup_created TEXT
                    )
                ''')
                
                # Query performance tracking table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS query_performance (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        query_hash TEXT NOT NULL,
                        query_type TEXT NOT NULL,
                        execution_time_ms REAL NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        affected_rows INTEGER DEFAULT 0
                    )
                ''')
                
                # System events table for audit logging
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS system_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_type TEXT NOT NULL,
                        event_data TEXT,
                        user_id INTEGER,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        severity TEXT DEFAULT 'INFO'
                    )
                ''')
                
                # Create indexes
                self._create_indexes(cursor)
                
                # Apply any schema migrations
                self._apply_schema_migrations(cursor)
                
                conn.commit()
                
                self.logger.info(f"Database initialized successfully at {self.db_path}")
    
    def _create_indexes(self, cursor):
        """Create database indexes for better performance"""
        indexes = [
            # User indexes
            "CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)",
            "CREATE INDEX IF NOT EXISTS idx_users_last_heartbeat ON users(last_heartbeat)",
            "CREATE INDEX IF NOT EXISTS idx_users_total_packs ON users(total_packs)",
            "CREATE INDEX IF NOT EXISTS idx_users_display_name ON users(display_name)",
            
            # Godpack indexes
            "CREATE INDEX IF NOT EXISTS idx_godpacks_state ON godpacks(state)",
            "CREATE INDEX IF NOT EXISTS idx_godpacks_timestamp ON godpacks(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_godpacks_expiration ON godpacks(expiration_date)",
            "CREATE INDEX IF NOT EXISTS idx_godpacks_pack_number ON godpacks(pack_number)",
            "CREATE INDEX IF NOT EXISTS idx_godpacks_name ON godpacks(name)",
            "CREATE INDEX IF NOT EXISTS idx_godpacks_friend_code ON godpacks(friend_code)",
            
            # Heartbeat indexes
            "CREATE INDEX IF NOT EXISTS idx_heartbeats_discord_id ON heartbeats(discord_id)",
            "CREATE INDEX IF NOT EXISTS idx_heartbeats_timestamp ON heartbeats(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_heartbeats_main_on ON heartbeats(main_on)",
            "CREATE INDEX IF NOT EXISTS idx_heartbeats_packs ON heartbeats(packs)",
            
            # Test result indexes
            "CREATE INDEX IF NOT EXISTS idx_test_results_gp_id ON test_results(gp_id)",
            "CREATE INDEX IF NOT EXISTS idx_test_results_discord_id ON test_results(discord_id)",
            "CREATE INDEX IF NOT EXISTS idx_test_results_timestamp ON test_results(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_test_results_type ON test_results(test_type)",
            
            # Statistics indexes
            "CREATE INDEX IF NOT EXISTS idx_gp_statistics_probability ON gp_statistics(probability_alive)",
            "CREATE INDEX IF NOT EXISTS idx_gp_statistics_total_tests ON gp_statistics(total_tests)",
            "CREATE INDEX IF NOT EXISTS idx_gp_statistics_last_calc ON gp_statistics(last_calculated)",
            
            # Heartbeat runs indexes
            "CREATE INDEX IF NOT EXISTS idx_heartbeat_runs_discord_id ON heartbeat_runs(discord_id)",
            "CREATE INDEX IF NOT EXISTS idx_heartbeat_runs_start_time ON heartbeat_runs(start_time)",
            "CREATE INDEX IF NOT EXISTS idx_heartbeat_runs_end_time ON heartbeat_runs(end_time)",
            
            # System monitoring indexes
            "CREATE INDEX IF NOT EXISTS idx_query_performance_timestamp ON query_performance(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_query_performance_type ON query_performance(query_type)",
            "CREATE INDEX IF NOT EXISTS idx_system_events_timestamp ON system_events(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_system_events_type ON system_events(event_type)",
            "CREATE INDEX IF NOT EXISTS idx_system_events_user_id ON system_events(user_id)"
        ]
        
        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
            except sqlite3.Error as e:
                self.logger.warning(f"Could not create index: {e}")
    
    def _apply_schema_migrations(self, cursor):
        """Apply schema migrations if needed"""
        try:
            # Get current schema version
            cursor.execute("SELECT MAX(version) FROM schema_version")
            result = cursor.fetchone()
            current_version = result[0] if result and result[0] is not None else 0
            
            # Define migrations
            migrations = [
                (1, "Add backup tracking and performance monitoring", self._migration_v1),
                (2, "Enhance performance indexes", self._migration_v2),
                (3, "Add system events and audit logging", self._migration_v3),
                (4, "Add advanced user statistics", self._migration_v4),
                (5, "Optimize godpack tracking", self._migration_v5)
            ]
            
            # Apply pending migrations
            for version, description, migration_func in migrations:
                if version > current_version:
                    self.logger.info(f"Applying migration v{version}: {description}")
                    try:
                        # Create backup before migration
                        backup_path = None
                        if AUTO_BACKUP_ENABLED:
                            backup_path = self.backup_manager.create_backup(
                                BackupType.MIGRATION,
                                f"Pre-migration v{version} backup"
                            )
                        
                        migration_func(cursor)
                        cursor.execute(
                            "INSERT INTO schema_version (version, description, backup_created) VALUES (?, ?, ?)",
                            (version, description, backup_path or "None")
                        )
                        self.logger.info(f"Migration v{version} completed successfully")
                    except Exception as e:
                        self.logger.error(f"Migration v{version} failed: {e}")
                        raise
            
        except sqlite3.Error as e:
            if "no such table: schema_version" in str(e).lower():
                # First time setup, insert initial version
                cursor.execute(
                    "INSERT INTO schema_version (version, description) VALUES (?, ?)",
                    (0, "Initial schema")
                )
            else:
                self.logger.error(f"Error applying schema migrations: {e}")
    
    def _migration_v1(self, cursor):
        """Migration v1: Add backup tracking and performance monitoring"""
        try:
            # Add columns to existing tables if they don't exist
            cursor.execute("PRAGMA table_info(users)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'last_backup' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN last_backup TIMESTAMP")
            
            if 'backup_count' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN backup_count INTEGER DEFAULT 0")
                
        except sqlite3.Error as e:
            if "duplicate column name" not in str(e).lower():
                raise
    
    def _migration_v2(self, cursor):
        """Migration v2: Enhance performance indexes"""
        additional_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_heartbeats_instances_online ON heartbeats(instances_online)",
            "CREATE INDEX IF NOT EXISTS idx_godpacks_ratio ON godpacks(ratio)",
            "CREATE INDEX IF NOT EXISTS idx_test_results_open_slots ON test_results(open_slots)",
            "CREATE INDEX IF NOT EXISTS idx_gp_statistics_confidence ON gp_statistics(confidence_level)"
        ]
        
        for index_sql in additional_indexes:
            try:
                cursor.execute(index_sql)
            except sqlite3.Error as e:
                self.logger.warning(f"Could not create additional index: {e}")
    
    def _migration_v3(self, cursor):
        """Migration v3: Add system events and audit logging"""
        # Tables already created in _initialize_database
        pass
    
    def _migration_v4(self, cursor):
        """Migration v4: Add advanced user statistics"""
        try:
            cursor.execute("PRAGMA table_info(users)")
            columns = [column[1] for column in cursor.fetchall()]
            
            new_columns = [
                ('total_online_time', 'INTEGER DEFAULT 0'),
                ('best_pack_streak', 'INTEGER DEFAULT 0'),
                ('current_streak', 'INTEGER DEFAULT 0'),
                ('last_activity', 'TIMESTAMP'),
                ('timezone_offset', 'INTEGER DEFAULT 0')
            ]
            
            for column_name, column_def in new_columns:
                if column_name not in columns:
                    cursor.execute(f"ALTER TABLE users ADD COLUMN {column_name} {column_def}")
                    
        except sqlite3.Error as e:
            if "duplicate column name" not in str(e).lower():
                raise
    
    def _migration_v5(self, cursor):
        """Migration v5: Optimize godpack tracking"""
        try:
            cursor.execute("PRAGMA table_info(godpacks)")
            columns = [column[1] for column in cursor.fetchall()]
            
            new_columns = [
                ('last_tested', 'TIMESTAMP'),
                ('test_count', 'INTEGER DEFAULT 0'),
                ('discovered_by', 'INTEGER'),
                ('verification_status', 'TEXT DEFAULT "UNVERIFIED"')
            ]
            
            for column_name, column_def in new_columns:
                if column_name not in columns:
                    cursor.execute(f"ALTER TABLE godpacks ADD COLUMN {column_name} {column_def}")
                    
        except sqlite3.Error as e:
            if "duplicate column name" not in str(e).lower():
                raise
# User Management Methods
    
    def add_user(self, discord_id: int, player_id: str = None, display_name: str = None, 
                 prefix: str = None) -> bool:
        """Add or update a user"""
        try:
            self._execute_query('''
                INSERT OR REPLACE INTO users 
                (discord_id, player_id, display_name, prefix, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (discord_id, player_id, display_name, prefix))
            
            self._log_system_event('USER_ADDED', {'discord_id': discord_id, 'display_name': display_name}, discord_id)
            self.logger.debug(f"Added/updated user: {discord_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding user {discord_id}: {e}")
            return False
    
    def get_user(self, discord_id: int) -> Optional[Dict]:
        """Get user information"""
        try:
            with self._pool.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('SELECT * FROM users WHERE discord_id = ?', (discord_id,))
                row = cursor.fetchone()
                
                return dict(row) if row else None
                
        except Exception as e:
            self.logger.error(f"Error getting user {discord_id}: {e}")
            return None
    
    def get_all_users(self, status_filter: str = None, limit: int = None) -> List[Dict]:
        """Get all users with optional filtering"""
        try:
            with self._pool.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                if status_filter:
                    query = 'SELECT * FROM users WHERE status = ? ORDER BY display_name'
                    params = (status_filter,)
                else:
                    query = 'SELECT * FROM users ORDER BY display_name'
                    params = ()
                
                if limit:
                    query += f' LIMIT {int(limit)}'
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            self.logger.error(f"Error getting all users: {e}")
            return []
    
    def update_user_status(self, discord_id: int, status: str) -> bool:
        """Update user status"""
        try:
            valid_statuses = ['active', 'inactive', 'farm', 'leech', 'banned', 'premium']
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
                self._log_system_event('USER_STATUS_CHANGED', {'discord_id': discord_id, 'new_status': status}, discord_id)
                self.logger.debug(f"Updated user {discord_id} status to {status}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error updating user status {discord_id}: {e}")
            return False
    
    def update_user_stats(self, discord_id: int, **kwargs) -> bool:
        """Update user statistics"""
        if not kwargs:
            return False
            
        try:
            # Build dynamic update query
            valid_fields = {
                'average_instances', 'total_packs', 'total_gps', 'last_heartbeat',
                'total_online_time', 'best_pack_streak', 'current_streak', 
                'last_activity', 'timezone_offset'
            }
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
            with self.transaction():
                affected_rows = self._execute_query(
                    'DELETE FROM users WHERE discord_id = ?', 
                    (discord_id,)
                )
                
                success = affected_rows > 0
                if success:
                    self._log_system_event('USER_DELETED', {'discord_id': discord_id}, discord_id)
                    self.logger.info(f"Deleted user {discord_id} and all related data")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Error deleting user {discord_id}: {e}")
            return False
    
    def get_user_statistics(self, discord_id: int) -> Dict:
        """Get comprehensive user statistics"""
        try:
            with self._pool.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Get user basic info
                cursor.execute('SELECT * FROM users WHERE discord_id = ?', (discord_id,))
                user_row = cursor.fetchone()
                
                if not user_row:
                    return {}
                
                user_data = dict(user_row)
                
                # Get heartbeat statistics
                cursor.execute('''
                    SELECT COUNT(*) as heartbeat_count,
                           MIN(timestamp) as first_heartbeat,
                           MAX(timestamp) as last_heartbeat,
                           AVG(instances_online) as avg_instances,
                           MAX(packs) as max_packs
                    FROM heartbeats 
                    WHERE discord_id = ?
                ''', (discord_id,))
                
                heartbeat_stats = dict(cursor.fetchone())
                
                # Get test participation
                cursor.execute('''
                    SELECT test_type, COUNT(*) as count
                    FROM test_results 
                    WHERE discord_id = ?
                    GROUP BY test_type
                ''', (discord_id,))
                
                test_stats = {row['test_type']: row['count'] for row in cursor.fetchall()}
                
                # Get recent activity (last 30 days)
                thirty_days_ago = datetime.now() - timedelta(days=30)
                cursor.execute('''
                    SELECT COUNT(*) as recent_heartbeats
                    FROM heartbeats 
                    WHERE discord_id = ? AND timestamp >= ?
                ''', (discord_id, thirty_days_ago))
                
                recent_activity = cursor.fetchone()['recent_heartbeats']
                
                return {
                    'user_info': user_data,
                    'heartbeat_stats': heartbeat_stats,
                    'test_participation': test_stats,
                    'recent_activity': recent_activity,
                    'is_active': recent_activity > 0
                }
                
        except Exception as e:
            self.logger.error(f"Error getting user statistics for {discord_id}: {e}")
            return {}
    
    def get_active_users(self, minutes_back: int = 60) -> List[Dict]:
        """Get users who have sent heartbeats recently"""
        try:
            with self._pool.get_connection() as conn:
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
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            self.logger.error(f"Error getting active users: {e}")
            return []
# God Pack Management Methods
    
    def add_godpack(self, message_id: int, timestamp: datetime, pack_number: int,
                   name: str, friend_code: str, state: GPState, screenshot_url: str,
                   ratio: int = -1, discovered_by: int = None) -> Optional[int]:
        """Add a new god pack"""
        try:
            # Calculate expiration date
            reset_time = timestamp.replace(hour=6, minute=0, second=0, microsecond=0)
            if timestamp < reset_time:
                expiration_date = reset_time + timedelta(days=3)
            else:
                expiration_date = reset_time + timedelta(days=4)
            
            with self.transaction() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO godpacks 
                    (message_id, timestamp, pack_number, name, friend_code, state, 
                     screenshot_url, ratio, expiration_date, discovered_by)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (message_id, timestamp, pack_number, name, friend_code, 
                      state.value, screenshot_url, ratio, expiration_date, discovered_by))
                
                gp_id = cursor.lastrowid
                
                # Initialize statistics
                cursor.execute('''
                    INSERT INTO gp_statistics (gp_id) VALUES (?)
                ''', (gp_id,))
                
                self._log_system_event('GODPACK_ADDED', {
                    'gp_id': gp_id, 'name': name, 'pack_number': pack_number
                }, discovered_by)
                
                self.logger.info(f"Added godpack {gp_id}: {name}")
                return gp_id
                
        except sqlite3.IntegrityError as e:
            self.logger.error(f"Godpack with message_id {message_id} already exists: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error adding godpack: {e}")
            return None
    
    def get_godpack(self, gp_id: int = None, message_id: int = None) -> Optional[GodPack]:
        """Get god pack by ID or message ID"""
        try:
            with self._pool.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                if gp_id:
                    cursor.execute('SELECT * FROM godpacks WHERE id = ?', (gp_id,))
                elif message_id:
                    cursor.execute('SELECT * FROM godpacks WHERE message_id = ?', (message_id,))
                else:
                    return None
                
                row = cursor.fetchone()
                
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
    
    def get_all_godpacks(self, state: GPState = None, limit: int = None, 
                        include_expired: bool = False) -> List[GodPack]:
        """Get all god packs with filtering options"""
        try:
            with self._pool.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                conditions = []
                params = []
                
                if state:
                    conditions.append('state = ?')
                    params.append(state.value)
                
                if not include_expired:
                    conditions.append('expiration_date > CURRENT_TIMESTAMP')
                
                where_clause = ''
                if conditions:
                    where_clause = 'WHERE ' + ' AND '.join(conditions)
                
                query = f'SELECT * FROM godpacks {where_clause} ORDER BY timestamp DESC'
                
                if limit:
                    query += f' LIMIT {int(limit)}'
                
                cursor.execute(query, tuple(params))
                rows = cursor.fetchall()
                
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
    
    def update_godpack_state(self, gp_id: int, state: GPState, updated_by: int = None) -> bool:
        """Update god pack state"""
        try:
            affected_rows = self._execute_query('''
                UPDATE godpacks 
                SET state = ?, updated_at = CURRENT_TIMESTAMP 
                WHERE id = ?
            ''', (state.value, gp_id))
            
            success = affected_rows > 0
            if success:
                self._log_system_event('GODPACK_STATE_CHANGED', {
                    'gp_id': gp_id, 'new_state': state.value
                }, updated_by)
                self.logger.info(f"Updated godpack {gp_id} state to {state.value}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error updating godpack state {gp_id}: {e}")
            return False
    
    def update_godpack_ratio(self, gp_id: int, ratio: int, updated_by: int = None) -> bool:
        """Update god pack ratio"""
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
                self._log_system_event('GODPACK_RATIO_CHANGED', {
                    'gp_id': gp_id, 'new_ratio': ratio
                }, updated_by)
                self.logger.debug(f"Updated godpack {gp_id} ratio to {ratio}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error updating godpack ratio {gp_id}: {e}")
            return False
    
    def get_expired_godpacks(self) -> List[GodPack]:
        """Get all expired god packs"""
        try:
            with self._pool.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM godpacks 
                    WHERE expiration_date < CURRENT_TIMESTAMP 
                    AND state NOT IN ('EXPIRED', 'DEAD', 'INVALID')
                    ORDER BY expiration_date ASC
                ''')
                
                rows = cursor.fetchall()
                
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
    
    def delete_godpack(self, gp_id: int, deleted_by: int = None) -> bool:
        """Delete a god pack and all related data"""
        try:
            with self.transaction():
                affected_rows = self._execute_query(
                    'DELETE FROM godpacks WHERE id = ?', 
                    (gp_id,)
                )
                
                success = affected_rows > 0
                if success:
                    self._log_system_event('GODPACK_DELETED', {'gp_id': gp_id}, deleted_by)
                    self.logger.info(f"Deleted godpack {gp_id} and all related data")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Error deleting godpack {gp_id}: {e}")
            return False
    
    def get_godpack_statistics(self, gp_id: int) -> Optional[GPStatistics]:
        """Get statistics for a specific god pack"""
        try:
            with self._pool.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('SELECT * FROM gp_statistics WHERE gp_id = ?', (gp_id,))
                row = cursor.fetchone()
                
                if row:
                    return GPStatistics(
                        gp_id=row['gp_id'],
                        probability_alive=row['probability_alive'],
                        total_tests=row['total_tests'],
                        miss_tests=row['miss_tests'],
                        noshow_tests=row['noshow_tests'],
                        confidence_level=row['confidence_level'],
                        last_calculated=datetime.fromisoformat(row['last_calculated']) if row['last_calculated'] else None
                    )
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting godpack statistics {gp_id}: {e}")
            return None
    
    def update_godpack_statistics(self, gp_id: int, probability_alive: float, 
                                 total_tests: int, miss_tests: int, noshow_tests: int,
                                 confidence_level: float) -> bool:
        """Update god pack statistics"""
        try:
            affected_rows = self._execute_query('''
                UPDATE gp_statistics 
                SET probability_alive = ?, total_tests = ?, miss_tests = ?, 
                    noshow_tests = ?, confidence_level = ?, last_calculated = CURRENT_TIMESTAMP
                WHERE gp_id = ?
            ''', (probability_alive, total_tests, miss_tests, noshow_tests, confidence_level, gp_id))
            
            success = affected_rows > 0
            if success:
                self.logger.debug(f"Updated statistics for godpack {gp_id}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error updating godpack statistics {gp_id}: {e}")
            return False
# Heartbeat Management Methods
    
    def add_heartbeat(self, message_id: int, discord_id: int, timestamp: datetime,
                     instances_online: int, instances_offline: int, time: int,
                     packs: int, main_on: bool, selected_packs: List[str] = None) -> bool:
        """Add a heartbeat entry"""
        try:
            # Validate inputs
            if instances_online < 0 or instances_offline < 0:
                self.logger.error("Invalid instance counts")
                return False
            
            if time < 0 or packs < 0:
                self.logger.error("Invalid time or packs values")
                return False
            
            selected_packs_str = json.dumps(selected_packs) if selected_packs else None
            
            with self.transaction() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO heartbeats 
                    (message_id, discord_id, timestamp, instances_online, instances_offline,
                     time, packs, main_on, selected_packs)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (message_id, discord_id, timestamp, instances_online, instances_offline,
                      time, packs, main_on, selected_packs_str))
                
                # Update user's last heartbeat and statistics
                cursor.execute('''
                    UPDATE users 
                    SET last_heartbeat = ?, total_packs = ?, last_activity = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE discord_id = ?
                ''', (timestamp, packs, discord_id))
                
                # If user doesn't exist, create them
                if cursor.rowcount == 0:
                    cursor.execute('''
                        INSERT OR IGNORE INTO users (discord_id, last_heartbeat, total_packs, last_activity)
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ''', (discord_id, timestamp, packs))
                
                self.logger.debug(f"Added heartbeat for user {discord_id}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error adding heartbeat: {e}")
            return False
    
    def get_heartbeat(self, message_id: int = None, discord_id: int = None, latest: bool = False) -> Optional[HeartBeat]:
        """Get heartbeat by message ID, or latest for a user"""
        try:
            with self._pool.get_connection() as conn:
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
    
    def get_heartbeats_for_user(self, discord_id: int, days_back: int = 7, 
                               limit: int = None) -> List[HeartBeat]:
        """Get heartbeats for a user within specified days"""
        try:
            with self._pool.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                since_date = datetime.now() - timedelta(days=days_back)
                
                query = '''
                    SELECT * FROM heartbeats 
                    WHERE discord_id = ? AND timestamp >= ?
                    ORDER BY timestamp ASC
                '''
                params = [discord_id, since_date]
                
                if limit:
                    query += f' LIMIT {int(limit)}'
                
                cursor.execute(query, tuple(params))
                rows = cursor.fetchall()
                
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
    
    def add_heartbeat_run(self, discord_id: int, start_time: datetime, end_time: datetime,
                         start_packs: int, end_packs: int, average_instances: float) -> bool:
        """Add a heartbeat run record"""
        try:
            self._execute_query('''
                INSERT INTO heartbeat_runs 
                (discord_id, start_time, end_time, start_packs, end_packs, average_instances)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (discord_id, start_time, end_time, start_packs, end_packs, average_instances))
            
            self.logger.debug(f"Added heartbeat run for user {discord_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding heartbeat run: {e}")
            return False
    
    def get_heartbeat_runs(self, discord_id: int, days_back: int = 30) -> List[HeartbeatRun]:
        """Get heartbeat runs for a user"""
        try:
            with self._pool.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                since_date = datetime.now() - timedelta(days=days_back)
                
                cursor.execute('''
                    SELECT * FROM heartbeat_runs 
                    WHERE discord_id = ? AND start_time >= ?
                    ORDER BY start_time DESC
                ''', (discord_id, since_date))
                
                rows = cursor.fetchall()
                
                runs = []
                for row in rows:
                    try:
                        runs.append(HeartbeatRun(
                            id=row['id'],
                            discord_id=row['discord_id'],
                            start_time=datetime.fromisoformat(row['start_time']),
                            end_time=datetime.fromisoformat(row['end_time']),
                            start_packs=row['start_packs'],
                            end_packs=row['end_packs'],
                            average_instances=row['average_instances'],
                            created_at=datetime.fromisoformat(row['created_at']) if row['created_at'] else None
                        ))
                    except Exception as e:
                        self.logger.error(f"Error parsing heartbeat run row: {e}")
                        continue
                
                return runs
                
        except Exception as e:
            self.logger.error(f"Error getting heartbeat runs for user {discord_id}: {e}")
            return []
# Test Results Methods
    
    def add_test_result(self, discord_id: int, gp_id: int, test_type: TestType,
                       open_slots: int = -1, number_friends: int = -1) -> bool:
        """Add a test result"""
        try:
            # Validate inputs
            if test_type == TestType.NOSHOW:
                if open_slots < 0 or number_friends < 0:
                    self.logger.error("No-show tests require valid open_slots and number_friends")
                    return False
            
            with self.transaction() as conn:
                cursor = conn.cursor()
                
                # Add the test result
                cursor.execute('''
                    INSERT INTO test_results 
                    (discord_id, gp_id, test_type, open_slots, number_friends)
                    VALUES (?, ?, ?, ?, ?)
                ''', (discord_id, gp_id, test_type.value, open_slots, number_friends))
                
                # Update godpack test count and last tested
                cursor.execute('''
                    UPDATE godpacks 
                    SET test_count = COALESCE(test_count, 0) + 1, 
                        last_tested = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (gp_id,))
                
                self._log_system_event('TEST_RESULT_ADDED', {
                    'gp_id': gp_id, 'test_type': test_type.value
                }, discord_id)
                
                self.logger.debug(f"Added {test_type.value} test for GP {gp_id} by user {discord_id}")
                return True
            
        except Exception as e:
            self.logger.error(f"Error adding test result: {e}")
            return False
    
    def get_test_results(self, gp_id: int) -> List[TestResult]:
        """Get all test results for a god pack"""
        try:
            with self._pool.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM test_results 
                    WHERE gp_id = ? 
                    ORDER BY timestamp ASC
                ''', (gp_id,))
                
                rows = cursor.fetchall()
                
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
    
    def get_user_test_history(self, discord_id: int, days_back: int = 30) -> List[TestResult]:
        """Get test history for a user"""
        try:
            with self._pool.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                since_date = datetime.now() - timedelta(days=days_back)
                
                cursor.execute('''
                    SELECT * FROM test_results 
                    WHERE discord_id = ? AND timestamp >= ?
                    ORDER BY timestamp DESC
                ''', (discord_id, since_date))
                
                rows = cursor.fetchall()
                
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
            self.logger.error(f"Error getting test history for user {discord_id}: {e}")
            return []
    
    # System Events Methods
    
    def get_system_events(self, event_type: str = None, user_id: int = None, 
                         days_back: int = 7, limit: int = 100) -> List[Dict]:
        """Get system events with filtering"""
        try:
            with self._pool.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                conditions = []
                params = []
                
                since_date = datetime.now() - timedelta(days=days_back)
                conditions.append('timestamp >= ?')
                params.append(since_date)
                
                if event_type:
                    conditions.append('event_type = ?')
                    params.append(event_type)
                
                if user_id:
                    conditions.append('user_id = ?')
                    params.append(user_id)
                
                where_clause = 'WHERE ' + ' AND '.join(conditions)
                
                cursor.execute(f'''
                    SELECT * FROM system_events 
                    {where_clause}
                    ORDER BY timestamp DESC 
                    LIMIT {int(limit)}
                ''', tuple(params))
                
                rows = cursor.fetchall()
                
                events = []
                for row in rows:
                    event = dict(row)
                    if event['event_data']:
                        try:
                            event['event_data'] = json.loads(event['event_data'])
                        except:
                            pass
                    events.append(event)
                
                return events
                
        except Exception as e:
            self.logger.error(f"Error getting system events: {e}")
            return []
    
    def get_system_event_summary(self, days_back: int = 7) -> Dict:
        """Get a summary of system events"""
        try:
            with self._pool.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                since_date = datetime.now() - timedelta(days=days_back)
                
                # Get event counts by type
                cursor.execute('''
                    SELECT event_type, COUNT(*) as count, severity
                    FROM system_events 
                    WHERE timestamp >= ?
                    GROUP BY event_type, severity
                    ORDER BY count DESC
                ''', (since_date,))
                
                event_counts = cursor.fetchall()
                
                # Get top active users
                cursor.execute('''
                    SELECT user_id, COUNT(*) as event_count
                    FROM system_events 
                    WHERE timestamp >= ? AND user_id IS NOT NULL
                    GROUP BY user_id
                    ORDER BY event_count DESC
                    LIMIT 10
                ''', (since_date,))
                
                active_users = cursor.fetchall()
                
                # Get recent critical events
                cursor.execute('''
                    SELECT * FROM system_events 
                    WHERE timestamp >= ? AND severity = 'CRITICAL'
                    ORDER BY timestamp DESC
                    LIMIT 5
                ''', (since_date,))
                
                critical_events = cursor.fetchall()
                
                return {
                    'event_counts': [dict(row) for row in event_counts],
                    'active_users': [dict(row) for row in active_users],
                    'critical_events': [dict(row) for row in critical_events],
                    'period_days': days_back
                }
                
        except Exception as e:
            self.logger.error(f"Error getting system event summary: {e}")
            return {}
    
    def add_expiration_warning(self, gp_id: int) -> bool:
        """Add an expiration warning for a god pack"""
        try:
            self._execute_query('''
                INSERT INTO expiration_warnings (gp_id)
                VALUES (?)
            ''', (gp_id,))
            
            self._log_system_event('EXPIRATION_WARNING_SENT', {'gp_id': gp_id})
            self.logger.debug(f"Added expiration warning for GP {gp_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding expiration warning: {e}")
            return False
    
    def get_expiration_warnings(self, gp_id: int = None, days_back: int = 7) -> List[Dict]:
        """Get expiration warnings"""
        try:
            with self._pool.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                since_date = datetime.now() - timedelta(days=days_back)
                
                if gp_id:
                    cursor.execute('''
                        SELECT * FROM expiration_warnings 
                        WHERE gp_id = ? AND warned_at >= ?
                        ORDER BY warned_at DESC
                    ''', (gp_id, since_date))
                else:
                    cursor.execute('''
                        SELECT ew.*, gp.name, gp.pack_number 
                        FROM expiration_warnings ew
                        JOIN godpacks gp ON ew.gp_id = gp.id
                        WHERE ew.warned_at >= ?
                        ORDER BY ew.warned_at DESC
                    ''', (since_date,))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            self.logger.error(f"Error getting expiration warnings: {e}")
            return []
# Backup and Maintenance Methods
    
    def list_backups(self, backup_type: BackupType = None, limit: int = None) -> List[Dict]:
        """List all available database backups"""
        return self.backup_manager.list_backups(backup_type, limit)
    
    def create_manual_backup(self, description: str = None) -> Optional[str]:
        """Create a manual backup"""
        return self.backup_manager.create_backup(BackupType.MANUAL, description)
    
    def restore_from_backup(self, backup_path: str) -> bool:
        """Restore database from backup"""
        success = self.backup_manager.restore_backup(backup_path)
        if success:
            self._log_system_event('DATABASE_RESTORED', {'backup_path': backup_path}, severity='CRITICAL')
        return success
    
    def get_backup_statistics(self) -> Dict:
        """Get backup statistics"""
        return self.backup_manager.get_backup_statistics()
    
    # Utility Methods
    
    def cleanup_old_data(self, days_to_keep: int = 30) -> Tuple[int, int, int, int, int]:
        """Clean up old data and return counts of deleted records"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            with self.transaction():
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
                
                # Clean up old system events (keep more recent ones)
                system_event_cutoff = datetime.now() - timedelta(days=days_to_keep * 2)
                deleted_events = self._execute_query(
                    'DELETE FROM system_events WHERE timestamp < ?', 
                    (system_event_cutoff,)
                )
                
                # Clean up old backups if enabled
                deleted_backups = 0
                if AUTO_BACKUP_ENABLED:
                    deleted_backups = self.backup_manager.cleanup_old_backups(days_to_keep)
                
                self._log_system_event('DATA_CLEANUP', {
                    'heartbeats': deleted_heartbeats,
                    'tests': deleted_tests,
                    'runs': deleted_runs,
                    'warnings': deleted_warnings,
                    'events': deleted_events,
                    'backups': deleted_backups
                })
                
                self.logger.info(f"Cleaned up {deleted_heartbeats} heartbeats, {deleted_tests} test results, "
                               f"{deleted_runs} runs, {deleted_warnings} warnings, {deleted_events} events, "
                               f"and {deleted_backups} backups")
                
                return deleted_heartbeats, deleted_tests, deleted_runs, deleted_warnings, deleted_events
                
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
            return 0, 0, 0, 0, 0
    
    def backup_database(self, backup_path: str) -> bool:
        """Create a backup of the database (legacy method)"""
        try:
            # Ensure backup directory exists
            backup_dir = os.path.dirname(backup_path)
            if backup_dir and not os.path.exists(backup_dir):
                os.makedirs(backup_dir, exist_ok=True)
            
            with self._pool.get_connection() as source:
                backup = sqlite3.connect(backup_path)
                source.backup(backup)
                backup.close()
            
            self.logger.info(f"Database backed up to {backup_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error backing up database: {e}")
            return False
    
    def get_database_info(self) -> Dict:
        """Get comprehensive information about the database"""
        try:
            with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get table sizes
                tables = ['users', 'godpacks', 'heartbeats', 'test_results', 
                         'gp_statistics', 'heartbeat_runs', 'expiration_warnings', 
                         'schema_version', 'query_performance', 'system_events']
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
                
                # Get schema version
                try:
                    cursor.execute("SELECT MAX(version) FROM schema_version")
                    schema_version = cursor.fetchone()[0] or 0
                except:
                    schema_version = 0
                
                # Get WAL mode info
                cursor.execute("PRAGMA journal_mode")
                journal_mode = cursor.fetchone()[0]
                
                # Get additional PRAGMA info
                cursor.execute("PRAGMA cache_size")
                cache_size = cursor.fetchone()[0]
                
                cursor.execute("PRAGMA synchronous")
                synchronous = cursor.fetchone()[0]
                
                return {
                    'database_path': self.db_path,
                    'size_bytes': db_size,
                    'size_mb': round(db_size / (1024 * 1024), 2),
                    'tables': table_info,
                    'total_records': sum(table_info.values()),
                    'integrity_check': integrity_result == 'ok',
                    'schema_version': schema_version,
                    'journal_mode': journal_mode,
                    'wal_mode': journal_mode.upper() == 'WAL',
                    'cache_size': cache_size,
                    'synchronous': synchronous,
                    'pool_size': self._pool.pool_size,
                    'backup_enabled': AUTO_BACKUP_ENABLED,
                    'backup_count': len(self.list_backups()),
                    'query_stats': self._query_stats.copy()
                }
                
        except Exception as e:
            self.logger.error(f"Error getting database info: {e}")
            return {'error': str(e)}
    
    def get_performance_stats(self) -> Dict:
        """Get database performance statistics"""
        try:
            with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get cache statistics
                cursor.execute("PRAGMA cache_size")
                cache_size = cursor.fetchone()[0]
                
                cursor.execute("PRAGMA cache_spill")
                cache_spill = cursor.fetchone()[0]
                
                # Get index information
                cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='index'")
                index_count = cursor.fetchone()[0]
                
                # Get WAL checkpoint info
                try:
                    cursor.execute("PRAGMA wal_checkpoint")
                    wal_info = cursor.fetchone()
                except:
                    wal_info = (0, 0, 0)
                
                # Get connection pool stats
                pool_stats = self._pool.get_pool_statistics()
                
                return {
                    'cache_size': cache_size,
                    'cache_spill': cache_spill,
                    'index_count': index_count,
                    'wal_busy': wal_info[0] if wal_info else 0,
                    'wal_log_pages': wal_info[1] if wal_info else 0,
                    'wal_checkpointed_pages': wal_info[2] if wal_info else 0,
                    'connection_pool': pool_stats,
                    'query_performance': self._query_stats.copy()
                }
                
        except Exception as e:
            self.logger.error(f"Error getting performance stats: {e}")
            return {'error': str(e)}
    
    def vacuum_database(self) -> bool:
        """Vacuum the database to reclaim space"""
        try:
            # Create backup before vacuum
            if AUTO_BACKUP_ENABLED:
                self.backup_manager.create_backup(BackupType.AUTOMATIC, "Pre-vacuum backup")
            
            with self._pool.get_connection() as conn:
                conn.execute("VACUUM")
            
            self._log_system_event('DATABASE_VACUUM', severity='INFO')
            self.logger.info("Database vacuumed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error vacuuming database: {e}")
            return False
    
    def analyze_database(self) -> bool:
        """Analyze database to update statistics"""
        try:
            with self._pool.get_connection() as conn:
                conn.execute("ANALYZE")
            
            self._log_system_event('DATABASE_ANALYZE', severity='INFO')
            self.logger.info("Database analyzed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error analyzing database: {e}")
            return False
    
    def optimize_database(self) -> bool:
        """Optimize database performance"""
        try:
            with self._pool.get_connection() as conn:
                # Run PRAGMA optimize
                conn.execute("PRAGMA optimize")
                
                # Update table statistics
                conn.execute("ANALYZE")
                
                # Checkpoint WAL file
                try:
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                except:
                    pass
            
            self._log_system_event('DATABASE_OPTIMIZE', severity='INFO')
            self.logger.info("Database optimized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error optimizing database: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                
                # Test basic query
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                
                # Test table access
                cursor.execute("SELECT COUNT(*) FROM users")
                cursor.fetchone()
                
                # Test transaction
                with self.transaction():
                    pass
                
                return result[0] == 1
                
        except Exception as e:
            self.logger.error(f"Database connection test failed: {e}")
            return False
    def health_check(self) -> Dict:
        """Comprehensive database health check"""
        try:
            health_status = {
                'overall_healthy': True,
                'issues': [],
                'warnings': [],
                'checks_performed': []
            }
            
            # Test basic connectivity
            try:
                if not self.test_connection():
                    health_status['overall_healthy'] = False
                    health_status['issues'].append('Database connection failed')
                else:
                    health_status['checks_performed'].append('Connection test: PASSED')
            except Exception as e:
                health_status['overall_healthy'] = False
                health_status['issues'].append(f'Connection test failed: {e}')
            
            # Check database integrity
            try:
                with self._pool.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("PRAGMA integrity_check")
                    result = cursor.fetchone()[0]
                    
                    if result != "ok":
                        health_status['overall_healthy'] = False
                        health_status['issues'].append(f'Integrity check failed: {result}')
                    else:
                        health_status['checks_performed'].append('Integrity check: PASSED')
            except Exception as e:
                health_status['overall_healthy'] = False
                health_status['issues'].append(f'Integrity check failed: {e}')
            
            # Check schema version
            try:
                current_version = self.get_schema_version()
                health_status['checks_performed'].append(f'Schema version: {current_version}')
            except Exception as e:
                health_status['warnings'].append(f'Could not check schema version: {e}')
            
            # Check connection pool health
            try:
                pool_stats = self._pool.get_pool_statistics()
                if pool_stats['connection_stats']['failed_requests'] > 0:
                    failed_ratio = pool_stats['connection_stats']['failed_requests'] / max(1, pool_stats['connection_stats']['total_requests'])
                    if failed_ratio > 0.1:  # More than 10% failure rate
                        health_status['warnings'].append(f'High connection failure rate: {failed_ratio:.2%}')
                
                health_status['checks_performed'].append('Connection pool: CHECKED')
            except Exception as e:
                health_status['warnings'].append(f'Could not check connection pool: {e}')
            
            # Check query performance
            try:
                with self._query_lock:
                    query_stats = self._query_stats.copy()
                
                if query_stats['total_queries'] > 0:
                    failure_rate = query_stats['failed_queries'] / query_stats['total_queries']
                    if failure_rate > 0.05:  # More than 5% failure rate
                        health_status['warnings'].append(f'High query failure rate: {failure_rate:.2%}')
                    
                    if query_stats['slow_queries'] > query_stats['total_queries'] * 0.1:  # More than 10% slow queries
                        health_status['warnings'].append('High number of slow queries detected')
                
                health_status['checks_performed'].append('Query performance: CHECKED')
            except Exception as e:
                health_status['warnings'].append(f'Could not check query performance: {e}')
            
            # Check disk space
            try:
                db_info = self.get_database_info()
                if db_info.get('size_mb', 0) > 1000:  # Warn if DB is over 1GB
                    health_status['warnings'].append(f'Large database size: {db_info["size_mb"]} MB')
                
                health_status['checks_performed'].append('Database size: CHECKED')
            except Exception as e:
                health_status['warnings'].append(f'Could not check database size: {e}')
            
            # Check backup status
            try:
                backup_stats = self.get_backup_statistics()
                if backup_stats.get('total_backups', 0) == 0:
                    health_status['warnings'].append('No backups available')
                else:
                    # Check if we have recent backups
                    newest_backup = backup_stats.get('newest_backup')
                    if newest_backup:
                        newest_date = datetime.fromisoformat(newest_backup)
                        days_since_backup = (datetime.now() - newest_date).days
                        if days_since_backup > 7:
                            health_status['warnings'].append(f'No recent backups (last backup: {days_since_backup} days ago)')
                
                health_status['checks_performed'].append('Backup status: CHECKED')
            except Exception as e:
                health_status['warnings'].append(f'Could not check backup status: {e}')
            
            return health_status
            
        except Exception as e:
            self.logger.error(f"Error during health check: {e}")
            return {
                'overall_healthy': False,
                'issues': [f'Health check failed: {e}'],
                'warnings': [],
                'checks_performed': []
            }
    
    def get_schema_version(self) -> int:
        """Get current schema version"""
        try:
            with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT MAX(version) FROM schema_version")
                result = cursor.fetchone()
                return result[0] if result and result[0] is not None else 0
        except:
            return 0
    
    def get_table_sizes(self) -> Dict[str, int]:
        """Get the number of records in each table"""
        try:
            with self._pool.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get all table names
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                tables = [row[0] for row in cursor.fetchall()]
                
                table_sizes = {}
                for table in tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        table_sizes[table] = count
                    except Exception as e:
                        self.logger.warning(f"Could not get size for table {table}: {e}")
                        table_sizes[table] = 0
                
                return table_sizes
                
        except Exception as e:
            self.logger.error(f"Error getting table sizes: {e}")
            return {}
    
    def get_query_statistics(self) -> Dict:
        """Get detailed query performance statistics"""
        with self._query_lock:
            stats = self._query_stats.copy()
        
        # Calculate derived statistics
        if stats['total_queries'] > 0:
            stats['failure_rate'] = stats['failed_queries'] / stats['total_queries']
            stats['slow_query_rate'] = stats['slow_queries'] / stats['total_queries']
            stats['rollback_rate'] = stats['rollback_count'] / max(1, stats['transaction_count'])
        else:
            stats['failure_rate'] = 0.0
            stats['slow_query_rate'] = 0.0
            stats['rollback_rate'] = 0.0
        
        return stats
    
    def reset_query_statistics(self):
        """Reset query performance statistics"""
        with self._query_lock:
            self._query_stats = {
                'total_queries': 0,
                'failed_queries': 0,
                'slow_queries': 0,
                'transaction_count': 0,
                'rollback_count': 0
            }
        
        self.logger.info("Query statistics reset")
    
    def export_data(self, table_name: str, output_file: str, format: str = 'json') -> bool:
        """Export table data to file"""
        try:
            with self._pool.get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute(f"SELECT * FROM {table_name}")
                rows = cursor.fetchall()
                
                data = [dict(row) for row in rows]
                
                if format.lower() == 'json':
                    import json
                    with open(output_file, 'w') as f:
                        json.dump(data, f, indent=2, default=str)
                elif format.lower() == 'csv':
                    import csv
                    if data:
                        with open(output_file, 'w', newline='') as f:
                            writer = csv.DictWriter(f, fieldnames=data[0].keys())
                            writer.writeheader()
                            writer.writerows(data)
                else:
                    raise ValueError(f"Unsupported format: {format}")
                
                self._log_system_event('DATA_EXPORT', {
                    'table': table_name, 'format': format, 'records': len(data)
                })
                
                self.logger.info(f"Exported {len(data)} records from {table_name} to {output_file}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error exporting data from {table_name}: {e}")
            return False
    
    def import_data(self, table_name: str, input_file: str, format: str = 'json') -> bool:
        """Import data from file to table"""
        try:
            # Load data from file
            if format.lower() == 'json':
                with open(input_file, 'r') as f:
                    data = json.load(f)
            elif format.lower() == 'csv':
                import csv
                data = []
                with open(input_file, 'r') as f:
                    reader = csv.DictReader(f)
                    data = list(reader)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            if not data:
                self.logger.warning(f"No data to import from {input_file}")
                return True
            
            # Import data in batches
            batch_size = 1000
            imported_count = 0
            
            with self.transaction() as conn:
                cursor = conn.cursor()
                
                # Get table columns
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [row[1] for row in cursor.fetchall()]
                
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    
                    for record in batch:
                        # Filter record to only include valid columns
                        filtered_record = {k: v for k, v in record.items() if k in columns}
                        
                        if filtered_record:
                            placeholders = ', '.join(['?' for _ in filtered_record])
                            column_names = ', '.join(filtered_record.keys())
                            
                            cursor.execute(f'''
                                INSERT OR REPLACE INTO {table_name} ({column_names})
                                VALUES ({placeholders})
                            ''', list(filtered_record.values()))
                            
                            imported_count += 1
            
            self._log_system_event('DATA_IMPORT', {
                'table': table_name, 'format': format, 'records': imported_count
            })
            
            self.logger.info(f"Imported {imported_count} records to {table_name} from {input_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error importing data to {table_name}: {e}")
            return False
    
    def get_database_statistics(self) -> Dict:
        """Get comprehensive database statistics"""
        try:
            stats = {
                'basic_info': self.get_database_info(),
                'performance': self.get_performance_stats(),
                'table_sizes': self.get_table_sizes(),
                'query_stats': self.get_query_statistics(),
                'backup_stats': self.get_backup_statistics(),
                'health_check': self.health_check()
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting database statistics: {e}")
            return {'error': str(e)}
    
    def close(self):
        """Close all database connections and cleanup"""
        try:
            # Log final statistics
            self._log_system_event('DATABASE_SHUTDOWN', {
                'query_stats': self.get_query_statistics(),
                'uptime_info': 'Database manager shutting down'
            })
            
            # Close connection pool
            self._pool.close_all()
            
            self.logger.info("Database connections closed and cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during database cleanup: {e}")

# Factory function for creating database manager instances
def create_database_manager(db_path: str = None, pool_size: int = None) -> DatabaseManager:
    """Factory function to create a properly configured DatabaseManager instance"""
    if pool_size is None:
        pool_size = POOL_SIZE
    
    # Temporarily override pool size if needed
    original_pool_size = POOL_SIZE
    if pool_size != POOL_SIZE:
        globals()['POOL_SIZE'] = pool_size
    
    try:
        db_manager = DatabaseManager(db_path)
        return db_manager
    finally:
        # Restore original pool size
        globals()['POOL_SIZE'] = original_pool_size

# Utility functions for common database operations
def migrate_database(old_db_path: str, new_db_path: str) -> bool:
    """Migrate data from old database to new database format"""
    try:
        # Create new database
        new_db = DatabaseManager(new_db_path)
        
        # Connect to old database
        old_conn = sqlite3.connect(old_db_path)
        old_conn.row_factory = sqlite3.Row
        
        # Migrate users
        cursor = old_conn.cursor()
        cursor.execute("SELECT * FROM users")
        for row in cursor.fetchall():
            new_db.add_user(
                discord_id=row['discord_id'],
                player_id=row.get('player_id'),
                display_name=row.get('display_name'),
                prefix=row.get('prefix')
            )
        
        # Migrate godpacks
        cursor.execute("SELECT * FROM godpacks")
        for row in cursor.fetchall():
            new_db.add_godpack(
                message_id=row['message_id'],
                timestamp=datetime.fromisoformat(row['timestamp']),
                pack_number=row['pack_number'],
                name=row['name'],
                friend_code=row['friend_code'],
                state=GPState(row['state']),
                screenshot_url=row['screenshot_url'],
                ratio=row.get('ratio', -1)
            )
        
        # Migrate heartbeats
        cursor.execute("SELECT * FROM heartbeats")
        for row in cursor.fetchall():
            selected_packs = json.loads(row['selected_packs']) if row.get('selected_packs') else None
            new_db.add_heartbeat(
                message_id=row['message_id'],
                discord_id=row['discord_id'],
                timestamp=datetime.fromisoformat(row['timestamp']),
                instances_online=row['instances_online'],
                instances_offline=row['instances_offline'],
                time=row['time'],
                packs=row['packs'],
                main_on=bool(row['main_on']),
                selected_packs=selected_packs
            )
        
        # Migrate test results
        cursor.execute("SELECT * FROM test_results")
        for row in cursor.fetchall():
            new_db.add_test_result(
                discord_id=row['discord_id'],
                gp_id=row['gp_id'],
                test_type=TestType(row['test_type']),
                open_slots=row.get('open_slots', -1),
                number_friends=row.get('number_friends', -1)
            )
        
        old_conn.close()
        new_db.close()
        
        logging.getLogger(__name__).info(f"Successfully migrated database from {old_db_path} to {new_db_path}")
        return True
        
    except Exception as e:
        logging.getLogger(__name__).error(f"Error during database migration: {e}")
        return False

def validate_database_integrity(db_path: str) -> bool:
    """Validate database file integrity"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Run integrity check
        cursor.execute("PRAGMA integrity_check")
        result = cursor.fetchone()[0]
        
        conn.close()
        return result == "ok"
        
    except Exception as e:
        logging.getLogger(__name__).error(f"Error validating database integrity: {e}")
        return False

def get_database_size(db_path: str) -> Dict:
    """Get database file size information"""
    try:
        if not os.path.exists(db_path):
            return {'exists': False}
        
        stat = os.stat(db_path)
        size_bytes = stat.st_size
        
        return {
            'exists': True,
            'size_bytes': size_bytes,
            'size_mb': round(size_bytes / (1024 * 1024), 2),
            'size_gb': round(size_bytes / (1024 * 1024 * 1024), 3),
            'modified_time': datetime.fromtimestamp(stat.st_mtime).isoformat()
        }
        
    except Exception as e:
        logging.getLogger(__name__).error(f"Error getting database size: {e}")
        return {'exists': False, 'error': str(e)}