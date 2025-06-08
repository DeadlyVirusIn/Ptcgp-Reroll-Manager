import os
import json
import asyncio
import threading
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import logging
import sqlite3

# Handle optional imports gracefully
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

class GoogleSheetsIntegration:
    def __init__(self, db_manager, credentials_file: str = "credentials.json"):
        self.db = db_manager
        self.logger = logging.getLogger(__name__)
        self.credentials_file = credentials_file
        self.client = None
        self.spreadsheets = {}
        self.lock = threading.RLock()
        self.sync_enabled = False
        
        # Initialize Google Sheets client
        self._initialize_client()

    def _initialize_client(self):
        """Initialize Google Sheets API client with proper error handling"""
        try:
            # Check if gspread is available
            if not GSPREAD_AVAILABLE:
                self.logger.warning("gspread or oauth2client not installed - Google Sheets integration disabled")
                self.sync_enabled = False
                return
            
            # Check if credentials file exists
            if not os.path.exists(self.credentials_file):
                self.logger.warning(f"Google Sheets credentials file not found: {self.credentials_file}")
                self.logger.info("Google Sheets integration disabled - to enable:")
                self.logger.info("1. Create a Google Cloud Project")
                self.logger.info("2. Enable Google Sheets API")
                self.logger.info("3. Create service account credentials")
                self.logger.info("4. Save credentials as 'credentials.json'")
                self.sync_enabled = False
                return
            
            # Validate credentials file format
            try:
                with open(self.credentials_file, 'r') as f:
                    creds_data = json.load(f)
                    
                # Basic validation of credentials structure
                required_fields = ['type', 'project_id', 'private_key', 'client_email']
                missing_fields = [field for field in required_fields if field not in creds_data]
                
                if missing_fields:
                    self.logger.error(f"Invalid credentials file - missing fields: {missing_fields}")
                    self.sync_enabled = False
                    return
                    
            except json.JSONDecodeError:
                self.logger.error("Invalid JSON format in credentials file")
                self.sync_enabled = False
                return
            except Exception as e:
                self.logger.error(f"Error reading credentials file: {e}")
                self.sync_enabled = False
                return
            
            # Initialize Google Sheets client
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
                "https://www.googleapis.com/auth/drive"
            ]
            
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                self.credentials_file, scope
            )
            self.client = gspread.authorize(creds)
            self.sync_enabled = True
            self.logger.info("Google Sheets integration initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Google Sheets client: {e}")
            self.logger.info("Google Sheets integration disabled")
            self.sync_enabled = False

    async def setup_guild_spreadsheet(self, guild_id: int, spreadsheet_name: str) -> bool:
        """Set up a spreadsheet for a specific guild with enhanced error handling"""
        if not self.sync_enabled:
            self.logger.warning("Google Sheets integration not available")
            return False
        
        try:
            # Validate inputs
            if not guild_id or not spreadsheet_name:
                self.logger.error("Invalid guild_id or spreadsheet_name")
                return False
            
            # Try to open existing spreadsheet or create new one
            try:
                spreadsheet = self.client.open(spreadsheet_name)
                self.logger.info(f"Opened existing spreadsheet: {spreadsheet_name}")
            except gspread.SpreadsheetNotFound:
                try:
                    spreadsheet = self.client.create(spreadsheet_name)
                    self.logger.info(f"Created new spreadsheet: {spreadsheet_name}")
                except Exception as e:
                    self.logger.error(f"Failed to create spreadsheet: {e}")
                    return False
            
            # Store spreadsheet reference safely
            with self.lock:
                self.spreadsheets[guild_id] = {
                    'spreadsheet': spreadsheet,
                    'name': spreadsheet_name,
                    'worksheets': {}
                }
            
            # Set up required worksheets
            success = await self._setup_worksheets(guild_id)
            if not success:
                self.logger.error(f"Failed to setup worksheets for guild {guild_id}")
                return False
            
            self.logger.info(f"Successfully set up spreadsheet for guild {guild_id}: {spreadsheet_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error setting up spreadsheet for guild {guild_id}: {e}")
            return False

    async def _setup_worksheets(self, guild_id: int) -> bool:
        """Set up required worksheets for a guild with error handling"""
        if guild_id not in self.spreadsheets:
            return False
        
        try:
            spreadsheet = self.spreadsheets[guild_id]['spreadsheet']
            worksheets = {}
            
            # Define worksheet configurations
            worksheet_configs = {
                'Users': {
                    'headers': [
                        'Discord ID', 'Player ID', 'Display Name', 'Status', 
                        'Total Packs', 'Total GPs', 'Efficiency Score', 'Last Active'
                    ]
                },
                'GodPacks': {
                    'headers': [
                        'GP ID', 'Message ID', 'Name', 'Friend Code', 'State', 
                        'Pack Number', 'Ratio', 'Probability', 'Created', 'Expires'
                    ]
                },
                'Statistics': {
                    'headers': [
                        'Date', 'Active Users', 'Total Instances', 'Packs/Hour', 
                        'GPs Found', 'Success Rate'
                    ]
                },
                'FriendIDs': {
                    'headers': ['Friend ID', 'Status', 'Pack Type']
                }
            }
            
            for sheet_name, config in worksheet_configs.items():
                try:
                    # Try to get existing worksheet
                    try:
                        worksheet = spreadsheet.worksheet(sheet_name)
                        self.logger.info(f"Found existing worksheet: {sheet_name}")
                    except gspread.WorksheetNotFound:
                        # Create new worksheet
                        try:
                            worksheet = spreadsheet.add_worksheet(
                                title=sheet_name, 
                                rows=1000, 
                                cols=len(config['headers'])
                            )
                            self.logger.info(f"Created new worksheet: {sheet_name}")
                        except Exception as e:
                            self.logger.error(f"Failed to create worksheet {sheet_name}: {e}")
                            continue
                    
                    # Set headers safely
                    try:
                        worksheet.update('1:1', [config['headers']])
                        self.logger.debug(f"Updated headers for worksheet: {sheet_name}")
                    except Exception as e:
                        self.logger.error(f"Failed to update headers for {sheet_name}: {e}")
                        continue
                    
                    # Store worksheet reference
                    worksheets[sheet_name] = worksheet
                    
                except Exception as e:
                    self.logger.error(f"Error setting up worksheet {sheet_name}: {e}")
                    continue
            
            # Store worksheet references safely
            with self.lock:
                self.spreadsheets[guild_id]['worksheets'] = worksheets
            
            return len(worksheets) > 0
            
        except Exception as e:
            self.logger.error(f"Error in _setup_worksheets: {e}")
            return False

    async def sync_users_to_sheet(self, guild_id: int) -> bool:
        """Sync user data to Google Sheets with enhanced error handling"""
        if not self._is_guild_configured(guild_id):
            return False
        
        try:
            # Get users from database safely
            users_data = self._get_users_from_database()
            if not users_data:
                self.logger.info("No user data to sync")
                return True
            
            # Prepare data for sheets
            sheet_data = []
            for user in users_data:
                try:
                    # Calculate efficiency score safely
                    total_packs = user.get('total_packs', 0) or 0
                    total_gps = user.get('total_gps', 0) or 0
                    efficiency = (total_packs / max(1, total_gps)) if total_gps else 0
                    
                    last_active = user.get('last_heartbeat', 'Never') or 'Never'
                    
                    sheet_data.append([
                        str(user.get('discord_id', '')),
                        str(user.get('player_id', '')),
                        str(user.get('display_name', '')),
                        str(user.get('status', 'inactive')),
                        int(total_packs),
                        int(total_gps),
                        round(efficiency, 2),
                        str(last_active)
                    ])
                except Exception as e:
                    self.logger.error(f"Error processing user data: {e}")
                    continue
            
            if not sheet_data:
                self.logger.warning("No valid user data to sync")
                return False
            
            # Update worksheet safely
            try:
                worksheet = self.spreadsheets[guild_id]['worksheets']['Users']
                
                # Clear existing data (except headers)
                try:
                    worksheet.batch_clear(['A2:Z1000'])
                except Exception as e:
                    self.logger.warning(f"Could not clear existing data: {e}")
                
                # Update with new data
                if sheet_data:
                    range_str = f'A2:H{len(sheet_data) + 1}'
                    worksheet.update(range_str, sheet_data)
                
                self.logger.info(f"Successfully synced {len(sheet_data)} users to sheet for guild {guild_id}")
                return True
                
            except Exception as e:
                self.logger.error(f"Error updating worksheet: {e}")
                return False
            
        except Exception as e:
            self.logger.error(f"Error syncing users to sheet for guild {guild_id}: {e}")
            return False

    def _get_users_from_database(self) -> List[Dict]:
        """Safely get users from database"""
        try:
            with self.db.lock:
                conn = sqlite3.connect(self.db.db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT discord_id, player_id, display_name, status, 
                           total_packs, total_gps, last_heartbeat
                    FROM users
                    ORDER BY total_packs DESC
                ''')
                
                users = cursor.fetchall()
                conn.close()
                
                return [dict(user) for user in users]
                
        except Exception as e:
            self.logger.error(f"Error getting users from database: {e}")
            return []

    async def sync_godpacks_to_sheet(self, guild_id: int, days_back: int = 30) -> bool:
        """Sync god pack data to Google Sheets with error handling"""
        if not self._is_guild_configured(guild_id):
            return False
        
        try:
            # Get god packs from database safely
            godpacks_data = self._get_godpacks_from_database(days_back)
            
            # Prepare data for sheets
            sheet_data = []
            for gp in godpacks_data:
                try:
                    probability_str = f"{gp.get('probability_alive', 0):.1f}%" if gp.get('probability_alive') else ''
                    
                    sheet_data.append([
                        str(gp.get('id', '')),
                        str(gp.get('message_id', '')),
                        str(gp.get('name', '')),
                        str(gp.get('friend_code', '')),
                        str(gp.get('state', '')),
                        int(gp.get('pack_number', 0)),
                        str(gp.get('ratio', '')) if gp.get('ratio', -1) > 0 else '',
                        probability_str,
                        str(gp.get('timestamp', '')),
                        str(gp.get('expiration_date', ''))
                    ])
                except Exception as e:
                    self.logger.error(f"Error processing godpack data: {e}")
                    continue
            
            # Update worksheet safely
            try:
                worksheet = self.spreadsheets[guild_id]['worksheets']['GodPacks']
                
                # Clear existing data (except headers)
                worksheet.batch_clear(['A2:Z1000'])
                
                # Update with new data
                if sheet_data:
                    range_str = f'A2:J{len(sheet_data) + 1}'
                    worksheet.update(range_str, sheet_data)
                
                self.logger.info(f"Successfully synced {len(sheet_data)} god packs to sheet for guild {guild_id}")
                return True
                
            except Exception as e:
                self.logger.error(f"Error updating godpacks worksheet: {e}")
                return False
            
        except Exception as e:
            self.logger.error(f"Error syncing god packs to sheet for guild {guild_id}: {e}")
            return False

    def _get_godpacks_from_database(self, days_back: int) -> List[Dict]:
        """Safely get godpacks from database"""
        try:
            with self.db.lock:
                conn = sqlite3.connect(self.db.db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                since_date = datetime.now() - timedelta(days=days_back)
                
                cursor.execute('''
                    SELECT g.*, gs.probability_alive
                    FROM godpacks g
                    LEFT JOIN gp_statistics gs ON g.id = gs.gp_id
                    WHERE g.timestamp >= ?
                    ORDER BY g.timestamp DESC
                ''', (since_date,))
                
                godpacks = cursor.fetchall()
                conn.close()
                
                return [dict(gp) for gp in godpacks]
                
        except Exception as e:
            self.logger.error(f"Error getting godpacks from database: {e}")
            return []

    async def update_daily_statistics(self, guild_id: int) -> bool:
        """Update daily statistics in the spreadsheet with error handling"""
        if not self._is_guild_configured(guild_id):
            return False
        
        try:
            # Calculate today's statistics safely
            today = datetime.now().strftime('%Y-%m-%d')
            stats_data = self._calculate_daily_statistics()
            
            if not stats_data:
                self.logger.warning("No statistics data to update")
                return False
            
            # Update worksheet safely
            try:
                worksheet = self.spreadsheets[guild_id]['worksheets']['Statistics']
                
                # Find next empty row
                existing_data = worksheet.get_all_values()
                next_row = len(existing_data) + 1
                
                # Add new data
                worksheet.update(f'A{next_row}:F{next_row}', [stats_data])
                
                self.logger.info(f"Successfully updated daily statistics for guild {guild_id}")
                return True
                
            except Exception as e:
                self.logger.error(f"Error updating statistics worksheet: {e}")
                return False
            
        except Exception as e:
            self.logger.error(f"Error updating daily statistics for guild {guild_id}: {e}")
            return False

    def _calculate_daily_statistics(self) -> List:
        """Calculate daily statistics safely"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            
            with self.db.lock:
                conn = sqlite3.connect(self.db.db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Get active users today
                cursor.execute('''
                    SELECT COUNT(DISTINCT discord_id) as active_users
                    FROM heartbeats
                    WHERE DATE(timestamp) = DATE('now')
                ''')
                active_users_result = cursor.fetchone()
                active_users = active_users_result['active_users'] if active_users_result else 0
                
                # Get average instances
                cursor.execute('''
                    SELECT AVG(instances_online + instances_offline) as avg_instances
                    FROM heartbeats
                    WHERE DATE(timestamp) = DATE('now')
                ''')
                avg_instances_result = cursor.fetchone()
                avg_instances = avg_instances_result['avg_instances'] or 0
                
                # Get god packs found today
                cursor.execute('''
                    SELECT COUNT(*) as gps_found
                    FROM godpacks
                    WHERE DATE(timestamp) = DATE('now')
                ''')
                gps_result = cursor.fetchone()
                gps_found = gps_result['gps_found'] if gps_result else 0
                
                # Get success rate (alive/expired vs total)
                cursor.execute('''
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN state IN ('ALIVE', 'EXPIRED') THEN 1 ELSE 0 END) as successful
                    FROM godpacks
                    WHERE DATE(timestamp) >= DATE('now', '-7 days')
                    AND state != 'INVALID'
                ''')
                success_result = cursor.fetchone()
                
                if success_result and success_result['total'] > 0:
                    success_rate = (success_result['successful'] / success_result['total']) * 100
                else:
                    success_rate = 0
                
                conn.close()
            
            # Calculate packs per hour (simplified estimate)
            packs_per_hour = round(avg_instances * 0.5, 1)
            
            return [
                today,
                int(active_users),
                round(avg_instances, 1),
                packs_per_hour,
                int(gps_found),
                f"{success_rate:.1f}%"
            ]
            
        except Exception as e:
            self.logger.error(f"Error calculating daily statistics: {e}")
            return []

    async def get_spreadsheet_url(self, guild_id: int) -> Optional[str]:
        """Get the URL of the guild's spreadsheet safely"""
        try:
            if guild_id in self.spreadsheets:
                return self.spreadsheets[guild_id]['spreadsheet'].url
        except Exception as e:
            self.logger.error(f"Error getting spreadsheet URL: {e}")
        return None

    def _is_guild_configured(self, guild_id: int) -> bool:
        """Check if a guild has been configured for sheets integration"""
        if not self.sync_enabled:
            return False
        
        try:
            return (guild_id in self.spreadsheets and 
                    'worksheets' in self.spreadsheets[guild_id] and
                    len(self.spreadsheets[guild_id]['worksheets']) > 0)
        except Exception:
            return False

    async def full_sync(self, guild_id: int) -> Dict[str, bool]:
        """Perform a full sync of all data to sheets with error handling"""
        results = {
            'users': False,
            'godpacks': False,
            'statistics': False
        }
        
        if not self._is_guild_configured(guild_id):
            self.logger.warning(f"Guild {guild_id} not configured for sheets sync")
            return results
        
        # Sync all data types with individual error handling
        try:
            results['users'] = await self.sync_users_to_sheet(guild_id)
        except Exception as e:
            self.logger.error(f"Error syncing users: {e}")
        
        try:
            results['godpacks'] = await self.sync_godpacks_to_sheet(guild_id)
        except Exception as e:
            self.logger.error(f"Error syncing godpacks: {e}")
        
        try:
            results['statistics'] = await self.update_daily_statistics(guild_id)
        except Exception as e:
            self.logger.error(f"Error syncing statistics: {e}")
        
        success_count = sum(results.values())
        self.logger.info(f"Full sync completed for guild {guild_id}: {success_count}/3 successful")
        
        return results

    def get_integration_status(self) -> Dict:
        """Get the current status of the Google Sheets integration"""
        try:
            return {
                'enabled': self.sync_enabled,
                'gspread_available': GSPREAD_AVAILABLE,
                'credentials_found': os.path.exists(self.credentials_file),
                'configured_guilds': list(self.spreadsheets.keys()),
                'total_spreadsheets': len(self.spreadsheets)
            }
        except Exception as e:
            self.logger.error(f"Error getting integration status: {e}")
            return {
                'enabled': False,
                'gspread_available': GSPREAD_AVAILABLE,
                'credentials_found': False,
                'configured_guilds': [],
                'total_spreadsheets': 0
            }

    def test_connection(self) -> bool:
        """Test the Google Sheets connection"""
        if not self.sync_enabled:
            return False
        
        try:
            # Try to access the client
            if self.client is None:
                return False
            
            # Try to list spreadsheets (this will test the connection)
            self.client.openall()
            return True
            
        except Exception as e:
            self.logger.error(f"Google Sheets connection test failed: {e}")
            return False