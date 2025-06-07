import asyncio
import aiohttp
import json
import os
import datetime
from typing import Dict, Any, Optional, List, Union
import base64
import hashlib

# Import configuration
try:
    import config
except ImportError:
    config = None

# GITHUB GIST UPLOAD UTILITIES

class GistUploader:
    """Handler for GitHub Gist uploads and updates."""
    
    def __init__(self, token: str = None, gist_id: str = None):
        """Initialize the Gist uploader."""
        self.token = token or getattr(config, 'github_token', None)
        self.gist_id = gist_id or getattr(config, 'gist_id', None)
        self.base_url = "https://api.github.com"
        self.session = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers for GitHub API requests."""
        headers = {
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'Discord-Reroll-Bot'
        }
        
        if self.token:
            headers['Authorization'] = f'token {self.token}'
            
        return headers
    
    async def create_gist(self, files: Dict[str, str], description: str = "Discord Bot Data", public: bool = False) -> Optional[str]:
        """Create a new gist and return its ID."""
        try:
            if not self.session:
                async with aiohttp.ClientSession() as session:
                    return await self._create_gist_request(session, files, description, public)
            else:
                return await self._create_gist_request(self.session, files, description, public)
                
        except Exception as e:
            print(f"❌ Error creating gist: {e}")
            return None
    
    async def _create_gist_request(self, session: aiohttp.ClientSession, files: Dict[str, str], description: str, public: bool) -> Optional[str]:
        """Internal method to create gist request."""
        url = f"{self.base_url}/gists"
        
        # Format files for GitHub API
        formatted_files = {}
        for filename, content in files.items():
            formatted_files[filename] = {"content": content}
        
        data = {
            "description": description,
            "public": public,
            "files": formatted_files
        }
        
        async with session.post(url, headers=self._get_headers(), json=data) as response:
            if response.status == 201:
                result = await response.json()
                gist_id = result.get('id')
                print(f"✅ Gist created successfully: {gist_id}")
                return gist_id
            else:
                error_text = await response.text()
                print(f"❌ Failed to create gist: {response.status} - {error_text}")
                return None
    
    async def update_gist(self, files: Dict[str, str], description: str = None) -> bool:
        """Update an existing gist."""
        try:
            if not self.gist_id:
                print("❌ No gist ID provided for update")
                return False
                
            if not self.session:
                async with aiohttp.ClientSession() as session:
                    return await self._update_gist_request(session, files, description)
            else:
                return await self._update_gist_request(self.session, files, description)
                
        except Exception as e:
            print(f"❌ Error updating gist: {e}")
            return False
    
    async def _update_gist_request(self, session: aiohttp.ClientSession, files: Dict[str, str], description: str) -> bool:
        """Internal method to update gist request."""
        url = f"{self.base_url}/gists/{self.gist_id}"
        
        # Format files for GitHub API
        formatted_files = {}
        for filename, content in files.items():
            formatted_files[filename] = {"content": content}
        
        data = {"files": formatted_files}
        if description:
            data["description"] = description
        
        async with session.patch(url, headers=self._get_headers(), json=data) as response:
            if response.status == 200:
                print(f"✅ Gist updated successfully: {self.gist_id}")
                return True
            else:
                error_text = await response.text()
                print(f"❌ Failed to update gist: {response.status} - {error_text}")
                return False
    
    async def get_gist_content(self) -> Optional[Dict[str, str]]:
        """Get the current content of the gist."""
        try:
            if not self.gist_id:
                print("❌ No gist ID provided")
                return None
                
            if not self.session:
                async with aiohttp.ClientSession() as session:
                    return await self._get_gist_content_request(session)
            else:
                return await self._get_gist_content_request(self.session)
                
        except Exception as e:
            print(f"❌ Error getting gist content: {e}")
            return None
    
    async def _get_gist_content_request(self, session: aiohttp.ClientSession) -> Optional[Dict[str, str]]:
        """Internal method to get gist content."""
        url = f"{self.base_url}/gists/{self.gist_id}"
        
        async with session.get(url, headers=self._get_headers()) as response:
            if response.status == 200:
                result = await response.json()
                files = {}
                
                for filename, file_data in result.get('files', {}).items():
                    files[filename] = file_data.get('content', '')
                
                return files
            else:
                error_text = await response.text()
                print(f"❌ Failed to get gist content: {response.status} - {error_text}")
                return None

# PASTEBIN UPLOAD UTILITIES

class PastebinUploader:
    """Handler for Pastebin uploads."""
    
    def __init__(self, api_key: str = None, user_key: str = None):
        """Initialize the Pastebin uploader."""
        self.api_key = api_key or getattr(config, 'pastebin_api_key', None)
        self.user_key = user_key or getattr(config, 'pastebin_user_key', None)
        self.base_url = "https://pastebin.com/api"
    
    async def upload_paste(self, content: str, title: str = "Discord Bot Data", 
                          privacy: int = 1, expiration: str = 'N') -> Optional[str]:
        """Upload content to Pastebin and return the URL."""
        try:
            if not self.api_key:
                print("❌ No Pastebin API key provided")
                return None
            
            data = {
                'api_dev_key': self.api_key,
                'api_option': 'paste',
                'api_paste_code': content,
                'api_paste_name': title,
                'api_paste_private': str(privacy),  # 0=public, 1=unlisted, 2=private
                'api_paste_expire_date': expiration  # N=never, 10M=10 minutes, 1H=1 hour, etc.
            }
            
            if self.user_key:
                data['api_user_key'] = self.user_key
            
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.base_url}/api_post.php", data=data) as response:
                    result = await response.text()
                    
                    if response.status == 200 and result.startswith('https://pastebin.com/'):
                        print(f"✅ Paste uploaded successfully: {result}")
                        return result
                    else:
                        print(f"❌ Failed to upload paste: {result}")
                        return None
                        
        except Exception as e:
            print(f"❌ Error uploading to Pastebin: {e}")
            return None

# FILE UPLOAD UTILITIES

class FileUploader:
    """Handler for various file upload services."""
    
    @staticmethod
    async def upload_to_discord_webhook(webhook_url: str, file_path: str, filename: str = None) -> Optional[str]:
        """Upload a file to Discord via webhook."""
        try:
            if not os.path.exists(file_path):
                print(f"❌ File not found: {file_path}")
                return None
            
            filename = filename or os.path.basename(file_path)
            
            with open(file_path, 'rb') as file:
                data = aiohttp.FormData()
                data.add_field('file', file, filename=filename)
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(webhook_url, data=data) as response:
                        if response.status == 200:
                            result = await response.json()
                            attachments = result.get('attachments', [])
                            if attachments:
                                url = attachments[0].get('url')
                                print(f"✅ File uploaded to Discord: {url}")
                                return url
                        
                        print(f"❌ Failed to upload file to Discord: {response.status}")
                        return None
                        
        except Exception as e:
            print(f"❌ Error uploading file to Discord: {e}")
            return None
    
    @staticmethod
    async def upload_to_catbox(file_path: str) -> Optional[str]:
        """Upload a file to catbox.moe (anonymous file hosting)."""
        try:
            if not os.path.exists(file_path):
                print(f"❌ File not found: {file_path}")
                return None
            
            with open(file_path, 'rb') as file:
                data = aiohttp.FormData()
                data.add_field('fileToUpload', file)
                data.add_field('reqtype', 'fileupload')
                
                async with aiohttp.ClientSession() as session:
                    async with session.post('https://catbox.moe/user/api.php', data=data) as response:
                        if response.status == 200:
                            result = await response.text()
                            if result.startswith('https://files.catbox.moe/'):
                                print(f"✅ File uploaded to Catbox: {result}")
                                return result
                        
                        print(f"❌ Failed to upload file to Catbox: {response.status}")
                        return None
                        
        except Exception as e:
            print(f"❌ Error uploading file to Catbox: {e}")
            return None

# DATA FORMATTING UTILITIES

def format_user_data_for_upload() -> str:
    """Format user data for upload."""
    try:
        from core_utils import get_all_users, get_attrib_value_from_user, get_id_from_user, get_username_from_user
        
        users = asyncio.run(get_all_users())
        formatted_data = []
        
        formatted_data.append("Discord Reroll Bot - User Data Export")
        formatted_data.append(f"Generated: {datetime.datetime.now().isoformat()}")
        formatted_data.append("=" * 50)
        formatted_data.append("")
        
        for user in users:
            user_id = get_id_from_user(user)
            username = get_username_from_user(user)
            
            total_packs = get_attrib_value_from_user(user, 'total_packs_opened', 0)
            total_time = get_attrib_value_from_user(user, 'total_time', 0)
            user_state = get_attrib_value_from_user(user, 'user_state', 'inactive')
            
            formatted_data.append(f"User: {username} ({user_id})")
            formatted_data.append(f"  Status: {user_state}")
            formatted_data.append(f"  Total Packs: {total_packs}")
            formatted_data.append(f"  Total Time: {total_time} minutes")
            formatted_data.append("")
        
        return "\n".join(formatted_data)
        
    except Exception as e:
        print(f"❌ Error formatting user data: {e}")
        return f"Error formatting user data: {e}"

def format_server_stats_for_upload() -> str:
    """Format server statistics for upload."""
    try:
        from core_utils import get_all_users, get_active_users, get_server_data_gps
        
        all_users = asyncio.run(get_all_users())
        active_users = asyncio.run(get_active_users())
        
        # Get GP data
        live_gps = asyncio.run(get_server_data_gps('live_gp'))
        eligible_gps = asyncio.run(get_server_data_gps('eligible_gp'))
        
        formatted_data = []
        formatted_data.append("Discord Reroll Bot - Server Statistics")
        formatted_data.append(f"Generated: {datetime.datetime.now().isoformat()}")
        formatted_data.append("=" * 50)
        formatted_data.append("")
        
        # Basic stats
        formatted_data.append("📊 Server Overview:")
        formatted_data.append(f"  Total Users: {len(all_users)}")
        formatted_data.append(f"  Active Users: {len(active_users)}")
        formatted_data.append("")
        
        # GP stats
        formatted_data.append("✨ GodPack Statistics:")
        formatted_data.append(f"  Live GPs: {len(live_gps) if live_gps else 0}")
        formatted_data.append(f"  Eligible GPs: {len(eligible_gps) if eligible_gps else 0}")
        
        if eligible_gps:
            success_rate = (len(live_gps) / len(eligible_gps) * 100) if live_gps else 0
            formatted_data.append(f"  Success Rate: {success_rate:.1f}%")
        
        formatted_data.append("")
        
        return "\n".join(formatted_data)
        
    except Exception as e:
        print(f"❌ Error formatting server stats: {e}")
        return f"Error formatting server stats: {e}"

def format_ids_for_upload() -> str:
    """Format active IDs for upload."""
    try:
        from core_utils import get_active_ids
        
        ids_content = asyncio.run(get_active_ids())
        
        formatted_data = []
        formatted_data.append("Discord Reroll Bot - Active IDs")
        formatted_data.append(f"Generated: {datetime.datetime.now().isoformat()}")
        formatted_data.append("=" * 40)
        formatted_data.append("")
        formatted_data.append(ids_content)
        
        return "\n".join(formatted_data)
        
    except Exception as e:
        print(f"❌ Error formatting IDs: {e}")
        return f"Error formatting IDs: {e}"

# MAIN UPLOAD FUNCTIONS

async def update_gist(include_stats: bool = True, include_ids: bool = True) -> bool:
    """Update the configured gist with current data."""
    try:
        if not config or not hasattr(config, 'github_token') or not hasattr(config, 'gist_id'):
            print("❌ GitHub configuration not found")
            return False
        
        files_to_upload = {}
        
        # Add IDs file
        if include_ids:
            ids_content = format_ids_for_upload()
            files_to_upload['active_ids.txt'] = ids_content
        
        # Add stats file
        if include_stats:
            stats_content = format_server_stats_for_upload()
            files_to_upload['server_stats.txt'] = stats_content
            
            # Add detailed user data
            user_data_content = format_user_data_for_upload()
            files_to_upload['user_data.txt'] = user_data_content
        
        # Upload to gist
        async with GistUploader() as uploader:
            success = await uploader.update_gist(
                files_to_upload,
                description=f"Discord Bot Data - Updated {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            
            if success:
                print("✅ Gist updated successfully")
                return True
            else:
                print("❌ Failed to update gist")
                return False
                
    except Exception as e:
        print(f"❌ Error updating gist: {e}")
        return False

async def backup_to_pastebin(content: str, title: str = "Bot Backup") -> Optional[str]:
    """Backup content to Pastebin."""
    try:
        uploader = PastebinUploader()
        url = await uploader.upload_paste(
            content=content,
            title=f"{title} - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            privacy=1,  # Unlisted
            expiration='1M'  # 1 month
        )
        
        if url:
            print(f"✅ Backup uploaded to Pastebin: {url}")
            return url
        else:
            print("❌ Failed to backup to Pastebin")
            return None
            
    except Exception as e:
        print(f"❌ Error backing up to Pastebin: {e}")
        return None

async def upload_file_to_discord(webhook_url: str, file_path: str, filename: str = None) -> Optional[str]:
    """Upload a file to Discord via webhook."""
    try:
        uploader = FileUploader()
        url = await uploader.upload_to_discord_webhook(webhook_url, file_path, filename)
        
        if url:
            print(f"✅ File uploaded to Discord: {url}")
            return url
        else:
            print("❌ Failed to upload file to Discord")
            return None
            
    except Exception as e:
        print(f"❌ Error uploading file to Discord: {e}")
        return None

async def upload_logs_to_catbox(log_file_path: str) -> Optional[str]:
    """Upload log files to Catbox for sharing."""
    try:
        if not os.path.exists(log_file_path):
            print(f"❌ Log file not found: {log_file_path}")
            return None
        
        uploader = FileUploader()
        url = await uploader.upload_to_catbox(log_file_path)
        
        if url:
            print(f"✅ Logs uploaded to Catbox: {url}")
            return url
        else:
            print("❌ Failed to upload logs to Catbox")
            return None
            
    except Exception as e:
        print(f"❌ Error uploading logs: {e}")
        return None

# SCHEDULED UPLOAD FUNCTIONS

async def scheduled_backup() -> bool:
    """Perform scheduled backup of all data."""
    try:
        success_count = 0
        total_operations = 0
        
        # Update gist if configured
        if config and hasattr(config, 'upload_gist') and config.upload_gist:
            total_operations += 1
            if await update_gist():
                success_count += 1
        
        # Backup to Pastebin if configured
        if config and hasattr(config, 'backup_to_pastebin') and config.backup_to_pastebin:
            total_operations += 1
            user_data = format_user_data_for_upload()
            if await backup_to_pastebin(user_data, "Scheduled User Data Backup"):
                success_count += 1
        
        # Upload logs if configured
        if config and hasattr(config, 'upload_logs') and config.upload_logs:
            log_path = os.path.join('logs', 'user_activity.log')
            if os.path.exists(log_path):
                total_operations += 1
                if await upload_logs_to_catbox(log_path):
                    success_count += 1
        
        print(f"✅ Scheduled backup completed: {success_count}/{total_operations} operations successful")
        return success_count == total_operations
        
    except Exception as e:
        print(f"❌ Error in scheduled backup: {e}")
        return False

async def emergency_data_upload() -> Dict[str, Optional[str]]:
    """Emergency upload of all critical data to multiple services."""
    try:
        results = {
            'gist': None,
            'pastebin_users': None,
            'pastebin_stats': None,
            'catbox_logs': None
        }
        
        # Format data
        user_data = format_user_data_for_upload()
        server_stats = format_server_stats_for_upload()
        
        # Upload to all available services
        tasks = []
        
        # Gist upload
        if config and hasattr(config, 'github_token'):
            tasks.append(('gist', update_gist()))
        
        # Pastebin uploads
        if config and hasattr(config, 'pastebin_api_key'):
            uploader = PastebinUploader()
            tasks.append(('pastebin_users', uploader.upload_paste(user_data, "Emergency User Data Backup")))
            tasks.append(('pastebin_stats', uploader.upload_paste(server_stats, "Emergency Server Stats Backup")))
        
        # Log upload
        log_path = os.path.join('logs', 'user_activity.log')
        if os.path.exists(log_path):
            tasks.append(('catbox_logs', upload_logs_to_catbox(log_path)))
        
        # Execute all uploads concurrently
        if tasks:
            completed_tasks = await asyncio.gather(
                *[task[1] for task in tasks],
                return_exceptions=True
            )
            
            for i, (name, _) in enumerate(tasks):
                result = completed_tasks[i]
                if not isinstance(result, Exception):
                    results[name] = result
        
        # Log results
        successful_uploads = sum(1 for v in results.values() if v is not None)
        total_uploads = len([k for k in results.keys() if results[k] is not None or k in [task[0] for task in tasks]])
        
        print(f"🚨 Emergency upload completed: {successful_uploads}/{total_uploads} successful")
        
        return results
        
    except Exception as e:
        print(f"❌ Error in emergency data upload: {e}")
        return {}

# UTILITY FUNCTIONS

def get_file_hash(file_path: str) -> Optional[str]:
    """Get SHA256 hash of a file."""
    try:
        with open(file_path, 'rb') as f:
            file_hash = hashlib.sha256()
            for chunk in iter(lambda: f.read(4096), b""):
                file_hash.update(chunk)
        return file_hash.hexdigest()
    except Exception as e:
        print(f"❌ Error getting file hash: {e}")
        return None

def compress_data(data: str) -> str:
    """Compress data using base64 encoding (placeholder for actual compression)."""
    try:
        # Simple base64 encoding - could be replaced with actual compression
        encoded_bytes = base64.b64encode(data.encode('utf-8'))
        return encoded_bytes.decode('utf-8')
    except Exception as e:
        print(f"❌ Error compressing data: {e}")
        return data

def decompress_data(compressed_data: str) -> str:
    """Decompress base64 encoded data."""
    try:
        decoded_bytes = base64.b64decode(compressed_data.encode('utf-8'))
        return decoded_bytes.decode('utf-8')
    except Exception as e:
        print(f"❌ Error decompressing data: {e}")
        return compressed_data

async def verify_upload_integrity(original_content: str, uploaded_url: str) -> bool:
    """Verify that uploaded content matches original."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(uploaded_url) as response:
                if response.status == 200:
                    downloaded_content = await response.text()
                    return original_content.strip() == downloaded_content.strip()
                return False
    except Exception as e:
        print(f"❌ Error verifying upload integrity: {e}")
        return False

def create_upload_metadata(files: Dict[str, str]) -> Dict[str, Any]:
    """Create metadata for uploads."""
    try:
        metadata = {
            'timestamp': datetime.datetime.now().isoformat(),
            'files': {},
            'total_size': 0,
            'file_count': len(files)
        }
        
        for filename, content in files.items():
            file_size = len(content.encode('utf-8'))
            metadata['files'][filename] = {
                'size': file_size,
                'lines': content.count('\n') + 1,
                'hash': hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
            }
            metadata['total_size'] += file_size
        
        return metadata
        
    except Exception as e:
        print(f"❌ Error creating upload metadata: {e}")
        return {}

# CONFIGURATION VALIDATION

def validate_upload_config() -> Dict[str, bool]:
    """Validate upload configuration."""
    validation_results = {
        'github_configured': False,
        'pastebin_configured': False,
        'webhook_configured': False,
        'upload_enabled': False
    }
    
    try:
        if config:
            # Check GitHub configuration
            if hasattr(config, 'github_token') and hasattr(config, 'gist_id'):
                validation_results['github_configured'] = bool(config.github_token and config.gist_id)
            
            # Check Pastebin configuration
            if hasattr(config, 'pastebin_api_key'):
                validation_results['pastebin_configured'] = bool(config.pastebin_api_key)
            
            # Check webhook configuration
            if hasattr(config, 'backup_webhook_url'):
                validation_results['webhook_configured'] = bool(config.backup_webhook_url)
            
            # Check if any upload is enabled
            validation_results['upload_enabled'] = (
                getattr(config, 'upload_gist', False) or
                getattr(config, 'backup_to_pastebin', False) or
                getattr(config, 'upload_logs', False)
            )
        
        return validation_results
        
    except Exception as e:
        print(f"❌ Error validating upload config: {e}")
        return validation_results

# LEGACY COMPATIBILITY FUNCTIONS

async def update_ids_file():
    """Legacy function for updating IDs file."""
    try:
        from core_utils import get_active_ids
        
        ids_content = await get_active_ids()
        
        # Write to local file
        with open('ids.txt', 'w', encoding='utf-8') as f:
            f.write(ids_content)
        
        print("✅ IDs file updated locally")
        
        # Upload to gist if configured
        if config and getattr(config, 'upload_gist', False):
            files = {'ids.txt': ids_content}
            async with GistUploader() as uploader:
                success = await uploader.update_gist(files)
                if success:
                    print("✅ IDs file uploaded to gist")
                else:
                    print("❌ Failed to upload IDs file to gist")
        
        return True
        
    except Exception as e:
        print(f"❌ Error updating IDs file: {e}")
        return False

# EXPORT ALL FUNCTIONS
__all__ = [
    # Classes
    'GistUploader', 'PastebinUploader', 'FileUploader',
    
    # Data formatting
    'format_user_data_for_upload', 'format_server_stats_for_upload', 'format_ids_for_upload',
    
    # Main upload functions
    'update_gist', 'backup_to_pastebin', 'upload_file_to_discord', 'upload_logs_to_catbox',
    
    # Scheduled operations
    'scheduled_backup', 'emergency_data_upload',
    
    # Utilities
    'get_file_hash', 'compress_data', 'decompress_data', 'verify_upload_integrity',
    'create_upload_metadata', 'validate_upload_config',
    
    # Legacy
    'update_ids_file'
]

print("✅ Enhanced upload_utils.py loaded successfully")