import xml.etree.ElementTree as ET
from datetime import datetime
import logging

def migrate_xml_to_sqlite(db_manager):
    """
    Migrate existing XML data to the new SQLite database
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Migrate UserData.xml
        try:
            tree = ET.parse('UserData.xml')
            root = tree.getroot()
            
            migrated_users = 0
            for user_elem in root.findall('user'):
                try:
                    discord_id = int(user_elem.get('id', 0))
                    player_id = user_elem.get('playerId')
                    display_name = user_elem.get('displayName', 'Unknown')
                    prefix = user_elem.get('prefix')
                    status = user_elem.get('status', 'inactive')
                    
                    # Add user to new database
                    db_manager.add_user(
                        discord_id=discord_id,
                        player_id=player_id,
                        display_name=display_name,
                        prefix=prefix
                    )
                    
                    # Update status
                    db_manager.update_user_status(discord_id, status)
                    migrated_users += 1
                    
                except Exception as e:
                    logger.error(f"Error migrating user {user_elem.get('id')}: {e}")
            
            logger.info(f"✅ Migrated {migrated_users} users from UserData.xml")
            
        except FileNotFoundError:
            logger.info("⚠️ UserData.xml not found - skipping user migration")
        except Exception as e:
            logger.error(f"Error reading UserData.xml: {e}")
    
        # Migrate ServerData.xml
        try:
            tree = ET.parse('ServerData.xml')
            root = tree.getroot()
            
            migrated_gps = 0
            
            # Look for god packs in various possible structures
            gp_elements = []
            gp_elements.extend(root.findall('.//godpack'))
            gp_elements.extend(root.findall('.//gp'))
            gp_elements.extend(root.findall('.//pack'))
            
            for gp_elem in gp_elements:
                try:
                    from database_manager import GPState
                    
                    message_id = int(gp_elem.get('messageId', 0))
                    if message_id == 0:
                        continue
                    
                    # Parse timestamp
                    timestamp_str = gp_elem.get('timestamp')
                    if timestamp_str:
                        try:
                            timestamp = datetime.fromisoformat(timestamp_str)
                        except:
                            timestamp = datetime.now()
                    else:
                        timestamp = datetime.now()
                    
                    pack_number = int(gp_elem.get('packNumber', 1))
                    name = gp_elem.get('name', 'Unknown')
                    friend_code = gp_elem.get('friendCode', 'Unknown')
                    
                    # Parse state
                    state_str = gp_elem.get('state', 'TESTING').upper()
                    try:
                        state = GPState(state_str)
                    except:
                        state = GPState.TESTING
                    
                    screenshot_url = gp_elem.get('screenshotUrl', '')
                    ratio = int(gp_elem.get('ratio', -1))
                    
                    # Add god pack to new database
                    gp_id = db_manager.add_godpack(
                        message_id=message_id,
                        timestamp=timestamp,
                        pack_number=pack_number,
                        name=name,
                        friend_code=friend_code,
                        state=state,
                        screenshot_url=screenshot_url,
                        ratio=ratio
                    )
                    
                    if gp_id:
                        migrated_gps += 1
                    
                except Exception as e:
                    logger.error(f"Error migrating god pack {gp_elem.get('messageId')}: {e}")
            
            logger.info(f"✅ Migrated {migrated_gps} god packs from ServerData.xml")
            
        except FileNotFoundError:
            logger.info("⚠️ ServerData.xml not found - skipping god pack migration")
        except Exception as e:
            logger.error(f"Error reading ServerData.xml: {e}")
    
        logger.info("🎉 Migration completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        return False