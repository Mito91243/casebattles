import asyncio
import json
from datetime import datetime
from loguru import logger
from .database import DatabaseManager

class DBWriter:
    def __init__(self, db_manager: DatabaseManager, backup_file: str = "data/failed_writes.jsonl"):
        self.db = db_manager
        self.backup_file = backup_file
        self.retry_delay = 2
        self.max_retries = 2
        
    async def process_queue(self, queue: asyncio.Queue):
        logger.info("🚀 DB Writer Worker started...")
        
        while True:
            try:
                players_data = await queue.get()
                await self._write_game_players(players_data)
                queue.task_done()
                
            except Exception as e:
                logger.error(f"❌ Queue processing error: {e}")
                await asyncio.sleep(1)
    
    async def _write_game_players(self, players_data: list):        
        for player in players_data:
            success = await self._write_player_with_retry(player)
            
            if not success:
                # Backup to file if all retries failed
                await self._backup_to_file(player)
    
    async def _write_player_with_retry(self, player: dict) -> bool:        
        for attempt in range(1, self.max_retries + 1):
            try:
                await self._write_player(player)
                return True
                
            except Exception as e:
                if attempt < self.max_retries:
                    wait_time = self.retry_delay * (2 ** (attempt - 1))
                    logger.warning(f"⚠️ DB write failed (attempt {attempt}/{self.max_retries}): {e}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"❌ DB write failed after {self.max_retries} attempts: {e}")
                    return False
        
        return False
    
    async def _write_player(self, player: dict):        
        async with self.db.get_connection() as conn:
            async with conn.cursor() as cursor:
                try:
                    await conn.begin()
                    
                    # IMPORTANT: Using positional parameters (%s) for aiomysql
                    # Not named parameters like %(username)s
                    upsert_user_sql = """
                        INSERT INTO user (
                            username,
                            external_id,
                            profile_url,
                            level,
                            avatar_url,
                            avatar_hash,
                            website,
                            created_at,
                            updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
                        )
                        ON DUPLICATE KEY UPDATE
                            username = VALUES(username),
                            profile_url = VALUES(profile_url),
                            level = VALUES(level),
                            avatar_url = VALUES(avatar_url),
                            updated_at = NOW()
                    """
                    
                    await cursor.execute(upsert_user_sql, (
                        player['username'],
                        player['external_id'],
                        player['profile_url'],
                        player['level'],
                        player['avatar_url'],
                        player['avatar_hash'],
                        player['website']
                    ))
                    
                    # Get the user's internal ID
                    await cursor.execute(
                        "SELECT id FROM user WHERE external_id = %s AND website = %s",
                        (player['external_id'], player['website'])
                    )
                    result = await cursor.fetchone()
                    
                    if not result:
                        raise Exception(f"Failed to retrieve user_id for external_id={player['external_id']}")
                    
                    user_id = result[0]
                    
                    # --- STEP 2: UPDATE DAILY WAGER ---
                    # Parse date from game timestamp or use today
                    if player.get('date'):
                        try:
                            # Parse ISO timestamp from game (e.g., "2025-11-15T18:57:54")
                            wager_date = datetime.fromisoformat(player['date'].replace('Z', '+00:00')).date()
                        except:
                            wager_date = datetime.now().date()
                    else:
                        wager_date = datetime.now().date()
                    
                    upsert_wager_sql = """
                        INSERT INTO user_daily_wager (
                            user_id,
                            date,
                            total_wager,
                            created_at,
                            updated_at
                        ) VALUES (
                            %s, %s, %s, NOW(), NOW()
                        )
                        ON DUPLICATE KEY UPDATE
                            total_wager = total_wager + VALUES(total_wager),
                            updated_at = NOW()
                    """
                    
                    await cursor.execute(upsert_wager_sql, (
                        user_id,
                        wager_date,
                        player['total_bet']
                    ))
                    
                    # Commit transaction
                    await conn.commit()
                    
                    logger.success(f"✅ DB Write Success: User={player['username']} (ID={user_id}), Bet=${player['total_bet']}, Date={wager_date}")
                    
                except Exception as e:
                    # Rollback on error
                    await conn.rollback()
                    raise e
    
    async def _backup_to_file(self, player: dict):
        """Backup failed writes to JSONL file"""
        try:
            import os
            os.makedirs(os.path.dirname(self.backup_file), exist_ok=True)
            
            with open(self.backup_file, 'a') as f:
                backup_entry = {
                    'timestamp': datetime.now().isoformat(),
                    'player': player
                }
                f.write(json.dumps(backup_entry) + '\n')
            
            logger.warning(f"💾 Backed up to file: {self.backup_file}")
            
        except Exception as e:
            logger.error(f"❌ Failed to backup to file: {e}")