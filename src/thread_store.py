"""Thread mapping storage for Slack thread to Zendesk ticket association."""
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class ThreadMappingStore:
    """Manages persistent storage of Slack thread_ts to Zendesk ticket_id mappings."""
    
    def __init__(self, db_path: str = "data/thread_mappings.db"):
        """
        Initialize the thread mapping store with SQLite backend.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        
        # Ensure data directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize database
        self._init_db()
        
        logger.info(f"ThreadMappingStore initialized with database: {db_path}")
    
    def _init_db(self):
        """Create database tables if they don't exist."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Thread mappings table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS thread_mappings (
                    thread_ts TEXT PRIMARY KEY,
                    ticket_id INTEGER NOT NULL,
                    channel_id TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Event deduplication table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_events (
                    event_id TEXT PRIMARY KEY,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create index for faster lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_created_at 
                ON thread_mappings(created_at)
            """)
            
            conn.commit()
            logger.info("Database tables initialized")
    
    def store_mapping(self, thread_ts: str, ticket_id: int, channel_id: str) -> bool:
        """
        Store a mapping between Slack thread and Zendesk ticket.
        
        Args:
            thread_ts: Slack thread timestamp
            ticket_id: Zendesk ticket ID
            channel_id: Slack channel ID
            
        Returns:
            True if stored successfully, False otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO thread_mappings 
                    (thread_ts, ticket_id, channel_id, created_at)
                    VALUES (?, ?, ?, ?)
                """, (thread_ts, ticket_id, channel_id, datetime.now()))
                conn.commit()
                
            logger.info(f"Stored mapping: thread_ts={thread_ts} → ticket_id={ticket_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store mapping: {e}")
            return False
    
    def get_ticket_id(self, thread_ts: str) -> Optional[int]:
        """
        Retrieve Zendesk ticket ID for a given Slack thread.
        
        Args:
            thread_ts: Slack thread timestamp
            
        Returns:
            Zendesk ticket ID if found, None otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT ticket_id FROM thread_mappings 
                    WHERE thread_ts = ?
                """, (thread_ts,))
                
                result = cursor.fetchone()
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Failed to get ticket ID for thread {thread_ts}: {e}")
            return None
    
    def get_thread_info(self, ticket_id: int) -> Optional[dict]:
        """
        Retrieve Slack thread info for a given Zendesk ticket (reverse lookup).
        
        Args:
            ticket_id: Zendesk ticket ID
            
        Returns:
            Dictionary with thread_ts and channel_id if found, None otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT thread_ts, channel_id FROM thread_mappings 
                    WHERE ticket_id = ?
                """, (ticket_id,))
                
                result = cursor.fetchone()
                if result:
                    return {
                        "thread_ts": result[0],
                        "channel_id": result[1]
                    }
                return None
                
        except Exception as e:
            logger.error(f"Failed to get thread info for ticket {ticket_id}: {e}")
            return None
    
    def is_event_processed(self, event_id: str) -> bool:
        """
        Check if an event has already been processed (deduplication).
        
        Args:
            event_id: Slack event ID
            
        Returns:
            True if event was already processed, False otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 1 FROM processed_events 
                    WHERE event_id = ?
                """, (event_id,))
                
                return cursor.fetchone() is not None
                
        except Exception as e:
            logger.error(f"Failed to check event {event_id}: {e}")
            return False
    
    def mark_event_processed(self, event_id: str) -> bool:
        """
        Mark an event as processed.
        
        Args:
            event_id: Slack event ID
            
        Returns:
            True if marked successfully, False otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR IGNORE INTO processed_events 
                    (event_id, processed_at)
                    VALUES (?, ?)
                """, (event_id, datetime.now()))
                conn.commit()
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark event {event_id} as processed: {e}")
            return False
    
    def cleanup_old_mappings(self, days: int = 30) -> int:
        """
        Delete mappings older than specified days.
        
        Args:
            days: Number of days to keep (default: 30)
            
        Returns:
            Number of mappings deleted
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Delete old thread mappings
                cursor.execute("""
                    DELETE FROM thread_mappings 
                    WHERE created_at < ?
                """, (cutoff_date,))
                
                deleted_mappings = cursor.rowcount
                
                # Also cleanup old processed events
                cursor.execute("""
                    DELETE FROM processed_events 
                    WHERE processed_at < ?
                """, (cutoff_date,))
                
                deleted_events = cursor.rowcount
                
                conn.commit()
                
            logger.info(f"Cleanup: Deleted {deleted_mappings} thread mappings and {deleted_events} processed events older than {days} days")
            return deleted_mappings
            
        except Exception as e:
            logger.error(f"Failed to cleanup old mappings: {e}")
            return 0
    
    def get_stats(self) -> dict:
        """
        Get statistics about stored mappings.
        
        Returns:
            Dictionary with mapping counts and other stats
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("SELECT COUNT(*) FROM thread_mappings")
                total_mappings = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM processed_events")
                total_events = cursor.fetchone()[0]
                
                return {
                    "total_mappings": total_mappings,
                    "total_processed_events": total_events
                }
                
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {"total_mappings": 0, "total_processed_events": 0}
