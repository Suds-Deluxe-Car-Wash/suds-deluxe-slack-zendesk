"""Thread mapping storage for Slack thread to Zendesk ticket association."""
import os
import logging
from datetime import datetime, timedelta
from typing import Optional
import psycopg2
from psycopg2 import pool

logger = logging.getLogger(__name__)


class ThreadMappingStore:
    """Manages persistent storage of Slack thread_ts to Zendesk ticket_id mappings."""
    
    def __init__(self):
        """
        Initialize the thread mapping store with PostgreSQL backend.
        
        Reads DATABASE_URL from environment variables.
        """
        self.database_url = os.getenv("DATABASE_URL")
        
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is required")
        
        # Create connection pool (min 1, max 10 connections)
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, 10,
                self.database_url
            )
            logger.info("PostgreSQL connection pool created successfully")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
        
        # Initialize database tables
        self._init_db()
        
        logger.info("ThreadMappingStore initialized with PostgreSQL")
    
    def _get_connection(self):
        """Get a connection from the pool."""
        return self.connection_pool.getconn()
    
    def _return_connection(self, conn):
        """Return a connection to the pool."""
        self.connection_pool.putconn(conn)
    
    def _init_db(self):
        """Create database tables if they don't exist."""
        conn = None
        try:
            conn = self._get_connection()
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
            logger.info("PostgreSQL tables initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database tables: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                cursor.close()
                self._return_connection(conn)
    
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
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # PostgreSQL uses ON CONFLICT for upsert
            cursor.execute("""
                INSERT INTO thread_mappings 
                (thread_ts, ticket_id, channel_id, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (thread_ts) 
                DO UPDATE SET 
                    ticket_id = EXCLUDED.ticket_id,
                    channel_id = EXCLUDED.channel_id,
                    created_at = EXCLUDED.created_at
            """, (thread_ts, ticket_id, channel_id, datetime.now()))
            
            conn.commit()
            logger.info(f"Stored mapping: thread_ts={thread_ts} → ticket_id={ticket_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store mapping: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                cursor.close()
                self._return_connection(conn)
    
    def get_ticket_id(self, thread_ts: str) -> Optional[int]:
        """
        Retrieve Zendesk ticket ID for a given Slack thread.
        
        Args:
            thread_ts: Slack thread timestamp
            
        Returns:
            Zendesk ticket ID if found, None otherwise
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT ticket_id FROM thread_mappings 
                WHERE thread_ts = %s
            """, (thread_ts,))
            
            result = cursor.fetchone()
            return result[0] if result else None
            
        except Exception as e:
            logger.error(f"Failed to get ticket ID for thread {thread_ts}: {e}")
            return None
        finally:
            if conn:
                cursor.close()
                self._return_connection(conn)
    
    def get_thread_info(self, ticket_id: int) -> Optional[dict]:
        """
        Retrieve Slack thread info for a given Zendesk ticket (reverse lookup).
        
        Args:
            ticket_id: Zendesk ticket ID
            
        Returns:
            Dictionary with thread_ts and channel_id if found, None otherwise
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT thread_ts, channel_id FROM thread_mappings 
                WHERE ticket_id = %s
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
        finally:
            if conn:
                cursor.close()
                self._return_connection(conn)
    
    def is_event_processed(self, event_id: str) -> bool:
        """
        Check if an event has already been processed (deduplication).
        
        Args:
            event_id: Slack event ID
            
        Returns:
            True if event was already processed, False otherwise
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 1 FROM processed_events 
                WHERE event_id = %s
            """, (event_id,))
            
            return cursor.fetchone() is not None
            
        except Exception as e:
            logger.error(f"Failed to check event {event_id}: {e}")
            return False
        finally:
            if conn:
                cursor.close()
                self._return_connection(conn)
    
    def mark_event_processed(self, event_id: str) -> bool:
        """
        Mark an event as processed.
        
        Args:
            event_id: Slack event ID
            
        Returns:
            True if marked successfully, False otherwise
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # PostgreSQL uses ON CONFLICT DO NOTHING for INSERT OR IGNORE
            cursor.execute("""
                INSERT INTO processed_events 
                (event_id, processed_at)
                VALUES (%s, %s)
                ON CONFLICT (event_id) DO NOTHING
            """, (event_id, datetime.now()))
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark event {event_id} as processed: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                cursor.close()
                self._return_connection(conn)
    
    def cleanup_old_mappings(self, days: int = 30) -> int:
        """
        Delete mappings older than specified days.
        
        Args:
            days: Number of days to keep (default: 30)
            
        Returns:
            Number of mappings deleted
        """
        conn = None
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Delete old thread mappings
            cursor.execute("""
                DELETE FROM thread_mappings 
                WHERE created_at < %s
            """, (cutoff_date,))
            
            deleted_mappings = cursor.rowcount
            
            # Also cleanup old processed events
            cursor.execute("""
                DELETE FROM processed_events 
                WHERE processed_at < %s
            """, (cutoff_date,))
            
            deleted_events = cursor.rowcount
            
            conn.commit()
            
            logger.info(f"Cleanup: Deleted {deleted_mappings} thread mappings and {deleted_events} processed events older than {days} days")
            return deleted_mappings
            
        except Exception as e:
            logger.error(f"Failed to cleanup old mappings: {e}")
            if conn:
                conn.rollback()
            return 0
        finally:
            if conn:
                cursor.close()
                self._return_connection(conn)
    
    def get_stats(self) -> dict:
        """
        Get statistics about stored mappings.
        
        Returns:
            Dictionary with mapping counts and other stats
        """
        conn = None
        try:
            conn = self._get_connection()
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
        finally:
            if conn:
                cursor.close()
                self._return_connection(conn)
    
    def close(self):
        """Close all connections in the pool."""
        if hasattr(self, 'connection_pool') and self.connection_pool:
            self.connection_pool.closeall()
            logger.info("PostgreSQL connection pool closed")
