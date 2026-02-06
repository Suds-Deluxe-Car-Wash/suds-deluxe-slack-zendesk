"""Thread mapping storage for Slack thread to Zendesk ticket association."""
import os
import logging
from datetime import datetime, timedelta
from typing import Optional
import psycopg
from psycopg_pool import ConnectionPool

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
        
        # Create connection pool (min 1, max 50 connections)
        # Configured for Supabase Connection Pooler (Transaction mode)
        try:
            self.connection_pool = ConnectionPool(
                self.database_url,
                min_size=1,
                max_size=50,
                kwargs={
                    "autocommit": True,  # Required for Supabase transaction pooler
                    "prepare_threshold": None,  # Disable prepared statements
                    "options": "-c statement_timeout=30000"  # 30 second timeout
                },
                check=ConnectionPool.check_connection,  # Health check for connections
                max_idle=300,  # Close idle connections after 5 minutes
                max_lifetime=3600  # Recycle connections after 1 hour
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
        return self.connection_pool.connection()
    
    def _return_connection(self, conn):
        """Return a connection to the pool (handled by context manager in psycopg3)."""
        pass  # psycopg3 uses context managers, no manual return needed
    
    def _init_db(self):
        """Create database tables if they don't exist."""
        try:
            with self.connection_pool.connection() as conn:
                with conn.cursor() as cursor:
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
                    
                    logger.info("PostgreSQL tables initialized successfully")
                    
        except Exception as e:
            logger.error(f"Failed to initialize database tables: {e}")
            raise
    
    def store_mapping(self, thread_ts: str, ticket_id: int, channel_id: str) -> bool:
        """
        Store a mapping between Slack thread and Zendesk ticket ATOMICALLY.
        
        This will ONLY succeed if this is the first ticket for this thread.
        On conflict, it returns False (another request already claimed it).
        
        Args:
            thread_ts: Slack thread timestamp
            ticket_id: Zendesk ticket ID
            channel_id: Slack channel ID
            
        Returns:
            True if this is the first ticket for this thread (success),
            False if another ticket already exists for this thread (duplicate)
        """
        try:
            with self.connection_pool.connection() as conn:
                with conn.cursor() as cursor:
                    # Try to insert - if thread_ts exists, DO NOTHING and return nothing
                    cursor.execute("""
                        INSERT INTO thread_mappings 
                        (thread_ts, ticket_id, channel_id, created_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (thread_ts) DO NOTHING
                        RETURNING thread_ts
                    """, (thread_ts, ticket_id, channel_id, datetime.now()))
                    
                    # If we got a row back, we successfully claimed this thread
                    # If no row, another request already created a ticket
                    result = cursor.fetchone()
                    
                    if result is not None:
                        logger.info(f"Stored mapping: thread_ts={thread_ts} → ticket_id={ticket_id}")
                        return True
                    else:
                        logger.warning(f"Thread {thread_ts} already has a ticket, duplicate prevented at storage")
                        return False
            
        except Exception as e:
            logger.error(f"Failed to store mapping: {e}")
            return False
    
    def claim_thread(self, thread_ts: str, channel_id: str) -> bool:
        """
        Atomically claim a thread for ticket creation.
        
        This MUST be called BEFORE creating the Zendesk ticket to prevent race conditions.
        It inserts a placeholder (-1) to reserve the thread_ts.
        Also cleans up stale placeholders from failed previous attempts.
        
        Args:
            thread_ts: Slack thread timestamp to claim
            channel_id: Slack channel ID
            
        Returns:
            True if this request successfully claimed the thread (first),
            False if another request already claimed it (duplicate)
        """
        try:
            with self.connection_pool.connection() as conn:
                with conn.cursor() as cursor:
                    # First, clean up stale placeholders (older than 30 seconds)
                    # This allows quick retry after connection failures during cold starts
                    stale_threshold = datetime.now() - timedelta(seconds=30)
                    cursor.execute("""
                        DELETE FROM thread_mappings 
                        WHERE thread_ts = %s 
                        AND ticket_id = -1 
                        AND created_at < %s
                    """, (thread_ts, stale_threshold))
                    
                    if cursor.rowcount > 0:
                        logger.warning(f"Cleaned up stale placeholder for thread {thread_ts}")
                    
                    # Now try to insert placeholder with ticket_id=-1 to claim the thread
                    cursor.execute("""
                        INSERT INTO thread_mappings 
                        (thread_ts, ticket_id, channel_id, created_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (thread_ts) DO NOTHING
                        RETURNING thread_ts
                    """, (thread_ts, -1, channel_id, datetime.now()))
                    
                    result = cursor.fetchone()
                    
                    if result is not None:
                        logger.info(f"Claimed thread {thread_ts} for ticket creation")
                        return True
                    else:
                        logger.info(f"Thread {thread_ts} already claimed by another request")
                        return False
            
        except Exception as e:
            logger.error(f"Failed to claim thread {thread_ts}: {e}")
            return False
    
    def update_ticket_mapping(self, thread_ts: str, ticket_id: int) -> bool:
        """
        Update a claimed thread with the actual ticket ID.
        
        Called after successfully creating a Zendesk ticket to replace
        the placeholder (-1) with the real ticket ID.
        
        Args:
            thread_ts: Slack thread timestamp
            ticket_id: Zendesk ticket ID
            
        Returns:
            True if updated successfully
        """
        try:
            with self.connection_pool.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE thread_mappings 
                        SET ticket_id = %s
                        WHERE thread_ts = %s AND ticket_id = -1
                    """, (ticket_id, thread_ts))
                    
                    if cursor.rowcount > 0:
                        logger.info(f"Updated mapping: thread_ts={thread_ts} → ticket_id={ticket_id}")
                        return True
                    else:
                        logger.warning(f"Failed to update mapping for {thread_ts} - no placeholder found")
                        return False
            
        except Exception as e:
            logger.error(f"Failed to update ticket mapping: {e}")
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
            with self.connection_pool.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT ticket_id FROM thread_mappings 
                        WHERE thread_ts = %s
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
            with self.connection_pool.connection() as conn:
                with conn.cursor() as cursor:
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
    
    def is_event_processed(self, event_id: str) -> bool:
        """
        Check if an event has already been processed (deduplication).
        
        Args:
            event_id: Slack event ID
            
        Returns:
            True if event was already processed, False otherwise
        """
        try:
            with self.connection_pool.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT 1 FROM processed_events 
                        WHERE event_id = %s
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
            with self.connection_pool.connection() as conn:
                with conn.cursor() as cursor:
                    # PostgreSQL uses ON CONFLICT DO NOTHING for INSERT OR IGNORE
                    cursor.execute("""
                        INSERT INTO processed_events 
                        (event_id, processed_at)
                        VALUES (%s, %s)
                        ON CONFLICT (event_id) DO NOTHING
                    """, (event_id, datetime.now()))
                    
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
            
            with self.connection_pool.connection() as conn:
                with conn.cursor() as cursor:
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
            with self.connection_pool.connection() as conn:
                with conn.cursor() as cursor:
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
    
    def close(self):
        """Close all connections in the pool."""
        if hasattr(self, 'connection_pool') and self.connection_pool:
            self.connection_pool.close()
            logger.info("PostgreSQL connection pool closed")
