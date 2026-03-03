"""Thread mapping storage for Slack thread to Zendesk ticket association."""
import json
import os
import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Literal
from psycopg_pool import ConnectionPool
from psycopg_pool import PoolTimeout

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClaimThreadResult:
    status: Literal["claimed", "duplicate", "db_error"]
    error: Optional[str] = None


@dataclass(frozen=True)
class TicketLookupResult:
    status: Literal["found", "not_found", "placeholder", "db_error"]
    ticket_id: Optional[int] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class EventProcessedResult:
    status: Literal["processed", "not_processed", "db_error"]
    error: Optional[str] = None


@dataclass(frozen=True)
class SlackEventStateResult:
    status: Literal["received", "completed", "failed", "not_found", "db_error"]
    failed_reason: Optional[str] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class DurableJobEnqueueResult:
    status: Literal["created", "duplicate", "db_error"]
    existing_status: Optional[str] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class DurableJobClaimResult:
    status: Literal["claimed", "not_found", "db_error"]
    job_id: Optional[str] = None
    job_type: Optional[str] = None
    payload: Optional[dict] = None
    attempts: int = 0
    error: Optional[str] = None


class ThreadMappingStore:
    """Manages persistent storage of Slack thread_ts to Zendesk ticket_id mappings."""
    _shared_pool = None
    _pool_lock = threading.Lock()
    _db_initialized = False

    @staticmethod
    def _get_int_env(var_name: str, default: int, min_value: int = 1) -> int:
        """Read and validate integer env vars used for DB pool tuning."""
        raw = os.getenv(var_name)
        if raw is None:
            return default
        try:
            value = int(raw)
            if value < min_value:
                raise ValueError(f"must be >= {min_value}")
            return value
        except ValueError:
            logger.warning("Invalid %s=%r; using default %s", var_name, raw, default)
            return default

    @staticmethod
    def _get_float_env(var_name: str, default: float, min_value: float = 0.1) -> float:
        """Read and validate float env vars used for DB pool tuning."""
        raw = os.getenv(var_name)
        if raw is None:
            return default
        try:
            value = float(raw)
            if value < min_value:
                raise ValueError(f"must be >= {min_value}")
            return value
        except ValueError:
            logger.warning("Invalid %s=%r; using default %s", var_name, raw, default)
            return default
    
    def __init__(self):
        """
        Initialize the thread mapping store with PostgreSQL backend.
        
        Reads DATABASE_URL from environment variables.
        """
        self.database_url = os.getenv("DATABASE_URL")
        
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is required")

        # Render-friendly defaults; override via env when needed.
        self.pool_min_size = self._get_int_env("DB_POOL_MIN_SIZE", 1, min_value=1)
        self.pool_max_size = self._get_int_env("DB_POOL_MAX_SIZE", 2, min_value=self.pool_min_size)
        self.pool_acquire_timeout = self._get_float_env("DB_POOL_ACQUIRE_TIMEOUT", 5.0, min_value=0.1)
        self.db_connect_timeout = self._get_int_env("DB_CONNECT_TIMEOUT", 5, min_value=1)
        self.db_statement_timeout_ms = self._get_int_env("DB_STATEMENT_TIMEOUT_MS", 15000, min_value=1000)
        self.db_pool_max_idle = self._get_int_env("DB_POOL_MAX_IDLE_SECONDS", 120, min_value=1)
        self.db_pool_max_lifetime = self._get_int_env("DB_POOL_MAX_LIFETIME_SECONDS", 900, min_value=30)
        
        # Create one shared pool per process to avoid unnecessary connection growth.
        with ThreadMappingStore._pool_lock:
            if ThreadMappingStore._shared_pool is None:
                # Configured for hosted PostgreSQL with conservative defaults.
                try:
                    ThreadMappingStore._shared_pool = ConnectionPool(
                        self.database_url,
                        min_size=self.pool_min_size,
                        max_size=self.pool_max_size,
                        timeout=self.pool_acquire_timeout,
                        kwargs={
                            "autocommit": True,
                            "prepare_threshold": None,  # Disable prepared statements
                            "connect_timeout": self.db_connect_timeout,
                            "options": f"-c statement_timeout={self.db_statement_timeout_ms}"
                        },
                        check=ConnectionPool.check_connection,  # Health check for connections
                        max_idle=self.db_pool_max_idle,
                        max_lifetime=self.db_pool_max_lifetime
                    )
                    logger.info(
                        "PostgreSQL connection pool created: min=%s max=%s acquire_timeout=%.2fs "
                        "connect_timeout=%ss statement_timeout_ms=%s max_idle=%ss max_lifetime=%ss",
                        self.pool_min_size,
                        self.pool_max_size,
                        self.pool_acquire_timeout,
                        self.db_connect_timeout,
                        self.db_statement_timeout_ms,
                        self.db_pool_max_idle,
                        self.db_pool_max_lifetime
                    )
                except Exception as e:
                    logger.error(f"Failed to create connection pool: {e}")
                    raise
            
            self.connection_pool = ThreadMappingStore._shared_pool
            
            # Initialize database tables once per process.
            if not ThreadMappingStore._db_initialized:
                self._init_db()
                ThreadMappingStore._db_initialized = True
        
        logger.info("ThreadMappingStore initialized with PostgreSQL")
    
    @contextmanager
    def _get_connection(self):
        """Get a connection from the pool with timeout diagnostics."""
        try:
            with self.connection_pool.connection() as conn:
                yield conn
        except PoolTimeout as e:
            logger.error(
                "DB pool acquisition timeout after %.2fs while acquiring connection. pool_stats=%s error=%s",
                self.pool_acquire_timeout,
                self.get_pool_stats(),
                e
            )
            raise

    def _return_connection(self, conn):
        """Return a connection to the pool (handled by context manager in psycopg3)."""
        pass  # psycopg3 uses context managers, no manual return needed

    def get_pool_stats(self) -> dict:
        """Get connection pool stats for diagnostics."""
        try:
            return self.connection_pool.get_stats()
        except Exception as e:
            return {"stats_error": str(e)}
    
    def _init_db(self):
        """Create database tables if they don't exist."""
        try:
            with self._get_connection() as conn:
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

                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS slack_event_states (
                            event_id TEXT PRIMARY KEY,
                            status TEXT NOT NULL,
                            failed_reason TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)

                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS durable_jobs (
                            job_id TEXT PRIMARY KEY,
                            job_type TEXT NOT NULL,
                            status TEXT NOT NULL,
                            payload TEXT NOT NULL,
                            attempts INTEGER NOT NULL DEFAULT 0,
                            last_error TEXT,
                            processing_started_at TIMESTAMP,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # Create index for faster lookups
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_created_at 
                        ON thread_mappings(created_at)
                    """)

                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_durable_jobs_status_created_at
                        ON durable_jobs(status, created_at)
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
            channel_id: Slack channel ID used if the final mapping must be inserted
            channel_id: Slack channel ID
            
        Returns:
            True if this is the first ticket for this thread (success),
            False if another ticket already exists for this thread (duplicate)
        """
        try:
            with self._get_connection() as conn:
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
    
    def claim_thread(self, thread_ts: str, channel_id: str) -> ClaimThreadResult:
        """
        Atomically claim a thread for ticket creation.
        
        This MUST be called BEFORE creating the Zendesk ticket to prevent race conditions.
        It inserts a placeholder (-1) to reserve the thread_ts.
        Also cleans up stale placeholders from failed previous attempts.
        
        Args:
            thread_ts: Slack thread timestamp to claim
            channel_id: Slack channel ID
            
        Returns:
            Claim result with explicit success, duplicate, or db_error state.
        """
        try:
            with self._get_connection() as conn:
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
                        return ClaimThreadResult(status="claimed")

                    logger.info(f"Thread {thread_ts} already claimed by another request")
                    return ClaimThreadResult(status="duplicate")
            
        except Exception as e:
            logger.error(f"Failed to claim thread {thread_ts}: {e}")
            return ClaimThreadResult(status="db_error", error=str(e))
    
    def update_ticket_mapping(self, thread_ts: str, ticket_id: int, channel_id: str = "") -> bool:
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
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE thread_mappings 
                        SET ticket_id = %s
                        WHERE thread_ts = %s AND ticket_id = -1
                    """, (ticket_id, thread_ts))
                    
                    if cursor.rowcount > 0:
                        logger.info(f"Updated mapping: thread_ts={thread_ts} → ticket_id={ticket_id}")
                        return True
                    cursor.execute("""
                        SELECT ticket_id
                        FROM thread_mappings
                        WHERE thread_ts = %s
                    """, (thread_ts,))
                    existing = cursor.fetchone()
                    if existing:
                        existing_ticket_id = existing[0]
                        if existing_ticket_id == ticket_id:
                            logger.info(
                                "Mapping already finalized for thread_ts=%s ticket_id=%s",
                                thread_ts,
                                ticket_id,
                            )
                            return True
                        logger.warning(
                            "Failed to finalize mapping for thread_ts=%s because existing ticket_id=%s differs",
                            thread_ts,
                            existing_ticket_id,
                        )
                        return False

                    logger.warning(
                        "No placeholder found for thread_ts=%s while finalizing ticket_id=%s; inserting final mapping",
                        thread_ts,
                        ticket_id,
                    )
                    cursor.execute("""
                        INSERT INTO thread_mappings
                        (thread_ts, ticket_id, channel_id, created_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (thread_ts) DO NOTHING
                    """, (thread_ts, ticket_id, channel_id, datetime.now()))
                    return True
            
        except Exception as e:
            logger.error(f"Failed to update ticket mapping: {e}")
            return False
    
    def get_ticket_id(self, thread_ts: str) -> TicketLookupResult:
        """
        Retrieve Zendesk ticket ID for a given Slack thread.
        
        Called frequently when converting Slack thread activity into Zendesk
        comments.  During the ticket-creation race we insert a placeholder value
        of ``-1``; this method treats that sentinel value as “not found” and
        returns ``None`` so callers don't attempt to post to a non-existent
        ticket.
        
        Args:
            thread_ts: Slack thread timestamp
            
        Returns:
            Lookup result with explicit found, not_found, placeholder, or
            db_error state.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT ticket_id FROM thread_mappings 
                        WHERE thread_ts = %s
                    """, (thread_ts,))
                    
                    result = cursor.fetchone()
                    if not result:
                        return TicketLookupResult(status="not_found")
                    ticket_id = result[0]
                    if ticket_id == -1:
                        logger.debug(
                            "get_ticket_id(%s) found placeholder (-1)",
                            thread_ts,
                        )
                        return TicketLookupResult(status="placeholder")
                    return TicketLookupResult(status="found", ticket_id=ticket_id)
                    
        except PoolTimeout as e:
            logger.error(
                "Failed to get ticket ID for thread %s: pool timeout after %.2fs. pool_stats=%s error=%s",
                thread_ts,
                self.pool_acquire_timeout,
                self.get_pool_stats(),
                e
            )
            return TicketLookupResult(status="db_error", error=str(e))
        except Exception as e:
            logger.error(f"Failed to get ticket ID for thread {thread_ts}: {e}")
            return TicketLookupResult(status="db_error", error=str(e))
    
    def get_thread_info(self, ticket_id: int) -> Optional[dict]:
        """
        Retrieve Slack thread info for a given Zendesk ticket (reverse lookup).
        
        Args:
            ticket_id: Zendesk ticket ID
            
        Returns:
            Dictionary with thread_ts and channel_id if found, None otherwise
        """
        try:
            with self._get_connection() as conn:
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
    
    def is_event_processed(self, event_id: str) -> EventProcessedResult:
        """
        Check if an event has already been processed (deduplication).
        
        Args:
            event_id: Slack event ID
            
        Returns:
            Explicit processed state.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT 1 FROM processed_events 
                        WHERE event_id = %s
                    """, (event_id,))
                    
                    if cursor.fetchone() is not None:
                        return EventProcessedResult(status="processed")
                    return EventProcessedResult(status="not_processed")
                    
        except Exception as e:
            logger.error(f"Failed to check event {event_id}: {e}")
            return EventProcessedResult(status="db_error", error=str(e))
    
    def mark_event_processed(self, event_id: str) -> bool:
        """
        Mark an event as processed.
        
        Args:
            event_id: Slack event ID
            
        Returns:
            True if marked successfully, False otherwise
        """
        try:
            with self._get_connection() as conn:
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

    def get_slack_event_state(self, event_id: str) -> SlackEventStateResult:
        """Fetch lifecycle state for a Slack event envelope."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT status, failed_reason
                        FROM slack_event_states
                        WHERE event_id = %s
                    """, (event_id,))
                    result = cursor.fetchone()
                    if not result:
                        return SlackEventStateResult(status="not_found")
                    return SlackEventStateResult(
                        status=result[0],
                        failed_reason=result[1],
                    )
        except Exception as e:
            logger.error("Failed to get Slack event state for %s: %s", event_id, e)
            return SlackEventStateResult(status="db_error", error=str(e))

    def record_slack_event_received(self, event_id: str) -> SlackEventStateResult:
        """Persist initial receipt of a Slack event if it has not been seen before."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO slack_event_states (event_id, status, failed_reason, created_at, updated_at)
                        VALUES (%s, %s, NULL, %s, %s)
                        ON CONFLICT (event_id) DO NOTHING
                    """, (event_id, "received", datetime.now(), datetime.now()))
            return self.get_slack_event_state(event_id)
        except Exception as e:
            logger.error("Failed to record Slack event receipt for %s: %s", event_id, e)
            return SlackEventStateResult(status="db_error", error=str(e))

    def mark_slack_event_completed(self, event_id: str) -> bool:
        """Mark a Slack event as completed after successful worker processing."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE slack_event_states
                        SET status = %s, failed_reason = NULL, updated_at = %s
                        WHERE event_id = %s
                    """, ("completed", datetime.now(), event_id))
            return True
        except Exception as e:
            logger.error("Failed to mark Slack event completed for %s: %s", event_id, e)
            return False

    def mark_slack_event_failed(self, event_id: str, failed_reason: str) -> bool:
        """Mark a Slack event as failed while preserving the original receipt row."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE slack_event_states
                        SET status = %s, failed_reason = %s, updated_at = %s
                        WHERE event_id = %s
                    """, ("failed", failed_reason[:1000], datetime.now(), event_id))
            return True
        except Exception as e:
            logger.error("Failed to mark Slack event failed for %s: %s", event_id, e)
            return False

    @staticmethod
    def _decode_job_payload(raw_payload: str) -> dict:
        """Decode stored durable job payload JSON."""
        return json.loads(raw_payload) if raw_payload else {}

    def enqueue_durable_job(self, job_id: str, job_type: str, payload: dict) -> DurableJobEnqueueResult:
        """Persist a durable background job before it is signaled in memory."""
        try:
            serialized_payload = json.dumps(payload)
            now = datetime.now()
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO durable_jobs
                        (job_id, job_type, status, payload, attempts, last_error, processing_started_at, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, 0, NULL, NULL, %s, %s)
                        ON CONFLICT (job_id) DO NOTHING
                    """, (job_id, job_type, "pending", serialized_payload, now, now))
                    if cursor.rowcount > 0:
                        return DurableJobEnqueueResult(status="created")

                    cursor.execute("""
                        SELECT status
                        FROM durable_jobs
                        WHERE job_id = %s
                    """, (job_id,))
                    existing = cursor.fetchone()
                    return DurableJobEnqueueResult(
                        status="duplicate",
                        existing_status=existing[0] if existing else None,
                    )
        except Exception as e:
            logger.error("Failed to enqueue durable job %s: %s", job_id, e)
            return DurableJobEnqueueResult(status="db_error", error=str(e))

    def enqueue_slack_event_job(self, event_id: str, payload: dict) -> DurableJobEnqueueResult:
        """Atomically persist Slack event receipt and its durable job."""
        job_id = f"slack_event:{event_id}"
        try:
            serialized_payload = json.dumps(payload)
            now = datetime.now()
            with self._get_connection() as conn:
                with conn.transaction():
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO slack_event_states
                            (event_id, status, failed_reason, created_at, updated_at)
                            VALUES (%s, %s, NULL, %s, %s)
                            ON CONFLICT (event_id) DO NOTHING
                            RETURNING event_id
                        """, (event_id, "received", now, now))
                        inserted = cursor.fetchone()
                        if inserted is None:
                            cursor.execute("""
                                SELECT status
                                FROM slack_event_states
                                WHERE event_id = %s
                            """, (event_id,))
                            existing = cursor.fetchone()
                            return DurableJobEnqueueResult(
                                status="duplicate",
                                existing_status=existing[0] if existing else None,
                            )

                        cursor.execute("""
                            INSERT INTO durable_jobs
                            (job_id, job_type, status, payload, attempts, last_error, processing_started_at, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, 0, NULL, NULL, %s, %s)
                        """, (job_id, "slack_message_event", "pending", serialized_payload, now, now))
                        return DurableJobEnqueueResult(status="created")
        except Exception as e:
            logger.error("Failed to enqueue Slack event job %s: %s", event_id, e)
            return DurableJobEnqueueResult(status="db_error", error=str(e))

    def _build_job_claim_result(self, row) -> DurableJobClaimResult:
        """Convert a durable job row into a typed claim result."""
        if not row:
            return DurableJobClaimResult(status="not_found")
        try:
            return DurableJobClaimResult(
                status="claimed",
                job_id=row[0],
                job_type=row[1],
                payload=self._decode_job_payload(row[2]),
                attempts=row[3] or 0,
            )
        except Exception as e:
            logger.error("Failed to decode durable job payload for %s: %s", row[0], e)
            return DurableJobClaimResult(status="db_error", error=str(e))

    def claim_durable_job(self, job_id: str, stale_after_seconds: int = 120) -> DurableJobClaimResult:
        """Claim a specific durable job if it is pending or stale-processing."""
        stale_before = datetime.now() - timedelta(seconds=stale_after_seconds)
        now = datetime.now()
        try:
            with self._get_connection() as conn:
                with conn.transaction():
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            UPDATE durable_jobs
                            SET status = %s,
                                processing_started_at = %s,
                                updated_at = %s,
                                attempts = attempts + 1
                            WHERE job_id = %s
                              AND (
                                  status = %s
                                  OR (status = %s AND processing_started_at < %s)
                              )
                            RETURNING job_id, job_type, payload, attempts
                        """, ("processing", now, now, job_id, "pending", "processing", stale_before))
                        return self._build_job_claim_result(cursor.fetchone())
        except Exception as e:
            logger.error("Failed to claim durable job %s: %s", job_id, e)
            return DurableJobClaimResult(status="db_error", error=str(e))

    def claim_next_durable_job(self, stale_after_seconds: int = 120) -> DurableJobClaimResult:
        """Claim the next oldest durable job that is pending or stale-processing."""
        stale_before = datetime.now() - timedelta(seconds=stale_after_seconds)
        now = datetime.now()
        try:
            with self._get_connection() as conn:
                with conn.transaction():
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            WITH next_job AS (
                                SELECT job_id
                                FROM durable_jobs
                                WHERE status = %s
                                   OR (status = %s AND processing_started_at < %s)
                                ORDER BY created_at ASC
                                LIMIT 1
                                FOR UPDATE SKIP LOCKED
                            )
                            UPDATE durable_jobs AS jobs
                            SET status = %s,
                                processing_started_at = %s,
                                updated_at = %s,
                                attempts = jobs.attempts + 1
                            FROM next_job
                            WHERE jobs.job_id = next_job.job_id
                            RETURNING jobs.job_id, jobs.job_type, jobs.payload, jobs.attempts
                        """, ("pending", "processing", stale_before, "processing", now, now))
                        return self._build_job_claim_result(cursor.fetchone())
        except Exception as e:
            logger.error("Failed to claim next durable job: %s", e)
            return DurableJobClaimResult(status="db_error", error=str(e))

    def mark_durable_job_completed(self, job_id: str) -> bool:
        """Mark a durable background job completed."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE durable_jobs
                        SET status = %s,
                            last_error = NULL,
                            processing_started_at = NULL,
                            updated_at = %s
                        WHERE job_id = %s
                    """, ("completed", datetime.now(), job_id))
            return True
        except Exception as e:
            logger.error("Failed to mark durable job completed for %s: %s", job_id, e)
            return False

    def mark_durable_job_failed(self, job_id: str, error_message: str) -> bool:
        """Mark a durable background job failed."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE durable_jobs
                        SET status = %s,
                            last_error = %s,
                            processing_started_at = NULL,
                            updated_at = %s
                        WHERE job_id = %s
                    """, ("failed", (error_message or "unknown_error")[:1000], datetime.now(), job_id))
            return True
        except Exception as e:
            logger.error("Failed to mark durable job failed for %s: %s", job_id, e)
            return False

    def get_durable_job_stats(self) -> dict:
        """Return durable job counts grouped by status."""
        stats = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT status, COUNT(*)
                        FROM durable_jobs
                        GROUP BY status
                    """)
                    for status, count in cursor.fetchall():
                        stats[status] = count
        except Exception as e:
            stats["stats_error"] = str(e)
        return stats
    
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
            
            with self._get_connection() as conn:
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

                    cursor.execute("""
                        DELETE FROM slack_event_states
                        WHERE updated_at < %s AND status IN (%s, %s)
                    """, (cutoff_date, "completed", "failed"))
                    deleted_slack_event_states = cursor.rowcount

                    cursor.execute("""
                        DELETE FROM durable_jobs
                        WHERE updated_at < %s AND status IN (%s, %s)
                    """, (cutoff_date, "completed", "failed"))
                    deleted_durable_jobs = cursor.rowcount
                    
                    logger.info(
                        "Cleanup: Deleted %s thread mappings, %s processed events, %s slack event states, and %s durable jobs older than %s days",
                        deleted_mappings,
                        deleted_events,
                        deleted_slack_event_states,
                        deleted_durable_jobs,
                        days,
                    )
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
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM thread_mappings")
                    total_mappings = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT COUNT(*) FROM processed_events")
                    total_events = cursor.fetchone()[0]

                    cursor.execute("SELECT COUNT(*) FROM slack_event_states")
                    total_slack_event_states = cursor.fetchone()[0]

                    cursor.execute("SELECT COUNT(*) FROM durable_jobs")
                    total_durable_jobs = cursor.fetchone()[0]
                    
                    return {
                        "total_mappings": total_mappings,
                        "total_processed_events": total_events,
                        "total_slack_event_states": total_slack_event_states,
                        "total_durable_jobs": total_durable_jobs,
                    }
                    
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {"total_mappings": 0, "total_processed_events": 0}
    
    def close(self):
        """Close all connections in the pool."""
        with ThreadMappingStore._pool_lock:
            if ThreadMappingStore._shared_pool:
                ThreadMappingStore._shared_pool.close()
                ThreadMappingStore._shared_pool = None
                ThreadMappingStore._db_initialized = False
                logger.info("PostgreSQL connection pool closed")
