"""Flask webhook server for Slack-Zendesk integration."""
import base64
import hashlib
import hmac
import json
import atexit
import logging
import logging.handlers
import queue
import sys
import threading
import time
from urllib.parse import parse_qs
from typing import Any, Dict, Optional

from flask import Flask, jsonify, request
from slack_bolt import App
from slack_bolt.adapter.flask import SlackRequestHandler

from src.config import Config, get_allowed_channel_ids, is_channel_allowed
from src.slack_handler import SlackHandler
from src.slack_log_alert_handler import SlackLogAlertHandler
from src.thread_store import ThreadMappingStore
from src.zendesk_webhook_handler import ZendeskWebhookHandler

# Configure logging with a QueueHandler so only one dedicated thread writes
# to stdout. Python 3.13's BufferedWriter is not reentrant-safe; putting a
# QueueListener between the root logger and StreamHandler serialises all
# writes and eliminates the "reentrant call inside <_io.BufferedWriter>" crash.
_log_queue = queue.SimpleQueue()
_stream_handler = logging.StreamHandler(sys.stdout)
_stream_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    handlers=[logging.handlers.QueueHandler(_log_queue)],
)
_log_listener = logging.handlers.QueueListener(
    _log_queue, _stream_handler, respect_handler_level=True
)
_log_listener.start()
atexit.register(_log_listener.stop)
logger = logging.getLogger(__name__)

_background_lock = threading.Lock()
_slack_worker_thread = None
_diagnostics_thread = None
_cleanup_thread = None


def configure_slack_log_alerts():
    """Attach Slack error alert logging handler if configured."""
    if not Config.SLACK_LOG_ALERTS_ENABLED:
        return

    if not Config.SLACK_LOG_ALERT_CHANNEL:
        logger.warning("SLACK_LOG_ALERTS_ENABLED is true but SLACK_LOG_ALERT_CHANNEL is not set")
        return

    level = getattr(logging, Config.SLACK_LOG_ALERT_LEVEL, logging.ERROR)
    root_logger = logging.getLogger()

    for existing_handler in root_logger.handlers:
        if isinstance(existing_handler, SlackLogAlertHandler):
            return

    slack_handler = SlackLogAlertHandler(
        token=Config.SLACK_BOT_TOKEN,
        channel=Config.SLACK_LOG_ALERT_CHANNEL,
        level=level,
    )
    slack_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    root_logger.addHandler(slack_handler)

    logger.info(
        "Slack log alerts enabled for channel %s at level %s",
        Config.SLACK_LOG_ALERT_CHANNEL,
        logging.getLevelName(level),
    )
    logger.warning(
        "Alert pipeline enabled channel=%s level=%s",
        Config.SLACK_LOG_ALERT_CHANNEL,
        logging.getLevelName(level),
    )


def validate_runtime_configuration():
    """Emit warnings for risky but non-fatal runtime configuration."""
    allowed_channels = set(get_allowed_channel_ids())

    if Config.SLACK_LOG_ALERTS_ENABLED and Config.SLACK_LOG_ALERT_CHANNEL in allowed_channels:
        logger.warning(
            "SLACK_LOG_ALERT_CHANNEL=%s is also in the allowed processing channel list; "
            "move alerts to a separate channel to avoid self-generated traffic",
            Config.SLACK_LOG_ALERT_CHANNEL,
        )

    if Config.ENVIRONMENT == "production" and not Config.ZENDESK_WEBHOOK_SIGNING_SECRET:
        logger.warning(
            "ZENDESK_WEBHOOK_SIGNING_SECRET is not set; Zendesk webhook signature validation is disabled"
        )


# Validate configuration before starting
missing_vars = Config.validate()
if missing_vars:
    logger.error("Missing required environment variables: %s", ", ".join(missing_vars))
    logger.error("Please copy .env.example to .env and fill in the values")
    sys.exit(1)

configure_slack_log_alerts()
validate_runtime_configuration()

# Initialize Slack Bolt app
bolt_app = App(token=Config.SLACK_BOT_TOKEN, signing_secret=Config.SLACK_SIGNING_SECRET)

# Initialize Flask app
flask_app = Flask(__name__)
handler = SlackRequestHandler(bolt_app)

# Initialize shared thread store and handlers
thread_store = ThreadMappingStore()
slack_handler = SlackHandler(thread_store=thread_store)
zendesk_webhook_handler = ZendeskWebhookHandler(thread_store=thread_store)
work_queue: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=Config.SLACK_EVENT_QUEUE_SIZE)


def _queue_depth() -> int:
    """Best-effort queue depth reporting."""
    try:
        return work_queue.qsize()
    except NotImplementedError:
        return -1


def _signal_job(job_id: str, description: str) -> None:
    """Signal the in-memory worker that a durable job is ready."""
    try:
        work_queue.put_nowait({"job_id": job_id})
        queue_depth = _queue_depth()
        logger.info("Durable job signaled job_id=%s job=%s queue_depth=%s", job_id, description, queue_depth)
    except queue.Full:
        logger.warning(
            "Durable job persisted but signal queue is full job_id=%s job=%s queue_depth=%s",
            job_id,
            description,
            _queue_depth(),
        )


def _process_queued_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """Process one durable job payload."""
    job_type = job.get("job_type")

    if job_type == "slack_message_event":
        return slack_handler.process_message_event_job(job)

    if job_type == "shortcut":
        return slack_handler.process_shortcut_job(job)

    logger.warning("Unknown queued job type=%s", job_type)
    return {"success": False, "error": f"unknown_job_type:{job_type}"}


def _claim_next_job_for_worker(job_id_hint: Optional[str] = None):
    """Claim the next durable job for the worker, optionally preferring a hinted job."""
    if job_id_hint:
        claimed_job = thread_store.claim_durable_job(job_id_hint, Config.DURABLE_JOB_STALE_SECONDS)
        if claimed_job.status == "claimed":
            return claimed_job
        if claimed_job.status == "db_error":
            logger.error("Failed to claim durable job from hint job_id=%s error=%s", job_id_hint, claimed_job.error)

    claimed_job = thread_store.claim_next_durable_job(Config.DURABLE_JOB_STALE_SECONDS)
    if claimed_job.status == "db_error":
        logger.error("Failed to claim next durable job error=%s", claimed_job.error)
        return None
    if claimed_job.status == "claimed":
        return claimed_job
    return None


def _slack_job_worker() -> None:
    """Serial Slack/Zendesk worker to keep DB pressure bounded."""
    logger.info(
        "Slack job worker started queue_size=%s durable_job_stats=%s",
        Config.SLACK_EVENT_QUEUE_SIZE,
        thread_store.get_durable_job_stats(),
    )
    while True:
        queued_signal = None
        claimed_job = None
        try:
            try:
                queued_signal = work_queue.get(timeout=Config.DURABLE_JOB_POLL_INTERVAL_SECONDS)
            except queue.Empty:
                queued_signal = None

            job_id_hint = queued_signal.get("job_id") if queued_signal else None
            claimed_job = _claim_next_job_for_worker(job_id_hint)
            if claimed_job is None:
                continue

            job = claimed_job.payload or {}
            job["job_id"] = claimed_job.job_id
            job.setdefault("job_type", claimed_job.job_type)
            job["queue_depth"] = _queue_depth()
            logger.info(
                "Durable job claimed job_id=%s job_type=%s attempts=%s queue_depth=%s",
                claimed_job.job_id,
                claimed_job.job_type,
                claimed_job.attempts,
                _queue_depth(),
            )

            result = _process_queued_job(job)
            if result.get("success"):
                if not thread_store.mark_durable_job_completed(claimed_job.job_id):
                    logger.error("Failed to mark durable job completed job_id=%s", claimed_job.job_id)
            else:
                if not thread_store.mark_durable_job_failed(
                    claimed_job.job_id,
                    result.get("error") or "job_failed",
                ):
                    logger.error("Failed to mark durable job failed job_id=%s", claimed_job.job_id)
        except Exception as exc:
            slack_event_id = None
            if claimed_job and claimed_job.payload:
                slack_event_id = claimed_job.payload.get("slack_event_id")
            if slack_event_id:
                thread_store.mark_slack_event_failed(slack_event_id, f"worker_exception:{exc}")
            if claimed_job and claimed_job.job_id:
                thread_store.mark_durable_job_failed(claimed_job.job_id, f"worker_exception:{exc}")
            logger.error("Unhandled error in Slack job worker: %s", exc, exc_info=True)
        finally:
            if queued_signal is not None:
                work_queue.task_done()


def _diagnostics_worker() -> None:
    """Emit periodic runtime diagnostics for queue and DB pool health."""
    logger.info(
        "Diagnostics worker started interval_seconds=%s",
        Config.DIAGNOSTICS_LOG_INTERVAL_SECONDS,
    )
    prev_connections_ms = None

    while True:
        time.sleep(Config.DIAGNOSTICS_LOG_INTERVAL_SECONDS)
        worker_alive = _slack_worker_thread.is_alive() if _slack_worker_thread else False
        pool_stats = thread_store.get_pool_stats()
        logger.info(
            "Runtime diagnostics queue_depth=%s worker_alive=%s db_pool_stats=%s durable_job_stats=%s",
            _queue_depth(),
            worker_alive,
            pool_stats,
            thread_store.get_durable_job_stats(),
        )

        # Proactive zombie pool detection: if the pool has connections but
        # none are available, requests are waiting, and the lifetime
        # connections_ms counter hasn't budged since the last cycle, the
        # pool's internal worker threads are likely dead.
        try:
            pool_size = pool_stats.get("pool_size", 0)
            pool_available = pool_stats.get("pool_available", 0)
            requests_waiting = pool_stats.get("requests_waiting", 0)
            connections_ms = pool_stats.get("connections_ms", 0)

            is_zombie = (
                pool_size > 0
                and pool_available == 0
                and requests_waiting > 0
                and prev_connections_ms is not None
                and connections_ms == prev_connections_ms
                and connections_ms > 0
            )

            if is_zombie:
                logger.warning(
                    "ZOMBIE POOL DETECTED by diagnostics: pool_size=%s pool_available=%s "
                    "requests_waiting=%s connections_ms=%s (unchanged). Triggering reset.",
                    pool_size,
                    pool_available,
                    requests_waiting,
                    connections_ms,
                )
                thread_store._reset_pool()

            prev_connections_ms = connections_ms
        except Exception as exc:
            logger.error("Error in zombie pool detection: %s", exc)


def _cleanup_worker() -> None:
    """Run cleanup of old mappings once per month."""
    logger.info("Scheduled monthly cleanup task started (runs every 30 days)")
    while True:
        try:
            time.sleep(30 * 24 * 60 * 60)
            deleted = thread_store.cleanup_old_mappings(days=30)
            logger.info("Monthly cleanup completed removed_mappings=%s", deleted)
        except Exception as exc:
            logger.error("Error in monthly cleanup task: %s", exc, exc_info=True)


def start_background_tasks() -> None:
    """Start queue, diagnostics, and cleanup threads once per process."""
    global _slack_worker_thread, _diagnostics_thread, _cleanup_thread

    with _background_lock:
        if _slack_worker_thread is None or not _slack_worker_thread.is_alive():
            _slack_worker_thread = threading.Thread(target=_slack_job_worker, daemon=True)
            _slack_worker_thread.start()

        if _diagnostics_thread is None or not _diagnostics_thread.is_alive():
            _diagnostics_thread = threading.Thread(target=_diagnostics_worker, daemon=True)
            _diagnostics_thread.start()

        if _cleanup_thread is None or not _cleanup_thread.is_alive():
            _cleanup_thread = threading.Thread(target=_cleanup_worker, daemon=True)
            _cleanup_thread.start()


@bolt_app.shortcut("create_custom_zendesk_ticket")
def handle_create_ticket_shortcut(ack, shortcut, client):
    """Persist and signal the shortcut request before returning control to Slack."""
    user_id = shortcut.get("user", {}).get("id")
    channel_id = shortcut.get("channel", {}).get("id")
    message_ts = shortcut.get("message", {}).get("ts")
    trigger_id = shortcut.get("trigger_id") or f"{channel_id}:{message_ts}:{user_id}:{time.time_ns()}"
    job_id = f"shortcut:{trigger_id}"
    durable_job = {
        "job_id": job_id,
        "job_type": "shortcut",
        "shortcut": shortcut,
        "enqueued_at": time.time(),
    }

    logger.info(
        "Received message shortcut user_id=%s channel_id=%s message_ts=%s",
        user_id,
        channel_id,
        message_ts,
    )

    enqueue_result = thread_store.enqueue_durable_job(job_id, "shortcut", durable_job)
    ack()

    if enqueue_result.status == "db_error" and channel_id and user_id:
        try:
            client.chat_postEphemeral(
                channel=channel_id,
                user=user_id,
                text="The ticket request could not be saved right now. Please try again in a moment.",
            )
        except Exception as exc:
            logger.error("Failed to notify shortcut user about durable queue error: %s", exc)
        return

    if enqueue_result.status == "created":
        _signal_job(
            job_id,
            f"shortcut user_id={user_id} channel_id={channel_id} message_ts={message_ts}",
        )
        return

    if enqueue_result.status == "duplicate" and enqueue_result.existing_status in {"pending", "processing"}:
        _signal_job(
            job_id,
            f"shortcut retry user_id={user_id} channel_id={channel_id} message_ts={message_ts}",
        )

def _is_workflow_message(event: dict) -> bool:
    """
    Detect if a message is from Slack Workflow Builder.

    Workflow messages typically have:
    - bot_profile field
    - Structured blocks (rich_text)
    - Not a thread reply
    """
    if "blocks" not in event or not event["blocks"]:
        return False

    if "bot_id" not in event and event.get("subtype") != "bot_message":
        return False

    if "bot_profile" in event:
        bot_name = event.get("bot_profile", {}).get("name", "")
        if "workflow" in bot_name.lower():
            return True

    for block in event.get("blocks", []):
        if block.get("type") == "rich_text":
            elements = block.get("elements", [])
            if elements and any(elem.get("type") == "rich_text_section" for elem in elements):
                return True

    return False


def _verify_slack_signature(raw_body: str) -> bool:
    """Verify Slack request signature for Events API deliveries."""
    timestamp = request.headers.get("X-Slack-Request-Timestamp")
    signature = request.headers.get("X-Slack-Signature")

    if not timestamp or not signature:
        return False

    try:
        request_age = abs(time.time() - int(timestamp))
    except ValueError:
        return False

    if request_age > 300:
        return False

    basestring = f"v0:{timestamp}:{raw_body}"
    computed = "v0=" + hmac.new(
        Config.SLACK_SIGNING_SECRET.encode("utf-8"),
        basestring.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(computed, signature)


def _verify_zendesk_signature(raw_body: str) -> bool:
    """Verify Zendesk webhook signature when a signing secret is configured."""
    if not Config.ZENDESK_WEBHOOK_SIGNING_SECRET:
        return True

    signature = request.headers.get("X-Zendesk-Webhook-Signature")
    timestamp = request.headers.get("X-Zendesk-Webhook-Signature-Timestamp")

    if not signature or not timestamp:
        return False

    digest = hmac.new(
        Config.ZENDESK_WEBHOOK_SIGNING_SECRET.encode("utf-8"),
        f"{timestamp}{raw_body}".encode("utf-8"),
        hashlib.sha256,
    ).digest()
    expected_signature = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(expected_signature, signature)


def _classify_slack_event(event: Dict[str, Any]) -> Optional[str]:
    """Return a normalized job kind for supported Slack events."""
    channel_id = event.get("channel")
    if not channel_id or not is_channel_allowed(channel_id):
        return None

    message_ts = event.get("ts")
    thread_ts = event.get("thread_ts")
    is_thread_reply = thread_ts is not None and thread_ts != message_ts

    if is_thread_reply:
        if "bot_id" in event or event.get("subtype") == "bot_message":
            return None
        return "thread_reply"

    if _is_workflow_message(event):
        return "workflow_message"

    return None


def _timed_json_response(payload: Dict[str, Any], status_code: int, route_name: str, started_at: float):
    """Return a JSON response and emit request duration logging."""
    duration_ms = round((time.time() - started_at) * 1000, 2)
    logger.info("Request finished route=%s status_code=%s duration_ms=%s", route_name, status_code, duration_ms)
    return jsonify(payload), status_code


@flask_app.route("/slack/events", methods=["POST"])
def slack_events():
    """Handle Slack Events API and Slack interactivity requests."""
    started_at = time.time()
    retry_num = request.headers.get("X-Slack-Retry-Num")
    retry_reason = request.headers.get("X-Slack-Retry-Reason")
    raw_body = request.get_data(cache=True, as_text=True)
    logger.info("Request started route=/slack/events content_type=%s", request.content_type)

    if retry_num or retry_reason:
        logger.warning(
            "Received Slack retry delivery retry_num=%s retry_reason=%s queue_depth=%s",
            retry_num,
            retry_reason,
            _queue_depth(),
        )

    parsed_form = parse_qs(raw_body, keep_blank_values=True) if raw_body else {}
    if "payload" in parsed_form:
        logger.info("Delegating Slack interactive payload to Bolt route=/slack/events")
        response = handler.handle(request)
        duration_ms = round((time.time() - started_at) * 1000, 2)
        logger.info(
            "Request finished route=/slack/events status_code=%s duration_ms=%s",
            response.status_code,
            duration_ms,
        )
        return response

    if not _verify_slack_signature(raw_body):
        logger.warning("Rejected Slack request due to invalid signature route=/slack/events")
        return _timed_json_response({"error": "Invalid signature"}, 401, "/slack/events", started_at)

    try:
        payload = json.loads(raw_body) if raw_body else {}
    except json.JSONDecodeError:
        logger.warning("Received invalid JSON on /slack/events")
        return _timed_json_response({"error": "Invalid JSON"}, 400, "/slack/events", started_at)
    payload_type = payload.get("type")
    if payload_type == "url_verification":
        return _timed_json_response({"challenge": payload.get("challenge")}, 200, "/slack/events", started_at)

    if payload_type != "event_callback":
        logger.info("Ignoring unsupported Slack payload type=%s", payload_type)
        return _timed_json_response({"ok": True, "ignored": True}, 200, "/slack/events", started_at)

    event = payload.get("event") or {}
    slack_event_id = payload.get("event_id")
    if not slack_event_id:
        logger.warning("Received Slack event without event_id")
        return _timed_json_response({"ok": True, "ignored": True}, 200, "/slack/events", started_at)

    event_kind = _classify_slack_event(event)
    if not event_kind:
        logger.info(
            "Ignoring unsupported Slack event slack_event_id=%s event_type=%s channel_id=%s",
            slack_event_id,
            event.get("type"),
            event.get("channel"),
        )
        return _timed_json_response({"ok": True, "ignored": True}, 200, "/slack/events", started_at)

    durable_job = {
        "job_id": f"slack_event:{slack_event_id}",
        "job_type": "slack_message_event",
        "event_kind": event_kind,
        "slack_event_id": slack_event_id,
        "event": event,
        "enqueued_at": time.time(),
    }
    enqueue_result = thread_store.enqueue_slack_event_job(slack_event_id, durable_job)
    if enqueue_result.status == "db_error":
        logger.error(
            "Failed to persist Slack ingress event slack_event_id=%s error=%s",
            slack_event_id,
            enqueue_result.error,
        )
        return _timed_json_response({"error": "Event state unavailable"}, 500, "/slack/events", started_at)

    if enqueue_result.status == "duplicate":
        logger.info(
            "Skipping duplicate Slack event at ingress slack_event_id=%s state=%s",
            slack_event_id,
            enqueue_result.existing_status,
        )
        if enqueue_result.existing_status in {"received"}:
            _signal_job(
                durable_job["job_id"],
                (
                    f"{event_kind} duplicate slack_event_id={slack_event_id} "
                    f"message_ts={event.get('ts')} thread_ts={event.get('thread_ts')}"
                ),
            )
        return _timed_json_response({"ok": True, "duplicate": True}, 200, "/slack/events", started_at)

    logger.info(
        "Accepted Slack event slack_event_id=%s event_kind=%s message_ts=%s thread_ts=%s retry_num=%s retry_reason=%s",
        slack_event_id,
        event_kind,
        event.get("ts"),
        event.get("thread_ts"),
        retry_num,
        retry_reason,
    )
    _signal_job(
        durable_job["job_id"],
        (
            f"{event_kind} slack_event_id={slack_event_id} "
            f"message_ts={event.get('ts')} thread_ts={event.get('thread_ts')}"
        ),
    )
    return _timed_json_response({"ok": True, "queued": True}, 200, "/slack/events", started_at)


@flask_app.route("/zendesk/webhook", methods=["POST"])
def zendesk_webhook():
    """Handle Zendesk webhook requests."""
    started_at = time.time()
    logger.info("Request started route=/zendesk/webhook")
    try:
        raw_body = request.get_data(cache=True, as_text=True)
        invocation_id = request.headers.get("X-Zendesk-Webhook-Invocation-Id")

        if not _verify_zendesk_signature(raw_body):
            logger.warning("Rejected Zendesk webhook due to invalid signature zendesk_invocation_id=%s", invocation_id)
            return _timed_json_response({"error": "Invalid signature"}, 401, "/zendesk/webhook", started_at)

        payload = request.get_json(silent=True)
        if not payload:
            logger.warning("Received empty Zendesk webhook payload zendesk_invocation_id=%s", invocation_id)
            return _timed_json_response({"error": "Empty payload"}, 400, "/zendesk/webhook", started_at)

        dedupe_key = f"zendesk:{invocation_id}" if invocation_id else None
        if dedupe_key:
            processed = thread_store.is_event_processed(dedupe_key)
            if processed.status == "processed":
                logger.info("Skipping duplicate Zendesk webhook zendesk_invocation_id=%s", invocation_id)
                return _timed_json_response({"status": "duplicate"}, 200, "/zendesk/webhook", started_at)
            if processed.status == "db_error":
                logger.error(
                    "Failed to check Zendesk webhook dedupe zendesk_invocation_id=%s error=%s",
                    invocation_id,
                    processed.error,
                )
                return _timed_json_response({"error": "Webhook dedupe unavailable"}, 500, "/zendesk/webhook", started_at)

        result = zendesk_webhook_handler.handle_webhook(payload, invocation_id=invocation_id)

        if result.get("success"):
            if dedupe_key and not thread_store.mark_event_processed(dedupe_key):
                logger.error(
                    "Failed to mark Zendesk webhook processed zendesk_invocation_id=%s",
                    invocation_id,
                )
            return _timed_json_response(
                {"status": "ok", "skipped": result.get("skipped", False)},
                200,
                "/zendesk/webhook",
                started_at,
            )

        return _timed_json_response(
            {"error": result.get("error", "Unknown error")},
            500,
            "/zendesk/webhook",
            started_at,
        )

    except Exception as exc:
        logger.error("Error processing Zendesk webhook: %s", exc, exc_info=True)
        return _timed_json_response({"error": str(exc)}, 500, "/zendesk/webhook", started_at)


@flask_app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for monitoring."""
    return jsonify(
        {
            "status": "healthy",
            "service": "slack-zendesk-integration",
            "environment": Config.ENVIRONMENT,
            "queue_depth": _queue_depth(),
            "worker_alive": _slack_worker_thread.is_alive() if _slack_worker_thread else False,
        }
    ), 200


@flask_app.route("/diagnostics", methods=["GET"])
def diagnostics():
    """Detailed diagnostics for queue and DB pool state."""
    return jsonify(
        {
            "service": "slack-zendesk-integration",
            "environment": Config.ENVIRONMENT,
            "queue_depth": _queue_depth(),
            "worker_alive": _slack_worker_thread.is_alive() if _slack_worker_thread else False,
            "db_pool_stats": thread_store.get_pool_stats(),
            "durable_job_stats": thread_store.get_durable_job_stats(),
        }
    ), 200


@flask_app.route("/", methods=["GET"])
def home():
    """Root endpoint with basic info."""
    return jsonify(
        {
            "service": "Slack-Zendesk Integration",
            "version": "1.0.0",
            "status": "running",
            "endpoints": {
                "slack_events": "/slack/events",
                "zendesk_webhook": "/zendesk/webhook",
                "health": "/health",
                "diagnostics": "/diagnostics",
            },
        }
    ), 200


start_background_tasks()


def main():
    """Start the Flask application."""
    logger.info("Starting Slack-Zendesk Integration Server")
    logger.info("Environment: %s", Config.ENVIRONMENT)
    logger.info("Port: %s", Config.PORT)
    start_background_tasks()

    flask_app.run(
        host="0.0.0.0",
        port=Config.PORT,
        debug=(Config.ENVIRONMENT == "development"),
    )


if __name__ == "__main__":
    main()
