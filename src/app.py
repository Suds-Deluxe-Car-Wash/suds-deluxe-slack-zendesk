"""Flask webhook server for Slack-Zendesk integration."""
import base64
import hashlib
import hmac
import logging
import queue
import sys
import threading
import time
from typing import Any, Dict

from flask import Flask, jsonify, request
from slack_bolt import App
from slack_bolt.adapter.flask import SlackRequestHandler

from src.config import Config, get_allowed_channel_ids, is_channel_allowed
from src.slack_handler import SlackHandler
from src.slack_log_alert_handler import SlackLogAlertHandler
from src.thread_store import ThreadMappingStore
from src.zendesk_webhook_handler import ZendeskWebhookHandler

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
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


def _enqueue_job(job: Dict[str, Any], description: str) -> bool:
    """Enqueue a background job without blocking the Slack request thread."""
    try:
        work_queue.put_nowait(job)
        logger.info("Enqueued job=%s queue_depth=%s", description, _queue_depth())
        return True
    except queue.Full:
        logger.warning("Dropping job=%s because queue is full queue_depth=%s", description, _queue_depth())
        return False


def _process_queued_job(job: Dict[str, Any]) -> None:
    """Process one queued job."""
    job_type = job.get("job_type")

    if job_type == "slack_message_event":
        slack_handler.process_message_event_job(job)
        return

    if job_type == "shortcut":
        slack_handler.process_shortcut_job(job)
        return

    logger.warning("Unknown queued job type=%s", job_type)


def _slack_job_worker() -> None:
    """Serial Slack/Zendesk worker to keep DB pressure bounded."""
    logger.info("Slack job worker started queue_size=%s", Config.SLACK_EVENT_QUEUE_SIZE)
    while True:
        job = work_queue.get()
        try:
            _process_queued_job(job)
        except Exception as exc:
            logger.error("Unhandled error in Slack job worker: %s", exc, exc_info=True)
        finally:
            work_queue.task_done()


def _diagnostics_worker() -> None:
    """Emit periodic runtime diagnostics for queue and DB pool health."""
    logger.info(
        "Diagnostics worker started interval_seconds=%s",
        Config.DIAGNOSTICS_LOG_INTERVAL_SECONDS,
    )
    while True:
        time.sleep(Config.DIAGNOSTICS_LOG_INTERVAL_SECONDS)
        worker_alive = _slack_worker_thread.is_alive() if _slack_worker_thread else False
        logger.info(
            "Runtime diagnostics queue_depth=%s worker_alive=%s db_pool_stats=%s",
            _queue_depth(),
            worker_alive,
            thread_store.get_pool_stats(),
        )


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
    """Queue the shortcut request and acknowledge immediately."""
    ack()

    user_id = shortcut.get("user", {}).get("id")
    channel_id = shortcut.get("channel", {}).get("id")
    message_ts = shortcut.get("message", {}).get("ts")

    logger.info(
        "Received message shortcut user_id=%s channel_id=%s message_ts=%s",
        user_id,
        channel_id,
        message_ts,
    )

    enqueued = _enqueue_job(
        {
            "job_type": "shortcut",
            "shortcut": shortcut,
            "enqueued_at": time.time(),
        },
        f"shortcut user_id={user_id} channel_id={channel_id} message_ts={message_ts}",
    )

    if not enqueued and channel_id and user_id:
        try:
            client.chat_postEphemeral(
                channel=channel_id,
                user=user_id,
                text="The ticket queue is full right now. Please try again in a moment.",
            )
        except Exception as exc:
            logger.error("Failed to notify shortcut user about full queue: %s", exc)


@bolt_app.event("message")
def handle_message_events(body, event, logger):
    """Queue relevant Slack message events for serialized processing."""
    channel_id = event.get("channel")
    if not channel_id:
        return

    if not is_channel_allowed(channel_id):
        logger.debug("Skipping message from non-allowed channel %s", channel_id)
        return

    message_ts = event.get("ts")
    thread_ts = event.get("thread_ts")
    slack_event_id = body.get("event_id")
    is_thread_reply = thread_ts is not None and thread_ts != message_ts

    if is_thread_reply:
        if "bot_id" in event or event.get("subtype") == "bot_message":
            logger.debug("Skipping bot message in thread %s", thread_ts)
            return

        _enqueue_job(
            {
                "job_type": "slack_message_event",
                "event_kind": "thread_reply",
                "slack_event_id": slack_event_id,
                "event": event,
                "enqueued_at": time.time(),
            },
            f"thread_reply slack_event_id={slack_event_id} thread_ts={thread_ts} message_ts={message_ts}",
        )
        return

    if _is_workflow_message(event):
        logger.info(
            "Detected workflow message channel_id=%s slack_event_id=%s message_ts=%s",
            channel_id,
            slack_event_id,
            message_ts,
        )
        _enqueue_job(
            {
                "job_type": "slack_message_event",
                "event_kind": "workflow_message",
                "slack_event_id": slack_event_id,
                "event": event,
                "enqueued_at": time.time(),
            },
            f"workflow slack_event_id={slack_event_id} channel_id={channel_id} message_ts={message_ts}",
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


@flask_app.route("/slack/events", methods=["POST"])
def slack_events():
    """Handle Slack Events API requests."""
    retry_num = request.headers.get("X-Slack-Retry-Num")
    retry_reason = request.headers.get("X-Slack-Retry-Reason")

    if retry_num or retry_reason:
        logger.warning(
            "Received Slack retry delivery retry_num=%s retry_reason=%s queue_depth=%s",
            retry_num,
            retry_reason,
            _queue_depth(),
        )

    return handler.handle(request)


@flask_app.route("/zendesk/webhook", methods=["POST"])
def zendesk_webhook():
    """Handle Zendesk webhook requests."""
    try:
        raw_body = request.get_data(cache=True, as_text=True)
        invocation_id = request.headers.get("X-Zendesk-Webhook-Invocation-Id")

        if not _verify_zendesk_signature(raw_body):
            logger.warning("Rejected Zendesk webhook due to invalid signature zendesk_invocation_id=%s", invocation_id)
            return jsonify({"error": "Invalid signature"}), 401

        payload = request.get_json(silent=True)
        if not payload:
            logger.warning("Received empty Zendesk webhook payload zendesk_invocation_id=%s", invocation_id)
            return jsonify({"error": "Empty payload"}), 400

        dedupe_key = f"zendesk:{invocation_id}" if invocation_id else None
        if dedupe_key:
            processed = thread_store.is_event_processed(dedupe_key)
            if processed.status == "processed":
                logger.info("Skipping duplicate Zendesk webhook zendesk_invocation_id=%s", invocation_id)
                return jsonify({"status": "duplicate"}), 200
            if processed.status == "db_error":
                logger.error(
                    "Failed to check Zendesk webhook dedupe zendesk_invocation_id=%s error=%s",
                    invocation_id,
                    processed.error,
                )
                return jsonify({"error": "Webhook dedupe unavailable"}), 500

        result = zendesk_webhook_handler.handle_webhook(payload, invocation_id=invocation_id)

        if result.get("success"):
            if dedupe_key and not thread_store.mark_event_processed(dedupe_key):
                logger.error(
                    "Failed to mark Zendesk webhook processed zendesk_invocation_id=%s",
                    invocation_id,
                )
            return jsonify({"status": "ok", "skipped": result.get("skipped", False)}), 200

        return jsonify({"error": result.get("error", "Unknown error")}), 500

    except Exception as exc:
        logger.error("Error processing Zendesk webhook: %s", exc, exc_info=True)
        return jsonify({"error": str(exc)}), 500


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
