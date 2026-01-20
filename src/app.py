"""Flask webhook server for Slack-Zendesk integration."""
import logging
import sys
from flask import Flask, request, jsonify
from slack_bolt import App
from slack_bolt.adapter.flask import SlackRequestHandler
from src.config import Config
from src.slack_handler import SlackHandler
from src.zendesk_webhook_handler import ZendeskWebhookHandler

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Validate configuration before starting
missing_vars = Config.validate()
if missing_vars:
    logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
    logger.error("Please copy .env.example to .env and fill in the values")
    sys.exit(1)

# Initialize Slack Bolt app
bolt_app = App(
    token=Config.SLACK_BOT_TOKEN,
    signing_secret=Config.SLACK_SIGNING_SECRET
)

# Initialize Flask app
flask_app = Flask(__name__)
handler = SlackRequestHandler(bolt_app)

# Initialize Slack handler
slack_handler = SlackHandler()

# Initialize Zendesk webhook handler
zendesk_webhook_handler = ZendeskWebhookHandler()


@bolt_app.shortcut("create_custom_zendesk_ticket")
def handle_create_ticket_shortcut(ack, shortcut, client):
    """
    Handle the 'create_custom_zendesk_ticket' message shortcut.
    
    This is triggered when a user clicks the message action menu
    and selects "Create Zendesk Ticket".
    """
    # Acknowledge the shortcut request immediately (within 3 seconds)
    ack()
    
    logger.info(f"Received message shortcut from user {shortcut['user']['id']}")
    
    try:
        # Process the shortcut using SlackHandler
        result = slack_handler.handle_message_shortcut(shortcut)
        
        if result.get("success"):
            # Send ephemeral success message to user
            client.chat_postEphemeral(
                channel=shortcut["channel"]["id"],
                user=shortcut["user"]["id"],
                text=f"✅ Zendesk ticket #{result['ticket_id']} created successfully! Check the thread for details."
            )
        else:
            # Send ephemeral error message to user
            error_message = result.get("error", "Unknown error occurred")
            user_friendly_message = _get_user_friendly_error(error_message)
            client.chat_postEphemeral(
                channel=shortcut["channel"]["id"],
                user=shortcut["user"]["id"],
                text=f"❌ {user_friendly_message}\n\n_If this continues, please contact support._"
            )
            logger.error(f"Ticket creation failed: {error_message}")
            
    except Exception as e:
        logger.error(f"Error processing message shortcut: {e}", exc_info=True)
        client.chat_postEphemeral(
            channel=shortcut["channel"]["id"],
            user=shortcut["user"]["id"],
            text=f"❌ Unable to create ticket at the moment. Please try again in a few seconds.\n\n_If this continues, please contact support._"
        )


def _get_user_friendly_error(error_message: str) -> str:
    """Convert technical error messages to user-friendly ones."""
    error_lower = error_message.lower()
    
    # Connection errors (likely cold start on free tier)
    if "connection" in error_lower or "reset by peer" in error_lower or "timeout" in error_lower:
        return "The ticket system is starting up. Please try again in a moment."
    
    # Channel not allowed
    if "not allowed" in error_lower or "channel" in error_lower:
        return "Tickets cannot be created from this channel."
    
    # Parsing errors
    if "parse" in error_lower or "workflow" in error_lower:
        return "Could not read the message format. Please use the workflow form."
    
    # Zendesk API errors
    if "zendesk" in error_lower or "api" in error_lower:
        return "Unable to connect to the ticketing system. Please try again."
    
    # Generic fallback
    return "Failed to create ticket. Please try again."


@bolt_app.event("message")
def handle_message_events(event, client, logger):
    """
    Handle message events to capture thread replies.
    
    When a user replies in a thread where a Zendesk ticket was created,
    add that reply as an internal note to the corresponding ticket.
    """
    try:
        # Skip messages that don't have thread_ts (not in a thread)
        if "thread_ts" not in event:
            return
        
        # Skip if this is the parent message itself (thread_ts == ts)
        if event.get("thread_ts") == event.get("ts"):
            return
        
        # Check if channel is allowed (only process configured channels)
        channel_id = event.get("channel")
        if not channel_id:
            return
        
        from src.config import is_channel_allowed
        if not is_channel_allowed(channel_id):
            logger.debug(f"Skipping message from non-allowed channel {channel_id}")
            return
        
        # Skip bot messages to avoid posting our own ticket links to Zendesk
        if "bot_id" in event or event.get("subtype") == "bot_message":
            logger.debug(f"Skipping bot message in thread {event.get('thread_ts')}")
            return
        
        # Get event_id for deduplication
        event_id = event.get("event_id") or event.get("client_msg_id")
        if event_id and slack_handler.thread_store.is_event_processed(event_id):
            logger.debug(f"Event {event_id} already processed, skipping")
            return
        
        # This is a user reply in a thread - try to add to Zendesk
        success = slack_handler.add_thread_reply_to_ticket(event)
        
        # Mark event as processed to prevent duplicates
        if event_id:
            slack_handler.thread_store.mark_event_processed(event_id)
        
        if success:
            logger.info(f"Thread reply added to Zendesk from thread {event.get('thread_ts')}")
        
    except Exception as e:
        logger.error(f"Error processing message event: {e}", exc_info=True)


@flask_app.route("/slack/events", methods=["POST"])
def slack_events():
    """
    Handle Slack Events API requests.
    
    This endpoint receives all Slack events including message shortcuts.
    """
    return handler.handle(request)


@flask_app.route("/zendesk/webhook", methods=["POST"])
def zendesk_webhook():
    """
    Handle Zendesk webhook requests.
    
    This endpoint receives Zendesk ticket updates and posts them to Slack.
    """
    try:
        payload = request.get_json()
        if not payload:
            logger.warning("Received empty Zendesk webhook payload")
            return jsonify({"error": "Empty payload"}), 400
        
        # Process the webhook
        result = zendesk_webhook_handler.handle_webhook(payload)
        
        if result.get("success"):
            return jsonify({"status": "ok"}), 200
        else:
            return jsonify({"error": result.get("error", "Unknown error")}), 500
            
    except Exception as e:
        logger.error(f"Error processing Zendesk webhook: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@flask_app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for monitoring."""
    return jsonify({
        "status": "healthy",
        "service": "slack-zendesk-integration",
        "environment": Config.ENVIRONMENT
    }), 200


@flask_app.route("/", methods=["GET"])
def home():
    """Root endpoint with basic info."""
    return jsonify({
        "service": "Slack-Zendesk Integration",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "slack_events": "/slack/events",
            "health": "/health"
        }
    }), 200


def main():
    """Start the Flask application."""
    logger.info("Starting Slack-Zendesk Integration Server")
    logger.info(f"Environment: {Config.ENVIRONMENT}")
    logger.info(f"Port: {Config.PORT}")
    
    # Run Flask app
    flask_app.run(
        host="0.0.0.0",
        port=Config.PORT,
        debug=(Config.ENVIRONMENT == "development")
    )


if __name__ == "__main__":
    main()
