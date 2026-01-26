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
    Handle message events for:
    1. Auto-creating Zendesk tickets from workflow form submissions
    2. Adding thread replies as internal notes to existing tickets
    """
    try:
        channel_id = event.get("channel")
        if not channel_id:
            return
        
        # Check if channel is allowed (only process configured channels)
        from src.config import is_channel_allowed
        if not is_channel_allowed(channel_id):
            logger.debug(f"Skipping message from non-allowed channel {channel_id}")
            return
        
        # Use message timestamp (ts) for deduplication - this is unique per message
        # and doesn't change across Slack webhook retries (unlike event_id)
        message_ts = event.get("ts")
        if message_ts and slack_handler.thread_store.is_event_processed(message_ts):
            logger.debug(f"Message {message_ts} already processed, skipping duplicate")
            return
        
        # Determine if this is a workflow message or a thread reply
        is_thread_reply = ("thread_ts" in event and 
                          event.get("thread_ts") != event.get("ts"))
        
        if is_thread_reply:
            # Skip bot messages to avoid posting our own ticket links to Zendesk
            if "bot_id" in event or event.get("subtype") == "bot_message":
                logger.debug(f"Skipping bot message in thread {event.get('thread_ts')}")
                return
            
            # This is a user reply in a thread - try to add to Zendesk
            success = slack_handler.add_thread_reply_to_ticket(event)
            
            # Mark as processed after successful thread reply
            if success and message_ts:
                slack_handler.thread_store.mark_event_processed(message_ts)
            
            if success:
                logger.info(f"Thread reply added to Zendesk from thread {event.get('thread_ts')}")
        
        else:
            # This is a new message (not a thread reply)
            # Check if it's a workflow message that should auto-create a ticket
            if _is_workflow_message(event):
                logger.info(f"Detected workflow message in channel {channel_id}, auto-creating ticket")
                
                # Automatically create ticket from workflow message
                result = slack_handler.handle_workflow_message(
                    message=event,
                    channel_id=channel_id,
                    user_id=event.get("user")
                )
                
                if result.get("success"):
                    logger.info(f"Auto-created ticket #{result['ticket_id']} from workflow message")
                else:
                    # Ticket creation failed - allow retries
                    logger.error(f"Failed to auto-create ticket: {result.get('error')} - will allow retry")
        
    except Exception as e:
        logger.error(f"Error processing message event: {e}", exc_info=True)


def _is_workflow_message(event: dict) -> bool:
    """
    Detect if a message is from Slack Workflow Builder.
    
    Workflow messages typically have:
    - bot_profile field (workflow bot)
    - Structured blocks (rich_text)
    - Not a thread reply
    
    Args:
        event: Slack message event
    
    Returns:
        True if message is from a workflow, False otherwise
    """
    # Must have blocks (workflow forms use structured blocks)
    if "blocks" not in event or not event["blocks"]:
        return False
    
    # Must be from a bot (workflows post as bots)
    if "bot_id" not in event and event.get("subtype") != "bot_message":
        return False
    
    # Check for workflow-specific indicators
    # Workflow Builder messages often have bot_profile with workflow data
    if "bot_profile" in event:
        bot_name = event.get("bot_profile", {}).get("name", "")
        # Workflow Builder creates bots with specific naming patterns
        if "workflow" in bot_name.lower() or "Workflow" in bot_name:
            return True
    
    # Additional check: workflow messages have rich_text blocks with specific structure
    for block in event.get("blocks", []):
        if block.get("type") == "rich_text":
            # Workflow forms use rich_text blocks with section elements
            elements = block.get("elements", [])
            if elements and any(elem.get("type") == "rich_text_section" for elem in elements):
                return True
    
    return False


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
