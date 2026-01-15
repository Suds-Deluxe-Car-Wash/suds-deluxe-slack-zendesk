"""Flask webhook server for Slack-Zendesk integration."""
import logging
import sys
from flask import Flask, request, jsonify
from slack_bolt import App
from slack_bolt.adapter.flask import SlackRequestHandler
from src.config import Config
from src.slack_handler import SlackHandler

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


@bolt_app.shortcut("create_zendesk_ticket")
def handle_create_ticket_shortcut(ack, shortcut, client):
    """
    Handle the 'create_zendesk_ticket' message shortcut.
    
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
            client.chat_postEphemeral(
                channel=shortcut["channel"]["id"],
                user=shortcut["user"]["id"],
                text=f"❌ Failed to create ticket: {error_message}"
            )
            logger.error(f"Ticket creation failed: {error_message}")
            
    except Exception as e:
        logger.error(f"Error processing message shortcut: {e}", exc_info=True)
        client.chat_postEphemeral(
            channel=shortcut["channel"]["id"],
            user=shortcut["user"]["id"],
            text=f"❌ An error occurred while creating the ticket. Please try again or contact support."
        )


@flask_app.route("/slack/events", methods=["POST"])
def slack_events():
    """
    Handle Slack Events API requests.
    
    This endpoint receives all Slack events including message shortcuts.
    """
    return handler.handle(request)


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
