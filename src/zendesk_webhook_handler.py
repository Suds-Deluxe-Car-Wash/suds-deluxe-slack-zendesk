"""Handle Zendesk webhook events and sync to Slack."""
import logging
from typing import Dict, Any, Optional
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from src.config import Config
from src.thread_store import ThreadMappingStore

logger = logging.getLogger(__name__)


class ZendeskWebhookHandler:
    """Handles Zendesk webhook events and posts updates to Slack."""
    
    def __init__(self):
        """Initialize Slack client and thread store."""
        self.client = WebClient(token=Config.SLACK_BOT_TOKEN)
        self.thread_store = ThreadMappingStore()
        logger.info("ZendeskWebhookHandler initialized")
    
    def handle_webhook(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process Zendesk webhook payload and post updates to Slack.
        
        Args:
            payload: Zendesk webhook payload
            
        Returns:
            Response dictionary with success status
        """
        try:
            # Extract ticket ID from webhook
            ticket_id = self._extract_ticket_id(payload)
            if not ticket_id:
                logger.warning("No ticket ID found in webhook payload")
                return {"success": False, "error": "No ticket ID"}
            
            # Get Slack thread info for this ticket
            thread_info = self.thread_store.get_thread_info(ticket_id)
            if not thread_info:
                logger.info(f"No Slack thread found for ticket #{ticket_id} - skipping")
                return {"success": True, "skipped": True}
            
            # Parse the event and create message
            messages = self._parse_webhook_event(payload)
            if not messages:
                logger.debug(f"No messages to post for ticket #{ticket_id}")
                return {"success": True, "skipped": True}
            
            # Post each message to Slack thread
            for message in messages:
                self._post_to_slack_thread(
                    channel_id=thread_info["channel_id"],
                    thread_ts=thread_info["thread_ts"],
                    message=message
                )
            
            logger.info(f"Posted {len(messages)} update(s) to Slack for ticket #{ticket_id}")
            return {"success": True, "messages_posted": len(messages)}
            
        except Exception as e:
            logger.error(f"Error handling Zendesk webhook: {e}", exc_info=True)
            return {"success": False, "error": str(e)}
    
    def _extract_ticket_id(self, payload: Dict[str, Any]) -> Optional[int]:
        """Extract ticket ID from Zendesk webhook payload."""
        # Zendesk sends ticket ID in different places depending on trigger type
        ticket_id = payload.get("ticket_id")
        if ticket_id:
            return int(ticket_id)
        
        # Check in ticket object
        ticket = payload.get("ticket", {})
        if ticket and ticket.get("id"):
            return int(ticket["id"])
        
        return None
    
    def _parse_webhook_event(self, payload: Dict[str, Any]) -> list:
        """
        Parse Zendesk webhook event and extract relevant updates.
        
        Args:
            payload: Zendesk webhook payload
            
        Returns:
            List of formatted message strings to post to Slack
        """
        messages = []
        
        # Get current comment/update
        current_comment = payload.get("current_comment")
        if current_comment:
            # Skip comments from "Slack Automation" to prevent loops
            author_name = current_comment.get("author_name", "")
            if author_name == "Slack Automation":
                logger.debug("Skipping comment from Slack Automation (loop prevention)")
                return messages
            
            is_public = current_comment.get("public", True)
            body = current_comment.get("body", "").strip()
            
            if body:
                if is_public:
                    # Public comment from customer or agent
                    author = current_comment.get("author_name", "Unknown")
                    author_email = current_comment.get("author_email", "")
                    if author_email:
                        messages.append(f"💬 {author} ({author_email}) replied:\n{body}")
                    else:
                        messages.append(f"💬 {author} replied:\n{body}")
                else:
                    # Internal note from agent
                    author = current_comment.get("author_name", "Agent")
                    messages.append(f"🔒 Internal note from {author}:\n{body}")
        
        return messages
    
    def _post_to_slack_thread(self, channel_id: str, thread_ts: str, message: str) -> bool:
        """
        Post a message to a Slack thread.
        
        Args:
            channel_id: Slack channel ID
            thread_ts: Thread timestamp
            message: Message text to post
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                text=message
            )
            return True
            
        except SlackApiError as e:
            logger.error(f"Failed to post to Slack thread: {e}")
            return False
