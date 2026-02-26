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
    
    def __init__(self, thread_store: Optional[ThreadMappingStore] = None):
        """Initialize Slack client and thread store."""
        self.client = WebClient(token=Config.SLACK_BOT_TOKEN)
        self.thread_store = thread_store or ThreadMappingStore()
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

            # Parse first: many Zendesk triggers carry no comment to sync to Slack.
            messages = self._parse_webhook_event(payload)
            if not messages:
                logger.debug(f"No messages to post for ticket #{ticket_id}")
                return {"success": True, "skipped": True}
            
            # Get Slack thread info for this ticket
            thread_info = self.thread_store.get_thread_info(ticket_id)
            if not thread_info:
                logger.info(f"No Slack thread found for ticket #{ticket_id} - skipping")
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
        messages: List[str] = []

        def _format_attachments(attachments):
            formatted = []
            for a in attachments or []:
                name = a.get("file_name") or a.get("name") or a.get("filename") or "attachment"
                url = (
                    a.get("content_url")
                    or a.get("content_url_https")
                    or a.get("content_url_http")
                    or a.get("url")
                    or a.get("attachment_url")
                    or a.get("public_url")
                )
                if url:
                    formatted.append(f"<{url}|{name}>")
                else:
                    formatted.append(name)
            return formatted

        def _process_comment_obj(comment_obj):
            if not comment_obj:
                return
            author_name = comment_obj.get("author_name", "") or comment_obj.get("author", {}).get("name", "")
            if author_name == "Slack Automation":
                logger.debug("Skipping comment from Slack Automation (loop prevention)")
                return

            body = (
                comment_obj.get("body")
                or comment_obj.get("plain_body")
                or comment_obj.get("html_body")
                or ""
            )
            if not isinstance(body, str):
                body = str(body)

            if "[Posted from Slack]" in body:
                logger.debug("Skipping comment from Slack thread (loop prevention)")
                return

            is_public = comment_obj.get("public", True)
            attachments = comment_obj.get("attachments") or comment_obj.get("uploads") or []

            attach_links = _format_attachments(attachments)

            if body:
                if is_public:
                    author_email = comment_obj.get("author_email", "") or (comment_obj.get("author", {}).get("email") if isinstance(comment_obj.get("author"), dict) else "")
                    author_display = author_name or (comment_obj.get("author", {}).get("name") if isinstance(comment_obj.get("author"), dict) else author_name) or "Unknown"
                    if author_email:
                        prefix = f"💬 {author_display} ({author_email}) replied:\n"
                    else:
                        prefix = f"💬 {author_display} replied:\n"
                    msg = f"{prefix}{body}"
                    if attach_links:
                        msg += "\n\nAttachments:\n" + "\n".join(f"- {l}" for l in attach_links)
                    messages.append(msg)
                else:
                    prefix = f"🔒 Internal note from {author_display}:\n"
                    msg = f"{prefix}{body}"
                    if attach_links:
                        msg += "\n\nAttachments:\n" + "\n".join(f"- {l}" for l in attach_links)
                    messages.append(msg)

        current_comment = payload.get("current_comment") or payload.get("comment")
        if current_comment:
            _process_comment_obj(current_comment)

        audit = payload.get("audit") or payload.get("audits") or payload.get("event") or payload.get("ticket_audit")
        if audit:
            events = []
            if isinstance(audit, dict):
                events = audit.get("events", [])
            elif isinstance(audit, list):
                for a in audit:
                    if isinstance(a, dict):
                        events.extend(a.get("events", []) or [])
            for ev in events:
                ev_type = ev.get("type", "").lower()
                if ev_type == "comment":
                    comment_obj = ev.get("comment") or ev
                    _process_comment_obj(comment_obj)

        ticket = payload.get("ticket") or {}
        if ticket and isinstance(ticket, dict):
            t_comment = ticket.get("comment")
            if t_comment:
                _process_comment_obj(t_comment)

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
