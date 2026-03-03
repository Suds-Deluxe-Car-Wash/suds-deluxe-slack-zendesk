"""Handle Zendesk webhook events and sync to Slack."""
import logging
from typing import Any, Dict, List, Optional

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

    def handle_webhook(
        self,
        payload: Dict[str, Any],
        invocation_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Process a Zendesk webhook payload and post updates to Slack."""
        try:
            ticket_id = self._extract_ticket_id(payload)
            if not ticket_id:
                logger.warning(
                    "No ticket ID found in webhook payload zendesk_invocation_id=%s",
                    invocation_id,
                )
                return {"success": False, "error": "No ticket ID"}

            messages = self._parse_webhook_event(payload)
            if not messages:
                logger.debug(
                    "No messages to post for ticket #%s zendesk_invocation_id=%s",
                    ticket_id,
                    invocation_id,
                )
                return {"success": True, "skipped": True}

            thread_info = self.thread_store.get_thread_info(ticket_id)
            if not thread_info:
                logger.info(
                    "No Slack thread found for ticket #%s zendesk_invocation_id=%s - skipping",
                    ticket_id,
                    invocation_id,
                )
                return {"success": True, "skipped": True}

            failed_posts = 0
            for message in messages:
                success = self._post_to_slack_thread(
                    channel_id=thread_info["channel_id"],
                    thread_ts=thread_info["thread_ts"],
                    message=message,
                )
                if not success:
                    failed_posts += 1

            if failed_posts:
                logger.error(
                    "Failed to post %s of %s Slack update(s) for ticket #%s zendesk_invocation_id=%s",
                    failed_posts,
                    len(messages),
                    ticket_id,
                    invocation_id,
                )
                return {"success": False, "error": "Failed to post one or more Slack thread updates"}

            logger.info(
                "Posted %s update(s) to Slack for ticket #%s zendesk_invocation_id=%s",
                len(messages),
                ticket_id,
                invocation_id,
            )
            return {"success": True, "messages_posted": len(messages)}

        except Exception as exc:
            logger.error("Error handling Zendesk webhook: %s", exc, exc_info=True)
            return {"success": False, "error": str(exc)}

    def _extract_ticket_id(self, payload: Dict[str, Any]) -> Optional[int]:
        """Extract ticket ID from Zendesk webhook payload."""
        ticket_id = payload.get("ticket_id")
        if ticket_id:
            return int(ticket_id)

        ticket = payload.get("ticket", {})
        if ticket and ticket.get("id"):
            return int(ticket["id"])

        return None

    def _parse_webhook_event(self, payload: Dict[str, Any]) -> List[str]:
        """Parse Zendesk webhook events into Slack message strings."""
        messages: List[str] = []
        seen_messages = set()

        def _format_attachments(attachments: Any) -> List[str]:
            formatted: List[str] = []
            for attachment in attachments or []:
                name = (
                    attachment.get("file_name")
                    or attachment.get("name")
                    or attachment.get("filename")
                    or "attachment"
                )
                url = (
                    attachment.get("content_url")
                    or attachment.get("content_url_https")
                    or attachment.get("content_url_http")
                    or attachment.get("url")
                    or attachment.get("attachment_url")
                    or attachment.get("public_url")
                )
                if url:
                    formatted.append(f"<{url}|{name}>")
                else:
                    formatted.append(name)
            return formatted

        def _process_comment_obj(comment_obj: Optional[Dict[str, Any]]) -> None:
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
            author_display = (
                author_name
                or (
                    comment_obj.get("author", {}).get("name")
                    if isinstance(comment_obj.get("author"), dict)
                    else ""
                )
                or "Unknown"
            )

            if not body:
                return

            if is_public:
                author_email = comment_obj.get("author_email", "") or (
                    comment_obj.get("author", {}).get("email")
                    if isinstance(comment_obj.get("author"), dict)
                    else ""
                )
                if author_email:
                    prefix = f"Comment from {author_display} ({author_email}):\n"
                else:
                    prefix = f"Comment from {author_display}:\n"
            else:
                prefix = f"Internal note from {author_display}:\n"

            message = f"{prefix}{body}"
            if attach_links:
                message += "\n\nAttachments:\n" + "\n".join(f"- {link}" for link in attach_links)
            if message in seen_messages:
                return
            seen_messages.add(message)
            messages.append(message)

        current_comment = payload.get("current_comment") or payload.get("comment")
        if current_comment:
            _process_comment_obj(current_comment)

        audit = payload.get("audit") or payload.get("audits") or payload.get("event") or payload.get("ticket_audit")
        if audit:
            events: List[Dict[str, Any]] = []
            if isinstance(audit, dict):
                events = audit.get("events", [])
            elif isinstance(audit, list):
                for audit_item in audit:
                    if isinstance(audit_item, dict):
                        events.extend(audit_item.get("events", []) or [])
            for event in events:
                if event.get("type", "").lower() == "comment":
                    _process_comment_obj(event.get("comment") or event)

        ticket = payload.get("ticket") or {}
        if isinstance(ticket, dict) and ticket.get("comment"):
            _process_comment_obj(ticket.get("comment"))

        return messages

    def _post_to_slack_thread(self, channel_id: str, thread_ts: str, message: str) -> bool:
        """Post a message to a Slack thread."""
        try:
            self.client.chat_postMessage(channel=channel_id, thread_ts=thread_ts, text=message)
            return True
        except SlackApiError as exc:
            logger.error("Failed to post to Slack thread: %s", exc)
            return False
