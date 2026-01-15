"""Slack message shortcut handler and message parsing."""
import logging
import re
from typing import Dict, Any, Optional, List
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from src.config import Config, is_channel_allowed
from src.zendesk_handler import ZendeskHandler

logger = logging.getLogger(__name__)


class SlackHandler:
    """Handles Slack API interactions and message parsing."""
    
    def __init__(self):
        """Initialize Slack client with bot token."""
        self.client = WebClient(token=Config.SLACK_BOT_TOKEN)
        self.zendesk_handler = ZendeskHandler()
        logger.info("Slack client initialized successfully")
    
    def handle_message_shortcut(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle message shortcut callback from Slack.
        
        Args:
            payload: Message shortcut payload from Slack
        
        Returns:
            Response dictionary with success status and message
        """
        try:
            # Extract message and channel information
            message = payload.get("message", {})
            channel_id = payload.get("channel", {}).get("id")
            user_id = payload.get("user", {}).get("id")
            
            # Validate channel is allowed
            if not is_channel_allowed(channel_id):
                logger.warning(f"Message shortcut used in unauthorized channel: {channel_id}")
                return {
                    "success": False,
                    "error": "This integration is not enabled for this channel."
                }
            
            # Parse the Slack workflow message
            parsed_data = self.parse_workflow_message(message, channel_id)
            
            if not parsed_data:
                return {
                    "success": False,
                    "error": "Could not parse message data. Please ensure this is a workflow form message."
                }
            
            # Create Zendesk ticket
            ticket_result = self.zendesk_handler.create_ticket_from_slack_message(parsed_data)
            
            if not ticket_result.get("success"):
                return {
                    "success": False,
                    "error": f"Failed to create ticket: {ticket_result.get('error', 'Unknown error')}"
                }
            
            # Post ticket link back to Slack thread
            self.post_ticket_link_to_thread(
                channel_id=channel_id,
                thread_ts=message.get("ts"),
                ticket_id=ticket_result["ticket_id"],
                ticket_url=ticket_result["ticket_url"],
                user_id=user_id
            )
            
            logger.info(f"Successfully created ticket #{ticket_result['ticket_id']} from Slack message")
            
            return {
                "success": True,
                "ticket_id": ticket_result["ticket_id"],
                "ticket_url": ticket_result["ticket_url"]
            }
            
        except Exception as e:
            logger.error(f"Error handling message shortcut: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def parse_workflow_message(
        self,
        message: Dict[str, Any],
        channel_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Parse a Slack workflow form message to extract structured data.
        
        This function handles both block-based and text-based workflow messages.
        
        Args:
            message: Slack message object
            channel_id: Channel ID where message was posted
        
        Returns:
            Dictionary with parsed message data or None if parsing fails
        """
        try:
            parsed_data = {
                "message_link": self._build_message_link(channel_id, message.get("ts")),
                "reporter_name": self._get_user_name(message.get("user")),
                "channel_name": self._get_channel_name(channel_id),
                "additional_fields": {}
            }
            
            # Try parsing blocks first (structured workflow output)
            if "blocks" in message and message["blocks"]:
                parsed_data.update(self._parse_blocks(message["blocks"]))
            
            # Fall back to parsing text
            elif "text" in message:
                parsed_data.update(self._parse_text(message["text"]))
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"Error parsing workflow message: {e}", exc_info=True)
            return None
    
    def _parse_blocks(self, blocks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Parse Slack message blocks to extract field data.
        
        Args:
            blocks: List of Slack block objects
        
        Returns:
            Dictionary with parsed fields
        """
        parsed = {
            "subject": "",
            "description": "",
            "additional_fields": {}
        }
        
        for block in blocks:
            block_type = block.get("type")
            
            # Header blocks often contain the subject
            if block_type == "header":
                text = block.get("text", {}).get("text", "")
                if not parsed["subject"]:
                    parsed["subject"] = text
            
            # Section blocks contain field data
            elif block_type == "section":
                if "fields" in block:
                    # Multiple fields in a section
                    for field in block["fields"]:
                        field_text = field.get("text", "")
                        self._extract_field_from_text(field_text, parsed)
                elif "text" in block:
                    # Single field
                    field_text = block["text"].get("text", "")
                    self._extract_field_from_text(field_text, parsed)
        
        return parsed
    
    def _parse_text(self, text: str) -> Dict[str, Any]:
        """
        Parse plain text message to extract field data.
        
        Expects format like:
        Field Name: Value
        Another Field: Another Value
        
        Args:
            text: Plain text message content
        
        Returns:
            Dictionary with parsed fields
        """
        parsed = {
            "subject": "",
            "description": text,  # Use full text as description by default
            "additional_fields": {}
        }
        
        # Split by lines and look for key:value patterns
        lines = text.split("\n")
        for line in lines:
            self._extract_field_from_text(line, parsed)
        
        return parsed
    
    def _extract_field_from_text(self, text: str, parsed_dict: Dict[str, Any]) -> None:
        """
        Extract a field from a text line and add to parsed dictionary.
        
        Args:
            text: Text line to parse
            parsed_dict: Dictionary to update with extracted field
        """
        # Remove Slack markdown formatting
        clean_text = re.sub(r'\*([^*]+)\*', r'\1', text)  # Remove bold
        clean_text = re.sub(r'_([^_]+)_', r'\1', clean_text)  # Remove italic
        
        # Look for "Label: Value" pattern
        match = re.match(r'\s*([^:]+):\s*(.+)', clean_text)
        if match:
            label = match.group(1).strip()
            value = match.group(2).strip()
            
            # Map common labels to standard fields
            label_lower = label.lower()
            if label_lower in ["subject", "title", "summary"]:
                parsed_dict["subject"] = value
            elif label_lower in ["description", "details", "issue", "problem"]:
                parsed_dict["description"] = value
            elif label_lower in ["priority"]:
                parsed_dict["priority"] = value
            else:
                # Add to additional fields
                parsed_dict["additional_fields"][label] = value
    
    def post_ticket_link_to_thread(
        self,
        channel_id: str,
        thread_ts: str,
        ticket_id: int,
        ticket_url: str,
        user_id: str
    ) -> None:
        """
        Post Zendesk ticket link as a threaded reply in Slack.
        
        Args:
            channel_id: Slack channel ID
            thread_ts: Thread timestamp (message to reply to)
            ticket_id: Zendesk ticket ID
            ticket_url: Zendesk ticket URL
            user_id: User who triggered the shortcut
        """
        try:
            message_blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f":ticket: *Zendesk Ticket Created*\n<@{user_id}> created a support ticket from this message."
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Ticket ID:*\n#{ticket_id}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*View Ticket:*\n<{ticket_url}|Open in Zendesk>"
                        }
                    ]
                }
            ]
            
            self.client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                blocks=message_blocks,
                text=f"Zendesk ticket #{ticket_id} created: {ticket_url}"
            )
            
            logger.info(f"Posted ticket link to Slack thread in channel {channel_id}")
            
        except SlackApiError as e:
            logger.error(f"Error posting to Slack: {e.response['error']}")
    
    def _build_message_link(self, channel_id: str, message_ts: str) -> str:
        """Build a permanent link to a Slack message."""
        # Get workspace info to build proper link
        try:
            team_info = self.client.team_info()
            team_domain = team_info["team"]["domain"]
            
            # Convert timestamp to message ID format
            message_id = message_ts.replace(".", "")
            
            return f"https://{team_domain}.slack.com/archives/{channel_id}/p{message_id}"
        except Exception as e:
            logger.warning(f"Could not build message link: {e}")
            return f"Channel: {channel_id}, TS: {message_ts}"
    
    def _get_user_name(self, user_id: str) -> str:
        """Get user's display name from Slack."""
        try:
            user_info = self.client.users_info(user=user_id)
            return user_info["user"]["profile"].get("display_name") or user_info["user"]["real_name"]
        except Exception as e:
            logger.warning(f"Could not get user name: {e}")
            return user_id
    
    def _get_channel_name(self, channel_id: str) -> str:
        """Get channel name from Slack."""
        try:
            channel_info = self.client.conversations_info(channel=channel_id)
            return channel_info["channel"]["name"]
        except Exception as e:
            logger.warning(f"Could not get channel name: {e}")
            return channel_id
