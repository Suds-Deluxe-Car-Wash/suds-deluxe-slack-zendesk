"""Zendesk ticket creation and management."""
import logging
import re
from typing import Dict, Any, Optional
from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket, CustomField, Comment, User
from src.config import Config

logger = logging.getLogger(__name__)


class ZendeskHandler:
    """Handles Zendesk API interactions for ticket creation."""
    
    def __init__(self):
        """Initialize Zendesk client with credentials from config."""
        try:
            self.client = Zenpy(
                subdomain=Config.ZENDESK_SUBDOMAIN,
                email=Config.ZENDESK_EMAIL,
                token=Config.ZENDESK_API_TOKEN
            )
            # Import WebClient for user name resolution
            from slack_sdk import WebClient
            self.slack_client = WebClient(token=Config.SLACK_BOT_TOKEN)
            logger.info("Zendesk client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Zendesk client: {e}")
            raise
    
    def create_ticket_from_slack_message(
        self,
        message_data: Dict[str, Any],
        custom_fields: Optional[Dict[str, Any]] = None,
        ticket_form_id: Optional[str] = None,
        group_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Create a Zendesk ticket from Slack message data.
        
        Args:
            message_data: Parsed data from Slack workflow message
            custom_fields: Dictionary of custom field IDs to values
            ticket_form_id: Optional override for ticket form ID
            group_id: Optional Zendesk group ID to assign ticket to
        
        Returns:
            Dictionary with ticket_id and ticket_url
        """
        try:
            # Build ticket description from Slack message
            description = self._build_ticket_description(message_data)
            
            # Prepare custom fields if provided
            custom_field_objects = []
            if custom_fields:
                custom_field_objects = [
                    CustomField(id=field_id, value=value)
                    for field_id, value in custom_fields.items()
                ]
            
            # Use provided form ID (required since we removed it from .env)
            if not ticket_form_id:
                raise ValueError("ticket_form_id is required")
            
            # Create ticket object
            ticket = Ticket(
                subject=message_data.get("subject", "Ticket from Slack"),
                description=description,
                priority=message_data.get("priority", "normal"),
                ticket_form_id=int(ticket_form_id),
                custom_fields=custom_field_objects if custom_field_objects else None,
                requester=User(name="Slack Automation", email=Config.ZENDESK_AUTOMATION_EMAIL),
                group_id=group_id,
                tags=["slack", "automated"]
            )
            
            # Submit ticket to Zendesk
            ticket_audit = self.client.tickets.create(ticket)
            
            # Extract the ticket from the audit response
            created_ticket = ticket_audit.ticket
            
            logger.info(f"Created Zendesk ticket #{created_ticket.id}")
            
            return {
                "ticket_id": created_ticket.id,
                "ticket_url": f"https://{Config.ZENDESK_SUBDOMAIN}.zendesk.com/agent/tickets/{created_ticket.id}",
                "success": True
            }
            
        except Exception as e:
            logger.error(f"Failed to create Zendesk ticket: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _build_ticket_description(self, message_data: Dict[str, Any]) -> str:
        """
        Build ticket description from Slack message data.
        
        Args:
            message_data: Parsed Slack message data
        
        Returns:
            Formatted ticket description
        """
        description_parts = []
        
        # Add main description if available
        if "description" in message_data:
            description_parts.append(message_data["description"])
            description_parts.append("\n---\n")
        
        # Add Slack message link
        if "message_link" in message_data:
            description_parts.append(f"Slack Message: {message_data['message_link']}")
        
        # Add channel information
        if "channel_name" in message_data:
            description_parts.append(f"Channel: #{message_data['channel_name']}")
        
        # Add any additional fields from workflow
        if "additional_fields" in message_data:
            description_parts.append("\n**Additional Information:**")
            for key, value in message_data["additional_fields"].items():
                # Resolve Slack user mentions to actual names
                resolved_value = self._resolve_user_mentions(str(value))
                description_parts.append(f"- {key}: {resolved_value}")
        
        return "\n".join(description_parts)
    
    def _resolve_user_mentions(self, text: str) -> str:
        """
        Replace Slack user mention codes with actual usernames.
        
        Args:
            text: Text potentially containing Slack user mentions like <@U12345>
        
        Returns:
            Text with user mentions resolved to actual names
        """
        if '<@' not in text:
            return text
        
        # Find all user mentions
        user_ids = re.findall(r'<@([A-Z0-9]+)>', text)
        
        for user_id in user_ids:
            try:
                user_info = self.slack_client.users_info(user=user_id)
                username = user_info["user"]["profile"].get("display_name") or user_info["user"]["real_name"]
                text = text.replace(f'<@{user_id}>', username)
            except Exception as e:
                logger.warning(f"Could not resolve user {user_id}: {e}")
                # Leave the mention as-is if we can't resolve it
        
        return text
    
    def add_comment_to_ticket(self, ticket_id: int, comment_text: str) -> bool:
        """
        Add a comment to an existing ticket.
        
        Args:
            ticket_id: Zendesk ticket ID
            comment_text: Comment text to add
        
        Returns:
            True if successful, False otherwise
        """
        try:
            ticket = self.client.tickets(id=ticket_id)
            ticket.comment = Comment(body=comment_text, public=False)
            self.client.tickets.update(ticket)
            logger.info(f"Added comment to ticket #{ticket_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to add comment to ticket #{ticket_id}: {e}")
            return False
