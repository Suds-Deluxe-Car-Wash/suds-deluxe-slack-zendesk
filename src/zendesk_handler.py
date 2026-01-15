"""Zendesk ticket creation and management."""
import logging
from typing import Dict, Any, Optional
from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket, CustomField, Comment
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
            logger.info("Zendesk client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Zendesk client: {e}")
            raise
    
    def create_ticket_from_slack_message(
        self,
        message_data: Dict[str, Any],
        custom_fields: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a Zendesk ticket from Slack message data.
        
        Args:
            message_data: Parsed data from Slack workflow message
            custom_fields: Dictionary of custom field IDs to values
        
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
            
            # Create ticket object
            ticket = Ticket(
                subject=message_data.get("subject", "Ticket from Slack"),
                description=description,
                priority=message_data.get("priority", "normal"),
                ticket_form_id=int(Config.ZENDESK_TICKET_FORM_ID),
                custom_fields=custom_field_objects if custom_field_objects else None,
                tags=["slack", "automated"]
            )
            
            # Submit ticket to Zendesk
            created_ticket = self.client.tickets.create(ticket)
            
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
        
        # Add reporter information
        if "reporter_name" in message_data:
            description_parts.append(f"Reported by: {message_data['reporter_name']}")
        
        if "reporter_email" in message_data:
            description_parts.append(f"Email: {message_data['reporter_email']}")
        
        # Add channel information
        if "channel_name" in message_data:
            description_parts.append(f"Channel: #{message_data['channel_name']}")
        
        # Add any additional fields from workflow
        if "additional_fields" in message_data:
            description_parts.append("\n**Additional Information:**")
            for key, value in message_data["additional_fields"].items():
                description_parts.append(f"- {key}: {value}")
        
        return "\n".join(description_parts)
    
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
