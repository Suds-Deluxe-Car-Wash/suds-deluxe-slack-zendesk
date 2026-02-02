"""Slack message shortcut handler and message parsing."""
import logging
import re
from typing import Dict, Any, Optional, List
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from src.config import Config, is_channel_allowed, get_form_config_for_channel
from src.zendesk_handler import ZendeskHandler
from src.thread_store import ThreadMappingStore

logger = logging.getLogger(__name__)


class SlackHandler:
    """Handles Slack API interactions and message parsing."""
    
    def __init__(self):
        """Initialize Slack client with bot token."""
        self.client = WebClient(token=Config.SLACK_BOT_TOKEN)
        self.zendesk_handler = ZendeskHandler()
        self.thread_store = ThreadMappingStore()
        logger.info("Slack client initialized successfully")
    
    def handle_workflow_message(self, message: Dict[str, Any], channel_id: str, user_id: str = None) -> Dict[str, Any]:
        """
        Handle workflow message and create Zendesk ticket.
        
        Args:
            message: Slack message object (from workflow or event)
            channel_id: Channel ID where message was posted
            user_id: Optional user ID (for ephemeral messages)
        
        Returns:
            Response dictionary with success status and message
        """
        try:
            message_ts = message.get("ts")
            
            # ATOMIC CLAIM: Reserve this thread BEFORE doing anything else
            # This prevents race conditions where multiple requests create tickets simultaneously
            if message_ts:
                claimed = self.thread_store.claim_thread(message_ts, channel_id)
                if not claimed:
                    # Another request already claimed this thread
                    # Get the existing ticket (may still be placeholder -1 if other request is in progress)
                    existing_ticket_id = self.thread_store.get_ticket_id(message_ts)
                    if existing_ticket_id and existing_ticket_id != -1:
                        logger.info(f"Ticket #{existing_ticket_id} already exists for message {message_ts}, preventing duplicate")
                        return {
                            "success": True,
                            "ticket_id": existing_ticket_id,
                            "ticket_url": f"https://{Config.ZENDESK_SUBDOMAIN}.zendesk.com/agent/tickets/{existing_ticket_id}",
                            "duplicate_prevented": True
                        }
                    else:
                        # Placeholder exists, another request is creating the ticket right now
                        logger.info(f"Another request is creating ticket for message {message_ts}, preventing duplicate")
                        return {
                            "success": True,
                            "duplicate_prevented": True,
                            "message": "Ticket creation in progress by another request"
                        }
            
            # Validate channel is allowed
            if not is_channel_allowed(channel_id):
                logger.warning(f"Workflow message in unauthorized channel: {channel_id}")
                return {
                    "success": False,
                    "error": "This integration is not enabled for this channel."
                }
            
            # Get form configuration for this channel
            form_config = get_form_config_for_channel(channel_id)
            if not form_config:
                logger.error(f"No form configuration found for channel {channel_id}")
                return {
                    "success": False,
                    "error": "No form configuration found for this channel."
                }
            
            logger.info(f"Using form: {form_config['name']}")
            
            # Parse the Slack workflow message
            parsed_data = self.parse_workflow_message(message, channel_id)
            
            if not parsed_data:
                return {
                    "success": False,
                    "error": "Could not parse message data. Please ensure this is a workflow form message."
                }
            
            # Build custom fields mapping for Zendesk using form config
            custom_fields = self._build_zendesk_custom_fields(parsed_data, form_config)
            
            # Build subject using form template
            subject = self._build_ticket_subject(parsed_data, form_config)
            parsed_data["subject"] = subject
            
            # Determine group assignment based on issue type
            group_id = self._determine_group(parsed_data, form_config)
            
            # Create Zendesk ticket with custom fields and specific form
            ticket_result = self.zendesk_handler.create_ticket_from_slack_message(
                parsed_data,
                custom_fields=custom_fields,
                ticket_form_id=form_config.get("zendesk_form_id"),
                group_id=group_id
            )
            
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
            
            # Update the claimed thread with the actual ticket ID
            if message_ts:
                updated = self.thread_store.update_ticket_mapping(
                    thread_ts=message_ts,
                    ticket_id=ticket_result["ticket_id"]
                )
                
                if not updated:
                    logger.error(f"Failed to update ticket mapping for {message_ts} - this should not happen!")
            
            logger.info(f"Successfully created ticket #{ticket_result['ticket_id']} from Slack workflow using form '{form_config['name']}'")
            
            return {
                "success": True,
                "ticket_id": ticket_result["ticket_id"],
                "ticket_url": ticket_result["ticket_url"]
            }
            
        except Exception as e:
            logger.error(f"Error handling workflow message: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def handle_message_shortcut(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle message shortcut callback from Slack (deprecated - kept for backward compatibility).
        
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
            
            # Use the common workflow message handler
            return self.handle_workflow_message(message, channel_id, user_id)
            
        except Exception as e:
            logger.error(f"Error handling message shortcut: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def _build_zendesk_custom_fields(
        self, 
        parsed_data: Dict[str, Any],
        form_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Build Zendesk custom fields dictionary from parsed Slack data.
        
        Args:
            parsed_data: Parsed data from Slack workflow message
            form_config: Form configuration with field mappings
        
        Returns:
            Dictionary mapping Zendesk field IDs to values
        """
        custom_fields = {}
        additional_fields = parsed_data.get("additional_fields", {})
        field_mappings = form_config.get("field_mappings", {})
        
        logger.info(f"Parsed fields from Slack: {list(additional_fields.keys())}")
        
        for slack_field_name, zendesk_field_id in field_mappings.items():
            if slack_field_name in additional_fields:
                value = additional_fields[slack_field_name]
                
                # Handle Slack user mentions - replace with actual username
                if '<@' in value:
                    # Extract user IDs and replace with usernames
                    user_ids = re.findall(r'<@([A-Z0-9]+)>', value)
                    for user_id in user_ids:
                        username = self._get_user_name(user_id)
                        value = value.replace(f'<@{user_id}>', username)
                
                value = value.strip()
                
                # Don't send empty values to Zendesk
                if value:
                    custom_fields[zendesk_field_id] = value
                    logger.info(f"[{form_config['name']}] Mapped '{slack_field_name}' → Field ID {zendesk_field_id}: {value}")
                else:
                    logger.warning(f"[{form_config['name']}] Field '{slack_field_name}' has empty value, skipping")
            else:
                logger.warning(f"[{form_config['name']}] Field '{slack_field_name}' not found in Slack message")
        
        logger.info(f"Total custom fields mapped: {len(custom_fields)}")
        return custom_fields
    
    def _build_ticket_subject(
        self,
        parsed_data: Dict[str, Any],
        form_config: Dict[str, Any]
    ) -> str:
        """
        Build ticket subject using form's subject template.
        
        Args:
            parsed_data: Parsed data from Slack workflow message
            form_config: Form configuration
        
        Returns:
            Formatted subject line
        """
        subject_template = form_config.get("subject_template", "Ticket from Slack")
        additional_fields = parsed_data.get("additional_fields", {})
        
        # Replace placeholders in template with actual values
        subject = subject_template
        for field_name, value in additional_fields.items():
            placeholder = f"{{{field_name}}}"
            if placeholder in subject:
                subject = subject.replace(placeholder, str(value))
        
        # If template still has unreplaced placeholders, use default
        if "{" in subject:
            return f"Customer Issue from {parsed_data.get('channel_name', 'Slack')}"
        
        return subject
    
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
            # TEMPORARY DEBUG - Remove after fixing field mapping
            import json
            logger.info("=" * 80)
            logger.info("RAW SLACK MESSAGE:")
            logger.info(json.dumps(message, indent=2))
            logger.info("=" * 80)
            
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
            
            logger.info(f"PARSED DATA - additional_fields: {parsed_data.get('additional_fields')}")
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"Error parsing workflow message: {e}", exc_info=True)
            return None
    
    def add_thread_reply_to_ticket(self, message_event: Dict[str, Any]) -> bool:
        """Add a Slack thread reply as an internal note to the corresponding Zendesk ticket.
        
        Args:
            message_event: Slack message event from a thread reply
            
        Returns:
            True if comment added successfully, False otherwise
        """
        try:
            thread_ts = message_event.get("thread_ts")
            user_id = message_event.get("user")
            text = message_event.get("text", "")
            
            # Get ticket ID from thread mapping
            ticket_id = self.thread_store.get_ticket_id(thread_ts)
            if not ticket_id:
                logger.warning(f"No ticket found for thread {thread_ts}")
                return False
            
            # Get user's display name
            user_name = self._get_user_name(user_id)
            
            # Replace Slack user mentions with actual usernames
            if '<@' in text:
                user_ids = re.findall(r'<@([A-Z0-9]+)>', text)
                for mentioned_user_id in user_ids:
                    mentioned_username = self._get_user_name(mentioned_user_id)
                    text = text.replace(f'<@{mentioned_user_id}>', f'@{mentioned_username}')
            
            # Format comment with username, message, and signature to prevent webhook loops
            comment_text = f"💬 Thread Reply from {user_name}:\n\n{text}\n\n---\n[Posted from Slack]"
            
            # Add as internal note to Zendesk ticket
            success = self.zendesk_handler.add_comment_to_ticket(ticket_id, comment_text)
            
            if success:
                logger.info(f"Added thread reply to ticket #{ticket_id} from user {user_name}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to add thread reply to ticket: {e}")
            return False
    
    def _determine_group(self, parsed_data: Dict[str, Any], form_config: Dict[str, Any]) -> Optional[int]:
        """Determine Zendesk group ID based on field values.
        
        Args:
            parsed_data: Parsed Slack message data
            form_config: Form configuration with group mappings
            
        Returns:
            Zendesk group ID or None
        """
        try:
            group_mappings = form_config.get("group_mappings")
            if not group_mappings:
                return None
            
            field_name = group_mappings.get("field_name")
            rules = group_mappings.get("rules", {})
            
            # Get the field value from parsed data
            field_value = parsed_data.get("additional_fields", {}).get(field_name)
            
            if not field_value:
                logger.warning(f"Field '{field_name}' not found for group assignment")
                return int(rules.get("default")) if rules.get("default") else None
            
            # Check if there's a specific rule for this value
            group_id = rules.get(field_value)
            
            # Fall back to default if no specific rule found
            if not group_id:
                group_id = rules.get("default")
            
            if group_id:
                logger.info(f"Assigning to group {group_id} based on '{field_name}' = '{field_value}'")
                return int(group_id)
            
            return None
            
        except Exception as e:
            logger.error(f"Error determining group: {e}")
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
            
            # Rich text blocks (Slack workflow builder format)
            elif block_type == "rich_text":
                self._parse_rich_text_block(block, parsed)
            
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
    
    def _parse_rich_text_block(self, block: Dict[str, Any], parsed: Dict[str, Any]) -> None:
        """
        Parse a rich_text block from Slack workflow builder.
        
        Slack workflow messages use rich_text blocks where:
        - Bold text is the field name
        - Non-bold text after it is the field value
        
        Args:
            block: Rich text block object
            parsed: Dictionary to update with extracted fields
        """
        elements = block.get("elements", [])
        
        for element in elements:
            if element.get("type") == "rich_text_section":
                section_elements = element.get("elements", [])
                
                current_field = None
                current_value = []
                
                for elem in section_elements:
                    elem_type = elem.get("type")
                    
                    # Bold text is a field name
                    if elem_type == "text" and elem.get("style", {}).get("bold"):
                        # Save previous field
                        if current_field and current_value:
                            value = " ".join(current_value).strip()
                            parsed["additional_fields"][current_field] = value
                        
                        # Start new field
                        current_field = elem.get("text", "").strip()
                        current_value = []
                    
                    # Regular text is a value
                    elif elem_type == "text" and current_field:
                        text = elem.get("text", "")
                        if text.strip() and text != "\n":
                            current_value.append(text.strip())
                    
                    # User mention
                    elif elem_type == "user" and current_field:
                        user_id = elem.get("user_id", "")
                        current_value.append(f"<@{user_id}>")
                
                # Save last field
                if current_field and current_value:
                    value = " ".join(current_value).strip()
                    parsed["additional_fields"][current_field] = value
    
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
                        "text": ":ticket: *Zendesk Ticket Created*"
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
