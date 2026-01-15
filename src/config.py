"""Configuration management for Slack-Zendesk integration."""
import os
import json
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Application configuration loaded from environment variables."""
    
    # Slack Configuration
    SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
    SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")
    
    # Zendesk Configuration
    ZENDESK_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN")
    ZENDESK_EMAIL = os.getenv("ZENDESK_EMAIL")
    ZENDESK_API_TOKEN = os.getenv("ZENDESK_API_TOKEN")
    ZENDESK_TICKET_FORM_ID = os.getenv("ZENDESK_TICKET_FORM_ID")
    
    # Server Configuration
    PORT = int(os.getenv("PORT", "3000"))
    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    @classmethod
    def validate(cls) -> List[str]:
        """Validate that all required configuration is present."""
        missing = []
        required_vars = [
            "SLACK_BOT_TOKEN",
            "SLACK_SIGNING_SECRET",
            "ZENDESK_SUBDOMAIN",
            "ZENDESK_EMAIL",
            "ZENDESK_API_TOKEN",
            "ZENDESK_TICKET_FORM_ID"
        ]
        
        for var in required_vars:
            if not getattr(cls, var):
                missing.append(var)
        
        return missing
    
    @classmethod
    def is_valid(cls) -> bool:
        """Check if configuration is valid."""
        return len(cls.validate()) == 0


def load_channel_mappings() -> Dict[str, Any]:
    """Load channel mappings from JSON configuration file."""
    config_path = Path(__file__).parent.parent / "config" / "channel_mappings.json"
    
    try:
        with open(config_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        raise Exception(f"Channel mappings file not found at {config_path}")
    except json.JSONDecodeError as e:
        raise Exception(f"Invalid JSON in channel mappings: {e}")


def get_allowed_channel_ids() -> List[str]:
    """Get list of allowed Slack channel IDs."""
    mappings = load_channel_mappings()
    return [channel["channel_id"] for channel in mappings.get("allowed_channels", [])]


def is_channel_allowed(channel_id: str) -> bool:
    """Check if a channel ID is in the allowed list."""
    return channel_id in get_allowed_channel_ids()
