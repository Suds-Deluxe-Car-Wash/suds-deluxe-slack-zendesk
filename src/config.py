"""Configuration management for Slack-Zendesk integration."""
import os
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
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
    ZENDESK_AUTOMATION_EMAIL = os.getenv("ZENDESK_AUTOMATION_EMAIL", "slack-automation@sudsdeluxecarwash.com")
    
    # Server Configuration
    PORT = int(os.getenv("PORT", "3000"))
    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Log Alert Configuration
    SLACK_LOG_ALERTS_ENABLED = os.getenv("SLACK_LOG_ALERTS_ENABLED", "false").lower() == "true"
    SLACK_LOG_ALERT_CHANNEL = os.getenv("SLACK_LOG_ALERT_CHANNEL")
    SLACK_LOG_ALERT_LEVEL = os.getenv("SLACK_LOG_ALERT_LEVEL", "ERROR").upper()
    
    @classmethod
    def validate(cls) -> List[str]:
        """Validate that all required configuration is present."""
        missing = []
        required_vars = [
            "SLACK_BOT_TOKEN",
            "SLACK_SIGNING_SECRET",
            "ZENDESK_SUBDOMAIN",
            "ZENDESK_EMAIL",
            "ZENDESK_API_TOKEN"
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


def load_form_mappings() -> Dict[str, Any]:
    """Load form mappings from JSON configuration file."""
    config_path = Path(__file__).parent.parent / "config" / "form_mappings.json"
    
    try:
        with open(config_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        raise Exception(f"Form mappings file not found at {config_path}")
    except json.JSONDecodeError as e:
        raise Exception(f"Invalid JSON in form mappings: {e}")


def get_allowed_channel_ids() -> List[str]:
    """Get list of allowed Slack channel IDs."""
    mappings = load_channel_mappings()
    return [channel["channel_id"] for channel in mappings.get("allowed_channels", [])]


def is_channel_allowed(channel_id: str) -> bool:
    """Check if a channel ID is in the allowed list."""
    return channel_id in get_allowed_channel_ids()


def get_form_config_for_channel(channel_id: str) -> Optional[Dict[str, Any]]:
    """Get form configuration for a specific channel."""
    channel_mappings = load_channel_mappings()
    form_mappings = load_form_mappings()
    
    # Find the channel
    for channel in channel_mappings.get("allowed_channels", []):
        if channel["channel_id"] == channel_id:
            form_key = channel.get("form_key")
            if form_key and form_key in form_mappings.get("forms", {}):
                return form_mappings["forms"][form_key]
    
    return None
