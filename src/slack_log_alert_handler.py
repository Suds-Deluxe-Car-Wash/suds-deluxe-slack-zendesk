"""Logging handler that sends ERROR/CRITICAL records to Slack."""
import logging
import os
import socket
import traceback
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class SlackLogAlertHandler(logging.Handler):
    """Forward selected log records to a Slack channel."""

    def __init__(self, token: str, channel: str, level: int = logging.ERROR):
        super().__init__(level=level)
        self.client = WebClient(token=token)
        self.channel = channel
        self.service_name = os.getenv("RENDER_SERVICE_NAME", "unknown-service")
        self.instance_id = os.getenv("RENDER_INSTANCE_ID", "unknown-instance")
        self.environment = os.getenv("ENVIRONMENT", "unknown")
        self.hostname = socket.gethostname()

    def emit(self, record: logging.LogRecord) -> None:
        # Prevent recursion if Slack client itself logs errors.
        if record.name.startswith("slack_sdk") or record.name.startswith(__name__):
            return

        try:
            message = self._format_message(record)
            self.client.chat_postMessage(channel=self.channel, text=message)
        except SlackApiError:
            # Avoid logging from inside a logging handler to prevent loops.
            pass
        except Exception:
            pass

    def _format_message(self, record: logging.LogRecord) -> str:
        level = record.levelname
        logger_name = record.name
        log_message = record.getMessage()
        ts = self.formatter.formatTime(record) if self.formatter else ""

        lines = [
            f":rotating_light: *{level}* in Render service",
            f"*Service:* `{self.service_name}`",
            f"*Instance:* `{self.instance_id}`",
            f"*Env:* `{self.environment}`",
            f"*Host:* `{self.hostname}`",
            f"*Logger:* `{logger_name}`",
        ]

        if ts:
            lines.append(f"*Time:* `{ts}`")

        lines.append(f"*Message:* `{log_message}`")

        if record.exc_info:
            exc_text = "".join(traceback.format_exception(*record.exc_info))
            truncated_exc = exc_text[:2500]
            lines.append("```")
            lines.append(truncated_exc)
            lines.append("```")

        return "\n".join(lines)
