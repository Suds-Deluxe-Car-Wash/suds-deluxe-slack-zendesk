# Slack-Zendesk Integration

Automated integration that creates Zendesk tickets from Slack messages using Message Shortcuts. Users can click "..." on any message in configured channels and select "Create Zendesk Ticket" to automatically create a support ticket with the message content.

## Features

- 🎫 **Message Shortcuts**: Create Zendesk tickets directly from Slack messages via the "..." menu
- 📝 **Workflow Message Parsing**: Automatically extracts structured data from Slack workflow form messages
- 🎯 **Custom Forms**: Uses pre-configured Zendesk ticket forms with custom fields
- 🔗 **Thread Responses**: Posts ticket links back to Slack threads for easy tracking
- 🔒 **Channel Restrictions**: Only works in configured channels for security
- ✅ **User Feedback**: Sends ephemeral messages to users confirming ticket creation

## Architecture

```
Slack Message Shortcut → Webhook Server → Slack Handler → Zendesk Handler → Zendesk API
                                    ↓
                              Thread Response
```

## Prerequisites

1. **Slack Workspace** with admin permissions to create apps
2. **Zendesk Account** with API access and a pre-configured ticket form
3. **Python 3.8+** installed
4. **Public HTTPS URL** for webhook (ngrok for local testing, or cloud deployment)

## Setup Instructions

### 1. Clone and Install Dependencies

```bash
# Navigate to project directory
cd /workspaces/suds-deluxe-slack-zendesk

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Zendesk

1. Log into your Zendesk Admin panel
2. Navigate to **Admin** → **Objects and rules** → **Tickets** → **Forms**
3. Create or locate your ticket form and note the **Form ID**
4. Configure custom fields in the form as needed
5. Navigate to **Admin** → **Apps and integrations** → **APIs** → **Zendesk API**
6. Enable token access and create a new API token
7. Save your subdomain, email, and API token

### 3. Create Slack App

1. Go to [Slack API Apps](https://api.slack.com/apps)
2. Click **Create New App** → **From scratch**
3. Name: "Zendesk Custom Ticket Creator"
4. Select your workspace

#### Configure Message Shortcut:
1. In the app settings, go to **Interactivity & Shortcuts**
2. Turn on **Interactivity**
3. Set **Request URL** to: `https://your-domain.com/slack/events` (update after deployment)
4. Click **Create New Shortcut** → **On messages**
5. Name: `Create Custom Zendesk Ticket`
6. Short Description: `Create a custom Zendesk support ticket from this message`
7. Callback ID: `create_custom_zendesk_ticket`

#### Configure OAuth & Permissions:
1. Go to **OAuth & Permissions**
2. Add these **Bot Token Scopes**:
   - `chat:write` - Post messages to channels
   - `channels:history` - View messages in public channels
   - `users:read` - View user information
   - `team:read` - View workspace name
3. Click **Install to Workspace**
4. Copy the **Bot User OAuth Token** (starts with `xoxb-`)

#### Get Signing Secret:
1. Go to **Basic Information**
2. Scroll to **App Credentials**
3. Copy the **Signing Secret**

### 4. Configure Environment Variables

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your credentials
nano .env  # or use your preferred editor
```

Fill in these values in `.env`:
```env
# Slack Configuration
SLACK_BOT_TOKEN=xoxb-your-actual-bot-token
SLACK_SIGNING_SECRET=your-actual-signing-secret

# Zendesk Configuration
ZENDESK_SUBDOMAIN=your-company
ZENDESK_EMAIL=admin@yourcompany.com
ZENDESK_API_TOKEN=your-zendesk-api-token
ZENDESK_TICKET_FORM_ID=12345678

# Server Configuration
PORT=3000
ENVIRONMENT=development
LOG_LEVEL=INFO
```

### 5. Configure Channel Mappings

Edit `config/channel_mappings.json` to add your 4 allowed Slack channels:

```json
{
  "description": "Maps Slack channel IDs to Zendesk configuration",
  "allowed_channels": [
    {
      "channel_id": "C01234ABCD",
      "channel_name": "support-requests",
      "description": "Main support channel"
    }
  ]
}
```

**To get Channel IDs:**
1. Right-click on a channel in Slack
2. Select **View channel details**
3. Scroll to bottom and copy the **Channel ID**

### 6. Run the Application

#### Local Development (with ngrok):

```bash
# Terminal 1: Start ngrok tunnel
ngrok http 3000

# Copy the HTTPS URL (e.g., https://abc123.ngrok.io)
# Update Slack App Request URL to: https://abc123.ngrok.io/slack/events

# Terminal 2: Run the application
python run.py
```

#### Production Deployment:

```bash
# Using gunicorn (production WSGI server)
gunicorn -w 4 -b 0.0.0.0:3000 src.app:flask_app
```

## Usage

1. Navigate to one of your configured Slack channels
2. Find a message (preferably from a Slack workflow form)
3. Click the **"..."** menu on the message
4. Select **"Create Zendesk Ticket"**
5. A Zendesk ticket will be created and the link posted to the thread
6. You'll receive an ephemeral confirmation message

## Project Structure

```
suds-deluxe-slack-zendesk/
├── src/
│   ├── app.py                 # Flask webhook server
│   ├── slack_handler.py       # Slack message parsing and API
│   ├── zendesk_handler.py     # Zendesk ticket creation
│   └── config.py              # Configuration management
├── config/
│   └── channel_mappings.json  # Channel allowlist
├── requirements.txt           # Python dependencies
├── .env.example              # Environment template
├── run.py                    # Application entry point
└── README.md                 # This file
```

## Customization

### Parsing Workflow Messages

The `slack_handler.py` includes a flexible parser that handles:
- **Block-based messages** (from Slack workflow builder)
- **Plain text messages** with key:value pairs

To customize field mapping, edit the `_extract_field_from_text()` method in `slack_handler.py`.

### Custom Fields

To map Slack data to Zendesk custom fields:

1. Get custom field IDs from Zendesk Admin
2. In `slack_handler.py`, modify the `handle_message_shortcut()` method to pass a `custom_fields` dictionary:

```python
custom_fields = {
    "360001234567": parsed_data.get("priority"),  # Example field ID
    "360007654321": parsed_data.get("category")
}

ticket_result = self.zendesk_handler.create_ticket_from_slack_message(
    parsed_data,
    custom_fields=custom_fields
)
```

## Troubleshooting

### "Missing required environment variables"
- Ensure you've copied `.env.example` to `.env`
- Verify all values are filled in (no placeholder text)

### "This integration is not enabled for this channel"
- Check that the channel ID is in `config/channel_mappings.json`
- Verify the channel ID is correct (right-click channel → View details)

### Slack doesn't show the shortcut
- Verify the Slack app is installed to the workspace
- Check that the callback ID is exactly `create_zendesk_ticket`
- Reinstall the Slack app if needed

### Ticket creation fails
- Verify Zendesk credentials in `.env`
- Check that `ZENDESK_TICKET_FORM_ID` is valid
- Review logs for specific error messages

### Webhook not receiving requests
- Ensure ngrok or your server is running
- Verify the Request URL in Slack app matches your endpoint
- Check firewall/network settings

## Development

### Run Tests
```bash
# Install dev dependencies
pip install pytest pytest-cov

# Run tests (when implemented)
pytest tests/
```

### View Logs
```bash
# Application logs are written to stdout
# In production, redirect to file or logging service
python run.py 2>&1 | tee app.log
```

## Deployment Options

- **AWS Lambda** with API Gateway (serverless)
- **Google Cloud Run** (containerized)
- **Heroku** (platform-as-a-service)
- **Docker** + any container platform
- **Traditional VM** with systemd service

## Security Notes

- Never commit `.env` file (already in `.gitignore`)
- Rotate API tokens regularly
- Use HTTPS for all webhook endpoints
- Validate Slack request signatures (handled by `slack-bolt`)
- Limit channel access via `channel_mappings.json`

## License

Proprietary - Suds Deluxe Car Wash

## Support

For issues or questions, contact your development team.
