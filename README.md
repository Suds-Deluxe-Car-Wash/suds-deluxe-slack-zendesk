# Slack-Zendesk Integration

> **Note**: This integration was built for Suds Deluxe Car Wash. Company-specific IDs and configurations are included but are non-sensitive. All API credentials are externalized via environment variables.

Automated bidirectional integration between Slack and Zendesk that creates support tickets from Slack Workflow Builder forms and syncs updates between both platforms. Tickets are automatically created when users submit workflow forms in configured channels, with thread replies synced as internal notes.

## Features

- 🤖 **Automatic Ticket Creation**: Workflow form submissions automatically create Zendesk tickets (no manual button clicks needed)
- 🔄 **Bidirectional Sync**: 
  - Slack thread replies → Zendesk internal notes
  - Zendesk comments/notes → Slack thread messages
- 📝 **Custom Field Mapping**: Automatically maps Slack workflow fields to Zendesk custom fields
- 👤 **User Mention Resolution**: Converts Slack user IDs (`<@U123>`) to actual names
- 🎯 **Smart Group Assignment**: Auto-assigns tickets to teams based on issue type
- 🔗 **Thread Tracking**: Posts ticket links to Slack threads with persistent mapping
- 🔒 **Channel Security**: Only processes messages from configured channels
- 🛡️ **Loop Prevention**: Prevents infinite sync loops with message signatures
- 💾 **PostgreSQL Storage**: Persistent thread-to-ticket mappings with automatic cleanup

## Architecture

```
Slack Workflow Form
        ↓
   Auto-Detection
        ↓
  Message Parser → Custom Field Mapper → Zendesk Ticket Creation
        ↓                                        ↓
  Thread Mapping ←─────────────────── Ticket Link Posted
        ↓
Thread Replies → Internal Notes (with signature)
        ↑
Zendesk Updates ← Webhook Handler (loop prevention)
```

## Tech Stack

- **Python 3.8+**: Core application
- **Flask 3.0.0**: Webhook server
- **Slack Bolt 1.18.0**: Slack API integration
- **Zenpy 2.0.50**: Zendesk API client
- **PostgreSQL**: Thread mapping storage (Supabase)
- **Gunicorn 21.2.0**: Production WSGI server
- **Render.com**: Deployment platform

## Prerequisites

1. **Slack Workspace** with admin permissions to create apps
2. **Zendesk Account** with API access and custom ticket forms
3. **PostgreSQL Database** (Supabase recommended - free tier)
4. **Python 3.8+** installed
5. **Public HTTPS URL** for webhooks (Render.com, ngrok, etc.)

## Quick Start

### 1. Clone and Install

```bash
git clone https://github.com/LeviJesus/suds-deluxe-slack-zendesk.git
cd suds-deluxe-slack-zendesk

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Set Up PostgreSQL Database

**Using Supabase (Recommended - Free):**
1. Go to [supabase.com](https://supabase.com) and create account
2. Create new project (save the database password)
3. Go to Settings → Database → Connection string → URI
4. Copy the connection string (format: `postgresql://postgres.xxxxx:[PASSWORD]@aws-0-us-east-1.pooler.supabase.com:6543/postgres`)

### 3. Configure Environment Variables

```bash
cp .env.example .env
nano .env  # Edit with your credentials
```

**Required environment variables:**
```env
# Slack Configuration
SLACK_BOT_TOKEN=xoxb-your-bot-token
SLACK_SIGNING_SECRET=your-signing-secret

# Zendesk Configuration
ZENDESK_SUBDOMAIN=yourcompany
ZENDESK_EMAIL=admin@yourcompany.com
ZENDESK_API_TOKEN=your-api-token
ZENDESK_AUTOMATION_EMAIL=slack-automation@yourcompany.com

# PostgreSQL Database
DATABASE_URL=postgresql://postgres.xxxxx:[PASSWORD]@aws-0-us-east-1.pooler.supabase.com:6543/postgres

# Server Configuration
PORT=3000
ENVIRONMENT=production
LOG_LEVEL=INFO

# Optional: Slack error log alerts (Render -> Slack)
SLACK_LOG_ALERTS_ENABLED=true
SLACK_LOG_ALERT_CHANNEL=C01234ABCD
SLACK_LOG_ALERT_LEVEL=ERROR
```

### 4. Configure Slack App

#### Create Slack App:
1. Go to [api.slack.com/apps](https://api.slack.com/apps) → **Create New App** → **From scratch**
2. Name: "Zendesk Integration" | Workspace: Select your workspace

#### OAuth & Permissions:
Add these **Bot Token Scopes**:
- `chat:write` - Post messages
- `channels:history` - Read channel messages
- `users:read` - View user information
- `team:read` - View workspace details

Install to workspace and copy the **Bot User OAuth Token**

#### Event Subscriptions:
1. Enable Events
2. Request URL: `https://your-domain.com/slack/events`
3. Subscribe to **Bot Events**:
   - `message.channels` - Listen to channel messages

#### Interactivity & Shortcuts (Optional - for manual fallback):
1. Enable Interactivity
2. Request URL: `https://your-domain.com/slack/events`
3. Create Message Shortcut:
   - Name: "Create Zendesk Ticket"
   - Callback ID: `create_custom_zendesk_ticket`

### 5. Configure Zendesk Webhooks

1. Zendesk Admin → Apps and integrations → Webhooks → Create webhook
2. **Endpoint URL**: `https://your-domain.com/zendesk/webhook`
3. **Method**: POST
4. **Format**: JSON
5. **Triggers**: Create trigger for ticket updates
   - Conditions: Ticket → Is updated
   - Actions: Notify webhook → Select your webhook

### 6. Configure Channel and Form Mappings

**Edit `config/channel_mappings.json`:**
```json
{
  "allowed_channels": [
    {
      "channel_id": "C01234ABCD",
      "channel_name": "customer-support",
      "form_key": "customer_issues_tracker"
    }
  ]
}
```

Get Channel IDs: Right-click channel → View details → Copy ID (at bottom)

**Edit `config/form_mappings.json`:**
```json
{
  "forms": {
    "customer_issues_tracker": {
      "name": "Customer Issues Tracker",
      "zendesk_form_id": "45508825246107",
      "field_mappings": {
        "Location": "45554499814299",
        "Customer Name": "43945003565979",
        "Customer Issue Type": "45508647518619"
      },
      "subject_template": "Customer Issue: {Customer Issue Type}",
      "group_mappings": {
        "field_name": "Customer Issue Type",
        "rules": {
          "Cancel": "42987935763867",
          "default": "42836454078875"
        }
      }
    }
  }
}
```

Get Zendesk Field IDs: Admin → Objects and rules → Tickets → Forms → Edit form → Inspect field IDs

## Deployment

### Deploy to Render.com (Recommended)

1. Create account at [render.com](https://render.com)
2. New Web Service → Connect your GitHub repo
3. Configure:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn -b 0.0.0.0:$PORT src.app:flask_app`
4. Add environment variables from `.env`
5. Deploy!

### Set Up Keepalive (Free Tier)

Render free tier sleeps after 15 minutes. Use cron-job.org:
1. Go to [cron-job.org](https://cron-job.org)
2. Create cron job: `https://your-app.onrender.com/health`
3. Schedule: Every 10 minutes

### Render Log Alerts to Slack

To receive Slack alerts when your app logs errors on Render:

1. Create or choose an alerts channel in Slack (example: `#render-alerts`)
2. Add your bot to that channel
3. In Render environment variables, set:
   - `SLACK_LOG_ALERTS_ENABLED=true`
   - `SLACK_LOG_ALERT_CHANNEL=<channel_id>`
   - `SLACK_LOG_ALERT_LEVEL=ERROR` (or `CRITICAL`)

When enabled, any Python log at or above the configured level is posted to the alert channel with service, instance, logger, timestamp, and message.

## Usage

### Automatic Ticket Creation

1. Create a Slack Workflow Builder form in a configured channel
2. Users submit the form
3. Ticket is automatically created in Zendesk
4. Ticket link posted to Slack thread
5. Thread replies sync as internal notes
6. Zendesk updates appear in Slack thread

### Workflow Form Requirements

For automatic detection, your Slack workflow should:
- Use Workflow Builder (posts as a bot)
- Include structured fields (bold field names)
- Match field names in `form_mappings.json`

## Project Structure

```
suds-deluxe-slack-zendesk/
├── src/
│   ├── app.py                      # Flask server & event handlers
│   ├── slack_handler.py            # Slack API & message parsing
│   ├── zendesk_handler.py          # Zendesk API & ticket creation
│   ├── zendesk_webhook_handler.py  # Webhook processing & loop prevention
│   ├── thread_store.py             # PostgreSQL thread mapping
│   └── config.py                   # Configuration management
├── config/
│   ├── channel_mappings.json       # Allowed channels
│   └── form_mappings.json          # Field mappings & templates
├── requirements.txt                # Python dependencies
├── run.py                          # Application entry point
└── README.md                       # Documentation
```

## Key Features Explained

### Automatic Workflow Detection

The system identifies Slack Workflow Builder messages by:
- Presence of `bot_profile` field
- Structured `rich_text` blocks
- Bot message subtype

When detected, tickets are created automatically without user intervention.

### User Mention Resolution

Slack user mentions (`<@U0A8W3T1136>`) are automatically converted to actual names in:
- Zendesk ticket descriptions
- Zendesk custom fields
- Thread reply comments

### Loop Prevention

Thread replies include a `[Posted from Slack]` signature. When Zendesk webhooks send updates back to Slack, comments with this signature are ignored to prevent infinite loops.

### Automatic Cleanup

Thread mappings older than 30 days are automatically deleted to prevent database bloat. This runs on every ticket creation.

## Troubleshooting

### Tickets Not Creating Automatically

- Check Render logs for workflow detection messages
- Verify workflow form uses Workflow Builder (not manual bot messages)
- Confirm channel ID in `channel_mappings.json`
- Check field names match exactly in `form_mappings.json`

### Thread Replies Not Syncing

- Verify `message.channels` event subscription in Slack app
- Check thread mapping exists in database
- Review logs for event processing messages

### Zendesk Updates Not Appearing in Slack

- Confirm webhook is configured in Zendesk
- Verify webhook URL is correct
- Check Zendesk webhook logs for delivery status
- Review Render logs for webhook receipt

### Database Connection Errors

- Verify `DATABASE_URL` is set in Render environment variables
- Test database connection string locally
- Check Supabase project is active

## Development

### Local Development with ngrok

```bash
# Terminal 1: Start ngrok
ngrok http 3000

# Terminal 2: Run app
python run.py

# Update Slack App Request URL to: https://xxxxx.ngrok.io/slack/events
```

### View Logs

```bash
# Local
python run.py

# Render.com
# Go to your service → Logs tab (live tail)
```

## Security

- ✅ All credentials in environment variables (never committed)
- ✅ `.env` file in `.gitignore`
- ✅ Slack request signature validation
- ✅ Channel allowlist prevents unauthorized access
- ✅ HTTPS-only webhooks
- ✅ PostgreSQL connections encrypted

## Performance

- **Cold Start**: ~5-10 seconds (Render free tier)
- **Ticket Creation**: ~2-3 seconds
- **Thread Reply Sync**: <1 second
- **Database Queries**: <100ms

## Limitations

- Free tier sleeps after 15 minutes (mitigated with keepalive)
- PostgreSQL connection pool: 1-10 connections
- Slack rate limits: 1 request/second per method
- Zendesk rate limits: 700 requests/minute

## Future Enhancements

- [ ] Multi-workspace support
- [ ] Attachment sync (Slack ↔ Zendesk)
- [ ] SLA tracking integration
- [ ] Analytics dashboard
- [ ] Email notification triggers

## Contributing

This is a proprietary project for Suds Deluxe Car Wash, but feel free to fork for your own use cases.

## License

Proprietary - Built by Levi Jesus for Suds Deluxe Car Wash

## Author

**Levi Jesus**  
GitHub: [@LeviJesus](https://github.com/LeviJesus)  
Project built January 2026

## Acknowledgments

- Slack Bolt SDK for Python
- Zenpy Zendesk Python client
- Render.com for deployment platform
- Supabase for PostgreSQL hosting

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
