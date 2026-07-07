# SmartLead Webhook Receiver

FastAPI receiver for SmartLead EMAIL_SENT and EMAIL_BOUNCE webhooks.

## Webhook Endpoints

Once deployed, you'll have these endpoints:

- **EMAIL_SENT:** `https://your-app.railway.app/webhooks/email-sent`
- **EMAIL_BOUNCE:** `https://your-app.railway.app/webhooks/email-bounce`
- **Health Check:** `https://your-app.railway.app/health`

## Deploy to Railway

### Option 1: Deploy from GitHub (Recommended)

1. **Push this code to a GitHub repository**
   ```bash
   cd /home/prateek/webhook-receiver
   git init
   git add .
   git commit -m "Initial webhook receiver"
   gh repo create smartlead-webhook-receiver --public --source=.
   git push -u origin main
   ```

2. **Deploy on Railway**
   - Go to [railway.app](https://railway.app)
   - Click **New Project** → **Deploy from GitHub repo**
   - Select your `smartlead-webhook-receiver` repository
   - Railway will auto-detect the `railway.toml` and `Dockerfile`
   - Click **Deploy**

3. **Get your URLs**
   - Once deployed, click your project
   - Go to **Settings** → **Domains**
   - Your public URL will be: `https://your-project-name.up.railway.app`
   - Add these paths:
     - EMAIL_SENT: `https://your-project-name.up.railway.app/webhooks/email-sent`
     - EMAIL_BOUNCE: `https://your-project-name.up.railway.app/webhooks/email-bounce`

### Option 2: Deploy via Railway CLI

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login
railway login

# Create project
railway init
railway up

# Get the URL
railway domain
```

### Option 3: Manual Deploy from Dashboard

1. Go to [railway.app](https://railway.app) and login
2. Click **New Project**
3. Click **Deploy from Dockerfile**
4. Upload these files:
   - `Dockerfile`
   - `requirements.txt`
   - `main.py`
5. Click **Deploy**

## Configure in SmartLead

Once you have your Railway URL:

1. Go to SmartLead → **Settings** → **Webhooks**
2. Add **EMAIL_SENT** webhook:
   - URL: `https://your-app.railway.app/webhooks/email-sent`
   - Events: `EMAIL_SENT`
3. Add **EMAIL_BOUNCE** webhook:
   - URL: `https://your-app.railway.app/webhooks/email-bounce`
   - Events: `EMAIL_BOUNCE`

## Test Webhooks

Use Railway logs to verify webhooks are received:

```bash
# View logs in Railway dashboard
railway logs

# Or use curl to test
curl -X POST https://your-app.railway.app/webhooks/email-sent \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "EMAIL_SENT",
    "from_email": "test@example.com",
    "to_email": "lead@example.com",
    "to_name": "Test Lead",
    "time_sent": "2025-01-15T09:00:00Z",
    "campaign_name": "Test Campaign",
    "campaign_id": 1,
    "sequence_number": 1,
    "custom_subject": "Test Subject",
    "custom_email_message": "<p>Test email body</p>",
    "message_id": "test-123"
  }'
```

## Current MVP Status

✅ FastAPI receiver with two endpoints
✅ Pydantic validation for webhook payloads
✅ Background task processing
✅ Health check endpoint
✅ Railway deployment config
✅ Detailed logging

## Next Phase (After Testing)

- Add Cloudflare R2 for raw JSON storage
- Add Neon PostgreSQL for searchable data
- Add background worker for processing
- Add idempotency (message_id unique constraint)
- Add error tracking (Sentry)
