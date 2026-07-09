"""
SmartLead Webhook Receiver - Phase 2
Receives EMAIL_SENT and EMAIL_BOUNCE webhooks from SmartLead
Stores raw JSON to R2 and normalized data to Neon
"""
import os
import logging
import json
import re
import asyncio
import boto3
from datetime import datetime, timezone
from typing import Optional
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr, Field
import uvicorn
import asyncpg

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "smartlead-webhooks")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")

app = FastAPI(
    title="SmartLead Webhook Receiver",
    description="Receives and stores SmartLead EMAIL_SENT and EMAIL_BOUNCE webhooks",
    version="2.1.0"
)

# ============================================================================
# R2 Client (S3-compatible)
# ============================================================================

def get_r2_client():
    """Get R2/S3 client"""
    return boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto'
    )

# ============================================================================
# Neon Database Connection Pool
# ============================================================================

async def get_neon_connection():
    """Get Neon database connection"""
    return await asyncpg.connect(NEON_DATABASE_URL)

# ============================================================================
# Pydantic Models for Webhook Validation
# ============================================================================

class EmailSentEvent(BaseModel):
    """SmartLead EMAIL_SENT webhook payload"""
    event_type: str = Field(default="EMAIL_SENT")
    from_email: EmailStr
    to_email: EmailStr
    to_name: Optional[str] = None
    time_sent: datetime
    campaign_name: str
    campaign_id: int
    sequence_number: int
    custom_subject: Optional[str] = None
    custom_email_message: Optional[str] = None  # HTML body
    message_id: str

    class Config:
        json_schema_extra = {
            "example": {
                "event_type": "EMAIL_SENT",
                "from_email": "sender@company.com",
                "to_email": "lead@example.com",
                "to_name": "John Doe",
                "time_sent": "2025-01-15T09:00:00Z",
                "campaign_name": "Q1 Outreach",
                "campaign_id": 123,
                "sequence_number": 1,
                "custom_subject": "Quick question",
                "custom_email_message": "<html>...</html>",
                "message_id": "abc123def456"
            }
        }


class SentMessage(BaseModel):
    """Nested sent_message object in actual SmartLead bounce webhook"""
    message_id: str
    html: Optional[str] = None
    text: Optional[str] = None
    time: datetime


class BounceMessage(BaseModel):
    """Nested bounce_message object in actual SmartLead bounce webhook"""
    message_id: str
    html: Optional[str] = None
    text: Optional[str] = None
    time: datetime


class Metadata(BaseModel):
    """Metadata object in SmartLead webhook"""
    webhook_created_at: Optional[datetime] = None


class EmailBounceEvent(BaseModel):
    """SmartLead EMAIL_BOUNCE webhook payload (actual format from SmartLead)"""
    event_type: str = Field(default="EMAIL_BOUNCE")
    campaign_name: str
    sl_email_lead_id: str
    campaign_status: Optional[str] = None
    client_id: Optional[int] = None
    stats_id: Optional[str] = None
    custom_email_message: Optional[str] = None
    sent_message_body: Optional[str] = None
    sent_message: SentMessage
    subject: Optional[str] = None
    message_id: str
    is_bounced: bool
    is_sender_originated_bounce: Optional[bool] = None
    bounce_reply_message_id: Optional[str] = None
    bounce_reply_email: Optional[str] = None
    bounce_reply_email_preview: Optional[str] = None
    bounce_message: BounceMessage
    secret_key: Optional[str] = None
    app_url: Optional[str] = None
    ui_master_inbox_link: Optional[str] = None
    description: Optional[str] = None
    metadata: Optional[Metadata] = None
    webhook_url: Optional[str] = None
    webhook_id: Optional[int] = None
    webhook_name: Optional[str] = None


# ============================================================================
# Storage Functions
# ============================================================================

async def store_raw_to_r2(event_data: dict, message_id: str, event_type: str):
    """Store raw webhook payload to R2 (non-blocking)"""
    try:
        # Run boto3 operations in thread pool to avoid blocking event loop
        def _upload_to_r2():
            s3_client = get_r2_client()

            # Generate storage path: YYYY/MM/DD/message_id.json
            now = datetime.now(timezone.utc)
            path = f"{event_type}/{now.year}/{now.month:02d}/{now.day:02d}/{message_id}.json"

            # Upload to R2
            s3_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=path,
                Body=json.dumps(event_data, default=str),
                ContentType='application/json'
            )
            return path

        # Run in thread pool with timeout to avoid hanging
        path = await asyncio.wait_for(
            asyncio.to_thread(_upload_to_r2),
            timeout=10.0  # 10 second timeout
        )

        logger.info(f"📦 Stored raw payload to R2: {path}")
        return path

    except asyncio.TimeoutError:
        logger.error(f"❌ R2 upload timeout for {message_id}")
        raise
    except Exception as e:
        logger.error(f"❌ Failed to store to R2: {str(e)}")
        raise


async def store_to_neon(event: EmailSentEvent, raw_storage_path: str):
    """Store normalized data to Neon with timeout"""
    try:
        # Get connection with timeout
        conn = await asyncio.wait_for(
            get_neon_connection(),
            timeout=5.0
        )

        try:
            # Insert normalized data with timeout
            await asyncio.wait_for(
                conn.execute(
                    """
                    INSERT INTO email_sent_events (
                        message_id, from_email, to_email, to_name, time_sent,
                        campaign_id, campaign_name, sequence_number, custom_subject,
                        raw_storage_path
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (message_id) DO NOTHING
                    """,
                    event.message_id,
                    event.from_email,
                    event.to_email,
                    event.to_name,
                    event.time_sent,
                    event.campaign_id,
                    event.campaign_name,
                    event.sequence_number,
                    event.custom_subject,
                    raw_storage_path
                ),
                timeout=5.0
            )

            logger.info(f"🗄️ Stored to Neon: {event.message_id}")

        finally:
            await asyncio.wait_for(conn.close(), timeout=2.0)

    except asyncio.TimeoutError:
        logger.error(f"❌ Neon operation timeout for {event.message_id}")
        # Don't raise - we don't want to fail the webhook if Neon is slow
    except Exception as e:
        logger.error(f"❌ Failed to store to Neon: {str(e)}")
        # Don't raise - we don't want to fail the webhook if Neon is down


async def store_email_sent(event: EmailSentEvent, raw_payload: dict):
    """
    Store EMAIL_SENT event to R2 (raw) and Neon (normalized)
    """
    logger.info(f"📧 EMAIL_SENT: {event.from_email} → {event.to_email} | Campaign: {event.campaign_name} ({event.campaign_id}) | Seq: {event.sequence_number}")
    logger.info(f"   Subject: {event.custom_subject}")
    logger.info(f"   Message ID: {event.message_id}")
    logger.info(f"   Time: {event.time_sent}")

    try:
        # Step 1: Store raw payload to R2
        raw_storage_path = await store_raw_to_r2(raw_payload, event.message_id, "EMAIL_SENT")

        # Step 2: Store normalized data to Neon
        await store_to_neon(event, raw_storage_path)

        logger.info(f"✅ Successfully stored EMAIL_SENT: {event.message_id}")

    except Exception as e:
        logger.error(f"❌ Error storing EMAIL_SENT: {str(e)}")


async def store_bounce_to_neon(event: EmailBounceEvent, raw_storage_path: str, lead_email: str):
    """Store bounce event to Neon with timeout"""
    try:
        # Get connection with timeout
        conn = await asyncio.wait_for(
            get_neon_connection(),
            timeout=5.0
        )

        try:
            # Insert normalized data with timeout
            await asyncio.wait_for(
                conn.execute(
                    """
                    INSERT INTO email_bounce_events (
                        to_email, campaign_name, time_bounced, raw_storage_path,
                        lead_id, message_id, subject, bounce_reason, is_sender_originated
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    lead_email,
                    event.campaign_name,
                    event.bounce_message.time,
                    raw_storage_path,
                    event.sl_email_lead_id,
                    event.message_id,
                    event.subject,
                    event.bounce_reply_email_preview,
                    event.is_sender_originated_bounce
                ),
                timeout=5.0
            )

            logger.info(f"🗄️ Stored bounce to Neon: {lead_email}")

        finally:
            await asyncio.wait_for(conn.close(), timeout=2.0)

    except asyncio.TimeoutError:
        logger.error(f"❌ Neon bounce operation timeout for {lead_email}")
    except Exception as e:
        logger.error(f"❌ Failed to store bounce to Neon: {str(e)}")


def extract_email_from_description(description: Optional[str]) -> Optional[str]:
    """Extract email address from description field"""
    if not description:
        return None
    # Pattern: "sent to email@example.com got bounced"
    match = re.search(r'sent to ([\w\.-@]+)\s+got bounced', description)
    return match.group(1) if match else None


async def store_email_bounce(event: EmailBounceEvent, raw_payload: dict):
    """
    Store BOUNCED event to R2 (raw) and Neon (normalized)
    """
    # Extract lead email from description
    lead_email = extract_email_from_description(event.description)

    logger.info(f"💥 BOUNCED: {lead_email or 'unknown'} | Campaign: {event.campaign_name}")
    logger.info(f"   Subject: {event.subject}")
    logger.info(f"   Bounce reason: {event.bounce_reply_email_preview}")
    logger.info(f"   Time bounced: {event.bounce_message.time}")

    try:
        # Use message_id for storage
        bounce_id = event.message_id

        # Store raw payload to R2
        raw_storage_path = await store_raw_to_r2(raw_payload, bounce_id, "BOUNCED")

        # Store normalized data to Neon
        if lead_email:
            await store_bounce_to_neon(event, raw_storage_path, lead_email)
        else:
            logger.warning(f"⚠️ Could not extract lead email from description")

        logger.info(f"✅ Successfully stored BOUNCED: {bounce_id}")

    except Exception as e:
        logger.error(f"❌ Error storing BOUNCED: {str(e)}")


# ============================================================================
# Webhook Endpoints
# ============================================================================

@app.post("/webhooks/email-sent")
async def handle_email_sent(request: Request, background_tasks: BackgroundTasks):
    """
    SmartLead EMAIL_SENT webhook endpoint

    Expected payload: EmailSentEvent JSON
    Returns: 200 OK immediately, processes in background
    """
    raw_payload = await request.json()

    try:
        # Validate structure
        event = EmailSentEvent(**raw_payload)

        # Queue background processing (fast ACK)
        background_tasks.add_task(store_email_sent, event, raw_payload)

        logger.info(f"✅ Queued EMAIL_SENT webhook: {event.message_id}")

        return JSONResponse(
            status_code=200,
            content={
                "status": "received",
                "message_id": event.message_id,
                "event_type": "EMAIL_SENT"
            }
        )

    except Exception as e:
        logger.error(f"❌ Error processing EMAIL_SENT webhook: {str(e)}")
        # Still return 200 to avoid SmartLead retries for malformed data
        return JSONResponse(
            status_code=200,
            content={"status": "error", "message": str(e)}
        )


@app.post("/webhooks/email-bounce")
async def handle_email_bounce(request: Request, background_tasks: BackgroundTasks):
    """
    SmartLead EMAIL_BOUNCE webhook endpoint

    Expected payload: EmailBounceEvent JSON
    Returns: 200 OK immediately, processes in background
    """
    raw_payload = await request.json()

    try:
        # Validate structure
        event = EmailBounceEvent(**raw_payload)

        # Queue background processing (fast ACK)
        background_tasks.add_task(store_email_bounce, event, raw_payload)

        logger.info(f"✅ Queued EMAIL_BOUNCE webhook: {event.message_id}")

        return JSONResponse(
            status_code=200,
            content={
                "status": "received",
                "message_id": event.message_id,
                "event_type": "EMAIL_BOUNCE"
            }
        )

    except Exception as e:
        logger.error(f"❌ Error processing EMAIL_BOUNCE webhook: {str(e)}")
        # Still return 200 to avoid SmartLead retries for malformed data
        return JSONResponse(
            status_code=200,
            content={"status": "error", "message": str(e)}
        )


# ============================================================================
# Health Check & Info
# ============================================================================

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "SmartLead Webhook Receiver",
        "version": "2.0.0",
        "status": "running",
        "storage": {
            "r2": R2_BUCKET_NAME,
            "neon": "connected" if NEON_DATABASE_URL else "not configured"
        },
        "endpoints": {
            "email_sent": "/webhooks/email-sent",
            "email_bounce": "/webhooks/email-bounce",
            "health": "/"
        }
    }


@app.get("/health")
async def health():
    """Detailed health check"""
    return {
        "status": "healthy",
        "storage": {
            "r2": R2_BUCKET_NAME,
            "neon": "configured" if NEON_DATABASE_URL else "missing"
        }
    }


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
