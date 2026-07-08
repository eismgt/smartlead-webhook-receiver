"""
SmartLead Webhook Receiver - Phase 2
Receives EMAIL_SENT and EMAIL_BOUNCE webhooks from SmartLead
Stores raw JSON to R2 and normalized data to Neon
"""
import os
import logging
import json
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


class LeadInfo(BaseModel):
    """Nested lead object in SmartLead bounce webhook"""
    email: EmailStr
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company_name: Optional[str] = None
    custom_fields: Optional[dict] = None


class EmailInfo(BaseModel):
    """Nested email object in SmartLead bounce webhook"""
    subject: Optional[str] = None
    message_id: Optional[str] = None


class EmailBounceEvent(BaseModel):
    """SmartLead EMAIL_BOUNCED webhook payload (actual format from SmartLead docs)"""
    event: str = Field(default="EMAIL_BOUNCED")
    timestamp: datetime
    campaign_id: int
    campaign_name: Optional[str] = None
    lead_id: Optional[int] = None
    email_account_id: Optional[int] = None
    lead: LeadInfo
    sequence_number: Optional[int] = None
    email: Optional[EmailInfo] = None

    class Config:
        json_schema_extra = {
            "example": {
                "event": "EMAIL_BOUNCED",
                "timestamp": "2024-01-15T10:30:00Z",
                "campaign_id": 123,
                "campaign_name": "Cold Outreach Q1",
                "lead_id": 789,
                "email_account_id": 456,
                "lead": {
                    "email": "lead@example.com",
                    "first_name": "Jane",
                    "last_name": "Doe",
                    "company_name": "Acme Corp",
                    "custom_fields": {"job_title": "CEO"}
                },
                "sequence_number": 1,
                "email": {
                    "subject": "Quick question",
                    "message_id": "abc123@smartlead.ai"
                }
            }
        }


# ============================================================================
# Storage Functions
# ============================================================================

async def store_raw_to_r2(event_data: dict, message_id: str, event_type: str):
    """Store raw webhook payload to R2"""
    try:
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

        logger.info(f"📦 Stored raw payload to R2: {path}")
        return path

    except Exception as e:
        logger.error(f"❌ Failed to store to R2: {str(e)}")
        raise


async def store_to_neon(event: EmailSentEvent, raw_storage_path: str):
    """Store normalized data to Neon"""
    try:
        conn = await get_neon_connection()

        try:
            # Insert normalized data
            await conn.execute(
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
            )

            logger.info(f"🗄️ Stored to Neon: {event.message_id}")

        finally:
            await conn.close()

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


async def store_bounce_to_neon(event: EmailBounceEvent, raw_storage_path: str):
    """Store bounce event to Neon"""
    try:
        conn = await get_neon_connection()

        try:
            await conn.execute(
                """
                INSERT INTO email_bounce_events (
                    to_email, campaign_id, campaign_name,
                    time_bounced, raw_storage_path, lead_id,
                    email_account_id, sequence_number, subject
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                event.lead.email,
                event.campaign_id,
                event.campaign_name,
                event.timestamp,
                raw_storage_path,
                event.lead_id,
                event.email_account_id,
                event.sequence_number,
                event.email.subject if event.email else None
            )

            logger.info(f"🗄️ Stored bounce to Neon: {event.lead.email}")

        finally:
            await conn.close()

    except Exception as e:
        logger.error(f"❌ Failed to store bounce to Neon: {str(e)}")


async def store_email_bounce(event: EmailBounceEvent, raw_payload: dict):
    """
    Store BOUNCED event to R2 (raw) and Neon (normalized)
    """
    logger.info(f"💥 BOUNCED: {event.lead.email} | Campaign: {event.campaign_name} ({event.campaign_id})")
    logger.info(f"   Lead: {event.lead.first_name} {event.lead.last_name}")
    logger.info(f"   Subject: {event.email.subject if event.email else 'N/A'}")
    logger.info(f"   Time: {event.timestamp}")

    try:
        # Generate unique ID for storage using message_id if available
        message_id = event.email.message_id if event.email else None
        if message_id:
            bounce_id = message_id
        else:
            bounce_id = f"bounce-{event.campaign_id}-{event.lead.email}-{int(event.timestamp.timestamp())}"

        # Store raw payload to R2
        raw_storage_path = await store_raw_to_r2(raw_payload, bounce_id, "BOUNCED")

        # Store normalized data to Neon
        await store_bounce_to_neon(event, raw_storage_path)

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

    # TEMPORARY: Log raw payload to see actual structure
    logger.info(f"🔍 RAW BOUNCE PAYLOAD: {json.dumps(raw_payload, indent=2)}")

    try:
        # Validate structure
        event = EmailBounceEvent(**raw_payload)

        # Queue background processing (fast ACK)
        background_tasks.add_task(store_email_bounce, event, raw_payload)

        message_id = event.email.message_id if event.email else "no-id"
        logger.info(f"✅ Queued EMAIL_BOUNCE webhook: {message_id}")

        return JSONResponse(
            status_code=200,
            content={
                "status": "received",
                "lead_email": event.lead.email,
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
