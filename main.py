"""
SmartLead Webhook Receiver - MVP
Receives EMAIL_SENT and EMAIL_BOUNCE webhooks from SmartLead
"""
import os
import logging
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr, Field
import uvicorn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="SmartLead Webhook Receiver",
    description="Receives and logs SmartLead EMAIL_SENT and EMAIL_BOUNCE webhooks",
    version="1.0.0"
)

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


class EmailBounceEvent(BaseModel):
    """SmartLead EMAIL_BOUNCE webhook payload"""
    event_type: str = Field(default="EMAIL_BOUNCE")
    from_email: EmailStr
    to_email: EmailStr
    bounce_type: str  # "hard" or "soft"
    bounce_reason: Optional[str] = None
    campaign_id: Optional[int] = None
    message_id: Optional[str] = None
    time_bounced: Optional[datetime] = None

    class Config:
        json_schema_extra = {
            "example": {
                "event_type": "EMAIL_BOUNCE",
                "from_email": "sender@company.com",
                "to_email": "lead@example.com",
                "bounce_type": "hard",
                "bounce_reason": "mailbox full",
                "campaign_id": 123,
                "message_id": "abc123",
                "time_bounced": "2025-01-15T09:05:00Z"
            }
        }


# ============================================================================
# Storage Functions (MVP: Logging Only)
# ============================================================================

async def store_email_sent(event: EmailSentEvent):
    """
    MVP: Log the event
    Phase 2: Store to R2 (raw) + Neon (normalized)
    """
    logger.info(f"📧 EMAIL_SENT: {event.from_email} → {event.to_email} | Campaign: {event.campaign_name} ({event.campaign_id}) | Seq: {event.sequence_number}")
    logger.info(f"   Subject: {event.custom_subject}")
    logger.info(f"   Message ID: {event.message_id}")
    logger.info(f"   Time: {event.time_sent}")

    # TODO Phase 2: Upload raw JSON to R2
    # TODO Phase 2: Insert normalized data to Neon


async def store_email_bounce(event: EmailBounceEvent):
    """
    MVP: Log the event
    Phase 2: Store to R2 (raw) + Neon (normalized)
    """
    logger.info(f"💥 EMAIL_BOUNCE: {event.from_email} → {event.to_email} | Type: {event.bounce_type} | Reason: {event.bounce_reason}")
    logger.info(f"   Campaign ID: {event.campaign_id}")
    logger.info(f"   Message ID: {event.message_id}")
    if event.time_bounced:
        logger.info(f"   Time: {event.time_bounced}")

    # TODO Phase 2: Upload raw JSON to R2
    # TODO Phase 2: Insert normalized data to Neon


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
    try:
        # Parse JSON payload
        payload = await request.json()

        # Validate structure
        event = EmailSentEvent(**payload)

        # Queue background processing (fast ACK)
        background_tasks.add_task(store_email_sent, event)

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
    try:
        # Parse JSON payload
        payload = await request.json()

        # Validate structure
        event = EmailBounceEvent(**payload)

        # Queue background processing (fast ACK)
        background_tasks.add_task(store_email_bounce, event)

        logger.info(f"✅ Queued EMAIL_BOUNCE webhook: {event.message_id or 'no-id'}")

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
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "email_sent": "/webhooks/email-sent",
            "email_bounce": "/webhooks/email-bounce",
            "health": "/"
        }
    }


@app.get("/health")
async def health():
    """Detailed health check"""
    return {"status": "healthy"}


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
