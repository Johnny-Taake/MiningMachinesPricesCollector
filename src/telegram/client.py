from pyrogram import Client

from src.config import settings


# Create a Pyrogram Client instance
app = Client(
    "message_forwarder_bot",
    api_id=settings.tg_bot_run.api_id,
    api_hash=settings.tg_bot_run.api_hash,
    # NOTE: Don't use bot token to login with verification code using phone number
    bot_token=None,
)
