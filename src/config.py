from enum import Enum
import os
from dotenv import load_dotenv

from pydantic import BaseModel
from pydantic_settings import BaseSettings

load_dotenv(".env")

API_ID = int(os.getenv("API_ID", "12345"))
API_HASH = os.getenv("API_HASH", "0123456789abcdef0123456789abcdef")


class BotMode(str, Enum):
    # message forward and collect
    FULL = "full"
    # message forward only
    FORWARD_ONLY = "forward-only"
    # prices collection only
    COLLECT_ONLY = "collect-only"


class TGBotRunConfig(BaseModel):
    # Telegram API
    api_id: int = API_ID
    api_hash: str = API_HASH

    # Bot start mode
    mode: BotMode = BotMode.FULL


class BotForwardingConfig(BaseModel):
    # chats for forwarding
    forward_folder_name: str = "Forward Bot"
    # File with forwarding configuration
    bot_chats_config_file: str = "forward_config.json"


class PDFCollectorConfig(BaseModel):
    # Where to look for PDFs
    collect_folder_name: str = "Collect Bot"
    # chats for bot admins
    admins_folder_name: str = "Admins Bot"

    # Where to save collected PDFs
    pdf_save_dir: str = "collected_pdfs"
    def get_pdf_save_dir(self, base_data_dir: str) -> str:
        return os.path.join(base_data_dir, self.pdf_save_dir)

    # Keywords for PDF files to get the price list
    pdf_filename_keywords: list[str] = [
        "прайс",
        "price",
        "price-list",
        "прайс-лист",
        "каталог",
        "catalog",
        "прайс-лист",
    ]


# class ChromeConfig(BaseModel):
#     path: str = os.path.abspath("/usr/local/bin/chromedriver")


class Config(BaseSettings):
    # Common data directory
    base_data_dir: str = "data"

    tg_bot_run: TGBotRunConfig = TGBotRunConfig()
    bot_forwarding: BotForwardingConfig = BotForwardingConfig()
    pdf_collector: PDFCollectorConfig = PDFCollectorConfig()
    # chrome: ChromeConfig = ChromeConfig()


settings = Config()
