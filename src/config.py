from enum import Enum
import os
from dotenv import load_dotenv

from pydantic import BaseModel
from pydantic_settings import BaseSettings

load_dotenv(".env")

API_ID = int(os.getenv("API_ID", "12345"))
API_HASH = os.getenv("API_HASH", "0123456789abcdef0123456789abcdef")

GOOGLE_SHEETS_EMAILS = [
    e.strip() for e in os.getenv("GOOGLE_SHEETS_EMAILS", "").split(",") if e.strip()
]

# UMINER_SEARCH_URLS = [
#     u.strip() for u in os.getenv("UMINER_SEARCH_URLS", "").split(",") if u.strip()
# ]

# NOTE: For shared folder use folder ID, for root folder use None
GOOGLE_DRIVE_FOLDER = os.getenv("GOOGLE_DRIVE_FOLDER", None)


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

    no_pdf_filename_check_chats: list[str] = [
        "РусТехМаш by Uminers",
    ]


class UminersScraperConfig(BaseModel):
    # Number of parallel workers for Chrome Selenuim driven scraping
    # NOTE: Important! If you set it to 1, the program will work slowly on web parsing
    # but the load on the server will much be lower
    max_workers: int = 2
    # Result file name
    output_file: str = "Web_Uminers.xlsx"
    # URL to scrape
    url_to_scrape: str = "https://uminers.com/catalog/"


class GoogleSheetsConfig(BaseModel):
    emails: list[str] = GOOGLE_SHEETS_EMAILS
    client_secret_file: str = "client_secret_google.json"
    # NOTE: If None -> root folder used
    files_folder_name: str = GOOGLE_DRIVE_FOLDER


class CronJobConfig(BaseModel):
    day_of_week: str = "tue"
    hour: int = 7


class Config(BaseSettings):
    # Common data directory
    base_data_dir: str = "data"

    # Common directory to store prepared excels
    prepared_excels_dir: str = f"{base_data_dir}/prepared_excels"

    # Ecxel files archive directory
    excels_archive_dir: str = f"{base_data_dir}/excels_archive"

    # Prepared excels directory
    prepared_data_dir: str = f"{base_data_dir}/prepared_data"

    tg_bot_run: TGBotRunConfig = TGBotRunConfig()
    bot_forwarding: BotForwardingConfig = BotForwardingConfig()
    pdf_collector: PDFCollectorConfig = PDFCollectorConfig()
    uminers_scraper: UminersScraperConfig = UminersScraperConfig()
    google_sheets: GoogleSheetsConfig = GoogleSheetsConfig()
    cron_job: CronJobConfig = CronJobConfig()


settings = Config()
