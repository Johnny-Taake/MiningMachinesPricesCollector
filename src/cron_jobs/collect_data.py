import asyncio
from datetime import datetime
import os

import gspread

from pyrogram.client import Client

from pyrogram.raw import functions

from src.config import settings
from src.logger import logger as log
from src.pdf_processing import pdf_parser_main
from src.parser.uminers import UminersScraper
from src.google_sheets import upload_collected_files_to_google_sheets
from src.telegram.handlers import collect_pdf_files


async def collect_data_to_google_sheets_cron_job(client: Client, limit: int = 100):
    """
    Cron job version: finds the latest PDF file with keywords in each chat from the "Collect Bot" folder
    without user interaction. Designed to run automatically.
    """
    print("🤖 Запуск автоматического сбора PDF файлов...")

    # Create a directory for the current collection
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    collection_dir = os.path.join(
        settings.pdf_collector.get_pdf_save_dir(settings.base_data_dir),
        f"collection_{now}",
    )
    os.makedirs(collection_dir, exist_ok=True)

    # Get a list of chats for the search from the "Collect Bot" folder
    chat_ids = []
    try:
        print("🔍 Поиск папки для сбора PDF...")

        # Find the folder with chats for the collection
        filters = await client.invoke(functions.messages.GetDialogFilters())

        collect_folder = next(
            (
                f
                for f in filters
                if hasattr(f, "title")
                and f.title == settings.pdf_collector.collect_folder_name
            ),
            None,
        )

        if not collect_folder:
            error_msg = (
                f"❌ Папка «{settings.pdf_collector.collect_folder_name}» не найдена."
            )
            log.error(error_msg)
            return False

        # Get IDs of chats from the folder
        for peer in collect_folder.include_peers:
            chat_id = None

            if hasattr(peer, "channel_id"):
                chat_id = -1000000000000 - peer.channel_id
            elif hasattr(peer, "chat_id"):
                chat_id = -peer.chat_id
            elif hasattr(peer, "user_id"):
                chat_id = peer.user_id

            if chat_id:
                chat_ids.append(chat_id)

        if not chat_ids:
            error_msg = (
                f"❌ Папка «{settings.pdf_collector.collect_folder_name}» пуста."
            )
            log.error(error_msg)
            return False

        print(
            f"✅ Найдено {len(chat_ids)} чатов в папке '{settings.pdf_collector.collect_folder_name}': {chat_ids}"
        )

    except Exception as e:
        error_msg = f"❌ Ошибка при получении чатов из папки: {e}"
        log.exception(error_msg)
        return False

    # Collect PDF files using the dedicated function
    print("🔍 Начинаю поиск и скачивание PDF файлов...")

    try:
        collection_log, total_pdfs, chats_with_files, all_files = (
            await collect_pdf_files(client, chat_ids, collection_dir, limit)
        )
    except Exception as e:
        error_msg = f"❌ Ошибка при сборе PDF файлов: {e}"
        log.exception(error_msg)
        return False

    # Save the report
    report_path = os.path.join(collection_dir, "collection_report.txt")
    try:
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(f"Отчет о сборе PDF файлов от {now}\n")
            f.write(f"Всего собрано файлов: {total_pdfs}\n")
            f.write(f"Чатов с файлами: {chats_with_files} из {len(chat_ids)}\n\n")

            for chat_log in collection_log:
                f.write(
                    f"== Чат: {chat_log['chat_name']} (ID: {chat_log['chat_id']}) ==\n"
                )

                if "error" in chat_log:
                    f.write(f"  Ошибка: {chat_log['error']}\n")
                    continue

                f.write(f"  Найдено файлов: {chat_log['files_count']}\n")
                for i, file in enumerate(chat_log.get("files", []), 1):
                    f.write(f"  {i}. {file['file_name']}\n")
                    f.write(f"     Дата сообщения: {file['date']}\n")
                    f.write(f"     От пользователя: {file['from_user']}\n")
                    f.write(f"     Путь: {file['file_path']}\n")
                f.write("\n")

        print(f"📄 Отчет сохранен: {report_path}")
    except Exception as e:
        error_msg = f"❌ Ошибка при сохранении отчета: {e}"
        log.exception(error_msg)

    # Log the collection results
    if total_pdfs > 0:
        result_message = (
            f"📊 Автоматический сбор PDF файлов завершен!\n"
            f"Всего найдено файлов: {total_pdfs}\n"
            f"Чатов с файлами: {chats_with_files} из {len(chat_ids)}\n"
            f"Файлы сохранены в директории: {collection_dir}"
        )
    else:
        result_message = (
            f"📊 Автоматический сбор PDF файлов завершен!\n"
            f"PDF файлы не найдены в доступных чатах.\n"
            f"Обработано чатов: {len(chat_ids)}"
        )

    print(result_message)

    # Process collected files only if we have any
    if total_pdfs > 0:
        print("⚙️ Извлечение данных из PDF файлов...")

        try:
            # Process the collected PDFs with pdf_parser_main
            pdf_parser_main()
            print("✅ Данные успешно извлечены из PDF файлов.")
        except Exception as e:
            error_msg = f"❌ Ошибка при извлечении данных из PDF: {e}"
            log.exception(error_msg)

        print("🌐 Сбор данных с сайта Uminers...")

        try:
            # Run the Uminers WebScraper
            uminers_scraper = UminersScraper(settings.uminers_scraper.url_to_scrape)
            uminers_scraper.run()
            print("✅ Данные с сайта Uminers собраны.")
        except Exception as e:
            error_msg = f"Ошибка при сборе данных с Uminers: {e}"
            log.exception(error_msg)

        print("📊 Загрузка данных в Google Sheets...")

        try:
            # Upload collected files to Google Sheets
            link = upload_collected_files_to_google_sheets()
            success_msg = f"✅ Данные загружены в Google Sheets: {link}"
            print(success_msg)
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 403 and "sharing quota" in str(e):
                warning_msg = (
                    "Квота SharingQuota исчерпана – пропускаю загрузку в Google Sheets"
                )
                log.warning(warning_msg)
            else:
                error_msg = f"Ошибка при загрузке в Google Sheets: {e}"
                log.exception(error_msg)
                raise
        except Exception as e:
            error_msg = f"Неожиданная ошибка при загрузке в Google Sheets: {e}"
            log.exception(error_msg)
    else:
        print("⚠️ Нет файлов для обработки - пропускаю дополнительные этапы.")

    print("✅ Автоматический сбор завершен.")
    return True


async def safe_data_collection_wrapper():
    start_time = datetime.now()
    print(f"🤖 Запущен сбор данных - время начала: {start_time}")
    try:
        from src.telegram.client import app

        success = await asyncio.wait_for(
            collect_data_to_google_sheets_cron_job(app), timeout=600
        )
        return success
    except asyncio.TimeoutError:
        duration = datetime.now() - start_time
        log.error(
            f"Сбор данных закончился истечением максимального времени (10 мин) на выполнение после {duration.total_seconds():.2f} секунд"
        )
        return False
    except Exception as e:
        duration = datetime.now() - start_time
        error_msg = f"Критическая ошибка в cron job: {e}"
        log.exception(f"{error_msg} после {duration.total_seconds():.2f} секунд: {e}")
        return False
