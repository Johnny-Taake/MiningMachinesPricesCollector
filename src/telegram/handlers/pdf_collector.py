import asyncio
from datetime import datetime
from itertools import islice
import os
import re

from pyrogram.types import InputMediaDocument
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait
from pyrogram.raw import functions

from src.config import settings
from src.logger import logger as log
from src.pdf_processing import pdf_parser_main
from src.parser.uminers import UminersScraper
from src.google_sheets import upload_collected_files_to_google_sheets


async def check_user_permission(client: Client, message: Message) -> bool:
    """Check if the user has permission to collect PDF files by looking for them in the "Admins Bot" folder."""
    try:
        if not message.from_user:
            return False
        user_id = message.from_user.id

        # 1. get a list of user filters (folders)
        filters = await client.invoke(functions.messages.GetDialogFilters())

        admin_folder = next(
            (
                f
                for f in filters
                if hasattr(f, "title")  # пропускаем default/pinned
                and f.title == settings.pdf_collector.admins_folder_name
            ),
            None,
        )
        if not admin_folder:
            print(f"Папка «{settings.pdf_collector.admins_folder_name}» не найдена")
            return False

        # 2. check peers
        for p in admin_folder.include_peers:
            if getattr(p, "user_id", None) == user_id:
                return True
            if getattr(p, "chat_id", None) and (-p.chat_id) == user_id:
                return True
            if (
                getattr(p, "channel_id", None)
                and (-1000000000000 - p.channel_id) == user_id
            ):
                return True

        print(f"Пользователь {user_id} не найден в папке администраторов")
        return False

    except Exception as e:
        print(f"Ошибка при проверке прав пользователя: {e}")
        return False


async def download_pdf(client: Client, message: Message, save_dir: str) -> str:
    """
    Download a PDF file from a Telegram message and save it to a specified directory.
    Returns the path to the downloaded file or None if the download failed.
    """
    try:
        if not message.document or not message.document.file_name.lower().endswith(
            ".pdf"
        ):
            return None

        # Check for keywords in the file name
        file_name_lower = message.document.file_name.lower()
        is_match = False
        for keyword in settings.pdf_collector.pdf_filename_keywords:
            if keyword.lower() in file_name_lower:
                is_match = True
                break

        if not is_match:
            print(
                f"Файл {message.document.file_name} не содержит ключевых слов из списка, пропускаем"
            )
            return None

        # Create directory if it doesn't exist
        os.makedirs(save_dir, exist_ok=True)

        # Form a file name
        chat_title = message.chat.title or str(message.chat.id)
        chat_title = re.sub(r"[^\w\-_\. ]", "_", chat_title)

        # Save the file under the chat name
        file_name = f"{chat_title}.pdf"
        file_path = os.path.join(save_dir, file_name)

        # Download the file
        await client.download_media(message, file_path)
        print(f"Скачан PDF файл: {file_path}")
        return file_path

    except Exception as e:
        log.exception(f"Ошибка при скачивании PDF: {e}")
        return None


async def collect_pdf_files(client: Client, message: Message, limit: int = 100):
    """
    Process the 'collect' command: finds the latest PDF file with keywords in each chat from the "Collect Bot" folder
    """
    # Check if the sender of the command has permission to collect files
    if not await check_user_permission(client, message):
        await message.reply(
            "❌ У вас нет прав на выполнение этой команды. Доступ только для администраторов."
        )
        return

    # Create a directory for the current collection
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    collection_dir = os.path.join(
        settings.pdf_collector.get_pdf_save_dir(settings.base_data_dir),
        f"collection_{now}",
    )
    os.makedirs(collection_dir, exist_ok=True)

    # Send a message to indicate that the collection is starting
    status_message = await message.reply(
        "🔍 Начинаю поиск последних PDF файлов в чатах из папки для сбора..."
    )

    # Get a list of chats for the search from the "Collect Bot" folder
    chat_ids = []
    try:
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
            await status_message.edit(
                f"❌ Папка «{settings.pdf_collector.collect_folder_name}» не найдена. "
                f"Создайте папку с названием «{settings.pdf_collector.collect_folder_name}» и добавьте в неё чаты для сбора PDF."
            )
            return

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
            await status_message.edit(
                f"❌ Папка «{settings.pdf_collector.collect_folder_name}» пуста. "
                f"Добавьте чаты в папку «{settings.pdf_collector.collect_folder_name}» для сбора PDF."
            )
            return

        print(
            f"Найдено {len(chat_ids)} чатов в папке '{settings.pdf_collector.collect_folder_name}': {chat_ids}"
        )
        await status_message.edit(f"🔍 Найдено {len(chat_ids)} чатов в папке для сбора")

    except Exception as e:
        log.exception(f"Ошибка при получении чатов из папки: {e}")
        await status_message.edit(f"❌ Ошибка при получении чатов из папки: {e}")
        return

    # Collection log
    collection_log = []
    total_pdfs = 0
    chats_with_files = 0

    # Process each chat
    for chat_id in chat_ids:
        try:
            # Get chat information
            chat = await client.get_chat(chat_id)
            chat_name = chat.title or f"Chat {chat_id}"

            # Show progress
            await status_message.edit(f"🔍 Поиск в чате: {chat_name}")
            print(f"Обрабатываю чат: {chat_name} (ID: {chat_id})")

            # Variable for storing the latest PDF
            latest_pdf = None
            latest_date = None

            # Get the last messages
            async for msg in client.get_chat_history(chat_id, limit=limit):
                # Check if there is a document (PDF)
                if msg.document and msg.document.file_name.endswith(".pdf"):
                    # Check if the file matches the name keywords
                    file_name_lower = msg.document.file_name.lower()
                    is_match = False
                    for keyword in settings.pdf_collector.pdf_filename_keywords:
                        if keyword.lower() in file_name_lower:
                            is_match = True
                            matching_keyword = keyword
                            break

                    if is_match:
                        print(
                            f"Найден подходящий файл: {msg.document.file_name} (ключевое слово: {matching_keyword})"
                        )
                        # If there is no latest PDF, or the current message is newer, update the latest PDF
                        if latest_date is None or msg.date > latest_date:
                            latest_pdf = msg
                            latest_date = msg.date
                            print(
                                f"Обновлен самый свежий файл: {msg.document.file_name} от {msg.date}"
                            )

            # Download the latest PDF if found
            if latest_pdf:
                print(
                    f"Скачиваю самый свежий файл из чата {chat_name}: {latest_pdf.document.file_name}"
                )
                file_path = await download_pdf(client, latest_pdf, collection_dir)
                if file_path:
                    pdf_info = {
                        "file_path": file_path,
                        "file_name": latest_pdf.document.file_name,
                        "date": latest_pdf.date.strftime("%Y-%m-%d %H:%M:%S"),
                        "from_user": (
                            latest_pdf.from_user.first_name
                            if latest_pdf.from_user
                            else "Unknown"
                        ),
                    }
                    collection_log.append(
                        {
                            "chat_name": chat_name,
                            "chat_id": chat_id,
                            "files_count": 1,
                            "files": [pdf_info],
                        }
                    )
                    total_pdfs += 1
                    chats_with_files += 1
                    print(f"Успешно скачан файл из чата {chat_name}: {file_path}")
            else:
                print(f"В чате {chat_name} не найдено подходящих PDF файлов")

        except FloodWait as fw:
            print(f"FloodWait при обработке чата {chat_id}: ждём {fw.value} секунд")
            await asyncio.sleep(fw.value)
        except Exception as e:
            log.exception(f"Ошибка при обработке чата {chat_id}: {e}")
            collection_log.append(
                {"chat_name": f"Chat {chat_id}", "chat_id": chat_id, "error": str(e)}
            )

    # Collect all file paths from the collection log
    all_files = [
        f_info["file_path"]
        for chat_log in collection_log
        if "files" in chat_log
        for f_info in chat_log["files"]
    ]

    # Save the report
    report_path = os.path.join(collection_dir, "collection_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"Отчет о сборе PDF файлов от {now}\n")
        f.write(f"Всего собрано файлов: {total_pdfs}\n")
        f.write(f"Чатов с файлами: {chats_with_files} из {len(chat_ids)}\n\n")

        for chat_log in collection_log:
            f.write(f"== Чат: {chat_log['chat_name']} (ID: {chat_log['chat_id']}) ==\n")

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

    # Send the report
    if total_pdfs > 0:
        result_message = (
            f"📊 Сбор PDF файлов завершен!\n\n"
            f"Всего найдено файлов: {total_pdfs}\n"
            f"Чатов с файлами: {chats_with_files} из {len(chat_ids)}\n"
            f"Файлы сохранены в директории: {collection_dir}"
        )
    else:
        result_message = (
            f"📊 Сбор PDF файлов завершен!\n\n"
            f"PDF файлы не найдены в доступных чатах.\n"
            f"Обработано чатов: {len(chat_ids)}"
        )

    await status_message.edit(result_message)

    # Send all collected files in batches
    CHUNK = 5
    it = iter(all_files)
    batch = list(islice(it, CHUNK))
    while batch:
        media_group = [InputMediaDocument(file_path) for file_path in batch]
        await client.send_media_group(chat_id=message.chat.id, media=media_group)
        batch = list(islice(it, CHUNK))

    status_message = await message.reply(
        "Извлечение данных из PDF файлов..."
    )

    # Process the collected PDFs with pdf_parser_main
    pdf_parser_main()
    
    await status_message.edit(
        "Сбор данных с сайта Uminers..."
    )
    
    # Run the Uminers WebScraper
    uminers_scraper = UminersScraper(settings.uminers_scraper.urls_to_scrape)
    uminers_scraper.run()
    
    await status_message.edit(
        "Загрузка данных в Google Sheets..."
    )
    
    # Upload collected files to Google Sheets
    link = upload_collected_files_to_google_sheets()
    await status_message.edit(
        f"✅ Данные загружены в Google Sheets: {link}"
    )


def register_pdf_collector_handlers(app: Client):
    """
    Register handlers for the 'collect' command for PDF collection.
    """

    @app.on_message(
        filters.command(["сбор", "collect"]) & (filters.group | filters.private)
    )
    async def pdf_collect_command(client, message):
        await collect_pdf_files(client, message)
