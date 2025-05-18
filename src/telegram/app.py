# src/app.py

import asyncio
import os

from pyrogram import filters
from pyrogram.handlers import MessageHandler

from src import logger as log
from src.config import settings, BotMode

from src.telegram.client import app
from src.telegram.services import ForwardConfigService
from src.telegram.handlers import (
    create_forwarding_handler,
    register_pdf_collector_handlers,
)
from src.telegram.utils import get_dialog_folder


# Folder with chats for forwarding
CHATS_FOLDER_NAME = settings.bot_forwarding.forward_folder_name

# File with forwarding configuration
CONFIG_FILE = settings.bot_forwarding.bot_chats_config_file

# Global variables for storing forwarding settings
SOURCE_CHAT_IDS = []
FORWARDING_CONFIG = {}

# Folders names for PDF collector
ADMINS_FOLDER_NAME = settings.pdf_collector.admins_folder_name
COLLECT_FOLDER_NAME = settings.pdf_collector.collect_folder_name


async def idle():
    """
    Function for supporting the bot's work until the forceful termination.
    """
    try:
        # Wait until endless or interruption
        while True:
            # Check every hour
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        # Process Ctrl+C
        print("Завершение работы бота...")


async def main():
    """
    Main function for launching the bot.
    Configures handlers and starts the client.
    """
    global SOURCE_CHAT_IDS, FORWARDING_CONFIG, CHATS_FOLDER_NAME, SKIP_FORWARDING
    forward_config_manager = ForwardConfigService(CONFIG_FILE, CHATS_FOLDER_NAME)

    print("Запуск бота...")

    # Client setup
    await app.start()

    # --- Read CLI‑args --mode=... ---------------------------------
    arg_mode = next(
        (a.split("=", 1)[1] for a in os.sys.argv[1:] if a.startswith("--mode=")),
        None,
    )
    if arg_mode:
        settings.tg_bot_run.mode = BotMode(arg_mode)

    SKIP_FORWARDING = settings.tg_bot_run.mode == BotMode.COLLECT_ONLY
    SKIP_COLLECT = settings.tg_bot_run.mode == BotMode.FORWARD_ONLY

    print(f"Запущен в режиме: {settings.tg_bot_run.mode.value}")

    # --- Register collector, if needed --------------------------
    if not SKIP_COLLECT:
        # Check folders for admins and chats to collect PDFs exists
        is_admins_folder_exists, _ = await get_dialog_folder(app, ADMINS_FOLDER_NAME)
        is_collect_folder_exists, _ = await get_dialog_folder(app, COLLECT_FOLDER_NAME)

        if not is_admins_folder_exists:
            print(f"Создайте папку с названием «{ADMINS_FOLDER_NAME}» и добавьте в неё чаты с администраторами бота.")
            return

        if not is_collect_folder_exists:
            print(f"Создайте папку с названием «{COLLECT_FOLDER_NAME}» и добавьте в неё чаты для сбора PDF.")
            return
        
        register_pdf_collector_handlers(app)
        print("Обработчики команд сборщика PDF зарегистрированы")

    # --- If forwarding is disabled – run only as a collector ------
    if SKIP_FORWARDING:
        print("Пересылка отключена, работаем только как PDF‑коллектор")
        print("Отправьте /сбор боту или в группе для старта")
        await idle()
        await app.stop()
        return

    # Initialize chat_info
    chat_info = {}

    # Check if there is a saved configuration
    has_config, saved_source_ids, saved_forwarding_config, saved_chat_info = (
        forward_config_manager.load_saved_config()
    )

    if has_config:
        SOURCE_CHAT_IDS = saved_source_ids
        FORWARDING_CONFIG = saved_forwarding_config
        chat_info = saved_chat_info
    else:
        # Check folder and configure forwarding
        folder_exists, folder_config, folder_chat_info = (
            await forward_config_manager.configure_forwarding()
        )
        if not folder_exists or not folder_config or not folder_chat_info:
            if not folder_exists:
                print(
                    f"Для работы бота необходимо создать папку '{CHATS_FOLDER_NAME}' в Telegram"
                )
                print(
                    "и добавить в неё чаты, из которых нужно пересылать сообщения, и чаты для пересылки."
                )

            if not folder_config or not folder_chat_info:
                log.error(
                    "Невозможно загрузить конигурацию пересылки. Что-то пошло не так."
                )
                return

            # Ask the user if they want to continue only with the collector mode
            while True:
                response = input(
                    "Продолжить в режиме только сборщика PDF? (да/нет): "
                ).lower()
                if response in ["да", "yes", "y", "д"]:
                    print("Запуск в режиме сборщика PDF...")
                    print("Запуск сбора: отправьте команду /сбор в любой чат с ботом")
                    await idle()
                    await app.stop()
                    return
                elif response in ["нет", "no", "n", "н"]:
                    print("Завершение работы.")
                    await app.stop()
                    return
                else:
                    print("Пожалуйста, введите 'да' или 'нет'.")

        SOURCE_CHAT_IDS = list(folder_config.keys())
        FORWARDING_CONFIG = folder_config
        chat_info = folder_chat_info

        # Save configuration for future runs
        is_config_saved = forward_config_manager.save_config(
            SOURCE_CHAT_IDS, FORWARDING_CONFIG, chat_info
        )
        if is_config_saved:
            print(f"Конфигурация сохранена в файл {forward_config_manager.config_file}")

    # Check and update access to chats before launch
    # It updates SOURCE_CHAT_IDS and FORWARDING_CONFIG based on the availability of chats. Prevents unexpected errors
    SOURCE_CHAT_IDS, FORWARDING_CONFIG, chat_info = (
        await forward_config_manager.validate_chats(
            app, SOURCE_CHAT_IDS, FORWARDING_CONFIG
        )
    )

    # If there are no configured chats, finish setting up forwarding but continue to work
    if not SOURCE_CHAT_IDS or not FORWARDING_CONFIG:
        print(
            "Не настроено ни одной пересылки. Бот работает только в режиме сборщика PDF."
        )
    else:
        # Create filter for tracking messages only from specified chats
        source_chats_filter = filters.chat(SOURCE_CHAT_IDS)
        print("Фильтр для отслеживания сообщений:", SOURCE_CHAT_IDS)

        # Register forwarding handler
        print("Регистрируем обработчик для всех входящих сообщений из указанных чатов")
        app.add_handler(
            MessageHandler(
                create_forwarding_handler(chat_info),
                filters=source_chats_filter,
            )
        )

        print("Пересылка настроена и активирована!")
        print(f"Отслеживаются сообщения из {len(SOURCE_CHAT_IDS)} чатов")

        # Print current forwarding configuration
        forward_config_manager.print_current_config(FORWARDING_CONFIG, chat_info)

    print("Бот запущен и готов к работе!")
    print("Запуск сбора PDF: отправьте команду /сбор в чат с ботом")
    print("Нажмите Ctrl+C для завершения работы")

    # Keep the bot running until the forceful termination
    await idle()

    # Stop the bot
    await app.stop()
