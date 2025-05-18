import json
import os

from pyrogram.enums import ChatType

from src import logger as log
from src.telegram.client import app
from src.telegram.utils import get_dialog_folder


class ForwardConfigService:
    def __init__(self, config_file, forward_chats_folder):
        self.config_file = config_file
        self.forward_chats_dir = forward_chats_folder

    async def interactive_setup(self, dialog_dict):
        """
        Interactive setup of forwarding configuration

        Args:
            dialog_dict (dict): Dict of available dialogs with numbers as keys

        Returns:
            tuple: (source_chat_ids, forwarding_config)
        """

        source_chat_ids = []
        forwarding_config = {}

        print("\n=== Настройка пересылки сообщений из папки ===")
        print(f"Доступные диалоги из папки '{self.forward_chats_dir}':")

        # Display list of dialogs with more understandable chat types
        for i, info in dialog_dict.items():
            chat_type_name = (
                "Группа"
                if info["type"] in [ChatType.GROUP, ChatType.SUPERGROUP]
                else (
                    "Личный чат"
                    if info["type"] == ChatType.PRIVATE
                    else (
                        "Канал"
                        if info["type"] == ChatType.CHANNEL
                        else "Бот" if info["type"] == ChatType.BOT else "UNKNOWN"
                    )
                )
            )
            print(f"[{i}] {info['name']} ({chat_type_name}) - ID: {info['id']}")

        # Select source chats for forwarding
        print(
            "\nВыберите номера чатов, ИЗ которых нужно пересылать сообщения (введите номера через запятую):"
        )
        source_input = input("> ")
        source_indexes = [
            int(i.strip()) for i in source_input.split(",") if i.strip().isdigit()
        ]

        if not source_indexes:
            print("Вы не выбрали ни одного чата для пересылки.")
            return [], {}

        # Dictionary for storing information about selected source chats
        selected_source_chats = {}

        for idx in source_indexes:
            if idx in dialog_dict:
                chat_id = dialog_dict[idx]["id"]
                chat_name = dialog_dict[idx]["name"]
                source_chat_ids.append(chat_id)
                selected_source_chats[chat_id] = {
                    "index": idx,
                    "name": chat_name,
                }

                # For each source chat, select destination chats
                print(
                    f"\nВыберите номера чатов, В которые нужно пересылать сообщения из {chat_name} (введите номера через запятую):"
                )
                dest_input = input("> ")
                dest_indexes = [
                    int(i.strip()) for i in dest_input.split(",") if i.strip().isdigit()
                ]

                dest_chat_ids = []
                for dest_idx in dest_indexes:
                    if dest_idx in dialog_dict:
                        dest_chat_id = dialog_dict[dest_idx]["id"]
                        # Check that we're not forwarding to the same chat
                        if dest_chat_id != chat_id:
                            dest_chat_ids.append(dest_chat_id)

                if dest_chat_ids:
                    forwarding_config[chat_id] = dest_chat_ids
                    print(
                        f"Пересылка из {chat_name} настроена в {len(dest_chat_ids)} чат(ов)"
                    )

        return source_chat_ids, forwarding_config

    def save_config(self, source_chat_ids, forwarding_config, chat_info):
        """
        Save config to file
        """
        try:
            # Serialize ChatType objects to JSON
            serializable_chat_info = {}
            for chat_id, info in chat_info.items():
                # Create a copy
                serializable_chat_info[chat_id] = dict(info)
                if "type" in info and hasattr(info["type"], "name"):
                    serializable_chat_info[chat_id]["type"] = str(info["type"].name)

            config_data = {
                "source_chat_ids": source_chat_ids,
                "forwarding_config": {str(k): v for k, v in forwarding_config.items()},
                "chat_info": {str(k): v for k, v in serializable_chat_info.items()},
            }

            with open(self.config_file, "w", encoding="utf-8") as f:
                json.dump(config_data, f, ensure_ascii=False, indent=2)

            return True

        except Exception as e:
            log.exception(f"Ошибка при сохранении конфигурации: {e}")
            return False

    def load_saved_config(self):
        """
        Load saved config from file

        Returns:
            tuple: (has_config, source_chat_ids, forwarding_config, chat_info)
                has_config (bool): True, if config file exists and loaded successfully
                source_chat_ids (list): ID of source chats
                forwarding_config (dict): Dict with source and destination chat IDs {source_id: [dest_ids]}
                chat_info (dict): Dict with chat info {chat_id: {username, type, title}}
        """
        try:
            if not os.path.exists(self.config_file):
                print(f"Файл конфигурации {self.config_file} не найден")
                return False, [], {}, {}

            with open(self.config_file, "r", encoding="utf-8") as f:
                config_data = json.load(f)

            source_chat_ids = config_data.get("source_chat_ids", [])

            # Keys to int (json stores them as strings)
            forwarding_config = {}
            for k, v in config_data.get("forwarding_config", {}).items():
                forwarding_config[int(k)] = v

            chat_info = {}
            for k, v in config_data.get("chat_info", {}).items():
                chat_info[int(k)] = v

            print(f"Загружена конфигурация из {self.config_file}")
            print(f"Настроено {len(source_chat_ids)} исходных чатов")

            for source_id, dest_ids in forwarding_config.items():
                print(
                    f"Из: Чат {source_id} -> В: {', '.join([f'Чат {dest_id}' for dest_id in dest_ids])}"
                )

            return True, source_chat_ids, forwarding_config, chat_info
        except Exception as e:
            log.exception(f"Ошибка при загрузке конфигурации: {e}")
            return False, [], {}, {}

    @staticmethod
    def print_current_config(forwarding_config, chat_info):
        """
        Print the current configuration with additional information
        """
        for source_id, dest_ids in forwarding_config.items():
            # Get source name
            source_name = chat_info.get(source_id, {}).get("name", f"Чат {source_id}")
            source_info = ""
            if source_id in chat_info:
                if "username" in chat_info[source_id]:
                    source_info += f" (@{chat_info[source_id]['username']})"
                source_info += f" [тип: {chat_info[source_id]['type']}]"

            # Collect info about destination chats
            dest_info = []
            for dest_id in dest_ids:
                dest_name = chat_info.get(dest_id, {}).get("name", str(dest_id))
                info = dest_name
                if dest_id in chat_info:
                    if "username" in chat_info[dest_id]:
                        info += f" (@{chat_info[dest_id]['username']})"
                    info += f" [тип: {chat_info[dest_id]['type']}]"
                dest_info.append(info)

            print(f"Из чата {source_name}{source_info} в чаты: {', '.join(dest_info)}")

    async def validate_chats(self, app, source_chat_ids, forwarding_config):
        """
        Check and restore the availability of all chats in the configuration,
        loading the list of dialogs for a more reliable access.
        """
        print("Проверка доступа к чатам...")

        # Dictionary for storing chat information
        chat_info = {}

        # Load all dialogs for caching
        print("Предварительная загрузка всех доступных диалогов...")
        cached_dialogs = {}
        async for dialog in app.get_dialogs():
            cached_dialogs[dialog.chat.id] = dialog.chat
            # Save chat information
            chat_entry = {
                "type": str(dialog.chat.type.name),
            }

            if hasattr(dialog.chat, "username") and dialog.chat.username:
                chat_entry["username"] = dialog.chat.username

            # Add readable name (title for groups/channels or first_name for private chats)
            if hasattr(dialog.chat, "title") and dialog.chat.title:
                chat_entry["name"] = dialog.chat.title
            elif hasattr(dialog.chat, "first_name"):
                chat_entry["name"] = (
                    f"{dialog.chat.first_name} {getattr(dialog.chat, 'last_name', '')}".strip()
                )

            chat_info[dialog.chat.id] = chat_entry

        print(f"Загружено {len(cached_dialogs)} диалогов")

        # Collect all unique IDs of chats to check
        all_chat_ids = set(source_chat_ids)
        for dest_ids in forwarding_config.values():
            all_chat_ids.update(dest_ids)

        # Set to store problematic chat IDs
        problematic_chats = set()

        # Check all chats one by one
        for chat_id in all_chat_ids:
            if chat_id in cached_dialogs:
                print(f"Доступ к чату {chat_id} подтвержден (из кэша диалогов)")
            else:
                try:
                    # Try to get chat information directly if it's not in dialogs
                    chat = await app.get_chat(chat_id)
                    print(f"Доступ к чату {chat_id} подтвержден (прямой запрос)")

                    # Save chat information
                    if hasattr(chat, "username") and chat.username:
                        chat_info[chat_id] = {
                            "username": chat.username,
                            "type": str(chat.type.name),
                        }
                    else:
                        chat_info[chat_id] = {"type": str(chat.type.name)}
                except Exception as e:
                    print(f"Ошибка доступа к чату {chat_id}: {e}")
                    problematic_chats.add(chat_id)

        if problematic_chats:
            print(f"Найдено недоступных чатов: {len(problematic_chats)}")

        # Delete problematic source chats
        for source_id in list(source_chat_ids):
            if source_id in problematic_chats:
                source_chat_ids.remove(source_id)
                if source_id in forwarding_config:
                    del forwarding_config[source_id]
                    print(f"Чат {source_id} удален из конфигурации (недоступен)")

        # Delete problematic destination chats
        for source_id in list(forwarding_config.keys()):
            dest_ids = forwarding_config[source_id]
            for dest_id in list(dest_ids):
                if dest_id in problematic_chats:
                    forwarding_config[source_id].remove(dest_id)
                    print(
                        f"Чат назначения {dest_id} удален из конфигурации (недоступен)"
                    )

            # If there is no more destination chats, delete the source completely
            if not forwarding_config[source_id]:
                del forwarding_config[source_id]
                if source_id in source_chat_ids:
                    source_chat_ids.remove(source_id)
                print(
                    f"Исходный чат {source_id} удален из конфигурации (нет доступных чатов назначения)"
                )

        # Save the updated config
        self.save_config(source_chat_ids, forwarding_config, chat_info)
        print("Конфигурация обновлена после проверки доступа к чатам")

        return source_chat_ids, forwarding_config, chat_info

    async def configure_forwarding(self):
        """
        Configure forwarding messages
        Returns:
            tuple: (forwarding_folder_exists, chats_config, chats_info)
        """
        try:
            print("\n=== Проверка папки для пересылки сообщений ===")
            target_folder_exists, target_folder = await get_dialog_folder(
                app, self.forward_chats_dir
            )

            if not target_folder_exists:
                return False, {}, {}

            # Get list of chats from the folder
            chat_info = {}
            folder_chats = []

            # Dictionary for storing information about chats by number in the list
            dialog_dict = {}

            # Collect all dialogs for fast search
            all_dialogs = {}
            print("Загрузка всех доступных диалогов...")
            async for dialog in app.get_dialogs():
                all_dialogs[dialog.chat.id] = dialog.chat

            # Collect chats from the folder
            print(f"Получение чатов из папки '{self.forward_chats_dir}'...")
            for peer in target_folder.include_peers:
                chat_id = None

                if hasattr(peer, "channel_id"):
                    chat_id = (
                        -1000000000000 - peer.channel_id
                    )
                elif hasattr(peer, "chat_id"):
                    chat_id = -peer.chat_id
                elif hasattr(peer, "user_id"):
                    chat_id = peer.user_id

                if chat_id:
                    folder_chats.append(chat_id)
                    chat_info[chat_id] = {}

            print(f"Найдено {len(folder_chats)} чатов")

            # Get additional information about chats for display in interactive menu
            # Start numbering from 1
            chat_index = 1

            for chat_id in folder_chats:
                # Check if chat is already loaded in dialogs
                if chat_id in all_dialogs:
                    chat = all_dialogs[chat_id]

                    # Get chat type
                    chat_info[chat_id]["type"] = str(chat.type.name)

                    # Определяем имя чата
                    if hasattr(chat, "title") and chat.title:
                        chat_name = chat.title
                    elif hasattr(chat, "first_name"):
                        chat_name = f"{chat.first_name} {chat.last_name or ''}".strip()
                    else:
                        chat_name = f"Чат {chat_id}"


                    # Save username if it exists
                    if hasattr(chat, "username") and chat.username:
                        chat_info[chat_id]["username"] = chat.username
                    # Save chat name
                    chat_info[chat_id]["name"] = chat_name

                    dialog_dict[chat_index] = {
                        "id": chat_id,
                        "name": chat_name,
                        "type": chat.type,
                    }

                    chat_index += 1
                else:
                    # If chat is not in dialogs
                    chat_name = f"Чат {chat_id}"
                    print(
                        f"- [{chat_index}] {chat_name} (ID: {chat_id}, type: {chat_info[chat_id]['type']}) - ограниченный доступ, будет проигнорирован"
                    )
                    chat_index += 1

            if len(dialog_dict) == 0:
                print(
                    f"⚠️ Папка '{self.forward_chats_dir}' не содержит доступных чатов."
                )
                return False, {}, {}

            # Start interactive setup
            source_chat_ids, forwarding_config = await self.interactive_setup(
                dialog_dict
            )

            # Save config to file
            self.save_config(source_chat_ids, forwarding_config, chat_info)

            return True, forwarding_config, chat_info

        except Exception as e:
            log.exception(f"⚠️ Ошибка при проверке папки: {str(e)}")
            return False, {}, {}
