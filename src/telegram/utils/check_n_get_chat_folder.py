from pyrogram.raw import functions


async def get_dialog_folder(app, folder_name: str):
        """
        Checks if folder exists and returns it

        Args:
            app: Pyrogram app instance
            folder_name (str): Name of the folder to check

        Returns:
            tuple[bool, Optional[DialogFilter]]: (Is folder found, folder object or None)
        """
        try:
            response = await app.invoke(functions.messages.GetDialogFilters())
            target_folder = next(
                (f for f in response if getattr(f, "title", "") == folder_name), None
            )

            if not target_folder:
                print(
                    f"❌ Папка '{folder_name}' не найдена. "
                    f"Создайте папку с именем '{folder_name}' и добавьте нужные чаты."
                )
                return False, None

            print(f"✅ Папка '{folder_name}' найдена! ID: {target_folder.id}")
            return True, target_folder

        except Exception as e:
            print(f"⚠️ Ошибка при получении списка папок: {str(e)}")
            return False, None
