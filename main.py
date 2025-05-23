import asyncio

from src import main
from src.logger import logger as log


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("Бот остановлен.")
    except Exception as e:
        log.exception(f"Произошла ошибка: {e}")
