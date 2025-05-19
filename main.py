import asyncio
import csv
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set, Tuple

from telethon import TelegramClient, events, errors

# ─────────────────────────────── CONFIG ─────────────────────────────── #

API_ID: int | None = int(os.getenv("API_ID", "0")) or None
API_HASH: str | None = os.getenv("API_HASH")
BOT_TOKEN: str | None = os.getenv("BOT_TOKEN")

if not all((API_ID, API_HASH, BOT_TOKEN)):
    raise RuntimeError("Не заданы переменные окружения API_ID / API_HASH / BOT_TOKEN")

# группы мониторинга
keyword_groups: List[dict] = [
    {
        "name": "wallets",                           # произвольное «человеческое» название
        "keywords_file": "wallets_keywords.txt",     # список искомых адресов/слов
        "target_chat_id": int(os.getenv("TARGET_CHAT_ID", "0")),
        "csv_file": "log.csv",                       # журнал совпадений (можно None)
    },
]

PROCESSED_TTL = 24 * 60 * 60  # 24 ч — кэш уже обработанных сообщений

# ────────────────────────────── LOGGING ─────────────────────────────── #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("parser")

# ───────────────────────────── UTILITIES ────────────────────────────── #


def load_keywords(path: str) -> Set[str]:
    """Читает файл ключевых слов, игнорируя пустые строки и комментарии."""
    p = Path(path)
    if not p.exists():
        logger.warning("Файл %s не найден — ключевых слов 0", path)
        return set()
    with p.open(encoding="utf-8") as fh:
        return {ln.strip().lower() for ln in fh if ln.strip() and not ln.startswith("#")}


def find_keyword(text: str, keywords: Set[str]) -> str | None:
    """Вернёт первое ключевое слово, найденное в тексте, иначе None."""
    text_lc = text.lower()
    for kw in keywords:
        if kw in text_lc:
            return kw
    return None


def tg_link(chat, msg_id: int) -> str:
    """Строит публичную ссылку на сообщение."""
    if chat.username:
        return f"https://t.me/{chat.username}/{msg_id}"
    return f"https://t.me/c/{abs(chat.id) - 10 ** 12}/{msg_id}"


def purge(cache: Dict[Tuple[int, int], float]) -> None:
    """Удаляет устаревшие элементы из кэша processed."""
    now = time.time()
    for key in [k for k, ts in cache.items() if now - ts > PROCESSED_TTL]:
        del cache[key]


# ────────────────────────────── WRAPPER ─────────────────────────────── #


class GroupData:
    __slots__ = ("name", "keywords", "target_chat_id", "csv_writer")

    def __init__(self, cfg: dict):
        self.name: str = cfg["name"]
        self.keywords: Set[str] = load_keywords(cfg["keywords_file"])
        self.target_chat_id: int = cfg["target_chat_id"]

        csv_file = cfg.get("csv_file")
        if csv_file:
            path = Path(csv_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            self.csv_writer = path.open("a", newline="", encoding="utf-8")
            if path.stat().st_size == 0:
                csv.writer(self.csv_writer).writerow(
                    ["datetime_utc", "chat_id", "message_id", "text"]
                )
        else:
            self.csv_writer = None


# ─────────────────────────────── MAIN ──────────────────────────────── #


async def main() -> None:
    """Создаём клиентов в рамках одного event-loop и запускаем парсер."""
    user_client = TelegramClient("user_session", API_ID, API_HASH)
    bot_client = TelegramClient("bot_session", API_ID, API_HASH)

    await bot_client.start(bot_token=BOT_TOKEN)
    await user_client.start()

    groups = [GroupData(cfg) for cfg in keyword_groups]
    processed: Dict[Tuple[int, int], float] = {}

    # ―――――――― handler ―――――――― #
    async def on_new_message(event: events.NewMessage.Event) -> None:
        msg = event.message
        chat = await event.get_chat()

        key = (chat.id, msg.id)
        if key in processed:
            return
        processed[key] = time.time()
        purge(processed)

        for g in groups:
            if chat.id == g.target_chat_id:
                continue  # не ловим собственные пересланные сообщения

            kw = find_keyword(msg.message or "", g.keywords)
            if kw:
                link = tg_link(chat, msg.id)
                text = (
                    f"🚨 Найдено совпадение по адресу кошелька:<b>{kw}</b>\n"
                    f"{msg.message}\n\n"
                    f'<a href="{link}">Открыть сообщение</a>'
                )
                try:
                    await bot_client.send_message(
                        g.target_chat_id,
                        text,
                        parse_mode="html",
                        link_preview=False,
                    )
                    logger.info("Переслано сообщение с ключом «%s» - %s", kw, link)
                except errors.rpcerrorlist.FloodWaitError as e:
                    logger.warning("FloodWait %d s", e.seconds)
                    await asyncio.sleep(e.seconds)

                if g.csv_writer:
                    csv.writer(g.csv_writer).writerow(
                        [
                            datetime.now(timezone.utc).isoformat(timespec="seconds"),
                            chat.id,
                            msg.id,
                            (msg.message or "").replace("\n", " "),
                        ]
                    )
                    g.csv_writer.flush()

    user_client.add_event_handler(on_new_message, events.NewMessage)

    logger.info("Парсер запущен, ждём сообщений…")
    await user_client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Завершено пользователем")
