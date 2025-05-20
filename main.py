import asyncio
import csv
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional

from telethon import TelegramClient, events, errors
from telethon.tl.types import Channel  # для проверки типа чата

# ─────────────────────────────── CONFIG ─────────────────────────────── #

API_ID: Optional[int] = int(os.getenv("API_ID", "0")) or None
API_HASH: Optional[str] = os.getenv("API_HASH")
BOT_TOKEN: Optional[str] = os.getenv("BOT_TOKEN")

if not all((API_ID, API_HASH, BOT_TOKEN)):
    raise RuntimeError("API_ID / API_HASH / BOT_TOKEN должны быть заданы в переменных окружения")

keyword_groups: List[dict] = [
    {
        "name": "wallets",
        "keywords_file": "wallets_keywords.txt",
        "target_chat_id": int(os.getenv("TARGET_CHAT_ID", "0")),
        "csv_file": "log.csv",
    },
]

PROCESSED_TTL = 24 * 60 * 60  # 24 ч — время жизни кэша дубликатов

# ────────────────────────────── LOGGING ─────────────────────────────── #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("parser")

# ───────────────────────────── UTILITIES ────────────────────────────── #


def load_keywords(path: str) -> Dict[str, str]:
    """
    Возвращает словарь {keyword_lower: alias}.

    Поддерживаемые форматы строк в wallets_keywords.txt:
        keyword|alias
        keyword:alias
        keyword,alias
        keyword            # alias = keyword

    Пустые строки и строки, начинающиеся с «#», игнорируются.
    """
    p = Path(path)
    if not p.exists():
        logger.warning("Файл ключевых слов %s не найден — список пуст", path)
        return {}

    mapping: Dict[str, str] = {}
    with p.open(encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            for sep in ("|", ":", ","):
                if sep in line:
                    kw, alias = map(str.strip, line.split(sep, 1))
                    break
            else:
                kw, alias = line, line

            mapping[kw.lower()] = alias
    return mapping


def find_keyword(text: str, kw_map: Dict[str, str]) -> Optional[str]:
    """
    Возвращает псевдоним первого найденного ключевого слова
    или None, если совпадений нет.
    """
    text_lc = text.lower()
    for kw, alias in kw_map.items():
        if kw in text_lc:
            return alias
    return None


def tg_link(chat, msg_id: int) -> str:
    """
    Пытается построить публичную ссылку на сообщение.
    • Публичные каналы/супергруппы — https://t.me/<username>/<id>
    • Приватные супергруппы       — https://t.me/c/<internal>/<id>
    • Малые приватные группы ссылки не имеют → «—»
    """
    username = getattr(chat, "username", None)
    if username:  # публичный канал/супергруппа или пользователь
        return f"https://t.me/{username}/{msg_id}"

    if isinstance(chat, Channel):  # приватная супергруппа
        return f"https://t.me/c/{abs(chat.id) - 10 ** 12}/{msg_id}"

    return "—"  # обычная (малая) группа


class DupCache:
    """Простейший TTL-кэш, позволяющий фильтровать дубликаты сообщений."""

    def __init__(self, ttl: int = PROCESSED_TTL):
        self.ttl = ttl
        self._cache: Dict[Tuple[int, int], float] = {}

    def add(self, chat_id: int, msg_id: int) -> bool:
        """True, если такого (chat_id, msg_id) не было за последние ttl секунд."""
        key = (chat_id, msg_id)
        now = time.time()
        # чистим просроченные записи
        self._cache = {k: v for k, v in self._cache.items() if now - v < self.ttl}
        if key in self._cache:
            return False
        self._cache[key] = now
        return True


class GroupData:
    __slots__ = ("name", "keywords", "target_chat_id", "csv_writer")

    def __init__(self, cfg: dict):
        self.name: str = cfg["name"]
        # теперь это Dict[str, str] {keyword: alias}
        self.keywords: Dict[str, str] = load_keywords(cfg["keywords_file"])
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


# ───────────────────────────── MAIN LOGIC ────────────────────────────── #


async def main() -> None:
    """Создаём клиентов в одном event-loop и запускаем парсер."""
    user_client = TelegramClient("user_session", API_ID, API_HASH)
    bot_client = TelegramClient("bot_session", API_ID, API_HASH)

    await bot_client.start(bot_token=BOT_TOKEN)
    await user_client.start()

    groups = [GroupData(cfg) for cfg in keyword_groups]
    dup_cache = DupCache()

    # ――――――――― handler ――――――――― #
    async def on_new_message(event: events.NewMessage.Event) -> None:
        msg = event.message
        chat = await event.get_chat()

        # смотрим, подходит ли чат к какому-нибудь из настроенных
        g: Optional[GroupData] = next((x for x in groups if x.target_chat_id and chat.id), None)
        if g is None:
            return

        # фильтр дубликатов
        if not dup_cache.add(chat.id, msg.id):
            return

        # не реагируем на собственные уведомления, чтобы не зациклиться
        if msg.out and msg.to_id and getattr(msg.to_id, "channel_id", None) == g.target_chat_id:
            return

        kw_alias = find_keyword(msg.message or "", g.keywords)
        if kw_alias:
            link = tg_link(chat, msg.id)
            anchor = (
                f'<a href="{link}">Открыть сообщение</a>' if link != "—" else "Ссылка недоступна"
            )
            text = (
                f"🚨 Найдено совпадение по адресу:\n<b>{kw_alias}</b>\n"
                f"Оригинал сообщения:\n"
                f"{msg.message}\n"
                f"{anchor}"
            )

            try:
                await bot_client.send_message(
                    g.target_chat_id,
                    text,
                    parse_mode="html",
                    link_preview=True,
                )
                logger.info("➡️  Совпадение «%s» переслано (%s)", kw_alias, link)
            except errors.ChatWriteForbiddenError:
                logger.error("❌ Нет прав писать в целевой чат %s", g.target_chat_id)
            except Exception as exc:
                logger.exception("Ошибка при отправке сообщения: %s", exc)

        # логируем всё, если задан csv_file
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

    logger.info("Парсер запущен, ждём сообщения…")
    await user_client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Завершено пользователем")
