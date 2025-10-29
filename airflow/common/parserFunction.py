# parser_telegram.py
import os
from datetime import datetime, timedelta, date
from typing import List, Dict, Tuple, Optional
import pytz
import asyncio
from getpass import getpass

from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError, SessionPasswordNeededError

# ==== Конфигурация =======================================
try:
    from config import TELEGRAM_API_ID as _CFG_API_ID
    from config import TELEGRAM_API_HASH as _CFG_API_HASH
    from config import TIMEZONE as _CFG_TIMEZONE
except Exception:
    _CFG_API_ID = None
    _CFG_API_HASH = None
    _CFG_TIMEZONE = None


TELEGRAM_API_ID: int = int(os.getenv("TELEGRAM_API_ID", _CFG_API_ID or "0"))
TELEGRAM_API_HASH: str = os.getenv("TELEGRAM_API_HASH", _CFG_API_HASH or "")

TIMEZONE: str = os.getenv("TIMEZONE", _CFG_TIMEZONE or "Europe/Moscow")

SESSION_DIR = os.getenv("TELEGRAM_SESSION_DIR", "session")
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "session_reader")
os.makedirs(SESSION_DIR, exist_ok=True)
SESSION_PATH = os.path.join(SESSION_DIR, SESSION_NAME)

SAFETY_MARGIN_MIN = 2

def _tz() -> pytz.BaseTzInfo:
    return pytz.timezone(TIMEZONE)

def _ensure_aware_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return pytz.UTC.localize(dt)
    return dt.astimezone(pytz.UTC)

def _to_iso_utc(dt: datetime) -> str:
    dt_utc = _ensure_aware_utc(dt)
    return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _parse_local_date_yyyy_mm_dd(d: str) -> date:
    try:
        return datetime.strptime(d.strip(), "%Y-%m-%d").date()
    except Exception:
        raise ValueError("Ожидается дата в формате 'YYYY-MM-DD'.")

async def _ensure_authorized(client: TelegramClient, *, interactive_login: bool = True) -> bool:
    """
    Гарантирует авторизацию клиента.
    Если нет — в интерактивном режиме попросит телефон, код и (если нужно) 2FA.
    Возвращает True/False (успех/неуспех).
    """
    await client.connect()
    try:
        if await client.is_user_authorized():
            return True

        if not interactive_login:
            print("[parser] ⚠️ Клиент не авторизован и interactive_login=False — пропуск.")
            return False

        print("🔐 Первая авторизация Telegram (сессия будет сохранена).")
        phone = input("📞 Введите номер телефона (формат +79998887766): ").strip()
        if not phone:
            print("[parser] ⚠️ Пустой номер телефона.")
            return False

        await client.send_code_request(phone)
        code = input("🔢 Код из Telegram (SMS/приложение): ").strip()

        try:
            await client.sign_in(phone=phone, code=code)
        except SessionPasswordNeededError:
            pwd = getpass("🔑 Пароль 2FA: ")
            await client.sign_in(password=pwd)

        print(f"✅ Авторизация выполнена, сессия сохранена: {SESSION_PATH}")
        return True

    except Exception as e:
        print(f"[parser] ❌ Ошибка авторизации: {e}")
        return False

# ==== Сборщик сообщений одного канала ========================================
async def _fetch_channel_messages_window(
    client: TelegramClient,
    channel: str,
    since_utc: datetime,
    until_utc: datetime,
) -> List[Tuple[str, str]]:
    rows: List[Tuple[str, str]] = []
    hard_until = until_utc + timedelta(minutes=SAFETY_MARGIN_MIN)

    try:
        async for msg in client.iter_messages(
            channel,
            offset_date=since_utc,
            limit=None,
            reverse=True
        ):
            if not getattr(msg, "date", None):
                continue
            if msg.date >= hard_until:
                break
            if msg.date < since_utc or msg.date >= until_utc:
                continue

            text = (msg.message or "").strip()
            if not text:
                continue

            rows.append((_to_iso_utc(msg.date), text))

    except ChannelPrivateError:
        print(f"[parser] ⚠️ Нет доступа к {channel} — пропуск.")
        return []
    except FloodWaitError as e:
        print(f"[parser] ⚠️ FloodWait {e.seconds}s на {channel} — пропуск.")
        return []
    except Exception as e:
        print(f"[parser] ⚠️ Ошибка при чтении {channel}: {e} — пропуск.")
        return []

    print(f"[parser] {channel}: собрано {len(rows)} сообщений в окне.")
    return rows

async def parse_telegram_channels_to_json(
    channels: List[str],
    from_date_local_inclusive: str,
    *,
    timezone: Optional[str] = None,
    now_utc: Optional[datetime] = None,
    interactive_login: bool = True,   # <-- добавлено
) -> Dict[str, List[Tuple[str, str]]]:
    """
    Возвращает:
      { "<channel>": [(iso_datetime_utc, text), ...], ... }
    """
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
        raise RuntimeError("TELEGRAM_API_ID/TELEGRAM_API_HASH не заданы.")

    tz = pytz.timezone(timezone) if timezone else _tz()
    day0: date = _parse_local_date_yyyy_mm_dd(from_date_local_inclusive)
    since_local_dt = tz.localize(datetime(day0.year, day0.month, day0.day, 0, 0, 0))
    since_utc = since_local_dt.astimezone(pytz.UTC)

    _now_utc = _ensure_aware_utc(now_utc or datetime.utcnow())
    until_utc = _now_utc

    print(
        f"[parser] Окно: {since_utc:%Y-%m-%d %H:%M}Z - {until_utc:%Y-%m-%d %H:%M}Z "
        f"(локально: {since_local_dt.strftime('%Y-%m-%d %H:%M')} {tz.zone} - now)"
    )

    # нормализуем каналы
    norm_channels: List[str] = []
    seen = set()
    for ch in channels or []:
        ch = (ch or "").strip()
        if ch and ch not in seen:
            norm_channels.append(ch)
            seen.add(ch)

    if not norm_channels:
        print("[parser] Пустой список каналов — пустой результат.")
        return {}

    client = TelegramClient(SESSION_PATH, TELEGRAM_API_ID, TELEGRAM_API_HASH)

    # <-- авторизация внутри
    ok = await _ensure_authorized(client, interactive_login=interactive_login)
    if not ok:
        await client.disconnect()
        return {ch: [] for ch in norm_channels}

    try:
        result: Dict[str, List[Tuple[str, str]]] = {ch: [] for ch in norm_channels}
        for ch in norm_channels:
            rows = await _fetch_channel_messages_window(client, ch, since_utc, until_utc)
            result[ch] = rows
        return result
    finally:
        await client.disconnect()

async def main(channels, date_str):
    return await parse_telegram_channels_to_json(channels, date_str, interactive_login=True)

if __name__ == "__main__":
    chs = ["@rbc_news", "@tass_agency"]
    out = asyncio.run(main(chs, "2025-10-21"))
    print(out)

#SOURCE_CHANNELS = [
#    "@interfax_news",
#    "@kommersant",
#    "@rbc_news",
#    "@banksta",
#    "@cbonds",
#]