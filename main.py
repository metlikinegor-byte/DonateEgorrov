"""Telegram-бот для пожертвований через Telegram Stars (XTR) + сервер Mini App — в одном файле.

Что внутри:
  • бот на aiogram 3: /start, /help, /terms, /paysupport, админские /stats и /refund;
  • приём оплаты Stars: pre_checkout_query и successful_payment с проверкой суммы, валюты и payload;
  • SQLite: счета и пожертвования, защита от повторного учёта одной оплаты;
  • HTTP API для Mini App (aiohttp) с проверкой подписи Telegram initData;
  • раздача собранного фронтенда (папка frontend/dist).

Запуск:
  pip install -r requirements.txt
  python main.py

Настройки — в файле .env рядом с main.py (см. .env.example): BOT_TOKEN, ADMIN_ID, WEBAPP_URL.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import html
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import parse_qsl, urlparse

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    MenuButtonWebApp,
    Message,
    PreCheckoutQuery,
    WebAppInfo,
)
from aiohttp import web
from dotenv import load_dotenv


# ======================================================================
# НАСТРОЙКИ (.env)
# ======================================================================

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

# .env ищется сначала рядом с backend/, затем в корне проекта
load_dotenv(BASE_DIR / ".env")
load_dotenv(PROJECT_ROOT / ".env")


class ConfigError(RuntimeError):
    """Ошибка конфигурации: не задана или неверно задана переменная окружения."""


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(
            f"Переменная окружения {name} не задана. "
            f"Скопируйте .env.example в .env и заполните значения."
        )
    return value


def _int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"Переменная {name} должна быть целым числом, получено: {raw!r}") from exc


def _origin(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}"


@dataclass(frozen=True)
class Settings:
    bot_token: str
    admin_id: int
    webapp_url: str
    project_name: str
    host: str
    port: int
    db_path: Path
    frontend_dist: Path
    min_amount: int
    max_amount: int
    invoice_ttl_seconds: int
    init_data_ttl_seconds: int
    cors_origins: tuple[str, ...] = field(default_factory=tuple)

    @property
    def preset_amounts(self) -> tuple[int, ...]:
        return (10, 25, 50, 100, 250, 500)


def load_settings() -> Settings:
    bot_token = _required("BOT_TOKEN")
    admin_raw = _required("ADMIN_ID")
    try:
        admin_id = int(admin_raw)
    except ValueError as exc:
        raise ConfigError(f"ADMIN_ID должен быть числом (ваш Telegram ID), получено: {admin_raw!r}") from exc

    webapp_url = _required("WEBAPP_URL").rstrip("/")
    if not webapp_url.startswith("https://"):
        raise ConfigError("WEBAPP_URL должен начинаться с https:// — Telegram открывает Mini App только по HTTPS.")

    min_amount = max(1, _int("MIN_AMOUNT", 1))
    max_amount = _int("MAX_AMOUNT", 10000)
    if max_amount < min_amount:
        raise ConfigError("MAX_AMOUNT не может быть меньше MIN_AMOUNT.")

    extra_origins = [o.strip().rstrip("/") for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
    origins = {o for o in [_origin(webapp_url), *extra_origins] if o}

    db_raw = os.getenv("DB_PATH", "").strip()
    db_path = Path(db_raw) if db_raw else BASE_DIR / "data" / "donations.sqlite3"
    if not db_path.is_absolute():
        db_path = BASE_DIR / db_path

    dist_raw = os.getenv("FRONTEND_DIST", "").strip()
    frontend_dist = Path(dist_raw) if dist_raw else PROJECT_ROOT / "frontend" / "dist"
    if not frontend_dist.is_absolute():
        frontend_dist = BASE_DIR / frontend_dist

    return Settings(
        bot_token=bot_token,
        admin_id=admin_id,
        webapp_url=webapp_url,
        project_name=os.getenv("PROJECT_NAME", "").strip() or "Lumi",
        host=os.getenv("HOST", "").strip() or "0.0.0.0",
        port=_int("PORT", 8080),
        db_path=db_path,
        frontend_dist=frontend_dist,
        min_amount=min_amount,
        max_amount=max_amount,
        invoice_ttl_seconds=_int("INVOICE_TTL_SECONDS", 3600),
        init_data_ttl_seconds=_int("INIT_DATA_TTL_SECONDS", 86400),
        cors_origins=tuple(sorted(origins)),
    )


# ======================================================================
# БАЗА ДАННЫХ SQLite
# ======================================================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS invoices (
    payload      TEXT PRIMARY KEY,
    user_id      INTEGER NOT NULL,
    amount       INTEGER NOT NULL CHECK (amount > 0),
    anonymous    INTEGER NOT NULL DEFAULT 0,
    message      TEXT,
    status       TEXT NOT NULL DEFAULT 'pending',   -- pending | paid
    created_at   INTEGER NOT NULL,
    paid_at      INTEGER
);
CREATE INDEX IF NOT EXISTS idx_invoices_user_created ON invoices (user_id, created_at);

CREATE TABLE IF NOT EXISTS donations (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    payload                     TEXT NOT NULL REFERENCES invoices (payload),
    user_id                     INTEGER NOT NULL,
    display_name                TEXT,
    username                    TEXT,
    amount                      INTEGER NOT NULL CHECK (amount > 0),
    currency                    TEXT NOT NULL,
    anonymous                   INTEGER NOT NULL DEFAULT 0,
    message                     TEXT,
    telegram_payment_charge_id  TEXT NOT NULL UNIQUE,
    provider_payment_charge_id  TEXT,
    refunded                    INTEGER NOT NULL DEFAULT 0,
    created_at                  INTEGER NOT NULL,
    refunded_at                 INTEGER
);
CREATE INDEX IF NOT EXISTS idx_donations_user ON donations (user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_donations_created ON donations (created_at);
"""


@dataclass(frozen=True)
class Invoice:
    payload: str
    user_id: int
    amount: int
    anonymous: bool
    message: str | None
    status: str
    created_at: int
    paid_at: int | None


@dataclass(frozen=True)
class Donation:
    id: int
    payload: str
    user_id: int
    display_name: str | None
    username: str | None
    amount: int
    currency: str
    anonymous: bool
    message: str | None
    telegram_payment_charge_id: str
    refunded: bool
    created_at: int

    def to_private_dict(self) -> dict[str, Any]:
        """Данные для владельца пожертвования (экран «Мои донаты»)."""
        return {
            "id": self.id,
            "amount": self.amount,
            "anonymous": self.anonymous,
            "message": self.message,
            "refunded": self.refunded,
            "created_at": self.created_at,
        }


def _invoice(row: aiosqlite.Row) -> Invoice:
    return Invoice(
        payload=row["payload"],
        user_id=row["user_id"],
        amount=row["amount"],
        anonymous=bool(row["anonymous"]),
        message=row["message"],
        status=row["status"],
        created_at=row["created_at"],
        paid_at=row["paid_at"],
    )


def _donation(row: aiosqlite.Row) -> Donation:
    return Donation(
        id=row["id"],
        payload=row["payload"],
        user_id=row["user_id"],
        display_name=row["display_name"],
        username=row["username"],
        amount=row["amount"],
        currency=row["currency"],
        anonymous=bool(row["anonymous"]),
        message=row["message"],
        telegram_payment_charge_id=row["telegram_payment_charge_id"],
        refunded=bool(row["refunded"]),
        created_at=row["created_at"],
    )


class Database:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._conn: aiosqlite.Connection | None = None
        # Одно соединение на всё приложение: запись сериализуем, чтобы транзакции не перемешивались
        self._write_lock = asyncio.Lock()

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("База данных не подключена: вызовите Database.connect()")
        return self._conn

    async def connect(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self._path, isolation_level=None)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.execute("PRAGMA busy_timeout=5000")
        await self._conn.executescript(SCHEMA)

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    # ---------- Счета ----------

    async def create_invoice(
        self,
        *,
        payload: str,
        user_id: int,
        amount: int,
        anonymous: bool,
        message: str | None,
    ) -> Invoice:
        now = int(time.time())
        async with self._write_lock:
            await self.conn.execute(
                "INSERT INTO invoices (payload, user_id, amount, anonymous, message, status, created_at) "
                "VALUES (?, ?, ?, ?, ?, 'pending', ?)",
                (payload, user_id, amount, int(anonymous), message, now),
            )
        return Invoice(payload, user_id, amount, anonymous, message, "pending", now, None)

    async def get_invoice(self, payload: str) -> Invoice | None:
        async with self.conn.execute("SELECT * FROM invoices WHERE payload = ?", (payload,)) as cur:
            row = await cur.fetchone()
        return _invoice(row) if row else None

    async def count_recent_invoices(self, user_id: int, window_seconds: int) -> int:
        since = int(time.time()) - window_seconds
        async with self.conn.execute(
            "SELECT COUNT(*) FROM invoices WHERE user_id = ? AND created_at >= ?", (user_id, since)
        ) as cur:
            row = await cur.fetchone()
        return int(row[0]) if row else 0

    async def cleanup_stale_invoices(self, older_than_seconds: int) -> int:
        """Удаляет неоплаченные счета старше указанного возраста (по ним нет пожертвований)."""
        threshold = int(time.time()) - older_than_seconds
        async with self._write_lock:
            cur = await self.conn.execute(
                "DELETE FROM invoices WHERE status = 'pending' AND created_at < ? "
                "AND payload NOT IN (SELECT payload FROM donations)",
                (threshold,),
            )
            return cur.rowcount or 0

    # ---------- Пожертвования ----------

    async def record_payment(
        self,
        *,
        invoice: Invoice,
        amount: int,
        currency: str,
        telegram_payment_charge_id: str,
        provider_payment_charge_id: str | None,
        display_name: str | None,
        username: str | None,
    ) -> tuple[bool, Donation]:
        """
        Атомарно учитывает оплату.
        Возвращает (True, donation), если оплата учтена впервые,
        и (False, donation), если этот telegram_payment_charge_id уже был обработан.
        """
        now = int(time.time())
        async with self._write_lock:
            await self.conn.execute("BEGIN IMMEDIATE")
            try:
                async with self.conn.execute(
                    "SELECT * FROM donations WHERE telegram_payment_charge_id = ?",
                    (telegram_payment_charge_id,),
                ) as cur:
                    existing = await cur.fetchone()
                if existing:
                    await self.conn.execute("ROLLBACK")
                    return False, _donation(existing)

                cur = await self.conn.execute(
                    "INSERT INTO donations (payload, user_id, display_name, username, amount, currency, "
                    "anonymous, message, telegram_payment_charge_id, provider_payment_charge_id, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        invoice.payload,
                        invoice.user_id,
                        display_name,
                        username,
                        amount,
                        currency,
                        int(invoice.anonymous),
                        invoice.message,
                        telegram_payment_charge_id,
                        provider_payment_charge_id,
                        now,
                    ),
                )
                donation_id = cur.lastrowid
                await self.conn.execute(
                    "UPDATE invoices SET status = 'paid', paid_at = COALESCE(paid_at, ?) WHERE payload = ?",
                    (now, invoice.payload),
                )
                async with self.conn.execute("SELECT * FROM donations WHERE id = ?", (donation_id,)) as cur2:
                    row = await cur2.fetchone()
                await self.conn.execute("COMMIT")
            except Exception:
                await self.conn.execute("ROLLBACK")
                raise
        assert row is not None
        return True, _donation(row)

    async def get_donation_by_charge(self, charge_id: str) -> Donation | None:
        async with self.conn.execute(
            "SELECT * FROM donations WHERE telegram_payment_charge_id = ?", (charge_id,)
        ) as cur:
            row = await cur.fetchone()
        return _donation(row) if row else None

    async def get_donation_by_payload(self, payload: str) -> Donation | None:
        async with self.conn.execute(
            "SELECT * FROM donations WHERE payload = ? ORDER BY id DESC LIMIT 1", (payload,)
        ) as cur:
            row = await cur.fetchone()
        return _donation(row) if row else None

    async def mark_refunded(self, charge_id: str) -> bool:
        async with self._write_lock:
            cur = await self.conn.execute(
                "UPDATE donations SET refunded = 1, refunded_at = ? "
                "WHERE telegram_payment_charge_id = ? AND refunded = 0",
                (int(time.time()), charge_id),
            )
            return (cur.rowcount or 0) > 0

    # ---------- Статистика ----------

    async def global_stats(self) -> dict[str, int]:
        async with self.conn.execute(
            "SELECT COALESCE(SUM(amount), 0) AS total, COUNT(*) AS cnt, COUNT(DISTINCT user_id) AS users "
            "FROM donations WHERE refunded = 0"
        ) as cur:
            row = await cur.fetchone()
        return {
            "total_stars": int(row["total"]) if row else 0,
            "donations_count": int(row["cnt"]) if row else 0,
            "supporters_count": int(row["users"]) if row else 0,
        }

    async def recent_public(self, limit: int = 5) -> list[dict[str, Any]]:
        """Публичная лента: имя анонимных доноров не раскрывается ни при каких условиях."""
        async with self.conn.execute(
            "SELECT anonymous, display_name, amount, created_at FROM donations "
            "WHERE refunded = 0 ORDER BY id DESC LIMIT ?",
            (limit,),
        ) as cur:
            rows = await cur.fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            anonymous = bool(row["anonymous"])
            result.append(
                {
                    "name": None if anonymous else (row["display_name"] or "Друг проекта"),
                    "anonymous": anonymous,
                    "amount": int(row["amount"]),
                    "created_at": int(row["created_at"]),
                }
            )
        return result

    async def user_history(self, user_id: int, limit: int = 100) -> dict[str, Any]:
        async with self.conn.execute(
            "SELECT COALESCE(SUM(amount), 0) AS total, COUNT(*) AS cnt FROM donations "
            "WHERE user_id = ? AND refunded = 0",
            (user_id,),
        ) as cur:
            agg = await cur.fetchone()
        async with self.conn.execute(
            "SELECT * FROM donations WHERE user_id = ? ORDER BY id DESC LIMIT ?", (user_id, limit)
        ) as cur:
            rows = await cur.fetchall()
        items = [_donation(r).to_private_dict() for r in rows]
        last = next((i for i in items if not i["refunded"]), None)
        return {
            "total_stars": int(agg["total"]) if agg else 0,
            "donations_count": int(agg["cnt"]) if agg else 0,
            "last": last,
            "items": items,
        }


# ======================================================================
# ПРОВЕРКА TELEGRAM initData
# ======================================================================

class InitDataError(Exception):
    """initData отсутствует, подделана или устарела."""


@dataclass(frozen=True)
class TelegramUser:
    id: int
    first_name: str
    last_name: str | None
    username: str | None
    language_code: str | None

    @property
    def display_name(self) -> str:
        full = " ".join(p for p in (self.first_name, self.last_name) if p).strip()
        return full or (f"@{self.username}" if self.username else "Друг проекта")


def validate_init_data(init_data: str, bot_token: str, max_age_seconds: int) -> TelegramUser:
    if not init_data:
        raise InitDataError("initData отсутствует")

    try:
        pairs = parse_qsl(init_data, keep_blank_values=True, strict_parsing=True)
    except ValueError as exc:
        raise InitDataError("initData повреждены") from exc

    data = dict(pairs)
    received_hash = data.pop("hash", None)
    if not received_hash:
        raise InitDataError("В initData нет подписи")

    data_check_string = "\n".join(f"{key}={value}" for key, value in sorted(data.items()))
    secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(calculated_hash, received_hash):
        raise InitDataError("Подпись initData неверна")

    try:
        auth_date = int(data.get("auth_date", "0"))
    except ValueError as exc:
        raise InitDataError("Некорректный auth_date") from exc
    now = int(time.time())
    if auth_date <= 0 or now - auth_date > max_age_seconds:
        raise InitDataError("Сессия устарела, перезапустите Mini App")

    raw_user = data.get("user")
    if not raw_user:
        raise InitDataError("В initData нет пользователя")
    try:
        user = json.loads(raw_user)
        user_id = int(user["id"])
    except (ValueError, KeyError, TypeError) as exc:
        raise InitDataError("Некорректные данные пользователя") from exc

    return TelegramUser(
        id=user_id,
        first_name=str(user.get("first_name") or ""),
        last_name=user.get("last_name"),
        username=user.get("username"),
        language_code=user.get("language_code"),
    )


# ======================================================================
# ОПЛАТА TELEGRAM STARS (XTR)
# ======================================================================

logger = logging.getLogger(__name__)

CURRENCY = "XTR"
MESSAGE_MAX_LENGTH = 200
# Не более 10 новых счетов от одного пользователя за минуту
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX = 10

payments_router = Router(name="payments")


class PaymentError(Exception):
    """Ошибка, текст которой можно безопасно показать пользователю."""


def stars_word(n: int) -> str:
    return "Star" if n == 1 else "Stars"


def normalize_message(raw: object) -> str | None:
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise PaymentError("Сообщение должно быть текстом")
    text = " ".join(raw.split())  # убираем переносы и лишние пробелы
    if not text:
        return None
    if len(text) > MESSAGE_MAX_LENGTH:
        raise PaymentError(f"Сообщение не должно быть длиннее {MESSAGE_MAX_LENGTH} символов")
    return text


def validate_amount(raw: object, settings: Settings) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise PaymentError("Сумма должна быть целым числом Stars")
    if raw < settings.min_amount or raw > settings.max_amount:
        raise PaymentError(f"Сумма должна быть от {settings.min_amount} до {settings.max_amount} Stars")
    return raw


async def create_donation_invoice(
    *,
    bot: Bot,
    db: Database,
    settings: Settings,
    user_id: int,
    amount: int,
    anonymous: bool,
    message: str | None,
) -> tuple[Invoice, str]:
    """Создаёт запись о счёте в БД и ссылку на оплату через Bot API (createInvoiceLink)."""
    recent = await db.count_recent_invoices(user_id, RATE_LIMIT_WINDOW)
    if recent >= RATE_LIMIT_MAX:
        raise PaymentError("Слишком много попыток. Подождите минуту и попробуйте снова.")

    payload = f"don_{uuid.uuid4().hex}"
    invoice = await db.create_invoice(
        payload=payload, user_id=user_id, amount=amount, anonymous=anonymous, message=message
    )

    title = f"Поддержка {settings.project_name}"[:32]
    description = f"Добровольная поддержка проекта: {amount} {stars_word(amount)}. Спасибо, что ты с нами!"
    link = await bot.create_invoice_link(
        title=title,
        description=description[:255],
        payload=payload,
        provider_token="",  # для Telegram Stars токен провайдера не нужен
        currency=CURRENCY,
        prices=[LabeledPrice(label="Поддержка проекта", amount=amount)],
    )
    return invoice, link


def _check_invoice(invoice: Invoice | None, *, user_id: int, amount: int, currency: str) -> str | None:
    """Возвращает текст ошибки или None, если счёт корректен."""
    if invoice is None:
        return "Счёт не найден. Откройте Mini App и попробуйте ещё раз."
    if currency != CURRENCY:
        return "Неверная валюта платежа."
    if invoice.user_id != user_id:
        return "Этот счёт создан для другого пользователя."
    if invoice.amount != amount:
        return "Сумма платежа не совпадает со счётом."
    return None


@payments_router.pre_checkout_query()
async def on_pre_checkout(query: PreCheckoutQuery, db: Database, settings: Settings) -> None:
    """Telegram спрашивает: можно ли проводить оплату? Отвечаем строго после проверки."""
    try:
        invoice = await db.get_invoice(query.invoice_payload)
        error = _check_invoice(
            invoice, user_id=query.from_user.id, amount=query.total_amount, currency=query.currency
        )
        if error is None and invoice is not None:
            if invoice.status != "pending":
                error = "Этот счёт уже оплачен. Создайте новый в Mini App."
            elif time.time() - invoice.created_at > settings.invoice_ttl_seconds:
                error = "Срок действия счёта истёк. Создайте новый в Mini App."
    except Exception:  # noqa: BLE001 — Telegram ждёт ответ не дольше 10 секунд, отвечаем в любом случае
        logger.exception("Ошибка при проверке pre_checkout_query")
        error = "Не удалось проверить платёж. Попробуйте ещё раз."

    if error:
        logger.warning("pre_checkout отклонён: %s (payload=%s)", error, query.invoice_payload)
        await query.answer(ok=False, error_message=error)
    else:
        await query.answer(ok=True)


def _admin_text(donation: Donation, settings: Settings, *, refused: str | None = None) -> str:
    when = datetime.fromtimestamp(donation.created_at, tz=timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
    who = html.escape(donation.display_name or "без имени")
    if donation.username:
        who += f" (@{html.escape(donation.username)})"
    lines = [
        f"⭐ <b>Новый донат: {donation.amount} {stars_word(donation.amount)}</b>",
        "",
        f"От: {who}, ID <code>{donation.user_id}</code>",
    ]
    if donation.anonymous:
        lines.append("🙈 Пользователь выбрал анонимность — имя скрыто в публичной статистике")
    if donation.message:
        lines.append(f"💬 {html.escape(donation.message)}")
    lines += [
        f"🕒 {when}",
        f"🧾 Charge ID: <code>{html.escape(donation.telegram_payment_charge_id)}</code>",
    ]
    if refused:
        lines.append(f"⚠️ {html.escape(refused)}")
    return "\n".join(lines)


async def notify_admin(bot: Bot, settings: Settings, text: str) -> None:
    try:
        await bot.send_message(settings.admin_id, text)
    except Exception:  # noqa: BLE001 — уведомление не должно ломать обработку оплаты
        logger.exception("Не удалось отправить уведомление администратору (ADMIN_ID=%s). "
                         "Администратор должен хотя бы раз написать боту /start.", settings.admin_id)


@payments_router.message(F.successful_payment)
async def on_successful_payment(message: Message, bot: Bot, db: Database, settings: Settings) -> None:
    """Единственное место, где пожертвование засчитывается."""
    payment = message.successful_payment
    user = message.from_user
    if payment is None or user is None:
        return

    charge_id = payment.telegram_payment_charge_id
    invoice = await db.get_invoice(payment.invoice_payload)
    error = _check_invoice(invoice, user_id=user.id, amount=payment.total_amount, currency=payment.currency)

    if error or invoice is None:
        # Деньги уже списаны, но платёж не прошёл проверку — не учитываем, сообщаем администратору для возврата
        logger.error("successful_payment не прошёл проверку: %s, charge_id=%s", error, charge_id)
        await notify_admin(
            bot,
            settings,
            "⚠️ <b>Платёж не прошёл проверку и не учтён</b>\n"
            f"Причина: {html.escape(error or 'неизвестно')}\n"
            f"Пользователь: <code>{user.id}</code>, сумма: {payment.total_amount} {html.escape(payment.currency)}\n"
            f"Charge ID: <code>{html.escape(charge_id)}</code>\n"
            f"Для возврата: <code>/refund {html.escape(charge_id)} {user.id}</code>",
        )
        await message.answer("Мы получили платёж, но не смогли его подтвердить. Администратор уже разбирается.")
        return

    full_name = " ".join(p for p in (user.first_name, user.last_name) if p).strip() or None
    created, donation = await db.record_payment(
        invoice=invoice,
        amount=payment.total_amount,
        currency=payment.currency,
        telegram_payment_charge_id=charge_id,
        provider_payment_charge_id=payment.provider_payment_charge_id,
        display_name=full_name,
        username=user.username,
    )
    if not created:
        logger.info("Повторное уведомление об оплате %s проигнорировано", charge_id)
        return

    logger.info("Донат учтён: %s XTR от %s, charge_id=%s", donation.amount, user.id, charge_id)
    await message.answer(
        f"Спасибо за поддержку! ❤️\nТы отправил(а) <b>{donation.amount} {stars_word(donation.amount)}</b>. "
        "Это правда очень помогает проекту."
    )
    await notify_admin(bot, settings, _admin_text(donation, settings))


async def refund_donation(bot: Bot, db: Database, charge_id: str, user_id: int | None = None) -> str:
    """Возврат Stars пользователю. Используется командой администратора /refund."""
    donation = await db.get_donation_by_charge(charge_id)
    if donation is None:
        if user_id is None:
            raise PaymentError("Пожертвование не найдено в базе. Для неучтённого платежа укажите ID пользователя: "
                               "/refund CHARGE_ID USER_ID")
        await bot.refund_star_payment(user_id=user_id, telegram_payment_charge_id=charge_id)
        return f"Неучтённый платёж возвращён пользователю {user_id}."
    if donation.refunded:
        raise PaymentError("Это пожертвование уже возвращено.")
    await bot.refund_star_payment(user_id=donation.user_id, telegram_payment_charge_id=charge_id)
    await db.mark_refunded(charge_id)
    return f"Возвращено {donation.amount} {stars_word(donation.amount)} пользователю {donation.user_id}."


# ======================================================================
# КОМАНДЫ БОТА
# ======================================================================

logger = logging.getLogger(__name__)
commands_router = Router(name="commands")


def open_app_keyboard(settings: Settings) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="❤️ Открыть приложение", web_app=WebAppInfo(url=settings.webapp_url))]]
    )


@commands_router.message(CommandStart())
async def cmd_start(message: Message, settings: Settings) -> None:
    name = html.escape(message.from_user.first_name) if message.from_user else "друг"
    await message.answer(
        f"Привет, {name}! ✨\n\n"
        f"Это <b>{html.escape(settings.project_name)}</b> — здесь можно поддержать проект любой удобной "
        "суммой в Telegram Stars. Каждый донат — большая помощь и мотивация двигаться дальше!\n\n"
        "Нажми кнопку ниже, чтобы открыть приложение.",
        reply_markup=open_app_keyboard(settings),
    )


@commands_router.message(Command("help"))
async def cmd_help(message: Message, settings: Settings) -> None:
    await message.answer(
        "Как поддержать проект:\n"
        "1. Открой приложение кнопкой ниже.\n"
        "2. Выбери сумму в Stars или введи свою.\n"
        "3. Подтверди оплату в окне Telegram.\n\n"
        "/terms — условия\n/paysupport — помощь с оплатой",
        reply_markup=open_app_keyboard(settings),
    )


@commands_router.message(Command("terms"))
async def cmd_terms(message: Message, settings: Settings) -> None:
    await message.answer(
        "<b>Условия</b>\n\n"
        f"Пожертвования в {html.escape(settings.project_name)} добровольны и не дают доступа к товарам или услугам. "
        "Оплата проходит только через официальную систему Telegram Stars. "
        "Если платёж прошёл по ошибке, напишите /paysupport — мы рассмотрим возврат."
    )


@commands_router.message(Command("paysupport"))
async def cmd_paysupport(message: Message, bot: Bot, settings: Settings) -> None:
    user = message.from_user
    text = (message.text or "").partition(" ")[2].strip()
    if not user or not text:
        await message.answer(
            "Опишите проблему с оплатой одним сообщением после команды, например:\n"
            "<code>/paysupport оплатил дважды</code>\n\n"
            "Администратор получит обращение и ответит вам."
        )
        return
    try:
        await bot.send_message(
            settings.admin_id,
            f"🆘 <b>Обращение по оплате</b>\nОт: {html.escape(user.full_name)} "
            f"(ID <code>{user.id}</code>)\n\n{html.escape(text[:1000])}",
        )
    except Exception:  # noqa: BLE001
        logger.exception("Не удалось переслать обращение администратору")
        await message.answer("Не получилось отправить обращение. Попробуйте позже.")
        return
    await message.answer("Обращение отправлено администратору. Спасибо за терпение! 🙏")


@commands_router.message(Command("stats"))
async def cmd_stats(message: Message, db: Database, settings: Settings) -> None:
    if not message.from_user or message.from_user.id != settings.admin_id:
        return
    stats = await db.global_stats()
    await message.answer(
        "📊 <b>Статистика</b>\n\n"
        f"Всего Stars: <b>{stats['total_stars']}</b>\n"
        f"Пожертвований: <b>{stats['donations_count']}</b>\n"
        f"Уникальных поддержавших: <b>{stats['supporters_count']}</b>"
    )


@commands_router.message(Command("refund"))
async def cmd_refund(message: Message, command: CommandObject, bot: Bot, db: Database, settings: Settings) -> None:
    if not message.from_user or message.from_user.id != settings.admin_id:
        return
    args = (command.args or "").split()
    if not args:
        await message.answer("Использование: <code>/refund CHARGE_ID</code> или <code>/refund CHARGE_ID USER_ID</code>")
        return
    charge_id = args[0]
    user_id: int | None = None
    if len(args) > 1:
        if not args[1].isdigit():
            await message.answer("USER_ID должен быть числом.")
            return
        user_id = int(args[1])
    try:
        result = await refund_donation(bot, db, charge_id, user_id)
    except PaymentError as exc:
        await message.answer(f"❌ {html.escape(str(exc))}")
        return
    except Exception as exc:  # noqa: BLE001 — показываем админу ответ Telegram
        logger.exception("Ошибка возврата")
        await message.answer(f"❌ Telegram отклонил возврат: {html.escape(str(exc))}")
        return
    await message.answer(f"✅ {html.escape(result)}")


# ======================================================================
# HTTP API ДЛЯ MINI APP
# ======================================================================

logger = logging.getLogger(__name__)

BOT_KEY = web.AppKey("bot", Bot)
DB_KEY = web.AppKey("db", Database)
SETTINGS_KEY = web.AppKey("settings", Settings)

Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


def json_error(status: int, message: str) -> web.Response:
    return web.json_response({"error": message}, status=status)


# ---------- Middleware ----------

@web.middleware
async def cors_middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
    settings = request.app[SETTINGS_KEY]
    origin = request.headers.get("Origin", "").rstrip("/")
    allowed = origin and origin in settings.cors_origins

    if request.method == "OPTIONS" and request.path.startswith("/api/"):
        response: web.StreamResponse = web.Response(status=204)
    else:
        response = await handler(request)

    if allowed:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Vary"] = "Origin"
        response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        response.headers["Access-Control-Max-Age"] = "600"
    return response


@web.middleware
async def error_middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
    try:
        response = await handler(request)
    except web.HTTPException as exc:
        if request.path.startswith("/api/"):
            return json_error(exc.status, exc.reason or "Ошибка запроса")
        raise
    except Exception:  # noqa: BLE001 — наружу не отдаём детали внутренних ошибок
        logger.exception("Необработанная ошибка в %s %s", request.method, request.path)
        return json_error(500, "Внутренняя ошибка сервера. Попробуйте позже.")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    return response


def authenticate(request: web.Request) -> TelegramUser:
    settings = request.app[SETTINGS_KEY]
    header = request.headers.get("Authorization", "")
    scheme, _, init_data = header.partition(" ")
    if scheme.lower() != "tma" or not init_data:
        raise web.HTTPUnauthorized(reason="Откройте приложение через Telegram")
    try:
        return validate_init_data(init_data.strip(), settings.bot_token, settings.init_data_ttl_seconds)
    except InitDataError as exc:
        raise web.HTTPUnauthorized(reason=str(exc)) from exc


# ---------- Эндпоинты ----------

async def health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def get_stats(request: web.Request) -> web.Response:
    db = request.app[DB_KEY]
    settings = request.app[SETTINGS_KEY]
    stats = await db.global_stats()
    recent = await db.recent_public(limit=5)
    return web.json_response(
        {
            "project_name": settings.project_name,
            **stats,
            "recent": recent,
            "limits": {
                "min": settings.min_amount,
                "max": settings.max_amount,
                "presets": list(settings.preset_amounts),
            },
        }
    )


async def get_my_donations(request: web.Request) -> web.Response:
    user = authenticate(request)
    history = await request.app[DB_KEY].user_history(user.id)
    return web.json_response(history)


async def create_invoice(request: web.Request) -> web.Response:
    user = authenticate(request)
    settings = request.app[SETTINGS_KEY]
    try:
        body: Any = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return json_error(400, "Некорректный формат запроса")
    if not isinstance(body, dict):
        return json_error(400, "Некорректный формат запроса")

    try:
        amount = validate_amount(body.get("amount"), settings)
        message = normalize_message(body.get("message"))
        anonymous = body.get("anonymous", False)
        if not isinstance(anonymous, bool):
            raise PaymentError("Поле anonymous должно быть true или false")
        invoice, link = await create_donation_invoice(
            bot=request.app[BOT_KEY],
            db=request.app[DB_KEY],
            settings=settings,
            user_id=user.id,
            amount=amount,
            anonymous=anonymous,
            message=message,
        )
    except PaymentError as exc:
        return json_error(400, str(exc))

    return web.json_response({"payload": invoice.payload, "invoice_link": link, "amount": invoice.amount})


async def get_invoice_status(request: web.Request) -> web.Response:
    user = authenticate(request)
    db = request.app[DB_KEY]
    payload = request.match_info["payload"]
    invoice = await db.get_invoice(payload)
    # Чужие и несуществующие счета неотличимы — 404 в обоих случаях
    if invoice is None or invoice.user_id != user.id:
        return json_error(404, "Счёт не найден")
    donation = await db.get_donation_by_payload(payload)
    return web.json_response(
        {
            "status": "paid" if donation else "pending",
            "amount": invoice.amount,
            "donation": donation.to_private_dict() if donation else None,
        }
    )


# ---------- Раздача фронтенда ----------

def setup_frontend(app: web.Application, dist: Path) -> None:
    index = dist / "index.html"
    if not index.is_file():
        logger.warning(
            "Собранный фронтенд не найден (%s). API работает, но страница Mini App не раздаётся. "
            "Выполните: cd frontend && npm install && npm run build",
            index,
        )
        return
    dist_root = dist.resolve()

    async def serve(request: web.Request) -> web.StreamResponse:
        tail = request.match_info.get("tail", "")
        if tail.startswith("api/"):
            return json_error(404, "Не найдено")
        if tail:
            candidate = (dist_root / tail).resolve()
            if candidate.is_file() and dist_root in candidate.parents:
                response = web.FileResponse(candidate)
                if "/assets/" in f"/{tail}":
                    response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
                return response
        response = web.FileResponse(index)
        response.headers["Cache-Control"] = "no-cache"
        return response

    app.router.add_get("/", serve)
    app.router.add_get("/{tail:.*}", serve)


def create_web_app(bot: Bot, db: Database, settings: Settings) -> web.Application:
    app = web.Application(middlewares=[cors_middleware, error_middleware], client_max_size=16 * 1024)
    app[BOT_KEY] = bot
    app[DB_KEY] = db
    app[SETTINGS_KEY] = settings

    app.router.add_get("/api/health", health)
    app.router.add_get("/api/stats", get_stats)
    app.router.add_get("/api/me/donations", get_my_donations)
    app.router.add_post("/api/invoices", create_invoice)
    app.router.add_get("/api/invoices/{payload}", get_invoice_status)
    # Preflight-запросы OPTIONS обрабатывает cors_middleware, отдельный маршрут не нужен

    setup_frontend(app, settings.frontend_dist)
    return app


# ======================================================================
# ЗАПУСК
# ======================================================================

logger = logging.getLogger("bot")


async def configure_bot(bot: Bot, settings: Settings) -> None:
    """Кнопка меню чата открывает Mini App; список команд для подсказок."""
    try:
        await bot.set_chat_menu_button(
            menu_button=MenuButtonWebApp(text="Поддержать", web_app=WebAppInfo(url=settings.webapp_url))
        )
        await bot.set_my_commands(
            [
                BotCommand(command="start", description="Открыть приложение"),
                BotCommand(command="help", description="Как поддержать проект"),
                BotCommand(command="terms", description="Условия"),
                BotCommand(command="paysupport", description="Помощь с оплатой"),
            ]
        )
    except Exception:  # noqa: BLE001 — не критично для работы
        logger.exception("Не удалось настроить кнопку меню или команды")


async def cleanup_loop(db: Database, settings: Settings) -> None:
    """Раз в час удаляет давно просроченные неоплаченные счета."""
    while True:
        try:
            removed = await db.cleanup_stale_invoices(older_than_seconds=max(settings.invoice_ttl_seconds, 3600) * 24)
            if removed:
                logger.info("Удалено просроченных счетов: %s", removed)
        except Exception:  # noqa: BLE001
            logger.exception("Ошибка очистки счетов")
        await asyncio.sleep(3600)


async def main() -> None:
    settings = load_settings()

    db = Database(settings.db_path)
    await db.connect()
    logger.info("База данных: %s", settings.db_path)

    bot = Bot(token=settings.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    # Эти объекты автоматически передаются в обработчики по имени аргумента (db, settings)
    dp["db"] = db
    dp["settings"] = settings
    dp.include_router(payments_router)
    dp.include_router(commands_router)

    web_app = create_web_app(bot, db, settings)
    runner = web.AppRunner(web_app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, settings.host, settings.port)
    await site.start()
    logger.info("HTTP-сервер Mini App запущен на http://%s:%s", settings.host, settings.port)
    logger.info("Mini App URL для Telegram: %s", settings.webapp_url)

    await configure_bot(bot, settings)
    cleanup_task = asyncio.create_task(cleanup_loop(db, settings))

    try:
        await bot.delete_webhook(drop_pending_updates=False)
        await dp.start_polling(
            bot,
            allowed_updates=["message", "pre_checkout_query"],
        )
    finally:
        cleanup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cleanup_task
        await runner.cleanup()
        await bot.session.close()
        await db.close()
        logger.info("Остановлено")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    try:
        asyncio.run(main())
    except ConfigError as error:
        logging.getLogger("config").error(str(error))
        sys.exit(1)
    except KeyboardInterrupt:
        pass
