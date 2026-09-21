import os
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Optional, List, Tuple

import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.filters.callback_data import CallbackData
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    ChatJoinRequest,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

# --- Configuration & Logging ---
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is missing!")

ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID"))
PRIVATE_CHANNEL_ID = int(os.getenv("PRIVATE_CHANNEL_ID"))
CRYPTO_BOT_URL = os.getenv("CRYPTO_BOT_URL", "https://pay.crypt.bot/app?startapp=invoice_12345")

# Optional: reuse a link you already created manually in Telegram (must have
# "Request Admin Approval" turned on). If not set, the bot creates one itself
# at startup and reuses it for every approved payer.
PRIVATE_CHANNEL_JOIN_LINK = os.getenv("PRIVATE_CHANNEL_JOIN_LINK")

CHECK_INTERVAL_SECONDS = 3600             # Check background queue hourly
DB_NAME = os.getenv("DB_NAME", "subscriptions.db")

PROCESSED_DECISIONS: set[int] = set()
PENDING_USERS: Dict[int, str] = {}

# Populated at startup with the reusable join-request invite link.
CHANNEL_JOIN_LINK: Optional[str] = None


def get_current_utc_time_str() -> str:
    """Returns current UTC timestamp formatted with GMT indicator."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC (GMT)")


# --- Database Layer ---
async def init_db():
    db_dir = os.path.dirname(DB_NAME)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id INTEGER PRIMARY KEY,
                user_name TEXT,
                utc_time TEXT,
                expire_timestamp INTEGER,
                status TEXT DEFAULT 'active',
                reminded_3d INTEGER DEFAULT 0
            )
        """)
        # Safe migration for existing databases
        async with db.execute("PRAGMA table_info(subscriptions)") as cursor:
            existing_cols = [row[1] for row in await cursor.fetchall()]

        if "user_name" not in existing_cols:
            await db.execute("ALTER TABLE subscriptions ADD COLUMN user_name TEXT")
        if "utc_time" not in existing_cols:
            await db.execute("ALTER TABLE subscriptions ADD COLUMN utc_time TEXT")

        # Tracks channel join requests that are sitting unanswered in
        # Telegram's queue, waiting on payment approval.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS join_requests (
                user_id INTEGER PRIMARY KEY,
                requested_at TEXT
            )
        """)

        await db.commit()


async def add_join_request(user_id: int):
    now_utc_str = get_current_utc_time_str()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR REPLACE INTO join_requests (user_id, requested_at) VALUES (?, ?)",
            (user_id, now_utc_str),
        )
        await db.commit()


async def pop_join_request(user_id: int) -> bool:
    """Returns True and removes the row if a pending join request exists for this user."""
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT 1 FROM join_requests WHERE user_id = ?", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if row:
            await db.execute("DELETE FROM join_requests WHERE user_id = ?", (user_id,))
            await db.commit()
            return True
        return False


async def add_subscription(user_id: int, user_name: Optional[str] = None, duration_days: int = 30):
    now_utc_str = get_current_utc_time_str()
    expire_at = int(time.time()) + (duration_days * 86400)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            INSERT INTO subscriptions (user_id, user_name, utc_time, expire_timestamp, status, reminded_3d)
            VALUES (?, ?, ?, ?, 'active', 0)
            ON CONFLICT(user_id) DO UPDATE SET
                user_name = COALESCE(excluded.user_name, subscriptions.user_name),
                utc_time = excluded.utc_time,
                expire_timestamp = excluded.expire_timestamp,
                status = 'active',
                reminded_3d = 0
        """, (user_id, user_name, now_utc_str, expire_at))
        await db.commit()


async def has_active_subscription(user_id: int) -> bool:
    """True if this user has a currently-active, unexpired subscription."""
    now = int(time.time())
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT 1 FROM subscriptions WHERE user_id = ? AND status = 'active' AND expire_timestamp > ?",
            (user_id, now),
        ) as cursor:
            row = await cursor.fetchone()
            return row is not None


async def get_upcoming_expirations(days_before: int = 3) -> List[Tuple[int, int]]:
    now = int(time.time())
    threshold_timestamp = now + (days_before * 86400)
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            """
            SELECT user_id, expire_timestamp 
            FROM subscriptions 
            WHERE status = 'active' 
              AND reminded_3d = 0 
              AND expire_timestamp <= ? 
              AND expire_timestamp > ?
            """,
            (threshold_timestamp, now)
        ) as cursor:
            return await cursor.fetchall()


async def mark_reminded(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE subscriptions SET reminded_3d = 1 WHERE user_id = ?", 
            (user_id,)
        )
        await db.commit()


async def get_expired_subscriptions() -> List[int]:
    now = int(time.time())
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT user_id FROM subscriptions WHERE status = 'active' AND expire_timestamp <= ?", 
            (now,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [r[0] for r in rows]


async def mark_expired(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE subscriptions SET status = 'expired' WHERE user_id = ?", 
            (user_id,)
        )
        await db.commit()


# --- FSM States ---
class SubscriptionState(StatesGroup):
    selecting_language = State()
    selecting_region = State()
    selecting_payment_method = State()
    waiting_for_receipt = State()


# --- Callback Data Factories ---
class LangCallback(CallbackData, prefix="lang"):
    code: str


class RegionCallback(CallbackData, prefix="region"):
    target: str


class PaymentMethodCallback(CallbackData, prefix="pay_method"):
    method: str


class AdminDecisionCallback(CallbackData, prefix="admin"):
    action: str
    user_id: int


# --- Texts & Localization ---
TEXTS = {
    "uz": {
        "welcome_lang": "👋 **Xush kelibsiz!**\n\nIltimos, tilni tanlang:",
        "select_region": "🌐 **Joylashuvni tanlang:**\n\nVIP signallarga kirish uchun to'lov turini belgilang:",
        "btn_uz": "📍 O'zbekiston (UzCard / HUMO)",
        "btn_intl": "🌍 Xalqaro (USDT / Crypto)",
        "uz_title": "🇺🇿 **O'zbekiston P2P To'lov Usuli**\n\nNarxi: **100,000 UZS / 30 kun**\n\nKarta turini tanlang:",
        "intl_title": "🌍 **Xalqaro Kripto Usuli**\n\nNarxi: **10 USDT / 30 kun**\n\nTo'lov usulini tanlang:",
        "btn_crypto_bot": "⚡ @CryptoBot orqali tezkor to'lov",
        "btn_trc20": "🔗 To'g'ridan-to'g'ri TRC-20 Hamyon",
        "btn_back_lang": "⬅️ Tilni o'zgartirish",
        "btn_back_region": "⬅️ Ortga",
        "btn_renew": "🔄 Obunani uzaytirish",
        "uzcard_info": (
            "💳 **UzCard P2P Rekvizitlari**\n\nKarta: `5440 8103 7817 5946`\nEga: **K. Suloymonov**\n"
            "Summa: **100,000 UZS**\n\n⚠️ Payme yoki Click orqali o'tkazma qilgach, "
            "**chek rasmini yoki PDF faylini shu chatga yuboring.**"
        ),
        "humo_info": (
            "💳 **HUMO P2P Rekvizitlari**\n\nKarta: `9860 0406 0809 0508`\nEga: **K. Suloymonov**\n"
            "Summa: **100,000 UZS**\n\n⚠️ Payme yoki Click orqali o'tkazma qilgach, "
            "**chek rasmini yoki PDF faylini shu chatga yuboring.**"
        ),
        "trc20_info": (
            "🔗 **USDT TRC-20 Hamyon**\n\nManzil: `TL1DhDQjYWppFy9YtKbCoqcpX5RtKu3VJK`\n"
            "Summa: **10.00 USDT**\n\n⚠️ To'lov amalga oshirilgach, "
            "**tranzaksiya chekini (skrinshot) yoki TrxID matnini shu chatga yuboring.**"
        ),
        "receipt_ok": "✅ Chekingiz qabul qilindi! Adminlar ko'rib chiqqach VIP status aktivlashtiriladi (odatda 5-15 daqiqa).",
        "receipt_invalid": "Iltimos, faqat to'lov chekining rasmi, PDF fayli yoki TrxID matnini yuboring.",
        "approved": (
            "✅ To'lovingiz tasdiqlandi! Kanalga kirish uchun quyidagi havolani bosing va "
            "\"So'rov yuborish\" (Request to Join) tugmasini bosing — so'rovingiz avtomatik tasdiqlanadi:"
        ),
        "approved_no_link": "✅ To'lovingiz tasdiqlandi, lekin taklif havolasi mavjud emas. Iltimos, admin bilan bog'laning.",
        "join_granted": "🎉 Kanalga kirish huquqi berildi! Endi barcha xabarlarni o'qiy olasiz.",
        "join_needs_payment": "⚠️ Kanalga kirishdan oldin avval to'lovni yakunlashingiz kerak. /start ni bosing.",
        "rejected": "❌ Kechirasiz, chekingiz tasdiqlanmadi. Iltimos, admin bilan bog'laning yoki qaytadan urinib ko'ring.",
        "reminder_3d": "⏳ **VIP obunangiz tugashiga 3 kun qoldi!**\n\nKanalga kirish huquqini yo'qotmaslik uchun obunani oldindan uzaytirishingiz mumkin.",
        "expired_kick": "⚠️ **VIP obunangiz muddati tugadi.**\n\nKanalga qayta kirish uchun obunani yangilang.",
    },
    "en": {
        "welcome_lang": "👋 **Welcome!**\n\nPlease select your language:",
        "select_region": "🌐 **Select your location:**\n\nChoose your preferred payment route to proceed:",
        "btn_uz": "📍 Uzbekistan (UzCard / HUMO)",
        "btn_intl": "🌍 International (USDT / Crypto)",
        "uz_title": "🇺🇿 **Uzbekistan P2P Payment Route**\n\nPrice: **100,000 UZS / 30 Days**\n\nSelect your card type:",
        "intl_title": "🌍 **International Crypto Route**\n\nPrice: **10 USDT / 30 Days**\n\nSelect payment method:",
        "btn_crypto_bot": "⚡ Instant Pay via @CryptoBot",
        "btn_trc20": "🔗 Direct TRC-20 Address",
        "btn_back_lang": "⬅️ Change Language",
        "btn_back_region": "⬅️ Back",
        "btn_renew": "🔄 Renew Subscription",
        "uzcard_info": (
            "💳 **UzCard P2P Details**\n\nCard: `5440 8103 7817 5946`\nHolder: **K. Suloymonov**\n"
            "Amount: **100,000 UZS**\n\n⚠️ After transferring via Payme or Click, "
            "**send the receipt screenshot or PDF to this chat.**"
        ),
        "humo_info": (
            "💳 **HUMO P2P Details**\n\nCard: `9860 0406 0809 0508`\nHolder: **K. Suloymonov**\n"
            "Amount: **100,000 UZS**\n\n⚠️ After transferring via Payme or Click, "
            "**send the receipt screenshot or PDF to this chat.**"
        ),
        "trc20_info": (
            "🔗 **USDT TRC-20 Address**\n\nAddress: `TL1DhDQjYWppFy9YtKbCoqcpX5RtKu3VJK`\n"
            "Amount: **10.00 USDT**\n\n⚠️ Once paid, "
            "**send the transaction screenshot or TxID text to this chat.**"
        ),
        "receipt_ok": "✅ Receipt received! VIP status will be activated once verified by admin (usually 5-15 minutes).",
        "receipt_invalid": "Please send only a receipt image, PDF file, or TxID text.",
        "approved": (
            "✅ Your payment was approved! Tap the link below and press \"Request to Join\" — "
            "your request will be approved automatically and you'll get full read access:"
        ),
        "approved_no_link": "✅ Your payment was approved, but no invite link is available. Please contact the admin.",
        "join_granted": "🎉 You've been granted access to the channel! You can now read all messages.",
        "join_needs_payment": "⚠️ You need to complete payment before joining the channel. Press /start.",
        "rejected": "❌ Sorry, your receipt was not approved. Please contact the admin or try again.",
        "reminder_3d": "⏳ **Your VIP subscription expires in 3 days!**\n\nTo ensure uninterrupted access, you can extend your subscription now.",
        "expired_kick": "⚠️ **Your VIP subscription has expired.**\n\nPlease renew your subscription to regain channel access.",
    },
}

METHOD_LABELS = {"p2p_uzcard": "UzCard", "p2p_humo": "HUMO", "trc20": "TRC-20 USDT"}


def t(lang: str) -> Dict[str, str]:
    return TEXTS.get(lang, TEXTS["en"])


# --- UI Helpers ---
async def safe_edit_text(call: CallbackQuery, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    try:
        await call.message.edit_text(text, reply_markup=reply_markup, parse_mode="Markdown")
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            logging.warning(f"edit_text failed: {e}")


async def safe_edit_caption(call: CallbackQuery, caption: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    try:
        await call.message.edit_caption(caption=caption, reply_markup=reply_markup, parse_mode="Markdown")
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            logging.warning(f"edit_caption failed: {e}")


# --- Bot Initialization ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)


# --- Handlers ---
@router.message(Command("start"))
async def start_handler(message: Message, state: FSMContext):
    await state.clear()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🇺🇿 O'zbekcha", callback_data=LangCallback(code="uz").pack()),
        InlineKeyboardButton(text="🇬🇧 English", callback_data=LangCallback(code="en").pack()),
    ]])
    await message.answer(TEXTS["en"]["welcome_lang"], reply_markup=keyboard, parse_mode="Markdown")
    await state.set_state(SubscriptionState.selecting_language)


@router.callback_query(LangCallback.filter())
async def set_language(call: CallbackQuery, callback_data: LangCallback, state: FSMContext):
    await call.answer()
    lang = callback_data.code
    await state.update_data(language=lang)
    tx = t(lang)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=tx["btn_uz"], callback_data=RegionCallback(target="uzbekistan").pack())],
        [InlineKeyboardButton(text=tx["btn_intl"], callback_data=RegionCallback(target="international").pack())],
        [InlineKeyboardButton(text=tx["btn_back_lang"], callback_data="back_to_language")],
    ])
    await safe_edit_text(call, tx["select_region"], reply_markup=keyboard)
    await state.set_state(SubscriptionState.selecting_region)


@router.callback_query(RegionCallback.filter(F.target == "uzbekistan"))
async def show_uzbekistan_payments(call: CallbackQuery, state: FSMContext):
    await call.answer()
    data = await state.get_data()
    lang = data.get("language", "en")
    tx = t(lang)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 UzCard", callback_data=PaymentMethodCallback(method="p2p_uzcard").pack())],
        [InlineKeyboardButton(text="💳 HUMO", callback_data=PaymentMethodCallback(method="p2p_humo").pack())],
        [InlineKeyboardButton(text=tx["btn_back_region"], callback_data=LangCallback(code=lang).pack())],
    ])
    await safe_edit_text(call, tx["uz_title"], reply_markup=keyboard)


@router.callback_query(RegionCallback.filter(F.target == "international"))
async def show_international_payments(call: CallbackQuery, state: FSMContext):
    await call.answer()
    data = await state.get_data()
    lang = data.get("language", "en")
    tx = t(lang)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=tx["btn_crypto_bot"], url=CRYPTO_BOT_URL)],
        [InlineKeyboardButton(text=tx["btn_trc20"], callback_data=PaymentMethodCallback(method="trc20").pack())],
        [InlineKeyboardButton(text=tx["btn_back_region"], callback_data=LangCallback(code=lang).pack())],
    ])
    await safe_edit_text(call, tx["intl_title"], reply_markup=keyboard)


@router.callback_query(PaymentMethodCallback.filter())
async def show_payment_details(call: CallbackQuery, callback_data: PaymentMethodCallback, state: FSMContext):
    await call.answer()
    data = await state.get_data()
    lang = data.get("language", "en")
    tx = t(lang)

    method_map = {
        "p2p_uzcard": tx["uzcard_info"],
        "p2p_humo": tx["humo_info"],
        "trc20": tx["trc20_info"],
    }
    selected_text = method_map.get(callback_data.method, "")

    await state.update_data(method=callback_data.method)
    await safe_edit_text(call, selected_text)
    await state.set_state(SubscriptionState.waiting_for_receipt)


@router.message(SubscriptionState.waiting_for_receipt, F.photo | F.document | F.text)
async def process_receipt(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    lang = data.get("language", "en")
    method = data.get("method", "unknown")
    tx = t(lang)

    user_id = message.from_user.id
    raw_user = message.from_user
    if raw_user.username:
        user_name = f"@{raw_user.username}"
        if raw_user.full_name:
            user_name += f" ({raw_user.full_name})"
    else:
        user_name = raw_user.full_name or f"User {user_id}"

    now_utc_str = get_current_utc_time_str()
    PENDING_USERS[user_id] = user_name

    admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="✅ Approve",
            callback_data=AdminDecisionCallback(action="approve", user_id=user_id).pack(),
        ),
        InlineKeyboardButton(
            text="❌ Reject",
            callback_data=AdminDecisionCallback(action="reject", user_id=user_id).pack(),
        ),
    ]])

    caption = (
        f"📥 **New payment receipt**\n\n"
        f"👤 User: {user_name} (`{user_id}`)\n"
        f"🕒 Time: `{now_utc_str}`\n"
        f"💳 Method: {METHOD_LABELS.get(method, method)}\n"
        f"🌐 Lang: {lang}"
    )

    if message.photo:
        await bot.send_photo(
            chat_id=ADMIN_CHAT_ID,
            photo=message.photo[-1].file_id,
            caption=caption,
            reply_markup=admin_keyboard,
            parse_mode="Markdown",
        )
    elif message.document:
        await bot.send_document(
            chat_id=ADMIN_CHAT_ID,
            document=message.document.file_id,
            caption=caption,
            reply_markup=admin_keyboard,
            parse_mode="Markdown",
        )
    else:
        await bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=f"{caption}\n\n🔗 TxID: `{message.text}`",
            reply_markup=admin_keyboard,
            parse_mode="Markdown",
        )

    await message.answer(tx["receipt_ok"], parse_mode="Markdown")
    await state.set_state(None)


@router.message(SubscriptionState.waiting_for_receipt)
async def invalid_receipt(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "en")
    await message.answer(t(lang)["receipt_invalid"])


@router.callback_query(AdminDecisionCallback.filter())
async def handle_admin_decision(call: CallbackQuery, callback_data: AdminDecisionCallback, bot: Bot):
    if call.from_user.id != ADMIN_CHAT_ID:
        await call.answer("Not authorized.", show_alert=True)
        return

    target_user_id = callback_data.user_id

    if target_user_id in PROCESSED_DECISIONS:
        await call.answer("Already decided — ignoring duplicate click.", show_alert=True)
        return
    PROCESSED_DECISIONS.add(target_user_id)

    approved = callback_data.action == "approve"

    if approved:
        # Retrieve user_name from pending cache or fallback to get_chat
        target_user_name = PENDING_USERS.pop(target_user_id, None)
        if not target_user_name:
            try:
                chat = await bot.get_chat(target_user_id)
                if chat.username:
                    target_user_name = f"@{chat.username}"
                    if chat.full_name:
                        target_user_name += f" ({chat.full_name})"
                else:
                    target_user_name = chat.full_name or f"User {target_user_id}"
            except Exception:
                target_user_name = f"user_{target_user_id}"

        # Register 30-day subscription in SQLite.
        await add_subscription(user_id=target_user_id, user_name=target_user_name, duration_days=30)

        # If this user already has a join request sitting unanswered in
        # Telegram's queue (they clicked the channel link before paying),
        # approve it right now — that's the moment they get read access.
        had_pending_request = await pop_join_request(target_user_id)

        if had_pending_request:
            try:
                await bot.approve_chat_join_request(chat_id=PRIVATE_CHANNEL_ID, user_id=target_user_id)
                text = f"{TEXTS['uz']['join_granted']}\n\n{TEXTS['en']['join_granted']}"
                logging.info(f"Approved pending join request for {target_user_id} after payment.")
            except Exception as e:
                logging.error(f"Failed to approve pending join request for {target_user_id}: {e}")
                # Fall back to sending the link so they can request again.
                had_pending_request = False

        if not had_pending_request:
            # No pending request on file — send them the channel link so
            # they can request to join; the chat_join_request handler will
            # see their active subscription and approve it instantly.
            text = f"{TEXTS['uz']['approved']}\n\n{TEXTS['en']['approved']}"
            if CHANNEL_JOIN_LINK:
                text += f"\n\n🔗 {CHANNEL_JOIN_LINK}"
            else:
                text = f"{TEXTS['uz']['approved_no_link']}\n\n{TEXTS['en']['approved_no_link']}"
    else:
        PENDING_USERS.pop(target_user_id, None)
        text = f"{TEXTS['uz']['rejected']}\n\n{TEXTS['en']['rejected']}"

    try:
        await bot.send_message(chat_id=target_user_id, text=text, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception as e:
        logging.warning(f"Could not notify user {target_user_id}: {e}")

    status_line = "✅ APPROVED" if approved else "❌ REJECTED"
    original_caption = call.message.caption or call.message.text or ""
    new_text = f"{original_caption}\n\n{status_line}"

    if call.message.caption is not None:
        await safe_edit_caption(call, new_text, reply_markup=None)
    else:
        await safe_edit_text(call, new_text, reply_markup=None)

    await call.answer(status_line)


@router.callback_query(F.data == "back_to_language")
async def back_to_language_handler(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await start_handler(call.message, state)


# --- Join Request Handler ---
# Anyone can tap "Request to Join" on the channel preview at any time — even
# before paying. This handler decides whether to grant read access right
# away (already paid) or leave the request pending in Telegram's queue until
# payment is approved (handle_admin_decision picks it up from there).
@router.chat_join_request()
async def handle_join_request(join_request: ChatJoinRequest, bot: Bot):
    if join_request.chat.id != PRIVATE_CHANNEL_ID:
        return

    user_id = join_request.from_user.id

    if await has_active_subscription(user_id):
        try:
            await bot.approve_chat_join_request(chat_id=PRIVATE_CHANNEL_ID, user_id=user_id)
            logging.info(f"Approved join request for already-paid user {user_id}")
            await bot.send_message(
                chat_id=user_id,
                text=f"{TEXTS['uz']['join_granted']}\n\n{TEXTS['en']['join_granted']}",
                parse_mode="Markdown",
            )
        except Exception as e:
            logging.error(f"Failed to approve join request for {user_id}: {e}")
    else:
        # Don't decline — leave it pending so we can approve it later
        # without making the user click "Request to Join" again.
        await add_join_request(user_id)
        try:
            await bot.send_message(
                chat_id=user_id,
                text=f"{TEXTS['uz']['join_needs_payment']}\n\n{TEXTS['en']['join_needs_payment']}",
                parse_mode="Markdown",
            )
        except Exception as e:
            logging.warning(f"Could not notify unpaid user {user_id}: {e}")


# --- Background Task Worker ---
async def subscription_checker(bot: Bot, channel_id: int):
    """Monitors subscriptions hourly for 3-day reminders and automated ejections."""
    while True:
        try:
            # 1. Advance 3-Day Expiration Warnings
            upcoming_users = await get_upcoming_expirations(days_before=3)
            for user_id, _ in upcoming_users:
                try:
                    renew_kb = InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="🔄 Renew / Uzaytirish", callback_data="back_to_language")
                    ]])
                    await bot.send_message(
                        chat_id=user_id,
                        text=f"{TEXTS['uz']['reminder_3d']}\n\n{TEXTS['en']['reminder_3d']}",
                        reply_markup=renew_kb,
                        parse_mode="Markdown"
                    )
                    await mark_reminded(user_id)
                    logging.info(f"Sent 3-day reminder to user {user_id}")
                except Exception as e:
                    logging.error(f"Failed to send reminder to user {user_id}: {e}")

            # 2. Automated Ejections for Expired Subscriptions
            # Kicking (ban + unban) is still the only way to revoke read
            # access from an already-admitted channel member.
            expired_user_ids = await get_expired_subscriptions()
            for user_id in expired_user_ids:
                try:
                    await bot.ban_chat_member(chat_id=channel_id, user_id=user_id)
                    await bot.unban_chat_member(chat_id=channel_id, user_id=user_id)

                    await bot.send_message(
                        chat_id=user_id,
                        text=f"{TEXTS['uz']['expired_kick']}\n\n{TEXTS['en']['expired_kick']}",
                        parse_mode="Markdown"
                    )
                    logging.info(f"Ejected expired user {user_id} from VIP channel.")
                except Exception as e:
                    logging.error(f"Failed to kick user {user_id}: {e}")

                await mark_expired(user_id)

        except Exception as e:
            logging.error(f"Error in subscription_checker execution loop: {e}")

        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


# --- Main Entry Point ---
async def main():
    global CHANNEL_JOIN_LINK

    await init_db()

    # Set up (or reuse) the reusable, join-request-gated invite link.
    if PRIVATE_CHANNEL_JOIN_LINK:
        CHANNEL_JOIN_LINK = PRIVATE_CHANNEL_JOIN_LINK
    else:
        try:
            invite = await bot.create_chat_invite_link(
                chat_id=PRIVATE_CHANNEL_ID,
                name="paid-access",
                creates_join_request=True,
            )
            CHANNEL_JOIN_LINK = invite.invite_link
            logging.info(f"Created join-request invite link: {CHANNEL_JOIN_LINK}")
        except Exception as e:
            logging.error(
                f"Could not create a join-request invite link — check that the bot is a "
                f"channel admin with 'Invite Users' permission: {e}"
            )

    # Spawn background subscription worker task
    asyncio.create_task(subscription_checker(bot, PRIVATE_CHANNEL_ID))

    logging.info("Bot starting polling...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())