import asyncio
import logging
import os
from typing import Dict, List

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from dotenv import load_dotenv

from data.analytics import analytics, days_ago, lifespan
from data.bot_content import (
    CATEGORIES,
    CALL_CENTER_TEXT,
    CONTACTS,
    LAW_SECTIONS,
    SPECIALISTS_TEXT,
    TRANSLATIONS,
    UI_TEXT,
)

DEFAULT_LANG = "ru"
ACTIVE_MENU_ID_KEY = "active_menu_message_id"
ADMIN_IDS: set[int] = set()
dp = Dispatcher(storage=MemoryStorage())


def _parse_admin_ids(raw: str | None) -> set[int]:
    if not raw:
        return set()
    out: set[int] = set()
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            out.add(int(token))
        except ValueError:
            logging.warning("Ignoring non-integer ADMIN_IDS entry: %r", token)
    return out


async def track(
    user_id: int,
    event_type: str,
    lang: str | None = None,
    section_key: str | None = None,
    username: str | None = None,
    first_name: str | None = None,
) -> None:
    try:
        await analytics.upsert_user(user_id, lang=lang, username=username, first_name=first_name)
        await analytics.log_event(user_id, event_type, lang=lang, section_key=section_key)
    except Exception:
        logging.exception("Failed to record analytics event %s for user %s", event_type, user_id)


async def get_lang(state: FSMContext) -> str:
    state_data = await state.get_data()
    return state_data.get("lang", DEFAULT_LANG)


async def safe_edit_text(
    message: Message,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> Message:
    try:
        return await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as error:
        if "message is not modified" in str(error).lower():
            return message
        raise


async def disable_keyboard(bot: Bot, chat_id: int, message_id: int) -> None:
    try:
        await bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=None)
    except TelegramBadRequest:
        return


async def set_active_menu(state: FSMContext, message: Message) -> None:
    data = await state.get_data()
    previous_id = data.get(ACTIVE_MENU_ID_KEY)
    if previous_id and previous_id != message.message_id:
        await disable_keyboard(message.bot, message.chat.id, previous_id)
    await state.update_data(**{ACTIVE_MENU_ID_KEY: message.message_id})


async def send_or_edit_active_menu(
    command_message: Message,
    state: FSMContext,
    text: str,
    reply_markup: InlineKeyboardMarkup,
) -> Message:
    data = await state.get_data()
    active_id = data.get(ACTIVE_MENU_ID_KEY)
    if active_id:
        try:
            edited = await command_message.bot.edit_message_text(
                chat_id=command_message.chat.id,
                message_id=active_id,
                text=text,
                reply_markup=reply_markup,
            )
            await state.update_data(**{ACTIVE_MENU_ID_KEY: edited.message_id})
            return edited
        except TelegramBadRequest as error:
            if "message is not modified" in str(error).lower():
                return command_message
            logging.warning("Could not edit old menu (id=%s): %s", active_id, error)

    sent = await command_message.answer(text, reply_markup=reply_markup)
    await set_active_menu(state, sent)
    return sent


async def is_stale_menu_callback(callback: CallbackQuery, state: FSMContext) -> bool:
    if not callback.message:
        return True

    data = await state.get_data()
    active_id = data.get(ACTIVE_MENU_ID_KEY)
    current_id = callback.message.message_id

    if active_id is None:
        await disable_keyboard(callback.bot, callback.message.chat.id, current_id)
        await callback.answer("Сессия обновлена. Нажмите /start или /menu.", show_alert=False)
        return True

    if current_id == active_id:
        return False

    await disable_keyboard(callback.bot, callback.message.chat.id, current_id)
    await callback.answer("Это старое меню. Используйте актуальное сообщение ниже.", show_alert=False)
    return True


def split_text(text: str, limit: int = 3800) -> List[str]:
    if len(text) <= limit:
        return [text]

    chunks: List[str] = []
    current: List[str] = []
    current_len = 0

    for line in text.splitlines():
        line_len = len(line) + 1
        if current and current_len + line_len > limit:
            chunks.append("\n".join(current))
            current = [line]
            current_len = line_len
        else:
            current.append(line)
            current_len += line_len

    if current:
        chunks.append("\n".join(current))

    return chunks


def nav_row(lang: str, back_callback: str) -> list:
    return [
        InlineKeyboardButton(text=UI_TEXT[lang]["back"], callback_data=back_callback),
        InlineKeyboardButton(text=UI_TEXT[lang]["to_menu"], callback_data="nav:categories"),
    ]


def language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Қазақша", callback_data="lang:kz"),
                InlineKeyboardButton(text="Русский", callback_data="lang:ru"),
            ]
        ]
    )


def categories_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=CATEGORIES["law"][lang], callback_data="cat:law")],
            [InlineKeyboardButton(text=CATEGORIES["call_center"][lang], callback_data="cat:call_center")],
            [InlineKeyboardButton(text=CATEGORIES["translations"][lang], callback_data="cat:translations")],
            [InlineKeyboardButton(text=CATEGORIES["specialists"][lang], callback_data="cat:specialists")],
            [InlineKeyboardButton(text="🌐 Сменить язык / Тілді ауыстыру", callback_data="nav:languages")],
        ]
    )


def law_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=LAW_SECTIONS["law_lang"]["title"][lang], callback_data="law:law_lang")],
            [InlineKeyboardButton(text=LAW_SECTIONS["law_adv"]["title"][lang], callback_data="law:law_adv")],
            [InlineKeyboardButton(text=LAW_SECTIONS["law_consumer"]["title"][lang], callback_data="law:law_consumer")],
            [InlineKeyboardButton(text=LAW_SECTIONS["law_koap"]["title"][lang], callback_data="law:law_koap")],
            nav_row(lang, "nav:categories"),
        ]
    )


def translations_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=TRANSLATIONS["trade"]["title"][lang], callback_data="tr:trade")],
            [InlineKeyboardButton(text=TRANSLATIONS["autoservice"]["title"][lang], callback_data="tr:autoservice")],
            [InlineKeyboardButton(text=TRANSLATIONS["construction"]["title"][lang], callback_data="tr:construction")],
            [InlineKeyboardButton(text=TRANSLATIONS["home"]["title"][lang], callback_data="tr:home")],
            [InlineKeyboardButton(text=TRANSLATIONS["cosmetology"]["title"][lang], callback_data="tr:cosmetology")],
            [InlineKeyboardButton(text=TRANSLATIONS["menu"]["title"][lang], callback_data="tr:menu")],
            nav_row(lang, "nav:categories"),
        ]
    )


def back_to_categories_keyboard(lang: str, back_callback: str = "nav:categories") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[nav_row(lang, back_callback)]
    )


def call_center_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💬 WhatsApp", url=CONTACTS["call_center_whatsapp_url"])],
            nav_row(lang, "nav:categories"),
        ]
    )


async def show_section_text(message: Message, text: str, lang: str, back_callback: str = "nav:categories") -> Message:
    chunks = split_text(text)
    keyboard = back_to_categories_keyboard(lang, back_callback)
    if len(chunks) == 1:
        return await safe_edit_text(message, chunks[0], reply_markup=keyboard)

    await safe_edit_text(message, chunks[0])
    for chunk in chunks[1:-1]:
        await message.answer(chunk)
    return await message.answer(chunks[-1], reply_markup=keyboard)


async def show_categories(message: Message, lang: str) -> Message:
    return await safe_edit_text(message, UI_TEXT[lang]["choose_category"], reply_markup=categories_keyboard(lang))


def _user_meta(message: Message) -> tuple[str | None, str | None]:
    if message.from_user is None:
        return None, None
    return message.from_user.username, message.from_user.first_name


@dp.message(Command("start"))
async def start(message: Message, state: FSMContext) -> None:
    if message.from_user is None:
        return
    lang = await get_lang(state)
    data = await state.get_data()
    old_id = data.get(ACTIVE_MENU_ID_KEY)
    if old_id:
        await disable_keyboard(message.bot, message.chat.id, old_id)
    sent = await message.answer(UI_TEXT[lang]["choose_language"], reply_markup=language_keyboard())
    await set_active_menu(state, sent)
    username, first_name = _user_meta(message)
    await track(message.from_user.id, "start", lang=lang, username=username, first_name=first_name)


@dp.message(Command("menu"))
async def menu(message: Message, state: FSMContext) -> None:
    if message.from_user is None:
        return
    lang = await get_lang(state)
    data = await state.get_data()
    old_id = data.get(ACTIVE_MENU_ID_KEY)
    if old_id:
        await disable_keyboard(message.bot, message.chat.id, old_id)
    sent = await message.answer(UI_TEXT[lang]["choose_category"], reply_markup=categories_keyboard(lang))
    await set_active_menu(state, sent)
    username, first_name = _user_meta(message)
    await track(message.from_user.id, "menu", lang=lang, username=username, first_name=first_name)


@dp.message(Command("help"))
async def help_command(message: Message, state: FSMContext) -> None:
    if message.from_user is None:
        return
    lang = await get_lang(state)
    text_map: Dict[str, str] = {
        "ru": "Команды:\n/start - начать\n/menu - показать меню\n/help - помощь",
        "kz": "Командалар:\n/start - бастау\n/menu - мәзір\n/help - көмек",
    }
    await message.answer(text_map[lang])
    await track(message.from_user.id, "help", lang=lang)


@dp.message(Command("stats"))
async def stats_command(message: Message) -> None:
    if message.from_user is None or message.from_user.id not in ADMIN_IDS:
        return

    total = await analytics.total_users()
    dau = await analytics.active_users_since(days_ago(1))
    wau = await analytics.active_users_since(days_ago(7))
    mau = await analytics.active_users_since(days_ago(30))
    langs = await analytics.language_split()
    top = await analytics.top_sections(
        event_types=("category", "law_section", "translation_section"),
        since=days_ago(30),
        limit=10,
    )

    lines = [
        "📊 Статистика бота",
        f"Всего пользователей: {total}",
        f"Активных за 24ч: {dau}",
        f"Активных за 7д:  {wau}",
        f"Активных за 30д: {mau}",
        "",
        "Языки: " + (", ".join(f"{k}={v}" for k, v in sorted(langs.items())) or "—"),
        "",
        "Топ разделов (30д):",
    ]
    if top:
        for event_type, section_key, count in top:
            lines.append(f"  {event_type}/{section_key}: {count}")
    else:
        lines.append("  (нет данных)")

    await message.answer("\n".join(lines))


@dp.callback_query()
async def handle_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.data or not callback.message:
        return

    if await is_stale_menu_callback(callback, state):
        return

    await callback.answer()
    data = callback.data
    lang = await get_lang(state)
    user_id = callback.from_user.id if callback.from_user else None
    username = callback.from_user.username if callback.from_user else None
    first_name = callback.from_user.first_name if callback.from_user else None

    if data.startswith("lang:"):
        lang = data.split(":", 1)[1]
        await state.update_data(lang=lang)
        edited = await show_categories(callback.message, lang)
        await set_active_menu(state, edited)
        if user_id is not None:
            await track(user_id, "lang_set", lang=lang, section_key=lang, username=username, first_name=first_name)
        return

    if data == "nav:languages":
        edited = await safe_edit_text(callback.message, UI_TEXT[lang]["choose_language"], reply_markup=language_keyboard())
        await set_active_menu(state, edited)
        if user_id is not None:
            await track(user_id, "nav", lang=lang, section_key="languages")
        return

    if data == "nav:categories":
        edited = await show_categories(callback.message, lang)
        await set_active_menu(state, edited)
        if user_id is not None:
            await track(user_id, "nav", lang=lang, section_key="categories")
        return

    if data == "cat:law":
        edited = await safe_edit_text(callback.message, UI_TEXT[lang]["choose_law_subcategory"], reply_markup=law_keyboard(lang))
        await set_active_menu(state, edited)
        if user_id is not None:
            await track(user_id, "category", lang=lang, section_key="law")
        return

    if data == "cat:call_center":
        edited = await safe_edit_text(callback.message, CALL_CENTER_TEXT[lang], reply_markup=call_center_keyboard(lang))
        await set_active_menu(state, edited)
        if user_id is not None:
            await track(user_id, "category", lang=lang, section_key="call_center")
        return

    if data == "cat:translations":
        edited = await safe_edit_text(
            callback.message,
            UI_TEXT[lang]["choose_translations_category"],
            reply_markup=translations_keyboard(lang),
        )
        await set_active_menu(state, edited)
        if user_id is not None:
            await track(user_id, "category", lang=lang, section_key="translations")
        return

    if data == "cat:specialists":
        edited = await safe_edit_text(
            callback.message,
            SPECIALISTS_TEXT[lang],
            reply_markup=back_to_categories_keyboard(lang),
        )
        await set_active_menu(state, edited)
        if user_id is not None:
            await track(user_id, "category", lang=lang, section_key="specialists")
        return

    if data.startswith("law:"):
        key = data.split(":", 1)[1]
        section = LAW_SECTIONS.get(key)
        if section:
            active_message = await show_section_text(callback.message, section["text"][lang], lang, back_callback="cat:law")
            await set_active_menu(state, active_message)
            if user_id is not None:
                await track(user_id, "law_section", lang=lang, section_key=key)
        return

    if data.startswith("tr:"):
        key = data.split(":", 1)[1]
        section = TRANSLATIONS.get(key)
        if section:
            active_message = await show_section_text(callback.message, section["text"], lang, back_callback="cat:translations")
            await set_active_menu(state, active_message)
            if user_id is not None:
                await track(user_id, "translation_section", lang=lang, section_key=key)
        return


async def main() -> None:
    load_dotenv()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set. Add it to .env or environment variables.")

    global ADMIN_IDS
    ADMIN_IDS = _parse_admin_ids(os.getenv("ADMIN_IDS"))

    logging.basicConfig(
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        level=logging.INFO,
    )

    bot = Bot(token=token)
    async with lifespan():
        try:
            await dp.start_polling(bot)
        finally:
            await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped.")
