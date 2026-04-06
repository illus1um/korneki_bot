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

from data.bot_content import (
    CATEGORIES,
    CALL_CENTER_TEXT,
    INTRO_TEXT,
    LAW_SECTIONS,
    SPECIALISTS_TEXT,
    TRANSLATIONS,
    UI_TEXT,
)

DEFAULT_LANG = "ru"
ACTIVE_MENU_ID_KEY = "active_menu_message_id"
dp = Dispatcher(storage=MemoryStorage())


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
                InlineKeyboardButton(text="🇰🇿 Қазақша", callback_data="lang:kz"),
                InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang:ru"),
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
            [InlineKeyboardButton(text="💬 WhatsApp", url="https://wa.me/77751630000")],
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


async def show_categories(message: Message, lang: str) -> None:
    await safe_edit_text(message, UI_TEXT[lang]["choose_category"], reply_markup=categories_keyboard(lang))


@dp.message(Command("start"))
async def start(message: Message, state: FSMContext) -> None:
    lang = await get_lang(state)
    start_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🚀 Старт / Бастау", callback_data="open_languages")]]
    )
    data = await state.get_data()
    old_id = data.get(ACTIVE_MENU_ID_KEY)
    if old_id:
        await disable_keyboard(message.bot, message.chat.id, old_id)
    sent = await message.answer(INTRO_TEXT[lang], reply_markup=start_keyboard)
    await set_active_menu(state, sent)


@dp.message(Command("menu"))
async def menu(message: Message, state: FSMContext) -> None:
    lang = await get_lang(state)
    data = await state.get_data()
    old_id = data.get(ACTIVE_MENU_ID_KEY)
    if old_id:
        await disable_keyboard(message.bot, message.chat.id, old_id)
    sent = await message.answer(UI_TEXT[lang]["choose_category"], reply_markup=categories_keyboard(lang))
    await set_active_menu(state, sent)


@dp.message(Command("help"))
async def help_command(message: Message, state: FSMContext) -> None:
    lang = await get_lang(state)
    text_map: Dict[str, str] = {
        "ru": "Команды:\n/start - начать\n/menu - показать меню\n/help - помощь",
        "kz": "Командалар:\n/start - бастау\n/menu - мәзір\n/help - көмек",
    }
    await message.answer(text_map[lang])


@dp.callback_query()
async def handle_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.data or not callback.message:
        return

    if await is_stale_menu_callback(callback, state):
        return

    await callback.answer()
    data = callback.data
    lang = await get_lang(state)

    if data == "open_languages":
        edited = await safe_edit_text(callback.message, UI_TEXT[lang]["choose_language"], reply_markup=language_keyboard())
        await set_active_menu(state, edited)
        return

    if data.startswith("lang:"):
        lang = data.split(":", 1)[1]
        await state.update_data(lang=lang)
        await show_categories(callback.message, lang)
        await set_active_menu(state, callback.message)
        return

    if data == "nav:languages":
        edited = await safe_edit_text(callback.message, UI_TEXT[lang]["choose_language"], reply_markup=language_keyboard())
        await set_active_menu(state, edited)
        return

    if data == "nav:categories":
        await show_categories(callback.message, lang)
        await set_active_menu(state, callback.message)
        return

    if data == "cat:law":
        edited = await safe_edit_text(callback.message, UI_TEXT[lang]["choose_law_subcategory"], reply_markup=law_keyboard(lang))
        await set_active_menu(state, edited)
        return

    if data == "cat:call_center":
        edited = await safe_edit_text(callback.message, CALL_CENTER_TEXT[lang], reply_markup=call_center_keyboard(lang))
        await set_active_menu(state, edited)
        return

    if data == "cat:translations":
        edited = await safe_edit_text(
            callback.message,
            UI_TEXT[lang]["choose_translations_category"],
            reply_markup=translations_keyboard(lang),
        )
        await set_active_menu(state, edited)
        return

    if data == "cat:specialists":
        edited = await safe_edit_text(
            callback.message,
            SPECIALISTS_TEXT[lang],
            reply_markup=back_to_categories_keyboard(lang),
        )
        await set_active_menu(state, edited)
        return

    if data.startswith("law:"):
        key = data.split(":", 1)[1]
        section = LAW_SECTIONS.get(key)
        if section:
            active_message = await show_section_text(callback.message, section["text"], lang, back_callback="cat:law")
            await set_active_menu(state, active_message)
        return

    if data.startswith("tr:"):
        key = data.split(":", 1)[1]
        section = TRANSLATIONS.get(key)
        if section:
            active_message = await show_section_text(callback.message, section["text"], lang, back_callback="cat:translations")
            await set_active_menu(state, active_message)
        return


async def main() -> None:
    load_dotenv()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set. Add it to .env or environment variables.")

    logging.basicConfig(
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        level=logging.INFO,
    )

    bot = Bot(token=token)
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped.")
