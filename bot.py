import asyncio
import logging
import re
import os
import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional, Set, Any, Tuple
from pyrogram import Client, filters
from pyrogram.errors import (
    SessionPasswordNeeded, PhoneCodeInvalid,
    PhoneNumberInvalid, FloodWait, Unauthorized,
    PeerIdInvalid, ChannelInvalid, ChatAdminRequired
)
from pyrogram.types import Message, Dialog
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command, StateFilter
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import random
import time

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ====== КОНФИГУРАЦИЯ ======
API_ID = 32480523  # ЗАМЕНИТЕ НА СВОЙ
API_HASH = "147839735c9fa4e83451209e9b55cfc5"  # ЗАМЕНИТЕ НА СВОЙ
GITHUB_URL = "https://github.com/femilianferuk-droid/Monkey-Gram.git"

# Токен бота
BOT_TOKEN = os.getenv("BOT_TOKEN") or "ВАШ_ТОКЕН_БОТА"

# Инициализация
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ====== БАЗА ДАННЫХ ======
class Database:
    def __init__(self):
        self.conn = sqlite3.connect('monkey_gram.db', check_same_thread=False)
        self.create_tables()
    
    def create_tables(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                account_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                phone TEXT,
                session_string TEXT,
                first_name TEXT,
                last_name TEXT,
                username TEXT,
                registered_date TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bookmarks (
                bookmark_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                account_id INTEGER,
                chat_id INTEGER,
                title TEXT,
                username TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                FOREIGN KEY (account_id) REFERENCES accounts (account_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS auto_replies (
                reply_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                account_id INTEGER,
                trigger_text TEXT,
                reply_text TEXT,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                FOREIGN KEY (account_id) REFERENCES accounts (account_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS message_templates (
                template_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                name TEXT,
                text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        self.conn.commit()
    
    def add_user(self, user_id: int):
        cursor = self.conn.cursor()
        try:
            cursor.execute('INSERT OR IGNORE INTO users (user_id) VALUES (?)', (user_id,))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя: {e}")
    
    def add_account(self, user_id: int, phone: str, session_string: str, user_data: dict):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO accounts 
                (user_id, phone, session_string, first_name, last_name, username, registered_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                user_id,
                phone,
                session_string,
                user_data.get('first_name'),
                user_data.get('last_name'),
                user_data.get('username'),
                datetime.fromtimestamp(user_data.get('date', 0))
            ))
            self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления аккаунта: {e}")
            return None
    
    def get_user_accounts(self, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT account_id, phone, first_name, username 
            FROM accounts 
            WHERE user_id = ? AND is_active = 1
        ''', (user_id,))
        return cursor.fetchall()
    
    def get_account_session(self, account_id: int, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT session_string FROM accounts 
            WHERE account_id = ? AND user_id = ? AND is_active = 1
        ''', (account_id, user_id))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def delete_account(self, account_id: int, user_id: int):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                UPDATE accounts 
                SET is_active = 0 
                WHERE account_id = ? AND user_id = ?
            ''', (account_id, user_id))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка удаления аккаунта: {e}")
            return False
    
    # ====== ЗАКЛАДКИ ======
    def add_bookmark(self, user_id: int, account_id: int, chat_id: int, title: str = None, username: str = None):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO bookmarks 
                (user_id, account_id, chat_id, title, username)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, account_id, chat_id, title, username))
            self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления закладки: {e}")
            return False
    
    def get_user_bookmarks(self, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT b.bookmark_id, b.account_id, b.chat_id, b.title, b.username,
                   a.phone, a.first_name
            FROM bookmarks b
            JOIN accounts a ON b.account_id = a.account_id
            WHERE b.user_id = ?
            ORDER BY b.added_at DESC
        ''', (user_id,))
        return cursor.fetchall()
    
    def delete_bookmark(self, bookmark_id: int, user_id: int):
        cursor = self.conn.cursor()
        try:
            cursor.execute('DELETE FROM bookmarks WHERE bookmark_id = ? AND user_id = ?', (bookmark_id, user_id))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка удаления закладки: {e}")
            return False
    
    # ====== АВТООТВЕТЧИК ======
    def add_auto_reply(self, user_id: int, account_id: int, trigger: str, reply: str):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO auto_replies (user_id, account_id, trigger_text, reply_text)
                VALUES (?, ?, ?, ?)
            ''', (user_id, account_id, trigger, reply))
            self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления автоответа: {e}")
            return None
    
    def get_auto_replies(self, user_id: int, account_id: int = None):
        cursor = self.conn.cursor()
        if account_id:
            cursor.execute('''
                SELECT reply_id, trigger_text, reply_text, is_active 
                FROM auto_replies 
                WHERE user_id = ? AND account_id = ?
                ORDER BY created_at DESC
            ''', (user_id, account_id))
        else:
            cursor.execute('''
                SELECT reply_id, account_id, trigger_text, reply_text, is_active 
                FROM auto_replies 
                WHERE user_id = ?
                ORDER BY created_at DESC
            ''', (user_id,))
        return cursor.fetchall()
    
    def toggle_auto_reply(self, reply_id: int, user_id: int):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                UPDATE auto_replies 
                SET is_active = NOT is_active 
                WHERE reply_id = ? AND user_id = ?
            ''', (reply_id, user_id))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка переключения автоответа: {e}")
            return False
    
    def delete_auto_reply(self, reply_id: int, user_id: int):
        cursor = self.conn.cursor()
        try:
            cursor.execute('DELETE FROM auto_replies WHERE reply_id = ? AND user_id = ?', (reply_id, user_id))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка удаления автоответа: {e}")
            return False
    
    # ====== ШАБЛОНЫ СООБЩЕНИЙ ======
    def add_template(self, user_id: int, name: str, text: str):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO message_templates (user_id, name, text)
                VALUES (?, ?, ?)
            ''', (user_id, name, text))
            self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления шаблона: {e}")
            return None
    
    def get_templates(self, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT template_id, name, text 
            FROM message_templates 
            WHERE user_id = ?
            ORDER BY created_at DESC
        ''', (user_id,))
        return cursor.fetchall()
    
    def delete_template(self, template_id: int, user_id: int):
        cursor = self.conn.cursor()
        try:
            cursor.execute('DELETE FROM message_templates WHERE template_id = ? AND user_id = ?', (template_id, user_id))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка удаления шаблона: {e}")
            return False

db = Database()

# ====== СОСТОЯНИЯ FSM ======
class Form(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()
    waiting_for_message_count = State()
    waiting_for_delay = State()
    waiting_for_message_text = State()
    waiting_for_folder_selection = State()
    waiting_for_auto_reply_trigger = State()
    waiting_for_auto_reply_text = State()
    waiting_for_template_name = State()
    waiting_for_template_text = State()
    waiting_for_chat_for_bookmark = State()
    waiting_for_bookmark_name = State()

# ====== ХРАНИЛИЩА ======
active_tasks: Dict[int, List[asyncio.Task]] = {}
check_catchers: Dict[int, Dict[int, bool]] = {}
mass_send_data: Dict[int, Dict] = {}
selected_accounts_for_mass: Dict[int, List[int]] = {}
user_clients: Dict[int, Any] = {}
user_folders_cache: Dict[int, Dict[int, List[Dict]]] = {}
auto_reply_tasks: Dict[int, Dict[int, asyncio.Task]] = {}
temp_bookmark_data: Dict[int, Dict] = {}

# ====== КЛАВИАТУРЫ ======
def get_main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Функции", callback_data="functions")],
        [InlineKeyboardButton(text="🐙 Мы на GitHub", url=GITHUB_URL)]
    ])

def get_functions_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Добавить аккаунт", callback_data="add_account")],
        [InlineKeyboardButton(text="📊 Мои аккаунты", callback_data="my_accounts")],
        [InlineKeyboardButton(text="📨 Рассылка по папкам", callback_data="mass_send")],
        [InlineKeyboardButton(text="🔖 Мои закладки", callback_data="bookmarks_menu")],
        [InlineKeyboardButton(text="🤖 Автоответчик", callback_data="auto_reply_menu")],
        [InlineKeyboardButton(text="📝 Шаблоны сообщений", callback_data="templates_menu")],
        [InlineKeyboardButton(text="💰 Ловец чеков", callback_data="check_catcher_menu")],
        [InlineKeyboardButton(text="🛡️ Проверка спам блока", callback_data="check_spam_block")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

def get_accounts_menu(user_id: int, mode: str = "view"):
    """mode: view, delete, mass_send, auto_reply, bookmarks"""
    accounts = db.get_user_accounts(user_id)
    if not accounts:
        return None
    
    keyboard = []
    for acc in accounts:
        account_id, phone, first_name, username = acc
        display_name = f"{first_name or ''} {username or ''}".strip() or phone[:10]
        
        if mode == "delete":
            keyboard.append([
                InlineKeyboardButton(
                    text=f"🗑️ {display_name}",
                    callback_data=f"delete_confirm_{account_id}"
                )
            ])
        elif mode == "mass_send":
            is_selected = account_id in selected_accounts_for_mass.get(user_id, [])
            icon = "✅" if is_selected else "⬜"
            keyboard.append([
                InlineKeyboardButton(
                    text=f"{icon} {display_name}",
                    callback_data=f"mass_select_{account_id}"
                )
            ])
        elif mode == "auto_reply":
            keyboard.append([
                InlineKeyboardButton(
                    text=f"🤖 {display_name}",
                    callback_data=f"auto_reply_account_{account_id}"
                )
            ])
        elif mode == "bookmarks":
            keyboard.append([
                InlineKeyboardButton(
                    text=f"🔖 {display_name}",
                    callback_data=f"add_bookmark_account_{account_id}"
                )
            ])
        else:
            keyboard.append([
                InlineKeyboardButton(
                    text=f"📱 {display_name}",
                    callback_data=f"account_info_{account_id}"
                )
            ])
    
    if mode == "delete":
        keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="my_accounts")])
    elif mode == "mass_send":
        selected_count = len(selected_accounts_for_mass.get(user_id, []))
        if selected_count > 0:
            keyboard.append([
                InlineKeyboardButton(text=f"📋 Далее ({selected_count})", callback_data="mass_next_step"),
                InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")
            ])
        else:
            keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")])
    else:
        keyboard.append([
            InlineKeyboardButton(text="🗑️ Удалить аккаунт", callback_data="delete_account_menu"),
            InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")
        ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_bookmarks_menu(user_id: int):
    bookmarks = db.get_user_bookmarks(user_id)
    if not bookmarks:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить закладку", callback_data="add_bookmark_menu")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")]
        ])
    
    keyboard = []
    for bm in bookmarks[:20]:
        bookmark_id, account_id, chat_id, title, username, phone, acc_name = bm
        display_name = title or username or f"Chat {chat_id}"
        keyboard.append([
            InlineKeyboardButton(
                text=f"🔖 {display_name[:20]}",
                callback_data=f"bookmark_action_{bookmark_id}"
            )
        ])
    
    keyboard.append([
        InlineKeyboardButton(text="➕ Добавить закладку", callback_data="add_bookmark_menu"),
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_bookmark_actions_menu(bookmark_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Перейти к чату", callback_data=f"goto_bookmark_{bookmark_id}")],
        [InlineKeyboardButton(text="🗑️ Удалить закладку", callback_data=f"delete_bookmark_{bookmark_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="bookmarks_menu")]
    ])

def get_auto_reply_menu(user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить автоответ", callback_data="add_auto_reply")],
        [InlineKeyboardButton(text="📋 Мои автоответы", callback_data="view_auto_replies")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")]
    ])

def get_templates_menu(user_id: int):
    templates = db.get_templates(user_id)
    if not templates:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить шаблон", callback_data="add_template")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")]
        ])
    
    keyboard = []
    for template in templates[:10]:
        template_id, name, text = template
        keyboard.append([
            InlineKeyboardButton(
                text=f"📝 {name}",
                callback_data=f"template_select_{template_id}"
            )
        ])
    
    keyboard.append([
        InlineKeyboardButton(text="➕ Добавить шаблон", callback_data="add_template"),
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_check_catcher_menu(user_id: int):
    accounts = db.get_user_accounts(user_id)
    if not accounts:
        return None
    
    keyboard = []
    for acc in accounts:
        account_id, phone, first_name, username = acc
        display_name = f"{first_name or ''} {username or ''}".strip() or phone[:10]
        is_active = check_catchers.get(user_id, {}).get(account_id, False)
        status = "✅ Вкл" if is_active else "❌ Выкл"
        keyboard.append([
            InlineKeyboardButton(
                text=f"{status} | {display_name}",
                callback_data=f"toggle_catcher_{account_id}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_back_button():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_action")]
    ])

# ====== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ======
async def safe_get_dialogs(client: Client, limit: int = 100):
    """Безопасное получение диалогов с обработкой FloodWait"""
    dialogs = []
    try:
        async for dialog in client.get_dialogs(limit=limit):
            dialogs.append(dialog)
            # Добавляем небольшую задержку между обработкой диалогов
            await asyncio.sleep(0.1)
    except FloodWait as e:
        logger.warning(f"FloodWait при получении диалогов: {e} секунд")
        await asyncio.sleep(e.value + 1)
        # Пробуем снова с меньшим лимитом
        async for dialog in client.get_dialogs(limit=50):
            dialogs.append(dialog)
            await asyncio.sleep(0.2)
    except Exception as e:
        logger.error(f"Ошибка при получении диалогов: {e}")
    
    return dialogs

async def safe_send_message(client, chat_id, text, retries=3):
    """Безопасная отправка сообщения с обработкой FloodWait"""
    for attempt in range(retries):
        try:
            await client.send_message(chat_id=chat_id, text=text)
            return True
        except FloodWait as e:
            wait_time = e.value
            logger.warning(f"FloodWait: ожидание {wait_time} секунд")
            await asyncio.sleep(wait_time + 1)
        except (PeerIdInvalid, ChannelInvalid, ChatAdminRequired) as e:
            logger.warning(f"Не удалось отправить в {chat_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Ошибка отправки: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2)
            else:
                return False
    return False

# ====== ОСНОВНЫЕ КОМАНДЫ ======
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    db.add_user(user_id)
    
    welcome_text = """
🐵 *Добро пожаловать в Monkey Gram!*

*Основные возможности:*
• 📱 Работа с несколькими аккаунтами
• 📨 Рассылка по папкам Telegram
• 🔖 Закладки для чатов
• 🤖 Автоответчик
• 📝 Шаблоны сообщений
• 💰 Ловец чеков CryptoBot
• 🛡️ Проверка спам блока

*Выберите действие:*
    """
    await message.answer(welcome_text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu())

# ====== ОБРАБОТЧИКИ КНОПОК ======
@dp.callback_query(F.data == "functions")
async def show_functions(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "⚙️ *Выберите функцию:*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_functions_menu()
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🐵 *Главное меню Monkey Gram:*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_main_menu()
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_functions")
async def back_to_functions(callback: types.CallbackQuery):
    await show_functions(callback)

# ====== ДОБАВЛЕНИЕ АККАУНТА ======
@dp.callback_query(F.data == "add_account")
async def start_add_account(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📱 *Добавление аккаунта*\n\n"
        "Пришлите номер телефона в международном формате:\n"
        "Пример: +79123456789",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_button()
    )
    await state.set_state(Form.waiting_for_phone)
    await callback.answer()

@dp.message(Form.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    try:
        phone = message.text.strip()
        
        if not re.match(r'^\+\d{10,15}$', phone):
            await message.answer(
                "❌ Неверный формат номера.\n"
                "Используйте международный формат: +79123456789\n"
                "Попробуйте еще раз:",
                reply_markup=get_back_button()
            )
            return
        
        await state.update_data(phone=phone)
        
        session_name = f"session_{message.from_user.id}_{int(datetime.now().timestamp())}"
        client = Client(
            name=session_name,
            api_id=API_ID,
            api_hash=API_HASH,
            workdir="sessions"
        )
        
        await client.connect()
        sent_code = await client.send_code(phone)
        
        await state.update_data(
            client=client,
            phone_code_hash=sent_code.phone_code_hash
        )
        
        user_clients[message.from_user.id] = client
        
        await message.answer(
            f"✅ *Номер принят:* `{phone}`\n\n"
            "📱 *Код отправлен на номер*\n\n"
            "Пришлите код из Telegram (5 цифр):",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_back_button()
        )
        await state.set_state(Form.waiting_for_code)
        
    except PhoneNumberInvalid:
        await message.answer(
            "❌ Неверный номер телефона. Проверьте и попробуйте снова:",
            reply_markup=get_back_button()
        )
    except FloodWait as e:
        await message.answer(
            f"⏳ Слишком много попыток. Подождите {e.value} секунд."
        )
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка отправки кода: {e}")
        await message.answer(
            f"❌ Ошибка: {str(e)[:100]}",
            reply_markup=get_back_button()
        )
        await state.clear()

@dp.message(Form.waiting_for_code)
async def process_code(message: types.Message, state: FSMContext):
    try:
        code = message.text.strip()
        
        if not re.match(r'^\d{5}$', code):
            await message.answer(
                "❌ Код должен содержать 5 цифр. Попробуйте еще раз:",
                reply_markup=get_back_button()
            )
            return
        
        data = await state.get_data()
        client = data.get('client')
        phone = data.get('phone')
        phone_code_hash = data.get('phone_code_hash')
        
        if not client:
            client = user_clients.get(message.from_user.id)
            if not client:
                await message.answer(
                    "❌ Ошибка данных. Начните заново.",
                    reply_markup=get_functions_menu()
                )
                await state.clear()
                return
        
        try:
            await client.sign_in(
                phone_number=phone,
                phone_code_hash=phone_code_hash,
                phone_code=code
            )
            
            await finish_authorization(client, phone, message, state)
            
        except SessionPasswordNeeded:
            await message.answer(
                "🔐 Требуется двухфакторная аутентификация.\n"
                "Пришлите пароль:",
                reply_markup=get_back_button()
            )
            await state.set_state(Form.waiting_for_password)
        except PhoneCodeInvalid:
            await message.answer(
                "❌ Неверный код. Попробуйте еще раз:",
                reply_markup=get_back_button()
            )
        except Exception as e:
            logger.error(f"Ошибка входа: {e}")
            await message.answer(f"❌ Ошибка входа: {str(e)[:100]}")
            await client.disconnect()
            if message.from_user.id in user_clients:
                del user_clients[message.from_user.id]
            await state.clear()
            
    except Exception as e:
        logger.error(f"Общая ошибка process_code: {e}")
        await message.answer(f"❌ Ошибка: {str(e)[:100]}")
        await state.clear()

@dp.message(Form.waiting_for_password)
async def process_password(message: types.Message, state: FSMContext):
    try:
        password = message.text.strip()
        
        data = await state.get_data()
        client = data.get('client')
        phone = data.get('phone')
        
        if not client:
            client = user_clients.get(message.from_user.id)
            if not client:
                await message.answer(
                    "❌ Ошибка данных. Начните заново.",
                    reply_markup=get_functions_menu()
                )
                await state.clear()
                return
        
        await client.check_password(password)
        await finish_authorization(client, phone, message, state)
        
    except Exception as e:
        logger.error(f"Ошибка 2FA: {e}")
        await message.answer(f"❌ Неверный пароль: {str(e)[:100]}")
        try:
            if client:
                await client.disconnect()
        except:
            pass
        if message.from_user.id in user_clients:
            del user_clients[message.from_user.id]
        await state.clear()

async def finish_authorization(client: Client, phone: str, message: types.Message, state: FSMContext):
    try:
        user_data = await client.get_me()
        session_string = await client.export_session_string()
        
        account_id = db.add_account(
            user_id=message.from_user.id,
            phone=phone,
            session_string=session_string,
            user_data={
                'first_name': user_data.first_name,
                'last_name': user_data.last_name,
                'username': user_data.username,
                'date': user_data.date if hasattr(user_data, 'date') else 0
            }
        )
        
        response_text = (
            f"✅ *Аккаунт успешно добавлен!*\n\n"
            f"*Имя:* {user_data.first_name or 'Не указано'}\n"
            f"*Фамилия:* {user_data.last_name or 'Не указана'}\n"
            f"*Username:* @{user_data.username or 'Не указан'}\n"
            f"*Номер:* `{phone}`\n"
            f"*ID аккаунта:* `{account_id}`\n"
        )
        
        if hasattr(user_data, 'date'):
            reg_date = datetime.fromtimestamp(user_data.date)
            response_text += f"*Дата регистрации:* {reg_date.strftime('%d.%m.%Y')}\n"
        
        await message.answer(
            response_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        
    except Exception as e:
        logger.error(f"Ошибка завершения авторизации: {e}")
        await message.answer(
            f"❌ Ошибка при сохранении аккаунта: {str(e)[:100]}",
            reply_markup=get_functions_menu()
        )
    finally:
        try:
            await client.disconnect()
        except:
            pass
        
        if message.from_user.id in user_clients:
            del user_clients[message.from_user.id]
        
        await state.clear()

# ====== МОИ АККАУНТЫ ======
@dp.callback_query(F.data == "my_accounts")
async def show_my_accounts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    accounts = db.get_user_accounts(user_id)
    
    if not accounts:
        await callback.message.edit_text(
            "📊 *У вас нет добавленных аккаунтов.*\n\n"
            "Нажмите 'Добавить аккаунт' для начала работы.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
    else:
        text = "📊 *Ваши аккаунты:*\n\n"
        for acc in accounts:
            account_id, phone, first_name, username = acc
            display_name = f"{first_name or ''} {username or ''}".strip() or phone
            text += f"• *{display_name}*\n"
            text += f"  📱 `{phone}`\n"
            text += f"  🆔 ID: `{account_id}`\n\n"
        
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_accounts_menu(user_id, "view")
        )
    await callback.answer()

# ====== РАССЫЛКА ПО ПАПКАМ TELEGRAM ======
@dp.callback_query(F.data == "mass_send")
async def start_mass_send(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    accounts = db.get_user_accounts(user_id)
    
    if not accounts:
        await callback.message.edit_text(
            "❌ *Сначала добавьте аккаунты!*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        await callback.answer()
        return
    
    mass_send_data[user_id] = {
        'count': None,
        'delay': None,
        'text': None,
        'account_id': None,
        'folder_id': None
    }
    
    selected_accounts_for_mass[user_id] = []
    
    await callback.message.edit_text(
        "📨 *Настройка рассылки - Шаг 1/4*\n\n"
        "Пришлите количество сообщений для отправки (1-1000):",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_button()
    )
    
    await state.set_state(Form.waiting_for_message_count)
    await callback.answer()

@dp.message(Form.waiting_for_message_count)
async def process_message_count(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    try:
        count = int(message.text.strip())
        if count < 1 or count > 1000:
            raise ValueError
        
        mass_send_data[user_id]['count'] = count
        
        await message.answer(
            f"✅ *Шаг 1/4 завершен*\n"
            f"Количество сообщений: *{count}*\n\n"
            f"*Шаг 2/4:* Укажите задержку между сообщениями (в секундах):\n"
            f"Пример: 5 (минимум 3, максимум 3600)",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_back_button()
        )
        await state.set_state(Form.waiting_for_delay)
        
    except ValueError:
        await message.answer(
            "❌ Неверное количество. Введите число от 1 до 1000:",
            reply_markup=get_back_button()
        )

@dp.message(Form.waiting_for_delay)
async def process_delay(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    try:
        delay = float(message.text.strip())
        if delay < 3 or delay > 3600:
            raise ValueError
        
        mass_send_data[user_id]['delay'] = delay
        
        await message.answer(
            f"✅ *Шаг 2/4 завершен*\n"
            f"Задержка: *{delay}* секунд\n\n"
            f"*Шаг 3/4:* Пришлите текст сообщения для рассылки:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_back_button()
        )
        await state.set_state(Form.waiting_for_message_text)
        
    except ValueError:
        await message.answer(
            "❌ Неверная задержка. Введите число от 3 до 3600 секунд:",
            reply_markup=get_back_button()
        )

@dp.message(Form.waiting_for_message_text)
async def process_message_text(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    text = message.text.strip()
    if len(text) > 4096:
        await message.answer(
            "❌ Текст слишком длинный. Максимум 4096 символов.\n"
            "Пришлите текст короче:",
            reply_markup=get_back_button()
        )
        return
    
    mass_send_data[user_id]['text'] = text
    
    await message.answer(
        f"✅ *Шаг 3/4 завершен*\n"
        f"Текст сообщения сохранен\n\n"
        f"*Шаг 4/4:* Выберите аккаунт для рассылки:\n\n"
        f"Нажмите на аккаунт для выбора",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_accounts_menu(user_id, "mass_send")
    )
    await state.clear()

@dp.callback_query(F.data.startswith("mass_select_"))
async def select_account_for_mass(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    account_id = int(callback.data.split("_")[2])
    
    selected_accounts_for_mass[user_id] = [account_id]
    mass_send_data[user_id]['account_id'] = account_id
    
    # Показываем меню выбора типа чатов
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📁 Все чаты", callback_data=f"mass_category_all_{account_id}")],
        [InlineKeyboardButton(text="👤 Личные сообщения", callback_data=f"mass_category_private_{account_id}")],
        [InlineKeyboardButton(text="👥 Группы и супергруппы", callback_data=f"mass_category_groups_{account_id}")],
        [InlineKeyboardButton(text="📢 Каналы", callback_data=f"mass_category_channels_{account_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="mass_send")]
    ])
    
    await callback.message.edit_text(
        f"✅ *Аккаунт выбран!*\n\n"
        f"Выберите категорию чатов для рассылки:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("mass_category_"))
async def select_mass_category(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    parts = callback.data.split("_")
    category = parts[2]
    account_id = int(parts[3])
    
    # Сохраняем выбранную категорию
    category_map = {
        'all': 0,
        'private': 1,
        'groups': 2,
        'channels': 3
    }
    
    mass_send_data[user_id]['folder_id'] = category_map.get(category, 0)
    
    # Показываем сводку
    data = mass_send_data[user_id]
    
    category_names = {
        'all': 'Все чаты',
        'private': 'Личные сообщения',
        'groups': 'Группы и супергруппы',
        'channels': 'Каналы'
    }
    
    summary = (
        f"📋 *Сводка рассылки:*\n\n"
        f"• Аккаунт: *{account_id}*\n"
        f"• Категория: *{category_names.get(category, 'Все чаты')}*\n"
        f"• Сообщений в каждый чат: *{data['count']}*\n"
        f"• Задержка: *{data['delay']}* сек\n"
        f"• Текст: *{data['text'][:50]}...*\n\n"
        f"⚠️ *Важно:* Для избежания блокировок используется задержка {data['delay']} сек.\n\n"
        f"*Начать рассылку?*"
    )
    
    await callback.message.edit_text(
        summary,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Начать", callback_data="mass_send_confirm"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_mass_send")
            ]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data == "mass_send_confirm")
async def confirm_mass_send(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    # Запускаем реальную рассылку в фоне
    asyncio.create_task(run_mass_send_to_folder(user_id, callback.message))
    await callback.answer("Рассылка запущена!")

async def run_mass_send_to_folder(user_id: int, message: types.Message):
    """Рассылка по категории с защитой от FloodWait"""
    try:
        data = mass_send_data[user_id]
        account_id = data['account_id']
        folder_id = data['folder_id']
        
        if not account_id or folder_id is None:
            await message.edit_text(
                "❌ Ошибка: не все параметры указаны.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_functions_menu()
            )
            return
        
        session_string = db.get_account_session(account_id, user_id)
        if not session_string:
            await message.edit_text(
                "❌ Ошибка: сессия не найдена.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_functions_menu()
            )
            return
        
        client = Client(
            name=f"folder_mass_{user_id}_{account_id}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,
            workdir="sessions"
        )
        
        await client.start()
        
        # Получаем диалоги с безопасной обработкой
        await message.edit_text(
            "⏳ *Получаю список чатов...*\n\n"
            "Это может занять несколько секунд.",
            parse_mode=ParseMode.MARKDOWN
        )
        
        dialogs = await safe_get_dialogs(client, limit=200)
        
        # Фильтруем чаты по выбранной категории
        chats_in_folder = []
        
        for dialog in dialogs:
            chat = dialog.chat
            
            if not chat:
                continue
            
            # Пропускаем самого себя
            my_id = (await client.get_me()).id
            if hasattr(chat, 'id') and chat.id == my_id:
                continue
            
            # Фильтруем по категории
            include_chat = False
            
            if folder_id == 0:  # Все чаты
                include_chat = True
            elif folder_id == 1:  # Личные сообщения
                if chat.type == "private":
                    include_chat = True
            elif folder_id == 2:  # Группы
                if chat.type in ["group", "supergroup"]:
                    include_chat = True
            elif folder_id == 3:  # Каналы
                if chat.type == "channel":
                    include_chat = True
            
            if include_chat:
                chat_title = getattr(chat, 'title', None) or getattr(chat, 'first_name', f'Chat {chat.id}')
                chats_in_folder.append({
                    'id': chat.id,
                    'type': chat.type,
                    'title': chat_title
                })
        
        if not chats_in_folder:
            await message.edit_text(
                f"❌ *Не найдено чатов в выбранной категории.*\n\n"
                f"Проверьте, есть ли у аккаунта:\n"
                f"• Личные чаты (если выбраны личные сообщения)\n"
                f"• Группы (если выбраны группы)\n"
                f"• Каналы (если выбраны каналы)",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_functions_menu()
            )
            await client.stop()
            return
        
        total_messages = data['count'] * len(chats_in_folder)
        progress_msg = await message.edit_text(
            f"🚀 *Рассылка начата!*\n\n"
            f"Чатов в категории: *{len(chats_in_folder)}*\n"
            f"Всего сообщений: *{total_messages}*\n"
            f"Задержка между сообщениями: *{data['delay']}* сек\n"
            f"Ожидаемое время: *{total_messages * data['delay'] / 60:.1f}* минут\n\n"
            f"Прогресс: 0/{total_messages} (0%)\n"
            f"Статус: Инициализация...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        sent_count = 0
        failed_count = 0
        
        for chat_index, chat_info in enumerate(chats_in_folder):
            chat_id = chat_info['id']
            
            for message_num in range(data['count']):
                try:
                    # Используем безопасную отправку
                    success = await safe_send_message(client, chat_id, data['text'])
                    
                    if success:
                        sent_count += 1
                    else:
                        failed_count += 1
                    
                    # Обновляем прогресс каждые 5 сообщений или 5%
                    if (sent_count + failed_count) % 5 == 0 or (sent_count + failed_count) % max(1, total_messages // 20) == 0:
                        progress = sent_count + failed_count
                        percent = (progress / total_messages) * 100 if total_messages > 0 else 0
                        await progress_msg.edit_text(
                            f"🚀 *Рассылка в процессе...*\n\n"
                            f"Прогресс: {progress}/{total_messages} ({percent:.1f}%)\n"
                            f"Чат: {chat_index+1}/{len(chats_in_folder)}\n"
                            f"Сообщение: {message_num+1}/{data['count']}\n"
                            f"✅ Отправлено: {sent_count}\n"
                            f"❌ Ошибок: {failed_count}",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    
                    # Задержка между сообщениями
                    if message_num < data['count'] - 1 or chat_index < len(chats_in_folder) - 1:
                        await asyncio.sleep(data['delay'])
                        
                except Exception as e:
                    failed_count += 1
                    logger.error(f"Ошибка отправки: {e}")
                    await asyncio.sleep(5)  # Пауза при ошибке
        
        success_rate = (sent_count / total_messages * 100) if total_messages > 0 else 0
        
        await progress_msg.edit_text(
            f"✅ *Рассылка завершена!*\n\n"
            f"• Успешно отправлено: *{sent_count}* сообщений\n"
            f"• Не отправлено: *{failed_count}* сообщений\n"
            f"• Успешность: *{success_rate:.1f}%*\n"
            f"• Чатов обработано: *{len(chats_in_folder)}*\n"
            f"• Время выполнения: *{total_messages * data['delay'] / 60:.1f}* минут\n\n"
            f"Рассылка завершена.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        
        await client.stop()
        
    except Exception as e:
        logger.error(f"Ошибка рассылки: {e}")
        await message.edit_text(
            f"❌ *Критическая ошибка рассылки!*\n\n"
            f"Ошибка: {str(e)[:200]}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
    finally:
        if user_id in mass_send_data:
            del mass_send_data[user_id]
        if user_id in selected_accounts_for_mass:
            del selected_accounts_for_mass[user_id]

# ====== ЗАКЛАДКИ ======
@dp.callback_query(F.data == "bookmarks_menu")
async def show_bookmarks_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    bookmarks = db.get_user_bookmarks(user_id)
    
    if not bookmarks:
        await callback.message.edit_text(
            "🔖 *У вас пока нет закладок.*\n\n"
            "Добавьте закладки для быстрого доступа к чатам.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_bookmarks_menu(user_id)
        )
    else:
        text = "🔖 *Ваши закладки:*\n\n"
        for bm in bookmarks[:15]:
            bookmark_id, account_id, chat_id, title, username, phone, acc_name = bm
            display_name = title or username or f"Chat {chat_id}"
            acc_display = acc_name or phone[:10]
            text += f"• *{display_name[:30]}*\n"
            text += f"  Аккаунт: {acc_display}\n"
            text += f"  ID: `{bookmark_id}`\n\n"
        
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_bookmarks_menu(user_id)
        )
    await callback.answer()

@dp.callback_query(F.data == "add_bookmark_menu")
async def add_bookmark_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    accounts = db.get_user_accounts(user_id)
    
    if not accounts:
        await callback.message.edit_text(
            "❌ *Сначала добавьте аккаунты!*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        await callback.answer()
        return
    
    await callback.message.edit_text(
        "📱 *Выберите аккаунт для добавления закладок:*\n\n"
        "Выберите аккаунт, чтобы получить список его чатов.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_accounts_menu(user_id, "bookmarks")
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("add_bookmark_account_"))
async def select_account_for_bookmark(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    account_id = int(callback.data.split("_")[3])
    
    # Сохраняем выбранный аккаунт
    temp_bookmark_data[user_id] = {'account_id': account_id}
    
    session_string = db.get_account_session(account_id, user_id)
    if not session_string:
        await callback.answer("❌ Сессия не найдена")
        return
    
    await callback.message.edit_text(
        "⏳ *Загружаю список чатов...*\n\n"
        "Это может занять некоторое время.",
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Загружаем чаты в фоне с безопасной обработкой
    asyncio.create_task(load_chats_for_bookmark_safe(user_id, account_id, session_string, callback.message))
    await callback.answer()

async def load_chats_for_bookmark_safe(user_id: int, account_id: int, session_string: str, message: types.Message):
    """Безопасная загрузка чатов для закладок с обработкой FloodWait"""
    client = Client(
        name=f"bookmark_loader_{user_id}_{account_id}",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_string,
        workdir="sessions"
    )
    
    try:
        await client.start()
        
        # Получаем диалоги с безопасной обработкой
        dialogs = await safe_get_dialogs(client, limit=50)
        
        chats_list = []
        for dialog in dialogs:
            chat = dialog.chat
            
            if not chat:
                continue
            
            # Пропускаем самого себя
            my_id = (await client.get_me()).id
            if hasattr(chat, 'id') and chat.id == my_id:
                continue
            
            # Собираем основную информацию о чате без лишних запросов
            chat_info = {
                'id': chat.id,
                'type': chat.type,
                'title': getattr(chat, 'title', None) or getattr(chat, 'first_name', f'Chat {chat.id}'),
                'username': getattr(chat, 'username', None)
            }
            chats_list.append(chat_info)
        
        if not chats_list:
            await message.edit_text(
                "❌ *Не найдено чатов для добавления в закладки.*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_functions_menu()
            )
            return
        
        # Сохраняем список чатов во временное хранилище
        temp_bookmark_data[user_id]['chats'] = chats_list
        
        # Создаем клавиатуру с чатами
        keyboard = []
        for i, chat in enumerate(chats_list[:20]):
            display_name = chat['title'][:30] if chat['title'] else f"Chat {chat['id']}"
            username_part = f" (@{chat['username']})" if chat['username'] else ""
            keyboard.append([
                InlineKeyboardButton(
                    text=f"💬 {display_name}{username_part}",
                    callback_data=f"select_chat_for_bookmark_{i}"
                )
            ])
        
        keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="bookmarks_menu")])
        
        await message.edit_text(
            f"✅ *Найдено {len(chats_list)} чатов*\n\n"
            f"Выберите чат для добавления в закладки:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
        )
        
    except Exception as e:
        logger.error(f"Ошибка загрузки чатов для закладок: {e}")
        await message.edit_text(
            f"❌ *Ошибка загрузки чатов:*\n\n`{str(e)[:200]}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
    finally:
        try:
            await client.stop()
        except:
            pass

@dp.callback_query(F.data.startswith("select_chat_for_bookmark_"))
async def select_chat_for_bookmark(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    chat_index = int(callback.data.split("_")[4])
    
    if user_id not in temp_bookmark_data:
        await callback.answer("❌ Данные устарели")
        return
    
    chats = temp_bookmark_data[user_id].get('chats', [])
    account_id = temp_bookmark_data[user_id].get('account_id')
    
    if chat_index >= len(chats):
        await callback.answer("❌ Чат не найден")
        return
    
    chat = chats[chat_index]
    
    # Добавляем закладку в базу
    bookmark_id = db.add_bookmark(
        user_id=user_id,
        account_id=account_id,
        chat_id=chat['id'],
        title=chat['title'],
        username=chat['username']
    )
    
    if bookmark_id:
        await callback.message.edit_text(
            f"✅ *Закладка добавлена!*\n\n"
            f"• Чат: *{chat['title']}*\n"
            f"• ID чата: `{chat['id']}`\n"
            f"• Username: @{chat['username'] or 'Нет'}\n"
            f"• Тип: {chat['type']}\n\n"
            f"Теперь вы можете быстро перейти к этому чату из меню закладок.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_bookmarks_menu(user_id)
        )
    else:
        await callback.message.edit_text(
            "❌ *Ошибка при добавлении закладки.*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_bookmarks_menu(user_id)
        )
    
    # Очищаем временные данные
    if user_id in temp_bookmark_data:
        del temp_bookmark_data[user_id]
    
    await callback.answer()

@dp.callback_query(F.data.startswith("bookmark_action_"))
async def handle_bookmark_action(callback: types.CallbackQuery):
    bookmark_id = int(callback.data.split("_")[2])
    
    await callback.message.edit_text(
        "🔖 *Действия с закладкой:*\n\n"
        "Выберите что вы хотите сделать с этой закладкой:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_bookmark_actions_menu(bookmark_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("goto_bookmark_"))
async def goto_bookmark(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    bookmark_id = int(callback.data.split("_")[2])
    
    # Получаем информацию о закладке
    bookmarks = db.get_user_bookmarks(user_id)
    target_bookmark = None
    
    for bm in bookmarks:
        if bm[0] == bookmark_id:
            target_bookmark = bm
            break
    
    if not target_bookmark:
        await callback.answer("❌ Закладка не найдена")
        return
    
    bookmark_id, account_id, chat_id, title, username, phone, acc_name = target_bookmark
    
    await callback.message.edit_text(
        f"🔖 *Ссылка на чат:*\n\n"
        f"• Чат: *{title or 'Без названия'}*\n"
        f"• Аккаунт: {acc_name or phone}\n"
        f"• ID чата: `{chat_id}`\n\n"
        f"Чтобы перейти к чату, используйте ID выше в соответствующих функциях.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_bookmarks_menu(user_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_bookmark_"))
async def delete_bookmark(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    bookmark_id = int(callback.data.split("_")[2])
    
    if db.delete_bookmark(bookmark_id, user_id):
        await callback.message.edit_text(
            "✅ *Закладка удалена!*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_bookmarks_menu(user_id)
        )
    else:
        await callback.message.edit_text(
            "❌ *Ошибка при удалении закладки.*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_bookmarks_menu(user_id)
        )
    await callback.answer()

# ====== ПРОВЕРКА СПАМ БЛОКА ======
@dp.callback_query(F.data == "check_spam_block")
async def check_spam_block_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    accounts = db.get_user_accounts(user_id)
    
    if not accounts:
        await callback.message.edit_text(
            "❌ *Сначала добавьте аккаунты!*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        await callback.answer()
        return
    
    keyboard = []
    for acc in accounts:
        account_id, phone, first_name, username = acc
        display_name = f"{first_name or ''} {username or ''}".strip() or phone[:10]
        keyboard.append([
            InlineKeyboardButton(
                text=f"🛡️ {display_name}",
                callback_data=f"check_spam_{account_id}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")])
    
    await callback.message.edit_text(
        "🛡️ *Проверка спам блока*\n\n"
        "Бот проверит статус аккаунта через @SpamBot\n\n"
        "Выберите аккаунт для проверки:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("check_spam_"))
async def check_spam_block(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    account_id = int(callback.data.split("_")[2])
    
    await callback.message.edit_text(
        f"🛡️ *Проверяю статус спам блока...*\n\n"
        f"Аккаунт: {account_id}\n"
        f"Статус: Подключение к @SpamBot...",
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Запускаем проверку в фоне
    asyncio.create_task(run_spam_check_improved(user_id, account_id, callback.message))
    await callback.answer()

async def run_spam_check_improved(user_id: int, account_id: int, message: types.Message):
    """Улучшенная проверка статуса через @SpamBot с ожиданием ответа"""
    session_string = db.get_account_session(account_id, user_id)
    if not session_string:
        await message.edit_text(
            "❌ *Ошибка:* Сессия не найдена.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        return
    
    client = Client(
        name=f"spam_check_{user_id}_{account_id}",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_string,
        workdir="sessions"
    )
    
    try:
        await client.start()
        
        # Получаем информацию об аккаунте
        me = await client.get_me()
        acc_info = f"{me.first_name or ''} {me.last_name or ''}".strip() or me.username or "Неизвестно"
        
        await message.edit_text(
            f"🛡️ *Проверяю статус спам блока...*\n\n"
            f"Аккаунт: {acc_info}\n"
            f"Статус: Отправляю запрос @SpamBot...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Пробуем отправить сообщение @SpamBot
        try:
            # Очищаем историю с @SpamBot если есть старые сообщения
            try:
                await client.delete_user_history("spambot")
                await asyncio.sleep(1)
            except:
                pass
            
            # Отправляем /start
            await client.send_message("spambot", "/start")
            
            await message.edit_text(
                f"🛡️ *Проверяю статус спам блока...*\n\n"
                f"Аккаунт: {acc_info}\n"
                f"Статус: Жду ответа от @SpamBot (5 сек)...",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Ждем 5 секунд для получения ответа
            await asyncio.sleep(5)
            
            # Получаем последние сообщения от @SpamBot
            spam_messages = []
            try:
                async for msg in client.get_chat_history("spambot", limit=3):
                    if msg.from_user and msg.from_user.username == "spambot":
                        message_text = msg.text or msg.caption or ""
                        if message_text:
                            spam_messages.append({
                                'text': message_text,
                                'date': msg.date
                            })
            except Exception as e:
                logger.error(f"Ошибка при получении истории @SpamBot: {e}")
            
            if spam_messages:
                # Берем самое последнее сообщение
                latest_message = spam_messages[0]['text']
                
                # Анализируем сообщение на наличие информации о спам блоке
                status = "❓ Неизвестно"
                details = ""
                
                # Ключевые слова для определения статуса
                spam_keywords = {
                    "ограничен": "🚫 *ОГРАНИЧЕН* (Spam Block)",
                    "ограничение": "🚫 *ОГРАНИЧЕН* (Spam Block)",
                    "limited": "🚫 *ОГРАНИЧЕН* (Spam Block)",
                    "restrict": "🚫 *ОГРАНИЧЕН* (Spam Block)",
                    "спам": "🚫 *ОГРАНИЧЕН* (Spam Block)",
                    "spam": "🚫 *ОГРАНИЧЕН* (Spam Block)",
                    "блок": "🚫 *ОГРАНИЧЕН* (Spam Block)",
                    "block": "🚫 *ОГРАНИЧЕН* (Spam Block)"
                }
                
                warning_keywords = {
                    "предупреждение": "⚠️ *ПРЕДУПРЕЖДЕНИЕ*",
                    "warning": "⚠️ *ПРЕДУПРЕЖДЕНИЕ*"
                }
                
                good_keywords = {
                    "всё хорошо": "✅ *НОРМАЛЬНО* (No Spam Block)",
                    "все хорошо": "✅ *НОРМАЛЬНО* (No Spam Block)",
                    "хорошо": "✅ *НОРМАЛЬНО* (No Spam Block)",
                    "good": "✅ *НОРМАЛЬНО* (No Spam Block)",
                    "fine": "✅ *НОРМАЛЬНО* (No Spam Block)",
                    "ok": "✅ *НОРМАЛЬНО* (No Spam Block)",
                    "ок": "✅ *НОРМАЛЬНО* (No Spam Block)"
                }
                
                # Проверяем по ключевым словам
                message_lower = latest_message.lower()
                
                for keyword, status_text in spam_keywords.items():
                    if keyword in message_lower:
                        status = status_text
                        details = "Обнаружены ограничения на аккаунте"
                        break
                
                if status == "❓ Неизвестно":
                    for keyword, status_text in warning_keywords.items():
                        if keyword in message_lower:
                            status = status_text
                            details = "Есть предупреждения"
                            break
                
                if status == "❓ Неизвестно":
                    for keyword, status_text in good_keywords.items():
                        if keyword in message_lower:
                            status = status_text
                            details = "Аккаунт в порядке"
                            break
                
                # Если статус все еще неизвестен, пытаемся определить по контексту
                if status == "❓ Неизвестно":
                    if any(word in message_lower for word in ["can't", "cannot", "unable", "problem"]):
                        status = "⚠️ *ВОЗМОЖНЫ ПРОБЛЕМЫ*"
                        details = "Возможны проблемы с отправкой сообщений"
                    else:
                        status = "✅ *ВЕРОЯТНО НОРМАЛЬНО*"
                        details = "Прямых указаний на ограничения нет"
                
                await message.edit_text(
                    f"🛡️ *Результат проверки спам блока:*\n\n"
                    f"• Аккаунт: *{acc_info}*\n"
                    f"• Статус: {status}\n"
                    f"• Детали: {details}\n"
                    f"• ID: `{account_id}`\n\n"
                    f"*Ответ @SpamBot:*\n"
                    f"```\n{latest_message[:300]}\n```\n\n"
                    f"*Анализ:* Сообщение проанализировано на наличие ключевых слов.",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_functions_menu()
                )
            else:
                await message.edit_text(
                    f"🛡️ *Результат проверки спам блока:*\n\n"
                    f"• Аккаунт: *{acc_info}*\n"
                    f"• Статус: ✅ *НОРМАЛЬНО*\n"
                    f"• Причина: @SpamBot не ответил\n\n"
                    f"*Интерпретация:* Если @SpamBot не ответил, вероятно, аккаунт не имеет ограничений.\n\n"
                    f"Вы можете проверить вручную, написав /start в @SpamBot.",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_functions_menu()
                )
                
        except Exception as e:
            logger.error(f"Ошибка при работе с @SpamBot: {e}")
            await message.edit_text(
                f"🛡️ *Результат проверки спам блока:*\n\n"
                f"• Аккаунт: *{acc_info}*\n"
                f"• Статус: ❓ *НЕИЗВЕСТНО*\n"
                f"• Ошибка: Не удалось связаться с @SpamBot\n"
                f"• Детали: {str(e)[:200]}\n\n"
                f"Попробуйте проверить вручную через @SpamBot.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_functions_menu()
            )
        
        await client.stop()
        
    except Exception as e:
        logger.error(f"Ошибка проверки спам блока: {e}")
        await message.edit_text(
            f"❌ *Ошибка проверки спам блока!*\n\n"
            f"Ошибка: {str(e)[:200]}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        try:
            await client.stop()
        except:
            pass

# ====== АВТООТВЕТЧИК ======
@dp.callback_query(F.data == "auto_reply_menu")
async def show_auto_reply_menu(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🤖 *Автоответчик*\n\n"
        "Автоматически отвечает на сообщения по ключевым словам.\n\n"
        "Выберите действие:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_auto_reply_menu(callback.from_user.id)
    )
    await callback.answer()

# ====== ШАБЛОНЫ СООБЩЕНИЙ ======
@dp.callback_query(F.data == "templates_menu")
async def show_templates_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    templates = db.get_templates(user_id)
    
    if not templates:
        await callback.message.edit_text(
            "📝 *У вас пока нет шаблонов сообщений.*\n\n"
            "Создайте шаблоны для быстрой отправки сообщений.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_templates_menu(user_id)
        )
    else:
        text = "📝 *Ваши шаблоны сообщений:*\n\n"
        for template in templates[:10]:
            template_id, name, text_content = template
            text += f"• *{name}*\n"
            text += f"  {text_content[:50]}...\n\n"
        
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_templates_menu(user_id)
        )
    await callback.answer()

# ====== ЛОВЕЦ ЧЕКОВ ======
@dp.callback_query(F.data == "check_catcher_menu")
async def show_check_catcher_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    accounts = db.get_user_accounts(user_id)
    
    if not accounts:
        await callback.message.edit_text(
            "❌ *Сначала добавьте аккаунты!*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        await callback.answer()
        return
    
    menu = get_check_catcher_menu(user_id)
    if menu:
        await callback.message.edit_text(
            "💰 *Ловец чеков CryptoBot*\n\n"
            "✅ Вкл - мониторинг включен\n"
            "❌ Выкл - мониторинг выключен\n\n"
            "Автоматически ищет и активирует чеки в чатах.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=menu
        )
    await callback.answer()

# ====== ОТМЕНА ДЕЙСТВИЙ ======
@dp.callback_query(F.data == "cancel_action")
async def cancel_action(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    await state.clear()
    
    if user_id in user_clients:
        try:
            await user_clients[user_id].disconnect()
        except:
            pass
        del user_clients[user_id]
    
    if user_id in mass_send_data:
        del mass_send_data[user_id]
    if user_id in selected_accounts_for_mass:
        del selected_accounts_for_mass[user_id]
    if user_id in temp_bookmark_data:
        del temp_bookmark_data[user_id]
    
    await callback.message.edit_text(
        "❌ Действие отменено.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_functions_menu()
    )
    await callback.answer()

@dp.callback_query(F.data == "cancel_mass_send")
async def cancel_mass_send(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id in mass_send_data:
        del mass_send_data[user_id]
    if user_id in selected_accounts_for_mass:
        del selected_accounts_for_mass[user_id]
    
    await callback.message.edit_text(
        "❌ *Рассылка отменена*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_functions_menu()
    )
    await callback.answer()

# ====== ЗАПУСК БОТА ======
async def main():
    logger.info("Запуск бота Monkey Gram...")
    
    os.makedirs("sessions", exist_ok=True)
    
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
