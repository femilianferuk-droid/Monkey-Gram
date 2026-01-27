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
from pyrogram.types import Message, Dialog  # Убрали Folder
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command, StateFilter
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import ClientSession
import random

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
    def add_bookmark(self, user_id: int, account_id: int, chat_data: dict):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO bookmarks 
                (user_id, account_id, chat_id, title, username)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                user_id,
                account_id,
                chat_data['id'],
                chat_data.get('title'),
                chat_data.get('username')
            ))
            self.conn.commit()
            return True
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
    waiting_for_bookmark_name = State()

# ====== ХРАНИЛИЩА ======
active_tasks: Dict[int, List[asyncio.Task]] = {}
check_catchers: Dict[int, Dict[int, bool]] = {}
mass_send_data: Dict[int, Dict] = {}
selected_accounts_for_mass: Dict[int, List[int]] = {}
user_clients: Dict[int, Any] = {}
user_folders_cache: Dict[int, Dict[int, List[Dict]]] = {}  # user_id: {account_id: folders}
auto_reply_tasks: Dict[int, Dict[int, asyncio.Task]] = {}  # user_id: {account_id: task}

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
        [InlineKeyboardButton(text="🔄 Аккаунт-спамер", callback_data="spammer_menu")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

def get_accounts_menu(user_id: int, mode: str = "view"):
    """mode: view, delete, mass_send, auto_reply, spammer"""
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
        elif mode == "spammer":
            keyboard.append([
                InlineKeyboardButton(
                    text=f"🔄 {display_name}",
                    callback_data=f"spammer_account_{account_id}"
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

def get_folders_menu(user_id: int, account_id: int):
    """Меню папок аккаунта - упрощенная версия"""
    folders = user_folders_cache.get(user_id, {}).get(account_id, [])
    
    if not folders:
        # Создаем стандартные папки если нет пользовательских
        folders = [
            {'id': 0, 'title': 'Все чаты', 'type': 'all'},
            {'id': 1, 'title': 'Личные сообщения', 'type': 'private'},
            {'id': 2, 'title': 'Группы', 'type': 'groups'},
            {'id': 3, 'title': 'Каналы', 'type': 'channels'}
        ]
        if user_id not in user_folders_cache:
            user_folders_cache[user_id] = {}
        user_folders_cache[user_id][account_id] = folders
    
    keyboard = []
    for folder in folders:
        folder_id = folder.get('id', 0)
        title = folder.get('title', f'Папка {folder_id}')
        
        keyboard.append([
            InlineKeyboardButton(
                text=f"📁 {title}",
                callback_data=f"select_folder_{account_id}_{folder_id}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="mass_send")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_bookmarks_menu(user_id: int):
    bookmarks = db.get_user_bookmarks(user_id)
    if not bookmarks:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить закладку", callback_data="add_bookmark_menu")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")]
        ])
    
    keyboard = []
    for bm in bookmarks[:20]:  # Ограничим 20 закладками
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

def get_spammer_menu(user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎯 Спам в ЛС", callback_data="spam_direct")],
        [InlineKeyboardButton(text="👥 Спам в группу", callback_data="spam_group")],
        [InlineKeyboardButton(text="🔄 Рандомный спам", callback_data="spam_random")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")]
    ])

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

def get_yes_no_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да", callback_data="confirm_yes"),
            InlineKeyboardButton(text="❌ Нет", callback_data="confirm_no")
        ]
    ])

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
• 🔄 Аккаунт-спамер

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
        "📨 *Настройка рассылки - Шаг 1/5*\n\n"
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
            f"✅ *Шаг 1/5 завершен*\n"
            f"Количество сообщений: *{count}*\n\n"
            f"*Шаг 2/5:* Укажите задержку между сообщениями (в секундах):\n"
            f"Пример: 5 (минимум 1, максимум 3600)",
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
        if delay < 1 or delay > 3600:
            raise ValueError
        
        mass_send_data[user_id]['delay'] = delay
        
        await message.answer(
            f"✅ *Шаг 2/5 завершен*\n"
            f"Задержка: *{delay}* секунд\n\n"
            f"*Шаг 3/5:* Пришлите текст сообщения для рассылки:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_back_button()
        )
        await state.set_state(Form.waiting_for_message_text)
        
    except ValueError:
        await message.answer(
            "❌ Неверная задержка. Введите число от 1 до 3600 секунд:",
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
        f"✅ *Шаг 3/5 завершен*\n"
        f"Текст сообщения сохранен\n\n"
        f"*Шаг 4/5:* Выберите аккаунт для рассылки:\n\n"
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
    
    # Показываем меню выбора папки
    menu = get_folders_menu(user_id, account_id)
    if menu:
        await callback.message.edit_text(
            f"✅ *Аккаунт выбран!*\n\n"
            f"*Шаг 5/5:* Выберите категорию для рассылки:\n\n"
            f"Сообщения будут отправлены во все чаты в выбранной категории.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=menu
        )
    else:
        await callback.message.edit_text(
            "❌ *Не удалось загрузить категории.*\n\n",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
    
    await callback.answer()

@dp.callback_query(F.data.startswith("select_folder_"))
async def select_folder_for_mass(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    parts = callback.data.split("_")
    account_id = int(parts[2])
    folder_id = int(parts[3])
    
    # Сохраняем выбранную папку
    mass_send_data[user_id]['folder_id'] = folder_id
    
    # Получаем информацию о папке
    folders = user_folders_cache.get(user_id, {}).get(account_id, [])
    folder_info = None
    for folder in folders:
        if folder.get('id') == folder_id:
            folder_info = folder
            break
    
    if not folder_info:
        await callback.answer("❌ Папка не найдена")
        return
    
    # Показываем сводку
    data = mass_send_data[user_id]
    
    summary = (
        f"📋 *Сводка рассылки:*\n\n"
        f"• Аккаунт: *{account_id}*\n"
        f"• Категория: *{folder_info.get('title', 'Неизвестно')}*\n"
        f"• Сообщений в каждый чат: *{data['count']}*\n"
        f"• Задержка: *{data['delay']}* сек\n"
        f"• Текст: *{data['text'][:50]}...*\n\n"
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
    """Рассылка по категории"""
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
        
        # Получаем чаты в зависимости от категории
        chats_in_folder = []
        folder_type = None
        
        # Определяем тип категории
        folders = user_folders_cache.get(user_id, {}).get(account_id, [])
        for folder in folders:
            if folder.get('id') == folder_id:
                folder_type = folder.get('type', 'all')
                break
        
        if not folder_type:
            folder_type = 'all'
        
        # Собираем чаты по категории
        async for dialog in client.get_dialogs(limit=200):  # Ограничим для производительности
            chat = dialog.chat
            
            if not chat:
                continue
            
            # Пропускаем самого себя
            if hasattr(chat, 'id') and chat.id == (await client.get_me()).id:
                continue
            
            # Фильтруем по типу категории
            if folder_type == 'all':
                chats_in_folder.append(chat.id)
            elif folder_type == 'private':
                if chat.type == "private":
                    chats_in_folder.append(chat.id)
            elif folder_type == 'groups':
                if chat.type in ["group", "supergroup"]:
                    chats_in_folder.append(chat.id)
            elif folder_type == 'channels':
                if chat.type == "channel":
                    chats_in_folder.append(chat.id)
        
        if not chats_in_folder:
            await message.edit_text(
                "❌ *Нет чатов для рассылки в выбранной категории.*",
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
            f"Прогресс: 0/{total_messages} (0%)\n"
            f"Статус: Инициализация...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        sent_count = 0
        failed_count = 0
        
        for chat_index, chat_id in enumerate(chats_in_folder):
            for message_num in range(data['count']):
                try:
                    await client.send_message(
                        chat_id=chat_id,
                        text=data['text']
                    )
                    
                    sent_count += 1
                    
                    # Обновляем прогресс каждые 10 сообщений или каждые 10%
                    if sent_count % 10 == 0 or sent_count % max(1, total_messages // 10) == 0:
                        percent = (sent_count / total_messages) * 100
                        await progress_msg.edit_text(
                            f"🚀 *Рассылка в процессе...*\n\n"
                            f"Прогресс: {sent_count}/{total_messages} ({percent:.1f}%)\n"
                            f"Чат: {chat_index+1}/{len(chats_in_folder)}\n"
                            f"Сообщение: {message_num+1}/{data['count']}\n"
                            f"Ошибок: {failed_count}",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    
                    if message_num < data['count'] - 1 or chat_index < len(chats_in_folder) - 1:
                        await asyncio.sleep(data['delay'])
                        
                except (PeerIdInvalid, ChannelInvalid, ChatAdminRequired, FloodWait) as e:
                    failed_count += 1
                    logger.error(f"Ошибка отправки в чат {chat_id}: {e}")
                    break
                except Exception as e:
                    failed_count += 1
                    logger.error(f"Ошибка отправки: {e}")
                    continue
        
        success_rate = (sent_count / total_messages * 100) if total_messages > 0 else 0
        
        await progress_msg.edit_text(
            f"✅ *Рассылка завершена!*\n\n"
            f"• Успешно отправлено: *{sent_count}* сообщений\n"
            f"• Не отправлено: *{failed_count}* сообщений\n"
            f"• Успешность: *{success_rate:.1f}%*\n"
            f"• Чатов в категории: *{len(chats_in_folder)}*\n"
            f"• Время выполнения: *{(sent_count + failed_count) * data['delay']:.1f}* сек\n\n"
            f"Рассылка по категории завершена.",
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
        for bm in bookmarks[:10]:  # Покажем первые 10
            bookmark_id, account_id, chat_id, title, username, phone, acc_name = bm
            display_name = title or username or f"Chat {chat_id}"
            text += f"• *{display_name}*\n"
            text += f"  Аккаунт: {acc_name or phone}\n"
            text += f"  ID: `{bookmark_id}`\n\n"
        
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_bookmarks_menu(user_id)
        )
    await callback.answer()

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

@dp.callback_query(F.data == "add_auto_reply")
async def add_auto_reply_prompt(callback: types.CallbackQuery):
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
        "📱 *Выберите аккаунт для автоответчика:*\n\n"
        "В этом аккаунте будут работать автоответы.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_accounts_menu(user_id, "auto_reply")
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("auto_reply_account_"))
async def select_auto_reply_account(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    account_id = int(callback.data.split("_")[3])
    
    await state.update_data(auto_reply_account=account_id)
    
    await callback.message.edit_text(
        "🤖 *Добавление автоответа*\n\n"
        "Введите ключевое слово или фразу, на которую нужно отвечать:\n\n"
        "Пример: привет, здравствуйте, как дела",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_button()
    )
    await state.set_state(Form.waiting_for_auto_reply_trigger)
    await callback.answer()

@dp.message(Form.waiting_for_auto_reply_trigger)
async def process_auto_reply_trigger(message: types.Message, state: FSMContext):
    trigger = message.text.strip()
    
    if not trigger or len(trigger) > 100:
        await message.answer(
            "❌ Неверный триггер. Максимум 100 символов.\n"
            "Введите ключевое слово:",
            reply_markup=get_back_button()
        )
        return
    
    await state.update_data(auto_reply_trigger=trigger)
    
    await message.answer(
        f"✅ *Триггер сохранен:* `{trigger}`\n\n"
        f"Теперь введите текст ответа:\n\n"
        f"Можно использовать разметку Markdown.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_button()
    )
    await state.set_state(Form.waiting_for_auto_reply_text)

@dp.message(Form.waiting_for_auto_reply_text)
async def process_auto_reply_text(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    reply_text = message.text.strip()
    
    if not reply_text or len(reply_text) > 2000:
        await message.answer(
            "❌ Текст слишком длинный. Максимум 2000 символов.\n"
            "Введите текст ответа:",
            reply_markup=get_back_button()
        )
        return
    
    data = await state.get_data()
    account_id = data.get('auto_reply_account')
    trigger = data.get('auto_reply_trigger')
    
    if not account_id or not trigger:
        await message.answer(
            "❌ Ошибка данных. Начните заново.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        await state.clear()
        return
    
    reply_id = db.add_auto_reply(user_id, account_id, trigger, reply_text)
    
    if reply_id:
        await message.answer(
            f"✅ *Автоответ добавлен!*\n\n"
            f"*Триггер:* `{trigger}`\n"
            f"*Ответ:* {reply_text[:100]}...\n\n"
            f"Автоответчик активирован для этого аккаунта.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🤖 Мои автоответы", callback_data="view_auto_replies")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="auto_reply_menu")]
            ])
        )
        
        # Запускаем автоответчик если он еще не запущен
        if user_id not in auto_reply_tasks:
            auto_reply_tasks[user_id] = {}
        
        if account_id not in auto_reply_tasks[user_id]:
            task = asyncio.create_task(run_auto_reply(user_id, account_id))
            auto_reply_tasks[user_id][account_id] = task
    else:
        await message.answer(
            "❌ Ошибка при добавлении автоответа.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
    
    await state.clear()

async def run_auto_reply(user_id: int, account_id: int):
    """Запуск автоответчика для аккаунта"""
    session_string = db.get_account_session(account_id, user_id)
    if not session_string:
        return
    
    client = Client(
        name=f"auto_reply_{user_id}_{account_id}",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_string,
        workdir="sessions"
    )
    
    @client.on_message(filters.private & filters.incoming)
    async def handle_private_message(client: Client, message: Message):
        try:
            # Получаем автоответы для этого аккаунта
            replies = db.get_auto_replies(user_id, account_id)
            
            text = message.text or message.caption or ""
            text_lower = text.lower()
            
            for reply in replies:
                reply_id, trigger, reply_text, is_active = reply
                if is_active and trigger.lower() in text_lower:
                    await message.reply(reply_text)
                    logger.info(f"Автоответ отправлен в чат {message.chat.id}")
                    break
                    
        except Exception as e:
            logger.error(f"Ошибка автоответчика: {e}")
    
    try:
        await client.start()
        logger.info(f"Автоответчик запущен для account_id={account_id}")
        
        # Бесконечный цикл
        while True:
            await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"Ошибка в автоответчике: {e}")
    finally:
        try:
            await client.stop()
        except:
            pass

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

@dp.callback_query(F.data == "add_template")
async def add_template_prompt(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📝 *Создание шаблона сообщения*\n\n"
        "Введите название для шаблона:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_button()
    )
    await state.set_state(Form.waiting_for_template_name)
    await callback.answer()

@dp.message(Form.waiting_for_template_name)
async def process_template_name(message: types.Message, state: FSMContext):
    name = message.text.strip()
    
    if not name or len(name) > 50:
        await message.answer(
            "❌ Неверное название. Максимум 50 символов.\n"
            "Введите название шаблона:",
            reply_markup=get_back_button()
        )
        return
    
    await state.update_data(template_name=name)
    
    await message.answer(
        f"✅ *Название сохранено:* `{name}`\n\n"
        f"Теперь введите текст шаблона:\n\n"
        f"Можно использовать разметку Markdown.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_button()
    )
    await state.set_state(Form.waiting_for_template_text)

@dp.message(Form.waiting_for_template_text)
async def process_template_text(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    text = message.text.strip()
    
    if not text or len(text) > 4000:
        await message.answer(
            "❌ Текст слишком длинный. Максимум 4000 символов.\n"
            "Введите текст шаблона:",
            reply_markup=get_back_button()
        )
        return
    
    data = await state.get_data()
    name = data.get('template_name')
    
    if not name:
        await message.answer(
            "❌ Ошибка данных. Начните заново.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        await state.clear()
        return
    
    template_id = db.add_template(user_id, name, text)
    
    if template_id:
        await message.answer(
            f"✅ *Шаблон создан!*\n\n"
            f"*Название:* {name}\n"
            f"*Текст:* {text[:100]}...\n\n"
            f"Теперь вы можете использовать этот шаблон в рассылке.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📝 Мои шаблоны", callback_data="templates_menu")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")]
            ])
        )
    else:
        await message.answer(
            "❌ Ошибка при создании шаблона.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
    
    await state.clear()

# ====== АККАУНТ-СПАМЕР ======
@dp.callback_query(F.data == "spammer_menu")
async def show_spammer_menu(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🔄 *Аккаунт-спамер*\n\n"
        "Функции для массовой рассылки сообщений:\n\n"
        "• 🎯 Спам в ЛС - рассылка личным сообщениям\n"
        "• 👥 Спам в группу - массовая отправка в группы\n"
        "• 🔄 Рандомный спам - случайная рассылка\n\n"
        "Выберите действие:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_spammer_menu(callback.from_user.id)
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

@dp.callback_query(F.data.startswith("toggle_catcher_"))
async def toggle_check_catcher(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    account_id = int(callback.data.split("_")[2])
    
    if user_id not in check_catchers:
        check_catchers[user_id] = {}
    
    current_state = check_catchers[user_id].get(account_id, False)
    new_state = not current_state
    check_catchers[user_id][account_id] = new_state
    
    menu = get_check_catcher_menu(user_id)
    if menu:
        await callback.message.edit_reply_markup(reply_markup=menu)
    
    if new_state:
        # Запускаем ловец чеков
        asyncio.create_task(run_check_catcher(user_id, account_id))
        await callback.answer(f"Ловец чеков включен для аккаунта {account_id}!")
    else:
        await callback.answer(f"Ловец чеков выключен для аккаунта {account_id}!")

async def run_check_catcher(user_id: int, account_id: int):
    """Ловец чеков CryptoBot"""
    session_string = db.get_account_session(account_id, user_id)
    if not session_string:
        return
    
    client = Client(
        name=f"check_catcher_{user_id}_{account_id}",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_string,
        workdir="sessions"
    )
    
    @client.on_message(filters.all)
    async def handle_message(client: Client, message: Message):
        try:
            # Проверяем активен ли еще мониторинг
            if not check_catchers.get(user_id, {}).get(account_id, False):
                return
            
            text = message.text or message.caption or ""
            
            # Ищем ссылки на чеки CryptoBot
            check_patterns = [
                r't\.me/[Cc]rypto[Bb]ot\?start=[A-Za-z0-9]+',
                r't\.me/[Ss]end\?start=[A-Za-z0-9]+',
                r'crypto\.bot/\w+',
                r'чек.*crypto',
                r'check.*crypto',
                r'cryptobot.*чек',
                r'cryptobot.*check'
            ]
            
            for pattern in check_patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    # Отправляем уведомление
                    await bot.send_message(
                        user_id,
                        f"💰 *Найден чек CryptoBot!*\n\n"
                        f"Аккаунт: `{account_id}`\n"
                        f"Чат: `{message.chat.id}`\n"
                        f"Сообщение: {text[:100]}...",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    
                    # Пытаемся активировать чек
                    try:
                        # Ищем параметр start
                        match = re.search(r'start=([A-Za-z0-9]+)', text)
                        if match:
                            start_param = match.group(1)
                            await client.send_message(
                                "cryptobot",
                                f"/start {start_param}"
                            )
                            logger.info(f"Чек активирован: {start_param}")
                    except Exception as e:
                        logger.error(f"Ошибка активации чека: {e}")
                    
                    break
                    
        except Exception as e:
            logger.error(f"Ошибка ловца чеков: {e}")
    
    try:
        await client.start()
        logger.info(f"Ловец чеков запущен для account_id={account_id}")
        
        # Мониторим пока включен
        while check_catchers.get(user_id, {}).get(account_id, False):
            await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"Ошибка ловца чеков: {e}")
    finally:
        try:
            await client.stop()
        except:
            pass

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
