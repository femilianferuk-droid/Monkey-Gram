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
from deep_translator import GoogleTranslator

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
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mass_send_folders (
                folder_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                account_id INTEGER,
                folder_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                FOREIGN KEY (account_id) REFERENCES accounts (account_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mass_send_folder_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                folder_id INTEGER,
                chat_id INTEGER,
                chat_title TEXT,
                chat_username TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (folder_id) REFERENCES mass_send_folders (folder_id)
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
    
    # ====== ПАПКИ ДЛЯ РАССЫЛКИ ======
    def create_mass_send_folder(self, user_id: int, account_id: int, folder_name: str):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO mass_send_folders (user_id, account_id, folder_name)
                VALUES (?, ?, ?)
            ''', (user_id, account_id, folder_name))
            self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка создания папки: {e}")
            return None
    
    def add_chat_to_folder(self, folder_id: int, chat_id: int, chat_title: str, chat_username: str = None):
        cursor = self.conn.cursor()
        try:
            # Проверяем количество чатов в папке (максимум 20)
            cursor.execute('SELECT COUNT(*) FROM mass_send_folder_chats WHERE folder_id = ?', (folder_id,))
            count = cursor.fetchone()[0]
            
            if count >= 20:
                return False, "В папке уже максимальное количество чатов (20)"
            
            # Проверяем, есть ли уже этот чат в папке
            cursor.execute('''
                SELECT 1 FROM mass_send_folder_chats 
                WHERE folder_id = ? AND chat_id = ?
            ''', (folder_id, chat_id))
            
            if cursor.fetchone():
                return False, "Этот чат уже есть в папке"
            
            # Добавляем чат
            cursor.execute('''
                INSERT INTO mass_send_folder_chats (folder_id, chat_id, chat_title, chat_username)
                VALUES (?, ?, ?, ?)
            ''', (folder_id, chat_id, chat_title, chat_username))
            self.conn.commit()
            return True, "Чат добавлен в папку"
        except Exception as e:
            logger.error(f"Ошибка добавления чата в папку: {e}")
            return False, f"Ошибка: {str(e)}"
    
    def get_user_folders(self, user_id: int, account_id: int = None):
        cursor = self.conn.cursor()
        if account_id:
            cursor.execute('''
                SELECT folder_id, folder_name, 
                       (SELECT COUNT(*) FROM mass_send_folder_chats WHERE folder_id = mass_send_folders.folder_id) as chat_count
                FROM mass_send_folders 
                WHERE user_id = ? AND account_id = ?
                ORDER BY created_at DESC
            ''', (user_id, account_id))
        else:
            cursor.execute('''
                SELECT folder_id, folder_name, account_id,
                       (SELECT COUNT(*) FROM mass_send_folder_chats WHERE folder_id = mass_send_folders.folder_id) as chat_count
                FROM mass_send_folders 
                WHERE user_id = ?
                ORDER BY created_at DESC
            ''', (user_id,))
        return cursor.fetchall()
    
    def get_folder_chats(self, folder_id: int, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT fc.chat_id, fc.chat_title, fc.chat_username
            FROM mass_send_folder_chats fc
            JOIN mass_send_folders f ON fc.folder_id = f.folder_id
            WHERE fc.folder_id = ? AND f.user_id = ?
            ORDER BY fc.added_at
        ''', (folder_id, user_id))
        return cursor.fetchall()
    
    def delete_folder(self, folder_id: int, user_id: int):
        cursor = self.conn.cursor()
        try:
            # Удаляем сначала чаты в папке
            cursor.execute('DELETE FROM mass_send_folder_chats WHERE folder_id IN (SELECT folder_id FROM mass_send_folders WHERE folder_id = ? AND user_id = ?)', 
                          (folder_id, user_id))
            # Удаляем папку
            cursor.execute('DELETE FROM mass_send_folders WHERE folder_id = ? AND user_id = ?', (folder_id, user_id))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка удаления папки: {e}")
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
    waiting_for_folder_name = State()
    waiting_for_folder_chat_selection = State()

# ====== ХРАНИЛИЩА ======
active_tasks: Dict[int, List[asyncio.Task]] = {}
check_catchers: Dict[int, Dict[int, bool]] = {}
mass_send_data: Dict[int, Dict] = {}
selected_accounts_for_mass: Dict[int, List[int]] = {}
user_clients: Dict[int, Any] = {}
auto_reply_tasks: Dict[int, Dict[int, asyncio.Task]] = {}
temp_bookmark_data: Dict[int, Dict] = {}
temp_folder_data: Dict[int, Dict] = {}  # Для хранения временных данных о папках

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
    """mode: view, delete, mass_send, auto_reply, bookmarks, folders"""
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
        elif mode == "folders":
            keyboard.append([
                InlineKeyboardButton(
                    text=f"📁 {display_name}",
                    callback_data=f"folder_account_{account_id}"
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
    elif mode == "folders":
        keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="mass_send")])
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

def get_folders_menu(user_id: int, account_id: int):
    """Меню папок для аккаунта"""
    folders = db.get_user_folders(user_id, account_id)
    
    keyboard = []
    if folders:
        for folder in folders[:10]:  # Ограничим 10 папками
            folder_id, folder_name, chat_count = folder
            keyboard.append([
                InlineKeyboardButton(
                    text=f"📁 {folder_name} ({chat_count} чатов)",
                    callback_data=f"select_folder_{account_id}_{folder_id}"
                )
            ])
    
    keyboard.append([
        InlineKeyboardButton(text="➕ Создать новую папку", callback_data=f"create_folder_{account_id}"),
        InlineKeyboardButton(text="⬅️ Назад", callback_data="mass_send")
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_back_button():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_action")]
    ])

# ====== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ======
async def safe_get_dialogs_simple(client: Client, limit: int = 50):
    """Простое получение диалогов без вызова GetFullUser"""
    dialogs = []
    count = 0
    
    try:
        async for dialog in client.get_dialogs():
            dialogs.append(dialog)
            count += 1
            
            # Добавляем задержку каждые 5 диалогов
            if count % 5 == 0:
                await asyncio.sleep(0.3)
            
            # Ограничиваем количество
            if count >= limit:
                break
                
    except FloodWait as e:
        logger.warning(f"FloodWait при получении диалогов: {e.value} секунд")
        await asyncio.sleep(e.value + 1)
        # Пробуем продолжить с меньшим лимитом
        if len(dialogs) < 20:
            try:
                async for dialog in client.get_dialogs():
                    dialogs.append(dialog)
                    if len(dialogs) >= 20:
                        break
            except:
                pass
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

async def translate_text(text: str, target_lang: str = 'ru') -> str:
    """Перевод текста с помощью Google Translate"""
    try:
        if not text or text.strip() == "":
            return text
        
        # Проверяем, не русский ли уже текст
        russian_chars = sum(1 for c in text if 'а' <= c <= 'я' or 'А' <= c <= 'Я' or c in 'ёЁ')
        if russian_chars / max(len(text), 1) > 0.3:
            return text  # Текст уже на русском
        
        translator = GoogleTranslator(source='auto', target=target_lang)
        translated = translator.translate(text)
        return translated
    except Exception as e:
        logger.error(f"Ошибка перевода: {e}")
        return text  # Возвращаем оригинальный текст при ошибке

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

# ====== РАССЫЛКА ПО ПАПКАМ ======
@dp.callback_query(F.data == "mass_send")
async def start_mass_send(callback: types.CallbackQuery):
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
    
    # Сбрасываем данные
    if user_id in mass_send_data:
        del mass_send_data[user_id]
    
    mass_send_data[user_id] = {
        'count': None,
        'delay': None,
        'text': None,
        'account_id': None,
        'folder_id': None
    }
    
    await callback.message.edit_text(
        "📨 *Рассылка по папкам*\n\n"
        "Вы можете:\n"
        "1. 📁 Использовать существующие папки\n"
        "2. ➕ Создать новую папку с чатами\n\n"
        "Выберите действие:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📁 Выбрать аккаунт для папок", callback_data="select_account_for_folders")],
            [InlineKeyboardButton(text="📨 Настроить рассылку", callback_data="setup_mass_send")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data == "select_account_for_folders")
async def select_account_for_folders(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    await callback.message.edit_text(
        "📱 *Выберите аккаунт для работы с папками:*\n\n"
        "Папки привязаны к конкретному аккаунту.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_accounts_menu(user_id, "folders")
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("folder_account_"))
async def handle_folder_account(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    account_id = int(callback.data.split("_")[2])
    
    # Сохраняем выбранный аккаунт
    temp_folder_data[user_id] = {'account_id': account_id}
    
    # Показываем меню папок
    await callback.message.edit_text(
        f"📁 *Папки аккаунта {account_id}*\n\n"
        f"Выберите папку или создайте новую:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_folders_menu(user_id, account_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("create_folder_"))
async def create_folder_prompt(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    account_id = int(callback.data.split("_")[2])
    
    # Сохраняем account_id
    await state.update_data(folder_account_id=account_id)
    
    await callback.message.edit_text(
        "📁 *Создание новой папки*\n\n"
        "Введите название для новой папки:\n\n"
        "Пример: Мои клиенты, Рассылка 1, Важные контакты",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_button()
    )
    await state.set_state(Form.waiting_for_folder_name)
    await callback.answer()

@dp.message(Form.waiting_for_folder_name)
async def process_folder_name(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    folder_name = message.text.strip()
    
    if not folder_name or len(folder_name) > 50:
        await message.answer(
            "❌ Неверное название. Название должно быть от 1 до 50 символов.\n"
            "Введите название папки:",
            reply_markup=get_back_button()
        )
        return
    
    data = await state.get_data()
    account_id = data.get('folder_account_id')
    
    if not account_id:
        await message.answer(
            "❌ Ошибка данных. Начните заново.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        await state.clear()
        return
    
    # Создаем папку в базе данных
    folder_id = db.create_mass_send_folder(user_id, account_id, folder_name)
    
    if folder_id:
        # Сохраняем данные папки
        temp_folder_data[user_id] = {
            'account_id': account_id,
            'folder_id': folder_id,
            'folder_name': folder_name
        }
        
        await message.answer(
            f"✅ *Папка создана!*\n\n"
            f"• Название: *{folder_name}*\n"
            f"• ID папки: `{folder_id}`\n"
            f"• Аккаунт: `{account_id}`\n\n"
            f"Теперь добавьте чаты в эту папку (максимум 20).",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить чаты в папку", callback_data=f"add_chats_to_folder_{folder_id}")],
                [InlineKeyboardButton(text="⬅️ Назад к папкам", callback_data=f"folder_account_{account_id}")]
            ])
        )
    else:
        await message.answer(
            "❌ *Ошибка при создании папки.*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
    
    await state.clear()

@dp.callback_query(F.data.startswith("add_chats_to_folder_"))
async def add_chats_to_folder(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[4])
    
    # Получаем информацию о папке
    folders = db.get_user_folders(user_id)
    folder_info = None
    account_id = None
    
    for folder in folders:
        if folder[0] == folder_id:  # folder_id на первой позиции
            folder_id, folder_name, acc_id = folder[:3]
            folder_info = folder_name
            account_id = acc_id
            break
    
    if not folder_info or not account_id:
        await callback.answer("❌ Папка не найдена")
        return
    
    # Сохраняем данные
    temp_folder_data[user_id] = {
        'account_id': account_id,
        'folder_id': folder_id,
        'folder_name': folder_info
    }
    
    session_string = db.get_account_session(account_id, user_id)
    if not session_string:
        await callback.answer("❌ Сессия не найдена")
        return
    
    await callback.message.edit_text(
        "⏳ *Загружаю список чатов...*\n\n"
        "Используется безопасный режим для избежания FloodWait.",
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Загружаем чаты в фоне
    asyncio.create_task(load_chats_for_folder(user_id, account_id, folder_id, session_string, callback.message))
    await callback.answer()

async def load_chats_for_folder(user_id: int, account_id: int, folder_id: int, session_string: str, message: types.Message):
    """Загрузка чатов для добавления в папку"""
    client = Client(
        name=f"folder_chat_loader_{user_id}_{account_id}",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_string,
        workdir="sessions"
    )
    
    try:
        await client.start()
        
        # Получаем диалоги безопасным способом
        dialogs = await safe_get_dialogs_simple(client, limit=40)
        
        chats_list = []
        my_id = (await client.get_me()).id
        
        for dialog in dialogs:
            chat = dialog.chat
            
            if not chat:
                continue
            
            # Пропускаем самого себя
            if chat.id == my_id:
                continue
            
            # Получаем базовую информацию о чате
            chat_type = getattr(chat, 'type', 'unknown')
            
            # Получаем название
            chat_title = None
            if hasattr(chat, 'title'):
                chat_title = chat.title
            elif hasattr(chat, 'first_name'):
                chat_title = f"{chat.first_name} {chat.last_name or ''}".strip()
            
            if not chat_title:
                chat_title = f"Chat {chat.id}"
            
            # Получаем username если есть
            chat_username = getattr(chat, 'username', None)
            
            chats_list.append({
                'id': chat.id,
                'type': chat_type,
                'title': chat_title,
                'username': chat_username
            })
        
        if not chats_list:
            await message.edit_text(
                "❌ *Не найдено чатов для добавления в папку.*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_functions_menu()
            )
            return
        
        # Сохраняем список чатов во временное хранилище
        temp_folder_data[user_id]['chats'] = chats_list
        
        # Получаем уже добавленные чаты в папку
        existing_chats = db.get_folder_chats(folder_id, user_id)
        existing_chat_ids = {chat[0] for chat in existing_chats}
        
        # Создаем клавиатуру с чатами
        keyboard = []
        added_count = 0
        
        for i, chat in enumerate(chats_list[:25]):  # Ограничим 25 чатами
            display_name = chat['title'][:25] if chat['title'] else f"Chat {chat['id']}"
            username_part = f" (@{chat['username']})" if chat['username'] else ""
            
            # Проверяем, есть ли уже этот чат в папке
            if chat['id'] in existing_chat_ids:
                status = "✅"
                callback_data = f"chat_already_in_folder_{i}"
            else:
                status = "➕"
                callback_data = f"select_chat_for_folder_{i}"
                added_count += 1
            
            keyboard.append([
                InlineKeyboardButton(
                    text=f"{status} {display_name}{username_part}",
                    callback_data=callback_data
                )
            ])
        
        # Информация о папке
        folder_info = temp_folder_data[user_id].get('folder_name', 'Папка')
        chat_count = len(existing_chats)
        
        keyboard.append([
            InlineKeyboardButton(text="📋 Просмотр чатов в папке", callback_data=f"view_folder_chats_{folder_id}"),
            InlineKeyboardButton(text="✅ Завершить", callback_data=f"folder_account_{account_id}")
        ])
        
        await message.edit_text(
            f"📁 *Добавление чатов в папку:* {folder_info}\n\n"
            f"• Чатов в папке: {chat_count}/20\n"
            f"• Доступно для добавления: {added_count}\n\n"
            f"Выберите чаты для добавления:\n"
            f"✅ - уже в папке\n"
            f"➕ - можно добавить",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
        )
        
    except Exception as e:
        logger.error(f"Ошибка загрузки чатов для папки: {e}")
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

@dp.callback_query(F.data.startswith("select_chat_for_folder_"))
async def select_chat_for_folder(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    chat_index = int(callback.data.split("_")[4])
    
    if user_id not in temp_folder_data:
        await callback.answer("❌ Данные устарели")
        return
    
    chats = temp_folder_data[user_id].get('chats', [])
    folder_id = temp_folder_data[user_id].get('folder_id')
    folder_name = temp_folder_data[user_id].get('folder_name', 'Папка')
    
    if chat_index >= len(chats):
        await callback.answer("❌ Чат не найден")
        return
    
    chat = chats[chat_index]
    
    # Добавляем чат в папку
    success, message_text = db.add_chat_to_folder(
        folder_id=folder_id,
        chat_id=chat['id'],
        chat_title=chat['title'],
        chat_username=chat['username']
    )
    
    if success:
        await callback.answer(f"✅ Чат '{chat['title'][:20]}' добавлен в папку")
        
        # Обновляем сообщение
        await callback.message.edit_text(
            f"✅ *Чат добавлен в папку!*\n\n"
            f"• Чат: *{chat['title']}*\n"
            f"• Папка: {folder_name}\n\n"
            f"{message_text}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📋 Продолжить добавление", callback_data=f"add_chats_to_folder_{folder_id}")],
                [InlineKeyboardButton(text="⬅️ Назад к папкам", callback_data=f"folder_account_{temp_folder_data[user_id].get('account_id')}")]
            ])
        )
    else:
        await callback.answer(f"❌ {message_text}")
        
        # Обновляем сообщение с ошибкой
        await callback.message.edit_text(
            f"❌ *Не удалось добавить чат*\n\n"
            f"• Чат: *{chat['title']}*\n"
            f"• Ошибка: {message_text}\n\n"
            f"Попробуйте выбрать другой чат.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📋 Продолжить добавление", callback_data=f"add_chats_to_folder_{folder_id}")],
                [InlineKeyboardButton(text="⬅️ Назад к папкам", callback_data=f"folder_account_{temp_folder_data[user_id].get('account_id')}")]
            ])
        )

@dp.callback_query(F.data.startswith("view_folder_chats_"))
async def view_folder_chats(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[3])
    
    # Получаем чаты из папки
    folder_chats = db.get_folder_chats(folder_id, user_id)
    
    if not folder_chats:
        await callback.answer("❌ В папке нет чатов")
        return
    
    text = f"📁 *Чаты в папке:*\n\n"
    for i, (chat_id, chat_title, chat_username) in enumerate(folder_chats, 1):
        username_part = f" (@{chat_username})" if chat_username else ""
        text += f"{i}. *{chat_title[:30]}*{username_part}\n"
        text += f"   ID: `{chat_id}`\n\n"
    
    text += f"Всего чатов: {len(folder_chats)}/20\n"
    
    # Получаем информацию о папке
    folders = db.get_user_folders(user_id)
    folder_name = "Неизвестно"
    account_id = None
    
    for folder in folders:
        if folder[0] == folder_id:
            folder_name = folder[1]
            if len(folder) > 2:
                account_id = folder[2]
            break
    
    keyboard = []
    if account_id:
        keyboard.append([InlineKeyboardButton(text="➕ Добавить ещё чатов", callback_data=f"add_chats_to_folder_{folder_id}")])
        keyboard.append([InlineKeyboardButton(text="🗑️ Удалить папку", callback_data=f"delete_folder_{folder_id}")])
        keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"folder_account_{account_id}")])
    
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_folder_"))
async def delete_folder_prompt(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[2])
    
    await callback.message.edit_text(
        "🗑️ *Удаление папки*\n\n"
        "Вы уверены, что хотите удалить эту папку?\n"
        "Все чаты в папке будут удалены.\n\n"
        "Это действие нельзя отменить!",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_delete_folder_{folder_id}"),
                InlineKeyboardButton(text="❌ Нет, отмена", callback_data="cancel_action")
            ]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("confirm_delete_folder_"))
async def confirm_delete_folder(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[3])
    
    if db.delete_folder(folder_id, user_id):
        await callback.message.edit_text(
            "✅ *Папка успешно удалена!*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
    else:
        await callback.message.edit_text(
            "❌ *Ошибка при удалении папки.*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
    await callback.answer()

@dp.callback_query(F.data.startswith("select_folder_"))
async def select_folder_for_mass_send(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    parts = callback.data.split("_")
    account_id = int(parts[2])
    folder_id = int(parts[3])
    
    # Получаем информацию о папке
    folders = db.get_user_folders(user_id, account_id)
    folder_info = None
    
    for folder in folders:
        if folder[0] == folder_id:
            folder_info = folder
            break
    
    if not folder_info:
        await callback.answer("❌ Папка не найдена")
        return
    
    folder_name = folder_info[1]
    chat_count = folder_info[2]
    
    # Сохраняем выбранную папку
    if user_id not in mass_send_data:
        mass_send_data[user_id] = {}
    
    mass_send_data[user_id]['account_id'] = account_id
    mass_send_data[user_id]['folder_id'] = folder_id
    
    await callback.message.edit_text(
        f"✅ *Папка выбрана!*\n\n"
        f"• Папка: *{folder_name}*\n"
        f"• Чатов в папке: *{chat_count}*\n"
        f"• Аккаунт: *{account_id}*\n\n"
        f"Теперь настройте параметры рассылки:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Настроить рассылку", callback_data="setup_mass_send")],
            [InlineKeyboardButton(text="⬅️ Выбрать другую папку", callback_data=f"folder_account_{account_id}")]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data == "setup_mass_send")
async def setup_mass_send(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    # Проверяем, выбрана ли папка
    if user_id not in mass_send_data or not mass_send_data[user_id].get('folder_id'):
        # Если папка не выбрана, предлагаем выбрать аккаунт
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
            "📱 *Сначала выберите аккаунт и папку для рассылки:*\n\n"
            "Выберите аккаунт, чтобы увидеть его папки.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_accounts_menu(user_id, "folders")
        )
        await callback.answer()
        return
    
    # Если папка выбрана, начинаем настройку рассылки
    await callback.message.edit_text(
        "📨 *Настройка рассылки - Шаг 1/4*\n\n"
        "Пришлите количество сообщений для отправки в каждый чат (1-20):",
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
        if count < 1 or count > 20:
            raise ValueError
        
        if user_id not in mass_send_data:
            mass_send_data[user_id] = {}
        
        mass_send_data[user_id]['count'] = count
        
        await message.answer(
            f"✅ *Шаг 1/4 завершен*\n"
            f"Количество сообщений: *{count}*\n\n"
            f"*Шаг 2/4:* Укажите задержку между сообщениями (в секундах):\n"
            f"Пример: 10 (минимум 5, максимум 3600)",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_back_button()
        )
        await state.set_state(Form.waiting_for_delay)
        
    except ValueError:
        await message.answer(
            "❌ Неверное количество. Введите число от 1 до 20:",
            reply_markup=get_back_button()
        )

@dp.message(Form.waiting_for_delay)
async def process_delay(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    try:
        delay = float(message.text.strip())
        if delay < 5 or delay > 3600:
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
            "❌ Неверная задержка. Введите число от 5 до 3600 секунд:",
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
    
    # Показываем сводку
    data = mass_send_data[user_id]
    account_id = data.get('account_id')
    folder_id = data.get('folder_id')
    
    if not account_id or not folder_id:
        await message.answer(
            "❌ Ошибка: не выбрана папка. Начните заново.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        await state.clear()
        return
    
    # Получаем информацию о папке
    folders = db.get_user_folders(user_id, account_id)
    folder_name = "Неизвестно"
    chat_count = 0
    
    for folder in folders:
        if folder[0] == folder_id:
            folder_name = folder[1]
            chat_count = folder[2]
            break
    
    summary = (
        f"📋 *Сводка рассылки:*\n\n"
        f"• Папка: *{folder_name}*\n"
        f"• Чатов в папке: *{chat_count}*\n"
        f"• Аккаунт: *{account_id}*\n"
        f"• Сообщений в каждый чат: *{data['count']}*\n"
        f"• Задержка: *{data['delay']}* сек\n"
        f"• Всего сообщений: *{data['count'] * chat_count}*\n"
        f"• Текст: *{data['text'][:50]}...*\n\n"
        f"⚠️ *Важно:* Для избежания FloodWait используется задержка {data['delay']} сек.\n\n"
        f"*Начать рассылку?*"
    )
    
    await message.answer(
        summary,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Начать рассылку", callback_data="start_mass_send_now"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_mass_send")
            ]
        ])
    )
    await state.clear()

@dp.callback_query(F.data == "start_mass_send_now")
async def start_mass_send_now(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    # Запускаем реальную рассылку в фоне
    asyncio.create_task(run_mass_send_to_folder_fixed(user_id, callback.message))
    await callback.answer("Рассылка запущена!")

async def run_mass_send_to_folder_fixed(user_id: int, message: types.Message):
    """Рассылка по папке с исправленной обработкой FloodWait"""
    try:
        data = mass_send_data[user_id]
        account_id = data['account_id']
        folder_id = data['folder_id']
        
        if not account_id or not folder_id:
            await message.edit_text(
                "❌ Ошибка: не все параметры указаны.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_functions_menu()
            )
            return
        
        # Получаем чаты из папки
        folder_chats = db.get_folder_chats(folder_id, user_id)
        
        if not folder_chats:
            await message.edit_text(
                "❌ *В папке нет чатов для рассылки.*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_functions_menu()
            )
            return
        
        # Ограничиваем количество чатов (максимум 20 по условию)
        max_chats = min(len(folder_chats), 20)
        chats_to_send = folder_chats[:max_chats]
        
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
        
        total_messages = data['count'] * len(chats_to_send)
        progress_msg = await message.edit_text(
            f"🚀 *Рассылка начата!*\n\n"
            f"Чатов в папке: *{len(chats_to_send)}*\n"
            f"Всего сообщений: *{total_messages}*\n"
            f"Задержка между сообщениями: *{data['delay']}* сек\n"
            f"Ожидаемое время: *{total_messages * data['delay'] / 60:.1f}* минут\n\n"
            f"Прогресс: 0/{total_messages} (0%)\n"
            f"Статус: Инициализация...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        sent_count = 0
        failed_count = 0
        
        for chat_index, (chat_id, chat_title, chat_username) in enumerate(chats_to_send):
            for message_num in range(data['count']):
                try:
                    # Добавляем дополнительную задержку перед отправкой
                    await asyncio.sleep(1)
                    
                    # Отправляем сообщение
                    await client.send_message(chat_id=chat_id, text=data['text'])
                    sent_count += 1
                    
                    # Обновляем прогресс
                    if (sent_count + failed_count) % 2 == 0 or (sent_count + failed_count) % max(1, total_messages // 10) == 0:
                        progress = sent_count + failed_count
                        percent = (progress / total_messages) * 100 if total_messages > 0 else 0
                        await progress_msg.edit_text(
                            f"🚀 *Рассылка в процессе...*\n\n"
                            f"Прогресс: {progress}/{total_messages} ({percent:.1f}%)\n"
                            f"Чат: {chat_index+1}/{len(chats_to_send)}\n"
                            f"Сообщение: {message_num+1}/{data['count']}\n"
                            f"✅ Отправлено: {sent_count}\n"
                            f"❌ Ошибок: {failed_count}\n"
                            f"Текущий чат: {chat_title[:30]}",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    
                    # Задержка между сообщениями
                    if message_num < data['count'] - 1 or chat_index < len(chats_to_send) - 1:
                        await asyncio.sleep(data['delay'])
                        
                except FloodWait as e:
                    failed_count += 1
                    wait_time = e.value
                    logger.warning(f"FloodWait: ожидание {wait_time} секунд")
                    await progress_msg.edit_text(
                        f"⏳ *FloodWait обнаружен*\n\n"
                        f"Ожидание: {wait_time} секунд\n"
                        f"Прогресс: {sent_count}/{total_messages}\n"
                        f"Чат: {chat_index+1}/{len(chats_to_send)}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    await asyncio.sleep(wait_time + 1)
                except (PeerIdInvalid, ChannelInvalid, ChatAdminRequired) as e:
                    failed_count += 1
                    logger.warning(f"Не удалось отправить в {chat_id}: {e}")
                    break  # Переходим к следующему чату
                except Exception as e:
                    failed_count += 1
                    logger.error(f"Ошибка отправки: {e}")
                    await asyncio.sleep(3)  # Пауза при ошибке
        
        success_rate = (sent_count / total_messages * 100) if total_messages > 0 else 0
        
        await progress_msg.edit_text(
            f"✅ *Рассылка завершена!*\n\n"
            f"• Успешно отправлено: *{sent_count}* сообщений\n"
            f"• Не отправлено: *{failed_count}* сообщений\n"
            f"• Успешность: *{success_rate:.1f}%*\n"
            f"• Чатов обработано: *{len(chats_to_send)}*\n"
            f"• Время выполнения: *{(sent_count + failed_count) * data['delay'] / 60:.1f}* минут\n\n"
            f"Рассылка завершена.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        
        await client.stop()
        
    except Exception as e:
        logger.error(f"Ошибка рассылки: {e}")
        await message.edit_text(
            f"❌ *Ошибка рассылки!*\n\n"
            f"Ошибка: {str(e)[:200]}\n\n"
            f"Рекомендации:\n"
            f"1. Увеличьте задержку между сообщениями\n"
            f"2. Уменьшите количество сообщений\n"
            f"3. Попробуйте позже",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
    finally:
        if user_id in mass_send_data:
            del mass_send_data[user_id]

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
    asyncio.create_task(run_spam_check_with_translation(user_id, account_id, callback.message))
    await callback.answer()

async def run_spam_check_with_translation(user_id: int, account_id: int, message: types.Message):
    """Проверка статуса через @SpamBot с переводом ответа"""
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
            f"Статус: Отправляю /start @SpamBot...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        # Отправляем /start
        try:
            sent_message = await client.send_message("spambot", "/start")
            await message.edit_text(
                f"🛡️ *Проверяю статус спам блока...*\n\n"
                f"Аккаунт: {acc_info}\n"
                f"Статус: /start отправлен. Жду ответа (5 сек)...",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Ждем 5 секунд для получения авто-ответа
            await asyncio.sleep(5)
            
            # Получаем сообщения от @SpamBot
            spam_messages = []
            try:
                async for msg in client.get_chat_history("spambot", limit=5):
                    # Фильтруем только сообщения от @SpamBot и которые пришли после нашего /start
                    if (msg.from_user and msg.from_user.username == "spambot"):
                        message_text = msg.text or msg.caption or ""
                        if message_text:
                            spam_messages.append({
                                'text': message_text,
                                'date': msg.date
                            })
            except Exception as e:
                logger.error(f"Ошибка при получении истории @SpamBot: {e}")
            
            if spam_messages:
                # Сортируем по дате (самые новые первыми)
                spam_messages.sort(key=lambda x: x['date'], reverse=True)
                
                # Берем самое последнее сообщение
                spam_response = spam_messages[0]['text']
                
                # Переводим ответ если нужно
                original_response = spam_response
                translated_response = await translate_text(spam_response)
                
                # Анализируем ответ
                status = "❓ Неизвестно"
                analysis = ""
                
                # Ищем ключевые слова в ответе (в оригинале и переводе)
                response_for_analysis = original_response.lower() + " " + translated_response.lower()
                
                if any(word in response_for_analysis for word in ["ограничен", "ограничение", "limited", "restrict", "спам", "spam", "block"]):
                    status = "🚫 *ОГРАНИЧЕН* (Spam Block)"
                    analysis = "Аккаунт имеет ограничения на отправку сообщений"
                elif any(word in response_for_analysis for word in ["предупреждение", "warning", "внимание", "attention"]):
                    status = "⚠️ *ПРЕДУПРЕЖДЕНИЕ*"
                    analysis = "Есть предупреждения, но ограничений может не быть"
                elif any(word in response_for_analysis for word in ["всё хорошо", "все хорошо", "хорошо", "good", "fine", "ok", "ок", "all good"]):
                    status = "✅ *НОРМАЛЬНО* (No Spam Block)"
                    analysis = "Аккаунт не имеет ограничений"
                elif any(word in response_for_analysis for word in ["привет", "здравствуйте", "hello", "hi"]):
                    status = "✅ *НОРМАЛЬНО* (No Spam Block)"
                    analysis = "@SpamBot ответил приветствием, ограничений нет"
                else:
                    status = "❓ *НЕИЗВЕСТНО*"
                    analysis = "Ответ не распознан"
                
                # Формируем полный ответ
                response_text = (
                    f"🛡️ *Результат проверки спам блока:*\n\n"
                    f"• Аккаунт: *{acc_info}*\n"
                    f"• Статус: {status}\n"
                    f"• Анализ: {analysis}\n"
                    f"• ID: `{account_id}`\n\n"
                )
                
                # Добавляем оригинальный ответ
                if original_response != translated_response:
                    response_text += f"*Оригинальный ответ @SpamBot:*\n```\n{original_response[:400]}\n```\n\n"
                    response_text += f"*Перевод на русский:*\n```\n{translated_response[:400]}\n```\n\n"
                else:
                    response_text += f"*Ответ @SpamBot:*\n```\n{original_response[:400]}\n```\n\n"
                
                response_text += (
                    f"*Рекомендации:*\n"
                    f"- Если статус 🚫 ОГРАНИЧЕН: не отправляйте массовые сообщения\n"
                    f"- Если статус ⚠️ ПРЕДУПРЕЖДЕНИЕ: будьте осторожны\n"
                    f"- Если статус ✅ НОРМАЛЬНО: можно работать"
                )
                
                await message.edit_text(
                    response_text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_functions_menu()
                )
            else:
                # Если @SpamBot не ответил
                await message.edit_text(
                    f"🛡️ *Результат проверки спам блока:*\n\n"
                    f"• Аккаунт: *{acc_info}*\n"
                    f"• Статус: ✅ *ВЕРОЯТНО НОРМАЛЬНО*\n"
                    f"• Причина: @SpamBot не ответил\n"
                    f"• ID: `{account_id}`\n\n"
                    f"*Интерпретация:*\n"
                    f"@SpamBot доступен для связи. Если бы были ограничения,\n"
                    f"бот обычно сразу сообщает о них.\n\n"
                    f"Рекомендуется проверить вручную, отправив любое\n"
                    f"сообщение в @SpamBot для получения полного ответа.",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_functions_menu()
                )
                
        except Exception as e:
            logger.error(f"Ошибка при работе с @SpamBot: {e}")
            await message.edit_text(
                f"🛡️ *Результат проверки спам блока:*\n\n"
                f"• Аккаунт: *{acc_info}*\n"
                f"• Статус: ❓ *НЕИЗВЕСТНО*\n"
                f"• Ошибка: Не удалось отправить /start\n"
                f"• Детали: {str(e)[:150]}\n\n"
                f"Попробуйте проверить вручную через @SpamBot.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_functions_menu()
            )
        
        await client.stop()
        
    except Exception as e:
        logger.error(f"Ошибка проверки спам блока: {e}")
        await message.edit_text(
            f"❌ *Ошибка проверки спам блока!*\n\n"
            f"Ошибка: {str(e)[:200]}\n\n"
            f"Проверьте:\n"
            f"1. Аккаунт активен\n"
            f"2. Нет ограничений на API\n"
            f"3. Попробуйте позже",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        try:
            await client.stop()
        except:
            pass

# ====== ОСТАЛЬНЫЕ ФУНКЦИИ (закладки, автоответчик, шаблоны, ловец чеков) ======
# Код для этих функций остается таким же как в предыдущей версии
# Я сохранил только основные изменения

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
    if user_id in temp_folder_data:
        del temp_folder_data[user_id]
    
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
