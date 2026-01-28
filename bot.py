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
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS folders (
                folder_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                account_id INTEGER,
                name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                FOREIGN KEY (account_id) REFERENCES accounts (account_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS folder_chats (
                folder_chat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                folder_id INTEGER,
                chat_id INTEGER,
                chat_title TEXT,
                chat_username TEXT,
                chat_type TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (folder_id) REFERENCES folders (folder_id)
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
    
    # ====== ПАПКИ ДЛЯ РАССЫЛКИ ======
    def create_folder(self, user_id: int, account_id: int, name: str):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO folders (user_id, account_id, name)
                VALUES (?, ?, ?)
            ''', (user_id, account_id, name))
            self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка создания папки: {e}")
            return None
    
    def get_user_folders(self, user_id: int, account_id: int = None):
        cursor = self.conn.cursor()
        if account_id:
            cursor.execute('''
                SELECT folder_id, name, 
                       (SELECT COUNT(*) FROM folder_chats WHERE folder_id = folders.folder_id) as chat_count
                FROM folders 
                WHERE user_id = ? AND account_id = ?
                ORDER BY created_at DESC
            ''', (user_id, account_id))
        else:
            cursor.execute('''
                SELECT folder_id, name, account_id,
                       (SELECT COUNT(*) FROM folder_chats WHERE folder_id = folders.folder_id) as chat_count
                FROM folders 
                WHERE user_id = ?
                ORDER BY created_at DESC
            ''', (user_id,))
        return cursor.fetchall()
    
    def get_folder_chats(self, folder_id: int, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT fc.folder_chat_id, fc.chat_id, fc.chat_title, fc.chat_username, fc.chat_type,
                   f.account_id
            FROM folder_chats fc
            JOIN folders f ON fc.folder_id = f.folder_id
            WHERE fc.folder_id = ? AND f.user_id = ?
            ORDER BY fc.added_at DESC
        ''', (folder_id, user_id))
        return cursor.fetchall()
    
    def add_chat_to_folder(self, folder_id: int, chat_id: int, chat_title: str = None, chat_username: str = None, chat_type: str = None):
        cursor = self.conn.cursor()
        try:
            # Проверяем количество чатов в папке
            cursor.execute('SELECT COUNT(*) FROM folder_chats WHERE folder_id = ?', (folder_id,))
            count = cursor.fetchone()[0]
            
            if count >= 20:
                return {"success": False, "message": "Максимум 20 чатов в папке"}
            
            # Проверяем, не добавлен ли уже этот чат
            cursor.execute('SELECT 1 FROM folder_chats WHERE folder_id = ? AND chat_id = ?', (folder_id, chat_id))
            if cursor.fetchone():
                return {"success": False, "message": "Чат уже в папке"}
            
            cursor.execute('''
                INSERT INTO folder_chats (folder_id, chat_id, chat_title, chat_username, chat_type)
                VALUES (?, ?, ?, ?, ?)
            ''', (folder_id, chat_id, chat_title, chat_username, chat_type))
            self.conn.commit()
            return {"success": True, "message": "Чат добавлен", "folder_chat_id": cursor.lastrowid}
        except Exception as e:
            logger.error(f"Ошибка добавления чата в папку: {e}")
            return {"success": False, "message": f"Ошибка: {str(e)}"}
    
    def remove_chat_from_folder(self, folder_chat_id: int, user_id: int):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                DELETE FROM folder_chats 
                WHERE folder_chat_id = ? AND folder_id IN (
                    SELECT folder_id FROM folders WHERE user_id = ?
                )
            ''', (folder_chat_id, user_id))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка удаления чата из папки: {e}")
            return False
    
    def delete_folder(self, folder_id: int, user_id: int):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                DELETE FROM folder_chats 
                WHERE folder_id IN (
                    SELECT folder_id FROM folders WHERE folder_id = ? AND user_id = ?
                )
            ''', (folder_id, user_id))
            
            cursor.execute('''
                DELETE FROM folders 
                WHERE folder_id = ? AND user_id = ?
            ''', (folder_id, user_id))
            
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка удаления папки: {e}")
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
    waiting_for_folder_name = State()

# ====== ХРАНИЛИЩА ======
active_tasks: Dict[int, List[asyncio.Task]] = {}
check_catchers: Dict[int, Dict[int, bool]] = {}
mass_send_data: Dict[int, Dict] = {}
selected_accounts_for_mass: Dict[int, List[int]] = {}
user_clients: Dict[int, Any] = {}
user_folders_cache: Dict[int, Dict[int, List[Dict]]] = {}
auto_reply_tasks: Dict[int, Dict[int, asyncio.Task]] = {}
temp_bookmark_data: Dict[int, Dict] = {}
temp_folder_data: Dict[int, Dict] = {}

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
        [InlineKeyboardButton(text="📂 Управление папками", callback_data="folders_menu")],
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
                    text=f"📂 {display_name}",
                    callback_data=f"folders_account_{account_id}"
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

def get_folders_menu(user_id: int, account_id: int = None):
    folders = db.get_user_folders(user_id, account_id)
    
    if not folders:
        if account_id:
            return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Создать папку", callback_data=f"create_folder_{account_id}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="folders_menu")]
            ])
        else:
            return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📂 Выбрать аккаунт", callback_data="folders_accounts")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")]
            ])
    
    keyboard = []
    for folder in folders[:15]:
        if account_id:
            folder_id, name, chat_count = folder
            keyboard.append([
                InlineKeyboardButton(
                    text=f"📁 {name} ({chat_count}/20)",
                    callback_data=f"folder_select_{folder_id}"
                )
            ])
        else:
            folder_id, name, acc_id, chat_count = folder
            keyboard.append([
                InlineKeyboardButton(
                    text=f"📁 {name} (Акк: {acc_id}, Чатов: {chat_count})",
                    callback_data=f"folder_select_{folder_id}"
                )
            ])
    
    if account_id:
        keyboard.append([
            InlineKeyboardButton(text="➕ Создать папку", callback_data=f"create_folder_{account_id}"),
            InlineKeyboardButton(text="⬅️ Назад", callback_data="folders_menu")
        ])
    else:
        keyboard.append([
            InlineKeyboardButton(text="📂 Выбрать аккаунт", callback_data="folders_accounts"),
            InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")
        ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_folder_actions_menu(folder_id: int, account_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить чаты", callback_data=f"add_chats_to_folder_{folder_id}")],
        [InlineKeyboardButton(text="👁️ Просмотреть чаты", callback_data=f"view_folder_chats_{folder_id}")],
        [InlineKeyboardButton(text="🗑️ Удалить папку", callback_data=f"delete_folder_{folder_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"folders_account_{account_id}")]
    ])

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
async def safe_get_dialogs_simple(client: Client, limit: int = 80):
    """Простое получение диалогов без вызова GetFullUser"""
    dialogs = []
    count = 0
    
    try:
        async for dialog in client.get_dialogs():
            dialogs.append(dialog)
            count += 1
            
            # Добавляем задержку каждые 10 диалогов
            if count % 10 == 0:
                await asyncio.sleep(0.5)
            
            # Ограничиваем количество
            if count >= limit:
                break
                
    except FloodWait as e:
        logger.warning(f"FloodWait при получении диалогов: {e.value} секунд")
        await asyncio.sleep(e.value + 1)
        # Пробуем продолжить с меньшим лимитом
        if len(dialogs) < 30:
            try:
                async for dialog in client.get_dialogs():
                    dialogs.append(dialog)
                    if len(dialogs) >= 30:
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

def translate_spambot_response(text: str) -> str:
    """Перевод ответов @SpamBot с английского на русский"""
    if not text:
        return text
    
    # Ключевые фразы для перевода
    translations = {
        # Статусы ограничений
        "your account is currently limited": "ваш аккаунт в настоящее время ограничен",
        "account limited": "аккаунт ограничен",
        "spam ban": "бан за спам",
        "spam restriction": "ограничение за спам",
        "flood ban": "бан за флуд",
        "flood restriction": "ограничение за флуд",
        "temporary restriction": "временное ограничение",
        "permanent restriction": "постоянное ограничение",
        
        # Предупреждения
        "warning": "предупреждение",
        "caution": "осторожность",
        "be careful": "будьте осторожны",
        "please slow down": "пожалуйста, замедлитесь",
        
        # Позитивные ответы
        "everything is fine": "всё хорошо",
        "all good": "всё в порядке",
        "no restrictions": "нет ограничений",
        "account is fine": "аккаунт в порядке",
        "everything is ok": "всё в порядке",
        "no issues": "нет проблем",
        
        # Приветствия
        "hello": "привет",
        "hi": "привет",
        "greetings": "приветствия",
        "welcome": "добро пожаловать",
        
        # Общие фразы
        "telegram": "телеграм",
        "account": "аккаунт",
        "message": "сообщение",
        "messages": "сообщения",
        "send": "отправить",
        "sending": "отправка",
        "spam": "спам",
        "user": "пользователь",
        "please": "пожалуйста",
        "contact": "контакт",
        "support": "поддержка",
        "help": "помощь",
        
        # Технические термины
        "restriction": "ограничение",
        "ban": "бан",
        "block": "блок",
        "suspended": "приостановлен",
        "penalty": "штраф",
        "action": "действие",
        "detected": "обнаружено",
        "system": "система",
        "automated": "автоматизированный",
        "review": "обзор",
        "appeal": "апелляция",
        
        # Вопросительные формы
        "what": "что",
        "when": "когда",
        "why": "почему",
        "how": "как",
        
        # Отрицания
        "not": "не",
        "no": "нет",
        "never": "никогда",
        
        # Время
        "temporary": "временный",
        "permanent": "постоянный",
        "duration": "длительность",
        "days": "дней",
        "hours": "часов",
    }
    
    # Заменяем ключевые фразы
    translated_text = text
    for eng, rus in translations.items():
        translated_text = translated_text.replace(eng, rus)
        translated_text = translated_text.replace(eng.capitalize(), rus.capitalize())
    
    return translated_text

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
• 📂 Создание папок для рассылки (до 20 чатов в папке)
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
            f"Пример: 10 (минимум 5, максимум 3600)",
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
        if delay < 5 or delay > 3600:  # Увеличена минимальная задержка
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
    
    # Получаем папки для этого аккаунта
    folders = db.get_user_folders(user_id, account_id)
    
    if not folders:
        await callback.message.edit_text(
            f"❌ *У аккаунта нет папок!*\n\n"
            f"Сначала создайте папку через меню 'Управление папками'.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📂 Управление папками", callback_data="folders_menu")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="mass_send")]
            ])
        )
        await callback.answer()
        return
    
    # Показываем меню выбора папки
    keyboard = []
    for folder in folders[:10]:  # Ограничим 10 папками
        folder_id, name, chat_count = folder
        keyboard.append([
            InlineKeyboardButton(
                text=f"📁 {name} ({chat_count}/20 чатов)",
                callback_data=f"mass_folder_{folder_id}"
            )
        ])
    
    keyboard.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="mass_send")
    ])
    
    await callback.message.edit_text(
        f"✅ *Аккаунт выбран!*\n\n"
        f"*Аккаунт ID:* `{account_id}`\n\n"
        f"Выберите папку для рассылки:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("mass_folder_"))
async def select_mass_folder(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[2])
    
    # Сохраняем выбранную папку
    mass_send_data[user_id]['folder_id'] = folder_id
    
    # Получаем информацию о папке
    folders = db.get_user_folders(user_id, mass_send_data[user_id]['account_id'])
    folder_name = "Неизвестно"
    chat_count = 0
    
    for folder in folders:
        if folder[0] == folder_id:
            folder_name = folder[1]
            chat_count = folder[2]
            break
    
    # Показываем сводку
    data = mass_send_data[user_id]
    
    summary = (
        f"📋 *Сводка рассылки:*\n\n"
        f"• Аккаунт: *{data['account_id']}*\n"
        f"• Папка: *{folder_name}* ({chat_count}/20 чатов)\n"
        f"• Сообщений в каждый чат: *{data['count']}*\n"
        f"• Задержка: *{data['delay']}* сек\n"
        f"• Текст: *{data['text'][:50]}...*\n\n"
        f"📊 *Статистика:*\n"
        f"• Всего сообщений: *{data['count'] * chat_count}*\n"
        f"• Примерное время: *{data['count'] * chat_count * data['delay'] / 60:.1f}* мин\n\n"
        f"⚠️ *Важно:* Для избежания FloodWait используется задержка {data['delay']} сек.\n\n"
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
    
    # Запускаем рассылку по папке в фоне
    asyncio.create_task(run_mass_send_to_folder(user_id, callback.message))
    await callback.answer("Рассылка запущена!")

async def run_mass_send_to_folder(user_id: int, message: types.Message):
    """Рассылка по папке (до 20 чатов)"""
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
        
        # Получаем чаты из папки
        folder_chats = db.get_folder_chats(folder_id, user_id)
        
        if not folder_chats:
            await message.edit_text(
                "❌ Ошибка: папка пуста или не найдена.",
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
        
        total_chats = len(folder_chats)
        total_messages = data['count'] * total_chats
        
        progress_msg = await message.edit_text(
            f"🚀 *Рассылка по папке начата!*\n\n"
            f"Чатов в папке: *{total_chats}*\n"
            f"Всего сообщений: *{total_messages}*\n"
            f"Задержка между сообщениями: *{data['delay']}* сек\n"
            f"Ожидаемое время: *{total_messages * data['delay'] / 60:.1f}* минут\n\n"
            f"Прогресс: 0/{total_messages} (0%)\n"
            f"Статус: Инициализация...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        sent_count = 0
        failed_count = 0
        
        for chat_index, chat_info in enumerate(folder_chats):
            folder_chat_id, chat_id, chat_title, chat_username, chat_type, acc_id = chat_info
            
            for message_num in range(data['count']):
                try:
                    # Добавляем дополнительную задержку перед отправкой
                    await asyncio.sleep(1)
                    
                    # Отправляем сообщение
                    await client.send_message(chat_id=chat_id, text=data['text'])
                    sent_count += 1
                    
                    # Обновляем прогресс
                    if (sent_count + failed_count) % 3 == 0 or (sent_count + failed_count) % max(1, total_messages // 10) == 0:
                        progress = sent_count + failed_count
                        percent = (progress / total_messages) * 100 if total_messages > 0 else 0
                        await progress_msg.edit_text(
                            f"🚀 *Рассылка в процессе...*\n\n"
                            f"Прогресс: {progress}/{total_messages} ({percent:.1f}%)\n"
                            f"Чат: {chat_index+1}/{total_chats}\n"
                            f"Сообщение: {message_num+1}/{data['count']}\n"
                            f"✅ Отправлено: {sent_count}\n"
                            f"❌ Ошибок: {failed_count}\n"
                            f"Чат: {chat_title[:20] if chat_title else f'ID {chat_id}'}",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    
                    # Задержка между сообщениями
                    if message_num < data['count'] - 1 or chat_index < total_chats - 1:
                        await asyncio.sleep(data['delay'])
                        
                except FloodWait as e:
                    failed_count += 1
                    wait_time = e.value
                    logger.warning(f"FloodWait: ожидание {wait_time} секунд")
                    await progress_msg.edit_text(
                        f"⏳ *FloodWait обнаружен*\n\n"
                        f"Ожидание: {wait_time} секунд\n"
                        f"Прогресс: {sent_count}/{total_messages}\n"
                        f"Чат: {chat_index+1}/{total_chats}",
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
            f"✅ *Рассылка по папке завершена!*\n\n"
            f"• Успешно отправлено: *{sent_count}* сообщений\n"
            f"• Не отправлено: *{failed_count}* сообщений\n"
            f"• Успешность: *{success_rate:.1f}%*\n"
            f"• Чатов обработано: *{total_chats}*\n"
            f"• Время выполнения: *{(sent_count + failed_count) * data['delay'] / 60:.1f}* минут\n\n"
            f"Рассылка завершена.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        
        await client.stop()
        
    except Exception as e:
        logger.error(f"Ошибка рассылки по папке: {e}")
        await message.edit_text(
            f"❌ *Ошибка рассылки!*\n\n"
            f"Ошибка: {str(e)[:200]}\n\n"
            f"Рекомендации:\n"
            f"1. Увеличьте задержку между сообщениями\n"
            f"2. Уменьшите количество сообщений\n"
            f"3. Проверьте, что аккаунт не ограничен\n"
            f"4. Попробуйте позже",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
    finally:
        if user_id in mass_send_data:
            del mass_send_data[user_id]
        if user_id in selected_accounts_for_mass:
            del selected_accounts_for_mass[user_id]

# ====== УПРАВЛЕНИЕ ПАПКАМИ ======
@dp.callback_query(F.data == "folders_menu")
async def show_folders_menu(callback: types.CallbackQuery):
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
        "📂 *Управление папками*\n\n"
        "Папки позволяют группировать до 20 чатов для рассылки.\n\n"
        "Выберите действие:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📂 Выбрать аккаунт", callback_data="folders_accounts")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data == "folders_accounts")
async def select_account_for_folders(callback: types.CallbackQuery):
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
        "📂 *Выберите аккаунт для управления папками:*\n\n"
        "Выберите аккаунт, чтобы просмотреть или создать папки.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_accounts_menu(user_id, "folders")
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("folders_account_"))
async def show_account_folders(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    account_id = int(callback.data.split("_")[2])
    
    folders = db.get_user_folders(user_id, account_id)
    
    if not folders:
        await callback.message.edit_text(
            f"📂 *У аккаунта {account_id} нет папок.*\n\n"
            f"Создайте первую папку для группировки чатов (макс. 20 чатов в папке).",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_folders_menu(user_id, account_id)
        )
    else:
        text = f"📂 *Папки аккаунта {account_id}:*\n\n"
        for folder in folders:
            folder_id, name, chat_count = folder
            text += f"• *{name}*\n"
            text += f"  Чатов: {chat_count}/20\n"
            text += f"  ID: `{folder_id}`\n\n"
        
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_folders_menu(user_id, account_id)
        )
    await callback.answer()

@dp.callback_query(F.data.startswith("create_folder_"))
async def create_new_folder(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    account_id = int(callback.data.split("_")[2])
    
    await state.update_data(account_id=account_id)
    
    await callback.message.edit_text(
        f"📂 *Создание новой папки*\n\n"
        f"Аккаунт: *{account_id}*\n\n"
        f"Пришлите название для новой папки:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_button()
    )
    await state.set_state(Form.waiting_for_folder_name)
    await callback.answer()

@dp.message(Form.waiting_for_folder_name)
async def process_folder_name(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    folder_name = message.text.strip()
    
    if len(folder_name) > 50:
        await message.answer(
            "❌ Название папки слишком длинное (макс. 50 символов).\n"
            "Пришлите более короткое название:",
            reply_markup=get_back_button()
        )
        return
    
    data = await state.get_data()
    account_id = data.get('account_id')
    
    folder_id = db.create_folder(user_id, account_id, folder_name)
    
    if folder_id:
        await message.answer(
            f"✅ *Папка создана!*\n\n"
            f"• Название: *{folder_name}*\n"
            f"• Аккаунт: *{account_id}*\n"
            f"• ID папки: `{folder_id}`\n\n"
            f"Теперь вы можете добавить чаты в эту папку.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_folders_menu(user_id, account_id)
        )
    else:
        await message.answer(
            "❌ *Ошибка при создании папки.*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_folders_menu(user_id, account_id)
        )
    
    await state.clear()

@dp.callback_query(F.data.startswith("folder_select_"))
async def select_folder(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[2])
    
    # Получаем информацию о папке
    folders = db.get_user_folders(user_id)
    account_id = None
    folder_name = "Неизвестно"
    
    for folder in folders:
        if folder[0] == folder_id:
            if len(folder) == 3:  # Если запрос был с account_id
                folder_name = folder[1]
                chat_count = folder[2]
                # Нужно получить account_id из другого запроса
                temp_folders = db.get_user_folders(user_id)
                for temp_folder in temp_folders:
                    if temp_folder[0] == folder_id:
                        account_id = temp_folder[2]
                        break
            else:  # Если запрос был без account_id
                folder_name = folder[1]
                account_id = folder[2]
                chat_count = folder[3]
            break
    
    if not account_id:
        await callback.answer("❌ Ошибка получения информации о папке")
        return
    
    await callback.message.edit_text(
        f"📂 *Папка: {folder_name}*\n\n"
        f"• Аккаунт: *{account_id}*\n"
        f"• ID папки: `{folder_id}`\n\n"
        f"Выберите действие:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_folder_actions_menu(folder_id, account_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("add_chats_to_folder_"))
async def add_chats_to_folder(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[4])
    
    # Получаем информацию о папке
    folders = db.get_user_folders(user_id)
    account_id = None
    
    for folder in folders:
        if folder[0] == folder_id:
            if len(folder) == 3:  # С account_id
                # Нужно получить account_id из другого запроса
                temp_folders = db.get_user_folders(user_id)
                for temp_folder in temp_folders:
                    if temp_folder[0] == folder_id:
                        account_id = temp_folder[2]
                        break
            else:  # Без account_id
                account_id = folder[2]
            break
    
    if not account_id:
        await callback.answer("❌ Ошибка получения информации о папке")
        return
    
    # Сохраняем данные во временное хранилище
    temp_folder_data[user_id] = {
        'folder_id': folder_id,
        'account_id': account_id
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
        name=f"folder_loader_{user_id}_{account_id}",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_string,
        workdir="sessions"
    )
    
    try:
        await client.start()
        
        # Получаем диалоги безопасным способом
        dialogs = await safe_get_dialogs_simple(client, limit=50)
        
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
                reply_markup=get_folders_menu(user_id, account_id)
            )
            return
        
        # Сохраняем список чатов во временное хранилище
        temp_folder_data[user_id]['chats'] = chats_list
        
        # Создаем клавиатуру с чатами
        keyboard = []
        for i, chat in enumerate(chats_list[:20]):  # Ограничим 20 чатами
            display_name = chat['title'][:25] if chat['title'] else f"Chat {chat['id']}"
            username_part = f" (@{chat['username']})" if chat['username'] else ""
            keyboard.append([
                InlineKeyboardButton(
                    text=f"💬 {display_name}{username_part}",
                    callback_data=f"select_chat_for_folder_{i}"
                )
            ])
        
        # Проверяем количество чатов уже в папке
        folder_chats = db.get_folder_chats(folder_id, user_id)
        current_count = len(folder_chats)
        
        keyboard.append([
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"folder_select_{folder_id}")
        ])
        
        await message.edit_text(
            f"✅ *Найдено {len(chats_list)} чатов*\n\n"
            f"Папка уже содержит: *{current_count}/20* чатов\n\n"
            f"Выберите чат для добавления в папку:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
        )
        
    except Exception as e:
        logger.error(f"Ошибка загрузки чатов для папки: {e}")
        await message.edit_text(
            f"❌ *Ошибка загрузки чатов:*\n\n`{str(e)[:200]}`\n\n"
            f"Рекомендации:\n"
            f"1. Подождите несколько минут\n"
            f"2. Убедитесь, что аккаунт активен\n"
            f"3. Попробуйте позже",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_folders_menu(user_id, account_id)
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
    account_id = temp_folder_data[user_id].get('account_id')
    
    if chat_index >= len(chats):
        await callback.answer("❌ Чат не найден")
        return
    
    chat = chats[chat_index]
    
    # Добавляем чат в папку
    result = db.add_chat_to_folder(
        folder_id=folder_id,
        chat_id=chat['id'],
        chat_title=chat['title'],
        chat_username=chat['username'],
        chat_type=chat['type']
    )
    
    if result["success"]:
        # Получаем обновленное количество чатов в папке
        folder_chats = db.get_folder_chats(folder_id, user_id)
        current_count = len(folder_chats)
        
        await callback.message.edit_text(
            f"✅ *Чат добавлен в папку!*\n\n"
            f"• Чат: *{chat['title']}*\n"
            f"• ID чата: `{chat['id']}`\n"
            f"• Username: @{chat['username'] or 'Нет'}\n"
            f"• Тип: {chat['type']}\n\n"
            f"Теперь в папке: *{current_count}/20* чатов\n\n"
            f"Вы можете добавить еще чаты.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить еще чаты", callback_data=f"add_chats_to_folder_{folder_id}")],
                [InlineKeyboardButton(text="👁️ Просмотреть чаты", callback_data=f"view_folder_chats_{folder_id}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"folder_select_{folder_id}")]
            ])
        )
    else:
        await callback.message.edit_text(
            f"❌ *{result['message']}*\n\n"
            f"Попробуйте выбрать другой чат.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить другой чат", callback_data=f"add_chats_to_folder_{folder_id}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"folder_select_{folder_id}")]
            ])
        )
    
    # Очищаем временные данные
    if user_id in temp_folder_data and 'chats' in temp_folder_data[user_id]:
        del temp_folder_data[user_id]['chats']
    
    await callback.answer()

@dp.callback_query(F.data.startswith("view_folder_chats_"))
async def view_folder_chats(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[3])
    
    # Получаем чаты из папки
    folder_chats = db.get_folder_chats(folder_id, user_id)
    
    if not folder_chats:
        await callback.message.edit_text(
            "📂 *Папка пуста.*\n\n"
            "Добавьте чаты в папку для рассылки.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить чаты", callback_data=f"add_chats_to_folder_{folder_id}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"folder_select_{folder_id}")]
            ])
        )
        await callback.answer()
        return
    
    # Получаем информацию о папке
    folders = db.get_user_folders(user_id)
    folder_name = "Неизвестно"
    account_id = None
    
    for folder in folders:
        if folder[0] == folder_id:
            if len(folder) == 3:  # С account_id
                folder_name = folder[1]
                # Нужно получить account_id из другого запроса
                temp_folders = db.get_user_folders(user_id)
                for temp_folder in temp_folders:
                    if temp_folder[0] == folder_id:
                        account_id = temp_folder[2]
                        break
            else:  # Без account_id
                folder_name = folder[1]
                account_id = folder[2]
            break
    
    text = f"📂 *Папка: {folder_name}*\n"
    text += f"Чатов: {len(folder_chats)}/20\n\n"
    text += "*Список чатов:*\n\n"
    
    keyboard = []
    for i, chat in enumerate(folder_chats[:15]):  # Ограничим показ 15 чатов
        folder_chat_id, chat_id, chat_title, chat_username, chat_type, acc_id = chat
        display_name = chat_title[:20] if chat_title else f"Chat {chat_id}"
        username_part = f" (@{chat_username})" if chat_username else ""
        text += f"{i+1}. {display_name}{username_part}\n"
        
        # Кнопка для удаления чата
        keyboard.append([
            InlineKeyboardButton(
                text=f"🗑️ {display_name}",
                callback_data=f"remove_folder_chat_{folder_chat_id}"
            )
        ])
    
    if len(folder_chats) > 15:
        text += f"\n... и еще {len(folder_chats) - 15} чатов"
    
    keyboard.append([
        InlineKeyboardButton(text="➕ Добавить еще чаты", callback_data=f"add_chats_to_folder_{folder_id}"),
        InlineKeyboardButton(text="⬅️ Назад", callback_data=f"folder_select_{folder_id}")
    ])
    
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("remove_folder_chat_"))
async def remove_folder_chat(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_chat_id = int(callback.data.split("_")[3])
    
    # Удаляем чат из папки
    if db.remove_chat_from_folder(folder_chat_id, user_id):
        await callback.answer("✅ Чат удален из папки")
        # Обновляем список чатов
        # Нужно найти folder_id для этого chat
        folder_chats = db.get_folder_chats(0, user_id)  # Получим все чаты для пользователя
        folder_id = None
        for chat in folder_chats:
            if chat[0] == folder_chat_id:
                folder_id = chat[5]  # account_id в этом случае
                # Нужно найти настоящий folder_id
                folders = db.get_user_folders(user_id, chat[5])
                for folder in folders:
                    # Проверить, есть ли чат в этой папке
                    folder_chats_check = db.get_folder_chats(folder[0], user_id)
                    for fc in folder_chats_check:
                        if fc[0] == folder_chat_id:
                            folder_id = folder[0]
                            break
                    if folder_id:
                        break
                break
        
        if folder_id:
            await view_folder_chats(callback)
        else:
            await callback.message.edit_text(
                "✅ *Чат удален из папки.*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="folders_menu")]
                ])
            )
    else:
        await callback.answer("❌ Ошибка удаления чата")

@dp.callback_query(F.data.startswith("delete_folder_"))
async def delete_folder_confirm(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[2])
    
    # Получаем информацию о папке
    folders = db.get_user_folders(user_id)
    folder_name = "Неизвестно"
    account_id = None
    
    for folder in folders:
        if folder[0] == folder_id:
            if len(folder) == 3:  # С account_id
                folder_name = folder[1]
                # Нужно получить account_id из другого запроса
                temp_folders = db.get_user_folders(user_id)
                for temp_folder in temp_folders:
                    if temp_folder[0] == folder_id:
                        account_id = temp_folder[2]
                        break
            else:  # Без account_id
                folder_name = folder[1]
                account_id = folder[2]
            break
    
    await callback.message.edit_text(
        f"🗑️ *Удаление папки*\n\n"
        f"Вы уверены, что хотите удалить папку?\n\n"
        f"• Название: *{folder_name}*\n"
        f"• ID: `{folder_id}`\n"
        f"• Аккаунт: *{account_id}*\n\n"
        f"*Внимание:* Все чаты в этой папке будут удалены!",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_delete_folder_{folder_id}"),
                InlineKeyboardButton(text="❌ Нет, отмена", callback_data=f"folder_select_{folder_id}")
            ]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("confirm_delete_folder_"))
async def confirm_delete_folder(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[3])
    
    # Получаем информацию о папке перед удалением
    folders = db.get_user_folders(user_id)
    account_id = None
    
    for folder in folders:
        if folder[0] == folder_id:
            if len(folder) == 3:  # С account_id
                # Нужно получить account_id из другого запроса
                temp_folders = db.get_user_folders(user_id)
                for temp_folder in temp_folders:
                    if temp_folder[0] == folder_id:
                        account_id = temp_folder[2]
                        break
            else:  # Без account_id
                account_id = folder[2]
            break
    
    if db.delete_folder(folder_id, user_id):
        await callback.message.edit_text(
            "✅ *Папка успешно удалена!*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_folders_menu(user_id, account_id)
        )
    else:
        await callback.message.edit_text(
            "❌ *Ошибка при удалении папки.*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_folders_menu(user_id, account_id)
        )
    await callback.answer()

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
    asyncio.create_task(run_spam_check_fixed(user_id, account_id, callback.message))
    await callback.answer()

async def run_spam_check_fixed(user_id: int, account_id: int, message: types.Message):
    """Проверка статуса через @SpamBot с переводом ответов"""
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
        
        # Очищаем историю с @SpamBot если есть
        try:
            # Получаем последние сообщения
            messages = []
            async for msg in client.get_chat_history("spambot", limit=3):
                messages.append(msg)
            
            # Если есть старые сообщения, удаляем их
            if messages:
                await message.edit_text(
                    f"🛡️ *Проверяю статус спам блока...*\n\n"
                    f"Аккаунт: {acc_info}\n"
                    f"Статус: Очищаю историю @SpamBot...",
                    parse_mode=ParseMode.MARKDOWN
                )
                for msg in messages:
                    try:
                        await msg.delete()
                    except:
                        pass
                await asyncio.sleep(1)
        except:
            pass
        
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
                    if (msg.from_user and msg.from_user.username == "spambot" and 
                        msg.date > sent_message.date):
                        message_text = msg.text or msg.caption or ""
                        if message_text:
                            spam_messages.append(message_text)
            except Exception as e:
                logger.error(f"Ошибка при получении истории @SpamBot: {e}")
            
            if spam_messages:
                # Берем самое первое (последнее по времени) сообщение
                original_response = spam_messages[0] if spam_messages else ""
                # Переводим ответ
                translated_response = translate_spambot_response(original_response)
                
                # Анализируем ответ
                status = "❓ Неизвестно"
                analysis = ""
                
                # Ищем ключевые слова в ответе (в оригинале)
                response_lower = original_response.lower()
                
                if any(word in response_lower for word in ["ограничен", "ограничение", "limited", "restrict", "спам", "spam"]):
                    status = "🚫 *ОГРАНИЧЕН* (Spam Block)"
                    analysis = "Аккаунт имеет ограничения на отправку сообщений"
                elif any(word in response_lower for word in ["предупреждение", "warning", "внимание"]):
                    status = "⚠️ *ПРЕДУПРЕЖДЕНИЕ*"
                    analysis = "Есть предупреждения, но ограничений может не быть"
                elif any(word in response_lower for word in ["всё хорошо", "все хорошо", "хорошо", "good", "fine", "ok", "ок"]):
                    status = "✅ *НОРМАЛЬНО* (No Spam Block)"
                    analysis = "Аккаунт не имеет ограничений"
                elif any(word in response_lower for word in ["привет", "здравствуйте", "hello", "hi"]):
                    status = "✅ *НОРМАЛЬНО* (No Spam Block)"
                    analysis = "@SpamBot ответил приветствием, ограничений нет"
                elif "не отвечает" in response_lower or "не могу" in response_lower:
                    status = "❓ *НЕОПРЕДЕЛЕНО*"
                    analysis = "@SpamBot не дал четкого ответа"
                else:
                    status = "❓ *НЕИЗВЕСТНО*"
                    analysis = "Ответ не распознан. Проверьте вручную"
                
                await message.edit_text(
                    f"🛡️ *Результат проверки спам блока:*\n\n"
                    f"• Аккаунт: *{acc_info}*\n"
                    f"• Статус: {status}\n"
                    f"• Анализ: {analysis}\n"
                    f"• ID: `{account_id}`\n\n"
                    f"*Ответ @SpamBot (оригинал):*\n"
                    f"```\n{original_response[:400]}\n```\n\n"
                    f"*Ответ @SpamBot (перевод):*\n"
                    f"```\n{translated_response[:400]}\n```\n\n"
                    f"*Рекомендации:*\n"
                    f"- Если статус 🚫 ОГРАНИЧЕН: не отправляйте массовые сообщения\n"
                    f"- Если статус ⚠️ ПРЕДУПРЕЖДЕНИЕ: будьте осторожны\n"
                    f"- Если статус ✅ НОРМАЛЬНО: можно работать",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_functions_menu()
                )
            else:
                # Если @SpamBot не ответил, проверяем есть ли диалог
                try:
                    # Пробуем получить информацию о чате
                    chat = await client.get_chat("spambot")
                    if chat:
                        await message.edit_text(
                            f"🛡️ *Результат проверки спам блока:*\n\n"
                            f"• Аккаунт: *{acc_info}*\n"
                            f"• Статус: ✅ *ВЕРОЯТНО НОРМАЛЬНО*\n"
                            f"• Причина: @SpamBot доступен, но не ответил\n"
                            f"• ID: `{account_id}`\n\n"
                            f"*Интерпретация:*\n"
                            f"@SpamBot доступен для связи. Если бы были ограничения,\n"
                            f"бот обычно сразу сообщает о них.\n\n"
                            f"Рекомендуется проверить вручную, отправив любое\n"
                            f"сообщение в @SpamBot для получения полного ответа.",
                            parse_mode=ParseMode.MARKDOWN,
                            reply_markup=get_functions_menu()
                        )
                except:
                    await message.edit_text(
                        f"🛡️ *Результат проверки спам блока:*\n\n"
                        f"• Аккаунт: *{acc_info}*\n"
                        f"• Статус: ❓ *НЕИЗВЕСТНО*\n"
                        f"• Причина: @SpamBot не ответил\n"
                        f"• ID: `{account_id}`\n\n"
                        f"*Рекомендации:*\n"
                        f"1. Проверьте вручную, отправив /start в @SpamBot\n"
                        f"2. Убедитесь, что аккаунт не заблокирован\n"
                        f"3. Попробуйте позже",
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

# ====== ОСТАЛЬНЫЕ ФУНКЦИИ ======
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
    if user_id in selected_accounts_for_mass:
        del selected_accounts_for_mass[user_id]
    if user_id in temp_folder_data:
        del temp_folder_data[user_id]
    
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
