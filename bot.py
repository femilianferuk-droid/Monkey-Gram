import os
import asyncio
import sqlite3
import logging
import json
from datetime import datetime
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass
from enum import Enum

from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup,
    InlineKeyboardButton, ReplyKeyboardRemove, FSInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from pyrogram import Client
from pyrogram.errors import (
    SessionPasswordNeeded, PhoneCodeInvalid,
    PhoneNumberInvalid, Unauthorized, FloodWait,
    AuthKeyUnregistered, UserNotParticipant
)
from pyrogram.types import Chat
import aiosqlite

# ========== НАСТРОЙКИ ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("Не установлена переменная окружения BOT_TOKEN")

# Создаем папку для сессий
if not os.path.exists("sessions"):
    os.makedirs("sessions")

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# ========== МОДЕЛИ ДАННЫХ ==========
class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    STOPPED = "stopped"

@dataclass
class Account:
    id: int
    user_id: int
    phone_number: str
    session_name: str
    added_at: str
    is_active: bool
    client: Optional[Client] = None

@dataclass
class Folder:
    id: int
    user_id: int
    folder_name: str

@dataclass
class FolderChat:
    id: int
    folder_id: int
    chat_username: str
    chat_id: int
    chat_title: str
    account_id: int

@dataclass
class MailingTask:
    id: int
    user_id: int
    folder_id: int
    message_text: str
    total_chats: int
    sent_count: int
    delay: int
    status: str
    created_at: str

# ========== БАЗА ДАННЫХ ==========
class Database:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_path: str = "monkeygram.db"):
        if not self._initialized:
            self.db_path = db_path
            self._init_db()
            self._initialized = True
    
    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Пользователи
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Аккаунты
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    phone_number TEXT NOT NULL,
                    session_name TEXT NOT NULL,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            # Папки
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS folders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    folder_name TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            # Чаты в папках
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS folder_chats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    folder_id INTEGER NOT NULL,
                    chat_username TEXT,
                    chat_id INTEGER NOT NULL,
                    chat_title TEXT NOT NULL,
                    account_id INTEGER NOT NULL,
                    FOREIGN KEY (folder_id) REFERENCES folders(id),
                    FOREIGN KEY (account_id) REFERENCES accounts(id)
                )
            ''')
            
            # Задачи рассылки
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS mailing_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    folder_id INTEGER NOT NULL,
                    message_text TEXT NOT NULL,
                    total_chats INTEGER NOT NULL,
                    sent_count INTEGER DEFAULT 0,
                    delay INTEGER NOT NULL,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (folder_id) REFERENCES folders(id)
                )
            ''')
            
            # Настройки автоподписки
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS auto_subscribe (
                    user_id INTEGER PRIMARY KEY,
                    is_enabled BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            conn.commit()
    
    @contextmanager
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    # ========== ПОЛЬЗОВАТЕЛИ ==========
    def add_user(self, user_id: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO users (user_id) VALUES (?)",
                    (user_id,)
                )
                return True
            except Exception as e:
                logger.error(f"Ошибка добавления пользователя: {e}")
                return False
    
    def get_user(self, user_id: int) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    # ========== АККАУНТЫ ==========
    def add_account(self, user_id: int, phone_number: str, session_name: str) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''INSERT INTO accounts 
                (user_id, phone_number, session_name, is_active) 
                VALUES (?, ?, ?, TRUE)''',
                (user_id, phone_number, session_name)
            )
            return cursor.lastrowid
    
    def get_user_accounts(self, user_id: int) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM accounts WHERE user_id = ? AND is_active = TRUE ORDER BY added_at DESC",
                (user_id,)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_account_count(self, user_id: int) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM accounts WHERE user_id = ? AND is_active = TRUE",
                (user_id,)
            )
            return cursor.fetchone()[0]
    
    def delete_account(self, account_id: int, user_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM accounts WHERE id = ? AND user_id = ?",
                (account_id, user_id)
            )
            return cursor.rowcount > 0
    
    def deactivate_account(self, account_id: int, user_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE accounts SET is_active = FALSE WHERE id = ? AND user_id = ?",
                (account_id, user_id)
            )
            return cursor.rowcount > 0
    
    def get_account(self, account_id: int, user_id: int) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM accounts WHERE id = ? AND user_id = ?",
                (account_id, user_id)
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    
    # ========== ПАПКИ ==========
    def create_folder(self, user_id: int, folder_name: str) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO folders (user_id, folder_name) VALUES (?, ?)",
                (user_id, folder_name)
            )
            return cursor.lastrowid
    
    def get_user_folders(self, user_id: int) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM folders WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_folder(self, folder_id: int, user_id: int) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM folders WHERE id = ? AND user_id = ?",
                (folder_id, user_id)
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def delete_folder(self, folder_id: int, user_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Удаляем сначала чаты в папке
            cursor.execute(
                "DELETE FROM folder_chats WHERE folder_id = ?",
                (folder_id,)
            )
            # Удаляем папку
            cursor.execute(
                "DELETE FROM folders WHERE id = ? AND user_id = ?",
                (folder_id, user_id)
            )
            return cursor.rowcount > 0
    
    # ========== ЧАТЫ В ПАПКАХ ==========
    def add_chat_to_folder(self, folder_id: int, chat_data: Dict, account_id: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''INSERT INTO folder_chats 
                (folder_id, chat_username, chat_id, chat_title, account_id) 
                VALUES (?, ?, ?, ?, ?)''',
                (
                    folder_id,
                    chat_data.get('username'),
                    chat_data['id'],
                    chat_data['title'],
                    account_id
                )
            )
    
    def get_folder_chats(self, folder_id: int) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''SELECT fc.*, a.phone_number 
                FROM folder_chats fc 
                JOIN accounts a ON fc.account_id = a.id 
                WHERE fc.folder_id = ?''',
                (folder_id,)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    # ========== ЗАДАЧИ РАССЫЛКИ ==========
    def create_mailing_task(self, user_id: int, folder_id: int, message_text: str, 
                          total_chats: int, delay: int) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''INSERT INTO mailing_tasks 
                (user_id, folder_id, message_text, total_chats, delay, status) 
                VALUES (?, ?, ?, ?, ?, 'pending')''',
                (user_id, folder_id, message_text, total_chats, delay)
            )
            return cursor.lastrowid
    
    def update_mailing_task(self, task_id: int, sent_count: int, status: str = None):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if status:
                cursor.execute(
                    "UPDATE mailing_tasks SET sent_count = ?, status = ? WHERE id = ?",
                    (sent_count, status, task_id)
                )
            else:
                cursor.execute(
                    "UPDATE mailing_tasks SET sent_count = ? WHERE id = ?",
                    (sent_count, task_id)
                )
    
    def get_mailing_task(self, task_id: int, user_id: int) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM mailing_tasks WHERE id = ? AND user_id = ?",
                (task_id, user_id)
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_user_tasks(self, user_id: int) -> List[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM mailing_tasks WHERE user_id = ? ORDER BY created_at DESC LIMIT 10",
                (user_id,)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    # ========== АВТОПОДПИСКА ==========
    def get_auto_subscribe_status(self, user_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT is_enabled FROM auto_subscribe WHERE user_id = ?",
                (user_id,)
            )
            row = cursor.fetchone()
            if row:
                return bool(row[0])
            # Если записи нет, создаем
            cursor.execute(
                "INSERT OR IGNORE INTO auto_subscribe (user_id, is_enabled) VALUES (?, FALSE)",
                (user_id,)
            )
            return False
    
    def toggle_auto_subscribe(self, user_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Получаем текущее состояние
            current = self.get_auto_subscribe_status(user_id)
            new_state = not current
            cursor.execute(
                '''INSERT OR REPLACE INTO auto_subscribe (user_id, is_enabled) 
                VALUES (?, ?)''',
                (user_id, new_state)
            )
            return new_state

# Инициализация базы данных
db = Database()

# ========== СОСТОЯНИЯ FSM ==========
class AddAccountStates(StatesGroup):
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()

class MailingStates(StatesGroup):
    waiting_count = State()
    waiting_delay = State()
    waiting_message = State()
    waiting_folder = State()

class CreateFolderStates(StatesGroup):
    selecting_chats = State()
    naming_folder = State()

# ========== МЕНЕДЖЕР СЕССИЙ ==========
class SessionManager:
    _sessions: Dict[int, Client] = {}
    
    @classmethod
    async def get_client(cls, session_name: str) -> Optional[Client]:
        """Получить клиент Pyrogram по имени сессии"""
        try:
            if session_name in cls._sessions:
                client = cls._sessions[session_name]
                try:
                    await client.get_me()
                    return client
                except (AuthKeyUnregistered, Unauthorized):
                    # Сессия устарела, удаляем
                    del cls._sessions[session_name]
                    return None
            
            # Создаем новый клиент
            session_path = f"sessions/{session_name}.session"
            if not os.path.exists(session_path):
                return None
            
            client = Client(
                name=session_name,
                api_id=API_ID,
                api_hash=API_HASH,
                workdir="sessions/",
                in_memory=False
            )
            
            await client.start()
            cls._sessions[session_name] = client
            return client
            
        except Exception as e:
            logger.error(f"Ошибка получения клиента {session_name}: {e}")
            return None
    
    @classmethod
    async def close_all(cls):
        """Закрыть все сессии"""
        for session_name, client in cls._sessions.items():
            try:
                await client.stop()
            except:
                pass
        cls._sessions.clear()

# ========== КЛАВИАТУРЫ ==========
def get_main_keyboard() -> InlineKeyboardMarkup:
    """Главное меню"""
    builder = InlineKeyboardBuilder()
    builder.button(text="Функции", callback_data="functions")
    return builder.as_markup()

def get_functions_keyboard() -> InlineKeyboardMarkup:
    """Меню функций"""
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить аккаунт", callback_data="add_account")
    builder.button(text="📋 Мои аккаунты", callback_data="my_accounts")
    builder.button(text="📢 Рассылка", callback_data="mailing")
    builder.button(text="🤖 Автоподписка", callback_data="auto_subscribe")
    builder.button(text="🛡️ Проверка спамблока", callback_data="spam_check")
    builder.button(text="⬅️ Назад", callback_data="main_menu")
    builder.adjust(1)
    return builder.as_markup()

def get_accounts_keyboard(accounts: List[Dict], page: int = 0, per_page: int = 5) -> InlineKeyboardMarkup:
    """Клавиатура для списка аккаунтов"""
    builder = InlineKeyboardBuilder()
    
    start_idx = page * per_page
    end_idx = start_idx + per_page
    paginated_accounts = accounts[start_idx:end_idx]
    
    for acc in paginated_accounts:
        builder.button(
            text=f"{acc['phone_number']} ({'✅' if acc['is_active'] else '❌'})",
            callback_data=f"account_{acc['id']}"
        )
    
    # Пагинация
    total_pages = (len(accounts) + per_page - 1) // per_page
    if total_pages > 1:
        row_builder = InlineKeyboardBuilder()
        if page > 0:
            row_builder.button(text="◀️", callback_data=f"accounts_page_{page-1}")
        row_builder.button(text=f"{page+1}/{total_pages}", callback_data="noop")
        if page < total_pages - 1:
            row_builder.button(text="▶️", callback_data=f"accounts_page_{page+1}")
        builder.attach(row_builder)
    
    builder.button(text="⬅️ Назад", callback_data="functions")
    builder.adjust(1)
    return builder.as_markup()

def get_account_management_keyboard(account_id: int) -> InlineKeyboardMarkup:
    """Управление конкретным аккаунтом"""
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Удалить сессию", callback_data=f"delete_session_{account_id}")
    builder.button(text="🗑️ Удалить аккаунт", callback_data=f"delete_account_{account_id}")
    builder.button(text="⬅️ Назад к аккаунтам", callback_data="my_accounts")
    builder.adjust(1)
    return builder.as_markup()

def get_confirmation_keyboard(action: str, item_id: int) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения"""
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да", callback_data=f"confirm_{action}_{item_id}")
    builder.button(text="❌ Нет", callback_data=f"cancel_{action}_{item_id}")
    return builder.as_markup()

def get_folders_keyboard(folders: List[Dict]) -> InlineKeyboardMarkup:
    """Список папок"""
    builder = InlineKeyboardBuilder()
    for folder in folders:
        builder.button(text=f"📁 {folder['folder_name']}", callback_data=f"folder_{folder['id']}")
    builder.button(text="📁 Создать папку", callback_data="create_folder")
    builder.button(text="⬅️ Назад", callback_data="mailing")
    builder.adjust(1)
    return builder.as_markup()

def get_chat_selection_keyboard(chats: List[Dict], selected_chats: List[int] = None) -> InlineKeyboardMarkup:
    """Выбор чатов для папки"""
    if selected_chats is None:
        selected_chats = []
    
    builder = InlineKeyboardBuilder()
    for chat in chats:
        is_selected = chat['id'] in selected_chats
        emoji = "✅" if is_selected else "☑️"
        builder.button(
            text=f"{emoji} {chat['title']}",
            callback_data=f"select_chat_{chat['id']}"
        )
    
    builder.button(text="📝 Сохранить папку", callback_data="save_folder")
    builder.button(text="❌ Отмена", callback_data="cancel_folder")
    builder.adjust(1)
    return builder.as_markup()

def get_auto_subscribe_keyboard(is_enabled: bool) -> InlineKeyboardMarkup:
    """Клавиатура автоподписки"""
    builder = InlineKeyboardBuilder()
    status = "✅ Включена" if is_enabled else "❌ Выключена"
    builder.button(text=f"Статус: {status}", callback_data="noop")
    builder.button(
        text="🔄 Переключить", 
        callback_data="toggle_auto_subscribe"
    )
    builder.button(text="⬅️ Назад", callback_data="functions")
    builder.adjust(1)
    return builder.as_markup()

# ========== ОБРАБОТЧИКИ КОМАНД ==========
@router.message(Command("start"))
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    db.add_user(message.from_user.id)
    
    welcome_text = (
        "Привет! Я Monkey Gram — бот для управления аккаунтами Telegram.\n"
        "Выбери действие ниже."
    )
    
    await message.answer(welcome_text, reply_markup=get_main_keyboard())

@router.callback_query(F.data == "main_menu")
async def back_to_main(callback: CallbackQuery):
    """Вернуться в главное меню"""
    await callback.message.edit_text(
        "Главное меню",
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data == "functions")
async def show_functions(callback: CallbackQuery):
    """Показать меню функций"""
    await callback.message.edit_text(
        "📋 Функции бота:",
        reply_markup=get_functions_keyboard()
    )
    await callback.answer()

# ========== ДОБАВЛЕНИЕ АККАУНТА ==========
@router.callback_query(F.data == "add_account")
async def add_account_start(callback: CallbackQuery, state: FSMContext):
    """Начало добавления аккаунта"""
    user_id = callback.from_user.id
    
    # Проверка лимита
    account_count = db.get_account_count(user_id)
    if account_count >= 20:
        await callback.message.answer(
            "❌ Вы достигли лимита в 20 аккаунтов. "
            "Удалите некоторые аккаунты, чтобы добавить новые."
        )
        await callback.answer()
        return
    
    await callback.message.answer(
        "📱 Отправьте номер телефона в международном формате (например: +79123456789):",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(AddAccountStates.waiting_phone)
    await callback.answer()

@router.message(AddAccountStates.waiting_phone)
async def process_phone(message: Message, state: FSMContext):
    """Обработка номера телефона"""
    phone_number = message.text.strip()
    
    # Проверка формата
    if not phone_number.startswith('+') or not phone_number[1:].isdigit():
        await message.answer(
            "❌ Неверный формат номера. "
            "Отправьте номер в международном формате (например: +79123456789):"
        )
        return
    
    # Генерация имени сессии
    session_name = f"user_{message.from_user.id}_acc_{int(datetime.now().timestamp())}"
    
    await state.update_data(phone_number=phone_number, session_name=session_name)
    
    try:
        # Создаем временный клиент для авторизации
        client = Client(
            name=session_name,
            api_id=API_ID,
            api_hash=API_HASH,
            workdir="sessions/",
            in_memory=False,
            phone_number=phone_number
        )
        
        await client.connect()
        sent_code = await client.send_code(phone_number)
        
        await state.update_data(
            client=client,
            phone_code_hash=sent_code.phone_code_hash
        )
        
        await message.answer(
            "✅ Код отправлен. Пожалуйста, отправьте код из Telegram:"
        )
        await state.set_state(AddAccountStates.waiting_code)
        
    except FloodWait as e:
        await message.answer(
            f"❌ Flood wait. Попробуйте через {e.value} секунд."
        )
        await state.clear()
    except PhoneNumberInvalid:
        await message.answer(
            "❌ Неверный номер телефона. Попробуйте снова:"
        )
    except Exception as e:
        logger.error(f"Ошибка отправки кода: {e}")
        await message.answer(
            "❌ Произошла ошибка. Попробуйте позже."
        )
        await state.clear()

@router.message(AddAccountStates.waiting_code)
async def process_code(message: Message, state: FSMContext):
    """Обработка кода подтверждения"""
    code = message.text.strip()
    
    data = await state.get_data()
    client: Client = data.get('client')
    phone_code_hash = data.get('phone_code_hash')
    phone_number = data.get('phone_number')
    session_name = data.get('session_name')
    
    if not client:
        await message.answer("❌ Ошибка сессии. Начните заново.")
        await state.clear()
        return
    
    try:
        # Пытаемся войти с кодом
        await client.sign_in(
            phone_number=phone_number,
            phone_code_hash=phone_code_hash,
            phone_code=code
        )
        
        # Успешная авторизация
        user = await client.get_me()
        
        # Сохраняем аккаунт в БД
        db.add_account(message.from_user.id, phone_number, session_name)
        
        await message.answer(
            f"✅ Аккаунт {phone_number} успешно добавлен!\n"
            f"Имя: {user.first_name or ''} {user.last_name or ''}\n"
            f"Username: @{user.username or 'нет'}"
        )
        
        await client.disconnect()
        await state.clear()
        
    except SessionPasswordNeeded:
        await message.answer(
            "🔐 Требуется двухфакторная аутентификация. "
            "Отправьте пароль 2FA:"
        )
        await state.set_state(AddAccountStates.waiting_password)
    except PhoneCodeInvalid:
        await message.answer("❌ Неверный код. Попробуйте снова:")
    except Exception as e:
        logger.error(f"Ошибка авторизации: {e}")
        await message.answer("❌ Произошла ошибка. Попробуйте позже.")
        await state.clear()

@router.message(AddAccountStates.waiting_password)
async def process_password(message: Message, state: FSMContext):
    """Обработка пароля 2FA"""
    password = message.text.strip()
    
    data = await state.get_data()
    client: Client = data.get('client')
    phone_number = data.get('phone_number')
    session_name = data.get('session_name')
    
    if not client:
        await message.answer("❌ Ошибка сессии. Начните заново.")
        await state.clear()
        return
    
    try:
        # Пытаемся войти с паролем
        await client.check_password(password)
        
        # Успешная авторизация
        user = await client.get_me()
        
        # Сохраняем аккаунт в БД
        db.add_account(message.from_user.id, phone_number, session_name)
        
        await message.answer(
            f"✅ Аккаунт {phone_number} успешно добавлен!\n"
            f"Имя: {user.first_name or ''} {user.last_name or ''}\n"
            f"Username: @{user.username or 'нет'}"
        )
        
        await client.disconnect()
        await state.clear()
        
    except Exception as e:
        logger.error(f"Ошибка 2FA: {e}")
        await message.answer("❌ Неверный пароль. Попробуйте снова:")
        # Можно дать несколько попыток, но пока просто сбрасываем
        await state.clear()

# ========== МОИ АККАУНТЫ ==========
@router.callback_query(F.data == "my_accounts")
async def show_my_accounts(callback: CallbackQuery):
    """Показать список аккаунтов"""
    user_id = callback.from_user.id
    accounts = db.get_user_accounts(user_id)
    
    if not accounts:
        await callback.message.answer(
            "📭 У вас нет добавленных аккаунтов.",
            reply_markup=get_functions_keyboard()
        )
    else:
        await callback.message.edit_text(
            f"📋 Ваши аккаунты ({len(accounts)}/20):",
            reply_markup=get_accounts_keyboard(accounts)
        )
    await callback.answer()

@router.callback_query(F.data.startswith("accounts_page_"))
async def paginate_accounts(callback: CallbackQuery):
    """Пагинация аккаунтов"""
    page = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    accounts = db.get_user_accounts(user_id)
    
    await callback.message.edit_text(
        f"📋 Ваши аккаунты ({len(accounts)}/20):",
        reply_markup=get_accounts_keyboard(accounts, page)
    )
    await callback.answer()

@router.callback_query(F.data.startswith("account_"))
async def show_account_management(callback: CallbackQuery):
    """Управление конкретным аккаунтом"""
    account_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id
    
    account = db.get_account(account_id, user_id)
    if not account:
        await callback.answer("Аккаунт не найден")
        return
    
    status = "✅ Активен" if account['is_active'] else "❌ Неактивен"
    text = (
        f"📱 Аккаунт: {account['phone_number']}\n"
        f"📅 Добавлен: {account['added_at']}\n"
        f"🔧 Статус: {status}\n\n"
        f"Выберите действие:"
    )
    
    await callback.message.edit_text(
        text,
        reply_markup=get_account_management_keyboard(account_id)
    )
    await callback.answer()

@router.callback_query(F.data.startswith("delete_session_"))
async def delete_session_handler(callback: CallbackQuery):
    """Удаление сессии"""
    account_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    
    account = db.get_account(account_id, user_id)
    if not account:
        await callback.answer("Аккаунт не найден")
        return
    
    await callback.message.edit_text(
        f"❌ Удалить сессию для аккаунта {account['phone_number']}?\n"
        f"Файл сессии будет удален, но аккаунт останется в базе данных.",
        reply_markup=get_confirmation_keyboard("session", account_id)
    )
    await callback.answer()

@router.callback_query(F.data.startswith("delete_account_"))
async def delete_account_handler(callback: CallbackQuery):
    """Удаление аккаунта"""
    account_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    
    account = db.get_account(account_id, user_id)
    if not account:
        await callback.answer("Аккаунт не найден")
        return
    
    await callback.message.edit_text(
        f"🗑️ Полностью удалить аккаунт {account['phone_number']}?\n"
        f"Это действие нельзя отменить!",
        reply_markup=get_confirmation_keyboard("account", account_id)
    )
    await callback.answer()

@router.callback_query(F.data.startswith("confirm_session_"))
async def confirm_delete_session(callback: CallbackQuery):
    """Подтверждение удаления сессии"""
    account_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    
    account = db.get_account(account_id, user_id)
    if not account:
        await callback.answer("Аккаунт не найден")
        return
    
    # Удаляем файл сессии
    session_file = f"sessions/{account['session_name']}.session"
    if os.path.exists(session_file):
        os.remove(session_file)
    
    # Деактивируем в БД
    db.deactivate_account(account_id, user_id)
    
    await callback.message.edit_text(
        f"✅ Сессия для аккаунта {account['phone_number']} удалена."
    )
    await callback.answer()

@router.callback_query(F.data.startswith("confirm_account_"))
async def confirm_delete_account(callback: CallbackQuery):
    """Подтверждение удаления аккаунта"""
    account_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    
    account = db.get_account(account_id, user_id)
    if not account:
        await callback.answer("Аккаунт не найден")
        return
    
    # Удаляем файл сессии
    session_file = f"sessions/{account['session_name']}.session"
    if os.path.exists(session_file):
        os.remove(session_file)
    
    # Удаляем из БД
    db.delete_account(account_id, user_id)
    
    await callback.message.edit_text(
        f"✅ Аккаунт {account['phone_number']} полностью удален."
    )
    await callback.answer()

@router.callback_query(F.data.startswith("cancel_"))
async def cancel_action(callback: CallbackQuery):
    """Отмена действия"""
    await callback.message.edit_text("Действие отменено.")
    await callback.answer()

# ========== РАССЫЛКА ==========
@router.callback_query(F.data == "mailing")
async def mailing_start(callback: CallbackQuery, state: FSMContext):
    """Начало создания рассылки"""
    await callback.message.answer(
        "📊 Введите количество сообщений для отправки (от 1 до 500):"
    )
    await state.set_state(MailingStates.waiting_count)
    await callback.answer()

@router.message(MailingStates.waiting_count)
async def process_mailing_count(message: Message, state: FSMContext):
    """Обработка количества сообщений"""
    try:
        count = int(message.text.strip())
        if count < 1 or count > 500:
            await message.answer("❌ Количество должно быть от 1 до 500. Попробуйте снова:")
            return
        
        await state.update_data(count=count)
        await message.answer(
            "⏱️ Введите задержку между сообщениями в секундах (от 20 до 3000):"
        )
        await state.set_state(MailingStates.waiting_delay)
        
    except ValueError:
        await message.answer("❌ Введите число. Попробуйте снова:")

@router.message(MailingStates.waiting_delay)
async def process_mailing_delay(message: Message, state: FSMContext):
    """Обработка задержки"""
    try:
        delay = int(message.text.strip())
        if delay < 20 or delay > 3000:
            await message.answer("❌ Задержка должна быть от 20 до 3000 секунд. Попробуйте снова:")
            return
        
        await state.update_data(delay=delay)
        await message.answer(
            "📝 Введите текст сообщения для рассылки:"
        )
        await state.set_state(MailingStates.waiting_message)
        
    except ValueError:
        await message.answer("❌ Введите число. Попробуйте снова:")

@router.message(MailingStates.waiting_message)
async def process_mailing_message(message: Message, state: FSMContext):
    """Обработка текста сообщения"""
    message_text = message.text.strip()
    if len(message_text) > 4000:
        await message.answer("❌ Текст слишком длинный. Максимум 4000 символов.")
        return
    
    await state.update_data(message_text=message_text)
    
    # Проверяем наличие папок
    user_id = message.from_user.id
    folders = db.get_user_folders(user_id)
    
    if not folders:
        await message.answer(
            "📂 У вас нет созданных папок. Хотите создать папку?",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📁 Создать папку", callback_data="create_folder_now")],
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="functions")]
                ]
            )
        )
    else:
        await message.answer(
            "📂 Выберите папку для рассылки:",
            reply_markup=get_folders_keyboard(folders)
        )
        await state.set_state(MailingStates.waiting_folder)

@router.callback_query(F.data == "create_folder_now")
async def create_folder_for_mailing(callback: CallbackQuery, state: FSMContext):
    """Создание папки для рассылки"""
    await process_create_folder_start(callback, state)

# ========== СОЗДАНИЕ ПАПКИ ==========
async def get_user_chats_from_accounts(user_id: int) -> List[Dict]:
    """Получить все чаты со всех активных аккаунтов пользователя"""
    accounts = db.get_user_accounts(user_id)
    all_chats = []
    
    for account in accounts:
        client = await SessionManager.get_client(account['session_name'])
        if not client:
            continue
        
        try:
            async for dialog in client.get_dialogs():
                if dialog.chat.type in ["group", "supergroup", "channel"]:
                    chat = dialog.chat
                    all_chats.append({
                        'id': chat.id,
                        'title': chat.title or "Без названия",
                        'username': getattr(chat, 'username', None),
                        'account_id': account['id']
                    })
        except Exception as e:
            logger.error(f"Ошибка получения чатов для аккаунта {account['phone_number']}: {e}")
    
    # Убираем дубликаты (по ID чата)
    unique_chats = {}
    for chat in all_chats:
        if chat['id'] not in unique_chats:
            unique_chats[chat['id']] = chat
    
    return list(unique_chats.values())

@router.callback_query(F.data == "create_folder")
async def process_create_folder_start(callback: CallbackQuery, state: FSMContext = None):
    """Начало создания папки"""
    user_id = callback.from_user.id
    
    # Проверяем наличие активных аккаунтов
    accounts = db.get_user_accounts(user_id)
    if not accounts:
        await callback.message.answer(
            "❌ У вас нет активных аккаунтов. Добавьте аккаунт сначала."
        )
        await callback.answer()
        return
    
    # Получаем чаты
    await callback.message.edit_text("⏳ Загружаю чаты с ваших аккаунтов...")
    
    chats = await get_user_chats_from_accounts(user_id)
    
    if not chats:
        await callback.message.edit_text(
            "❌ Не удалось найти чаты на ваших аккаунтах."
        )
        await callback.answer()
        return
    
    # Сохраняем чаты в состоянии
    if state:
        await state.set_state(CreateFolderStates.selecting_chats)
        await state.update_data(chats=chats, selected_chats=[])
    
    await callback.message.edit_text(
        f"📋 Выберите до 20 чатов для добавления в папку:\n"
        f"Найдено чатов: {len(chats)}",
        reply_markup=get_chat_selection_keyboard(chats[:20], [])
    )
    await callback.answer()

@router.callback_query(F.data.startswith("select_chat_"))
async def process_chat_selection(callback: CallbackQuery, state: FSMContext):
    """Выбор/отмена выбора чата"""
    chat_id = int(callback.data.split("_")[2])
    
    data = await state.get_data()
    chats = data.get('chats', [])
    selected_chats = data.get('selected_chats', [])
    
    if chat_id in selected_chats:
        selected_chats.remove(chat_id)
    else:
        if len(selected_chats) >= 20:
            await callback.answer("❌ Максимум 20 чатов в папке")
            return
        selected_chats.append(chat_id)
    
    await state.update_data(selected_chats=selected_chats)
    
    # Обновляем клавиатуру
    await callback.message.edit_reply_markup(
        reply_markup=get_chat_selection_keyboard(chats[:20], selected_chats)
    )
    await callback.answer(f"Выбрано: {len(selected_chats)}/20")

@router.callback_query(F.data == "save_folder")
async def save_folder_name(callback: CallbackQuery, state: FSMContext):
    """Сохранение папки с именем"""
    data = await state.get_data()
    selected_chats = data.get('selected_chats', [])
    
    if not selected_chats:
        await callback.answer("❌ Выберите хотя бы один чат")
        return
    
    await callback.message.edit_text(
        "📝 Введите название для папки:"
    )
    await state.set_state(CreateFolderStates.naming_folder)

@router.message(CreateFolderStates.naming_folder)
async def process_folder_name(message: Message, state: FSMContext):
    """Обработка названия папки"""
    folder_name = message.text.strip()
    if not folder_name or len(folder_name) > 50:
        await message.answer("❌ Название должно быть от 1 до 50 символов. Попробуйте снова:")
        return
    
    data = await state.get_data()
    chats = data.get('chats', [])
    selected_chats = data.get('selected_chats', [])
    
    # Создаем папку
    folder_id = db.create_folder(message.from_user.id, folder_name)
    
    # Добавляем выбранные чаты
    selected_chat_data = [chat for chat in chats if chat['id'] in selected_chats]
    for chat in selected_chat_data:
        db.add_chat_to_folder(folder_id, chat, chat['account_id'])
    
    await message.answer(
        f"✅ Папка '{folder_name}' создана!\n"
        f"Добавлено чатов: {len(selected_chats)}"
    )
    await state.clear()

# ========== ЗАПУСК РАССЫЛКИ ==========
@router.callback_query(MailingStates.waiting_folder, F.data.startswith("folder_"))
async def start_mailing(callback: CallbackQuery, state: FSMContext):
    """Запуск рассылки"""
    folder_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id
    
    # Проверяем папку
    folder = db.get_folder(folder_id, user_id)
    if not folder:
        await callback.answer("Папка не найдена")
        return
    
    # Получаем данные из состояния
    data = await state.get_data()
    count = data.get('count')
    delay = data.get('delay')
    message_text = data.get('message_text')
    
    # Получаем чаты из папки
    chats = db.get_folder_chats(folder_id)
    if not chats:
        await callback.message.answer("❌ В папке нет чатов.")
        await state.clear()
        await callback.answer()
        return
    
    # Ограничиваем количество чатов
    chats_to_send = chats[:count]
    
    # Создаем задачу рассылки
    task_id = db.create_mailing_task(
        user_id, folder_id, message_text,
        len(chats_to_send), delay
    )
    
    await callback.message.edit_text(
        f"🚀 Запускаю рассылку...\n"
        f"• Папка: {folder['folder_name']}\n"
        f"• Сообщений: {len(chats_to_send)}\n"
        f"• Задержка: {delay} сек.\n\n"
        f"Статус: ⏳ Подготовка..."
    )
    
    # Запускаем рассылку в фоне
    asyncio.create_task(
        run_mailing_task(task_id, user_id, chats_to_send, message_text, delay, callback.message)
    )
    
    await state.clear()
    await callback.answer()

async def run_mailing_task(task_id: int, user_id: int, chats: List[Dict], 
                          message_text: str, delay: int, message: Message):
    """Запуск задачи рассылки"""
    sent_count = 0
    failed_count = 0
    
    # Обновляем статус задачи
    db.update_mailing_task(task_id, 0, "running")
    
    try:
        for i, chat in enumerate(chats):
            try:
                # Получаем клиент для аккаунта
                account = db.get_account(chat['account_id'], user_id)
                if not account:
                    continue
                
                client = await SessionManager.get_client(account['session_name'])
                if not client:
                    failed_count += 1
                    continue
                
                # Отправляем сообщение
                await client.send_message(
                    chat['chat_id'],
                    message_text
                )
                
                sent_count += 1
                
                # Обновляем счетчик в БД
                db.update_mailing_task(task_id, sent_count)
                
                # Обновляем статус в сообщении каждые 5 отправок или в конце
                if sent_count % 5 == 0 or i == len(chats) - 1:
                    status_msg = (
                        f"📊 Статус рассылки:\n"
                        f"• Отправлено: {sent_count}/{len(chats)}\n"
                        f"• Неудачных: {failed_count}\n"
                        f"• Статус: {'✅ Завершено' if i == len(chats) - 1 else '🔄 В процессе'}"
                    )
                    
                    try:
                        await message.edit_text(status_msg)
                    except:
                        pass  # Игнорируем ошибки редактирования
                
                # Задержка между сообщениями
                if i < len(chats) - 1:
                    await asyncio.sleep(delay)
                    
            except Exception as e:
                logger.error(f"Ошибка отправки в чат {chat['chat_title']}: {e}")
                failed_count += 1
                continue
        
        # Финальный статус
        db.update_mailing_task(task_id, sent_count, "completed")
        
        final_msg = (
            f"✅ Рассылка завершена!\n"
            f"• Успешно отправлено: {sent_count}/{len(chats)}\n"
            f"• Неудачных попыток: {failed_count}"
        )
        
        await message.edit_text(final_msg)
        
    except Exception as e:
        logger.error(f"Ошибка в задаче рассылки {task_id}: {e}")
        db.update_mailing_task(task_id, sent_count, "stopped")
        await message.edit_text(f"❌ Рассылка остановлена из-за ошибки: {str(e)}")

# ========== АВТОПОДПИСКА ==========
@router.callback_query(F.data == "auto_subscribe")
async def show_auto_subscribe(callback: CallbackQuery):
    """Показать настройки автоподписки"""
    user_id = callback.from_user.id
    is_enabled = db.get_auto_subscribe_status(user_id)
    
    await callback.message.edit_text(
        "🤖 Автоподписка на инлайн-кнопки\n\n"
        "При активной рассылке бот будет автоматически нажимать на первые инлайн-кнопки "
        "в ответах на отправленные сообщения (кнопки 'Подписаться', 'Join', 'Перейти' и т.д.)",
        reply_markup=get_auto_subscribe_keyboard(is_enabled)
    )
    await callback.answer()

@router.callback_query(F.data == "toggle_auto_subscribe")
async def toggle_auto_subscribe_handler(callback: CallbackQuery):
    """Переключение автоподписки"""
    user_id = callback.from_user.id
    new_state = db.toggle_auto_subscribe(user_id)
    
    status = "включена" if new_state else "выключена"
    await callback.answer(f"Автоподписка {status}")
    
    # Обновляем клавиатуру
    await callback.message.edit_reply_markup(
        reply_markup=get_auto_subscribe_keyboard(new_state)
    )

# ========== ПРОВЕРКА СПАМБЛОКА ==========
@router.callback_query(F.data == "spam_check")
async def spam_check_handler(callback: CallbackQuery):
    """Проверка спамблока"""
    user_id = callback.from_user.id
    
    # Получаем первый активный аккаунт
    accounts = db.get_user_accounts(user_id)
    if not accounts:
        await callback.message.answer("❌ У вас нет активных аккаунтов.")
        await callback.answer()
        return
    
    account = accounts[0]
    
    await callback.message.edit_text("🛡️ Проверяю спамблок...")
    
    try:
        # Получаем клиент
        client = await SessionManager.get_client(account['session_name'])
        if not client:
            await callback.message.edit_text("❌ Не удалось подключиться к аккаунту.")
            await callback.answer()
            return
        
        # Ищем бота @spambot
        try:
            await client.send_message("spambot", "/start")
        except Exception as e:
            logger.error(f"Ошибка отправки /start spambot: {e}")
        
        # Получаем историю сообщений с ботом
        async for message in client.get_chat_history("spambot", limit=5):
            if message.text and ("спам" in message.text.lower() or "spam" in message.text.lower()):
                await callback.message.answer(
                    f"🛡️ Результат проверки спамблока для {account['phone_number']}:\n\n"
                    f"{message.text[:4000]}"
                )
                await callback.answer()
                return
        
        await callback.message.edit_text(
            f"✅ Аккаунт {account['phone_number']} не имеет ограничений спамблока."
        )
        
    except Exception as e:
        logger.error(f"Ошибка проверки спамблока: {e}")
        await callback.message.edit_text(
            f"❌ Ошибка проверки спамблока: {str(e)[:200]}"
        )
    
    await callback.answer()

# ========== ЗАПУСК БОТА ==========
async def main():
    """Основная функция запуска бота"""
    logger.info("Запуск бота Monkey Gram...")
    
    try:
        # Проверяем подключение
        await bot.delete_webhook(drop_pending_updates=True)
        
        # Запускаем поллинг
        await dp.start_polling(bot)
        
    except Exception as e:
        logger.error(f"Ошибка запуска бота: {e}")
    finally:
        # Закрываем все сессии Pyrogram
        await SessionManager.close_all()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
