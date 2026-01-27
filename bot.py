import asyncio
import logging
import re
import os
import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional, Set, Any
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
            CREATE TABLE IF NOT EXISTS folders (
                folder_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS folder_chats (
                folder_id INTEGER,
                chat_id INTEGER,
                title TEXT,
                username TEXT,
                type TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (folder_id, chat_id),
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
    
    # ====== ПАПКИ ======
    def create_folder(self, user_id: int, name: str):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO folders (user_id, name) 
                VALUES (?, ?)
            ''', (user_id, name))
            self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка создания папки: {e}")
            return None
    
    def get_user_folders(self, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT folder_id, name, 
                   (SELECT COUNT(*) FROM folder_chats WHERE folder_id = folders.folder_id) as chat_count
            FROM folders 
            WHERE user_id = ?
            ORDER BY folder_id
        ''', (user_id,))
        return cursor.fetchall()
    
    def get_folder(self, folder_id: int, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT folder_id, name FROM folders 
            WHERE folder_id = ? AND user_id = ?
        ''', (folder_id, user_id))
        return cursor.fetchone()
    
    def delete_folder(self, folder_id: int, user_id: int):
        cursor = self.conn.cursor()
        try:
            # Удаляем чаты из папки
            cursor.execute('DELETE FROM folder_chats WHERE folder_id = ?', (folder_id,))
            # Удаляем папку
            cursor.execute('DELETE FROM folders WHERE folder_id = ? AND user_id = ?', (folder_id, user_id))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка удаления папки: {e}")
            return False
    
    def rename_folder(self, folder_id: int, user_id: int, new_name: str):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                UPDATE folders 
                SET name = ? 
                WHERE folder_id = ? AND user_id = ?
            ''', (new_name, folder_id, user_id))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка переименования папки: {e}")
            return False
    
    # ====== ЧАТЫ В ПАПКАХ ======
    def add_chat_to_folder(self, folder_id: int, chat_data: dict):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO folder_chats 
                (folder_id, chat_id, title, username, type)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                folder_id,
                chat_data['id'],
                chat_data.get('title'),
                chat_data.get('username'),
                chat_data.get('type', 'unknown')
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления чата в папку: {e}")
            return False
    
    def get_folder_chats(self, folder_id: int):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT chat_id, title, username 
            FROM folder_chats 
            WHERE folder_id = ?
            ORDER BY title
        ''', (folder_id,))
        return cursor.fetchall()
    
    def remove_chat_from_folder(self, folder_id: int, chat_id: int):
        cursor = self.conn.cursor()
        try:
            cursor.execute('DELETE FROM folder_chats WHERE folder_id = ? AND chat_id = ?', (folder_id, chat_id))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка удаления чата из папки: {e}")
            return False
    
    def get_folder_chat_count(self, folder_id: int):
        cursor = self.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM folder_chats WHERE folder_id = ?', (folder_id,))
        result = cursor.fetchone()
        return result[0] if result else 0

db = Database()

# ====== СОСТОЯНИЯ FSM ======
class Form(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()
    waiting_for_message_count = State()
    waiting_for_delay = State()
    waiting_for_message_text = State()
    waiting_for_folder_number = State()
    waiting_for_folder_name = State()
    waiting_for_new_folder_name = State()

# ====== ХРАНИЛИЩА ======
active_tasks: Dict[int, List[asyncio.Task]] = {}
check_catchers: Dict[int, Dict[int, bool]] = {}
mass_send_data: Dict[int, Dict] = {}
selected_accounts_for_mass: Dict[int, List[int]] = {}
user_clients: Dict[int, Any] = {}  # Хранение клиентов Pyrogram по user_id
temp_folders_data: Dict[int, Dict] = {}  # Временные данные для работы с папками

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
        [InlineKeyboardButton(text="📨 Рассылка", callback_data="mass_send")],
        [InlineKeyboardButton(text="📁 Папки с чатами", callback_data="folders_menu")],
        [InlineKeyboardButton(text="💰 Ловец чеков", callback_data="check_catcher_menu")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

def get_accounts_menu(user_id: int, mode: str = "view"):
    """mode: view, delete, mass_send"""
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

def get_folders_menu(user_id: int):
    folders = db.get_user_folders(user_id)
    if not folders:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📁 Создать папку", callback_data="create_folder")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")]
        ])
    
    keyboard = []
    for folder in folders:
        folder_id, name, chat_count = folder
        keyboard.append([
            InlineKeyboardButton(
                text=f"📁 {name} ({chat_count} чатов)",
                callback_data=f"folder_view_{folder_id}"
            )
        ])
    
    keyboard.append([
        InlineKeyboardButton(text="📁 Создать папку", callback_data="create_folder"),
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_folder_actions_menu(folder_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить чаты", callback_data=f"folder_add_chats_{folder_id}")],
        [InlineKeyboardButton(text="👁️ Просмотреть чаты", callback_data=f"folder_view_chats_{folder_id}")],
        [InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"folder_rename_{folder_id}")],
        [InlineKeyboardButton(text="🗑️ Удалить папку", callback_data=f"folder_delete_{folder_id}")],
        [InlineKeyboardButton(text="⬅️ Назад к папкам", callback_data="folders_menu")]
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
• 📨 Массовая рассылка по папкам
• 📁 Организация чатов в папки
• 💰 Автоматический сбор чеков
• 👁️ Мониторинг чатов

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
    """Обработчик кнопки 'Добавить аккаунт'"""
    try:
        await callback.message.edit_text(
            "📱 *Добавление аккаунта*\n\n"
            "Пришлите номер телефона в международном формате:\n"
            "Пример: +79123456789",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_back_button()
        )
        
        await state.set_state(Form.waiting_for_phone)
        
    except Exception as e:
        logger.error(f"Ошибка в start_add_account: {e}")
        await callback.answer(f"Ошибка: {str(e)[:50]}")
    
    await callback.answer()

@dp.message(Form.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    """Обработка номера телефона"""
    try:
        phone = message.text.strip()
        
        # Проверка формата номера
        if not re.match(r'^\+\d{10,15}$', phone):
            await message.answer(
                "❌ Неверный формат номера.\n"
                "Используйте международный формат: +79123456789\n"
                "Попробуйте еще раз:",
                reply_markup=get_back_button()
            )
            return
        
        await state.update_data(phone=phone)
        
        # Создаем клиент Pyrogram
        session_name = f"session_{message.from_user.id}_{int(datetime.now().timestamp())}"
        client = Client(
            name=session_name,
            api_id=API_ID,
            api_hash=API_HASH,
            workdir="sessions"
        )
        
        await client.connect()
        
        # Отправляем код
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
    """Обработка кода подтверждения"""
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
    """Обработка пароля 2FA"""
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
    """Завершение авторизации и сохранение аккаунта"""
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

# ====== ПАПКИ С ЧАТАМИ ======
@dp.callback_query(F.data == "folders_menu")
async def show_folders_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folders = db.get_user_folders(user_id)
    
    if not folders:
        await callback.message.edit_text(
            "📁 *У вас пока нет папок с чатами.*\n\n"
            "Создайте папку, чтобы организовать чаты для рассылки.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_folders_menu(user_id)
        )
    else:
        text = "📁 *Ваши папки с чатами:*\n\n"
        for folder in folders:
            folder_id, name, chat_count = folder
            text += f"• *Папка {folder_id}: {name}*\n"
            text += f"  📊 Чатов: {chat_count}\n\n"
        
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_folders_menu(user_id)
        )
    await callback.answer()

@dp.callback_query(F.data == "create_folder")
async def create_folder_prompt(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📁 *Создание новой папки*\n\n"
        "Введите название для новой папки:",
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
            "❌ Неверное название. Название не должно быть пустым и не более 50 символов.\n"
            "Введите название папки:",
            reply_markup=get_back_button()
        )
        return
    
    folder_id = db.create_folder(user_id, folder_name)
    
    if folder_id:
        await message.answer(
            f"✅ *Папка создана!*\n\n"
            f"*Название:* {folder_name}\n"
            f"*ID папки:* `{folder_id}`\n\n"
            f"Теперь вы можете добавить чаты в эту папку.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить чаты", callback_data=f"folder_add_chats_{folder_id}")],
                [InlineKeyboardButton(text="⬅️ К списку папок", callback_data="folders_menu")]
            ])
        )
    else:
        await message.answer(
            "❌ Ошибка при создании папки.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_folders_menu(user_id)
        )
    
    await state.clear()

@dp.callback_query(F.data.startswith("folder_view_"))
async def view_folder(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[2])
    
    folder = db.get_folder(folder_id, user_id)
    if not folder:
        await callback.answer("❌ Папка не найдена")
        return
    
    folder_id, name = folder
    chat_count = db.get_folder_chat_count(folder_id)
    
    text = (
        f"📁 *Папка {folder_id}: {name}*\n\n"
        f"📊 *Количество чатов:* {chat_count}\n\n"
        f"*Выберите действие:*"
    )
    
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_folder_actions_menu(folder_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("folder_view_chats_"))
async def view_folder_chats(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[3])
    
    folder = db.get_folder(folder_id, user_id)
    if not folder:
        await callback.answer("❌ Папка не найдена")
        return
    
    chats = db.get_folder_chats(folder_id)
    folder_id, name = folder
    
    if not chats:
        text = f"📁 *Папка {folder_id}: {name}*\n\n*В этой папке пока нет чатов.*"
    else:
        text = f"📁 *Папка {folder_id}: {name}*\n\n*Чаты в папке ({len(chats)}):*\n\n"
        for i, chat in enumerate(chats, 1):
            chat_id, title, username = chat
            display_name = title or username or f"Chat {chat_id}"
            text += f"{i}. *{display_name}*\n"
            if username:
                text += f"   @{username}\n"
            text += f"   🆔 `{chat_id}`\n\n"
    
    await callback.message.edit_text(
        text[:4000],  # Ограничение Telegram
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад к папке", callback_data=f"folder_view_{folder_id}")]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("folder_add_chats_"))
async def add_chats_to_folder(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[3])
    
    folder = db.get_folder(folder_id, user_id)
    if not folder:
        await callback.answer("❌ Папка не найдена")
        return
    
    accounts = db.get_user_accounts(user_id)
    if not accounts:
        await callback.message.edit_text(
            "❌ *Сначала добавьте аккаунты!*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        await callback.answer()
        return
    
    text = "📱 *Выберите аккаунт для получения чатов:*\n\n"
    keyboard = []
    
    for acc in accounts:
        account_id, phone, first_name, username = acc
        display_name = f"{first_name or ''} {username or ''}".strip() or phone[:10]
        keyboard.append([
            InlineKeyboardButton(
                text=f"📱 {display_name}",
                callback_data=f"get_folder_chats_{folder_id}_{account_id}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад к папке", callback_data=f"folder_view_{folder_id}")])
    
    await callback.message.edit_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("get_folder_chats_"))
async def get_account_chats_for_folder(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    parts = callback.data.split("_")
    folder_id = int(parts[3])
    account_id = int(parts[4])
    
    session_string = db.get_account_session(account_id, user_id)
    if not session_string:
        await callback.answer("❌ Сессия не найдена")
        return
    
    await callback.message.edit_text(
        "⏳ *Получаю список чатов...*\n\n"
        "Это может занять несколько секунд.",
        parse_mode=ParseMode.MARKDOWN
    )
    
    asyncio.create_task(fetch_and_add_chats_to_folder(user_id, folder_id, account_id, session_string, callback.message))
    await callback.answer()

async def fetch_and_add_chats_to_folder(user_id: int, folder_id: int, account_id: int, session_string: str, message: types.Message):
    """Получение и добавление чатов в папку"""
    client = Client(
        name=f"folder_chats_{user_id}_{folder_id}_{account_id}",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_string,
        workdir="sessions"
    )
    
    try:
        await client.start()
        
        # Получаем диалоги
        chats = []
        async for dialog in client.get_dialogs():
            if dialog.chat:
                chat = dialog.chat
                if hasattr(chat, 'id') and chat.id != user_id:
                    chats.append({
                        'id': chat.id,
                        'title': getattr(chat, 'title', None),
                        'username': getattr(chat, 'username', None),
                        'type': str(chat.type)
                    })
        
        if not chats:
            await message.edit_text(
                "❌ *Не найдено чатов для добавления.*\n\n"
                "Убедитесь, что аккаунт состоит в группах или каналах.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад к папке", callback_data=f"folder_view_{folder_id}")]
                ])
            )
            return
        
        # Добавляем все чаты в папку
        added_count = 0
        for chat in chats:
            if db.add_chat_to_folder(folder_id, chat):
                added_count += 1
        
        await message.edit_text(
            f"✅ *Чаты добавлены в папку!*\n\n"
            f"Добавлено чатов: *{added_count}*\n"
            f"Все чаты успешно добавлены в папку.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад к папке", callback_data=f"folder_view_{folder_id}")]
            ])
        )
        
    except Exception as e:
        logger.error(f"Ошибка получения чатов: {e}")
        await message.edit_text(
            f"❌ *Ошибка получения чатов:*\n\n`{str(e)[:200]}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад к папке", callback_data=f"folder_view_{folder_id}")]
            ])
        )
    finally:
        try:
            await client.stop()
        except:
            pass

@dp.callback_query(F.data.startswith("folder_rename_"))
async def rename_folder_prompt(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[2])
    
    folder = db.get_folder(folder_id, user_id)
    if not folder:
        await callback.answer("❌ Папка не найдена")
        return
    
    await state.update_data(folder_id=folder_id)
    
    await callback.message.edit_text(
        f"✏️ *Переименование папки*\n\n"
        f"Текущее название: *{folder[1]}*\n\n"
        f"Введите новое название для папки:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_button()
    )
    await state.set_state(Form.waiting_for_new_folder_name)
    await callback.answer()

@dp.message(Form.waiting_for_new_folder_name)
async def process_new_folder_name(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    folder_id = data.get('folder_id')
    
    if not folder_id:
        await message.answer(
            "❌ Ошибка данных. Начните заново.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_folders_menu(user_id)
        )
        await state.clear()
        return
    
    new_name = message.text.strip()
    
    if not new_name or len(new_name) > 50:
        await message.answer(
            "❌ Неверное название. Название не должно быть пустым и не более 50 символов.\n"
            "Введите новое название папки:",
            reply_markup=get_back_button()
        )
        return
    
    success = db.rename_folder(folder_id, user_id, new_name)
    
    if success:
        await message.answer(
            f"✅ *Папка переименована!*\n\n"
            f"Новое название: *{new_name}*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад к папке", callback_data=f"folder_view_{folder_id}")]
            ])
        )
    else:
        await message.answer(
            "❌ Ошибка при переименовании папки.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_folders_menu(user_id)
        )
    
    await state.clear()

@dp.callback_query(F.data.startswith("folder_delete_"))
async def delete_folder_prompt(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[2])
    
    folder = db.get_folder(folder_id, user_id)
    if not folder:
        await callback.answer("❌ Папка не найдена")
        return
    
    chat_count = db.get_folder_chat_count(folder_id)
    
    await callback.message.edit_text(
        f"🗑️ *Удаление папки*\n\n"
        f"*Папка:* {folder[1]}\n"
        f"*ID папки:* {folder_id}\n"
        f"*Чатов в папке:* {chat_count}\n\n"
        f"Вы уверены, что хотите удалить эту папку?",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_delete_folder_{folder_id}"),
                InlineKeyboardButton(text="❌ Нет", callback_data=f"folder_view_{folder_id}")
            ]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("confirm_delete_folder_"))
async def confirm_delete_folder(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    folder_id = int(callback.data.split("_")[3])
    
    success = db.delete_folder(folder_id, user_id)
    
    if success:
        await callback.message.edit_text(
            "✅ *Папка успешно удалена!*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_folders_menu(user_id)
        )
    else:
        await callback.message.edit_text(
            "❌ *Ошибка при удалении папки*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_folders_menu(user_id)
        )
    await callback.answer()

# ====== РАССЫЛКА ПО ПАПКАМ ======
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
    
    # Инициализируем данные для рассылки
    mass_send_data[user_id] = {
        'count': None,
        'delay': None,
        'text': None,
        'folder_id': None
    }
    
    # Инициализируем список выбранных аккаунтов
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
    
    # Показываем меню выбора аккаунтов
    await message.answer(
        f"✅ *Шаг 3/5 завершен*\n"
        f"Текст сообщения сохранен\n\n"
        f"*Шаг 4/5:* Выберите аккаунты для рассылки:\n\n"
        f"✅ - выбран\n⬜ - не выбран\n\n"
        f"Нажмите на аккаунт, чтобы выбрать/отменить выбор",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_accounts_menu(user_id, "mass_send")
    )
    await state.clear()

@dp.callback_query(F.data.startswith("mass_select_"))
async def select_account_for_mass(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    account_id = int(callback.data.split("_")[2])
    
    if user_id not in selected_accounts_for_mass:
        selected_accounts_for_mass[user_id] = []
    
    if account_id in selected_accounts_for_mass[user_id]:
        selected_accounts_for_mass[user_id].remove(account_id)
        action = "удален"
    else:
        selected_accounts_for_mass[user_id].append(account_id)
        action = "добавлен"
    
    await callback.message.edit_reply_markup(
        reply_markup=get_accounts_menu(user_id, "mass_send")
    )
    
    accounts_info = db.get_user_accounts(user_id)
    account_name = "Неизвестно"
    for acc in accounts_info:
        if acc[0] == account_id:
            account_name = f"{acc[2] or ''} {acc[3] or ''}".strip() or acc[1][:10]
            break
    
    await callback.answer(f"Аккаунт {account_name} {action}")

@dp.callback_query(F.data == "mass_next_step")
async def mass_next_step(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    # Проверяем выбранные аккаунты
    if not selected_accounts_for_mass.get(user_id):
        await callback.answer("❌ Выберите хотя бы один аккаунт")
        return
    
    # Получаем список папок пользователя
    folders = db.get_user_folders(user_id)
    
    if not folders:
        await callback.message.edit_text(
            "❌ *У вас нет папок с чатами!*\n\n"
            "Сначала создайте папку и добавьте в неё чаты.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📁 Создать папку", callback_data="create_folder")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="mass_send")]
            ])
        )
    else:
        text = "📁 *Выберите папку для рассылки:*\n\n"
        for folder in folders:
            folder_id, name, chat_count = folder
            text += f"*Папка {folder_id}:* {name} ({chat_count} чатов)\n"
        
        text += "\n*Шаг 5/5:* Введите номер папки для рассылки:"
        
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_back_button()
        )
        await state.set_state(Form.waiting_for_folder_number)
    
    await callback.answer()

@dp.message(Form.waiting_for_folder_number)
async def process_folder_number(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    try:
        folder_id = int(message.text.strip())
        
        # Проверяем существование папки
        folder = db.get_folder(folder_id, user_id)
        if not folder:
            await message.answer(
                f"❌ Папка с номером {folder_id} не найдена.\n"
                f"Введите номер существующей папки:",
                reply_markup=get_back_button()
            )
            return
        
        # Проверяем что в папке есть чаты
        chat_count = db.get_folder_chat_count(folder_id)
        if chat_count == 0:
            await message.answer(
                f"❌ В папке '{folder[1]}' нет чатов.\n"
                f"Сначала добавьте чаты в папку.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="➕ Добавить чаты", callback_data=f"folder_add_chats_{folder_id}")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="mass_send")]
                ])
            )
            await state.clear()
            return
        
        # Сохраняем ID папки
        mass_send_data[user_id]['folder_id'] = folder_id
        
        # Показываем сводку
        data = mass_send_data[user_id]
        accounts = selected_accounts_for_mass[user_id]
        
        total_messages = data['count'] * len(accounts) * chat_count
        
        summary = (
            f"📋 *Сводка рассылки:*\n\n"
            f"• Папка: *{folder[1]}* (ID: {folder_id})\n"
            f"• Чатов в папке: *{chat_count}*\n"
            f"• Аккаунтов: *{len(accounts)}*\n"
            f"• Сообщений на аккаунт в чат: *{data['count']}*\n"
            f"• Всего сообщений: *{total_messages}*\n"
            f"• Задержка: *{data['delay']}* сек\n"
            f"• Примерное время: *{total_messages * data['delay'] / 60:.1f}* мин\n"
            f"• Текст: *{data['text'][:50]}...*\n\n"
            f"*Начать рассылку?*"
        )
        
        await message.answer(
            summary,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Начать", callback_data="mass_send_confirm"),
                    InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_mass_send")
                ]
            ])
        )
        
        await state.clear()
        
    except ValueError:
        await message.answer(
            "❌ Неверный номер папки. Введите целое число:",
            reply_markup=get_back_button()
        )

@dp.callback_query(F.data == "mass_send_confirm")
async def confirm_mass_send(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    # Запускаем реальную рассылку в фоне
    asyncio.create_task(run_real_mass_send(user_id, callback.message))
    await callback.answer("Рассылка запущена!")

async def run_real_mass_send(user_id: int, message: types.Message):
    """Реальная фоновая задача рассылки по папке"""
    try:
        data = mass_send_data[user_id]
        accounts = selected_accounts_for_mass[user_id]
        folder_id = data['folder_id']
        
        if not folder_id:
            await message.edit_text(
                "❌ Ошибка: не указана папка для рассылки.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_functions_menu()
            )
            return
        
        # Получаем чаты из папки
        chats = db.get_folder_chats(folder_id)
        if not chats:
            await message.edit_text(
                "❌ Ошибка: в папке нет чатов.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_functions_menu()
            )
            return
        
        chat_ids = [chat[0] for chat in chats]  # Извлекаем только ID чатов
        
        total_messages = data['count'] * len(accounts) * len(chat_ids)
        progress_msg = await message.edit_text(
            f"🚀 *Рассылка начата!*\n\n"
            f"Папка: *{data['folder_id']}*\n"
            f"Чатов: *{len(chat_ids)}*\n"
            f"Аккаунтов: *{len(accounts)}*\n"
            f"Всего сообщений: *{total_messages}*\n"
            f"Прогресс: 0/{total_messages} (0%)\n"
            f"Статус: Инициализация...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        sent_count = 0
        failed_count = 0
        
        # Для каждого аккаунта
        for account_index, account_id in enumerate(accounts):
            session_string = db.get_account_session(account_id, user_id)
            if not session_string:
                logger.error(f"Сессия не найдена для account_id={account_id}")
                failed_count += data['count'] * len(chat_ids)
                continue
            
            # Создаем клиент для аккаунта
            client = Client(
                name=f"mass_sender_{user_id}_{account_id}_{int(datetime.now().timestamp())}",
                api_id=API_ID,
                api_hash=API_HASH,
                session_string=session_string,
                workdir="sessions"
            )
            
            try:
                await client.start()
                
                # Для каждого чата в папке
                for chat_index, chat_id in enumerate(chat_ids):
                    # Для каждого сообщения в чате
                    for message_num in range(data['count']):
                        try:
                            # Отправляем сообщение
                            await client.send_message(
                                chat_id=chat_id,
                                text=data['text']
                            )
                            
                            sent_count += 1
                            
                            # Обновляем прогресс каждые 10 сообщений или 10%
                            if sent_count % 10 == 0 or sent_count % max(1, total_messages // 10) == 0:
                                percent = (sent_count / total_messages) * 100
                                await progress_msg.edit_text(
                                    f"🚀 *Рассылка в процессе...*\n\n"
                                    f"Папка: *{data['folder_id']}*\n"
                                    f"Прогресс: {sent_count}/{total_messages} ({percent:.1f}%)\n"
                                    f"Аккаунт: {account_index+1}/{len(accounts)}\n"
                                    f"Чат: {chat_index+1}/{len(chat_ids)}\n"
                                    f"Сообщение: {message_num+1}/{data['count']}\n"
                                    f"Ошибок: {failed_count}",
                                    parse_mode=ParseMode.MARKDOWN
                                )
                            
                            # Задержка между сообщениями
                            if message_num < data['count'] - 1 or chat_index < len(chat_ids) - 1:
                                await asyncio.sleep(data['delay'])
                                
                        except (PeerIdInvalid, ChannelInvalid, ChatAdminRequired, FloodWait) as e:
                            failed_count += 1
                            logger.error(f"Ошибка отправки в чат {chat_id}: {e}")
                            # Пропускаем этот чат
                            break
                        except Exception as e:
                            failed_count += 1
                            logger.error(f"Ошибка отправки: {e}")
                            # Продолжаем с другим сообщением
                            continue
                
                # Задержка между аккаунтами (чтобы избежать флуда)
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Ошибка аккаунта {account_id}: {e}")
                failed_count += data['count'] * len(chat_ids)
            finally:
                try:
                    await client.stop()
                except:
                    pass
        
        # Итоговый отчет
        success_rate = (sent_count / total_messages * 100) if total_messages > 0 else 0
        
        await progress_msg.edit_text(
            f"✅ *Рассылка завершена!*\n\n"
            f"• Папка: *{data['folder_id']}*\n"
            f"• Успешно отправлено: *{sent_count}* сообщений\n"
            f"• Не отправлено: *{failed_count}* сообщений\n"
            f"• Успешность: *{success_rate:.1f}%*\n"
            f"• Задействовано аккаунтов: *{len(accounts)}*\n"
            f"• Чатов в папке: *{len(chat_ids)}*\n"
            f"• Время выполнения: *{(sent_count + failed_count) * data['delay']:.1f}* сек\n\n"
            f"Рассылка по папке завершена.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
        
    except Exception as e:
        logger.error(f"Ошибка рассылки: {e}")
        await message.edit_text(
            f"❌ *Критическая ошибка рассылки!*\n\n"
            f"Ошибка: {str(e)[:200]}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
    finally:
        # Очищаем данные
        if user_id in mass_send_data:
            del mass_send_data[user_id]
        if user_id in selected_accounts_for_mass:
            del selected_accounts_for_mass[user_id]

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
            "💰 *Ловец чеков*\n\n"
            "✅ Вкл - мониторинг включен\n"
            "❌ Выкл - мониторинг выключен\n\n"
            "Нажмите на аккаунт для переключения статуса:",
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
    
    await callback.answer(f"Мониторинг {'включен' if new_state else 'выключен'}!")

# ====== ЗАПУСК БОТА ======
async def main():
    logger.info("Запуск бота Monkey Gram...")
    
    os.makedirs("sessions", exist_ok=True)
    
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
