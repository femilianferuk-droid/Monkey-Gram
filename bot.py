import asyncio
import logging
import re
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Set
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
            CREATE TABLE IF NOT EXISTS mass_chats (
                chat_id INTEGER PRIMARY KEY,
                user_id INTEGER,
                account_id INTEGER,
                title TEXT,
                username TEXT,
                type TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                FOREIGN KEY (account_id) REFERENCES accounts (account_id)
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
    
    def add_chat(self, user_id: int, account_id: int, chat_data: dict):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO mass_chats 
                (chat_id, user_id, account_id, title, username, type)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                chat_data['id'],
                user_id,
                account_id,
                chat_data.get('title'),
                chat_data.get('username'),
                chat_data.get('type', 'unknown')
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления чата: {e}")
            return False
    
    def get_user_chats(self, user_id: int):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT DISTINCT chat_id, title, username 
            FROM mass_chats 
            WHERE user_id = ?
            ORDER BY title
        ''', (user_id,))
        return cursor.fetchall()
    
    def delete_chat(self, chat_id: int, user_id: int):
        cursor = self.conn.cursor()
        try:
            cursor.execute('DELETE FROM mass_chats WHERE chat_id = ? AND user_id = ?', (chat_id, user_id))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка удаления чата: {e}")
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
    waiting_for_account_to_delete = State()
    waiting_for_chats_selection = State()
    adding_chats = State()

# ====== ХРАНИЛИЩА ======
active_tasks: Dict[int, List[asyncio.Task]] = {}
check_catchers: Dict[int, Dict[int, bool]] = {}
mass_send_data: Dict[int, Dict] = {}
selected_accounts_for_mass: Dict[int, List[int]] = {}
selected_chats_for_mass: Dict[int, Set[int]] = {}

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
        [InlineKeyboardButton(text="💬 Мои чаты", callback_data="my_chats")],
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

def get_chats_menu(user_id: int, mode: str = "view"):
    """mode: view, delete, mass_send"""
    chats = db.get_user_chats(user_id)
    if not chats:
        return None
    
    keyboard = []
    for chat in chats:
        chat_id, title, username = chat
        display_name = title or username or f"Chat {chat_id}"
        
        if mode == "delete":
            keyboard.append([
                InlineKeyboardButton(
                    text=f"🗑️ {display_name[:30]}",
                    callback_data=f"chat_delete_{chat_id}"
                )
            ])
        elif mode == "mass_send":
            is_selected = chat_id in selected_chats_for_mass.get(user_id, set())
            icon = "✅" if is_selected else "⬜"
            keyboard.append([
                InlineKeyboardButton(
                    text=f"{icon} {display_name[:30]}",
                    callback_data=f"chat_select_{chat_id}"
                )
            ])
        else:
            keyboard.append([
                InlineKeyboardButton(
                    text=f"💬 {display_name[:30]}",
                    callback_data=f"chat_info_{chat_id}"
                )
            ])
    
    if mode == "delete":
        keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="my_chats")])
    elif mode == "mass_send":
        selected_count = len(selected_chats_for_mass.get(user_id, set()))
        if selected_count > 0:
            keyboard.append([
                InlineKeyboardButton(text=f"🚀 Запуск ({selected_count})", callback_data="start_mass_action"),
                InlineKeyboardButton(text="⬅️ Назад", callback_data="mass_send")
            ])
        else:
            keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="mass_send")])
    else:
        keyboard.append([
            InlineKeyboardButton(text="🗑️ Очистить все", callback_data="clear_all_chats"),
            InlineKeyboardButton(text="➕ Добавить чаты", callback_data="add_chats_menu"),
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
• 📨 Массовая рассылка сообщений
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

# ====== ДОБАВЛЕНИЕ АККАУНТА - ИСПРАВЛЕННЫЙ КОД ======
@dp.callback_query(F.data == "add_account")
async def start_add_account(callback: types.CallbackQuery):
    """Обработчик кнопки 'Добавить аккаунт'"""
    try:
        await callback.message.edit_text(
            "📱 *Добавление аккаунта*\n\n"
            "Пришлите номер телефона в международном формате:\n"
            "Пример: +79123456789",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_back_button()
        )
        
        # Используем state для установки состояния
        from aiogram.fsm.context import FSMContext
        storage = MemoryStorage()
        state = FSMContext(storage, callback.from_user.id, callback.chat.id)
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
        
        # Сохраняем номер в состоянии
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
        
        # Сохраняем данные в состоянии
        await state.update_data(
            client=client,
            phone_code_hash=sent_code.phone_code_hash
        )
        
        await message.answer(
            f"✅ *Номер принят:* `{phone}`\n\n"
            "📱 *Код отправлен на номер*\n\n"
            "Пришлите код из Telegram (5 цифр):",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_back_button()
        )
        
        # Меняем состояние на ожидание кода
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
    except Exception as e:
        logger.error(f"Ошибка отправки кода: {e}")
        await message.answer(
            f"❌ Ошибка: {str(e)[:100]}",
            reply_markup=get_back_button()
        )
        try:
            if 'client' in locals():
                await client.disconnect()
        except:
            pass

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
        
        # Получаем данные из состояния
        data = await state.get_data()
        client = data.get('client')
        phone = data.get('phone')
        phone_code_hash = data.get('phone_code_hash')
        
        if not all([client, phone, phone_code_hash]):
            await message.answer(
                "❌ Ошибка данных. Начните заново.",
                reply_markup=get_functions_menu()
            )
            await state.clear()
            return
        
        # Пытаемся войти
        await client.sign_in(
            phone_number=phone,
            phone_code_hash=phone_code_hash,
            phone_code=code
        )
        
        # Успешный вход
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
        try:
            if 'client' in locals():
                await client.disconnect()
        except:
            pass
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
            if 'client' in locals():
                await client.disconnect()
        except:
            pass
        await state.clear()

async def finish_authorization(client: Client, phone: str, message: types.Message, state: FSMContext):
    """Завершение авторизации и сохранение аккаунта"""
    try:
        # Получаем информацию о пользователе
        user_data = await client.get_me()
        
        # Сохраняем сессию
        session_string = await client.export_session_string()
        
        # Сохраняем в БД
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
        
        # Формируем ответ
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

# ====== УДАЛЕНИЕ АККАУНТА ======
@dp.callback_query(F.data == "delete_account_menu")
async def show_delete_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    accounts = db.get_user_accounts(user_id)
    
    if not accounts:
        await callback.answer("❌ У вас нет аккаунтов для удаления")
        return
    
    await callback.message.edit_text(
        "🗑️ *Выберите аккаунт для удаления:*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_accounts_menu(user_id, "delete")
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_confirm_"))
async def confirm_delete_account(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    account_id = int(callback.data.split("_")[2])
    
    # Получаем информацию об аккаунте
    accounts = db.get_user_accounts(user_id)
    account_info = None
    for acc in accounts:
        if acc[0] == account_id:
            account_info = acc
            break
    
    if not account_info:
        await callback.answer("❌ Аккаунт не найден")
        return
    
    _, phone, first_name, username = account_info
    display_name = f"{first_name or ''} {username or ''}".strip() or phone
    
    await callback.message.edit_text(
        f"❓ *Подтвердите удаление аккаунта:*\n\n"
        f"*Аккаунт:* {display_name}\n"
        f"*Телефон:* `{phone}`\n\n"
        f"Вы уверены, что хотите удалить этот аккаунт?",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_yes_no_keyboard()
    )
    
    # Сохраняем ID аккаунта во временном хранилище
    from aiogram.fsm.context import FSMContext
    storage = MemoryStorage()
    state = FSMContext(storage, callback.from_user.id, callback.chat.id)
    await state.update_data(account_to_delete=account_id)
    await callback.answer()

@dp.callback_query(F.data == "confirm_yes")
async def delete_account_yes(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    # Получаем сохраненный ID аккаунта
    from aiogram.fsm.context import FSMContext
    storage = MemoryStorage()
    state = FSMContext(storage, callback.from_user.id, callback.chat.id)
    data = await state.get_data()
    account_id = data.get('account_to_delete')
    
    if not account_id:
        await callback.answer("❌ Ошибка: аккаунт не найден")
        return
    
    # Удаляем аккаунт
    success = db.delete_account(account_id, user_id)
    
    if success:
        # Удаляем из активных ловцов
        if user_id in check_catchers and account_id in check_catchers[user_id]:
            del check_catchers[user_id][account_id]
        
        # Удаляем из выбранных для рассылки
        if user_id in selected_accounts_for_mass and account_id in selected_accounts_for_mass[user_id]:
            selected_accounts_for_mass[user_id].remove(account_id)
        
        await callback.message.edit_text(
            "✅ *Аккаунт успешно удален!*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
    else:
        await callback.message.edit_text(
            "❌ *Ошибка при удалении аккаунта*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_functions_menu()
        )
    
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data == "confirm_no")
async def delete_account_no(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "❌ *Удаление отменено*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_functions_menu()
    )
    await callback.answer()

# ====== ЧАТЫ (остальной код из предыдущего ответа) ======
@dp.callback_query(F.data == "my_chats")
async def show_my_chats(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    chats = db.get_user_chats(user_id)
    
    if not chats:
        await callback.message.edit_text(
            "💬 *У вас нет добавленных чатов.*\n\n"
            "Добавьте чаты для рассылки сообщений.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить чаты", callback_data="add_chats_menu")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")]
            ])
        )
    else:
        text = f"💬 *Ваши чаты ({len(chats)}):*\n\n"
        for chat in chats:
            chat_id, title, username = chat
            display_name = title or username or f"Chat {chat_id}"
            text += f"• *{display_name}*\n"
            if username:
                text += f"  @{username}\n"
            text += f"  🆔 `{chat_id}`\n\n"
        
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_chats_menu(user_id, "view")
        )
    await callback.answer()

# ====== РАССЫЛКА ======
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
    
    # Инициализируем данные для рассылки
    mass_send_data[user_id] = {
        'count': None,
        'delay': None,
        'text': None
    }
    
    # Инициализируем списки выбранных аккаунтов и чатов
    selected_accounts_for_mass[user_id] = []
    selected_chats_for_mass[user_id] = set()
    
    await callback.message.edit_text(
        "📨 *Настройка рассылки - Шаг 1/4*\n\n"
        "Пришлите количество сообщений для отправки (1-1000):",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_button()
    )
    
    # Устанавливаем состояние
    from aiogram.fsm.context import FSMContext
    storage = MemoryStorage()
    state = FSMContext(storage, callback.from_user.id, callback.chat.id)
    await state.set_state(Form.waiting_for_message_count)
    
    await callback.answer()

# Обработка сообщений для рассылки
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
            f"✅ *Шаг 2/4 завершен*\n"
            f"Задержка: *{delay}* секунд\n\n"
            f"*Шаг 3/4:* Пришлите текст сообщения для рассылки:",
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
        f"✅ *Шаг 3/4 завершен*\n"
        f"Текст сообщения сохранен\n\n"
        f"*Шаг 4/4:* Выберите аккаунты для рассылки:\n\n"
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
    
    # Добавляем или удаляем аккаунт из выбранных
    if account_id in selected_accounts_for_mass[user_id]:
        selected_accounts_for_mass[user_id].remove(account_id)
        action = "удален"
    else:
        selected_accounts_for_mass[user_id].append(account_id)
        action = "добавлен"
    
    # Обновляем меню
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
async def mass_next_step(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    # Проверяем выбранные аккаунты
    if not selected_accounts_for_mass.get(user_id):
        await callback.answer("❌ Выберите хотя бы один аккаунт")
        return
    
    # Показываем выбор чатов
    chats = db.get_user_chats(user_id)
    
    if not chats:
        await callback.message.edit_text(
            "❌ *У вас нет добавленных чатов!*\n\n"
            "Сначала добавьте чаты для рассылки.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить чаты", callback_data="add_chats_menu")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="mass_send")]
            ])
        )
    else:
        selected_count = len(selected_accounts_for_mass[user_id])
        await callback.message.edit_text(
            f"✅ *Аккаунты выбраны: {selected_count}*\n\n"
            f"*Выберите чаты для рассылки:*\n\n"
            f"✅ - выбран\n⬜ - не выбран\n\n"
            f"Нажмите на чат, чтобы выбрать/отменить выбор",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_chats_menu(user_id, "mass_send")
        )
    
    await callback.answer()

@dp.callback_query(F.data.startswith("chat_select_"))
async def select_chat_for_mass(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    chat_id = int(callback.data.split("_")[2])
    
    if user_id not in selected_chats_for_mass:
        selected_chats_for_mass[user_id] = set()
    
    # Добавляем или удаляем чат из выбранных
    if chat_id in selected_chats_for_mass[user_id]:
        selected_chats_for_mass[user_id].remove(chat_id)
        action = "удален"
    else:
        selected_chats_for_mass[user_id].add(chat_id)
        action = "добавлен"
    
    # Обновляем меню
    await callback.message.edit_reply_markup(
        reply_markup=get_chats_menu(user_id, "mass_send")
    )
    
    # Получаем имя чата
    chats = db.get_user_chats(user_id)
    chat_name = "Неизвестно"
    for chat in chats:
        if chat[0] == chat_id:
            chat_name = chat[1] or chat[2] or f"Chat {chat_id}"
            break
    
    await callback.answer(f"Чат '{chat_name[:20]}' {action}")

@dp.callback_query(F.data == "start_mass_action")
async def start_mass_send_process(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    # Проверяем все данные
    if (user_id not in mass_send_data or 
        not selected_accounts_for_mass.get(user_id) or 
        not selected_chats_for_mass.get(user_id)):
        await callback.answer("❌ Не все параметры заполнены")
        return
    
    data = mass_send_data[user_id]
    accounts = selected_accounts_for_mass[user_id]
    chats = list(selected_chats_for_mass[user_id])
    
    # Показываем сводку
    total_messages = data['count'] * len(accounts) * len(chats)
    
    summary = (
        f"📋 *Сводка рассылки:*\n\n"
        f"• Аккаунтов: *{len(accounts)}*\n"
        f"• Чатов: *{len(chats)}*\n"
        f"• Сообщений на аккаунт в чат: *{data['count']}*\n"
        f"• Всего сообщений: *{total_messages}*\n"
        f"• Задержка: *{data['delay']}* сек\n"
        f"• Примерное время: *{total_messages * data['delay'] / 60:.1f}* мин\n"
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
    asyncio.create_task(run_real_mass_send(user_id, callback.message))
    await callback.answer("Рассылка запущена!")

async def run_real_mass_send(user_id: int, message: types.Message):
    """Реальная фоновая задача рассылки"""
    try:
        data = mass_send_data[user_id]
        accounts = selected_accounts_for_mass[user_id]
        chat_ids = list(selected_chats_for_mass[user_id])
        
        total_messages = data['count'] * len(accounts) * len(chat_ids)
        progress_msg = await message.edit_text(
            f"🚀 *Рассылка начата!*\n\n"
            f"Прогресс: 0/{total_messages} (0%)\n"
            f"Аккаунтов: {len(accounts)}\n"
            f"Чатов: {len(chat_ids)}\n"
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
                
                # Для каждого чата
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
            f"• Успешно отправлено: *{sent_count}* сообщений\n"
            f"• Не отправлено: *{failed_count}* сообщений\n"
            f"• Успешность: *{success_rate:.1f}%*\n"
            f"• Задействовано аккаунтов: *{len(accounts)}*\n"
            f"• Чатов: *{len(chat_ids)}*\n"
            f"• Время выполнения: *{(sent_count + failed_count) * data['delay']:.1f}* сек\n\n"
            f"Рассылка завершена.",
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
        if user_id in selected_chats_for_mass:
            del selected_chats_for_mass[user_id]

# ====== ОТМЕНА ДЕЙСТВИЙ ======
@dp.callback_query(F.data == "cancel_action")
async def cancel_action(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    # Очищаем состояние
    from aiogram.fsm.context import FSMContext
    storage = MemoryStorage()
    state = FSMContext(storage, callback.from_user.id, callback.chat.id)
    await state.clear()
    
    # Очищаем данные рассылки если есть
    if user_id in mass_send_data:
        del mass_send_data[user_id]
    if user_id in selected_accounts_for_mass:
        del selected_accounts_for_mass[user_id]
    if user_id in selected_chats_for_mass:
        del selected_chats_for_mass[user_id]
    
    await callback.message.edit_text(
        "❌ Действие отменено.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_functions_menu()
    )
    await callback.answer()

@dp.callback_query(F.data == "cancel_mass_send")
async def cancel_mass_send(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    # Очищаем данные
    if user_id in mass_send_data:
        del mass_send_data[user_id]
    if user_id in selected_accounts_for_mass:
        del selected_accounts_for_mass[user_id]
    if user_id in selected_chats_for_mass:
        del selected_chats_for_mass[user_id]
    
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
    
    # Обновляем меню
    menu = get_check_catcher_menu(user_id)
    if menu:
        await callback.message.edit_reply_markup(reply_markup=menu)
    
    await callback.answer(f"Мониторинг {'включен' if new_state else 'выключен'}!")

# ====== ЗАПУСК БОТА ======
async def main():
    logger.info("Запуск бота Monkey Gram...")
    
    # Создаем папку для сессий
    os.makedirs("sessions", exist_ok=True)
    
    # Запускаем бота
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
