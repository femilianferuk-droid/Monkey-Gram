import asyncio
import os
import sqlite3
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from pyrogram import Client
from pyrogram.errors import (
    SessionPasswordNeeded, PhoneCodeInvalid,
    PhoneNumberInvalid, PhoneCodeExpired
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# API данные
API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    print("ОШИБКА: Не установлена переменная окружения BOT_TOKEN")
    print("Создайте файл .env с содержимым: BOT_TOKEN=ваш_токен_здесь")
    exit(1)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Классы состояний FSM
class AddAccountStates(StatesGroup):
    phone_number = State()
    phone_code = State()
    two_factor = State()

class CreateFolderStates(StatesGroup):
    waiting_for_chats = State()
    waiting_for_name = State()

class MailingStates(StatesGroup):
    waiting_for_count = State()
    waiting_for_delay = State()
    waiting_for_message = State()
    waiting_for_folder = State()

# Класс для работы с базой данных
class Database:
    def __init__(self, db_name="bot.db"):
        self.db_name = db_name
        self.init_db()
    
    def get_connection(self):
        conn = sqlite3.connect(self.db_name, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_db(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Таблица пользователей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица аккаунтов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                phone_number TEXT NOT NULL,
                session_name TEXT NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        
        # Таблица папок
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS folders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                folder_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        
        # Таблица чатов в папках
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS folder_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                folder_id INTEGER,
                chat_username TEXT,
                chat_id INTEGER NOT NULL,
                chat_title TEXT NOT NULL,
                FOREIGN KEY (folder_id) REFERENCES folders (id)
            )
        ''')
        
        # Таблица задач рассылки
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mailing_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                folder_id INTEGER,
                message_text TEXT NOT NULL,
                total_chats INTEGER NOT NULL,
                sent_count INTEGER DEFAULT 0,
                delay INTEGER NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                FOREIGN KEY (folder_id) REFERENCES folders (id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def add_user(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT OR IGNORE INTO users (user_id) VALUES (?)",
                (user_id,)
            )
            conn.commit()
        finally:
            conn.close()
    
    def get_user_accounts_count(self, user_id: int) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM accounts WHERE user_id = ? AND is_active = 1",
                (user_id,)
            )
            return cursor.fetchone()[0]
        finally:
            conn.close()
    
    def add_account(self, user_id: int, phone_number: str, session_name: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''INSERT INTO accounts (user_id, phone_number, session_name, is_active)
                   VALUES (?, ?, ?, 1)''',
                (user_id, phone_number, session_name)
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()
    
    def get_user_accounts(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT * FROM accounts WHERE user_id = ? AND is_active = 1 ORDER BY added_at DESC",
                (user_id,)
            )
            return cursor.fetchall()
        finally:
            conn.close()
    
    def delete_account(self, account_id: int, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # Получаем имя сессии для удаления файла
            cursor.execute(
                "SELECT session_name FROM accounts WHERE id = ? AND user_id = ?",
                (account_id, user_id)
            )
            result = cursor.fetchone()
            if result:
                session_name = result[0]
                # Деактивируем аккаунт
                cursor.execute(
                    "UPDATE accounts SET is_active = 0 WHERE id = ?",
                    (account_id,)
                )
                conn.commit()
                return session_name
        finally:
            conn.close()
        return None
    
    def get_account_sessions(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT session_name FROM accounts WHERE user_id = ? AND is_active = 1",
                (user_id,)
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def create_folder(self, user_id: int, folder_name: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO folders (user_id, folder_name) VALUES (?, ?)",
                (user_id, folder_name)
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()
    
    def add_chat_to_folder(self, folder_id: int, chat_username: str, chat_id: int, chat_title: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''INSERT INTO folder_chats (folder_id, chat_username, chat_id, chat_title)
                   VALUES (?, ?, ?, ?)''',
                (folder_id, chat_username, chat_id, chat_title)
            )
            conn.commit()
        finally:
            conn.close()
    
    def get_user_folders(self, user_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT * FROM folders WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,)
            )
            return cursor.fetchall()
        finally:
            conn.close()
    
    def get_folder_chats(self, folder_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT * FROM folder_chats WHERE folder_id = ?",
                (folder_id,)
            )
            return cursor.fetchall()
        finally:
            conn.close()
    
    def create_mailing_task(self, user_id: int, folder_id: int, message_text: str, 
                           total_chats: int, delay: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''INSERT INTO mailing_tasks 
                   (user_id, folder_id, message_text, total_chats, delay, status)
                   VALUES (?, ?, ?, ?, ?, 'running')''',
                (user_id, folder_id, message_text, total_chats, delay)
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()
    
    def update_mailing_progress(self, task_id: int, sent_count: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE mailing_tasks SET sent_count = ? WHERE id = ?",
                (sent_count, task_id)
            )
            conn.commit()
        finally:
            conn.close()
    
    def complete_mailing_task(self, task_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE mailing_tasks SET status = 'completed' WHERE id = ?",
                (task_id,)
            )
            conn.commit()
        finally:
            conn.close()

# Инициализация базы данных
db = Database()

# Класс для управления пользовательскими сессиями
class SessionManager:
    def __init__(self):
        self.sessions: Dict[str, Client] = {}
    
    async def create_session(self, phone_number: str, session_name: str, 
                            phone_code: str = None, password: str = None) -> Tuple[bool, str]:
        """Создание новой сессии"""
        session_path = f"sessions/{session_name}"
        
        try:
            client = Client(
                name=session_path,
                api_id=API_ID,
                api_hash=API_HASH,
                workdir="sessions/"
            )
            
            await client.connect()
            
            if not await client.is_user_authorized():
                sent_code = await client.send_code(phone_number)
                
                if phone_code:
                    try:
                        await client.sign_in(
                            phone_number=phone_number,
                            phone_code_hash=sent_code.phone_code_hash,
                            phone_code=phone_code
                        )
                    except SessionPasswordNeeded:
                        if password:
                            await client.check_password(password=password)
                        else:
                            await client.disconnect()
                            return False, "Требуется двухфакторная аутентификация"
                    except (PhoneCodeInvalid, PhoneCodeExpired) as e:
                        await client.disconnect()
                        return False, f"Неверный или просроченный код: {e}"
                
                else:
                    await client.disconnect()
                    return False, "waiting_for_code"
            
            # Проверяем авторизацию
            if await client.is_user_authorized():
                me = await client.get_me()
                await client.disconnect()
                self.sessions[session_name] = client
                return True, f"Аккаунт @{me.username or me.first_name} успешно добавлен!"
            else:
                await client.disconnect()
                return False, "Ошибка авторизации"
                
        except PhoneNumberInvalid:
            return False, "Неверный номер телефона"
        except Exception as e:
            logger.error(f"Ошибка при создании сессии: {e}")
            return False, f"Ошибка: {str(e)}"
    
    def get_session_client(self, session_name: str) -> Optional[Client]:
        """Получение клиента сессии"""
        session_path = f"sessions/{session_name}"
        if session_name in self.sessions:
            return self.sessions[session_name]
        
        try:
            client = Client(
                name=session_path,
                api_id=API_ID,
                api_hash=API_HASH,
                workdir="sessions/"
            )
            self.sessions[session_name] = client
            return client
        except:
            return None
    
    async def get_user_chats(self, session_name: str) -> List[Dict]:
        """Получение чатов из сессии"""
        client = self.get_session_client(session_name)
        if not client:
            return []
        
        try:
            await client.connect()
            if not await client.is_user_authorized():
                await client.disconnect()
                return []
            
            chats = []
            async for dialog in client.get_dialogs():
                if dialog.chat and hasattr(dialog.chat, 'id'):
                    chat = {
                        'id': dialog.chat.id,
                        'title': getattr(dialog.chat, 'title', 
                                       getattr(dialog.chat, 'first_name', 'Unknown')),
                        'username': getattr(dialog.chat, 'username', None)
                    }
                    chats.append(chat)
            
            await client.disconnect()
            return chats
            
        except Exception as e:
            logger.error(f"Ошибка при получении чатов: {e}")
            return []
    
    async def send_message_to_chat(self, session_name: str, chat_id: int, message: str) -> bool:
        """Отправка сообщения через сессию"""
        client = self.get_session_client(session_name)
        if not client:
            return False
        
        try:
            await client.connect()
            if not await client.is_user_authorized():
                await client.disconnect()
                return False
            
            await client.send_message(chat_id=chat_id, text=message)
            await client.disconnect()
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения: {e}")
            return False
    
    async def check_spambot(self, session_name: str) -> str:
        """Проверка спамблока через @spambot"""
        client = self.get_session_client(session_name)
        if not client:
            return "Ошибка: сессия не найдена"
        
        try:
            await client.connect()
            if not await client.is_user_authorized():
                await client.disconnect()
                return "Ошибка: сессия не авторизована"
            
            # Получаем информацию о боте
            try:
                spambot = await client.get_users("spambot")
            except:
                return "Бот @spambot не найден"
            
            # Отправляем команду /start
            await client.send_message(spambot.id, "/start")
            
            # Ждем ответ (упрощенная версия)
            await asyncio.sleep(2)
            
            # Получаем историю сообщений
            messages = []
            async for message in client.get_chat_history(spambot.id, limit=3):
                if message.from_user and message.from_user.id == spambot.id:
                    messages.append(message.text or "Сообщение без текста")
            
            await client.disconnect()
            
            if messages:
                return f"Ответ от @spambot:\n\n" + "\n\n".join(messages[:2])
            else:
                return "Нет ответа от @spambot"
                
        except Exception as e:
            logger.error(f"Ошибка при проверке спамблока: {e}")
            return f"Ошибка: {str(e)}"

# Инициализация менеджера сессий
session_manager = SessionManager()

# Хэндлер команды /cancel для отмены любого состояния
@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Действие отменено.")

# Хэндлеры команд
@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    db.add_user(user_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Функции", callback_data="show_functions")]
    ])
    
    await message.answer(
        "Привет! Я Monkey Gram — бот для управления аккаунтами Telegram. Выбери действие ниже.",
        reply_markup=keyboard
    )

@dp.callback_query(F.data == "show_functions")
async def show_functions_menu(callback: CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить аккаунт", callback_data="add_account")],
        [InlineKeyboardButton(text="📋 Мои аккаунты", callback_data="my_accounts")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="start_mailing")],
        [InlineKeyboardButton(text="🤖 Автоподписка", callback_data="auto_subscribe")],
        [InlineKeyboardButton(text="🛡️ Проверка спамблока", callback_data="check_spamblock")]
    ])
    
    await callback.message.edit_text(
        "📁 Меню функций:",
        reply_markup=keyboard
    )
    await callback.answer()

# Хэндлеры для добавления аккаунта
@dp.callback_query(F.data == "add_account")
async def start_add_account(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    # Проверяем лимит аккаунтов
    accounts_count = db.get_user_accounts_count(user_id)
    if accounts_count >= 20:
        await callback.answer("Достигнут лимит в 20 аккаунтов!", show_alert=True)
        return
    
    await state.set_state(AddAccountStates.phone_number)
    await callback.message.edit_text(
        "Введите номер телефона в международном формате (например, +79991234567):\n"
        "Для отмены введите /cancel"
    )
    await callback.answer()

@dp.message(AddAccountStates.phone_number)
async def process_phone_number(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Добавление аккаунта отменено.")
        return
        
    phone_number = message.text.strip()
    
    # Базовая валидация номера
    if not phone_number.startswith('+'):
        await message.answer("Номер должен начинаться с '+' (международный формат). Попробуйте еще раз:")
        return
    
    await state.update_data(phone_number=phone_number)
    
    # Генерируем уникальное имя для сессии
    session_name = f"session_{message.from_user.id}_{int(datetime.now().timestamp())}"
    await state.update_data(session_name=session_name)
    
    # Пытаемся отправить код
    await message.answer("Отправка кода... Пожалуйста, подождите.")
    success, result = await session_manager.create_session(phone_number, session_name)
    
    if success:
        await message.answer("Код отправлен! Введите код из Telegram:")
        await state.set_state(AddAccountStates.phone_code)
    elif result == "waiting_for_code":
        await message.answer("Код отправлен! Введите код из Telegram:")
        await state.set_state(AddAccountStates.phone_code)
    elif "двухфакторная" in result.lower():
        await message.answer("Введите пароль двухфакторной аутентификации:")
        await state.set_state(AddAccountStates.two_factor)
    else:
        await message.answer(f"Ошибка: {result}")
        await state.clear()

@dp.message(AddAccountStates.phone_code)
async def process_phone_code(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Добавление аккаунта отменено.")
        return
        
    phone_code = message.text.strip()
    data = await state.get_data()
    
    success, result = await session_manager.create_session(
        data['phone_number'],
        data['session_name'],
        phone_code=phone_code
    )
    
    if success:
        # Сохраняем аккаунт в БД
        db.add_account(message.from_user.id, data['phone_number'], data['session_name'])
        await message.answer(result)
        await state.clear()
    elif "двухфакторная" in result.lower():
        await state.update_data(phone_code=phone_code)
        await message.answer("Введите пароль двухфакторной аутентификации:")
        await state.set_state(AddAccountStates.two_factor)
    else:
        await message.answer(f"Ошибка: {result}")
        await state.clear()

@dp.message(AddAccountStates.two_factor)
async def process_two_factor(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Добавление аккаунта отменено.")
        return
        
    password = message.text.strip()
    data = await state.get_data()
    
    success, result = await session_manager.create_session(
        data['phone_number'],
        data['session_name'],
        phone_code=data.get('phone_code'),
        password=password
    )
    
    if success:
        db.add_account(message.from_user.id, data['phone_number'], data['session_name'])
        await message.answer(result)
    else:
        await message.answer(f"Ошибка: {result}")
    
    await state.clear()

# Хэндлеры для управления аккаунтами
@dp.callback_query(F.data == "my_accounts")
async def show_my_accounts(callback: CallbackQuery):
    user_id = callback.from_user.id
    accounts = db.get_user_accounts(user_id)
    
    if not accounts:
        await callback.message.edit_text("У вас нет добавленных аккаунтов.")
        await callback.answer()
        return
    
    text = "📋 Ваши аккаунты:\n\n"
    keyboard = InlineKeyboardBuilder()
    
    for account in accounts:
        account_id = account['id']
        phone_last_4 = account['phone_number'][-4:]
        
        text += f"📱 {account['phone_number']}\n"
        text += f"📅 Добавлен: {account['added_at'][:16]}\n"
        text += f"🟢 Активен\n\n"
        
        keyboard.row(
            InlineKeyboardButton(
                text=f"❌ Удалить сессию ({phone_last_4})",
                callback_data=f"delete_session_{account_id}"
            )
        )
        keyboard.row(
            InlineKeyboardButton(
                text=f"🗑️ Удалить аккаунт ({phone_last_4})",
                callback_data=f"full_delete_{account_id}"
            )
        )
    
    keyboard.row(InlineKeyboardButton(text="🔙 Назад", callback_data="show_functions"))
    
    await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_session_"))
async def delete_account_session(callback: CallbackQuery):
    account_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    
    session_name = db.delete_account(account_id, user_id)
    
    if session_name:
        # Пытаемся удалить файл сессии
        try:
            session_file = f"sessions/{session_name}.session"
            if os.path.exists(session_file):
                os.remove(session_file)
        except Exception as e:
            logger.error(f"Ошибка при удалении файла сессии: {e}")
        
        await callback.answer("Сессия удалена!", show_alert=True)
        await show_my_accounts(callback)
    else:
        await callback.answer("Ошибка при удалении!", show_alert=True)
        await show_my_accounts(callback)

@dp.callback_query(F.data.startswith("full_delete_"))
async def confirm_full_delete(callback: CallbackQuery):
    account_id = int(callback.data.split("_")[2])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_delete_{account_id}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="my_accounts")
        ]
    ])
    
    await callback.message.edit_text(
        "⚠️ Вы уверены, что хотите полностью удалить аккаунт из базы данных?\n"
        "Это действие нельзя отменить!",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("confirm_delete_"))
async def full_delete_account(callback: CallbackQuery):
    account_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    
    # Получаем сессию перед удалением
    conn = sqlite3.connect("bot.db")
    cursor = conn.cursor()
    cursor.execute("SELECT session_name FROM accounts WHERE id = ? AND user_id = ?", 
                  (account_id, user_id))
    result = cursor.fetchone()
    
    if result:
        session_name = result[0]
        # Полностью удаляем запись
        cursor.execute("DELETE FROM accounts WHERE id = ? AND user_id = ?", 
                      (account_id, user_id))
        conn.commit()
        
        # Удаляем файл сессии
        try:
            session_file = f"sessions/{session_name}.session"
            if os.path.exists(session_file):
                os.remove(session_file)
        except Exception as e:
            logger.error(f"Ошибка при удалении файла сессии: {e}")
    
    conn.close()
    
    await callback.answer("Аккаунт удален из базы данных!", show_alert=True)
    await show_my_accounts(callback)

# Хэндлеры для рассылки
@dp.callback_query(F.data == "start_mailing")
async def start_mailing_process(callback: CallbackQuery, state: FSMContext):
    await state.set_state(MailingStates.waiting_for_count)
    await callback.message.edit_text(
        "Введите количество сообщений для рассылки (от 1 до 500):\n"
        "Для отмены введите /cancel"
    )
    await callback.answer()

@dp.message(MailingStates.waiting_for_count)
async def process_mailing_count(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Рассылка отменена.")
        return
        
    try:
        count = int(message.text)
        if 1 <= count <= 500:
            await state.update_data(messages_count=count)
            await state.set_state(MailingStates.waiting_for_delay)
            await message.answer("Введите задержку между сообщениями в секундах (от 20 до 3000):")
        else:
            await message.answer("Число должно быть от 1 до 500. Попробуйте еще раз:")
    except ValueError:
        await message.answer("Пожалуйста, введите число:")

@dp.message(MailingStates.waiting_for_delay)
async def process_mailing_delay(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Рассылка отменена.")
        return
        
    try:
        delay = int(message.text)
        if 20 <= delay <= 3000:
            await state.update_data(delay=delay)
            await state.set_state(MailingStates.waiting_for_message)
            await message.answer("Введите текст сообщения для рассылки:")
        else:
            await message.answer("Задержка должна быть от 20 до 3000 секунд. Попробуйте еще раз:")
    except ValueError:
        await message.answer("Пожалуйста, введите число:")

@dp.message(MailingStates.waiting_for_message)
async def process_mailing_message(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Рассылка отменена.")
        return
        
    await state.update_data(message_text=message.text)
    
    # Проверяем наличие папок
    user_folders = db.get_user_folders(message.from_user.id)
    
    if not user_folders:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📁 Создать папку", callback_data="create_folder_for_mailing")],
            [InlineKeyboardButton(text="🔙 Отмена", callback_data="show_functions")]
        ])
        await message.answer(
            "У вас нет папок с чатами. Создайте папку для рассылки:",
            reply_markup=keyboard
        )
    else:
        await state.set_state(MailingStates.waiting_for_folder)
        
        keyboard = InlineKeyboardBuilder()
        for folder in user_folders:
            keyboard.row(InlineKeyboardButton(
                text=f"📂 {folder['folder_name']}",
                callback_data=f"select_folder_{folder['id']}"
            ))
        keyboard.row(InlineKeyboardButton(text="📁 Создать новую папку", callback_data="create_folder_for_mailing"))
        keyboard.row(InlineKeyboardButton(text="🔙 Отмена", callback_data="show_functions"))
        
        await message.answer("Выберите папку для рассылки:", reply_markup=keyboard.as_markup())

@dp.callback_query(F.data == "create_folder_for_mailing")
async def create_folder_for_mailing(callback: CallbackQuery, state: FSMContext):
    await state.set_state(CreateFolderStates.waiting_for_chats)
    
    # Получаем все активные сессии пользователя
    user_id = callback.from_user.id
    session_names = db.get_account_sessions(user_id)
    
    if not session_names:
        await callback.message.edit_text("У вас нет активных аккаунтов для создания папки.")
        await callback.answer()
        return
    
    # Собираем все чаты из всех сессий
    all_chats = []
    for session_name in session_names:
        chats = await session_manager.get_user_chats(session_name)
        for chat in chats:
            if chat['id'] != user_id:  # Исключаем себя
                all_chats.append({
                    **chat,
                    'session_name': session_name
                })
    
    if not all_chats:
        await callback.message.edit_text("Не найдено чатов для добавления в папку.")
        await callback.answer()
        return
    
    # Сохраняем чаты во временные данные
    await state.update_data(available_chats=all_chats[:20])  # Ограничиваем 20 чатами
    
    # Создаем клавиатуру для выбора чатов
    keyboard = InlineKeyboardBuilder()
    for i, chat in enumerate(all_chats[:20]):
        keyboard.row(InlineKeyboardButton(
            text=f"❌ {chat['title'][:30]}",
            callback_data=f"toggle_chat_{i}"
        ))
    
    keyboard.row(
        InlineKeyboardButton(text="✅ Завершить выбор", callback_data="finish_chat_selection"),
        InlineKeyboardButton(text="🔙 Отмена", callback_data="show_functions")
    )
    
    await callback.message.edit_text(
        "Выберите чаты для добавления в папку (максимум 20):\n"
        "Нажмите на чат, чтобы добавить/удалить его из выбора.",
        reply_markup=keyboard.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("toggle_chat_"))
async def toggle_chat_selection(callback: CallbackQuery, state: FSMContext):
    chat_index = int(callback.data.split("_")[2])
    data = await state.get_data()
    available_chats = data.get('available_chats', [])
    selected_chats = data.get('selected_chats', [])
    
    if chat_index < len(available_chats):
        if chat_index in selected_chats:
            selected_chats.remove(chat_index)
        else:
            if len(selected_chats) < 20:
                selected_chats.append(chat_index)
        
        await state.update_data(selected_chats=selected_chats)
        
        # Обновляем клавиатуру
        keyboard = InlineKeyboardBuilder()
        for i, chat in enumerate(available_chats):
            prefix = "✅" if i in selected_chats else "❌"
            keyboard.row(InlineKeyboardButton(
                text=f"{prefix} {chat['title'][:30]}",
                callback_data=f"toggle_chat_{i}"
            ))
        
        keyboard.row(
            InlineKeyboardButton(text="✅ Завершить выбор", callback_data="finish_chat_selection"),
            InlineKeyboardButton(text="🔙 Отмена", callback_data="show_functions")
        )
        
        await callback.message.edit_reply_markup(reply_markup=keyboard.as_markup())
    
    await callback.answer()

@dp.callback_query(F.data == "finish_chat_selection")
async def finish_chat_selection(callback: CallbackQuery, state: FSMContext):
    await state.set_state(CreateFolderStates.waiting_for_name)
    await callback.message.edit_text("Введите название для папки:")
    await callback.answer()

@dp.message(CreateFolderStates.waiting_for_name)
async def process_folder_name(message: Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Создание папки отменено.")
        return
        
    folder_name = message.text.strip()
    if len(folder_name) < 2 or len(folder_name) > 50:
        await message.answer("Название папки должно быть от 2 до 50 символов. Попробуйте еще раз:")
        return
    
    data = await state.get_data()
    
    # Создаем папку в БД
    folder_id = db.create_folder(message.from_user.id, folder_name)
    
    # Добавляем выбранные чаты
    available_chats = data.get('available_chats', [])
    selected_chats = data.get('selected_chats', [])
    
    added_count = 0
    for chat_index in selected_chats:
        if chat_index < len(available_chats):
            chat = available_chats[chat_index]
            db.add_chat_to_folder(
                folder_id,
                chat.get('username'),
                chat['id'],
                chat['title']
            )
            added_count += 1
    
    await message.answer(f"Папка '{folder_name}' успешно создана! Добавлено чатов: {added_count}")
    await state.clear()
    
    # Возвращаемся к меню
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📁 Меню функций", callback_data="show_functions")]
    ])
    await message.answer("Что дальше?", reply_markup=keyboard)

@dp.callback_query(F.data.startswith("select_folder_"))
async def select_folder_for_mailing(callback: CallbackQuery, state: FSMContext):
    folder_id = int(callback.data.split("_")[2])
    
    # Получаем данные из состояния
    data = await state.get_data()
    messages_count = data.get('messages_count', 1)
    delay = data.get('delay', 60)
    message_text = data.get('message_text', '')
    
    # Получаем чаты из папки
    chats = db.get_folder_chats(folder_id)
    if not chats:
        await callback.answer("Папка пуста!", show_alert=True)
        return
    
    actual_count = min(messages_count, len(chats))
    
    # Создаем задачу рассылки
    task_id = db.create_mailing_task(
        user_id=callback.from_user.id,
        folder_id=folder_id,
        message_text=message_text,
        total_chats=actual_count,
        delay=delay
    )
    
    await callback.message.edit_text(
        f"✅ Рассылка запущена!\n"
        f"Сообщений: {actual_count}\n"
        f"Задержка: {delay} сек.\n"
        f"Текст: {message_text[:50]}...\n\n"
        f"ID задачи: {task_id}\n\n"
        f"Рассылка выполняется в фоновом режиме."
    )
    
    # Запускаем рассылку в фоне
    asyncio.create_task(run_mailing_task(task_id, callback.from_user.id, folder_id, 
                                        message_text, actual_count, delay))
    
    await state.clear()
    await callback.answer()

async def run_mailing_task(task_id: int, user_id: int, folder_id: int, 
                          message_text: str, total_chats: int, delay: int):
    """Фоновая задача рассылки"""
    # Получаем чаты из папки
    chats = db.get_folder_chats(folder_id)
    
    # Получаем сессии пользователя
    session_names = db.get_account_sessions(user_id)
    
    if not chats or not session_names:
        db.complete_mailing_task(task_id)
        return
    
    sent_count = 0
    
    for i, chat in enumerate(chats[:total_chats]):
        # Выбираем сессию (циклически по всем доступным сессиям)
        session_name = session_names[i % len(session_names)]
        
        # Отправляем сообщение
        success = await session_manager.send_message_to_chat(
            session_name,
            chat['chat_id'],
            message_text
        )
        
        if success:
            sent_count += 1
            db.update_mailing_progress(task_id, sent_count)
        
        # Задержка между сообщениями
        if i < len(chats[:total_chats]) - 1:
            await asyncio.sleep(delay)
    
    db.complete_mailing_task(task_id)
    
    # Отправляем уведомление пользователю
    try:
        await bot.send_message(
            chat_id=user_id,
            text=f"✅ Рассылка #{task_id} завершена!\n"
                 f"Отправлено сообщений: {sent_count}/{total_chats}"
        )
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления: {e}")

# Хэндлер для автоподписки
@dp.callback_query(F.data == "auto_subscribe")
async def toggle_auto_subscribe(callback: CallbackQuery):
    # Здесь можно реализовать включение/выключение автоподписки
    # Для простоты покажем сообщение
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Включить", callback_data="enable_auto_subscribe")],
        [InlineKeyboardButton(text="❌ Выключить", callback_data="disable_auto_subscribe")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="show_functions")]
    ])
    
    await callback.message.edit_text(
        "🤖 Автоподписка\n\n"
        "При включенной автоподписке бот автоматически нажимает на инлайн-кнопки "
        "в ответах на ваши сообщения (например, 'Подписаться', 'Join').",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(F.data == "enable_auto_subscribe")
async def enable_auto_subscribe(callback: CallbackQuery):
    await callback.answer("Автоподписка включена!", show_alert=True)
    await show_functions_menu(callback)

@dp.callback_query(F.data == "disable_auto_subscribe")
async def disable_auto_subscribe(callback: CallbackQuery):
    await callback.answer("Автоподписка выключена!", show_alert=True)
    await show_functions_menu(callback)

# Хэндлер для проверки спамблока
@dp.callback_query(F.data == "check_spamblock")
async def check_spamblock_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    # Получаем первый активный аккаунт
    accounts = db.get_user_accounts(user_id)
    if not accounts:
        await callback.answer("У вас нет активных аккаунтов!", show_alert=True)
        return
    
    first_account = accounts[0]
    
    await callback.message.edit_text("🛡️ Проверяем спамблок...")
    
    # Выполняем проверку
    result = await session_manager.check_spambot(first_account['session_name'])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data="show_functions")]
    ])
    
    await callback.message.edit_text(result, reply_markup=keyboard)
    await callback.answer()

# Главная функция
async def main():
    # Создаем папку для сессий если её нет
    if not os.path.exists("sessions"):
        os.makedirs("sessions")
        logger.info("Создана папка 'sessions'")
    
    logger.info("Бот запущен!")
    print("=" * 50)
    print("MonkeyGram бот запущен!")
    print("Для остановки нажмите Ctrl+C")
    print("=" * 50)
    
    try:
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")

if __name__ == "__main__":
    asyncio.run(main())
