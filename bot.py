import asyncio
import logging
import re
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pyrogram import Client, filters
from pyrogram.errors import (
    SessionPasswordNeeded, PhoneCodeInvalid,
    PhoneNumberInvalid, FloodWait, Unauthorized
)
from pyrogram.types import Message
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, 
    ReplyKeyboardMarkup, KeyboardButton,
    ReplyKeyboardRemove
)
from aiogram.filters import Command, StateFilter
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ====== КОНФИГУРАЦИЯ ======
# ВАЖНО: Получите НОВЫЕ данные на https://my.telegram.org
API_ID = 32480523  # ЗАМЕНИТЕ НА НОВЫЙ!
API_HASH = "147839735c9fa4e83451209e9b55cfc5"  # ЗАМЕНИТЕ НА НОВЫЙ!
GITHUB_URL = "https://github.com/femilianferuk-droid/Monkey-Gram.git"

# Получаем токен бота из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    # Для тестирования можно использовать переменную окружения
    logger.warning("BOT_TOKEN не найден в переменных окружения")

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
        # Таблица пользователей бота
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Таблица аккаунтов Telegram
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
        # Таблица настроек
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                user_id INTEGER,
                key TEXT,
                value TEXT,
                PRIMARY KEY (user_id, key)
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
            SELECT account_id, phone, first_name, username, registered_date 
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

db = Database()

# ====== СОСТОЯНИЯ FSM ======
class Form(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()
    waiting_for_message_count = State()
    waiting_for_delay = State()
    waiting_for_message_text = State()
    waiting_for_chats = State()

# ====== ХРАНИЛИЩА ======
active_tasks: Dict[int, List[asyncio.Task]] = {}
check_catchers: Dict[int, Dict[int, bool]] = {}  # user_id: {account_id: enabled}

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
        [InlineKeyboardButton(text="💰 Ловец чеков", callback_data="check_catcher_menu")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

def get_accounts_menu(user_id: int):
    accounts = db.get_user_accounts(user_id)
    if not accounts:
        return None
    
    keyboard = []
    for acc in accounts:
        account_id, phone, first_name, username, reg_date = acc
        display_name = first_name or username or phone
        keyboard.append([
            InlineKeyboardButton(
                text=f"📱 {display_name} ({phone})",
                callback_data=f"account_select_{account_id}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_check_catcher_menu(user_id: int):
    accounts = db.get_user_accounts(user_id)
    if not accounts:
        return None
    
    keyboard = []
    for acc in accounts:
        account_id, phone, first_name, username, _ = acc
        display_name = first_name or username or phone
        is_active = check_catchers.get(user_id, {}).get(account_id, False)
        status = "✅" if is_active else "❌"
        keyboard.append([
            InlineKeyboardButton(
                text=f"{status} {display_name}",
                callback_data=f"toggle_catcher_{account_id}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_functions")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_back_button():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_action")]
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
            account_id, phone, first_name, username, reg_date = acc
            display_name = f"{first_name or ''} {username or ''}".strip() or phone
            text += f"• *{display_name}*\n"
            text += f"  📱 `{phone}`\n"
            text += f"  📅 Добавлен: {reg_date}\n\n"
        
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_accounts_menu(user_id)
        )
    await callback.answer()

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
    
    await callback.message.edit_text(
        "📨 *Настройка рассылки*\n\n"
        "Пришлите количество сообщений для отправки (1-1000):",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_back_button()
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
            "💰 *Ловец чеков*\n\n"
            "✅ - мониторинг включен\n"
            "❌ - мониторинг выключен\n\n"
            "Выберите аккаунты для мониторинга:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=menu
        )
    await callback.answer()

@dp.callback_query(F.data.startswith("toggle_catcher_"))
async def toggle_check_catcher(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    account_id = int(callback.data.split("_")[2])
    
    # Инициализация словаря для пользователя
    if user_id not in check_catchers:
        check_catchers[user_id] = {}
    
    # Переключение состояния
    current_state = check_catchers[user_id].get(account_id, False)
    check_catchers[user_id][account_id] = not current_state
    
    # Обновляем меню
    await show_check_catcher_menu(callback)
    await callback.answer(f"Мониторинг {'включен' if not current_state else 'выключен'}!")

@dp.callback_query(F.data == "cancel_action")
async def cancel_action(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        "❌ Действие отменено.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_functions_menu()
    )
    await callback.answer()

# ====== ДОБАВЛЕНИЕ АККАУНТА ======
@dp.message(Form.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
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
    client = Client(
        name=f"session_{message.from_user.id}_{int(datetime.now().timestamp())}",
        api_id=API_ID,
        api_hash=API_HASH,
        workdir="sessions"
    )
    
    try:
        await client.connect()
        
        # Отправляем код
        sent_code = await client.send_code(phone)
        await state.update_data(
            client=client,
            phone_code_hash=sent_code.phone_code_hash
        )
        
        await message.answer(
            f"📱 Код отправлен на номер {phone}\n\n"
            "Пришлите код из Telegram (5 цифр):",
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
    except Exception as e:
        logger.error(f"Ошибка отправки кода: {e}")
        await message.answer(
            f"❌ Ошибка: {str(e)}"
        )
        if 'client' in locals():
            await client.disconnect()

@dp.message(Form.waiting_for_code)
async def process_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    
    if not re.match(r'^\d{5}$', code):
        await message.answer(
            "❌ Код должен содержать 5 цифр. Попробуйте еще раз:",
            reply_markup=get_back_button()
        )
        return
    
    data = await state.get_data()
    client: Client = data['client']
    phone = data['phone']
    phone_code_hash = data['phone_code_hash']
    
    try:
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
        await message.answer(f"❌ Ошибка входа: {str(e)}")
        await client.disconnect()
        await state.clear()

@dp.message(Form.waiting_for_password)
async def process_password(message: types.Message, state: FSMContext):
    password = message.text.strip()
    
    data = await state.get_data()
    client: Client = data['client']
    phone = data['phone']
    
    try:
        await client.check_password(password)
        await finish_authorization(client, phone, message, state)
    except Exception as e:
        logger.error(f"Ошибка 2FA: {e}")
        await message.answer(f"❌ Неверный пароль: {str(e)}")
        await client.disconnect()
        await state.clear()

async def finish_authorization(client: Client, phone: str, message: types.Message, state: FSMContext):
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
            f"❌ Ошибка при сохранении аккаунта: {str(e)}",
            reply_markup=get_functions_menu()
        )
    finally:
        await client.disconnect()
        await state.clear()

# ====== ЛОВЕЦ ЧЕКОВ ======
async def check_catcher_task(user_id: int, account_id: int):
    """Фоновая задача для мониторинга чеков"""
    session_string = db.get_account_session(account_id, user_id)
    if not session_string:
        return
    
    client = Client(
        name=f"catcher_{user_id}_{account_id}",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_string
    )
    
    @client.on_message(filters.all)
    async def handle_message(client: Client, message: Message):
        try:
            # Проверяем, активен ли еще мониторинг
            if not check_catchers.get(user_id, {}).get(account_id, False):
                return
            
            text = message.text or message.caption or ""
            
            # Поиск чеков CryptoBot
            if "@send" in text or "@cryptobot" in text:
                logger.info(f"Найден чек в сообщении от {message.chat.id}")
                
                # Можно добавить логику для автоматического нажатия на чек
                # Но это требует дополнительной реализации
            
            # Поиск ссылок на чеки
            check_patterns = [
                r't\.me/(cryptobot|send)\?start=',
                r'crypto\.bot',
                r'чек\b',
                r'check\b',
                r'\b[a-zA-Z0-9]{10,}\b'  # Возможный код чека
            ]
            
            for pattern in check_patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    # Отправляем уведомление владельцу бота
                    await bot.send_message(
                        user_id,
                        f"💰 *Найден возможный чек!*\n\n"
                        f"Аккаунт: `{account_id}`\n"
                        f"Чат: `{message.chat.id}`\n"
                        f"Сообщение: {text[:100]}...",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    break
                    
        except Exception as e:
            logger.error(f"Ошибка в ловце чеков: {e}")
    
    try:
        await client.start()
        logger.info(f"Ловец чеков запущен для account_id={account_id}")
        
        # Бесконечный цикл мониторинга
        while check_catchers.get(user_id, {}).get(account_id, False):
            await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"Ошибка в ловце чеков для account_id={account_id}: {e}")
    finally:
        await client.stop()

# ====== ЗАПУСК БОТА ======
async def main():
    logger.info("Запуск бота Monkey Gram...")
    
    # Запускаем бота
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    # Создаем папку для сессий
    os.makedirs("sessions", exist_ok=True)
    
    asyncio.run(main())
