# bot.py  — полноценный рабочий Telegram-бот на aiogram 3.7+
# Поддержка: только Кости (🎲). Баскетбол удалён / недоступен.
# Хранение: database.json (рядом с этим файлом).
#
# Добавлен экономический слой: баланс, блокировки, ставки, комиссия, история.
# ВАЖНО: вставьте свой токен в TOKEN перед запуском.

import asyncio
import random
import json
import time
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime, timedelta, timezone
import threading

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.bot import DefaultBotProperties
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
import os

DATABASE_URL = os.getenv("DATABASE_URL")


# --------------------------
# НАСТРОЙКИ
# --------------------------
TOKEN = "8256535045:AAE5V4-t5KpXxnAqY1Mspm_3x78mUOHVYW0"  # замените на свой
DB_PATH = Path("database.json")

# Параметры игр
DICE_ROUNDS = 6
HOUSE_COMMISSION = 0.10  # 10%
DEFAULT_BET = 100
DAILY_BONUS_AMOUNT = 1000  # сумма ежедневного бонуса
DAILY_BONUS_INTERVAL = 24 * 3600  # 24 часа в секундах

# лок для защиты операций с файлом (упрощённая защита от race conditions в одном процессе)
_db_lock = threading.Lock()

# --------------------------
# БАЗА (JSON)
# --------------------------
def _init_db_if_needed():
    if not DB_PATH.exists():
        initial = {
            "users": {},
            "lobbies": {},
            "bet_history": [],
            "house_balance": 0
        }
        DB_PATH.write_text(json.dumps(initial, ensure_ascii=False, indent=4), encoding="utf-8")

def cleanup_empty_lobbies(db: Dict[str, Any]):
    to_delete = [lid for lid, lobby in db.get("lobbies", {}).items() if not lobby.get("players")]
    for lid in to_delete:
        del db["lobbies"][lid]

def load_db() -> Dict[str, Any]:
    _init_db_if_needed()
    with _db_lock:
        data = json.loads(DB_PATH.read_text(encoding="utf-8"))
        cleanup_empty_lobbies(data)
        return data

def save_db(data: Dict[str, Any]):
    _init_db_if_needed()
    cleanup_empty_lobbies(data)
    tmp = DB_PATH.with_suffix(".tmp")
    with _db_lock:
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=4), encoding="utf-8")
        tmp.replace(DB_PATH)

# --------------------------
# УТИЛИТЫ ПОЛЬЗОВАТЕЛЯ / БАЛАНСЫ / ИСТОРИЯ
# --------------------------
def ensure_user(uid: int, name: str):
    db = load_db()
    s = str(uid)
    if s not in db["users"]:
        db["users"][s] = {
    "name": name,
    "balance": 1000,
    "locked": 0,
    "games": 0,
    "wins": 0,
    "dailyBonusClaimed": False,
    "lastBonusTimestamp": None,
    "slot_wins": 0,        # 🎰 сколько монет выиграл в слотах
    "slot_jackpots": 0     # 💎 сколько раз выбил джекпот
}
        save_db(db)

def get_user(uid: int) -> Dict[str, Any]:
    db = load_db()
    return db["users"].get(str(uid))

def user_available_balance(uid: int) -> int:
    u = get_user(uid)
    if not u:
        return 0
    return int(u.get("balance", 0) - u.get("locked", 0))

def change_balance_atomic(uid: int, delta: int):
    """
    Безопасно изменить баланс пользователя на delta (прибавить/отнять).
    Эта функция меняет поле balance (и не трогает locked).
    """
    db = load_db()
    s = str(uid)
    if s not in db["users"]:
        raise ValueError("Пользователь не найден")
    db["users"][s]["balance"] = int(db["users"][s].get("balance", 0) + delta)
    save_db(db)

def inc_locked(uid: int, amount: int):
    db = load_db()
    s = str(uid)
    if s not in db["users"]:
        raise ValueError("Пользователь не найден")
    db["users"][s]["locked"] = int(db["users"][s].get("locked", 0) + amount)
    save_db(db)

def dec_locked(uid: int, amount: int):
    db = load_db()
    s = str(uid)
    if s not in db["users"]:
        raise ValueError("Пользователь не найден")
    cur = int(db["users"][s].get("locked", 0))
    db["users"][s]["locked"] = max(0, cur - int(amount))
    save_db(db)

def add_game_result(uid: int, won: bool):
    db = load_db()
    s = str(uid)
    if s not in db["users"]:
        db["users"][s] = {
            "name": f"User {s}",
            "balance": 1000,
            "locked": 0,
            "games": 0,
            "wins": 0,
            "dailyBonusClaimed": False,
            "lastBonusTimestamp": None
        }
    db["users"][s]["games"] = int(db["users"][s].get("games", 0)) + 1
    if won:
        db["users"][s]["wins"] = int(db["users"][s].get("wins", 0)) + 1
    save_db(db)

def record_bet_history(user_id: int, bet_amount: int, opponent_id: int, outcome: str, profit: int):
    db = load_db()
    entry = {
        "userId": int(user_id),
        "betAmount": int(bet_amount),
        "opponentId": int(opponent_id),
        "outcome": outcome,  # 'win', 'loss', 'draw'
        "profit": int(profit),
        "date": datetime.utcnow().isoformat() + "Z"
    }
    db.setdefault("bet_history", []).append(entry)
    save_db(db)

# --------------------------
# ЛОББИ: создание/присоединение/выход
# --------------------------
def max_players(game: str) -> int:
    return 2 if game == "dice" else 2  # сейчас только dice — 2 игрока

def min_players(game: str) -> int:
    return 2

def new_lobby_id() -> str:
    db = load_db()
    while True:
        lid = str(random.randint(100000, 999999))
        if lid not in db["lobbies"]:
            return lid

def create_lobby(game: str, creator_uid: int, bet: int = DEFAULT_BET) -> str:
    db = load_db()
    lid = new_lobby_id()
    db["lobbies"][lid] = {
        "game": game,
        "creator": creator_uid,
        "players": [creator_uid],
        "started": False,
        "bet": int(bet),  # сумма ставки на игрока
        "bet_locked": False,
        "game_state": {}
    }
    save_db(db)
    return lid

def join_lobby(lid: str, uid: int) -> (bool, str):
    db = load_db()
    if lid not in db["lobbies"]:
        return False, "Лобби не найдено"
    lobby = db["lobbies"][lid]
    if lobby["started"]:
        return False, "Игра уже началась"
    if uid in lobby["players"]:
        return False, "Вы уже в лобби"
    if len(lobby["players"]) >= max_players(lobby["game"]):
        return False, "Лобби заполнено"
    lobby["players"].append(uid)
    save_db(db)
    return True, "Успешно"

def leave_lobby(lid: str, uid: int, penalize_if_started: bool = False) -> (bool, str):
    db = load_db()
    if lid not in db["lobbies"]:
        return False, "Лобби не найдено"
    lobby = db["lobbies"][lid]
    if uid in lobby["players"]:
        lobby["players"].remove(uid)
    if penalize_if_started and lobby.get("started", False):
        # в новой логике штрафов оставляем без изменений (можно доработать)
        pass
    # если пусто — удаляем лобби
    if len(lobby["players"]) == 0:
        del db["lobbies"][lid]
    save_db(db)
    return True, "Вышли"

def kick_from_lobby(lid: str, by_uid: int, target_uid: int) -> (bool, str):
    db = load_db()
    if lid not in db["lobbies"]:
        return False, "Лобби не найдено"
    lobby = db["lobbies"][lid]
    if lobby["creator"] != by_uid:
        return False, "Только создатель может выкидывать"
    if target_uid not in lobby["players"]:
        return False, "Игрок не в лобби"
    lobby["players"].remove(target_uid)
    save_db(db)
    return True, "Выгнан"

def delete_lobby(lid: str):
    db = load_db()
    if lid in db["lobbies"]:
        del db["lobbies"][lid]
        save_db(db)

# --------------------------
# ВСПОМОГАТЕЛИ UI
# --------------------------
def format_players_list(lobby: Dict[str, Any]) -> str:
    db = load_db()
    lines = []
    for p in lobby["players"]:
        user = db["users"].get(str(p), {"name": f"User {p}", "wins": 0, "balance": 0})
        prefix = "👑 " if p == lobby["creator"] else "• "
        lines.append(f"{prefix}{user['name']} — баланс: {user.get('balance',0)}")
    return "\n".join(lines)

def lobby_overview_text(lid: str, lobby: Dict[str, Any]) -> str:
    lobby_name = lobby.get("name", lid)
    game_name = "Кости 🎲"
    bet = lobby.get("bet", DEFAULT_BET)
    text = (
        f"🔷 <b>Лобби: {lobby_name}</b>\n(ID: {lid})\n"
        f"🎮 Игра: {game_name}\n"
        f"💰 Ставка: {bet} монет (с каждого игрока)\n"
        f"👑 Создатель: {load_db()['users'].get(str(lobby['creator']), {'name': 'Unknown'})['name']}\n\n"
        f"<b>Игроки ({len(lobby['players'])}/{max_players(lobby['game'])}):</b>\n"
        f"{format_players_list(lobby)}\n\n"
    )
    if not lobby.get("started", False):
        text += "⚠️ Игра не начата. Ожидайте старта создателя."
    else:
        text += "✅ Игра идёт — ждите уведомлений о ходе раундов."
    return text

# --------------------------
# UI / клавиатуры
# --------------------------
def kb_main(uid: int = None):
    kb = InlineKeyboardBuilder()
    kb.button(text="🎮 Играть", callback_data="play")
    kb.button(text="💼 Мой профиль", callback_data="profile")
    kb.button(text="🏬 Магазин", callback_data="shop")
    kb.button(text="🏆 Таблица лидеров", callback_data="leaders")
    kb.button(text="ℹ️ Об игре", callback_data="about")

    # 🟦 Если игрок уже состоит в лобби — показать кнопку
    if uid:
        db = load_db()
        for lid, lobby in db["lobbies"].items():
            if uid in lobby["players"]:
                kb.button(text="🟦 Моё лобби", callback_data=f"view_{lid}")
                break

    return kb.adjust(2).as_markup()

def kb_only_back_main():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data="back_main")
    return kb.as_markup()

# --------------------------
# FSM для создания лобби
# --------------------------
class CreateLobby(StatesGroup):
    waiting_for_name = State()
    waiting_for_bet = State()   # <-- добавляем состояние для выбора ставки

# --------------------------
# ТЕКСТ ДЛЯ ГЛАВНОГО МЕНЮ
# --------------------------
def main_menu_text(name: str) -> str:
    return (
        f"👋 Привет, <b>{name}</b>!\n"
        "Добро пожаловать в мини-казино с костями. "
        "Делай ставки на свои победы, выигрывай и трать кэшик в магазине (скоро)! "
        "Версия сборки: 0.6 Beta"
    )

def kb_play():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Создать лобби", callback_data="create_game")
    kb.button(text="🔎 Найти лобби", callback_data="join_game")
    kb.button(text="⬅️ Назад", callback_data="back_main")
    return kb.adjust(2).as_markup()

def kb_game_types():
    kb = InlineKeyboardBuilder()
    kb.button(text="🎲 Кости", callback_data="create_lobby_name")  # 🔹 теперь сначала спрашиваем имя лобби
    kb.button(text="🎰 Слоты", callback_data="slots")
    kb.button(text="⬅️ Назад", callback_data="play")
    return kb.adjust(2).as_markup()

def kb_bet_options():
    kb = InlineKeyboardBuilder()
    bets = [100, 500, 1000, 5000, 10000, 50000, 100000]
    for b in bets:
        kb.button(text=f"{b} монет", callback_data=f"create_dice_{b}")
    kb.button(text="⬅️ Назад", callback_data="create_game")
    return kb.adjust(2).as_markup()  # 🔥 кнопки по 2 в ряд

def kb_slot_bets():
    kb = InlineKeyboardBuilder()
    bets = [50, 100, 1000, 10000, 100000, 1000000]
    for b in bets:
        kb.button(text=f"{b} монет", callback_data=f"slot_bet_{b}")
    kb.button(text="⬅️ Назад", callback_data="play")
    return kb.adjust(2).as_markup()

def kb_lobby_actions(lid: str, uid: int):
    db = load_db()
    if lid not in db["lobbies"]:
        kb = InlineKeyboardBuilder()
        kb.button(text="⬅️ Назад", callback_data="play")
        return kb.adjust(2).as_markup()
    lobby = db["lobbies"][lid]
    kb = InlineKeyboardBuilder()
    if not lobby.get("started", False):
        if uid in lobby["players"]:
            kb.button(text="🔙 Выйти из лобби", callback_data=f"leave_{lid}")
        if uid == lobby["creator"]:
            kb.button(text="🚀 Начать игру", callback_data=f"start_{lid}")
            kb.button(text="⚙️ Управлять игроками", callback_data=f"manage_{lid}")
        kb.button(text="⬅️ Назад", callback_data="play")
    else:
        kb.button(text="🚪 Выйти (прервать матч)", callback_data=f"leave_in_game_{lid}")
    return kb.adjust(2).as_markup()

# --------------------------
# БОТ ИНИЦИАЛИЗАЦИЯ (aiogram 3.7+)
# --------------------------
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# --------------------------
# Хэндлеры меню
# --------------------------
@dp.message(F.text == "/start")
async def cmd_start(msg: Message):
    ensure_user(msg.from_user.id, msg.from_user.full_name)
    # дружелюбный привет с "стикером" (эмодзи)
    await msg.answer(main_menu_text(msg.from_user.full_name), reply_markup=kb_main(msg.from_user.id))

@dp.callback_query(F.data == "back_main")
async def cb_back_main(c: CallbackQuery):
     await c.message.edit_text(main_menu_text(c.from_user.full_name), reply_markup=kb_main(c.from_user.id))

@dp.callback_query(F.data == "play")
async def cb_play(c: CallbackQuery):
    await c.message.edit_text("🎮 Меню игр - выбирай:", reply_markup=kb_play())

@dp.callback_query(F.data == "create_game")
async def cb_create_game(c: CallbackQuery):
    await c.message.edit_text("Выберите игру:", reply_markup=kb_game_types())
    
@dp.callback_query(F.data == "choose_dice_bet")
async def cb_choose_dice_bet(c: CallbackQuery):
    await c.message.edit_text("Выберите ставку для игры в кости:", reply_markup=kb_bet_options())
    
@dp.callback_query(F.data == "create_lobby_name")
async def cb_create_lobby_name(c: CallbackQuery, state: FSMContext):
    await c.message.edit_text("Введите название для вашего лобби:")
    await state.set_state(CreateLobby.waiting_for_name)
    
@dp.message(CreateLobby.waiting_for_name)
async def process_lobby_name(msg: Message, state: FSMContext):
    lobby_name = msg.text.strip()
    if not lobby_name:
        await msg.answer("❌ Название не может быть пустым. Введите ещё раз:")
        return

    # сохраняем название во временные данные FSM
    await state.update_data(lobby_name=lobby_name)

    # теперь предлагаем выбрать ставку
    await msg.answer("Теперь выберите ставку для игры в кости:", reply_markup=kb_bet_options())

    # переключаем FSM в состояние выбора ставки
    await state.set_state(CreateLobby.waiting_for_bet)

@dp.callback_query(F.data == "slots")
async def cb_slots(c: CallbackQuery):
    await c.message.edit_text("🎰 Слоты!\n\nВыберите ставку:", reply_markup=kb_slot_bets())

@dp.callback_query(F.data.startswith("slot_bet_"))
async def cb_slot_bet(c: CallbackQuery):
    bet = int(c.data.split("_", 2)[2])
    ensure_user(c.from_user.id, c.from_user.full_name)

    # проверим баланс
    if user_available_balance(c.from_user.id) < bet:
        await c.answer("❌ Недостаточно монет для ставки!", show_alert=True)
        return

    # списываем ставку и кладём её в дом
    change_balance_atomic(c.from_user.id, -bet)
    db = load_db()
    db["house_balance"] = db.get("house_balance", 0) + bet
    save_db(db)

    # запускаем анимацию слота (чисто для красоты, результат решаем сами)
    try:
        await bot.send_dice(c.from_user.id, emoji="🎰")
    except Exception:
        pass

    # решаем результат вручную по шансам
    outcome = random.choices(
    population=["lose", "small", "big", "jackpot"],
    weights=[84, 10, 5, 1],  # lose=84%, small=10%, big=5%, jackpot=1%
    k=1
    )[0]

    payout = 0
    jackpot = False
    if outcome == "jackpot":
        payout = bet * 50
        jackpot = True
    elif outcome == "big":
        payout = bet * 10
    elif outcome == "small":
        payout = bet * 2
    # если lose → payout = 0

    # обновляем баланс игрока
    profit = -bet
    if payout > 0:
        change_balance_atomic(c.from_user.id, payout)

        db = load_db()
        db["house_balance"] = max(0, db.get("house_balance", 0) - payout)
        u = db["users"][str(c.from_user.id)]
        u["slot_wins"] = u.get("slot_wins", 0) + payout
        if jackpot:
            u["slot_jackpots"] = u.get("slot_jackpots", 0) + 1
        save_db(db)

        profit = payout - bet
        await c.answer(f"🎉 Вы выиграли {payout} монет!", show_alert=True)
    else:
        await c.answer("🙃 Увы, пусто. Попробуйте ещё!", show_alert=True)

    # сводка
    db = load_db()
    balance = db["users"][str(c.from_user.id)]["balance"]
    result_text = (
        f"🎰 Итог игры:\n"
        f"• Ставка: {bet}\n"
        f"• Результат: {'+' if profit > 0 else ''}{profit}\n"
        f"• Баланс: {balance} монет\n\n"
        "Хотите сыграть ещё раз?"
    )

    await c.message.answer(result_text, reply_markup=kb_slot_bets())

@dp.callback_query(F.data == "shop")
async def cb_shop(c: CallbackQuery):
    # пока нерабочая вкладка магазина
    await c.message.edit_text("🏬 Магазин — пока в разработке!\nСкоро здесь можно будет тратить монеты на приколюхи.", reply_markup=kb_only_back_main())

@dp.callback_query(F.data == "leaders")
async def cb_leaders(c: CallbackQuery):
    db = load_db()
    users = list(db["users"].items())
    # сортируем по балансу (убывание)
    users.sort(key=lambda x: x[1].get("balance", 0), reverse=True)
    text = "<b>🏆 Топ по монетам</b>\n\n"
    if not users:
        text += "Пока пусто — сыграйте первую партию!"
    else:
        for i, (uid, info) in enumerate(users[:20], 1):
            medal = ""
            if i == 1:
                medal = "🥇"
            elif i == 2:
                medal = "🥈"
            elif i == 3:
                medal = "🥉"
            text += f"{i}. {medal} {info['name']} — {info.get('balance',0)} монет\n"
    await c.message.edit_text(text, reply_markup=kb_only_back_main())
@dp.callback_query(F.data == "about")
async def cb_about(c: CallbackQuery):
    text = (
        "Версия: <b>0.6 Beta</b>\n\n"
        "✨ <b>Изменения:</b>\n"
        "• Добавлено 🎰 Казино (слоты) с анимацией и ставками.\n"
        "• Появилась статистика по слотам в профиле: «Всего выиграно» и «Джекпотов».\n"
        "• Лобби теперь можно называть вручную при создании.\n"
        "• Улучшен интерфейс\n"
        "• Переработана система ежедневной награды (зависит от баланса “дома”).\n"
        "• Добавлены ограничения на создание/вход в бесконечное количество лобби.\n"
        "• Мелкие улучшения и исправления.\n\n"
        "🛠 <b>В разработке:</b>\n"
        "• 🏬 Магазин с покупками за монеты\n"
        "• 🎮 Новые мини-игры\n"
        "• 🎒 Инвентарь с коллекционными предметами и редкими наградами.\n"
        "• 🎁 Еженедельные ивенты с особыми бонусами.\n"
        "• 🔧 Мелкие улучшения интерфейса и фиксы.\n\n"
        "Спасибо, что тестируешь ❤️"
    )
    await c.message.edit_text(text, reply_markup=kb_only_back_main())

# --------------------------
# CREATE LOBBY handlers (Кости)
# --------------------------
@dp.callback_query(F.data.startswith("create_dice_"))
async def cb_create_dice_with_bet(c: CallbackQuery, state: FSMContext):
    bet = int(c.data.split("_", 2)[2])
    data = await state.get_data()
    lobby_name = data["lobby_name"]  # 🔹 название ОБЯЗАТЕЛЬНО, без рандома

    ensure_user(c.from_user.id, c.from_user.full_name)

    # проверка: у игрока уже есть лобби
    db = load_db()
    for lid, lobby in db["lobbies"].items():
        if c.from_user.id in lobby["players"]:
            await c.answer("⚠️ У вас уже есть активное лобби!", show_alert=True)
            return

    # создаём лобби только здесь
    lid = new_lobby_id()
    db["lobbies"][lid] = {
        "name": lobby_name,
        "game": "dice",
        "creator": c.from_user.id,
        "players": [c.from_user.id],
        "started": False,
        "bet": bet,
        "bet_locked": False,
        "game_state": {}
    }
    save_db(db)

    await c.message.edit_text(
        f"✅ Лобби создано: <b>{lobby_name}</b>\n"
        f"🆔 ID: {lid}\n"
        f"💰 Ставка: {bet} монет (с каждого игрока)\n\n"
        "Ожидание игроков...",
        reply_markup=kb_lobby_actions(lid, c.from_user.id)
    )
    await state.clear()

# --------------------------
# JOIN: показать список и присоединиться
# --------------------------
@dp.callback_query(F.data == "join_game")
async def cb_list_lobbies(c: CallbackQuery):
    db = load_db()
    text_lines = ["🔎 <b>Доступные лобби (Кости)</b>:\n"]
    kb = InlineKeyboardBuilder()
    found = False

    for lid, lobby in db["lobbies"].items():
        if lobby.get("started", False):
            continue
        found = True
        emoji = "🎲"
        lobby_name = lobby.get("name", f"Лобби {lid}")

        text_lines.append(
            f"🔷 {lobby_name} (ID: {lid}) | {emoji} {lobby['game']} | "
            f"Ставка: {lobby.get('bet', DEFAULT_BET)} | "
            f"({len(lobby['players'])}/{max_players(lobby['game'])})"
        )
        kb.button(text=f"➡️ Войти в {lobby_name}", callback_data=f"join_{lid}")

    if not found:
        text_lines.append("❌ Нет доступных лобби")

    kb.button(text="⬅️ Назад", callback_data="play")
    await c.message.edit_text("\n".join(text_lines), reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("join_"))
async def cb_join_specific(c: CallbackQuery):
    lid = c.data.split("_", 1)[1]
    ensure_user(c.from_user.id, c.from_user.full_name)
    ok, reason = join_lobby(lid, c.from_user.id)
    if not ok:
        await c.answer(reason, show_alert=True)
        await c.message.edit_text(reason, reply_markup=kb_play())
        return

    # Успешно присоединились — покажем лобби новому игроку
    db = load_db()
    lobby = db["lobbies"][lid]

    text = (
        f"✅ Вы присоединились к лобби <b>{lid}</b>\n\n"
        f"{lobby_overview_text(lid, lobby)}\n\n"
        "Ожидайте старта от создателя или вернитесь в меню."
    )
    await c.message.edit_text(text, reply_markup=kb_lobby_actions(lid, c.from_user.id))

    async def notify_joiners(lid_local: str, new_uid: int):
        db_local = load_db()
        if lid_local not in db_local["lobbies"]:
            return
        lobby_local = db_local["lobbies"][lid_local]
        name = db_local["users"].get(str(new_uid), {"name": f"User {new_uid}"})["name"]
        notify_text = f"✨ <b>{name}</b> присоединился к лобби <code>{lid_local}</code>!"
        overview = lobby_overview_text(lid_local, lobby_local)
        tasks = []
        for pid in lobby_local["players"]:
            try:
                tasks.append(asyncio.create_task(bot.send_message(pid, notify_text)))
                tasks.append(asyncio.create_task(bot.send_message(pid, overview, reply_markup=kb_lobby_actions(lid_local, pid))))
            except Exception:
                pass
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.create_task(notify_joiners(lid, c.from_user.id))

# --------------------------
# УПРАВЛЕНИЕ/ВЫХОД/ВЫГОН
# --------------------------
@dp.callback_query(F.data.startswith("leave_in_game_"))
async def cb_leave_in_game(c: CallbackQuery):
    # leave_in_game_{lid}
    lid = c.data.split("_", 3)[3]
    db = load_db()
    if lid not in db["lobbies"]:
        await c.answer("Лобби не найдено", show_alert=True)
        await c.message.edit_text("Лобби не найдено", reply_markup=kb_main(c.from_user.id))
        return
    lobby = db["lobbies"][lid]
    # удаляем игрока
    leave_lobby(lid, c.from_user.id, penalize_if_started=False)

    async def notify_and_close(lid_local: str, leaving_uid: int):
        db_local = load_db()
        if lid_local not in db_local["lobbies"]:
            return
        lobby_local = db_local["lobbies"][lid_local]
        tasks = []
        msg = f"⚠️ {db_local['users'].get(str(leaving_uid), {'name': 'Игрок'})['name']} вышел — матч прерван. Возврат в главное меню."
        for pid in list(lobby_local["players"]):
            try:
                tasks.append(asyncio.create_task(bot.send_message(pid, msg, reply_markup=kb_main(pid))))
            except Exception:
                pass
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        delete_lobby(lid_local)

    asyncio.create_task(notify_and_close(lid, c.from_user.id))

    await c.answer("Вы вышли из матча")
    await c.message.edit_text("Вы вышли из матча", reply_markup=kb_main(c.from_user.id))

@dp.callback_query(F.data.startswith("leave_"))
async def cb_leave(c: CallbackQuery):
    lid = c.data.split("_", 1)[1]
    ok, reason = leave_lobby(lid, c.from_user.id, penalize_if_started=False)
    if ok:
        async def notify_leave(lid_local: str, left_uid: int):
            db_local = load_db()
            if lid_local not in db_local["lobbies"]:
                return
            lobby_local = db_local["lobbies"][lid_local]
            name = db_local["users"].get(str(left_uid), {"name": f"User {left_uid}"})["name"]
            text = f"⚠️ <b>{name}</b> покинул лобби <code>{lid_local}</code>."
            overview = lobby_overview_text(lid_local, lobby_local)
            tasks = []
            for pid in lobby_local["players"]:
                try:
                    tasks.append(asyncio.create_task(bot.send_message(pid, text)))
                    tasks.append(asyncio.create_task(bot.send_message(pid, overview, reply_markup=kb_lobby_actions(lid_local, pid))))
                except Exception:
                    pass
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        asyncio.create_task(notify_leave(lid, c.from_user.id))

        await c.answer("Вы покинули лобби")
        await c.message.edit_text("Вы покинули лобби. Возврат в меню.", reply_markup=kb_main(c.from_user.id))
    else:
        await c.answer(reason, show_alert=True)

@dp.callback_query(F.data.startswith("manage_"))
async def cb_manage(c: CallbackQuery):
    lid = c.data.split("_", 1)[1]
    db = load_db()
    if lid not in db["lobbies"]:
        await c.answer("Лобби не найдено", show_alert=True)
        return
    lobby = db["lobbies"][lid]
    if c.from_user.id != lobby["creator"]:
        await c.answer("Только создатель может управлять", show_alert=True)
        return
    kb = InlineKeyboardBuilder()
    for p in lobby["players"]:
        if p == lobby["creator"]:
            continue
        name = db["users"].get(str(p), {"name": f"User {p}"})["name"]
        kb.button(text=f"❌ Выгнать {name}", callback_data=f"kick_{lid}_{p}")
    kb.button(text="⬅️ Назад", callback_data=f"view_{lid}")
    await c.message.edit_text("Выберите игрока для удаления:", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("kick_"))
async def cb_kick(c: CallbackQuery):
    _, lid, pid_str = c.data.split("_", 2)
    pid = int(pid_str)
    ok, reason = kick_from_lobby(lid, c.from_user.id, pid)
    if ok:
        try:
            await bot.send_message(pid, f"Вас выгнали из лобби {lid}.", reply_markup=kb_main(pid))
        except Exception:
            pass
        async def notify_kick(lid_local: str, kicked_uid: int):
            db_local = load_db()
            if lid_local not in db_local["lobbies"]:
                return
            lobby_local = db_local["lobbies"][lid_local]
            text = f"❌ Игрок {db_local['users'].get(str(kicked_uid), {'name':'Игрок'})['name']} был исключён из лобби."
            overview = lobby_overview_text(lid_local, lobby_local)
            tasks = []
            for pid_local in lobby_local["players"]:
                try:
                    tasks.append(asyncio.create_task(bot.send_message(pid_local, text)))
                    tasks.append(asyncio.create_task(bot.send_message(pid_local, overview, reply_markup=kb_lobby_actions(lid_local, pid_local))))
                except Exception:
                    pass
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        asyncio.create_task(notify_kick(lid, pid))

        await c.answer("Игрок выгнан")
        await c.message.edit_text("Игрок выгнан.", reply_markup=kb_play())
    else:
        await c.answer(reason, show_alert=True)

@dp.callback_query(F.data.startswith("view_"))
async def cb_view(c: CallbackQuery):
    lid = c.data.split("_", 1)[1]
    db = load_db()
    if lid not in db["lobbies"]:
        await c.answer("Лобби не найдено", show_alert=True)
        await c.message.edit_text("Лобби не найдено.", reply_markup=kb_play())
        return
    lobby = db["lobbies"][lid]
    text = lobby_overview_text(lid, lobby)
    await c.message.edit_text(text, reply_markup=kb_lobby_actions(lid, c.from_user.id))

# --------------------------
# СТАРТ ИГРЫ + блокировка ставок
# --------------------------
@dp.callback_query(F.data.startswith("start_"))
async def cb_start_game(c: CallbackQuery):
    lid = c.data.split("_", 1)[1]
    db = load_db()
    if lid not in db["lobbies"]:
        await c.answer("Лобби не найдено", show_alert=True)
        return
    lobby = db["lobbies"][lid]
    if c.from_user.id != lobby["creator"]:
        await c.answer("Только создатель может стартовать", show_alert=True)
        return
    # проверки по игрокам
    if len(lobby["players"]) < min_players(lobby["game"]):
        await c.answer(f"Нужно минимум {min_players(lobby['game'])} игроков", show_alert=True)
        return
    if len(lobby["players"]) > max_players(lobby["game"]):
        await c.answer("Слишком много игроков для этой игры", show_alert=True)
        return

    bet = int(lobby.get("bet", DEFAULT_BET))
    # проверим баланс каждого участника и заблокируем ставку
    for pid in lobby["players"]:
        ensure_user(pid, load_db()["users"].get(str(pid), {}).get("name", f"User {pid}"))
        available = user_available_balance(pid)
        if available < bet:
            # отменяем старт и сообщаем
            await c.answer("❌ Недостаточно монет для ставки!", show_alert=True)
            await c.message.edit_text("❌ Недостаточно монет у одного из игроков для ставки. Лобби не стартует.", reply_markup=kb_lobby_actions(lid, c.from_user.id))
            return
    # блокируем ставки (все атомарно в пределах процесса)
    try:
        for pid in lobby["players"]:
            inc_locked(pid, bet)
    except Exception as e:
        # в случае ошибки откат
        for pid in lobby["players"]:
            try:
                dec_locked(pid, bet)
            except Exception:
                pass
        await c.answer("Ошибка при блокировке ставок. Попробуйте снова.", show_alert=True)
        return

    # ставим флаг, что ставки заблокированы
    db = load_db()
    db["lobbies"][lid]["bet_locked"] = True
    # и инициализируем game_state как раньше
    db["lobbies"][lid]["started"] = True
    db["lobbies"][lid]["game_state"] = {
        "current_round": 1,
        "rounds_total": DICE_ROUNDS,
        "round_rolls": {},        # {round: {uid: value}}
        "match_scores": {str(p): 0 for p in db["lobbies"][lid]["players"]}
    }
    save_db(db)

    async def notify_start(lid_local: str):
        db_local = load_db()
        if lid_local not in db_local["lobbies"]:
            return
        lobby_local = db_local["lobbies"][lid_local]
        tasks = []
        lobby_name = lobby_local.get("name", lid)
        start_text = f"🚀 Матч в лобби <b>{lobby_name}</b> начался!\nИгра: Кости 🎲\nСтавка: {lobby_local.get('bet')} монет с игрока. Удачи!"
        overview = lobby_overview_text(lid_local, lobby_local)
        for pid in lobby_local["players"]:
            try:
                tasks.append(asyncio.create_task(bot.send_message(pid, start_text)))
                tasks.append(asyncio.create_task(bot.send_message(pid, overview, reply_markup=kb_lobby_actions(lid_local, pid))))
            except Exception:
                pass
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.create_task(notify_start(lid))

    # запускаем игру (не блокирующе)
    asyncio.create_task(announce_dice_round(lid))

# --------------------------
# ИГРА: КОСТИ (🎲)
# --------------------------
async def announce_dice_round(lid: str):
    db = load_db()
    if lid not in db["lobbies"]:
        return
    lobby = db["lobbies"][lid]
    gs = lobby["game_state"]
    r = gs["current_round"]
    total = gs["rounds_total"]
    kb = InlineKeyboardBuilder()
    kb.button(text="🎲 Бросить кубик", callback_data=f"dice_roll_{lid}")
    kb.button(text="🚪 Выйти (прервать матч)", callback_data=f"leave_in_game_{lid}")
    markup = kb.as_markup()
    tasks = []
    for pid in lobby["players"]:
        try:
            tasks.append(asyncio.create_task(bot.send_message(pid, f"Раунд {r}/{total} — нажмите «🎲 Бросить кубик»", reply_markup=markup)))
        except Exception:
            pass
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

@dp.callback_query(F.data.startswith("dice_roll_"))
async def cb_dice_roll(c: CallbackQuery):
    lid = c.data.split("_", 2)[2]
    db = load_db()
    if lid not in db["lobbies"]:
        await c.answer("Лобби не найдено", show_alert=True)
        return
    lobby = db["lobbies"][lid]
    if not lobby.get("started", False):
        await c.answer("Игра не начата", show_alert=True)
        return
    uid = c.from_user.id
    if uid not in lobby["players"]:
        await c.answer("Вы не в этом лобби", show_alert=True)
        return
    gs = lobby["game_state"]
    r = gs["current_round"]
    rkey = str(r)
    if rkey not in gs["round_rolls"]:
        gs["round_rolls"][rkey] = {}
    if str(uid) in gs["round_rolls"][rkey]:
        await c.answer("Вы уже бросали в этом раунде", show_alert=True)
        return
    # бросаем кубик через эмодзи (отправляем пользователю анимацию кубика)
    try:
        msg = await bot.send_dice(uid, emoji="🎲")
    except Exception:
        # в чатах-ботах отправлять send_dice юзеру напрямую может быть запрещено (если бот не может писать)
        # тогда имитируем бросок
        value = random.randint(1, 6)
    else:
        value = msg.dice.value
    # фиксируем бросок
    db = load_db()
    if lid not in db["lobbies"]:
        await c.answer("Лобби пропало", show_alert=True)
        return
    db["lobbies"][lid]["game_state"]["round_rolls"].setdefault(rkey, {})[str(uid)] = value
    save_db(db)
    await c.answer(f"🎲 Вы бросили: {value}")

    db = load_db()
    lobby = db["lobbies"][lid]
    gs = lobby["game_state"]
    rolls = gs["round_rolls"][rkey]
    # если все бросили — подводим итоги раунда
    if len(rolls) >= len(lobby["players"]):
        max_val = max(rolls.values())
        winners = [int(pid) for pid, v in rolls.items() if v == max_val]
        if len(winners) == 1:
            gs["match_scores"][str(winners[0])] += 1
            round_result = f"🏆 Раунд {r}: победитель — {load_db()['users'][str(winners[0])]['name']} ({max_val})"
        else:
            # ничья по раунду — +1 всем в соответствии с прежней логикой
            for pid in winners:
                gs["match_scores"][str(pid)] += 1
            round_result = f"🤝 Раунд {r}: ничья! (макс {max_val}) — всем +1"
        # общий счёт
        score_text = "🔢 Счёт матча:\n"
        for p in lobby["players"]:
            score_text += f"- {load_db()['users'][str(p)]['name']}: {gs['match_scores'][str(p)]}\n"
        # отправляем сводку всем
        summary = f"🎲 Подсчёт раунда {r}:\n"
        for p_str, val in rolls.items():
            summary += f"- {load_db()['users'][p_str]['name']}: {val}\n"
        summary += f"\n{round_result}\n\n{score_text}"
        tasks = []
        for pid in lobby["players"]:
            try:
                tasks.append(asyncio.create_task(bot.send_message(pid, summary)))
            except Exception:
                pass
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        # следующий раунд или конец
        gs["current_round"] += 1
        db = load_db()
        db["lobbies"][lid]["game_state"] = gs
        save_db(db)
        if gs["current_round"] > gs["rounds_total"]:
            # конец матча — теперь рассчитываем выплаты и обновляем балансы
            await finish_dice_match(lid)
            return
        else:
            await asyncio.sleep(1)
            await announce_dice_round(lid)
    else:
        await c.answer("Ваш бросок зафиксирован. Ждём остальных игроков...")

async def finish_dice_match(lid: str):
    """
    В конце матча: определяем победителя(ей).
    Расчёт выигрыша с комиссией:
      payout = (bet * 2) * (1 - HOUSE_COMMISSION)
    Победитель получает payout на баланс (и profit = payout - bet).
    Проигравший получает profit = -bet.
    При ничье: ставки возвращаются (profit = 0).
    Все операции с балансом аккуратно обновляются; в случае ошибки — разблокируем оба и уведомим.
    """
    db = load_db()
    if lid not in db["lobbies"]:
        return
    lobby = db["lobbies"][lid]
    gs = lobby["game_state"]
    players = lobby["players"]
    scores = {p: gs.get("match_scores", {}).get(str(p), 0) for p in players}
    max_score = max(scores.values())
    winners = [p for p, s in scores.items() if s == max_score]
    bet = int(lobby.get("bet", DEFAULT_BET))

    # подготовим уведомления
    try:
        if len(winners) == 1:
            winner = winners[0]
            # вычисляем выплату
            total_bank = bet * 2
            payout = int(total_bank * (1 - HOUSE_COMMISSION))
            # прибыль победителя = payout - его ставка
            profit_winner = payout - bet
            profit_loser = -bet
            loser = [p for p in players if p != winner][0]

            # обновляем балансы и разблокируем деньги
            # делаем в safe try-except: если ошибка — откатим блокировки
            try:
                # уменьшение locked и изменение баланса:
                dec_locked(winner, bet)
                dec_locked(loser, bet)

                # начислим выплату победителю
                change_balance_atomic(winner, payout)
                # проигравший ставка не возвращается — баланс у него уже уменьшен за счёт locked (мы не трогаем его balance)
                # но поскольку мы использовали locked отдельно от balance, на момент блокировки мы не снимали balance,
                # поэтому нужно реализовать логику: при блокировке мы не снимали balance; сейчас снимаем проигравшему ставку
                change_balance_atomic(loser, -bet)

                # record stats
                add_game_result(winner, True)
                add_game_result(loser, False)

                # записываем историю ставок для обоих
                record_bet_history(winner, bet, loser, "win", profit_winner)
                record_bet_history(loser, bet, winner, "loss", profit_loser)

                # уведомляем
                result_text = (
                    f"🎉 Матч завершён! Победитель: <b>{load_db()['users'][str(winner)]['name']}</b>\n"
                    f"Ставка: {bet} монет\n"
                    f"Банк: {total_bank} → Комиссия дома: {int(total_bank * HOUSE_COMMISSION)} монет\n"
                    f"Победителю начислено: {payout} монет (прибыль: {profit_winner})\n"
                )
            except Exception as e:
                # в случае ошибки — разблокируем всё и уведомим
                for p in players:
                    try:
                        dec_locked(p, bet)
                    except Exception:
                        pass
                for p in players:
                    try:
                        await bot.send_message(p, "❌ Произошла ошибка при распределении выигрыша. Ставки возвращены.")
                    except Exception:
                        pass
                # удалим лобби и выйдем
                delete_lobby(lid)
                return

            # отправляем финал
            final_scores_text = "Итоговый счёт:\n" + "\n".join([f"- {load_db()['users'][str(p)]['name']}: {scores[p]}" for p in players])
            tasks = []
            for pid in players:
                try:
                    tasks.append(asyncio.create_task(bot.send_message(pid, f"{result_text}\n{final_scores_text}", reply_markup=kb_main(pid))))
                except Exception:
                    pass
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        else:
            # ничья: возвращаем все заблокированные ставки, profit = 0
            for p in players:
                try:
                    dec_locked(p, bet)
                except Exception:
                    pass
            # не меняем balance
            for p in players:
                add_game_result(p, False)  # ничью считаем как сыгранную игру без победы
                record_bet_history(p, bet, [x for x in players if x != p][0], "draw", 0)
            names = ", ".join([load_db()['users'][str(p)]['name'] for p in winners])
            result_text = f"🤝 Ничья между: {names}. Ставки возвращены, profit: 0."
            final_scores_text = "Итоговый счёт:\n" + "\n".join([f"- {load_db()['users'][str(p)]['name']}: {scores[p]}" for p in players])
            tasks = []
            for pid in players:
                try:
                    tasks.append(asyncio.create_task(bot.send_message(pid, f"{result_text}\n\n{final_scores_text}", reply_markup=kb_main())))
                except Exception:
                    pass
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        # удалить лобби
        delete_lobby(lid)

# --------------------------
# ПРОФИЛЬ И ЕЖЕДНЕВНЫЙ БОНУС
# --------------------------
def profile_text_for(uid: int) -> str:
    ensure_user(uid, f"User {uid}")
    u = get_user(uid)
    last_bonus = u.get("lastBonusTimestamp")

    # статус бонуса
    if not last_bonus:
        bonus_status = "✨ Доступна к получению!"
    else:
        try:
            next_time = datetime.fromtimestamp(
                int(last_bonus) + DAILY_BONUS_INTERVAL
            ).astimezone(timezone(timedelta(hours=3))).strftime("%d.%m.%Y %H:%M (МСК)")
            bonus_status = f"✅ Уже получена\n⏳ Следующая: {next_time}"
        except Exception:
            bonus_status = "❌ Ошибка даты"

    # новый вид профиля йоу
    return (
        "═══════════════════\n"
        f"👑 <b>Профиль игрока</b>\n"
        "═══════════════════\n\n"
        f"🆔 <b>ID:</b> <code>{uid}</code>\n"
        f"👤 <b>Имя:</b> {u.get('name')}\n\n"
        "📊 <b>Статистика</b>\n"
        f"• 🕹️ Игр сыграно: {u.get('games',0)}\n"
        f"• 🏆 Побед: {u.get('wins',0)}\n\n"
        "💰 <b>Финансы</b>\n"
        f"• Баланс: {u.get('balance',0)} монет\n\n"
        "🎰 <b>Слоты</b>\n"
        f"• Всего выиграно: {u.get('slot_wins', 0)} монет\n"
        f"• Джекпотов: {u.get('slot_jackpots', 0)}\n\n"
        "🎁 <b>Ежедневная награда</b>\n"
        f"{bonus_status}\n"
        "═══════════════════"
    )

def kb_profile(uid: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="🎁 Ежедневная награда", callback_data=f"daily_{uid}")
    kb.button(text="⬅️ Назад", callback_data="back_main")
    return kb.adjust(2).as_markup()

@dp.callback_query(F.data == "profile")
async def cb_profile(c: CallbackQuery):
    ensure_user(c.from_user.id, c.from_user.full_name)
    await c.message.edit_text(profile_text_for(c.from_user.id), reply_markup=kb_profile(c.from_user.id))

@dp.callback_query(F.data.startswith("daily_"))
async def cb_daily(c: CallbackQuery):
    parts = c.data.split("_", 1)
    if len(parts) < 2:
        await c.answer("Ошибка", show_alert=True)
        return
    target_uid = int(parts[1])
    if target_uid != c.from_user.id:
        await c.answer("Это не ваш профиль!", show_alert=True)
        return

    ensure_user(c.from_user.id, c.from_user.full_name)
    u = get_user(c.from_user.id)
    last_ts = u.get("lastBonusTimestamp")
    now = int(time.time())
    if last_ts:
        try:
            last_ts_int = int(last_ts)
        except Exception:
            last_ts_int = 0
    else:
        last_ts_int = 0

    if now - last_ts_int < DAILY_BONUS_INTERVAL:
        next_time = datetime.utcfromtimestamp(last_ts_int + DAILY_BONUS_INTERVAL).strftime("%Y-%m-%d %H:%M:%S UTC")
        await c.answer("⏳ Награда уже получена. Следующая будет доступна: " + next_time, show_alert=True)
        await c.message.edit_text(profile_text_for(c.from_user.id), reply_markup=kb_profile(c.from_user.id))
        return

    # 💰 считаем бонус из копилки
    db = load_db()
    house_balance = db.get("house_balance", 0)

    # 50% от копилки делится на всех игроков
    total_users = max(1, len(db["users"]))
    pool_for_bonus = int(house_balance * 0.5)
    bonus_per_user = max(1, pool_for_bonus // total_users)  # хотя бы 1 монета

    if bonus_per_user > 0:
        # уменьшаем копилку
        db["house_balance"] = house_balance - bonus_per_user
        # обновляем профиль игрока
        db["users"][str(c.from_user.id)]["lastBonusTimestamp"] = now
        db["users"][str(c.from_user.id)]["dailyBonusClaimed"] = True
        db["users"][str(c.from_user.id)]["balance"] += bonus_per_user
        save_db(db)

        await c.answer(f"🎉 Получено {bonus_per_user} монет из копилки дома!", show_alert=True)
    else:
        await c.answer("🙃 Копилка пуста, бонуса нет. Попробуй завтра!", show_alert=True)

    await c.message.edit_text(profile_text_for(c.from_user.id), reply_markup=kb_profile(c.from_user.id))

# --------------------------
# fallback — любой текст
# --------------------------
@dp.message()
async def fallback(msg: Message):
    ensure_user(msg.from_user.id, msg.from_user.full_name)
    await msg.answer("Нажмите кнопку в меню:", reply_markup=kb_main(msg.from_user.id))

# --------------------------
# ЗАПУСК
# --------------------------
async def main():
    print("Bot starting...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped")
