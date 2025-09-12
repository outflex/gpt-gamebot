# bot.py — переработанная версия под требования (только "Кости")
# ВАЖНО: Убедитесь, что в окружении (Railway) задана переменная TOKEN
# Требует aiogram 3.7+
# Структура DB: database.json (в той же папке)

import os
import asyncio
import random
import json
import time
from pathlib import Path
from typing import Dict, Any, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.bot import DefaultBotProperties

# --------------------------
# Настройки
# --------------------------
TOKEN = os.environ.get("TOKEN")  # <- обязательно задайте в Railway / окружении
if not TOKEN:
    raise RuntimeError("TOKEN environment variable is required. Set TOKEN to your bot token.")

DB_PATH = Path("database.json")
DICE_ROUNDS = 6
CONFIRM_TTL = 60  # секунды — время в течение которого подтверждение действительно

# --------------------------
# Простая JSON база
# --------------------------
def load_db() -> Dict[str, Any]:
    if not DB_PATH.exists():
        DB_PATH.write_text(json.dumps({
            "users": {},
            "lobbies": {},
            "pending_confirmations": {}
        }, ensure_ascii=False, indent=4), encoding="utf-8")
    data = json.loads(DB_PATH.read_text(encoding="utf-8"))
    changed = False
    if "pending_confirmations" not in data:
        data["pending_confirmations"] = {}
        changed = True
    for lid, lobby in data.get("lobbies", {}).items():
        if "last_messages" not in lobby:
            lobby["last_messages"] = {}
            changed = True
        if "last_activity" not in lobby:
            lobby["last_activity"] = time.time()
            changed = True
    if changed:
        save_db(data)
    return data

def save_db(data: Dict[str, Any]):
    DB_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=4), encoding="utf-8")

# --------------------------
# Пользовательские утилиты
# --------------------------
def ensure_user(uid: int, name: str):
    db = load_db()
    s = str(uid)
    if s not in db["users"]:
        db["users"][s] = {"name": name, "wins": 0}
        save_db(db)

def add_win(uid: int, amount: int = 1):
    db = load_db()
    s = str(uid)
    if s not in db["users"]:
        db["users"][s] = {"name": f"User {s}", "wins": 0}
    db["users"][s]["wins"] += amount
    save_db(db)

def user_label(uid: int) -> str:
    db = load_db()
    u = db["users"].get(str(uid), {"name": f"User {uid}", "wins": 0})
    return f"{u['name']} ({u['wins']})"

# --------------------------
# Лобби: CRUD и проверки
# --------------------------
def max_players(game: str) -> int:
    return 2  # только кости

def min_players(game: str) -> int:
    return 2

def new_lobby_id() -> str:
    db = load_db()
    while True:
        lid = str(random.randint(100000, 999999))
        if lid not in db["lobbies"]:
            return lid

def create_lobby_record(game: str, creator_uid: int) -> str:
    db = load_db()
    lid = new_lobby_id()
    db["lobbies"][lid] = {
        "game": game,
        "creator": creator_uid,
        "players": [creator_uid],
        "started": False,
        "game_state": {},
        "last_messages": {},  # {uid_str: message_id}
        "last_activity": time.time()
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
    lobby["last_activity"] = time.time()
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
        add_win(uid, -5)
    # удаляем запись о личной карточке
    try:
        lobby["last_messages"].pop(str(uid), None)
    except Exception:
        pass
    # если осталось 0 игроков — удаляем лобби
    if len(lobby["players"]) == 0:
        del db["lobbies"][lid]
    else:
        lobby["last_activity"] = time.time()
    save_db(db)
    return True, "Вышли"

def kick_from_lobby(lid: str, by_uid: int, target_uid: int) -> (bool, str):
    db = load_db()
    if lid not in db["lobbies"]:
        return False, "Лобби не найдено"
    lobby = db["lobbies"][lid]
    if lobby["creator"] != by_uid:
        return False, "Только создатель может выгнать"
    if target_uid not in lobby["players"]:
        return False, "Игрок не в лобби"
    lobby["players"].remove(target_uid)
    lobby["last_messages"].pop(str(target_uid), None)
    lobby["last_activity"] = time.time()
    if len(lobby["players"]) == 0:
        del db["lobbies"][lid]
    save_db(db)
    return True, "Выгнан"

def delete_lobby(lid: str):
    db = load_db()
    if lid in db["lobbies"]:
        del db["lobbies"][lid]
        save_db(db)

def user_active_created_lobby(uid: int) -> Optional[str]:
    db = load_db()
    for lid, lobby in db["lobbies"].items():
        if lobby.get("creator") == uid:
            return lid
    return None

# --------------------------
# UI helpers: минималистично и дружелюбно
# --------------------------
def format_players_list(lobby: Dict[str, Any]) -> str:
    db = load_db()
    lines = []
    for p in lobby["players"]:
        user = db["users"].get(str(p), {"name": f"User {p}"})
        prefix = "👑 " if p == lobby["creator"] else "• "
        lines.append(f"{prefix}{user['name']}")
    return "\n".join(lines)

def lobby_overview_text(lid: str, lobby: Dict[str, Any], extra: Optional[str] = None) -> str:
    db = load_db()
    creator_name = db["users"].get(str(lobby["creator"]), {"name": "Unknown"})["name"]
    text = (
        f"🎲 <b>Лобби {lid}</b>\n"
        f"Создатель: 👑 <b>{creator_name}</b>\n\n"
        f"<b>Игроки ({len(lobby['players'])}/{max_players(lobby['game'])}):</b>\n"
        f"{format_players_list(lobby)}"
    )
    if not lobby.get("started", False):
        text += "\n\n⚠️ Ожидается старт от создателя."
    else:
        text += "\n\n✅ Игра идёт."
    if extra:
        text += f"\n\n{extra}"
    return text

# --------------------------
# Кнопки / клавиатуры
# --------------------------
def kb_main():
    kb = InlineKeyboardBuilder()
    kb.button(text="Играть 🎮", callback_data="play")
    kb.button(text="Топ 🏆", callback_data="leaders")
    return kb.as_markup()

def kb_play():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Создать", callback_data="create_game")
    kb.button(text="🔎 Найти", callback_data="join_game")
    kb.button(text="⬅️ Назад", callback_data="back_main")
    return kb.as_markup()

def kb_create_exists(lid: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="⤴️ Перейти в моё", callback_data=f"view_{lid}")
    kb.button(text="🗑 Удалить моё", callback_data=f"confirm_delete_{lid}")
    kb.button(text="⬅️ Назад", callback_data="play")
    return kb.as_markup()

def kb_game_types():
    kb = InlineKeyboardBuilder()
    kb.button(text="🎲 Кости", callback_data="create_dice")
    kb.button(text="⬅️ Назад", callback_data="play")
    return kb.as_markup()

def kb_lobby_actions(lid: str, uid: int, in_round: bool = False):
    db = load_db()
    kb = InlineKeyboardBuilder()
    lobby = db["lobbies"].get(lid)
    if not lobby:
        kb.button(text="⬅️ Назад", callback_data="play")
        return kb.as_markup()
    if not lobby.get("started", False):
        if uid in lobby["players"]:
            kb.button(text="Выйти 🚪", callback_data=f"leave_{lid}")
        if uid == lobby["creator"]:
            kb.button(text="Начать 🚀", callback_data=f"start_{lid}")
            kb.button(text="Управлять ⚙️", callback_data=f"manage_{lid}")
        kb.button(text="⬅️ Назад", callback_data="play")
    else:
        if in_round:
            kb.button(text="🎲 Бросить", callback_data=f"dice_roll_{lid}")
        else:
            kb.button(text="⬅️ Назад", callback_data="play")
        kb.button(text="Выйти (-5) 🚪", callback_data=f"leave_in_game_{lid}")
    return kb.as_markup()

def kb_confirm_delete(lid: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="Удалить лобби 🔥", callback_data=f"delete_{lid}")
    kb.button(text="⬅️ Отмена", callback_data=f"view_{lid}")
    return kb.as_markup()

# --------------------------
# Инициализация бота
# --------------------------
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# --------------------------
# Сообщения лобби: удерживаем одно актуальное сообщение у пользователя
# --------------------------
def set_last_message_record(lid: str, uid: int, message_id: int):
    db = load_db()
    if lid not in db["lobbies"]:
        return
    db["lobbies"][lid]["last_messages"][str(uid)] = message_id
    db["lobbies"][lid]["last_activity"] = time.time()
    save_db(db)

def remove_last_message_record(lid: str, uid: int):
    db = load_db()
    if lid not in db["lobbies"]:
        return
    db["lobbies"][lid]["last_messages"].pop(str(uid), None)
    db["lobbies"][lid]["last_activity"] = time.time()
    save_db(db)

async def safe_delete_message(chat_id: int, message_id: int):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        # Игнорируем ошибки удаления
        pass

async def update_lobby_card_for_user(lid: str, uid: int, in_round: bool = False, extra: Optional[str] = None):
    """
    Попытка отредактировать существующую карточку у пользователя;
    если не удалось — удаляем старую и отправляем новую (и обновляем запись).
    Если отправить новую нельзя (пользователь не начал чат с ботом), удаляем запись.
    """
    db = load_db()
    if lid not in db["lobbies"]:
        return
    lobby = db["lobbies"][lid]
    text = lobby_overview_text(lid, lobby, extra=extra)
    markup = kb_lobby_actions(lid, uid, in_round=in_round)
    uid_str = str(uid)
    prev_mid = lobby.get("last_messages", {}).get(uid_str)
    # Попробуем редактировать
    if prev_mid:
        try:
            await bot.edit_message_text(text, chat_id=uid, message_id=int(prev_mid), reply_markup=markup)
            lobby["last_activity"] = time.time()
            save_db(db)
            return
        except Exception:
            # попытка редактирования не удалась: удалим старое (если возможно) и отправим новое
            try:
                await safe_delete_message(uid, int(prev_mid))
            except Exception:
                pass
    # Отправляем новое сообщение пользователю
    try:
        msg = await bot.send_message(uid, text, reply_markup=markup)
        set_last_message_record(lid, uid, msg.message_id)
    except Exception:
        # не удалось отправить — убираем запись
        remove_last_message_record(lid, uid)

async def update_all_lobby_cards(lid: str, in_round: bool = False, extra: Optional[str] = None):
    db = load_db()
    if lid not in db["lobbies"]:
        return
    lobby = db["lobbies"][lid]
    tasks = []
    for pid in list(lobby["players"]):
        tasks.append(asyncio.create_task(update_lobby_card_for_user(lid, pid, in_round=in_round, extra=extra)))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

# --------------------------
# Подтверждения (простая реализация)
# --------------------------
def add_pending_confirmation(uid: int, action: str, lid: str):
    db = load_db()
    db["pending_confirmations"][str(uid)] = {
        "action": action,
        "lid": lid,
        "expires": time.time() + CONFIRM_TTL
    }
    save_db(db)

def pop_pending_confirmation(uid: int) -> Optional[Dict[str, Any]]:
    db = load_db()
    s = str(uid)
    info = db["pending_confirmations"].get(s)
    if not info:
        return None
    if info.get("expires", 0) < time.time():
        db["pending_confirmations"].pop(s, None)
        save_db(db)
        return None
    db["pending_confirmations"].pop(s, None)
    save_db(db)
    return info

def peek_pending_confirmation(uid: int) -> Optional[Dict[str, Any]]:
    db = load_db()
    s = str(uid)
    info = db["pending_confirmations"].get(s)
    if not info:
        return None
    if info.get("expires", 0) < time.time():
        db["pending_confirmations"].pop(s, None)
        save_db(db)
        return None
    return info

# --------------------------
# Хэндлеры — меню
# --------------------------
@dp.message(F.text == "/start")
async def cmd_start(msg: Message):
    ensure_user(msg.from_user.id, msg.from_user.full_name)
    await msg.answer(f"Привет, <b>{msg.from_user.full_name}</b>! Я помогу запускать быстрые партии в Кости. 🎲\nВыбирай:", reply_markup=kb_main())

@dp.callback_query(F.data == "back_main")
async def cb_back_main(c: CallbackQuery):
    await c.message.edit_text("🏠 Главное меню:", reply_markup=kb_main())

@dp.callback_query(F.data == "play")
async def cb_play(c: CallbackQuery):
    await c.message.edit_text("🎮 Меню — играть:", reply_markup=kb_play())

@dp.callback_query(F.data == "create_game")
async def cb_create_game(c: CallbackQuery):
    ensure_user(c.from_user.id, c.from_user.full_name)
    existing = user_active_created_lobby(c.from_user.id)
    if existing:
        # Предлагаем перейти или удалить
        await c.message.edit_text("У вас уже есть активное лобби:", reply_markup=kb_create_exists(existing))
        return
    await c.message.edit_text("Выберите игру:", reply_markup=kb_game_types())

@dp.callback_query(F.data == "leaders")
async def cb_leaders(c: CallbackQuery):
    db = load_db()
    users = list(db["users"].items())
    users.sort(key=lambda x: x[1]["wins"], reverse=True)
    text = "<b>🏆 Таблица лидеров</b>\n\n"
    if not users:
        text += "Пока пусто — сыграйте первую партию!"
    else:
        for i, (uid, info) in enumerate(users[:20], 1):
            text += f"{i}. {info['name']} — {info['wins']} побед\n"
    await c.message.edit_text(text, reply_markup=kb_main())

# --------------------------
# Создание лобби (Кости)
# --------------------------
@dp.callback_query(F.data == "create_dice")
async def cb_create_dice(c: CallbackQuery):
    ensure_user(c.from_user.id, c.from_user.full_name)
    existing = user_active_created_lobby(c.from_user.id)
    if existing:
        await c.answer("У вас уже есть активное лобби", show_alert=True)
        await c.message.edit_text("У вас уже есть активное лобби:", reply_markup=kb_create_exists(existing))
        return
    lid = create_lobby_record("dice", c.from_user.id)
    # отправим карточку создателю (лично) и запомним id
    try:
        msg = await bot.send_message(c.from_user.id, lobby_overview_text(lid, load_db()["lobbies"][lid]), reply_markup=kb_lobby_actions(lid, c.from_user.id))
        set_last_message_record(lid, c.from_user.id, msg.message_id)
    except Exception:
        # если не смогли отправить — просто продолжаем
        pass
    await c.message.edit_text(f"✅ Лобби создано — <b>Кости 🎲</b>\n🆔 <code>{lid}</code>\nКарточка лобби отправлена вам в личные сообщения.", reply_markup=kb_lobby_actions(lid, c.from_user.id))

# --------------------------
# JOIN: показать список и присоединиться
# --------------------------
@dp.callback_query(F.data == "join_game")
async def cb_list_lobbies(c: CallbackQuery):
    db = load_db()
    text_lines = ["🔎 <b>Доступные лобби</b>:\n"]
    kb = InlineKeyboardBuilder()
    found = False
    for lid, lobby in db["lobbies"].items():
        if lobby.get("started", False):
            continue
        found = True
        text_lines.append(f"🆔 {lid} | 🎲 | ({len(lobby['players'])}/{max_players(lobby['game'])})")
        kb.button(text=f"➡️ Войти {lid}", callback_data=f"join_{lid}")
    if not found:
        text_lines.append("❌ Нет доступных лобби")
        await c.message.edit_text("\n".join(text_lines), reply_markup=kb_play())
        return
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

    db = load_db()
    lobby = db["lobbies"][lid]

    # отправим карточку новому игроку (лично) и сохраним message_id
    try:
        msg = await bot.send_message(c.from_user.id, lobby_overview_text(lid, lobby), reply_markup=kb_lobby_actions(lid, c.from_user.id))
        set_last_message_record(lid, c.from_user.id, msg.message_id)
    except Exception:
        pass

    # уведомим вызывающего в интерфейсе и обновим карточки всех участников (редактирование/замена)
    await c.message.edit_text("Вы присоединились — карточка лобби отправлена в личные сообщения.", reply_markup=kb_main())
    await update_all_lobby_cards(lid)

# --------------------------
# Управление / выход / кик (с подтверждением через alert)
# --------------------------
@dp.callback_query(F.data.startswith("leave_in_game_"))
async def cb_leave_in_game(c: CallbackQuery):
    lid = c.data.split("_", 3)[3]
    # подтверждение через pending
    pending = peek_pending_confirmation(c.from_user.id)
    if not pending or pending.get("action") != "leave_in_game" or pending.get("lid") != lid:
        add_pending_confirmation(c.from_user.id, "leave_in_game", lid)
        await c.answer("Подтвердите выход (штраф -5): нажмите ещё раз в течение 60 с.", show_alert=True)
        return
    pop_pending_confirmation(c.from_user.id)
    # выполняем выход
    db = load_db()
    if lid not in db["lobbies"]:
        await c.answer("Лобби не найдено", show_alert=True)
        await c.message.edit_text("Лобби не найдено", reply_markup=kb_main())
        return
    add_win(c.from_user.id, -5)
    leave_lobby(lid, c.from_user.id, penalize_if_started=False)
    # уведомим остальных и удалим лобби (матч прерван)
    db = load_db()
    if lid in db["lobbies"]:
        lobby = db["lobbies"][lid]
        msg = f"⚠️ {db['users'].get(str(c.from_user.id), {'name':'Игрок'})['name']} вышел — матч прерван."
        tasks = []
        for pid in list(lobby["players"]):
            try:
                tasks.append(asyncio.create_task(bot.send_message(pid, msg, reply_markup=kb_main())))
            except Exception:
                pass
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        delete_lobby(lid)
    await c.answer("Вы вышли из матча. -5 побед", show_alert=False)
    await c.message.edit_text("Вы вышли из матча. -5 побед", reply_markup=kb_main())

@dp.callback_query(F.data.startswith("leave_"))
async def cb_leave(c: CallbackQuery):
    lid = c.data.split("_", 1)[1]
    pending = peek_pending_confirmation(c.from_user.id)
    if not pending or pending.get("action") != "leave" or pending.get("lid") != lid:
        add_pending_confirmation(c.from_user.id, "leave", lid)
        await c.answer("Подтвердите выход: нажмите ещё раз в течение 60 с.", show_alert=True)
        return
    pop_pending_confirmation(c.from_user.id)
    ok, reason = leave_lobby(lid, c.from_user.id, penalize_if_started=False)
    if ok:
        # обновляем карточки оставшихся (удаление/редактирование)
        await update_all_lobby_cards(lid)
        await c.answer("Вы покинули лобби", show_alert=False)
        await c.message.edit_text("Вы покинули лобби. Возврат в меню.", reply_markup=kb_main())
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
        kb.button(text=f"Выгнать {name}", callback_data=f"kick_{lid}_{p}")
    kb.button(text="⬅️ Назад", callback_data=f"view_{lid}")
    await c.message.edit_text("Выберите игрока для удаления:", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("kick_"))
async def cb_kick(c: CallbackQuery):
    _, lid, pid_str = c.data.split("_", 2)
    pid = int(pid_str)
    pending = peek_pending_confirmation(c.from_user.id)
    if not pending or pending.get("action") != f"kick_{pid}" or pending.get("lid") != lid:
        add_pending_confirmation(c.from_user.id, f"kick_{pid}", lid)
        await c.answer("Подтвердите удаление игрока: нажмите кнопку ещё раз в течение 60 с.", show_alert=True)
        return
    pop_pending_confirmation(c.from_user.id)
    ok, reason = kick_from_lobby(lid, c.from_user.id, pid)
    if ok:
        try:
            await bot.send_message(pid, f"Вас выгнали из лобби {lid}.", reply_markup=kb_main())
        except Exception:
            pass
        await update_all_lobby_cards(lid)
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
    await c.message.edit_text(lobby_overview_text(lid, lobby), reply_markup=kb_lobby_actions(lid, c.from_user.id))

@dp.callback_query(F.data.startswith("confirm_delete_"))
async def cb_confirm_delete(c: CallbackQuery):
    lid = c.data.split("_", 2)[2]
    db = load_db()
    if lid not in db["lobbies"]:
        await c.answer("Лобби не найдено", show_alert=True)
        await c.message.edit_text("Лобби не найдено.", reply_markup=kb_play())
        return
    lobby = db["lobbies"][lid]
    if c.from_user.id != lobby["creator"]:
        await c.answer("Только создатель может удалить", show_alert=True)
        return
    await c.message.edit_text("Подтвердите удаление лобби:", reply_markup=kb_confirm_delete(lid))

@dp.callback_query(F.data.startswith("delete_"))
async def cb_delete(c: CallbackQuery):
    lid = c.data.split("_", 1)[1]
    db = load_db()
    if lid not in db["lobbies"]:
        await c.answer("Лобби не найдено", show_alert=True)
        await c.message.edit_text("Лобби не найдено", reply_markup=kb_play())
        return
    lobby = db["lobbies"][lid]
    if c.from_user.id != lobby["creator"]:
        await c.answer("Только создатель может удалить", show_alert=True)
        return
    pending = peek_pending_confirmation(c.from_user.id)
    if not pending or pending.get("action") != "delete" or pending.get("lid") != lid:
        add_pending_confirmation(c.from_user.id, "delete", lid)
        await c.answer("Подтвердите удаление: нажмите ещё раз в течение 60 с.", show_alert=True)
        return
    pop_pending_confirmation(c.from_user.id)
    # уведомляем участников
    text = f"🗑 Лобби {lid} было удалено создателем."
    tasks = []
    for pid in list(lobby["players"]):
        try:
            tasks.append(asyncio.create_task(bot.send_message(pid, text, reply_markup=kb_main())))
        except Exception:
            pass
    delete_lobby(lid)
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    await c.message.edit_text("Лобби удалено.", reply_markup=kb_main())

# --------------------------
# Старт игры
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
    if len(lobby["players"]) < min_players(lobby["game"]):
        await c.answer(f"Нужно минимум {min_players(lobby['game'])} игроков", show_alert=True)
        return
    if len(lobby["players"]) > max_players(lobby["game"]):
        await c.answer("Слишком много игроков для этой игры", show_alert=True)
        return
    # инициализация состояния
    lobby["started"] = True
    lobby["game_state"] = {
        "current_round": 1,
        "rounds_total": DICE_ROUNDS,
        "round_rolls": {},  # {round: {uid: value}}
        "match_scores": {str(p): 0 for p in lobby["players"]}
    }
    lobby["last_activity"] = time.time()
    save_db(db)
    # обновляем карточки участников с приглашением бросить (редактируем / заменяем)
    await update_all_lobby_cards(lid, in_round=True, extra=f"<b>Раунд 1/{DICE_ROUNDS}</b>\nНажмите «🎲 Бросить»")
    asyncio.create_task(announce_dice_round(lid))

# --------------------------
# Игра: Кости
# --------------------------
async def announce_dice_round(lid: str):
    db = load_db()
    if lid not in db["lobbies"]:
        return
    lobby = db["lobbies"][lid]
    gs = lobby["game_state"]
    r = gs["current_round"]
    total = gs["rounds_total"]
    extra = f"<b>Раунд {r}/{total}</b>\nНажмите «🎲 Бросить»"
    await update_all_lobby_cards(lid, in_round=True, extra=extra)

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
    # бросок: пробуем send_dice (анимация), если нет прав — fallback random
    try:
        msg = await bot.send_dice(uid, emoji="🎲")
        value = msg.dice.value
    except Exception:
        value = random.randint(1, 6)
    gs["round_rolls"][rkey][str(uid)] = value
    lobby["last_activity"] = time.time()
    save_db(db)
    await c.answer(f"Вы бросили: {value}", show_alert=False)
    # если все бросили — подведение итогов
    if len(gs["round_rolls"][rkey]) >= len(lobby["players"]):
        rolls = gs["round_rolls"][rkey]
        max_val = max(rolls.values())
        winners = [int(pid) for pid, v in rolls.items() if v == max_val]
        if len(winners) == 1:
            gs["match_scores"][str(winners[0])] += 1
            round_result = f"🏆 Раунд {r}: победитель — {load_db()['users'][str(winners[0])]['name']} ({max_val})"
        else:
            for pid in winners:
                gs["match_scores"][str(pid)] += 1
            round_result = f"🤝 Раунд {r}: ничья! (макс {max_val}) — всем +1"
        # общий счёт
        score_text = "🔢 Счёт матча:\n"
        for p in lobby["players"]:
            score_text += f"- {load_db()['users'][str(p)]['name']}: {gs['match_scores'][str(p)]}\n"
        # отредактируем карточки, чтобы показать краткий итог (чтобы не спамить)
        extra = f"Раунд {r} завершён.\n{round_result}\n\n{score_text}"
        await update_all_lobby_cards(lid, in_round=False, extra=extra)
        # следующий раунд или конец
        gs["current_round"] += 1
        lobby["last_activity"] = time.time()
        save_db(db)
        if gs["current_round"] > gs["rounds_total"]:
            await finish_dice_match(lid)
            return
        else:
            await asyncio.sleep(1)
            await announce_dice_round(lid)
    else:
        await c.answer("Ваш бросок принят. Ждём остальных.", show_alert=False)

async def finish_dice_match(lid: str):
    db = load_db()
    if lid not in db["lobbies"]:
        return
    lobby = db["lobbies"][lid]
    gs = lobby["game_state"]
    ms = gs["match_scores"]
    players = lobby["players"]
    scores = {p: ms.get(str(p), 0) for p in players}
    max_score = max(scores.values()) if scores else 0
    winners = [p for p, s in scores.items() if s == max_score]
    if len(winners) == 1:
        winner = winners[0]
        add_win(winner, 1)
        result_text = f"🎉 Победитель матча: {load_db()['users'][str(winner)]['name']} — {max_score} очков"
    else:
        for p in winners:
            add_win(p, 1)
        names = ", ".join([load_db()['users'][str(p)]['name'] for p in winners])
        result_text = f"🤝 Ничья между: {names}. Всем начислена +1 победа"
    final_scores_text = "Итоговый счёт:\n" + "\n".join([f"- {load_db()['users'][str(p)]['name']}: {scores[p]}" for p in players])
    tasks = []
    for pid in players:
        try:
            tasks.append(asyncio.create_task(bot.send_message(pid, f"{result_text}\n\n{final_scores_text}", reply_markup=kb_main())))
        except Exception:
            pass
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    # удаляем лобби (требование: лобби удаляется сразу после окончания)
    delete_lobby(lid)

# --------------------------
# Фоллбэк на любые текстовые сообщения
# --------------------------
@dp.message()
async def fallback(msg: Message):
    ensure_user(msg.from_user.id, msg.from_user.full_name)
    await msg.answer("Нажми кнопку в меню, чтобы начать игру.", reply_markup=kb_main())

# --------------------------
# Запуск
# --------------------------
async def main():
    print("Bot starting...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped")
