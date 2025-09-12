# bot.py  — полноценный рабочий Telegram-бот на aiogram 3.7+
# Поддержка: Кости (🎲) и Баскетбол (🏀).
# Хранение: database.json (рядом с этим файлом).
#
# ВАЖНО: вставьте свой токен в TOKEN перед запуском.

import asyncio
import random
import json
from pathlib import Path
from typing import Dict, Any, List

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.bot import DefaultBotProperties

# --------------------------
# НАСТРОЙКИ
# --------------------------
# Вставьте сюда токен или используйте переменные окружения / безопасное хранилище
TOKEN = "8001643590:AAG93uhtCw-MwIOTXJkeGGOG7k7FjdSTeQM"
DB_PATH = Path("database.json")

# Параметры игр
DICE_ROUNDS = 6
BASKET_GOAL = 10
BASKET_HIT_PROB = 0.40  # шанс заброса в баскетболе

# --------------------------
# БАЗА (JSON)
# --------------------------
def load_db() -> Dict[str, Any]:
    if not DB_PATH.exists():
        DB_PATH.write_text(json.dumps({"users": {}, "lobbies": {}}, ensure_ascii=False, indent=4), encoding="utf-8")
    return json.loads(DB_PATH.read_text(encoding="utf-8"))

def save_db(data: Dict[str, Any]):
    DB_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=4), encoding="utf-8")

# --------------------------
# УТИЛИТЫ ПОЛЬЗОВАТЕЛЯ
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
# ЛОББИ: создание/присоединение/выход
# --------------------------
def max_players(game: str) -> int:
    return 2 if game == "dice" else 10

def min_players(game: str) -> int:
    return 2

def new_lobby_id() -> str:
    db = load_db()
    while True:
        lid = str(random.randint(100000, 999999))
        if lid not in db["lobbies"]:
            return lid

def create_lobby(game: str, creator_uid: int) -> str:
    db = load_db()
    lid = new_lobby_id()
    db["lobbies"][lid] = {
        "game": game,
        "creator": creator_uid,
        "players": [creator_uid],
        "started": False,
        "game_state": {}  # заполняется при старте
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
        add_win(uid, -5)
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
    """Вернёт аккуратно отформатированный список игроков для показа в сообщениях."""
    db = load_db()
    lines = []
    for p in lobby["players"]:
        user = db["users"].get(str(p), {"name": f"User {p}", "wins": 0})
        prefix = "👑 " if p == lobby["creator"] else "• "
        lines.append(f"{prefix}{user['name']} — {user['wins']} wins")
    return "\n".join(lines)

def lobby_overview_text(lid: str, lobby: Dict[str, Any]) -> str:
    """Красивая карточка лобби."""
    game_name = "Кости 🎲" if lobby["game"] == "dice" else "Баскетбол 🏀"
    text = (
        f"🔷 <b>Лобби {lid}</b>\n"
        f"🎮 Игра: {game_name}\n"
        f"👑 Создатель: {load_db()['users'].get(str(lobby['creator']), {'name': 'Unknown'})['name']}\n\n"
        f"<b>Игроки ({len(lobby['players'])}/{max_players(lobby['game'])}):</b>\n"
        f"{format_players_list(lobby)}\n\n"
    )
    if not lobby.get("started", False):
        text += "⚠️ Игра не начата. Ожидайте старта создателя."
    else:
        text += "✅ Игра идёт — ожидайте уведомлений о ходе раундов."
    return text

# --------------------------
# UI / клавиатуры
# --------------------------
def kb_main():
    kb = InlineKeyboardBuilder()
    kb.button(text="🎮 Играть", callback_data="play")
    kb.button(text="🏆 Таблица лидеров", callback_data="leaders")
    return kb.as_markup()

def kb_play():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Создать лобби", callback_data="create_game")
    kb.button(text="🔎 Найти лобби", callback_data="join_game")
    kb.button(text="⬅️ Назад", callback_data="back_main")
    return kb.as_markup()

def kb_game_types():
    kb = InlineKeyboardBuilder()
    kb.button(text="🎲 Кости", callback_data="create_dice")
    kb.button(text="🏀 Баскетбол", callback_data="create_basket")
    kb.button(text="⬅️ Назад", callback_data="play")
    return kb.as_markup()

def kb_lobby_actions(lid: str, uid: int):
    db = load_db()
    if lid not in db["lobbies"]:
        kb = InlineKeyboardBuilder()
        kb.button(text="⬅️ Назад", callback_data="play")
        return kb.as_markup()
    lobby = db["lobbies"][lid]
    kb = InlineKeyboardBuilder()
    if not lobby.get("started", False):
        # не начата
        if uid in lobby["players"]:
            kb.button(text="🔙 Выйти из лобби", callback_data=f"leave_{lid}")
        if uid == lobby["creator"]:
            kb.button(text="🚀 Начать игру", callback_data=f"start_{lid}")
            kb.button(text="⚙️ Управлять игроками", callback_data=f"manage_{lid}")
        kb.button(text="⬅️ Назад", callback_data="play")
    else:
        # начата
        kb.button(text="🚪 Выйти (штраф -5)", callback_data=f"leave_in_game_{lid}")
    return kb.as_markup()

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
    await msg.answer(f"Привет, <b>{msg.from_user.full_name}</b>!\nДобро пожаловать в мини-игры. Выбирай:", reply_markup=kb_main())

@dp.callback_query(F.data == "back_main")
async def cb_back_main(c: CallbackQuery):
    await c.message.edit_text("🏠 Главное меню:", reply_markup=kb_main())

@dp.callback_query(F.data == "play")
async def cb_play(c: CallbackQuery):
    await c.message.edit_text("🎮 Меню — играть:", reply_markup=kb_play())

@dp.callback_query(F.data == "create_game")
async def cb_create_game(c: CallbackQuery):
    await c.message.edit_text("Выберите игру для лобби:", reply_markup=kb_game_types())

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
# CREATE LOBBY handlers
# --------------------------
@dp.callback_query(F.data == "create_dice")
async def cb_create_dice(c: CallbackQuery):
    ensure_user(c.from_user.id, c.from_user.full_name)
    lid = create_lobby("dice", c.from_user.id)
    text = (
        f"✅ Лобби создано — <b>Кости 🎲</b>\n\n"
        f"🆔 <code>{lid}</code>\n"
        f"Создатель: {c.from_user.full_name} 👑\n\n"
        "Ожидайте присоединения второго игрока. Когда будете готовы — нажмите <b>Начать игру</b>."
    )
    await c.message.edit_text(text, reply_markup=kb_lobby_actions(lid, c.from_user.id))

@dp.callback_query(F.data == "create_basket")
async def cb_create_basket(c: CallbackQuery):
    ensure_user(c.from_user.id, c.from_user.full_name)
    lid = create_lobby("basketball", c.from_user.id)
    text = (
        f"✅ Лобби создано — <b>Баскетбол 🏀</b>\n\n"
        f"🆔 <code>{lid}</code>\n"
        f"Создатель: {c.from_user.full_name} 👑\n\n"
        "Ожидайте игроков (2–10). Когда готовы — нажмите <b>Начать игру</b>."
    )
    await c.message.edit_text(text, reply_markup=kb_lobby_actions(lid, c.from_user.id))

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
        emoji = "🎲" if lobby["game"] == "dice" else "🏀"
        text_lines.append(f"🆔 {lid} | {emoji} {lobby['game']} | ({len(lobby['players'])}/{max_players(lobby['game'])})")
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

    # Успешно присоединились — покажем лобби новому игроку
    db = load_db()
    lobby = db["lobbies"][lid]

    # Красивый текст лобби
    text = (
        f"✅ Вы присоединились к лобби <b>{lid}</b>\n\n"
        f"{lobby_overview_text(lid, lobby)}\n\n"
        "Ожидайте старта от создателя или вернитесь в меню."
    )
    await c.message.edit_text(text, reply_markup=kb_lobby_actions(lid, c.from_user.id))

    # уведомим других игроков, что появился новый участник,
    # и отправим обновлённую карточку лобби всем участникам.
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
            # не нужно слать уведомление самому игроку (он уже увидел своё окно),
            # но обновим ему карточку через новое сообщение для согласованности
            try:
                tasks.append(asyncio.create_task(bot.send_message(pid, notify_text)))
                tasks.append(asyncio.create_task(bot.send_message(pid, overview, reply_markup=kb_lobby_actions(lid_local, pid))))
            except Exception:
                pass
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # запустим уведомления неблокирующе
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
        await c.message.edit_text("Лобби не найдено", reply_markup=kb_main())
        return
    lobby = db["lobbies"][lid]
    # штраф -5
    add_win(c.from_user.id, -5)
    # удаляем игрока
    leave_lobby(lid, c.from_user.id, penalize_if_started=False)

    # уведомляем всех и закрываем лобби (по желанию — прерываем матч)
    async def notify_and_close(lid_local: str, leaving_uid: int):
        db_local = load_db()
        if lid_local not in db_local["lobbies"]:
            # возможно уже удалено
            return
        lobby_local = db_local["lobbies"][lid_local]
        tasks = []
        msg = f"⚠️ {db_local['users'].get(str(leaving_uid), {'name': 'Игрок'})['name']} вышел — матч прерван. Возврат в главное меню."
        for pid in list(lobby_local["players"]):
            try:
                tasks.append(asyncio.create_task(bot.send_message(pid, msg, reply_markup=kb_main())))
            except Exception:
                pass
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        # удалим лобби
        delete_lobby(lid_local)

    asyncio.create_task(notify_and_close(lid, c.from_user.id))

    await c.answer("Вы вышли из матча. -5 побед")
    await c.message.edit_text("Вы вышли из матча. -5 побед", reply_markup=kb_main())

@dp.callback_query(F.data.startswith("leave_"))
async def cb_leave(c: CallbackQuery):
    lid = c.data.split("_", 1)[1]
    ok, reason = leave_lobby(lid, c.from_user.id, penalize_if_started=False)
    if ok:
        # уведомим оставшихся игроков об уходе
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
            await bot.send_message(pid, f"Вас выгнали из лобби {lid}.", reply_markup=kb_main())
        except Exception:
            pass
        # уведомим оставшихся
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
    # view_{lid}
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
# СТАРТ ИГРЫ
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
    # Проверки по игрокам
    if len(lobby["players"]) < min_players(lobby["game"]):
        await c.answer(f"Нужно минимум {min_players(lobby['game'])} игроков", show_alert=True)
        return
    if len(lobby["players"]) > max_players(lobby["game"]):
        await c.answer("Слишком много игроков для этой игры", show_alert=True)
        return
    # Инициализация состояния
    lobby["started"] = True
    if lobby["game"] == "dice":
        lobby["game_state"] = {
            "current_round": 1,
            "rounds_total": DICE_ROUNDS,
            "round_rolls": {},        # {round: {uid: value}}
            "match_scores": {str(p): 0 for p in lobby["players"]}
        }
    else:  # basketball
        lobby["game_state"] = {
            "round": 1,
            "goal_limit": BASKET_GOAL,
            "round_tries": {},        # {round: [uids]}
            "match_scores": {str(p): 0 for p in lobby["players"]}
        }
    save_db(db)
    # уведомляем участников
    async def notify_start(lid_local: str):
        db_local = load_db()
        if lid_local not in db_local["lobbies"]:
            return
        lobby_local = db_local["lobbies"][lid_local]
        tasks = []
        start_text = f"🚀 Матч в лобби {lid_local} начался! Игра: {'Кости 🎲' if lobby_local['game']=='dice' else 'Баскетбол 🏀'}"
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
    if lobby["game"] == "dice":
        asyncio.create_task(announce_dice_round(lid))
    else:
        asyncio.create_task(announce_basket_round(lid))

# --------------------------
# ИГРА: КОСТИ (🎲) — через send_dice по нажатию кнопки
# --------------------------
async def announce_dice_round(lid: str):
    db = load_db()
    if lid not in db["lobbies"]:
        return
    lobby = db["lobbies"][lid]
    gs = lobby["game_state"]
    r = gs["current_round"]
    total = gs["rounds_total"]
    # клавиатура с одной кнопкой броска и выходом
    kb = InlineKeyboardBuilder()
    kb.button(text="🎲 Бросить кубик", callback_data=f"dice_roll_{lid}")
    kb.button(text="🚪 Выйти (штраф -5)", callback_data=f"leave_in_game_{lid}")
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
    msg = await bot.send_dice(uid, emoji="🎲")
    value = msg.dice.value
    gs["round_rolls"][rkey][str(uid)] = value
    save_db(db)
    await c.answer(f"Вы бросили: {value}")
    # если все бросили — подводим итоги
    if len(gs["round_rolls"][rkey]) >= len(lobby["players"]):
        # подсчёт
        rolls = gs["round_rolls"][rkey]
        max_val = max(rolls.values())
        winners = [int(pid) for pid, v in rolls.items() if v == max_val]
        if len(winners) == 1:
            gs["match_scores"][str(winners[0])] += 1
            round_result = f"🏆 Раунд {r}: победитель — {load_db()['users'][str(winners[0])]['name']} ({max_val})"
        else:
            # ничья — +1 всем (по правилу)
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
        # сохраняем
        save_db(db)
        if gs["current_round"] > gs["rounds_total"]:
            # конец матча
            await finish_dice_match(lid)
            return
        else:
            await asyncio.sleep(1)
            await announce_dice_round(lid)
    else:
        # ждём другого игрока...
        await c.answer("Ваш бросок зафиксирован. Ждём остальных игроков...")

async def finish_dice_match(lid: str):
    db = load_db()
    if lid not in db["lobbies"]:
        return
    lobby = db["lobbies"][lid]
    gs = lobby["game_state"]
    ms = gs["match_scores"]
    # решаем победителя матча
    players = lobby["players"]
    scores = {p: ms.get(str(p), 0) for p in players}
    # найдем максимальный
    max_score = max(scores.values())
    winners = [p for p, s in scores.items() if s == max_score]
    if len(winners) == 1:
        winner = winners[0]
        add_win(winner, 1)
        result_text = f"🎉 Победитель матча: {load_db()['users'][str(winner)]['name']} — {max_score} очков"
    else:
        # ничья — даём всем по +1 победе (как в правилах оговорено для ничьих)
        for p in winners:
            add_win(p, 1)
        names = ", ".join([load_db()['users'][str(p)]['name'] for p in winners])
        result_text = f"🤝 Ничья между: {names}. Всем начислена +1 победа"
    # отправляем финал
    final_scores_text = "Итоговый счёт:\n" + "\n".join([f"- {load_db()['users'][str(p)]['name']}: {scores[p]}" for p in players])
    tasks = []
    for pid in players:
        try:
            tasks.append(asyncio.create_task(bot.send_message(pid, f"{result_text}\n\n{final_scores_text}", reply_markup=kb_main())))
        except Exception:
            pass
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    # удаляем лобби
    delete_lobby(lid)

# --------------------------
# ИГРА: БАСКЕТБОЛ 🏀
# --------------------------
async def announce_basket_round(lid: str):
    db = load_db()
    if lid not in db["lobbies"]:
        return
    lobby = db["lobbies"][lid]
    gs = lobby["game_state"]
    r = gs["round"]
    # клавиатура: Попытка и выход
    kb = InlineKeyboardBuilder()
    kb.button(text="🏀 Попытка", callback_data=f"basket_try_{lid}")
    kb.button(text="🚪 Выйти (штраф -5)", callback_data=f"leave_in_game_{lid}")
    markup = kb.as_markup()
    tasks = []
    for pid in lobby["players"]:
        try:
            tasks.append(asyncio.create_task(bot.send_message(pid, f"Раунд {r}: Нажмите «🏀 Попытка» — шанс заброса {int(BASKET_HIT_PROB*100)}%", reply_markup=markup)))
        except Exception:
            pass
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

@dp.callback_query(F.data.startswith("basket_try_"))
async def cb_basket_try(c: CallbackQuery):
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
    rkey = str(gs["round"])
    if rkey not in gs["round_tries"]:
        gs["round_tries"][rkey] = []
    if uid in gs["round_tries"][rkey]:
        await c.answer("Вы уже сделали попытку в этом раунде", show_alert=True)
        return
    # делаем попытку
    hit = random.random() < BASKET_HIT_PROB
    gs["round_tries"][rkey].append(uid)
    if hit:
        gs["match_scores"][str(uid)] += 1
        await c.answer("Закинул! +1")
    else:
        await c.answer("Мимо (0)")
    save_db(db)
    # отправляем сводку
    summary = f"Раунд {gs['round']} — попытка от {load_db()['users'][str(uid)]['name']}:\n"
    summary += ("🏀 ЗАБРОШЕНО!\n" if hit else "❌ ПРОМАХ\n")
    summary += "\nТекущий счёт:\n"
    for pid_str, pts in gs["match_scores"].items():
        summary += f"- {load_db()['users'][pid_str]['name']}: {pts}\n"
    tasks = []
    for pid in lobby["players"]:
        try:
            tasks.append(asyncio.create_task(bot.send_message(pid, summary)))
        except Exception:
            pass
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    # проверка победы
    winner = None
    for pid_str, pts in gs["match_scores"].items():
        if pts >= gs["goal_limit"]:
            winner = int(pid_str)
            break
    if winner:
        add_win(winner, 1)
        final = f"🎉 Победитель: {load_db()['users'][str(winner)]['name']}!\nИтог:\n"
        final += "\n".join([f"- {load_db()['users'][k]['name']}: {v}" for k, v in gs["match_scores"].items()])
        tasks = []
        for pid in lobby["players"]:
            try:
                tasks.append(asyncio.create_task(bot.send_message(pid, final, reply_markup=kb_main())))
            except Exception:
                pass
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        delete_lobby(lid)
        return
    # если все сделали попытку — следующий раунд
    if len(gs["round_tries"][rkey]) >= len(lobby["players"]):
        gs["round"] += 1
        save_db(db)
        await announce_basket_round(lid)
    # иначе ждём остальных

# --------------------------
# fallback — любой текст
# --------------------------
@dp.message()
async def fallback(msg: Message):
    ensure_user(msg.from_user.id, msg.from_user.full_name)
    await msg.answer("Нажмите кнопку в меню:", reply_markup=kb_main())

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
