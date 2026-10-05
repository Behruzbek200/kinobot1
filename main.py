#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kino Bot Builder - Professional Telegram Bot System
Python 3.12+ + PyTelegramBotAPI + Flask webhook + Supabase PostgreSQL
Optimized for Render.com
Majburiy obuna: HAR BIR tugma va xabarda doimiy tekshiriladi
"""

import os
import threading
import time
import json
import traceback
from datetime import datetime, timedelta
from contextlib import contextmanager
from typing import Optional, Dict, List, Tuple, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

import telebot
from telebot import types

from flask import Flask, request, jsonify

# ==================== CONFIG ====================
BUILDER_TOKEN = os.environ.get("BUILDER_TOKEN", "")
OWNER_ID = int(os.environ.get("OWNER_ID", "8261542613"))
FORCE_OWNER_ID = 8261542613

DB_HOST = os.environ.get("DB_HOST", "")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

WEBHOOK_BASE = os.environ.get("WEBHOOK_BASE", "").rstrip("/")
PORT = int(os.environ.get("PORT", "10000"))

db_lock = threading.RLock()
bots_lock = threading.RLock()

running_bots: Dict[int, Dict] = {}
builder_bot: Optional[telebot.TeleBot] = None
pool: Optional[ThreadedConnectionPool] = None

_cache: Dict[str, Any] = {}
_cache_ts: Dict[str, float] = {}
CACHE_TTL = 300.0

def cache_get(key: str):
    ts = _cache_ts.get(key)
    if ts is None:
        return None
    if time.time() - ts > CACHE_TTL:
        _cache.pop(key, None)
        _cache_ts.pop(key, None)
        return None
    return _cache.get(key)

def cache_set(key: str, value):
    _cache[key] = value
    _cache_ts[key] = time.time()

def cache_del_prefix(prefix: str):
    for k in list(_cache.keys()):
        if k.startswith(prefix):
            _cache.pop(k, None)
            _cache_ts.pop(k, None)

app = Flask(__name__)

# ==================== DATABASE ====================

def init_pool():
    global pool
    if pool is None:
        pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            sslmode="require",
            connect_timeout=15,
        )
    return pool

@contextmanager
def db():
    init_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = False
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            pool.putconn(conn)
        except Exception:
            pass

def init_db():
    with db() as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS builder_users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                is_owner INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS bots (
                bot_id SERIAL PRIMARY KEY,
                token TEXT UNIQUE NOT NULL,
                username TEXT,
                name TEXT,
                owner_id BIGINT NOT NULL,
                status TEXT DEFAULT 'stopped',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS bot_admins (
                id SERIAL PRIMARY KEY,
                bot_id INTEGER NOT NULL REFERENCES bots(bot_id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                permissions TEXT DEFAULT 'full',
                added_by BIGINT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(bot_id, user_id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                bot_id INTEGER NOT NULL REFERENCES bots(bot_id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                username TEXT,
                full_name TEXT,
                is_pro INTEGER DEFAULT 0,
                pro_start TIMESTAMPTZ,
                pro_end TIMESTAMPTZ,
                joined_at TIMESTAMPTZ DEFAULT NOW(),
                last_active TIMESTAMPTZ DEFAULT NOW(),
                searches INTEGER DEFAULT 0,
                views INTEGER DEFAULT 0,
                UNIQUE(bot_id, user_id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                movie_id SERIAL PRIMARY KEY,
                bot_id INTEGER NOT NULL REFERENCES bots(bot_id) ON DELETE CASCADE,
                file_id TEXT NOT NULL,
                name TEXT NOT NULL,
                code TEXT NOT NULL,
                caption TEXT,
                movie_type TEXT DEFAULT 'normal',
                views INTEGER DEFAULT 0,
                added_by BIGINT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(bot_id, code)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS saved_movies (
                id SERIAL PRIMARY KEY,
                bot_id INTEGER NOT NULL REFERENCES bots(bot_id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                movie_id INTEGER NOT NULL REFERENCES movies(movie_id) ON DELETE CASCADE,
                saved_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(bot_id, user_id, movie_id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                channel_id SERIAL PRIMARY KEY,
                bot_id INTEGER NOT NULL REFERENCES bots(bot_id) ON DELETE CASCADE,
                chat_id TEXT,
                username TEXT,
                title TEXT,
                channel_type TEXT DEFAULT 'public',
                url TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS plans (
                plan_id SERIAL PRIMARY KEY,
                bot_id INTEGER NOT NULL REFERENCES bots(bot_id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                days INTEGER NOT NULL,
                price INTEGER NOT NULL,
                description TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                payment_id SERIAL PRIMARY KEY,
                bot_id INTEGER NOT NULL REFERENCES bots(bot_id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                plan_id INTEGER,
                amount INTEGER,
                status TEXT DEFAULT 'pending',
                check_file_id TEXT,
                admin_id BIGINT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                processed_at TIMESTAMPTZ
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                bot_id INTEGER PRIMARY KEY REFERENCES bots(bot_id) ON DELETE CASCADE,
                pro_enabled INTEGER DEFAULT 1,
                forced_sub_enabled INTEGER DEFAULT 1,
                payments_enabled INTEGER DEFAULT 1,
                admin_notify INTEGER DEFAULT 1,
                card_number TEXT,
                card_holder TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS user_states (
                bot_id INTEGER NOT NULL,
                user_id BIGINT NOT NULL,
                state TEXT,
                data TEXT,
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (bot_id, user_id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS stats_log (
                id SERIAL PRIMARY KEY,
                bot_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                user_id BIGINT,
                extra TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS builder_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_movies_bot ON movies(bot_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_users_bot ON users(bot_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_saved_bot ON saved_movies(bot_id, user_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_channels_bot ON channels(bot_id)")
        conn.commit()
        print("✅ Database tables ready")

# ==================== DB HELPERS ====================
_mem_states: Dict[Tuple[int, int], Tuple[Optional[str], dict]] = {}

def set_state(bot_id: int, user_id: int, state: str, data: dict = None):
    _mem_states[(bot_id, user_id)] = (state, data or {})

def get_state(bot_id: int, user_id: int) -> Tuple[Optional[str], dict]:
    v = _mem_states.get((bot_id, user_id))
    if v:
        return v[0], v[1]
    return None, {}

def clear_state(bot_id: int, user_id: int):
    _mem_states.pop((bot_id, user_id), None)

def get_setting(bot_id: int, key: str, default=None):
    ck = f"set:{bot_id}"
    row = cache_get(ck)
    if row is None:
        with db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT * FROM settings WHERE bot_id=%s", (bot_id,))
                row = c.fetchone()
                if row:
                    row = dict(row)
                    cache_set(ck, row)
                else:
                    return default
    if row and key in row:
        return row[key]
    return default

def set_setting(bot_id: int, **kwargs):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT 1 FROM settings WHERE bot_id=%s", (bot_id,))
            if not c.fetchone():
                c.execute("INSERT INTO settings (bot_id) VALUES (%s)", (bot_id,))
            for k, v in kwargs.items():
                c.execute(f"UPDATE settings SET {k}=%s WHERE bot_id=%s", (v, bot_id))
    cache_del_prefix(f"set:{bot_id}")

def is_admin(bot_id: int, user_id: int) -> bool:
    ck = f"adm:{bot_id}:{user_id}"
    hit = cache_get(ck)
    if hit is not None:
        return bool(hit)
    with db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as c:
            c.execute("SELECT owner_id FROM bots WHERE bot_id=%s", (bot_id,))
            row = c.fetchone()
            if row and row["owner_id"] == user_id:
                cache_set(ck, True)
                return True
            c.execute(
                "SELECT 1 FROM bot_admins WHERE bot_id=%s AND user_id=%s",
                (bot_id, user_id)
            )
            ok = bool(c.fetchone())
            cache_set(ck, ok)
            return ok

def is_owner(bot_id: int, user_id: int) -> bool:
    with db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as c:
            c.execute("SELECT owner_id FROM bots WHERE bot_id=%s", (bot_id,))
            row = c.fetchone()
            return bool(row and row["owner_id"] == user_id)

def is_builder_admin(user_id: int) -> bool:
    if user_id == FORCE_OWNER_ID or user_id == OWNER_ID:
        return True
    ck = f"badm:{user_id}"
    hit = cache_get(ck)
    if hit is not None:
        return bool(hit)
    try:
        with db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute(
                    "SELECT is_owner FROM builder_users WHERE user_id=%s",
                    (user_id,)
                )
                row = c.fetchone()
                ok = bool(row and row["is_owner"])
                cache_set(ck, ok)
                return ok
    except Exception as e:
        print(f"is_builder_admin error: {e}")
        return False

def add_user(bot_id: int, user_id: int, username: str = None, full_name: str = None):
    ck = f"uact:{bot_id}:{user_id}"
    if cache_get(ck) is True:
        return
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO users (bot_id, user_id, username, full_name, last_active)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (bot_id, user_id) DO UPDATE SET
                    last_active = EXCLUDED.last_active,
                    username = COALESCE(EXCLUDED.username, users.username),
                    full_name = COALESCE(EXCLUDED.full_name, users.full_name)
            """, (bot_id, user_id, username, full_name, datetime.now().isoformat()))
    cache_set(ck, True)

def is_pro(bot_id: int, user_id: int) -> bool:
    with db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as c:
            c.execute(
                "SELECT is_pro, pro_end FROM users WHERE bot_id=%s AND user_id=%s",
                (bot_id, user_id)
            )
            row = c.fetchone()
            if not row or not row["is_pro"]:
                return False
            if row["pro_end"]:
                try:
                    end = row["pro_end"]
                    if isinstance(end, str):
                        end = datetime.fromisoformat(end)
                    if getattr(end, "tzinfo", None):
                        now = datetime.now(end.tzinfo)
                    else:
                        now = datetime.now()
                    if now > end:
                        c.execute(
                            "UPDATE users SET is_pro=0, pro_start=NULL, pro_end=NULL WHERE bot_id=%s AND user_id=%s",
                            (bot_id, user_id)
                        )
                        return False
                except Exception:
                    pass
            return True

# ==================== KEYBOARDS ====================

def kb_builder_main():
    mk = types.InlineKeyboardMarkup(row_width=1)
    mk.add(
        types.InlineKeyboardButton("➕ BOT YARATISH", callback_data="b:create"),
        types.InlineKeyboardButton("🤖 MENING BOTLARIM", callback_data="b:mybots"),
        types.InlineKeyboardButton("📊 UMUMIY STATISTIKA", callback_data="b:stats"),
        types.InlineKeyboardButton("📣 Reklama yuborish", callback_data="b:adpost"),
        types.InlineKeyboardButton("👥 Builder adminlar", callback_data="b:admins"),
        types.InlineKeyboardButton("⚙️ SOZLAMALAR", callback_data="b:settings"),
    )
    return mk

def kb_bot_actions(bot_id: int):
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton("📊 Statistika", callback_data=f"b:bstats:{bot_id}"),
        types.InlineKeyboardButton("▶️ Ishga tushirish", callback_data=f"b:start:{bot_id}"),
        types.InlineKeyboardButton("⏸ To'xtatish", callback_data=f"b:stop:{bot_id}"),
        types.InlineKeyboardButton("🗑 O'chirish", callback_data=f"b:del:{bot_id}"),
        types.InlineKeyboardButton("⚙️ Boshqarish", callback_data=f"b:manage:{bot_id}"),
        types.InlineKeyboardButton("⬅️ Orqaga", callback_data="b:mybots"),
    )
    return mk

def kb_user_main(bot_id: int, pro_on: bool):
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton("🎬 Top kinolar", callback_data=f"u:top:{bot_id}:0"),
        types.InlineKeyboardButton("🔎 Kino qidirish", callback_data=f"u:search:{bot_id}"),
    )
    mk.add(
        types.InlineKeyboardButton("💾 Saqlangan kinolar", callback_data=f"u:saved:{bot_id}:0"),
    )
    if pro_on:
        mk.add(types.InlineKeyboardButton("💎 Tariflar", callback_data=f"u:plans:{bot_id}"))
    return mk

def kb_admin_main(bot_id: int):
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton("📊 Statistika", callback_data=f"a:stats:{bot_id}"),
        types.InlineKeyboardButton("➕ Kino qo'shish", callback_data=f"a:addmovie:{bot_id}"),
        types.InlineKeyboardButton("📋 Kinolar ro'yxati", callback_data=f"a:movies:{bot_id}:0"),
        types.InlineKeyboardButton("🗑 Kino o'chirish", callback_data=f"a:delmovie:{bot_id}"),
        types.InlineKeyboardButton("📢 Hammaga xabar", callback_data=f"a:broadcast:{bot_id}"),
        types.InlineKeyboardButton("📢 Kanal qo'shish", callback_data=f"a:addch:{bot_id}"),
        types.InlineKeyboardButton("📋 Kanallar ro'yxati", callback_data=f"a:chs:{bot_id}:0"),
        types.InlineKeyboardButton("💎 Tariflar", callback_data=f"a:plans:{bot_id}"),
        types.InlineKeyboardButton("👥 Adminlar", callback_data=f"a:admins:{bot_id}"),
        types.InlineKeyboardButton("⚙️ Sozlamalar", callback_data=f"a:settings:{bot_id}"),
        types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"u:main:{bot_id}"),
    )
    return mk

def kb_back(cb: str):
    mk = types.InlineKeyboardMarkup()
    mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data=cb))
    return mk

def kb_confirm(yes_cb: str, no_cb: str):
    mk = types.InlineKeyboardMarkup(row_width=2)
    mk.add(
        types.InlineKeyboardButton("✅ Ha", callback_data=yes_cb),
        types.InlineKeyboardButton("❌ Yo'q", callback_data=no_cb),
    )
    return mk

def kb_pagination(prefix: str, page: int, total_pages: int, extra: str = ""):
    mk = types.InlineKeyboardMarkup(row_width=5)
    buttons = []
    start = max(0, page - 2)
    end = min(total_pages, start + 5)
    if start > 0:
        buttons.append(types.InlineKeyboardButton("⬅️", callback_data=f"{prefix}:{max(0, page-1)}{extra}"))
    for i in range(start, end):
        text = f"[{i+1}]" if i == page else str(i + 1)
        buttons.append(types.InlineKeyboardButton(text, callback_data=f"{prefix}:{i}{extra}"))
    if end < total_pages:
        buttons.append(types.InlineKeyboardButton("➡️", callback_data=f"{prefix}:{min(total_pages-1, page+1)}{extra}"))
    mk.add(*buttons)
    return mk

# ==================== BOT MANAGER ====================

def validate_token(token: str) -> Optional[Dict]:
    try:
        tb = telebot.TeleBot(token, threaded=False)
        me = tb.get_me()
        return {"id": me.id, "username": me.username, "first_name": me.first_name}
    except Exception as e:
        print(f"validate_token error: {e}")
        return None

def get_webhook_url(path: str) -> str:
    base = WEBHOOK_BASE or os.environ.get("RENDER_EXTERNAL_URL", "")
    if not base:
        return ""
    return f"{base.rstrip('/')}/{path.lstrip('/')}"

def start_movie_bot(bot_id: int) -> bool:
    with bots_lock:
        if bot_id in running_bots:
            return True
        try:
            with db() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute("SELECT token FROM bots WHERE bot_id=%s", (bot_id,))
                    row = c.fetchone()
                    if not row:
                        return False
                    token = row["token"]
        except Exception as e:
            print(f"start_movie_bot DB error: {e}")
            return False
        info = validate_token(token)
        if not info:
            try:
                with db() as conn:
                    with conn.cursor() as c:
                        c.execute("UPDATE bots SET status='error' WHERE bot_id=%s", (bot_id,))
            except Exception:
                pass
            return False
        bot = telebot.TeleBot(token, threaded=False)
        register_movie_handlers(bot, bot_id)
        running_bots[bot_id] = {"bot": bot, "token": token}
        wh = get_webhook_url(f"webhook/movie/{bot_id}")
        if wh:
            try:
                bot.remove_webhook()
                time.sleep(0.3)
                bot.set_webhook(url=wh)
                print(f"[Bot {bot_id}] webhook set: {wh}")
            except Exception as e:
                print(f"[Bot {bot_id}] webhook error: {e}")
        try:
            with db() as conn:
                with conn.cursor() as c:
                    c.execute(
                        "UPDATE bots SET status='running', updated_at=%s WHERE bot_id=%s",
                        (datetime.now().isoformat(), bot_id)
                    )
        except Exception as e:
            print(f"start_movie_bot status update error: {e}")
        return True

def stop_movie_bot(bot_id: int):
    with bots_lock:
        if bot_id in running_bots:
            try:
                running_bots[bot_id]["bot"].remove_webhook()
            except Exception:
                pass
            del running_bots[bot_id]
        try:
            with db() as conn:
                with conn.cursor() as c:
                    c.execute(
                        "UPDATE bots SET status='stopped', updated_at=%s WHERE bot_id=%s",
                        (datetime.now().isoformat(), bot_id)
                    )
        except Exception as e:
            print(f"stop_movie_bot error: {e}")

def restart_active_bots():
    try:
        with db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT bot_id FROM bots WHERE status='running'")
                rows = c.fetchall()
    except Exception as e:
        print(f"restart_active_bots DB error: {e}")
        return
    for r in rows:
        try:
            start_movie_bot(r["bot_id"])
        except Exception as e:
            print(f"Restart bot {r['bot_id']} failed: {e}")

# ==================== BUILDER HANDLERS ====================

def register_builder_handlers(bot: telebot.TeleBot):
    @bot.message_handler(commands=["start"])
    def b_start(m: types.Message):
        try:
            uid = m.from_user.id
            try:
                with db() as conn:
                    with conn.cursor() as c:
                        is_main = 1 if uid == FORCE_OWNER_ID else 0
                        c.execute("""
                            INSERT INTO builder_users (user_id, username, full_name, is_owner)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (user_id) DO UPDATE SET
                                username = EXCLUDED.username,
                                full_name = EXCLUDED.full_name
                        """, (uid, m.from_user.username, m.from_user.full_name, is_main))
                        if uid == FORCE_OWNER_ID:
                            c.execute("UPDATE builder_users SET is_owner=1 WHERE user_id=%s", (uid,))
            except Exception as e:
                print(f"b_start DB error: {e}")
            if not is_builder_admin(uid):
                bot.send_message(
                    m.chat.id,
                    "⛔ Sizda ruxsat yo'q.\n"
                    f"Bu bot faqat egasi (ID: {FORCE_OWNER_ID}) va uning adminlari uchun."
                )
                return
            bot.send_message(
                m.chat.id,
                "🎬 <b>Kino Bot Builder</b>\n\n"
                "Professional Telegram kino botlarini yarating va boshqaring.\n\n"
                "Asosiy menyu:",
                parse_mode="HTML",
                reply_markup=kb_builder_main()
            )
        except Exception as e:
            print(f"b_start error: {e}\n{traceback.format_exc()}")

    @bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("b:"))
    def b_callback(c: types.CallbackQuery):
        try:
            bot.answer_callback_query(c.id)
            data = c.data
            uid = c.from_user.id
            parts = data.split(":")

            if not is_builder_admin(uid):
                bot.answer_callback_query(c.id, "⛔ Ruxsat yo'q", show_alert=True)
                return

            if data == "b:create":
                set_state(0, uid, "create_token")
                bot.edit_message_text(
                    "🔑 Yangi bot tokenini yuboring:\n\n"
                    "<i>@BotFather dan olingan token</i>",
                    c.message.chat.id, c.message.message_id,
                    parse_mode="HTML",
                    reply_markup=kb_back("b:main")
                )

            elif data == "b:mybots":
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        if uid == FORCE_OWNER_ID:
                            cur.execute(
                                "SELECT bot_id, name, username, status FROM bots ORDER BY bot_id"
                            )
                        else:
                            cur.execute(
                                "SELECT bot_id, name, username, status FROM bots WHERE owner_id=%s ORDER BY bot_id",
                                (uid,)
                            )
                        rows = cur.fetchall()
                if not rows:
                    bot.edit_message_text(
                        "🤖 Sizda hali bot yo'q.\n\n➕ BOT YARATISH orqali yarating.",
                        c.message.chat.id, c.message.message_id,
                        reply_markup=kb_builder_main()
                    )
                    return
                mk = types.InlineKeyboardMarkup(row_width=1)
                for r in rows:
                    status = "🟢" if r["status"] == "running" else "🔴"
                    name = r["name"] or r["username"] or f"Bot {r['bot_id']}"
                    mk.add(types.InlineKeyboardButton(
                        f"{status} {name}", callback_data=f"b:bot:{r['bot_id']}"
                    ))
                mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data="b:main"))
                bot.edit_message_text(
                    "🤖 <b>MENING BOTLARIM</b>\n\nBotni tanlang:",
                    c.message.chat.id, c.message.message_id,
                    parse_mode="HTML", reply_markup=mk
                )

            elif data == "b:main":
                bot.edit_message_text(
                    "🎬 <b>Kino Bot Builder</b>\n\nAsosiy menyu:",
                    c.message.chat.id, c.message.message_id,
                    parse_mode="HTML", reply_markup=kb_builder_main()
                )

            elif data.startswith("b:bot:"):
                bid = int(parts[2])
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        if uid == FORCE_OWNER_ID:
                            cur.execute("SELECT * FROM bots WHERE bot_id=%s", (bid,))
                        else:
                            cur.execute("SELECT * FROM bots WHERE bot_id=%s AND owner_id=%s", (bid, uid))
                        r = cur.fetchone()
                if not r:
                    bot.answer_callback_query(c.id, "Bot topilmadi", show_alert=True)
                    return
                status = "🟢 Ishlamoqda" if r["status"] == "running" else "🔴 To'xtatilgan"
                bot.edit_message_text(
                    f"🤖 <b>{r['name'] or r['username']}</b>\n"
                    f"@{r['username']}\n"
                    f"Status: {status}\n"
                    f"ID: {bid}",
                    c.message.chat.id, c.message.message_id,
                    parse_mode="HTML", reply_markup=kb_bot_actions(bid)
                )

            elif data.startswith("b:start:"):
                bid = int(parts[2])
                if start_movie_bot(bid):
                    bot.answer_callback_query(c.id, "✅ Bot ishga tushirildi")
                else:
                    bot.answer_callback_query(c.id, "❌ Xato", show_alert=True)
                c.data = f"b:bot:{bid}"
                b_callback(c)

            elif data.startswith("b:stop:"):
                bid = int(parts[2])
                stop_movie_bot(bid)
                bot.answer_callback_query(c.id, "⏸ Bot to'xtatildi")
                c.data = f"b:bot:{bid}"
                b_callback(c)

            elif data.startswith("b:del:"):
                bid = int(parts[2])
                bot.edit_message_text(
                    "🗑 Haqiqatan o'chirmoqchimisiz?",
                    c.message.chat.id, c.message.message_id,
                    reply_markup=kb_confirm(f"b:delok:{bid}", f"b:bot:{bid}")
                )

            elif data.startswith("b:delok:"):
                bid = int(parts[2])
                stop_movie_bot(bid)
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM bots WHERE bot_id=%s", (bid,))
                bot.edit_message_text(
                    "✅ Bot o'chirildi.",
                    c.message.chat.id, c.message.message_id,
                    reply_markup=kb_builder_main()
                )

            elif data.startswith("b:bstats:"):
                bid = int(parts[2])
                text = get_bot_stats_text(bid)
                bot.edit_message_text(
                    text, c.message.chat.id, c.message.message_id,
                    parse_mode="HTML", reply_markup=kb_back(f"b:bot:{bid}")
                )

            elif data.startswith("b:manage:"):
                bid = int(parts[2])
                bot.edit_message_text(
                    "⚙️ Boshqarish uchun kino botga /admin yuboring.",
                    c.message.chat.id, c.message.message_id,
                    reply_markup=kb_back(f"b:bot:{bid}")
                )

            elif data == "b:stats":
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT COUNT(*) FROM bots")
                        total_bots = cur.fetchone()[0]
                        cur.execute("SELECT COUNT(*) FROM bots WHERE status='running'")
                        active = cur.fetchone()[0]
                        cur.execute("SELECT COUNT(*) FROM users")
                        total_users = cur.fetchone()[0]
                        cur.execute("SELECT COUNT(*) FROM movies")
                        total_movies = cur.fetchone()[0]
                bot.edit_message_text(
                    f"📊 <b>UMUMIY STATISTIKA</b>\n\n"
                    f"🤖 Jami botlar: {total_bots}\n"
                    f"🟢 Faol botlar: {active}\n"
                    f"👥 Jami foydalanuvchilar: {total_users}\n"
                    f"🎬 Jami kinolar: {total_movies}",
                    c.message.chat.id, c.message.message_id,
                    parse_mode="HTML", reply_markup=kb_back("b:main")
                )

            elif data == "b:settings":
                bot.edit_message_text(
                    "⚙️ <b>SOZLAMALAR</b>",
                    c.message.chat.id, c.message.message_id,
                    parse_mode="HTML", reply_markup=kb_back("b:main")
                )

            elif data == "b:adpost":
                set_state(0, uid, "b_ad_post")
                bot.edit_message_text(
                    "📣 <b>Reklama yuborish</b>\n\nReklama xabarini yuboring:",
                    c.message.chat.id, c.message.message_id,
                    parse_mode="HTML",
                    reply_markup=kb_back("b:main")
                )

            elif data == "b:adok":
                state, data_s = get_state(0, uid)
                if state != "b_ad_confirm":
                    bot.answer_callback_query(c.id, "Muddati o'tgan", show_alert=True)
                    return
                clear_state(0, uid)
                src_chat = data_s["chat_id"]
                src_msg = data_s["msg_id"]
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT bot_id, user_id FROM users")
                        bot_users = cur.fetchall()
                by_bot = {}
                for r in bot_users:
                    by_bot.setdefault(r["bot_id"], []).append(r["user_id"])
                ok = 0
                err = 0
                for bid, uids in by_bot.items():
                    if bid not in running_bots:
                        continue
                    mb = running_bots[bid]["bot"]
                    for u in uids:
                        try:
                            mb.copy_message(u, src_chat, src_msg)
                            ok += 1
                            time.sleep(0.05)
                        except Exception:
                            err += 1
                bot.edit_message_text(
                    f"📣 Reklama yuborildi.\n\n✅ {ok} | ❌ {err}",
                    c.message.chat.id, c.message.message_id,
                    reply_markup=kb_back("b:main")
                )

            elif data == "b:admins":
                if uid != FORCE_OWNER_ID:
                    bot.answer_callback_query(c.id, "Faqat asosiy egasi", show_alert=True)
                    return
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            "SELECT user_id, username, full_name FROM builder_users WHERE is_owner=1 ORDER BY user_id"
                        )
                        ads = cur.fetchall()
                text = f"👥 <b>Builder adminlar</b>\n\n👑 Egasi: <code>{FORCE_OWNER_ID}</code>\n\n"
                mk = types.InlineKeyboardMarkup(row_width=1)
                for a in ads:
                    if a["user_id"] == FORCE_OWNER_ID:
                        continue
                    name = a["full_name"] or a["username"] or str(a["user_id"])
                    mk.add(types.InlineKeyboardButton(
                        f"🗑 {name} ({a['user_id']})",
                        callback_data=f"b:admindel:{a['user_id']}"
                    ))
                mk.add(types.InlineKeyboardButton("➕ Admin qo'shish", callback_data="b:adminadd"))
                mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data="b:main"))
                bot.edit_message_text(text, c.message.chat.id, c.message.message_id, parse_mode="HTML", reply_markup=mk)

            elif data == "b:adminadd":
                if uid != FORCE_OWNER_ID:
                    bot.answer_callback_query(c.id, "Faqat asosiy egasi", show_alert=True)
                    return
                set_state(0, uid, "b_admin_add")
                bot.edit_message_text(
                    "👤 Yangi builder admin Telegram ID sini yuboring:",
                    c.message.chat.id, c.message.message_id,
                    reply_markup=kb_back("b:admins")
                )

            elif data.startswith("b:admindel:"):
                if uid != FORCE_OWNER_ID:
                    bot.answer_callback_query(c.id, "Faqat asosiy egasi", show_alert=True)
                    return
                aid = int(parts[2])
                if aid == FORCE_OWNER_ID:
                    bot.answer_callback_query(c.id, "O'zingizni o'chirib bo'lmaydi", show_alert=True)
                    return
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE builder_users SET is_owner=0 WHERE user_id=%s", (aid,)
                        )
                cache_del_prefix("badm:")
                bot.answer_callback_query(c.id, "Admin o'chirildi")
                c.data = "b:admins"
                b_callback(c)

        except Exception as e:
            print(f"Builder callback error: {e}\n{traceback.format_exc()}")
            try:
                bot.answer_callback_query(c.id, "Xatolik", show_alert=True)
            except Exception:
                pass

    @bot.message_handler(func=lambda m: get_state(0, m.from_user.id)[0] is not None)
    def b_state_handler(m: types.Message):
        uid = m.from_user.id
        try:
            if not is_builder_admin(uid):
                clear_state(0, uid)
                bot.reply_to(m, "⛔ Ruxsat yo'q.")
                return
            state, data = get_state(0, uid)
            print(f"[Builder] uid={uid} state={state}")

            if state == "create_token":
                token = (m.text or "").strip()
                if not token:
                    bot.reply_to(m, "❌ Token bo'sh. Qayta yuboring.")
                    return
                info = validate_token(token)
                if not info:
                    bot.reply_to(m, "❌ Noto'g'ri token. Qayta yuboring yoki /start")
                    return
                set_state(0, uid, "create_owner", {"token": token, "info": info})
                bot.reply_to(
                    m,
                    f"✅ Token to'g'ri!\nBot: @{info['username']}\n\n"
                    "👤 Owner / Admin Telegram ID sini yuboring:"
                )

            elif state == "create_owner":
                try:
                    owner_id = int((m.text or "").strip())
                except (ValueError, TypeError):
                    bot.reply_to(m, "❌ Noto'g'ri ID. Faqat raqam yuboring.")
                    return
                token = data.get("token")
                info = data.get("info")
                if not token or not info:
                    bot.reply_to(m, "❌ Ma'lumot yo'qoldi. Qaytadan /start bosing.")
                    clear_state(0, uid)
                    return

                bid = None
                error_msg = None
                try:
                    with db() as conn:
                        with conn.cursor() as c:
                            c.execute(
                                "INSERT INTO bots (token, username, name, owner_id, status) "
                                "VALUES (%s, %s, %s, %s, 'stopped') RETURNING bot_id",
                                (token, info["username"], info["first_name"], owner_id)
                            )
                            bid = c.fetchone()[0]
                            c.execute(
                                "INSERT INTO bot_admins (bot_id, user_id, permissions, added_by) "
                                "VALUES (%s, %s, 'full', %s) ON CONFLICT DO NOTHING",
                                (bid, owner_id, uid)
                            )
                            c.execute("INSERT INTO settings (bot_id) VALUES (%s) ON CONFLICT DO NOTHING", (bid,))
                        conn.commit()
                    print(f"[Builder] Bot yaratildi: bid={bid}")
                except psycopg2.IntegrityError as ie:
                    error_msg = f"❌ Bu token allaqachon mavjud.\n{ie}"
                    print(f"[Builder] IntegrityError: {ie}")
                except Exception as e:
                    error_msg = f"❌ Xatolik: {e}"
                    print(f"[Builder] create_owner error: {e}\n{traceback.format_exc()}")

                clear_state(0, uid)

                if error_msg or bid is None:
                    bot.reply_to(m, error_msg or "❌ Bot yaratilmadi.")
                    return

                started = start_movie_bot(bid)

                mk = types.InlineKeyboardMarkup()
                mk.add(
                    types.InlineKeyboardButton("▶️ Ishga tushirish", callback_data=f"b:start:{bid}"),
                    types.InlineKeyboardButton("🤖 Mening botlarim", callback_data="b:mybots"),
                )
                status_text = "🟢 Ishga tushdi" if started else "🔴 Ishga tushmadi"
                bot.reply_to(
                    m,
                    f"✅ <b>Bot yaratildi!</b>\n\n"
                    f"🤖 @{info['username']}\n"
                    f"ID: {bid}\n"
                    f"Owner: {owner_id}\n"
                    f"Status: {status_text}",
                    parse_mode="HTML", reply_markup=mk
                )

            elif state == "b_ad_post":
                set_state(0, uid, "b_ad_confirm", {"msg_id": m.message_id, "chat_id": m.chat.id})
                mk = kb_confirm("b:adok", "b:main")
                bot.reply_to(m, "📣 Barcha foydalanuvchilarga yuboraymi?", reply_markup=mk)

            elif state == "b_admin_add":
                if uid != FORCE_OWNER_ID:
                    clear_state(0, uid)
                    bot.reply_to(m, "⛔ Faqat asosiy egasi.")
                    return
                try:
                    aid = int((m.text or "").strip())
                except (ValueError, TypeError):
                    bot.reply_to(m, "❌ Noto'g'ri ID.")
                    return
                if aid == FORCE_OWNER_ID:
                    bot.reply_to(m, "Bu allaqachon asosiy egasi.")
                    clear_state(0, uid)
                    return
                with db() as conn:
                    with conn.cursor() as c:
                        c.execute("""
                            INSERT INTO builder_users (user_id, username, full_name, is_owner)
                            VALUES (%s, NULL, NULL, 1)
                            ON CONFLICT (user_id) DO UPDATE SET is_owner=1
                        """, (aid,))
                cache_del_prefix("badm:")
                clear_state(0, uid)
                bot.reply_to(m, f"✅ Admin qo'shildi: <code>{aid}</code>", parse_mode="HTML", reply_markup=kb_builder_main())

        except Exception as e:
            print(f"b_state_handler error: {e}\n{traceback.format_exc()}")
            clear_state(0, uid)
            try:
                bot.reply_to(m, f"❌ Xatolik: {str(e)[:100]}")
            except Exception:
                pass

# ==================== MOVIE BOT HANDLERS ====================

def register_movie_handlers(bot: telebot.TeleBot, bot_id: int):
        def check_sub(user_id: int) -> Tuple[bool, List]:
        """
        Har safar tekshiradi (cache yo'q).
        - Public kanal: @username yoki chat_id orqali
        - Private kanal: chat_id orqali (bot ADMIN bo'lishi shart!)
        - External/link: tekshirib bo'lmaydi — o'tkazib yuboriladi
        """
        if not get_setting(bot_id, "forced_sub_enabled", 1):
            return True, []

        ck = f"chs:{bot_id}"
        channels = cache_get(ck)
        if channels is None:
            with db() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute(
                        "SELECT * FROM channels WHERE bot_id=%s AND is_active=1", (bot_id,)
                    )
                    channels = [dict(r) for r in c.fetchall()]
            cache_set(ck, channels)

        if not channels:
            return True, []

        show_list = []
        missing_any = False

        for ch in channels:
            ctype = (ch["channel_type"] or "public").lower()

            # External/link — tekshirib bo'lmaydi, o'tkazib yuboramiz
            if ctype in ("external", "link"):
                continue

            # Chat ID ni aniqlash
            chat_id = ch["chat_id"]
            if not chat_id and ch["username"]:
                chat_id = "@" + ch["username"].lstrip("@")

            if not chat_id:
                # chat_id ham, username ham yo'q — o'tkazib yuboramiz
                continue

            subscribed = False
            try:
                member = bot.get_chat_member(chat_id, user_id)
                if member.status not in ("left", "kicked"):
                    subscribed = True
            except Exception as e:
                # Agar bot admin bo'lmasa yoki kanal topilmasa — xatolik
                print(f"check_sub error ({chat_id}): {e}")
                subscribed = False

            if not subscribed:
                missing_any = True
                show_list.append(ch)

        return (not missing_any), show_list
    def show_forced_sub(chat_id: int, user_id: int, missing: List, edit_msg_id=None):
        text = "📢 <b>Botdan foydalanish uchun quyidagi kanallarga obuna bo'ling:</b>\n\n"
        mk = types.InlineKeyboardMarkup(row_width=1)
        for ch in missing:
            title = ch["title"] or ch["username"] or "Kanal"
            url = ch["url"]
            if not url and ch["username"]:
                url = f"https://t.me/{ch['username'].lstrip('@')}"
            icon = "🔒" if (ch["channel_type"] or "") == "private" else "📢"
            if url:
                btn_text = f"{icon} {title}"
                mk.add(types.InlineKeyboardButton(btn_text, url=url))
            else:
                text += f"• {icon} {title}\n"
        mk.add(types.InlineKeyboardButton("🔄 Tekshirish", callback_data=f"u:checksub:{bot_id}"))
        try:
            if edit_msg_id:
                bot.edit_message_text(text, chat_id, edit_msg_id, parse_mode="HTML", reply_markup=mk)
            else:
                bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=mk)
        except Exception:
            try:
                bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=mk)
            except Exception:
                pass

    def user_main_menu(chat_id: int, user_id: int, edit=False, msg_id=None):
        pro_on = bool(get_setting(bot_id, "pro_enabled", 1))
        text = "🎬 <b>Asosiy menyu</b>\n\nKerakli bo'limni tanlang:"
        mk = kb_user_main(bot_id, pro_on)
        if edit and msg_id:
            try:
                bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=mk)
            except Exception:
                bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=mk)
        else:
            bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=mk)

    @bot.message_handler(commands=["start", "admin"])
    def m_start(m: types.Message):
        try:
            uid = m.from_user.id
            add_user(bot_id, uid, m.from_user.username, m.from_user.full_name)
            if m.text and m.text.startswith("/admin") and is_admin(bot_id, uid):
                bot.send_message(
                    m.chat.id,
                    "🔐 <b>ADMIN PANEL</b>",
                    parse_mode="HTML",
                    reply_markup=kb_admin_main(bot_id)
                )
                return
            ok, show_list = check_sub(uid)
            if not ok:
                show_forced_sub(m.chat.id, uid, show_list)
                return
            user_main_menu(m.chat.id, uid)
        except Exception as e:
            print(f"m_start error: {e}\n{traceback.format_exc()}")

    @bot.callback_query_handler(func=lambda c: c.data and c.data.startswith(("u:", "a:")))
    def m_callback(c: types.CallbackQuery):
        try:
            bot.answer_callback_query(c.id)
            data = c.data
            uid = c.from_user.id
            parts = data.split(":")
            chat_id = c.message.chat.id
            msg_id = c.message.message_id

            # ============ MAJBURIY OBUNA — HAR BIR TUGMADA ============
            # a: (admin) va u:checksub: bundan mustasno
            if not data.startswith("a:") and not data.startswith("u:checksub:"):
                ok, show_list = check_sub(uid)
                if not ok:
                    show_forced_sub(chat_id, uid, show_list, edit_msg_id=msg_id)
                    return
            # ============ MAJBURIY OBUNA TUGADI ============

            if data.startswith("u:checksub:"):
                ok, show_list = check_sub(uid)
                if ok:
                    user_main_menu(chat_id, uid, edit=True, msg_id=msg_id)
                else:
                    show_forced_sub(chat_id, uid, show_list, edit_msg_id=msg_id)

            elif data.startswith("u:main:"):
                user_main_menu(chat_id, uid, edit=True, msg_id=msg_id)

            elif data.startswith("u:top:"):
                page = int(parts[3]) if len(parts) > 3 else 0
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT COUNT(*) AS cnt FROM movies WHERE bot_id=%s", (bot_id,))
                        total = cur.fetchone()["cnt"]
                        cur.execute(
                            "SELECT movie_id, name, code, movie_type, views FROM movies WHERE bot_id=%s ORDER BY views DESC, movie_id DESC LIMIT 10 OFFSET %s",
                            (bot_id, page * 10)
                        )
                        rows = cur.fetchall()
                if not rows:
                    bot.edit_message_text(
                        "🎬 Hozircha kinolar yo'q.",
                        chat_id, msg_id,
                        reply_markup=kb_back(f"u:main:{bot_id}")
                    )
                    return
                text = "🎬 <b>Top kinolar</b>\n\n"
                mk = types.InlineKeyboardMarkup(row_width=5)
                for i, r in enumerate(rows, 1):
                    text += f"{page*10 + i}. {r['name']} ({r['code']})\n"
                    mk.add(types.InlineKeyboardButton(
                        str(page * 10 + i), callback_data=f"u:movie:{bot_id}:{r['movie_id']}"
                    ))
                total_pages = max(1, (total + 9) // 10)
                if total_pages > 1:
                    pag = kb_pagination(f"u:top:{bot_id}", page, total_pages)
                    for row in pag.keyboard:
                        mk.add(*row)
                mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"u:main:{bot_id}"))
                bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=mk)

            elif data.startswith("u:movie:"):
                mid = int(parts[3])
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT * FROM movies WHERE movie_id=%s AND bot_id=%s", (mid, bot_id))
                        r = cur.fetchone()
                if not r:
                    bot.answer_callback_query(c.id, "Kino topilmadi", show_alert=True)
                    return
                pro_on = bool(get_setting(bot_id, "pro_enabled", 1))
                if pro_on and r["movie_type"] == "pro" and not is_pro(bot_id, uid):
                    with db() as conn:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute(
                                "SELECT * FROM plans WHERE bot_id=%s AND is_active=1 ORDER BY days",
                                (bot_id,)
                            )
                            plans = cur.fetchall()
                    if not plans:
                        bot.answer_callback_query(c.id, "💎 Bu PRO kino. Tariflar yo'q.", show_alert=True)
                        return
                    text = "💎 <b>Bu PRO kino</b>\n\nAvval PRO tarifni sotib oling:\n\n"
                    mk = types.InlineKeyboardMarkup(row_width=1)
                    for p in plans:
                        text += f"• {p['name']} — {p['days']} kun — {p['price']} so'm\n"
                        mk.add(types.InlineKeyboardButton(
                            f"{p['name']} ({p['price']} so'm)",
                            callback_data=f"u:plan:{bot_id}:{p['plan_id']}"
                        ))
                    mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"u:main:{bot_id}"))
                    try:
                        bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=mk)
                    except Exception:
                        bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=mk)
                    return
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE movies SET views=views+1 WHERE movie_id=%s", (mid,))
                        cur.execute("UPDATE users SET views=views+1 WHERE bot_id=%s AND user_id=%s", (bot_id, uid))
                caption = f"🎬 <b>{r['name']}</b>\n🔢 Kod: <code>{r['code']}</code>\n"
                if r["caption"]:
                    caption += f"\n{r['caption']}"
                mk = types.InlineKeyboardMarkup(row_width=2)
                mk.add(
                    types.InlineKeyboardButton("💾 Saqlash", callback_data=f"u:save:{bot_id}:{mid}"),
                    types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"u:top:{bot_id}:0"),
                )
                try:
                    bot.send_video(chat_id, r["file_id"], caption=caption, parse_mode="HTML", reply_markup=mk)
                except Exception:
                    bot.send_message(chat_id, caption + "\n\n⚠️ Video yuborilmadi.", parse_mode="HTML", reply_markup=mk)

            elif data.startswith("u:save:"):
                mid = int(parts[3])
                with db() as conn:
                    with conn.cursor() as cur:
                        try:
                            cur.execute(
                                "INSERT INTO saved_movies (bot_id, user_id, movie_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                                (bot_id, uid, mid)
                            )
                            bot.answer_callback_query(c.id, "✅ Saqlandi")
                        except Exception:
                            bot.answer_callback_query(c.id, "Allaqachon saqlangan")

            elif data.startswith("u:search:"):
                set_state(bot_id, uid, "search")
                bot.edit_message_text(
                    "🔎 Kino nomi yoki kodini yuboring:",
                    chat_id, msg_id,
                    reply_markup=kb_back(f"u:main:{bot_id}")
                )

            elif data.startswith("u:saved:"):
                page = int(parts[3]) if len(parts) > 3 else 0
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            "SELECT COUNT(*) AS cnt FROM saved_movies WHERE bot_id=%s AND user_id=%s",
                            (bot_id, uid)
                        )
                        total = cur.fetchone()["cnt"]
                        cur.execute("""
                            SELECT m.movie_id, m.name, m.code FROM saved_movies s
                            JOIN movies m ON s.movie_id=m.movie_id
                            WHERE s.bot_id=%s AND s.user_id=%s
                            ORDER BY s.saved_at DESC LIMIT 10 OFFSET %s
                        """, (bot_id, uid, page * 10))
                        rows = cur.fetchall()
                if not rows:
                    bot.edit_message_text(
                        "💾 Saqlangan kinolar yo'q.",
                        chat_id, msg_id,
                        reply_markup=kb_back(f"u:main:{bot_id}")
                    )
                    return
                text = "💾 <b>Saqlangan kinolar</b>\n\n"
                mk = types.InlineKeyboardMarkup(row_width=1)
                for r in rows:
                    mk.add(types.InlineKeyboardButton(
                        f"🎬 {r['name']}", callback_data=f"u:savedm:{bot_id}:{r['movie_id']}"
                    ))
                total_pages = max(1, (total + 9) // 10)
                if total_pages > 1:
                    pag = kb_pagination(f"u:saved:{bot_id}", page, total_pages)
                    for row in pag.keyboard:
                        mk.add(*row)
                mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"u:main:{bot_id}"))
                bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=mk)

            elif data.startswith("u:savedm:"):
                mid = int(parts[3])
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT * FROM movies WHERE movie_id=%s AND bot_id=%s", (mid, bot_id))
                        r = cur.fetchone()
                if not r:
                    bot.answer_callback_query(c.id, "Topilmadi", show_alert=True)
                    return
                mk = types.InlineKeyboardMarkup(row_width=1)
                mk.add(
                    types.InlineKeyboardButton("▶️ Videoni olish", callback_data=f"u:movie:{bot_id}:{mid}"),
                    types.InlineKeyboardButton("🗑 Olib tashlash", callback_data=f"u:unsave:{bot_id}:{mid}"),
                    types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"u:saved:{bot_id}:0"),
                )
                bot.edit_message_text(
                    f"🎬 <b>{r['name']}</b>\nKod: <code>{r['code']}</code>",
                    chat_id, msg_id, parse_mode="HTML", reply_markup=mk
                )

            elif data.startswith("u:unsave:"):
                mid = int(parts[3])
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "DELETE FROM saved_movies WHERE bot_id=%s AND user_id=%s AND movie_id=%s",
                            (bot_id, uid, mid)
                        )
                bot.answer_callback_query(c.id, "O'chirildi")
                c.data = f"u:saved:{bot_id}:0"
                m_callback(c)

            elif data.startswith("u:plans:"):
                if not get_setting(bot_id, "pro_enabled", 1):
                    bot.answer_callback_query(c.id, "PRO o'chirilgan", show_alert=True)
                    return
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            "SELECT * FROM plans WHERE bot_id=%s AND is_active=1 ORDER BY days",
                            (bot_id,)
                        )
                        plans = cur.fetchall()
                if not plans:
                    bot.edit_message_text("💎 Tariflar yo'q.", chat_id, msg_id, reply_markup=kb_back(f"u:main:{bot_id}"))
                    return
                text = "💎 <b>PRO TARIFLAR</b>\n\n"
                mk = types.InlineKeyboardMarkup(row_width=1)
                for p in plans:
                    text += f"• {p['name']} — {p['days']} kun — {p['price']} so'm\n"
                    mk.add(types.InlineKeyboardButton(
                        f"{p['name']} ({p['price']} so'm)",
                        callback_data=f"u:plan:{bot_id}:{p['plan_id']}"
                    ))
                mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"u:main:{bot_id}"))
                bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=mk)

            elif data.startswith("u:plan:"):
                pid = int(parts[3])
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT * FROM plans WHERE plan_id=%s AND bot_id=%s", (pid, bot_id))
                        p = cur.fetchone()
                if not p:
                    bot.answer_callback_query(c.id, "Topilmadi", show_alert=True)
                    return
                card_num = get_setting(bot_id, "card_number") or "—"
                card_holder = get_setting(bot_id, "card_holder") or "—"
                text = (
                    f"💎 <b>{p['name']}</b>\n\n"
                    f"📅 {p['days']} kun\n"
                    f"💰 <b>{p['price']}</b> so'm\n\n"
                    f"💳 <b>To'lov kartasi:</b>\n"
                    f"🔢 <code>{card_num}</code>\n"
                    f"👤 <b>{card_holder}</b>\n\n"
                    f"To'lov qiling, so'ng «Chek yuborish» ni bosing."
                )
                mk = types.InlineKeyboardMarkup()
                mk.add(types.InlineKeyboardButton("🧾 Chek yuborish", callback_data=f"u:pay:{bot_id}:{pid}"))
                mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"u:plans:{bot_id}"))
                bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=mk)

            elif data.startswith("u:pay:"):
                pid = int(parts[3])
                set_state(bot_id, uid, "pay_check", {"plan_id": pid})
                bot.edit_message_text(
                    "🧾 To'lov chekini (rasm yoki fayl) yuboring:",
                    chat_id, msg_id,
                    reply_markup=kb_back(f"u:plans:{bot_id}")
                )

            # ----- ADMIN -----
            elif data.startswith("a:") and not is_admin(bot_id, uid):
                bot.answer_callback_query(c.id, "⛔ Ruxsat yo'q", show_alert=True)
                return

            elif data.startswith("a:stats:"):
                text = get_bot_stats_text(bot_id)
                bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=kb_back(f"a:main:{bot_id}"))

            elif data.startswith("a:main:"):
                bot.edit_message_text("🔐 <b>ADMIN PANEL</b>", chat_id, msg_id, parse_mode="HTML", reply_markup=kb_admin_main(bot_id))

            elif data.startswith("a:addmovie:"):
                set_state(bot_id, uid, "add_type")
                mk = types.InlineKeyboardMarkup(row_width=2)
                mk.add(
                    types.InlineKeyboardButton("🎬 Oddiy kino", callback_data=f"a:mtype:{bot_id}:normal"),
                    types.InlineKeyboardButton("💎 PRO kino", callback_data=f"a:mtype:{bot_id}:pro"),
                )
                mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"a:main:{bot_id}"))
                bot.edit_message_text("➕ Kino turini tanlang:", chat_id, msg_id, reply_markup=mk)

            elif data.startswith("a:mtype:"):
                mtype = parts[3]
                set_state(bot_id, uid, "add_video", {"type": mtype})
                bot.edit_message_text("📹 Videoni yuboring:", chat_id, msg_id, reply_markup=kb_back(f"a:main:{bot_id}"))

            elif data.startswith("a:movies:"):
                page = int(parts[3]) if len(parts) > 3 else 0
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT COUNT(*) AS cnt FROM movies WHERE bot_id=%s", (bot_id,))
                        total = cur.fetchone()["cnt"]
                        cur.execute(
                            "SELECT movie_id, name, code, movie_type, views FROM movies WHERE bot_id=%s ORDER BY movie_id DESC LIMIT 10 OFFSET %s",
                            (bot_id, page * 10)
                        )
                        rows = cur.fetchall()
                text = "📋 <b>Kinolar ro'yxati</b>\n\n"
                mk = types.InlineKeyboardMarkup(row_width=1)
                for r in rows:
                    t = "💎" if r["movie_type"] == "pro" else "⭐"
                    mk.add(types.InlineKeyboardButton(
                        f"{t} {r['name']} ({r['code']}) 👁{r['views']}",
                        callback_data=f"a:mview:{bot_id}:{r['movie_id']}"
                    ))
                total_pages = max(1, (total + 9) // 10)
                if total_pages > 1:
                    pag = kb_pagination(f"a:movies:{bot_id}", page, total_pages)
                    for row in pag.keyboard:
                        mk.add(*row)
                mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"a:main:{bot_id}"))
                bot.edit_message_text(text or "Bo'sh", chat_id, msg_id, parse_mode="HTML", reply_markup=mk)

            elif data.startswith("a:mview:"):
                mid = int(parts[3])
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT * FROM movies WHERE movie_id=%s AND bot_id=%s", (mid, bot_id))
                        r = cur.fetchone()
                if not r:
                    bot.answer_callback_query(c.id, "Topilmadi", show_alert=True)
                    return
                text = (
                    f"🎬 <b>{r['name']}</b>\n"
                    f"🔢 Kod: <code>{r['code']}</code>\n"
                    f"⭐ Turi: {r['movie_type']}\n"
                    f"👁 Ko'rishlar: {r['views']}\n"
                    f"📅 {r['created_at']}"
                )
                mk = types.InlineKeyboardMarkup()
                mk.add(types.InlineKeyboardButton("🗑 O'chirish", callback_data=f"a:mdel:{bot_id}:{mid}"))
                mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"a:movies:{bot_id}:0"))
                bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=mk)

            elif data.startswith("a:mdel:"):
                mid = int(parts[3])
                bot.edit_message_text(
                    "Haqiqatan o'chirasizmi?",
                    chat_id, msg_id,
                    reply_markup=kb_confirm(f"a:mdelok:{bot_id}:{mid}", f"a:mview:{bot_id}:{mid}")
                )

            elif data.startswith("a:mdelok:"):
                mid = int(parts[3])
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM movies WHERE movie_id=%s AND bot_id=%s", (mid, bot_id))
                bot.edit_message_text("✅ Kino o'chirildi.", chat_id, msg_id, reply_markup=kb_back(f"a:movies:{bot_id}:0"))

            elif data.startswith("a:delmovie:"):
                set_state(bot_id, uid, "del_code")
                bot.edit_message_text("🗑 O'chirish uchun kino kodini yuboring:", chat_id, msg_id, reply_markup=kb_back(f"a:main:{bot_id}"))

            elif data.startswith("a:broadcast:"):
                set_state(bot_id, uid, "broadcast")
                bot.edit_message_text(
                    "📢 Barchaga yubormoqchi bo'lgan xabarni yuboring:",
                    chat_id, msg_id,
                    reply_markup=kb_back(f"a:main:{bot_id}")
                )

            elif data.startswith("a:addch:"):
                set_state(bot_id, uid, "add_channel")
                bot.edit_message_text(
                    "📢 Kanaldan xabarni forward qiling yoki link yuboring:",
                    chat_id, msg_id,
                    reply_markup=kb_back(f"a:main:{bot_id}")
                )

            elif data.startswith("a:chs:"):
                page = int(parts[3]) if len(parts) > 3 else 0
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            "SELECT * FROM channels WHERE bot_id=%s ORDER BY channel_id LIMIT 10 OFFSET %s",
                            (bot_id, page * 10)
                        )
                        rows = cur.fetchall()
                text = "📋 <b>Kanallar</b>\n\n"
                mk = types.InlineKeyboardMarkup(row_width=1)
                for r in rows:
                    icon = "🔒" if r["channel_type"] == "private" else ("🔗" if r["channel_type"] == "external" else "📢")
                    mk.add(types.InlineKeyboardButton(
                        f"{icon} {r['title'] or r['username'] or r['url']}",
                        callback_data=f"a:chview:{bot_id}:{r['channel_id']}"
                    ))
                mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"a:main:{bot_id}"))
                bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=mk)

            elif data.startswith("a:chview:"):
                cid = int(parts[3])
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT * FROM channels WHERE channel_id=%s AND bot_id=%s", (cid, bot_id))
                        r = cur.fetchone()
                if not r:
                    return
                text = f"📢 {r['title'] or r['username']}\nTuri: {r['channel_type']}\nURL: {r['url'] or '-'}"
                mk = types.InlineKeyboardMarkup()
                mk.add(types.InlineKeyboardButton("🗑 O'chirish", callback_data=f"a:chdel:{bot_id}:{cid}"))
                mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"a:chs:{bot_id}:0"))
                bot.edit_message_text(text, chat_id, msg_id, reply_markup=mk)

            elif data.startswith("a:chdel:"):
                cid = int(parts[3])
                bot.edit_message_text(
                    "O'chirasizmi?",
                    chat_id, msg_id,
                    reply_markup=kb_confirm(f"a:chdelok:{bot_id}:{cid}", f"a:chs:{bot_id}:0")
                )

            elif data.startswith("a:chdelok:"):
                cid = int(parts[3])
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM channels WHERE channel_id=%s AND bot_id=%s", (cid, bot_id))
                bot.edit_message_text("✅ O'chirildi", chat_id, msg_id, reply_markup=kb_back(f"a:chs:{bot_id}:0"))

            elif data.startswith("a:plans:"):
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT * FROM plans WHERE bot_id=%s ORDER BY days", (bot_id,))
                        plans = cur.fetchall()
                text = "💎 <b>Tariflar</b>\n\n"
                mk = types.InlineKeyboardMarkup(row_width=1)
                for p in plans:
                    text += f"• {p['name']} — {p['days']}k — {p['price']} so'm\n"
                    mk.add(types.InlineKeyboardButton(
                        f"🗑 {p['name']}", callback_data=f"a:plandel:{bot_id}:{p['plan_id']}"
                    ))
                mk.add(types.InlineKeyboardButton("➕ Tarif qo'shish", callback_data=f"a:planadd:{bot_id}"))
                mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"a:main:{bot_id}"))
                bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=mk)

            elif data.startswith("a:planadd:"):
                set_state(bot_id, uid, "plan_name")
                bot.edit_message_text("💎 Tarif nomini yuboring:", chat_id, msg_id, reply_markup=kb_back(f"a:plans:{bot_id}"))

            elif data.startswith("a:plandel:"):
                pid = int(parts[3])
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM plans WHERE plan_id=%s AND bot_id=%s", (pid, bot_id))
                bot.answer_callback_query(c.id, "O'chirildi")
                c.data = f"a:plans:{bot_id}"
                m_callback(c)

            elif data.startswith("a:admins:"):
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT owner_id FROM bots WHERE bot_id=%s", (bot_id,))
                        owner = cur.fetchone()["owner_id"]
                        cur.execute("SELECT user_id FROM bot_admins WHERE bot_id=%s", (bot_id,))
                        ads = cur.fetchall()
                text = f"👥 <b>Adminlar</b>\n\n👤 Owner: {owner}\n"
                mk = types.InlineKeyboardMarkup(row_width=1)
                for a in ads:
                    if a["user_id"] != owner:
                        mk.add(types.InlineKeyboardButton(
                            f"👤 {a['user_id']}", callback_data=f"a:admindel:{bot_id}:{a['user_id']}"
                        ))
                mk.add(types.InlineKeyboardButton("➕ Admin qo'shish", callback_data=f"a:adminadd:{bot_id}"))
                mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"a:main:{bot_id}"))
                bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=mk)

            elif data.startswith("a:adminadd:"):
                set_state(bot_id, uid, "admin_add")
                bot.edit_message_text("👤 Yangi admin Telegram ID sini yuboring:", chat_id, msg_id, reply_markup=kb_back(f"a:admins:{bot_id}"))

            elif data.startswith("a:admindel:"):
                aid = int(parts[3])
                if is_owner(bot_id, aid):
                    bot.answer_callback_query(c.id, "Ownerni o'chirib bo'lmaydi", show_alert=True)
                    return
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM bot_admins WHERE bot_id=%s AND user_id=%s", (bot_id, aid))
                bot.answer_callback_query(c.id, "O'chirildi")
                c.data = f"a:admins:{bot_id}"
                m_callback(c)

            elif data.startswith("a:settings:"):
                pro = "ON" if get_setting(bot_id, "pro_enabled", 1) else "OFF"
                fsub = "ON" if get_setting(bot_id, "forced_sub_enabled", 1) else "OFF"
                pay = "ON" if get_setting(bot_id, "payments_enabled", 1) else "OFF"
                notif = "ON" if get_setting(bot_id, "admin_notify", 1) else "OFF"
                card_num = get_setting(bot_id, "card_number") or "—"
                card_holder = get_setting(bot_id, "card_holder") or "—"
                text = (
                    f"⚙️ <b>SOZLAMALAR</b>\n\n"
                    f"💎 PRO: {pro}\n"
                    f"📢 Obuna: {fsub}\n"
                    f"💳 To'lov: {pay}\n"
                    f"🔔 Notify: {notif}\n\n"
                    f"💳 <b>Karta</b>\n"
                    f"🔢 <code>{card_num}</code>\n"
                    f"👤 {card_holder}"
                )
                mk = types.InlineKeyboardMarkup(row_width=1)
                mk.add(
                    types.InlineKeyboardButton(f"💎 PRO: {pro}", callback_data=f"a:tog:{bot_id}:pro_enabled"),
                    types.InlineKeyboardButton(f"📢 Obuna: {fsub}", callback_data=f"a:tog:{bot_id}:forced_sub_enabled"),
                    types.InlineKeyboardButton(f"💳 To'lov: {pay}", callback_data=f"a:tog:{bot_id}:payments_enabled"),
                    types.InlineKeyboardButton(f"🔔 Notify: {notif}", callback_data=f"a:tog:{bot_id}:admin_notify"),
                    types.InlineKeyboardButton("💳 Karta qo'shish", callback_data=f"a:card:{bot_id}"),
                    types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"a:main:{bot_id}"),
                )
                bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=mk)

            elif data.startswith("a:card:"):
                set_state(bot_id, uid, "card_number")
                bot.edit_message_text(
                    "💳 Karta raqamini yuboring (faqat raqamlar):",
                    chat_id, msg_id,
                    reply_markup=kb_back(f"a:settings:{bot_id}")
                )

            elif data.startswith("a:tog:"):
                key = parts[3]
                cur = get_setting(bot_id, key, 1)
                set_setting(bot_id, **{key: 0 if cur else 1})
                c.data = f"a:settings:{bot_id}"
                m_callback(c)

            elif data.startswith("a:payok:"):
                pid = int(parts[3])
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT * FROM payments WHERE payment_id=%s", (pid,))
                        pay = cur.fetchone()
                        if not pay or pay["status"] != "pending":
                            bot.answer_callback_query(c.id, "Allaqachon qayta ishlangan", show_alert=True)
                            return
                        cur.execute("SELECT * FROM plans WHERE plan_id=%s", (pay["plan_id"],))
                        plan = cur.fetchone()
                        days = plan["days"] if plan else 30
                        start = datetime.now()
                        end = start + timedelta(days=days)
                        cur.execute(
                            "UPDATE users SET is_pro=1, pro_start=%s, pro_end=%s WHERE bot_id=%s AND user_id=%s",
                            (start.isoformat(), end.isoformat(), bot_id, pay["user_id"])
                        )
                        cur.execute(
                            "UPDATE payments SET status='approved', admin_id=%s, processed_at=%s WHERE payment_id=%s",
                            (uid, datetime.now().isoformat(), pid)
                        )
                try:
                    bot.send_message(pay["user_id"], f"✅ PRO tasdiqlandi!\n📅 {end.strftime('%Y-%m-%d')}")
                except Exception:
                    pass
                bot.edit_message_text("✅ Tasdiqlandi", chat_id, msg_id)

            elif data.startswith("a:payno:"):
                pid = int(parts[3])
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT * FROM payments WHERE payment_id=%s", (pid,))
                        pay = cur.fetchone()
                        if pay:
                            cur.execute(
                                "UPDATE payments SET status='rejected', admin_id=%s, processed_at=%s WHERE payment_id=%s",
                                (uid, datetime.now().isoformat(), pid)
                            )
                            try:
                                bot.send_message(pay["user_id"], "❌ To'lovingiz rad etildi.")
                            except Exception:
                                pass
                bot.edit_message_text("❌ Rad etildi", chat_id, msg_id)

        except Exception as e:
            print(f"Movie callback error bot={bot_id}: {e}\n{traceback.format_exc()}")
            try:
                bot.answer_callback_query(c.id, "Xatolik", show_alert=True)
            except Exception:
                pass

    @bot.message_handler(
        content_types=["text", "photo", "video", "document", "audio", "voice", "sticker", "animation"],
        func=lambda m: get_state(bot_id, m.from_user.id)[0] is not None
    )
    def m_state(m: types.Message):
        uid = m.from_user.id
        state, data = get_state(bot_id, uid)
        try:
            # ============ MAJBURIY OBUNA — MATNLI XABARLARDA ============
            # Faqat foydalanuvchi holatlari uchun (admin holatlari emas)
            user_states = ("search", "premium_amount", "premium_check", "pay_check")
            if state in user_states:
                ok, show_list = check_sub(uid)
                if not ok:
                    clear_state(bot_id, uid)
                    show_forced_sub(m.chat.id, uid, show_list)
                    return
            # ============ MAJBURIY OBUNA TUGADI ============

            if state == "search":
                q = (m.text or "").strip()
                if not q:
                    bot.reply_to(m, "Matn yuboring.")
                    return
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""
                            SELECT movie_id, name, code FROM movies
                            WHERE bot_id=%s AND (name ILIKE %s OR code ILIKE %s)
                            LIMIT 20
                        """, (bot_id, f"%{q}%", f"%{q}%"))
                        rows = cur.fetchall()
                        cur.execute("UPDATE users SET searches=searches+1 WHERE bot_id=%s AND user_id=%s", (bot_id, uid))
                clear_state(bot_id, uid)
                if not rows:
                    bot.reply_to(m, "❌ Kino topilmadi.", reply_markup=kb_back(f"u:main:{bot_id}"))
                    return
                text = "🔎 <b>Qidiruv natijalari</b>\n\n"
                mk = types.InlineKeyboardMarkup(row_width=1)
                for r in rows:
                    mk.add(types.InlineKeyboardButton(
                        f"🎬 {r['name']} ({r['code']})",
                        callback_data=f"u:movie:{bot_id}:{r['movie_id']}"
                    ))
                mk.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"u:main:{bot_id}"))
                bot.reply_to(m, text, parse_mode="HTML", reply_markup=mk)

            elif state == "premium_amount":
                try:
                    amount = int((m.text or "").strip().replace(" ", "").replace(",", ""))
                    if amount < 1000:
                        bot.reply_to(m, "❌ Minimal 1000 so'm.")
                        return
                except (ValueError, TypeError):
                    bot.reply_to(m, "❌ Raqam yuboring.")
                    return
                set_state(bot_id, uid, "premium_check", {"amount": amount})
                bot.reply_to(m, f"💰 Summa: <b>{amount}</b> so'm\n\n🧾 Chekni yuboring:", parse_mode="HTML")

            elif state == "premium_check":
                file_id = None
                if m.photo:
                    file_id = m.photo[-1].file_id
                elif m.document:
                    file_id = m.document.file_id
                if not file_id:
                    bot.reply_to(m, "Rasm yoki fayl yuboring.")
                    return
                amount = data.get("amount", 0)
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            "INSERT INTO payments (bot_id, user_id, plan_id, amount, check_file_id, status) "
                            "VALUES (%s, %s, NULL, %s, %s, 'pending') RETURNING payment_id",
                            (bot_id, uid, amount, file_id)
                        )
                        pay_id = cur.fetchone()["payment_id"]
                        cur.execute("SELECT owner_id FROM bots WHERE bot_id=%s", (bot_id,))
                        owner = cur.fetchone()["owner_id"]
                        cur.execute("SELECT user_id FROM bot_admins WHERE bot_id=%s", (bot_id,))
                        admins = [r["user_id"] for r in cur.fetchall()]
                        admins.append(owner)
                clear_state(bot_id, uid)
                bot.reply_to(m, "✅ Chek yuborildi.")
                mk = types.InlineKeyboardMarkup(row_width=2)
                mk.add(
                    types.InlineKeyboardButton("✅ Tasdiqlash", callback_data=f"a:payok:{bot_id}:{pay_id}"),
                    types.InlineKeyboardButton("❌ Rad etish", callback_data=f"a:payno:{bot_id}:{pay_id}"),
                )
                text = f"💳 <b>Yangi PREMIUM to'lov</b>\n\n👤 {uid}\n💰 {amount} so'm"
                for aid in set(admins):
                    try:
                        if m.photo:
                            bot.send_photo(aid, file_id, caption=text, parse_mode="HTML", reply_markup=mk)
                        else:
                            bot.send_document(aid, file_id, caption=text, parse_mode="HTML", reply_markup=mk)
                    except Exception:
                        pass

            elif state == "add_video":
                file_id = None
                if m.video:
                    file_id = m.video.file_id
                elif m.document:
                    file_id = m.document.file_id
                if not file_id:
                    bot.reply_to(m, "Video yoki document yuboring.")
                    return
                data["file_id"] = file_id
                set_state(bot_id, uid, "add_name", data)
                bot.reply_to(m, "🎬 Kino nomini yuboring:")

            elif state == "add_name":
                data["name"] = (m.text or "").strip()
                set_state(bot_id, uid, "add_code", data)
                bot.reply_to(m, "🔢 Kino kodini yuboring (unique):")

            elif state == "add_code":
                code = (m.text or "").strip()
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1 FROM movies WHERE bot_id=%s AND code=%s", (bot_id, code))
                        if cur.fetchone():
                            bot.reply_to(m, "❌ Bu kod mavjud. Boshqa kod yuboring.")
                            return
                data["code"] = code
                set_state(bot_id, uid, "add_caption", data)
                bot.reply_to(m, "📝 Caption yuboring (yoki - deb yozing):")

            elif state == "add_caption":
                caption = (m.text or "").strip() if m.text != "-" else ""
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO movies (bot_id, file_id, name, code, caption, movie_type, added_by)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (bot_id, data["file_id"], data["name"], data["code"], caption, data["type"], uid))
                clear_state(bot_id, uid)
                mk = types.InlineKeyboardMarkup()
                mk.add(
                    types.InlineKeyboardButton("➕ Yana qo'shish", callback_data=f"a:addmovie:{bot_id}"),
                    types.InlineKeyboardButton("⬅️ Orqaga", callback_data=f"a:main:{bot_id}"),
                )
                bot.reply_to(m, "✅ Kino qo'shildi!", reply_markup=mk)

            elif state == "del_code":
                code = (m.text or "").strip()
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT movie_id, name FROM movies WHERE bot_id=%s AND code=%s", (bot_id, code))
                        r = cur.fetchone()
                clear_state(bot_id, uid)
                if not r:
                    bot.reply_to(m, "❌ Kino topilmadi.", reply_markup=kb_back(f"a:main:{bot_id}"))
                    return
                mk = kb_confirm(f"a:mdelok:{bot_id}:{r['movie_id']}", f"a:main:{bot_id}")
                bot.reply_to(m, f"🎬 {r['name']}\n🔢 {code}\n\nO'chirasizmi?", reply_markup=mk)

            elif state == "broadcast":
                set_state(bot_id, uid, "broadcast_confirm", {"msg_id": m.message_id, "chat_id": m.chat.id})
                mk = kb_confirm(f"a:bcok:{bot_id}", f"a:main:{bot_id}")
                bot.reply_to(m, "📢 Barchaga yuboraymi?", reply_markup=mk)

            elif state == "add_channel":
                if m.forward_from_chat and m.forward_from_chat.type == "channel":
                    ch = m.forward_from_chat
                    chat_id_str = str(ch.id)
                    username = ch.username
                    title = ch.title or "Kanal"
                    if username:
                        url = f"https://t.me/{username.lstrip('@')}"
                        with db() as conn:
                            with conn.cursor() as cur:
                                cur.execute("""
                                    INSERT INTO channels (bot_id, chat_id, username, title, channel_type, url)
                                    VALUES (%s, %s, %s, %s, 'public', %s)
                                """, (bot_id, chat_id_str, username, title, url))
                        cache_del_prefix(f"chs:{bot_id}")
                        clear_state(bot_id, uid)
                        bot.reply_to(m, f"✅ Kanal qo'shildi: {title}", reply_markup=kb_back(f"a:main:{bot_id}"))
                    else:
                        set_state(bot_id, uid, "add_channel_invite", {"chat_id": chat_id_str, "title": title})
                        bot.reply_to(m, f"🔒 {title}\n\nEndi invite link yuboring:")
                elif m.text and (m.text.startswith("http") or "t.me/" in m.text.lower()):
                    url = m.text.strip()
                    with db() as conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO channels (bot_id, title, channel_type, url)
                                VALUES (%s, %s, 'external', %s)
                            """, (bot_id, url, url))
                    cache_del_prefix(f"chs:{bot_id}")
                    clear_state(bot_id, uid)
                    bot.reply_to(m, "✅ Link qo'shildi.", reply_markup=kb_back(f"a:main:{bot_id}"))
                else:
                    bot.reply_to(m, "Kanal forward qiling yoki link yuboring.")

            elif state == "add_channel_invite":
                url = (m.text or "").strip()
                if not url or ("t.me/" not in url and "telegram.me/" not in url):
                    bot.reply_to(m, "❌ To'g'ri invite link yuboring")
                    return
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO channels (bot_id, chat_id, title, channel_type, url)
                            VALUES (%s, %s, %s, 'private', %s)
                        """, (bot_id, data.get("chat_id"), data.get("title", "Maxfiy kanal"), url))
                cache_del_prefix(f"chs:{bot_id}")
                clear_state(bot_id, uid)
                bot.reply_to(m, f"✅ Maxfiy kanal qo'shildi!\nLink: {url}", reply_markup=kb_back(f"a:main:{bot_id}"))

            elif state == "plan_name":
                data["name"] = (m.text or "").strip()
                set_state(bot_id, uid, "plan_days", data)
                bot.reply_to(m, "📅 Kunlar sonini yuboring (masalan: 30):")

            elif state == "plan_days":
                try:
                    days = int((m.text or "").strip())
                except (ValueError, TypeError):
                    bot.reply_to(m, "Raqam yuboring.")
                    return
                data["days"] = days
                set_state(bot_id, uid, "plan_price", data)
                bot.reply_to(m, "💰 Narxni so'mda yuboring:")

            elif state == "plan_price":
                try:
                    price = int((m.text or "").strip().replace(" ", ""))
                except (ValueError, TypeError):
                    bot.reply_to(m, "Raqam yuboring.")
                    return
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO plans (bot_id, name, days, price) VALUES (%s, %s, %s, %s)",
                            (bot_id, data["name"], data["days"], price)
                        )
                clear_state(bot_id, uid)
                bot.reply_to(m, "✅ Tarif qo'shildi!", reply_markup=kb_back(f"a:plans:{bot_id}"))

            elif state == "admin_add":
                try:
                    aid = int((m.text or "").strip())
                except (ValueError, TypeError):
                    bot.reply_to(m, "Noto'g'ri ID.")
                    return
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO bot_admins (bot_id, user_id, permissions, added_by) "
                            "VALUES (%s, %s, 'full', %s) ON CONFLICT DO NOTHING",
                            (bot_id, aid, uid)
                        )
                clear_state(bot_id, uid)
                bot.reply_to(m, f"✅ Admin qo'shildi: {aid}", reply_markup=kb_back(f"a:admins:{bot_id}"))

            elif state == "card_number":
                num = (m.text or "").strip().replace(" ", "")
                if not num.isdigit() or len(num) < 12:
                    bot.reply_to(m, "❌ To'g'ri karta raqamini yuboring.")
                    return
                set_state(bot_id, uid, "card_holder", {"card_number": num})
                bot.reply_to(m, f"✅ Karta: <code>{num}</code>\n\n👤 Endi karta egasining ism familiyasini yuboring:", parse_mode="HTML")

            elif state == "card_holder":
                name = (m.text or "").strip()
                if len(name) < 3:
                    bot.reply_to(m, "❌ Ism familiyani to'liq yozing.")
                    return
                num = data.get("card_number", "")
                set_setting(bot_id, card_number=num, card_holder=name)
                clear_state(bot_id, uid)
                bot.reply_to(
                    m,
                    f"✅ <b>Karta saqlandi!</b>\n\n🔢 <code>{num}</code>\n👤 <b>{name}</b>",
                    parse_mode="HTML",
                    reply_markup=kb_back(f"a:settings:{bot_id}")
                )

            elif state == "pay_check":
                file_id = None
                if m.photo:
                    file_id = m.photo[-1].file_id
                elif m.document:
                    file_id = m.document.file_id
                if not file_id:
                    bot.reply_to(m, "Rasm yoki fayl yuboring.")
                    return
                plan_id = data.get("plan_id")
                with db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT * FROM plans WHERE plan_id=%s", (plan_id,))
                        plan = cur.fetchone()
                        amount = plan["price"] if plan else 0
                        cur.execute(
                            "INSERT INTO payments (bot_id, user_id, plan_id, amount, check_file_id, status) "
                            "VALUES (%s, %s, %s, %s, %s, 'pending') RETURNING payment_id",
                            (bot_id, uid, plan_id, amount, file_id)
                        )
                        pay_id = cur.fetchone()["payment_id"]
                        cur.execute("SELECT owner_id FROM bots WHERE bot_id=%s", (bot_id,))
                        owner = cur.fetchone()["owner_id"]
                        cur.execute("SELECT user_id FROM bot_admins WHERE bot_id=%s", (bot_id,))
                        admins = [r["user_id"] for r in cur.fetchall()]
                        admins.append(owner)
                clear_state(bot_id, uid)
                bot.reply_to(m, "✅ Chek yuborildi.")
                mk = types.InlineKeyboardMarkup(row_width=2)
                mk.add(
                    types.InlineKeyboardButton("✅ Tasdiqlash", callback_data=f"a:payok:{bot_id}:{pay_id}"),
                    types.InlineKeyboardButton("❌ Rad etish", callback_data=f"a:payno:{bot_id}:{pay_id}"),
                )
                text = f"💳 <b>Yangi PRO to'lov</b>\n\n👤 {uid}\n💎 {plan['name'] if plan else '-'}\n💰 {amount} so'm"
                for aid in set(admins):
                    try:
                        if m.photo:
                            bot.send_photo(aid, file_id, caption=text, parse_mode="HTML", reply_markup=mk)
                        else:
                            bot.send_document(aid, file_id, caption=text, parse_mode="HTML", reply_markup=mk)
                    except Exception:
                        pass

        except Exception as e:
            print(f"State error: {e}\n{traceback.format_exc()}")
            clear_state(bot_id, uid)
            bot.reply_to(m, f"❌ Xatolik: {str(e)[:100]}")

    @bot.callback_query_handler(func=lambda c: c.data and c.data.startswith(f"a:bcok:{bot_id}"))
    def do_broadcast(c: types.CallbackQuery):
        try:
            bot.answer_callback_query(c.id)
            uid = c.from_user.id
            state, data = get_state(bot_id, uid)
            if state != "broadcast_confirm":
                return
            clear_state(bot_id, uid)
            src_chat = data["chat_id"]
            src_msg = data["msg_id"]
            with db() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT user_id FROM users WHERE bot_id=%s", (bot_id,))
                    users = cur.fetchall()
            ok = 0
            err = 0
            for u in users:
                try:
                    bot.copy_message(u["user_id"], src_chat, src_msg)
                    ok += 1
                    time.sleep(0.05)
                except Exception:
                    err += 1
            bot.edit_message_text(
                f"📊 Tugadi.\n\n✅ {ok} | ❌ {err}",
                c.message.chat.id, c.message.message_id,
                reply_markup=kb_back(f"a:main:{bot_id}")
            )
        except Exception as e:
            print(f"do_broadcast error: {e}")

def get_bot_stats_text(bot_id: int) -> str:
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT COUNT(*) FROM users WHERE bot_id=%s", (bot_id,))
            total_u = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM users WHERE bot_id=%s AND is_pro=1", (bot_id,))
            pro_u = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM movies WHERE bot_id=%s", (bot_id,))
            total_m = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM movies WHERE bot_id=%s AND movie_type='normal'", (bot_id,))
            normal_m = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM movies WHERE bot_id=%s AND movie_type='pro'", (bot_id,))
            pro_m = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM channels WHERE bot_id=%s", (bot_id,))
            chs = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM bot_admins WHERE bot_id=%s", (bot_id,))
            ads = c.fetchone()[0]
            c.execute("SELECT COALESCE(SUM(searches),0) FROM users WHERE bot_id=%s", (bot_id,))
            searches = c.fetchone()[0]
            c.execute("SELECT COALESCE(SUM(views),0) FROM movies WHERE bot_id=%s", (bot_id,))
            views = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM saved_movies WHERE bot_id=%s", (bot_id,))
            saved = c.fetchone()[0]
            week_ago = (datetime.now() - timedelta(days=7)).isoformat()
            c.execute(
                "SELECT COUNT(*) FROM users WHERE bot_id=%s AND last_active > %s",
                (bot_id, week_ago)
            )
            active = c.fetchone()[0]
    return (
        f"📊 <b>STATISTIKA</b>\n\n"
        f"👥 Jami: {total_u}\n"
        f"🟢 Faol (7 kun): {active}\n"
        f"🎬 Jami kinolar: {total_m}\n"
        f"⭐ Oddiy: {normal_m}\n"
        f"💎 PRO kinolar: {pro_m}\n"
        f"💎 PRO foydalanuvchilar: {pro_u}\n"
        f"📺 Kanallar: {chs}\n"
        f"👨‍💼 Adminlar: {ads}\n"
        f"🔎 Qidiruvlar: {searches}\n"
        f"▶️ Ko'rishlar: {views}\n"
        f"💾 Saqlangan: {saved}"
    )

# ==================== FLASK WEBHOOK ====================

@app.route("/")
def index():
    return "🎬 Kino Bot Builder is running", 200

@app.route("/health")
def health():
    return jsonify({"status": "ok", "bots": list(running_bots.keys())}), 200

@app.route("/webhook/builder", methods=["POST"])
def webhook_builder():
    if request.headers.get("content-type") == "application/json":
        try:
            json_string = request.get_data().decode("utf-8")
            update = telebot.types.Update.de_json(json_string)
            if builder_bot and update:
                builder_bot.process_new_updates([update])
            return "", 200
        except Exception as e:
            print(f"Builder webhook error: {e}")
            return "", 200
    return "", 403

@app.route("/webhook/movie/<int:bot_id>", methods=["POST"])
def webhook_movie(bot_id: int):
    if request.headers.get("content-type") == "application/json":
        try:
            with bots_lock:
                entry = running_bots.get(bot_id)
            if not entry:
                return "", 200
            json_string = request.get_data().decode("utf-8")
            update = telebot.types.Update.de_json(json_string)
            if update:
                entry["bot"].process_new_updates([update])
            return "", 200
        except Exception as e:
            print(f"Movie webhook {bot_id} error: {e}")
            return "", 200
    return "", 403

# ==================== MAIN ====================

def setup_webhooks():
    global builder_bot
    base = WEBHOOK_BASE or os.environ.get("RENDER_EXTERNAL_URL", "")
    if not base:
        print("⚠️ WEBHOOK_BASE not set")
        return
    try:
        builder_bot.remove_webhook()
        time.sleep(0.5)
        url = f"{base.rstrip('/')}/webhook/builder"
        builder_bot.set_webhook(url=url)
        print(f"✅ Builder webhook: {url}")
    except Exception as e:
        print(f"Builder webhook set error: {e}")

_bootstrapped = False

def bootstrap():
    global builder_bot, _bootstrapped
    if _bootstrapped:
        return
    _bootstrapped = True
    print("🎬 Kino Bot Builder ishga tushmoqda...")
    try:
        init_pool()
        init_db()
    except Exception as e:
        print(f"❌ DB ulanish xatosi: {e}")
    builder_bot = telebot.TeleBot(BUILDER_TOKEN, threaded=False)
    register_builder_handlers(builder_bot)
    try:
        restart_active_bots()
    except Exception as e:
        print(f"restart_active_bots error: {e}")
    try:
        setup_webhooks()
    except Exception as e:
        print(f"setup_webhooks error: {e}")
    print("✅ Bootstrap complete")

bootstrap()

if __name__ == "__main__":
    print(f"✅ Flask listening on 0.0.0.0:{PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
