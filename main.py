import asyncio

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import base64
import csv
import os
import random
import re
from collections import defaultdict

from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ChatType
from pyrogram.types import (
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from pytgcalls import PyTgCalls
from pytgcalls.types import AudioQuality, MediaStream
from rapidfuzz import fuzz, process
from yt_dlp import YoutubeDL

import db
import panel

load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
SESSION_STRING = os.environ["SESSION_STRING"]
PROXY = os.environ.get("PROXY")  # optional, e.g. http://user:pass@host:port
COOKIES = "cookies.txt"  # optional (dummy Google account ki cookies)
# Railway: cookies.txt ka content COOKIES_B64 (base64) ya COOKIES_TXT (plain text) variable me daalo
if os.environ.get("COOKIES_B64"):
    open(COOKIES, "wb").write(base64.b64decode(os.environ["COOKIES_B64"]))
elif os.environ.get("COOKIES_TXT"):
    open(COOKIES, "w", encoding="utf-8").write(os.environ["COOKIES_TXT"])
SONGS_CSV = "songs.csv"
RADIO_COUNT = 10

bot = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
assistant = Client("assistant", api_id=API_ID, api_hash=API_HASH,
                   session_string=SESSION_STRING, in_memory=True)
call = PyTgCalls(assistant)

# Sab kuch RAM me. Restart par sab saaf, koi record nahi.
queues: dict[int, list[dict]] = {}
loops: dict[int, bool] = {}
dm_state: dict[int, dict] = {}  # radio menu state: user_id -> {"chat", "lang"}
BOT_USERNAME = ""
BOT_ID = 0

BAD_WORDS = ["remix", "cover", "live", "slowed", "reverb", "karaoke",
             "instrumental", "8d", "lofi", "sped up", "nightcore", "reaction"]
GOOD_WORDS = ["official audio", "official video", "official music video", "audio", "lyrical"]


# ---------------- Radio data (songs.csv) ----------------
SPLIT = re.compile(r"\s+(?:&|x|[Ff]eat\.?|[Ff]t\.?)\s+|\s*,\s+")
LANG_ORDER = ["Hindi", "Punjabi", "Haryanvi", "English"]
LANGS: list[str] = []
SONGS: dict[str, dict[str, list[tuple]]] = {}  # lang -> artist -> [(title, full_artist)]
ARTISTS: dict[str, list[str]] = {}  # lang -> artists sorted by song count
LANG_ALL: dict[str, list[tuple]] = {}  # lang -> every song


def load_songs():
    if not os.path.exists(SONGS_CSV):
        print("songs.csv not found, /radio disabled")
        return
    tmp: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    allsongs: dict[str, list] = defaultdict(list)
    with open(SONGS_CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            title, full, lang = r["title"].strip(), r["artist"].strip(), r["language"].strip()
            if not (title and full and lang):
                continue
            allsongs[lang].append((title, full))
            for a in SPLIT.split(full):
                tmp[lang][a.strip()].append((title, full))
    langs = [l for l in LANG_ORDER if l in tmp] + [l for l in tmp if l not in LANG_ORDER]
    LANGS.extend(langs)
    for l in langs:
        SONGS[l] = dict(tmp[l])
        ARTISTS[l] = sorted(tmp[l], key=lambda a: (-len(tmp[l][a]), a.lower()))
        LANG_ALL[l] = allsongs[l]


load_songs()


# ---------------- Search ----------------
def _ydl(extra=None):
    opts = {"quiet": True, "no_warnings": True, "noplaylist": True}
    if os.path.exists(COOKIES):
        opts["cookiefile"] = COOKIES
    if PROXY:
        opts["proxy"] = PROXY
    if extra:
        opts.update(extra)
    return YoutubeDL(opts)


def _score(query: str, e: dict) -> float:
    title = (e.get("title") or "").lower()
    channel = (e.get("channel") or e.get("uploader") or "").lower()
    q = query.lower()
    s = fuzz.token_set_ratio(q, f"{title} {channel}")
    if any(w in title for w in GOOD_WORDS):
        s += 8
    if "topic" in channel or "vevo" in channel or "official" in channel:
        s += 8
    for w in BAD_WORDS:
        if w in title and w not in q:
            s -= 25
    dur = e.get("duration") or 0
    if dur > 900:  # 15 min se lamba = mix/podcast
        s -= 40
    if 0 < dur < 45:
        s -= 30
    return s


def _search_sync(query: str) -> dict | None:
    for prefix in ("ytsearch5", "scsearch5"):  # YouTube, fir SoundCloud fallback
        try:
            with _ydl({"extract_flat": True}) as y:
                info = y.extract_info(f"{prefix}:{query}", download=False)
            entries = [e for e in (info.get("entries") or []) if e]
            if not entries:
                continue
            best = max(entries, key=lambda e: _score(query, e))
            url = best.get("url") or best.get("webpage_url")
            if url and not url.startswith("http"):
                url = f"https://www.youtube.com/watch?v={best['id']}"
            return {"title": best.get("title") or query, "page": url}
        except Exception:
            continue
    return None


def _stream_sync(page: str) -> tuple[str, str]:
    with _ydl({"format": "bestaudio/best"}) as y:
        info = y.extract_info(page, download=False)
    return info["url"], info.get("title") or page


async def run(fn, *a):
    return await asyncio.get_running_loop().run_in_executor(None, fn, *a)


# ---------------- Player ----------------
def buttons(chat_id: int) -> InlineKeyboardMarkup:
    lp = "🔁 Loop: ON" if loops.get(chat_id) else "🔁 Loop"
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⏸ Pause", callback_data="pause"),
        InlineKeyboardButton("▶️ Resume", callback_data="resume"),
        InlineKeyboardButton("⏭ Skip", callback_data="skip"),
    ], [
        InlineKeyboardButton("⏹ Stop", callback_data="stop"),
        InlineKeyboardButton(lp, callback_data="loop"),
    ]])


async def ensure_assistant(chat_id: int):
    try:
        await assistant.get_chat_member(chat_id, "me")
    except Exception:
        link = await bot.export_chat_invite_link(chat_id)
        await assistant.join_chat(link)


async def play_next(chat_id: int, announce: bool = True):
    q = queues.get(chat_id, [])
    fails = 0
    while q:
        song = q[0]
        try:
            # Search + stream URL sirf play hone se just pehle (kam requests, URL expire nahi hota)
            if not song.get("page"):
                found = await run(_search_sync, song["query"])
                if not found:
                    raise RuntimeError("not found")
                song["page"] = found["page"]
            url, real_title = await run(_stream_sync, song["page"])
            song["title"] = song.get("title") or real_title
            await call.play(chat_id, MediaStream(url, audio_parameters=AudioQuality.HIGH,
                                                 video_flags=MediaStream.Flags.IGNORE))
            if announce:
                await bot.send_message(
                    chat_id,
                    f"🎵 Now playing: {song['title']}\nRequested by {song['by']}",
                    reply_markup=buttons(chat_id),
                )
            return
        except Exception as ex:
            print("play error:", type(ex).__name__, ex)
            db.log_error(chat_id, "Play failed", ex)
            if "NoActiveGroupCall" in type(ex).__name__ or "GroupCallNotFound" in type(ex).__name__:
                await bot.send_message(chat_id, "❌ No active voice chat. Start a voice chat in this group and try again.")
                break
            name = song.get("title") or song.get("page") or "song"
            await bot.send_message(chat_id, f"⚠️ Couldn't play '{name}', skipping.")
            q.pop(0)
            fails += 1
            if fails >= 3:  # spam se bachne ke liye
                await bot.send_message(chat_id, "⚠️ Too many errors in a row, stopping.")
                break
    await stop_all(chat_id)


async def stop_all(chat_id: int):
    queues.pop(chat_id, None)
    loops.pop(chat_id, None)
    try:
        await call.leave_call(chat_id)
    except Exception:
        pass


@call.on_update()
async def on_update(_, update):
    # Sirf "gaana khatam" wale update par kaam karo (StreamEnded / StreamAudioEnded)
    names = {c.__name__ for c in type(update).__mro__}
    if not names & {"StreamEnded", "StreamAudioEnded"}:
        return
    chat_id = update.chat_id
    q = queues.get(chat_id)
    if not q:
        return
    if loops.get(chat_id):
        await play_next(chat_id, announce=False)
        return
    q.pop(0)
    if q:
        await play_next(chat_id)
    else:
        await stop_all(chat_id)
        await bot.send_message(chat_id, "✅ Queue finished. Left the voice chat.")


async def is_admin(chat_id: int, user_id: int) -> bool:
    try:
        m = await bot.get_chat_member(chat_id, user_id)
        return m.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception:
        return False


# ---------------- Group add / leave tracking (admin panel) ----------------
GROUP_TYPES = (ChatType.GROUP, ChatType.SUPERGROUP)
ACTIVE_STATUS = (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)


@bot.on_chat_member_updated(group=5)
async def track_member(_, u: ChatMemberUpdated):
    try:
        if u.chat.type not in GROUP_TYPES:
            return
        old, new = u.old_chat_member, u.new_chat_member
        member = new or old
        if not member or member.user.id != BOT_ID:
            return
        if new is None or new.status in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED):
            db.mark_left(u.chat.id, u.chat.title)
        elif new.status in ACTIVE_STATUS:
            db.mark_added(u.chat.id, u.chat.title)
    except Exception as ex:
        print("track error:", ex)


@bot.on_message(filters.group & (filters.new_chat_members | filters.left_chat_member), group=6)
async def track_service(_, m: Message):
    # Backup tracking (same event dobara aaye to db ignore kar deta hai)
    try:
        if m.new_chat_members and any(u.id == BOT_ID for u in m.new_chat_members):
            db.mark_added(m.chat.id, m.chat.title)
        elif m.left_chat_member and m.left_chat_member.id == BOT_ID:
            db.mark_left(m.chat.id, m.chat.title)
    except Exception as ex:
        print("track error:", ex)


# ---------------- Commands ----------------
async def _start_help(m: Message):
    await m.reply_text(
        "🎧 Add me to your group, make me admin, start a voice chat and send:\n"
        "/play song name or link\n"
        "/radio (admins only)\n\n"
        "/pause /resume /skip /stop /queue /loop"
    )


@bot.on_message(filters.command("start") & filters.private)
async def start(_, m: Message):
    if len(m.command) > 1 and m.command[1].startswith("radio_"):
        return await radio_menu(m)
    await _start_help(m)


@bot.on_message(filters.command("play") & filters.group)
async def play(_, m: Message):
    query = m.text.split(None, 1)[1].strip() if len(m.command) > 1 else ""
    if not query and m.reply_to_message and m.reply_to_message.text:
        query = m.reply_to_message.text.strip()
    if not query:
        return await m.reply_text("Usage: /play song name or link")
    if "spotify.com" in query:
        return await m.reply_text("Spotify links aren't supported. Send the song name instead.")

    db.seen(m.chat.id, m.chat.title)
    status = await m.reply_text("🔎 Searching...")
    try:
        await ensure_assistant(m.chat.id)
    except Exception as ex:
        db.log_error(m.chat.id, "Assistant could not join", ex)
        return await status.edit_text(
            "❌ Make me admin (with 'Invite users' permission) and start a voice chat first."
        )

    by = m.from_user.first_name if m.from_user else "Unknown"
    if re.match(r"https?://", query):  # direct link (YouTube, SoundCloud etc.)
        song = {"title": None, "page": query, "query": None, "by": by}
    else:
        found = await run(_search_sync, query)
        if not found:
            return await status.edit_text("❌ Song not found.")
        song = {"title": found["title"], "page": found["page"], "query": query, "by": by}

    q = queues.setdefault(m.chat.id, [])
    q.append(song)
    if len(q) == 1:
        await status.delete()
        await play_next(m.chat.id)
    else:
        await status.edit_text(f"➕ Added to queue #{len(q) - 1}: {song['title'] or 'link'}")


@bot.on_message(filters.command("pause") & filters.group)
async def pause(_, m: Message):
    try:
        await call.pause(m.chat.id)
        await m.reply_text("⏸ Paused")
    except Exception:
        await m.reply_text("Nothing is playing.")


@bot.on_message(filters.command("resume") & filters.group)
async def resume(_, m: Message):
    try:
        await call.resume(m.chat.id)
        await m.reply_text("▶️ Resumed")
    except Exception:
        await m.reply_text("Nothing is playing.")


async def do_skip(chat_id: int) -> str:
    q = queues.get(chat_id)
    if not q:
        return "Nothing is playing."
    q.pop(0)
    loops[chat_id] = False
    if q:
        await play_next(chat_id)
        return "⏭ Skipped"
    await stop_all(chat_id)
    return "⏭ Skipped. Queue is empty, left the voice chat."


@bot.on_message(filters.command("skip") & filters.group)
async def skip(_, m: Message):
    await m.reply_text(await do_skip(m.chat.id))


@bot.on_message(filters.command("stop") & filters.group)
async def stop(_, m: Message):
    await stop_all(m.chat.id)
    await m.reply_text("⏹ Stopped.")


@bot.on_message(filters.command("loop") & filters.group)
async def loop_cmd(_, m: Message):
    if not queues.get(m.chat.id):
        return await m.reply_text("Nothing is playing.")
    loops[m.chat.id] = not loops.get(m.chat.id, False)
    await m.reply_text("🔁 Loop ON" if loops[m.chat.id] else "🔁 Loop OFF")


@bot.on_message(filters.command("queue") & filters.group)
async def queue_cmd(_, m: Message):
    q = queues.get(m.chat.id)
    if not q:
        return await m.reply_text("Queue is empty.")
    name = lambda s: s.get("title") or s.get("page")
    text = f"🎵 Now: {name(q[0])}\n"
    for i, s in enumerate(q[1:11], 1):
        text += f"{i}. {name(s)}\n"
    if len(q) > 11:
        text += f"...and {len(q) - 11} more"
    await m.reply_text(text)


# ---------------- Radio ----------------
# Flow: group me admin /radio -> bot "Open radio menu" button deta hai -> admin ke private chat me
# language -> singer choose -> 10 songs group ke VC me bajte hain.
@bot.on_message(filters.command("radio") & filters.group)
async def radio_cmd(_, m: Message):
    db.seen(m.chat.id, m.chat.title)
    if not LANGS:
        return await m.reply_text("Radio is not available (songs.csv missing).")
    if not m.from_user:
        return await m.reply_text("Please turn off anonymous admin and send /radio again.")
    if not await is_admin(m.chat.id, m.from_user.id):
        return await m.reply_text("📻 Only admins can use radio.")
    await m.reply_text(
        "📻 Radio menu opens in private chat.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
            "Open radio menu", url=f"https://t.me/{BOT_USERNAME}?start=radio_{m.chat.id}")]]),
    )


def lang_kb() -> InlineKeyboardMarkup:
    rows, row = [], []
    for i, l in enumerate(LANGS):
        row.append(InlineKeyboardButton(f"🎵 {l}", callback_data=f"rl:{i}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def artist_kb(li: int, page: int) -> InlineKeyboardMarkup:
    lang = LANGS[li]
    names = ARTISTS[lang]
    per = 40  # Telegram limit: ek message me max 100 buttons
    pages = (len(names) + per - 1) // per
    page = max(0, min(page, pages - 1))
    rows, row = [], []
    for ai in range(page * per, min(len(names), (page + 1) * per)):
        n = names[ai]
        row.append(InlineKeyboardButton(n, callback_data=f"ra:{li}:{ai}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    if pages > 1:
        rows.append([
            InlineKeyboardButton("⬅️", callback_data=f"rp:{li}:{max(page - 1, 0)}"),
            InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"),
            InlineKeyboardButton("➡️", callback_data=f"rp:{li}:{min(page + 1, pages - 1)}"),
        ])
    rows.append([InlineKeyboardButton("🎲 Mix (any singer)", callback_data=f"rm:{li}"),
                 InlineKeyboardButton("🔙 Languages", callback_data="rb")])
    return InlineKeyboardMarkup(rows)


async def radio_menu(m: Message):
    try:
        chat_id = int(m.command[1].split("_", 1)[1])
    except Exception:
        return await m.reply_text("Send /radio in your group first.")
    if not await is_admin(chat_id, m.from_user.id):
        return await m.reply_text("📻 Only admins of that group can use radio.")
    dm_state[m.from_user.id] = {"chat": chat_id, "lang": None}
    await m.reply_text("📻 Radio\n\nChoose a language:", reply_markup=lang_kb())


async def start_radio(user_id: int, songs: list[tuple], label: str) -> str:
    st = dm_state.get(user_id)
    if not st:
        return "Session expired. Send /radio in the group again."
    chat_id = st["chat"]
    if not await is_admin(chat_id, user_id):
        return "Only admins can use radio."
    try:
        await ensure_assistant(chat_id)
    except Exception as ex:
        db.log_error(chat_id, "Assistant could not join (radio)", ex)
        return "Make the bot admin (Invite users permission) and start a voice chat in the group first."
    picked = random.sample(songs, min(RADIO_COUNT, len(songs)))
    queues[chat_id] = [
        {"title": f"{t} - {a}", "page": None, "query": f"{t} {a}", "by": "📻 Radio"}
        for t, a in picked
    ]
    loops.pop(chat_id, None)
    await bot.send_message(chat_id, f"📻 Radio started: {label}")
    await play_next(chat_id)
    return f"✅ Radio started in the group: {label}"


@bot.on_callback_query(filters.regex(r"^(rl|ra|rp|rm|rb|noop)"))
async def radio_cb(_, c: CallbackQuery):
    d = c.data
    uid = c.from_user.id
    st = dm_state.get(uid)
    if d == "noop":
        return await c.answer()
    if not st:
        return await c.answer("Session expired. Send /radio in the group again.", show_alert=True)
    p = d.split(":")
    try:
        if p[0] == "rb":
            await c.message.edit_text("📻 Radio\n\nChoose a language:", reply_markup=lang_kb())
        elif p[0] in ("rl", "rp"):
            li = int(p[1])
            st["lang"] = LANGS[li]
            page = int(p[2]) if p[0] == "rp" else 0
            await c.message.edit_text(f"📻 {LANGS[li]} - choose a singer (or type a name):",
                                      reply_markup=artist_kb(li, page))
        elif p[0] == "ra":
            lang = LANGS[int(p[1])]
            name = ARTISTS[lang][int(p[2])]
            await c.answer("Starting...")
            msg = await start_radio(uid, SONGS[lang][name], name)
            await c.message.edit_text(msg)
        elif p[0] == "rm":
            lang = LANGS[int(p[1])]
            await c.answer("Starting...")
            msg = await start_radio(uid, LANG_ALL[lang], f"{lang} mix")
            await c.message.edit_text(msg)
    except Exception as ex:
        print("radio cb error:", ex)
        db.log_error(dm_state.get(uid, {}).get("chat"), "Radio menu error", ex)
        await c.answer("Something went wrong, try again.", show_alert=True)


@bot.on_message(filters.private & filters.text & ~filters.command(["start", "play", "radio"]))
async def radio_typed(_, m: Message):
    st = dm_state.get(m.from_user.id)
    if not st or not st.get("lang"):
        return
    lang = st["lang"]
    hit = process.extractOne(m.text, ARTISTS[lang], scorer=fuzz.WRatio, score_cutoff=70)
    if not hit:
        return await m.reply_text("Singer not found. Pick from the list or try another spelling.")
    name = hit[0]
    await m.reply_text(await start_radio(m.from_user.id, SONGS[lang][name], name))


# ---------------- Player buttons ----------------
@bot.on_callback_query(filters.regex(r"^(pause|resume|skip|stop|loop)$"))
async def cb(_, c: CallbackQuery):
    chat_id = c.message.chat.id
    d = c.data
    try:
        if d == "pause":
            await call.pause(chat_id)
            await c.answer("Paused")
        elif d == "resume":
            await call.resume(chat_id)
            await c.answer("Resumed")
        elif d == "skip":
            await c.answer(await do_skip(chat_id))
        elif d == "stop":
            await stop_all(chat_id)
            await c.answer("Stopped")
        elif d == "loop":
            loops[chat_id] = not loops.get(chat_id, False)
            await c.answer("Loop ON" if loops[chat_id] else "Loop OFF")
            await c.message.edit_reply_markup(buttons(chat_id))
    except Exception:
        await c.answer("Nothing is playing.", show_alert=True)


# ---------------- Broadcast (admin panel se) ----------------
GONE_ERRORS = {"ChatWriteForbidden", "ChannelPrivate", "UserBannedInChannel", "PeerIdInvalid",
               "ChannelInvalid", "ChatIdInvalid", "ChatRestricted", "ChatAdminRequired"}


async def do_broadcast(text: str, st: dict):
    ids = db.active_group_ids()
    st["total"] = len(ids)
    for cid in ids:
        for attempt in range(2):
            try:
                await bot.send_message(cid, text)
                st["sent"] += 1
                break
            except Exception as ex:
                name = type(ex).__name__
                if name == "FloodWait" and attempt == 0:
                    await asyncio.sleep(getattr(ex, "value", 30) + 1)
                    continue
                st["failed"] += 1
                if name in GONE_ERRORS:  # bot hata diya gaya / likh nahi sakta
                    db.mark_left(cid, None)
                    st["removed"] += 1
                else:
                    db.log_error(cid, "Broadcast failed", ex)
                break
        await asyncio.sleep(0.3)  # Telegram limit se bachne ke liye


# ---------------- Run ----------------
async def main():
    global BOT_USERNAME, BOT_ID
    await bot.start()
    me = await bot.get_me()
    BOT_USERNAME, BOT_ID = me.username, me.id
    await assistant.start()
    await call.start()
    panel.library = {
        "total_songs": sum(len(v) for v in LANG_ALL.values()),
        "total_singers": sum(len(v) for v in ARTISTS.values()),
        "languages": [{"language": l, "songs": len(LANG_ALL[l]), "singers": len(ARTISTS[l])} for l in LANGS],
    }
    panel.broadcast_fn = do_broadcast
    await panel.start()
    asyncio.create_task(panel.cleanup_loop())
    print(f"Bot is running as @{BOT_USERNAME} | radio languages: {LANGS}")
    await idle()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
