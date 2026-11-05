import asyncio

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import csv
import hashlib
import html
import logging
import os
import random
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta

from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ChatType, ParseMode
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

import db
import media
import panel
from errors import diagnose

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("tgmusic")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
SONGS_CSV = "songs.csv"
RADIO_COUNT = 15  # radio plays this many songs once, then stops (no auto-refill)
MIN_ARTIST_SONGS = int(os.environ.get("MIN_ARTIST_SONGS", "3"))  # singers with fewer songs are hidden

bot = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)


# ---------------- Assistant pool ----------------
# One Telegram user account can only be in so many groups / join so fast. For big scale put
# several accounts in SESSION_STRINGS (comma or newline separated). Each group is assigned to
# the assistant that currently has the fewest groups and keeps it.
class Assistant:
    def __init__(self, idx: int, session: str):
        self.idx = idx
        self.client = Client(f"assistant{idx}", api_id=API_ID, api_hash=API_HASH,
                             session_string=session, in_memory=True)
        self.call = PyTgCalls(self.client)
        self.join_lock = asyncio.Lock()  # joins are spaced out -> no FloodWait
        self.chats = 0

    @property
    def tag(self) -> str:
        me = getattr(self.client, "me", None)
        return f"@{me.username}" if me and getattr(me, "username", None) else f"assistant {self.idx + 1}"


_sessions: list[str] = []
for _s in re.split(r"[,\s]+", (os.environ.get("SESSION_STRINGS", "") + "," +
                               os.environ.get("SESSION_STRING", ""))):
    if _s and _s not in _sessions:
        _sessions.append(_s)
if not _sessions:
    raise SystemExit("Set SESSION_STRING (or SESSION_STRINGS for several assistants).")
ASSISTANTS = [Assistant(i, s) for i, s in enumerate(_sessions)]
_assigned: dict[int, Assistant] = {}


def assistant_for(chat_id: int) -> Assistant:
    a = _assigned.get(chat_id)
    if a is None:
        a = min(ASSISTANTS, key=lambda x: x.chats)
        a.chats += 1
        _assigned[chat_id] = a
    return a


def call_for(chat_id: int) -> PyTgCalls:
    return assistant_for(chat_id).call


# Sab kuch RAM me. Restart par sab saaf, koi record nahi.
queues: dict[int, list[dict]] = {}
loops: dict[int, bool] = {}
radio_src: dict[int, tuple[list, str]] = {}  # chat -> (songs, label): radio refills itself
dm_state: dict[int, dict] = {}  # radio menu state: user_id -> {"chat", "lang"}
BOT_USERNAME = ""
BOT_ID = 0

# ---------------- Radio data (songs.csv) ----------------
SPLIT = re.compile(r"\s+(?:&|x|[Ff]eat\.?|[Ff]t\.?)\s+|\s*,\s+")
LANG_ORDER = ["Hindi", "Punjabi", "Haryanvi", "English"]
LANGS: list[str] = []
SONGS: dict[str, dict[str, list[tuple]]] = {}  # lang -> artist -> [(title, full_artist)]
ARTISTS: dict[str, list[str]] = {}  # lang -> artists (>= MIN_ARTIST_SONGS) sorted by song count
LANG_ALL: dict[str, list[tuple]] = {}  # lang -> every song


def load_songs():
    if not os.path.exists(SONGS_CSV):
        log.warning("songs.csv not found, /radio disabled")
        return
    tmp: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    allsongs: dict[str, list] = defaultdict(list)
    with open(SONGS_CSV, encoding="utf-8-sig") as f:
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
        names = sorted(tmp[l], key=lambda a: (-len(tmp[l][a]), a.lower()))
        ARTISTS[l] = [a for a in names if len(tmp[l][a]) >= MIN_ARTIST_SONGS] or names
        LANG_ALL[l] = allsongs[l]


load_songs()


# ---------------- Player ----------------
MAX_QUEUE = int(os.environ.get("MAX_QUEUE", "30"))  # songs per group
PLAY_COOLDOWN = float(os.environ.get("PLAY_COOLDOWN", "3"))  # seconds between /play of one user
CONTROLS_ADMIN_ONLY = os.environ.get("CONTROLS_ADMIN_ONLY", "1") == "1"  # pause/skip/stop/loop
STALL_GRACE = int(os.environ.get("STALL_GRACE", "20"))  # watchdog: seconds past song end

_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)  # one lock per group: no queue races
_last_play: dict[int, float] = {}  # user_id -> last /play time (anti-spam)
_joined: set[int] = set()  # groups where the assistant is already a member
_admin_cache: dict[tuple[int, int], tuple[float, bool]] = {}
_bg: set[asyncio.Task] = set()  # keeps background tasks alive


def spawn(coro) -> None:
    t = asyncio.create_task(coro)
    _bg.add(t)
    t.add_done_callback(_bg.discard)


media.in_use_provider = lambda: {s["file"] for q in queues.values() for s in q if s.get("file")}


async def say(chat_id: int, text: str, **kw):
    """send_message that never raises (FloodWait, no permission, kicked...)."""
    try:
        return await bot.send_message(chat_id, text, **kw)
    except Exception as ex:
        log.warning("send_message to %s failed: %s %s", chat_id, type(ex).__name__, ex)
        return None


def report(chat_id: int | None, where: str, ex: Exception, song: dict | None = None):
    """Single place where every error is classified, logged (Railway logs + admin panel) and
    returned as a Diag (code / user text / fix hint). Unknown errors also get a full traceback."""
    d = diagnose(ex)
    if d.code == "IGNORE_NOT_MODIFIED":
        return d
    detail = f"{type(ex).__name__}: {ex}"
    if song:
        detail += f" | song={song.get('title') or song.get('query') or song.get('page')}"
    log.warning("[%s] %s | chat=%s | %s | FIX: %s", d.code, where, chat_id, detail, d.hint,
                exc_info=ex if d.code == "UNKNOWN" else None)
    try:
        db.log_error(chat_id, where, ex, code=d.code, hint=d.hint, detail=detail)
    except Exception:
        pass
    return d


def buttons(chat_id: int) -> InlineKeyboardMarkup:
    lp = "🔂 Loop ON" if loops.get(chat_id) else "🔁 Loop"
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⏸ Pause", callback_data="pause"),
        InlineKeyboardButton("▶️ Resume", callback_data="resume"),
        InlineKeyboardButton("⏭ Skip", callback_data="skip"),
    ], [
        InlineKeyboardButton(lp, callback_data="loop"),
        InlineKeyboardButton("⏹ Stop", callback_data="stop"),
    ]])


def fmt_dur(sec) -> str:
    try:
        sec = int(sec)
    except (TypeError, ValueError):
        return "Live"
    return f"{sec // 60}:{sec % 60:02d} min"


def now_playing_text(song: dict) -> str:
    title = html.escape(song.get("title") or song.get("page") or "Unknown")
    page = song.get("page")
    if page and str(page).startswith("http"):
        title = f'<a href="{html.escape(str(page), quote=True)}">{title}</a>'  # blue clickable title
    by = html.escape(str(song.get("by") or ""))
    return (f"<u><b>Started streaming</b></u>\n\n"
            f"<b>Title:</b> {title}\n\n"
            f"<b>Duration:</b> {fmt_dur(song.get('duration'))}\n"
            f"<b>Requested by:</b> {by}")


async def is_admin(chat_id: int, user_id: int) -> bool:
    key = (chat_id, user_id)
    hit = _admin_cache.get(key)
    if hit and time.time() - hit[0] < 60:  # cache: fewer Telegram API calls
        return hit[1]
    try:
        m = await bot.get_chat_member(chat_id, user_id)
        ok = m.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception:
        ok = False
    if len(_admin_cache) > 5000:
        _admin_cache.clear()
    _admin_cache[key] = (time.time(), ok)
    return ok


async def can_control(chat_id: int, user) -> bool:
    if not CONTROLS_ADMIN_ONLY:
        return True
    return user is not None and await is_admin(chat_id, user.id)


async def msg_can_control(m: Message) -> bool:
    if m.sender_chat and m.sender_chat.id == m.chat.id:  # anonymous admin
        return True
    return await can_control(m.chat.id, m.from_user)


async def _deny(m: Message) -> bool:
    """True (and replies) if the sender may not control playback."""
    if await msg_can_control(m):
        return False
    await m.reply_text("🔒 Only admins can control playback.")
    return True


async def _assistant_is_member(a: Assistant, chat_id: int) -> bool:
    """Is the assistant already in the group? Asked twice: by the assistant itself and, if that
    fails (assistant has no cached info about the group after a restart), by the bot."""
    try:
        m = await a.client.get_chat_member(chat_id, "me")
        return m.status not in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED)
    except Exception:
        pass
    try:
        me = a.client.me or await a.client.get_me()
        m = await bot.get_chat_member(chat_id, me.id)
        return m.status not in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED)
    except Exception:
        return False


async def _fresh_link(chat_id: int) -> str:
    """A NEW temporary invite link. export_chat_invite_link() revokes the group's main link every
    time, which breaks links in use (and caused INVITE_HASH_EXPIRED when joins overlapped)."""
    try:
        l = await bot.create_chat_invite_link(chat_id, name="assistant",
                                              expire_date=datetime.now() + timedelta(minutes=10))
        return l.invite_link
    except Exception:
        return await bot.export_chat_invite_link(chat_id)


async def _join(a: Assistant, chat_id: int):
    def already(ex: Exception) -> bool:
        return type(ex).__name__ == "UserAlreadyParticipant"

    try:  # public group: join by @username, no invite link needed
        chat = await bot.get_chat(chat_id)
        if getattr(chat, "username", None):
            try:
                await a.client.join_chat(chat.username)
                return
            except Exception as ex:
                if already(ex):
                    return
    except Exception:
        pass
    last: Exception | None = None
    for _ in range(2):  # expired link -> ask for a fresh one once
        link = await _fresh_link(chat_id)
        try:
            await a.client.join_chat(link)
            return
        except Exception as ex:
            if already(ex):
                return
            if type(ex).__name__ not in ("InviteHashExpired", "InviteHashInvalid"):
                raise
            last = ex
            await asyncio.sleep(1)
    if await _assistant_is_member(a, chat_id):  # link is dead because assistant is already inside
        return
    raise last  # type: ignore[misc]


async def ensure_assistant(chat_id: int):
    if chat_id in _joined:
        return
    a = assistant_for(chat_id)
    if not await _assistant_is_member(a, chat_id):
        async with a.join_lock:
            await _join(a, chat_id)
            await asyncio.sleep(1.5)  # space out joins of one account (FloodWait protection)
    _joined.add(chat_id)


def join_error_text(ex: Exception, chat_id: int) -> str:
    d = diagnose(ex)
    tag = assistant_for(chat_id).tag
    if d.code == "UNKNOWN":
        return ("❌ I could not bring my assistant into this group. Make me admin (with 'Invite "
                f"users' permission), or add {tag} manually, and try again. [JOIN_FAILED]")
    return f"❌ {d.user} (assistant: {tag}) [{d.code}]"


# ---------------- Song storage in a Telegram channel ----------------
# STORAGE_CHANNEL = id of a private channel where the bot is admin (e.g. -1001234567890).
# (RADIO songs only.) First time a song is played it is downloaded from YouTube, played, and uploaded to the channel
# (its file_id is saved in SQLite). Next time ANY group asks for the same song it is downloaded
# from Telegram (fast, no YouTube request, no cookie use). If anything fails -> YouTube is used.
STORAGE_CHANNEL = int(os.environ.get("STORAGE_CHANNEL", "0") or 0)
_uploading: set[str] = set()
_upload_sem = asyncio.Semaphore(2)


def _cache_key(song: dict) -> str | None:
    if not song.get("radio"):  # only RADIO songs use the storage channel, /play never does
        return None
    if song.get("query"):
        return "q:" + " ".join(song["query"].lower().split())
    if song.get("page"):
        return "u:" + song["page"]
    return None


async def _from_telegram(key: str, row: dict) -> str:
    name = "tg-" + hashlib.sha1(key.encode()).hexdigest()[:16] + row["ext"]
    path = os.path.join(media.DL_DIR, name)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        media._last_used[path] = time.time()
        return path

    async def job():
        tmp = path + ".part"
        got = await bot.download_media(row["file_id"], file_name=tmp)
        if not got or not os.path.exists(got) or os.path.getsize(got) == 0:
            raise RuntimeError("empty download from storage channel")
        os.replace(got, path)
        return path

    res = await media._dedupe(("t", key), job)
    media._last_used[res] = time.time()
    return res


async def _upload_to_channel(key: str, path: str, title: str | None, dur, page: str | None):
    if key in _uploading:
        return
    _uploading.add(key)
    try:
        if await asyncio.to_thread(db.tgcache_get, key):
            return
        async with _upload_sem:
            m = await bot.send_audio(STORAGE_CHANNEL, path, title=(title or "")[:60],
                                     duration=int(dur or 0), caption=f"{key}\n{page or ''}"[:900])
        media_obj = m.audio or m.document
        if not media_obj:
            return
        await asyncio.to_thread(db.tgcache_put, key, media_obj.file_id, title, dur, page,
                                os.path.splitext(path)[1] or ".m4a")
        log.info("stored in channel: %s", key)
    except Exception as ex:
        report(None, "Storage upload failed", ex)
    finally:
        _uploading.discard(key)


async def get_audio(song: dict) -> tuple[str, str, int | None]:
    """(local file, title, duration): Telegram storage first, YouTube/SoundCloud otherwise."""
    key = _cache_key(song)
    if STORAGE_CHANNEL and key:
        row = await asyncio.to_thread(db.tgcache_get, key)
        if row:
            try:
                path = await _from_telegram(key, row)
                if row.get("page") and not song.get("page"):
                    song["page"] = row["page"]  # keeps the clickable title
                return path, row["title"] or song.get("title") or "song", row["duration"]
            except Exception as ex:
                log.warning("storage download failed (%s): %s %s", key, type(ex).__name__, ex)
                await asyncio.to_thread(db.tgcache_delete, key)  # stale file_id -> re-store below
    if not song.get("page"):
        found = await media.search(song["query"])
        if not found:
            raise media.MediaError("Song not found.")
        song["page"] = found["page"]
        song["duration"] = song.get("duration") or found.get("duration")
    path, title, dur = await media.fetch(song["page"])
    if STORAGE_CHANNEL and key:
        spawn(_upload_to_channel(key, path, title, dur or song.get("duration"), song["page"]))
    return path, title, dur


async def _prefetch(q: list):
    """Download the NEXT song while the current one plays -> no gap between songs."""
    if len(q) < 2:
        return
    nxt = q[1]
    try:
        path, _t, dur = await get_audio(nxt)
        nxt["file"] = path
        nxt["duration"] = nxt.get("duration") or dur
    except Exception as ex:
        log.debug("prefetch failed: %s", ex)


def _radio_items(songs: list[tuple]) -> list[dict]:
    picked = random.sample(songs, min(RADIO_COUNT, len(songs)))
    return [{"title": f"{t} - {a}", "page": None, "query": f"{t} {a}", "by": "📻 Radio", "radio": True}
            for t, a in picked]


def _refill_radio(chat_id: int, q: list) -> bool:
    """Radio does NOT refill: after its songs finish the bot leaves the voice chat."""
    return False


async def _play_locked(chat_id: int, announce: bool = True):
    """Play queue head. Caller must hold the chat lock."""
    q = queues.get(chat_id)
    fails = 0
    while q:
        if queues.get(chat_id) is not q:  # stopped meanwhile
            return
        song = q[0]
        try:
            # Search + download only right before playing (cache + dedupe live in media.py)
            path, real_title, dur = await get_audio(song)
            song["file"] = path
            song["title"] = song.get("title") or real_title
            song["duration"] = dur or song.get("duration")
            if queues.get(chat_id) is not q or not q or q[0] is not song:
                return  # stopped / skipped while downloading
            await call_for(chat_id).play(
                chat_id, MediaStream(path, audio_parameters=AudioQuality.HIGH,
                                     video_flags=MediaStream.Flags.IGNORE))
        except Exception as ex:
            d = report(chat_id, "Play failed", ex, song)
            if d.fatal:
                # e.g. no voice chat / no admin right: every next song would fail the same way,
                # so stop instead of skipping the whole queue and spamming the group.
                extra = (f" (or make {assistant_for(chat_id).tag} admin with 'Manage video chats')"
                         if d.code == "NO_VOICE_CHAT" else "")
                await say(chat_id, f"❌ {d.user}{extra} [{d.code}]")
                break
            if d.code == "ASSISTANT_REMOVED":
                _joined.discard(chat_id)  # assistant was removed, re-join next time
            title = song.get("title") or song.get("query") or song.get("page") or "song"
            await say(chat_id, f"⚠️ Couldn't play '{title}': {d.user} [{d.code}]")
            if q and q[0] is song:
                q.pop(0)
            if not q and _refill_radio(chat_id, q):
                pass  # radio never runs dry because of a bad song
            fails += 1
            if fails >= 3:  # avoid spam
                await say(chat_id, "⚠️ Too many errors in a row, stopping.")
                break
            continue
        # ---- playing: the "Now playing" message is separate, its failure must not skip the song
        song["started"] = time.time()
        song.pop("paused_at", None)
        song.pop("wd", None)
        if announce:
            await say(chat_id, now_playing_text(song), parse_mode=ParseMode.HTML,
                      disable_web_page_preview=True, reply_markup=buttons(chat_id),
                      reply_to_message_id=song.get("msg_id"))  # reply to the /play message
        spawn(_prefetch(q))
        return
    if queues.get(chat_id) is q:
        await stop_all(chat_id)


async def _play_safe(chat_id: int, announce: bool = True):
    """_play_locked that can never leave the queue stuck because of an unexpected exception."""
    try:
        await _play_locked(chat_id, announce)
    except Exception as ex:
        report(chat_id, "Player crashed", ex)
        await say(chat_id, "⚠️ Player error. Stopped, please /play again.")
        await stop_all(chat_id)


async def play_next(chat_id: int, announce: bool = True, status: Message | None = None):
    try:
        async with _locks[chat_id]:
            await _play_safe(chat_id, announce)
    finally:
        if status:
            try:
                await status.delete()
            except Exception:
                pass


async def stop_all(chat_id: int):
    queues.pop(chat_id, None)
    loops.pop(chat_id, None)
    radio_src.pop(chat_id, None)
    try:
        await call_for(chat_id).leave_call(chat_id)
    except Exception:
        pass


async def _on_stream_end(chat_id: int, expect: dict | None = None):
    async with _locks[chat_id]:
        q = queues.get(chat_id)
        if not q:
            return
        if expect is not None and q[0] is not expect:  # watchdog: song already changed
            return
        if loops.get(chat_id):
            await _play_safe(chat_id, announce=False)
            return
        q.pop(0)
        if q:
            await _play_safe(chat_id)
        else:
            was_radio = chat_id in radio_src
            await stop_all(chat_id)
            await say(chat_id, "📻 Radio finished. Left the voice chat." if was_radio
                      else "✅ Queue finished. Left the voice chat.")


async def watchdog_loop():
    """If Telegram never tells us a stream ended (lost event), move on after duration + grace."""
    while True:
        await asyncio.sleep(30)
        now = time.time()
        for chat_id, q in list(queues.items()):
            try:
                if not q:
                    continue
                s = q[0]
                st, dur = s.get("started"), s.get("duration")
                if not st or not dur or s.get("paused_at") or s.get("wd"):
                    continue
                if now - st > dur + STALL_GRACE:
                    s["wd"] = True
                    log.warning("watchdog: stream stalled in %s, advancing", chat_id)
                    spawn(_on_stream_end(chat_id, expect=s))
            except Exception as ex:
                log.warning("watchdog error: %s", ex)


async def on_update(_, update):
    try:
        names = {c.__name__ for c in type(update).__mro__}
        if names & {"StreamEnded", "StreamAudioEnded"}:
            await _on_stream_end(update.chat_id)
        elif "ChatUpdate" in names:
            # bot kicked / voice chat closed: drop the queue so no stale state is left
            status = str(getattr(update, "status", ""))
            if any(k in status for k in ("KICKED", "LEFT_GROUP", "CLOSED_VOICE_CHAT", "DISCARDED_CALL")):
                queues.pop(update.chat_id, None)
                loops.pop(update.chat_id, None)
                radio_src.pop(update.chat_id, None)
                _joined.discard(update.chat_id)
    except Exception as ex:
        report(getattr(update, "chat_id", None), "on_update failed", ex)


for _a in ASSISTANTS:
    _a.call.on_update()(on_update)


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
        log.warning("track error: %s", ex)


@bot.on_message(filters.group & (filters.new_chat_members | filters.left_chat_member), group=6)
async def track_service(_, m: Message):
    # Backup tracking (same event dobara aaye to db ignore kar deta hai)
    try:
        if m.new_chat_members and any(u.id == BOT_ID for u in m.new_chat_members):
            db.mark_added(m.chat.id, m.chat.title)
        elif m.left_chat_member and m.left_chat_member.id == BOT_ID:
            db.mark_left(m.chat.id, m.chat.title)
    except Exception as ex:
        log.warning("track error: %s", ex)


# ---------------- Commands ----------------
START_PHOTO = os.environ.get("START_PHOTO", "")  # URL / file_id; else assets/start.jpg is used
SUPPORT_URL = os.environ.get("SUPPORT_URL", "")  # e.g. https://t.me/YourSupportGroup
START_TEXT = (
    "🎧 <b>Powerful music bot, never falls!</b>\n\n"
    "Lag-free, high quality songs in your group voice chat.\n"
    "Add me to your group, make me admin and enjoy the music.\n\n"
    "<b>Tap Commands to see everything I can do.</b>"
)
HELP_TEXT = (
    "<b>Commands</b>\n\n"
    "/play <i>song name or link</i> - play a song\n"
    "/pause - pause\n/resume - resume\n/skip - next song\n"
    "/stop - stop and leave\n/queue - show queue\n/loop - loop on/off\n"
    "/radio - radio (admins only)"
)


def start_kb(back: bool = False) -> InlineKeyboardMarkup:
    if back:
        return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="home")]])
    rows = [[InlineKeyboardButton(
        "➕ Add me to your group",
        url=f"https://t.me/{BOT_USERNAME}?startgroup=true&admin=manage_video_chats+delete_messages+invite_users")]]
    row2 = [InlineKeyboardButton("📖 Commands", callback_data="cmds")]
    if SUPPORT_URL:
        row2.append(InlineKeyboardButton("💬 Support", url=SUPPORT_URL))
    rows.append(row2)
    return InlineKeyboardMarkup(rows)


def _start_photo():
    if START_PHOTO:
        return START_PHOTO
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "start.jpg")
    return p if os.path.exists(p) else None


async def _start_help(m: Message):
    photo = _start_photo()
    if photo:
        try:
            return await m.reply_photo(photo, caption=START_TEXT, parse_mode=ParseMode.HTML,
                                       reply_markup=start_kb())
        except Exception as ex:
            log.warning("start photo failed: %s", ex)
    await m.reply_text(START_TEXT, parse_mode=ParseMode.HTML, reply_markup=start_kb())


@bot.on_callback_query(filters.regex(r"^(cmds|home)$"))
async def start_cb(_, c: CallbackQuery):
    back = c.data == "cmds"
    text = HELP_TEXT if back else START_TEXT
    try:
        if c.message.photo:
            await c.message.edit_caption(text, parse_mode=ParseMode.HTML, reply_markup=start_kb(back))
        else:
            await c.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=start_kb(back))
    except Exception:
        pass
    await c.answer()


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
    query = query[:200]
    is_link = bool(re.match(r"https?://", query))
    link = media.clean_url(query) if is_link else None
    if is_link and not link:
        return await m.reply_text("❌ Only YouTube and SoundCloud links (single songs) are supported.")

    uid = m.from_user.id if m.from_user else None
    if uid is not None:  # anti-spam cooldown per user
        now = time.time()
        if now - _last_play.get(uid, 0) < PLAY_COOLDOWN:
            return await m.reply_text("⏳ Please wait a few seconds before the next /play.")
        _last_play[uid] = now
        if len(_last_play) > 5000:
            _last_play.clear()
    if len(queues.get(m.chat.id, [])) >= MAX_QUEUE:
        return await m.reply_text(f"❌ Queue is full ({MAX_QUEUE} songs). Wait for some songs to finish.")

    db.seen(m.chat.id, m.chat.title)
    status = await m.reply_text("🔎 Searching...")
    try:
        await ensure_assistant(m.chat.id)
    except Exception as ex:
        report(m.chat.id, "Assistant could not join", ex)
        return await status.edit_text(join_error_text(ex, m.chat.id))

    by = m.from_user.first_name if m.from_user else "Unknown"
    if link:  # direct link (YouTube, SoundCloud)
        song = {"title": None, "page": link, "query": None, "by": by, "msg_id": m.id}
    else:
        search_diag = None
        try:
            found = await media.search(query)
        except Exception as ex:
            search_diag = report(m.chat.id, "Search failed", ex)
            found = None
        if not found:
            if search_diag:  # search itself broke (YouTube block, network...) - not "no such song"
                return await status.edit_text(f"❌ {search_diag.user} [{search_diag.code}]")
            return await status.edit_text("❌ Song not found.")
        song = {"title": found["title"], "page": found["page"], "query": query, "by": by,
                "msg_id": m.id, "duration": found.get("duration")}

    q = queues.setdefault(m.chat.id, [])
    q.append(song)
    if len(q) == 1:
        await status.edit_text("⬇️ Preparing audio...")
        await play_next(m.chat.id, status=status)
    else:
        await status.edit_text(f"➕ Added to queue #{len(q) - 1}: {song['title'] or 'link'}")
        if len(q) == 2:
            spawn(_prefetch(q))


async def do_pause(chat_id: int):
    await call_for(chat_id).pause(chat_id)
    q = queues.get(chat_id)
    if q and not q[0].get("paused_at"):
        q[0]["paused_at"] = time.time()


async def do_resume(chat_id: int):
    await call_for(chat_id).resume(chat_id)
    q = queues.get(chat_id)
    if q:
        p = q[0].pop("paused_at", None)
        if p and q[0].get("started"):
            q[0]["started"] += time.time() - p  # pause time does not count for the watchdog


@bot.on_message(filters.command("pause") & filters.group)
async def pause(_, m: Message):
    if await _deny(m):
        return
    try:
        await do_pause(m.chat.id)
        await m.reply_text("⏸ Paused")
    except Exception:
        await m.reply_text("Nothing is playing.")


@bot.on_message(filters.command("resume") & filters.group)
async def resume(_, m: Message):
    if await _deny(m):
        return
    try:
        await do_resume(m.chat.id)
        await m.reply_text("▶️ Resumed")
    except Exception:
        await m.reply_text("Nothing is playing.")


async def do_skip(chat_id: int) -> str:
    async with _locks[chat_id]:
        q = queues.get(chat_id)
        if not q:
            return "Nothing is playing."
        q.pop(0)
        loops[chat_id] = False
        if not q:
            _refill_radio(chat_id, q)
        if q:
            await _play_safe(chat_id)
            return "⏭ Skipped"
        await stop_all(chat_id)
        return "⏭ Skipped. Queue is empty, left the voice chat."


@bot.on_message(filters.command("skip") & filters.group)
async def skip(_, m: Message):
    if await _deny(m):
        return
    await m.reply_text(await do_skip(m.chat.id))


@bot.on_message(filters.command("stop") & filters.group)
async def stop(_, m: Message):
    if await _deny(m):
        return
    await stop_all(m.chat.id)
    await m.reply_text("⏹ Stopped.")


@bot.on_message(filters.command("loop") & filters.group)
async def loop_cmd(_, m: Message):
    if await _deny(m):
        return
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
# language -> singer choose -> gaane group ke VC me bajte hain (khatam hone par apne aap naye aate hain,
# /stop tak radio chalta rehta hai).
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
    if len(dm_state) > 5000:
        dm_state.clear()
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
        report(chat_id, "Assistant could not join (radio)", ex)
        return join_error_text(ex, chat_id)
    async with _locks[chat_id]:
        queues[chat_id] = _radio_items(songs)
        radio_src[chat_id] = (songs, label)
        loops.pop(chat_id, None)
    await say(chat_id, f"📻 Radio started: {label}\nSend /stop to end it.")
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
        try:
            if type(ex).__name__ == "MessageNotModified":  # same button pressed twice: harmless
                return await c.answer()
            report(dm_state.get(uid, {}).get("chat"), "Radio menu error", ex)
            await c.answer("Something went wrong, try again.", show_alert=True)
        except Exception:
            pass  # query already answered


@bot.on_message(filters.private & filters.text & ~filters.regex(r"^/"))
async def radio_typed(_, m: Message):
    st = dm_state.get(m.from_user.id) if m.from_user else None
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
    if not await can_control(chat_id, c.from_user):
        return await c.answer("Only admins can do this.", show_alert=True)
    try:
        if d == "pause":
            await do_pause(chat_id)
            await c.answer("Paused")
        elif d == "resume":
            await do_resume(chat_id)
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
        for attempt in range(3):
            try:
                await bot.send_message(cid, text)
                st["sent"] += 1
                break
            except Exception as ex:
                name = type(ex).__name__
                if name == "FloodWait" and attempt < 2:
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
async def notify_shutdown():
    """Redeploy / SIGTERM: tell groups that are playing, so nobody is left wondering."""
    chats = list(queues)
    if not chats:
        return
    msg = "🔄 Bot is restarting for an update. Queue was cleared, please /play again in a minute."
    try:
        await asyncio.wait_for(asyncio.gather(*(say(c, msg) for c in chats)), timeout=8)
    except Exception:
        pass


async def main():
    global BOT_USERNAME, BOT_ID
    await bot.start()
    me = await bot.get_me()
    BOT_USERNAME, BOT_ID = me.username, me.id
    if STORAGE_CHANNEL:
        try:
            ch = await bot.get_chat(STORAGE_CHANNEL)
            log.info("storage channel OK: %s | songs stored: %s", ch.title, db.tgcache_count())
        except Exception as ex:
            log.warning("STORAGE CHANNEL NOT USABLE (%s %s). Make the bot admin there and check "
                        "STORAGE_CHANNEL id (-100...). Falling back to YouTube only.",
                        type(ex).__name__, ex)
    else:
        log.info("STORAGE_CHANNEL not set: every song comes from YouTube")
    for a in ASSISTANTS:
        await a.client.start()
        await a.call.start()
    panel.library = {
        "total_songs": sum(len(v) for v in LANG_ALL.values()),
        "total_singers": sum(len(v) for v in ARTISTS.values()),
        "languages": [{"language": l, "songs": len(LANG_ALL[l]), "singers": len(ARTISTS[l])} for l in LANGS],
    }
    panel.broadcast_fn = do_broadcast
    await panel.start()
    spawn(panel.cleanup_loop())
    spawn(media.evict_loop())
    spawn(watchdog_loop())
    log.info("Bot is running as @%s | assistants: %s | radio languages: %s",
             BOT_USERNAME, [a.tag for a in ASSISTANTS], LANGS)
    await idle()
    await notify_shutdown()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
