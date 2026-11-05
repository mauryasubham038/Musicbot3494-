"""Search + audio download layer.

Many groups play at once, so this module makes sure that:
  - blocking yt-dlp work runs in its own thread pool (never blocks the bot's event loop)
  - at most DL_CONCURRENCY downloads / SEARCH_CONCURRENCY searches run at the same time
  - the same song asked by 100 groups is downloaded ONCE (in-flight dedupe + file cache)
  - repeated searches are answered from memory + SQLite cache (survives restarts)
  - the disk cache is capped (MAX_CACHE_MB); least recently used files are evicted,
    files of songs that are still queued are never deleted
  - only allowed domains (YouTube / SoundCloud) are fetched, with a file-size cap
  - every yt-dlp call gets its OWN copy of cookies.txt (no corruption) and a random proxy
"""
import asyncio
import base64
import hashlib
import logging
import os
import random
import re
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable
from urllib.parse import parse_qs, urlparse

from rapidfuzz import fuzz
from yt_dlp import YoutubeDL
from yt_dlp.utils import match_filter_func

import db

log = logging.getLogger("tgmusic.media")

# ---- config (all overridable via environment variables) ----
# PROXY: one proxy, or several separated by comma/space (a random one is used per request)
PROXIES = [p for p in re.split(r"[,\s]+", os.environ.get("PROXY", "")) if p]
COOKIE_COOLDOWN = int(os.environ.get("COOKIE_COOLDOWN", "1800"))  # seconds a blocked cookie rests
DL_DIR = os.environ.get("DL_DIR", "/tmp/tgm_audio")
MAX_CACHE_MB = int(os.environ.get("MAX_CACHE_MB", "1500"))
MAX_DURATION = int(os.environ.get("MAX_DURATION", "900"))  # seconds (15 min), longer = rejected
MAX_FILESIZE_MB = int(os.environ.get("MAX_FILESIZE_MB", "80"))
DL_CONCURRENCY = int(os.environ.get("DL_CONCURRENCY", "8"))
SEARCH_CONCURRENCY = int(os.environ.get("SEARCH_CONCURRENCY", "8"))
ALLOWED_DOMAINS = [d.strip().lower() for d in
                   os.environ.get("ALLOWED_DOMAINS", "youtube.com,youtu.be,soundcloud.com").split(",")
                   if d.strip()]
SEARCH_TTL = 6 * 3600  # memory cache
SEARCH_DB_TTL = 7 * 24 * 3600  # sqlite cache
SEARCH_CACHE_MAX = 2000

BAD_WORDS = ["remix", "cover", "live", "slowed", "reverb", "karaoke",
             "instrumental", "8d", "lofi", "sped up", "nightcore", "reaction"]
GOOD_WORDS = ["official audio", "official video", "official music video", "lyrical", "full audio"]
_BAD_RE = re.compile(r"\b(" + "|".join(re.escape(w) for w in BAD_WORDS) + r")\b")

os.makedirs(DL_DIR, exist_ok=True)

_pool = ThreadPoolExecutor(max_workers=DL_CONCURRENCY + SEARCH_CONCURRENCY,
                           thread_name_prefix="media")
_dl_sem = asyncio.Semaphore(DL_CONCURRENCY)
_search_sem = asyncio.Semaphore(SEARCH_CONCURRENCY)

_search_cache: dict[str, tuple[float, dict]] = {}
_file_index: dict[str, tuple[str, str, int | None]] = {}  # page url -> (file path, title, duration)
_last_used: dict[str, float] = {}  # file path -> last use time
_inflight: dict[tuple, asyncio.Future] = {}

# main.py sets this: returns the set of file paths still needed by queues
in_use_provider: Callable[[], set[str]] = lambda: set()


class MediaError(Exception):
    """Error whose message is safe to show to the user."""


# ---------------- cookie pool ----------------
# Cookies come from env: COOKIES_B64 / COOKIES_TXT (name "main") and COOKIES_B64_1 .. _20 (names
# "cookie1" ..). Each yt-dlp call uses the least-used healthy cookie. If YouTube blocks one, it rests
# for COOKIE_COOLDOWN seconds and the call is retried with another cookie. Use dummy accounts only.
_BLOCK_RE = re.compile(r"sign in to confirm|not a bot|http error 429|too many requests|"
                       r"cookies? (are|is) no longer valid|please sign in", re.I)
_DEAD_RE = re.compile(r"no longer valid|account.{0,30}(terminated|suspended|disabled)", re.I)
_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_clock = threading.Lock()


@dataclass
class Cookie:
    name: str
    path: str
    uses: int = 0
    blocks: int = 0
    blocked_until: float = 0.0
    dead: bool = False
    last_ok: float = 0.0
    last_error: str = ""


COOKIE_POOL: list[Cookie] = []


def _load_cookies():
    d = os.path.join(tempfile.gettempdir(), "tgm_cookies")
    os.makedirs(d, exist_ok=True)
    seen: set[str] = set()

    def add(name: str, data: bytes):
        h = hashlib.sha1(data).hexdigest()
        if h in seen or not data.strip():
            return
        seen.add(h)
        path = os.path.join(d, f"{name}.txt")
        with open(path, "wb") as f:
            f.write(data)
        COOKIE_POOL.append(Cookie(name, path))

    for suffix in [""] + [f"_{i}" for i in range(1, 21)]:
        name = "main" if not suffix else f"cookie{suffix[1:]}"
        try:
            if os.environ.get("COOKIES_B64" + suffix):
                add(name, base64.b64decode(os.environ["COOKIES_B64" + suffix]))
            elif os.environ.get("COOKIES_TXT" + suffix):
                add(name, os.environ["COOKIES_TXT" + suffix].encode("utf-8"))
        except Exception as ex:
            log.warning("cookie %s could not be loaded: %s", name, ex)
    if os.path.exists("cookies.txt"):  # local file (running without Railway variables)
        add("file", open("cookies.txt", "rb").read())
    log.info("cookies loaded: %s", [c.name for c in COOKIE_POOL] or "none")


_load_cookies()


def _is_block(ex: Exception) -> bool:
    return bool(_BLOCK_RE.search(_ANSI.sub("", str(ex))))


def _pick_cookie(exclude: set[str]) -> Cookie | None:
    now = time.time()
    with _clock:
        free = [c for c in COOKIE_POOL if c.name not in exclude and not c.dead and c.blocked_until <= now]
        if free:
            low = min(c.uses for c in free)
            c = random.choice([c for c in free if c.uses == low])
        else:  # everything rests: try the one that rests the shortest (it may be fine again)
            rest = [c for c in COOKIE_POOL if c.name not in exclude and not c.dead]
            if not rest:
                return None
            c = min(rest, key=lambda x: x.blocked_until)
        c.uses += 1
        return c


def _cookie_ok(c: Cookie):
    with _clock:
        c.last_ok = time.time()
        c.blocked_until = 0.0


def _cookie_blocked(c: Cookie, ex: Exception):
    msg = _ANSI.sub("", str(ex)).replace("ERROR: ", "").strip()[:200]
    with _clock:
        c.blocks += 1
        c.last_error = msg
        if _DEAD_RE.search(msg):
            c.dead = True
        else:
            c.blocked_until = time.time() + COOKIE_COOLDOWN
        state = "DEAD (invalid)" if c.dead else f"blocked for {COOKIE_COOLDOWN // 60} min"
    log.warning("cookie %s %s: %s", c.name, state, msg)
    try:
        db.log_error(None, f"Cookie '{c.name}' {state}", None, code="YT_COOKIE",
                     hint="YouTube refused this cookie: add more cookies / a proxy, or re-export it "
                          "from a fresh browser session", detail=msg)
    except Exception:
        pass


def cookie_status() -> list[dict]:
    """For the admin panel."""
    now = time.time()
    out = []
    with _clock:
        for c in COOKIE_POOL:
            if c.dead:
                st = "dead"
            elif c.blocked_until > now:
                st = "blocked"
            else:
                st = "ok"
            out.append({"name": c.name, "status": st, "uses": c.uses, "blocks": c.blocks,
                        "rest_min": max(0, int((c.blocked_until - now) / 60) + 1) if st == "blocked" else 0,
                        "last_ok": c.last_ok, "last_error": c.last_error})
    return out


def cookie_reset(name: str) -> bool:
    """Panel 'Retry now': forget block/dead state of one cookie."""
    with _clock:
        for c in COOKIE_POOL:
            if c.name == name:
                c.dead = False
                c.blocked_until = 0.0
                return True
    return False


def _run_with_cookies(fn, use_cookies: bool = True):
    """fn(cookie_or_None). If YouTube blocks the cookie, mark it and retry with another one."""
    pool = COOKIE_POOL if use_cookies else []
    attempts = max(1, min(3, len(pool)))
    tried: set[str] = set()
    last: Exception | None = None
    for _ in range(attempts):
        c = _pick_cookie(tried) if pool else None
        if c:
            tried.add(c.name)
        try:
            res = fn(c)
            if c:
                _cookie_ok(c)
            return res
        except Exception as ex:
            last = ex
            if c and _is_block(ex):
                _cookie_blocked(c, ex)
                continue
            raise
    raise last  # type: ignore[misc]


# ---------------- links ----------------
def clean_url(url: str) -> str | None:
    """Return a safe, canonical URL, or None if the link is not allowed."""
    try:
        u = urlparse(url.strip())
    except ValueError:
        return None
    host = (u.hostname or "").lower()
    if u.scheme not in ("http", "https") or not host:
        return None
    if not any(host == d or host.endswith("." + d) for d in ALLOWED_DOMAINS):
        return None
    if host == "youtu.be" or host.endswith("youtube.com"):
        vid = None
        if host == "youtu.be":
            vid = u.path.strip("/").split("/")[0]
        elif u.path == "/watch":
            vid = (parse_qs(u.query).get("v") or [None])[0]
        else:
            m = re.match(r"^/(?:shorts|embed|live|v)/([\w-]{11})", u.path)
            vid = m.group(1) if m else None
        if vid and re.fullmatch(r"[\w-]{11}", vid):
            return f"https://www.youtube.com/watch?v={vid}"
        return None  # playlist / channel / unknown YouTube page
    return url.strip()


# ---------------- helpers ----------------
@contextmanager
def _ydl(extra: dict | None = None, cookie: Cookie | None = None):
    opts = {"quiet": True, "no_warnings": True, "noplaylist": True,
            "playlistend": 1, "socket_timeout": 20}
    tmp = None
    if cookie:
        # private copy: yt-dlp rewrites its cookie file, parallel threads would corrupt a shared one
        fd, tmp = tempfile.mkstemp(prefix="ck_", suffix=".txt")
        os.close(fd)
        shutil.copyfile(cookie.path, tmp)
        opts["cookiefile"] = tmp
    if PROXIES:
        opts["proxy"] = random.choice(PROXIES)
    if extra:
        opts.update(extra)
    try:
        with YoutubeDL(opts) as y:
            yield y
    finally:
        if tmp:
            try:
                os.remove(tmp)
            except OSError:
                pass


async def _in_thread(fn, *args):
    return await asyncio.get_running_loop().run_in_executor(_pool, fn, *args)


async def _dedupe(key: tuple, factory):
    """Run factory() once per key; concurrent callers share the same result."""
    fut = _inflight.get(key)
    if fut is None:
        fut = asyncio.ensure_future(factory())
        _inflight[key] = fut
        fut.add_done_callback(lambda _f: _inflight.pop(key, None))
    return await asyncio.shield(fut)


def _score(query: str, e: dict) -> float:
    title = (e.get("title") or "").lower()
    channel = (e.get("channel") or e.get("uploader") or "").lower()
    q = query.lower()
    s = fuzz.token_set_ratio(q, f"{title} {channel}")
    if any(w in title for w in GOOD_WORDS):
        s += 8
    if "topic" in channel or "vevo" in channel or "official" in channel:
        s += 8
    # whole-word match only ("alive" must not count as "live")
    s -= 25 * len(set(_BAD_RE.findall(title)) - set(_BAD_RE.findall(q)))
    dur = e.get("duration") or 0
    if dur > 900:  # longer than 15 min = mix / podcast
        s -= 40
    if 0 < dur < 45:
        s -= 30
    return s


# ---------------- search ----------------
def _search_once(prefix: str, query: str, cookie: Cookie | None) -> dict | None:
    with _ydl({"extract_flat": True}, cookie) as y:
        info = y.extract_info(f"{prefix}:{query}", download=False)
    entries = [e for e in (info.get("entries") or []) if e]
    if not entries:
        return None
    best = max(entries, key=lambda e: _score(query, e))
    url = best.get("url") or best.get("webpage_url")
    if url and not url.startswith("http"):
        url = f"https://www.youtube.com/watch?v={best['id']}"
    if not url:
        return None
    return {"title": best.get("title") or query, "page": url, "duration": best.get("duration")}


def _search_sync(query: str) -> dict | None:
    for prefix in ("ytsearch5", "scsearch5"):  # YouTube, then SoundCloud fallback
        try:
            res = _run_with_cookies(lambda c: _search_once(prefix, query, c),
                                    use_cookies=prefix.startswith("yt"))
            if res:
                return res
        except Exception as ex:
            log.warning("search via %s failed: %s", prefix, ex)
    return None


async def search(query: str) -> dict | None:
    key = " ".join(query.lower().split())
    hit = _search_cache.get(key)
    if hit and time.time() - hit[0] < SEARCH_TTL:
        return hit[1]
    try:  # persistent cache: fewer YouTube hits after restarts
        row = await asyncio.to_thread(db.search_get, key, SEARCH_DB_TTL)
    except Exception:
        row = None
    if row:
        _remember(key, row)
        return row

    async def job():
        async with _search_sem:
            return await _in_thread(_search_sync, query)

    res = await _dedupe(("s", key), job)
    if res:
        _remember(key, res)
        try:
            await asyncio.to_thread(db.search_put, key, res)
        except Exception as ex:
            log.debug("search_put failed: %s", ex)
    return res


def _remember(key: str, res: dict):
    if len(_search_cache) >= SEARCH_CACHE_MAX:
        _search_cache.pop(next(iter(_search_cache)))  # drop oldest
    _search_cache[key] = (time.time(), res)


# ---------------- download ----------------
def _download_once(page: str, cookie: Cookie | None) -> tuple[str, str, int | None]:
    opts = {
        "format": "bestaudio[ext=m4a]/bestaudio/best",
        "outtmpl": os.path.join(DL_DIR, "%(extractor)s-%(id)s.%(ext)s"),
        "retries": 3,
        "fragment_retries": 3,
        "concurrent_fragment_downloads": 4,
        "max_filesize": MAX_FILESIZE_MB * 1024 * 1024,
        "match_filter": match_filter_func(f"!is_live & duration <=? {MAX_DURATION}"),
    }
    too_big = (f"This track is a live stream, longer than {MAX_DURATION // 60} min "
               f"or bigger than {MAX_FILESIZE_MB} MB.")
    with _ydl(opts, cookie) as y:
        info = y.extract_info(page, download=True)
        if info and info.get("entries"):  # playlist-like page: take the first item only
            info = next((e for e in info["entries"] if e), None)
        if not info:
            raise MediaError(too_big)
        done = info.get("requested_downloads") or []
        path = (done[0].get("filepath") if done else None) or y.prepare_filename(info)
    if not os.path.exists(path):  # rejected by match_filter / max_filesize
        raise MediaError(too_big)
    return path, info.get("title") or page, info.get("duration")


def _download_sync(page: str) -> tuple[str, str, int | None]:
    return _run_with_cookies(lambda c: _download_once(page, c), use_cookies="youtu" in page)


async def fetch(page: str) -> tuple[str, str, int | None]:
    """Return (local audio file, title, duration). Uses cache, dedupes parallel requests."""
    hit = _file_index.get(page)
    if hit and os.path.exists(hit[0]):
        _last_used[hit[0]] = time.time()
        return hit

    async def job():
        async with _dl_sem:
            res = await _in_thread(_download_sync, page)
        _file_index[page] = res
        _last_used[res[0]] = time.time()
        return res

    res = await _dedupe(("d", page), job)
    _last_used[res[0]] = time.time()
    evict(extra_keep={res[0]})  # never evict the file we are about to return
    return res


# ---------------- disk cache ----------------
def evict(extra_keep: set[str] | None = None):
    """Keep cache under MAX_CACHE_MB (LRU) and drop stale files. Never touches files in use."""
    try:
        keep = in_use_provider() | (extra_keep or set())
        now = time.time()
        files = []
        for name in os.listdir(DL_DIR):
            fp = os.path.join(DL_DIR, name)
            try:
                st = os.stat(fp)
            except OSError:
                continue
            files.append((_last_used.get(fp, st.st_mtime), fp, st.st_size))
        files.sort()  # oldest first
        total = sum(f[2] for f in files)
        limit = MAX_CACHE_MB * 1024 * 1024
        for used, fp, size in files:
            stale = now - used > 6 * 3600
            if fp in keep or (total <= limit and not stale):
                continue
            try:
                os.remove(fp)
                total -= size
                _last_used.pop(fp, None)
            except OSError:
                pass
        for page, entry in list(_file_index.items()):
            if not os.path.exists(entry[0]):
                _file_index.pop(page, None)
    except Exception as ex:
        log.warning("cache evict failed: %s", ex)


async def evict_loop():
    while True:
        await asyncio.sleep(600)
        evict()
