"""Chhota SQLite storage.

Store hota hai:
  - groups        : bot kin groups me hai (id + naam)
  - events        : group add / leave ki date-time
  - errors        : error log (24 ghante baad apne aap delete)
  - searches      : search cache
  - tgcache       : Telegram storage channel me save gaane (file_id)
  - assign        : kis group ka kaun sa assistant hai (restart ke baad bhi yaad rehta hai)
  - chat_settings : group ki chhoti settings (vc notice, welcome on/off)
Gaane, users ya queue ka koi record nahi rakha jata.

Speed: tgcache, settings aur "known group" RAM me rehte hain. Baar-baar likhne wali cheezein
(hit count, assistant last_used) RAM me jama hoti hain aur flush_buffers() se ek saath likhi jati hain.
"""
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

DB_PATH = os.environ.get("DB_PATH", "data/bot.db")
ERROR_TTL = 24 * 60 * 60  # 1 din
STATS_TTL = 20  # seconds: panel refresh par bhaari queries baar-baar na chalein

os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
con = sqlite3.connect(DB_PATH, check_same_thread=False)
_lock = threading.RLock()  # db is used from the event loop AND worker threads
con.execute("PRAGMA journal_mode=WAL")
con.execute("PRAGMA synchronous=NORMAL")
con.execute("PRAGMA busy_timeout=5000")
con.execute("PRAGMA temp_store=MEMORY")
con.execute("PRAGMA cache_size=-20000")  # ~20 MB page cache
con.execute("PRAGMA mmap_size=67108864")
con.executescript("""
CREATE TABLE IF NOT EXISTS groups (
    chat_id INTEGER PRIMARY KEY, title TEXT, active INTEGER NOT NULL,
    added_at REAL, left_at REAL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL,
    chat_id INTEGER, title TEXT, kind TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL,
    chat_id INTEGER, msg TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS searches (
    key TEXT PRIMARY KEY, title TEXT, page TEXT NOT NULL, duration INTEGER, ts REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS tgcache (
    key TEXT PRIMARY KEY, file_id TEXT NOT NULL, title TEXT, duration INTEGER,
    page TEXT, ext TEXT, ts REAL NOT NULL, hits INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS assign (
    chat_id INTEGER PRIMARY KEY, assistant_id INTEGER NOT NULL,
    assigned_at REAL NOT NULL, last_used REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS chat_settings (
    chat_id INTEGER NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
    PRIMARY KEY (chat_id, key)
);
CREATE INDEX IF NOT EXISTS ix_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS ix_errors_ts ON errors(ts);
CREATE INDEX IF NOT EXISTS ix_groups_active ON groups(active);
CREATE INDEX IF NOT EXISTS ix_assign_last ON assign(last_used);
""")
try:  # older databases (before error codes) get the new column
    con.execute("ALTER TABLE errors ADD COLUMN detail TEXT")
except sqlite3.OperationalError:
    pass
con.commit()

_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_VIDEO_ID = re.compile(r"(\[[^\]]+\]) [\w-]{6,}:")


def _locked(fn):
    import functools

    @functools.wraps(fn)
    def w(*a, **k):
        with _lock:
            return fn(*a, **k)
    return w


def _q(sql, *a):
    with _lock:
        return con.execute(sql, a).fetchall()


# ---------- RAM copies (loaded once) ----------
_tg: dict[str, dict] = {}  # tgcache key -> row
_tg_hits: dict[str, int] = {}  # key -> hits not yet written
_assign_touch: dict[int, float] = {}  # chat_id -> last_used not yet written
_settings: dict[tuple[int, str], str] = {}
_active_seen: set[int] = set()  # groups known to be active: seen() skips the DB for them

for _r in con.execute("SELECT key, file_id, title, duration, page, ext FROM tgcache"):
    _tg[_r[0]] = {"file_id": _r[1], "title": _r[2], "duration": _r[3], "page": _r[4],
                  "ext": _r[5] or ".m4a"}
for _r in con.execute("SELECT chat_id, key, value FROM chat_settings"):
    _settings[(_r[0], _r[1])] = _r[2]


# ---------- groups ----------
@_locked
def mark_added(chat_id: int, title: str | None):
    row = _q("SELECT active FROM groups WHERE chat_id=?", chat_id)
    now = time.time()
    if row and row[0][0] == 1:
        con.execute("UPDATE groups SET title=? WHERE chat_id=?", (title, chat_id))
    else:
        con.execute(
            "INSERT INTO groups(chat_id,title,active,added_at) VALUES(?,?,1,?) "
            "ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title, active=1, added_at=?, left_at=NULL",
            (chat_id, title, now, now))
        con.execute("INSERT INTO events(ts,chat_id,title,kind) VALUES(?,?,?, 'add')", (now, chat_id, title))
    con.commit()
    _active_seen.add(chat_id)


@_locked
def mark_left(chat_id: int, title: str | None):
    _active_seen.discard(chat_id)
    row = _q("SELECT active, title FROM groups WHERE chat_id=?", chat_id)
    if not row or row[0][0] != 1:
        return
    now = time.time()
    t = title or row[0][1]
    con.execute("UPDATE groups SET active=0, left_at=? WHERE chat_id=?", (now, chat_id))
    con.execute("INSERT INTO events(ts,chat_id,title,kind) VALUES(?,?,?, 'leave')", (now, chat_id, t))
    con.commit()


@_locked
def seen(chat_id: int, title: str | None):
    """Group me koi command aaya. Naya group ho to chupchap list me jodo (add event nahi).
    Known active group: DB ko chhua bhi nahi jata (RAM set)."""
    if chat_id in _active_seen:
        return
    row = _q("SELECT active FROM groups WHERE chat_id=?", chat_id)
    if not row:
        con.execute("INSERT INTO groups(chat_id,title,active,added_at) VALUES(?,?,1,?)",
                    (chat_id, title, time.time()))
        con.commit()
        _active_seen.add(chat_id)
    elif row[0][0] == 0:
        mark_added(chat_id, title)
    else:
        _active_seen.add(chat_id)


def active_group_ids() -> list[int]:
    return [r[0] for r in _q("SELECT chat_id FROM groups WHERE active=1")]


# ---------- errors ----------
@_locked
def log_error(chat_id: int | None, where: str, ex: Exception | None = None,
              code: str | None = None, hint: str | None = None, detail: str | None = None):
    """With `code`: msg = '[CODE] where -> what to do' (stable, so equal causes are counted together)
    and the raw technical text goes to `detail`. Without code: old behaviour."""
    if code:
        msg = f"[{code}] {where}" + (f" -> {hint}" if hint else "")
        detail = detail or (f"{type(ex).__name__}: {ex}" if ex is not None else None)
    else:
        msg = where if ex is None else f"{where}: {type(ex).__name__}: {ex}"
    msg = _ANSI.sub("", msg).replace("ERROR: ", "").strip()
    msg = _VIDEO_ID.sub(r"\1:", msg)[:400]  # video id hatao, taaki same error ek hi row me gine
    if detail:
        detail = _ANSI.sub("", detail).replace("ERROR: ", "").strip()[:600]
    print("[error]", chat_id, msg, "|", detail or "")
    con.execute("INSERT INTO errors(ts,chat_id,msg,detail) VALUES(?,?,?,?)",
                (time.time(), chat_id, msg, detail))
    con.commit()


@_locked
def cleanup_errors():
    con.execute("DELETE FROM errors WHERE ts < ?", (time.time() - ERROR_TTL,))
    con.execute("DELETE FROM searches WHERE ts < ?", (time.time() - 30 * 24 * 3600,))
    con.commit()


# ---------- persistent search cache (used by media.py) ----------
def search_get(key: str, ttl: float) -> dict | None:
    row = _q("SELECT title, page, duration, ts FROM searches WHERE key=?", key)
    if row and time.time() - row[0][3] < ttl:
        return {"title": row[0][0], "page": row[0][1], "duration": row[0][2]}
    return None


@_locked
def search_put(key: str, res: dict):
    con.execute("INSERT OR REPLACE INTO searches(key,title,page,duration,ts) VALUES(?,?,?,?,?)",
                (key, res.get("title"), res["page"], res.get("duration"), time.time()))
    con.commit()


# ---------- songs stored in the Telegram storage channel (used by main.py) ----------
# Lookups are served from RAM (_tg). Hit counts are buffered and written by flush_buffers().
@_locked
def tgcache_get(key: str) -> dict | None:
    row = _tg.get(key)
    if not row:
        return None
    _tg_hits[key] = _tg_hits.get(key, 0) + 1
    return dict(row)


@_locked
def tgcache_put(key: str, file_id: str, title: str | None, duration, page: str | None, ext: str):
    con.execute("INSERT OR REPLACE INTO tgcache(key,file_id,title,duration,page,ext,ts) "
                "VALUES(?,?,?,?,?,?,?)", (key, file_id, title, duration, page, ext, time.time()))
    con.commit()
    _tg[key] = {"file_id": file_id, "title": title, "duration": duration, "page": page,
                "ext": ext or ".m4a"}


@_locked
def tgcache_put_if_missing(key: str, file_id: str, title: str | None, duration, page: str | None,
                           ext: str) -> bool:
    """For rebuild_cache.py: never overwrite a working row. True if inserted."""
    if key in _tg:
        return False
    tgcache_put(key, file_id, title, duration, page, ext)
    return True


@_locked
def tgcache_delete(key: str):
    con.execute("DELETE FROM tgcache WHERE key=?", (key,))
    con.commit()
    _tg.pop(key, None)
    _tg_hits.pop(key, None)


def tgcache_has(key: str) -> bool:
    """Is this song already stored? (does not count as a hit)"""
    return key in _tg


def tgcache_count() -> int:
    return len(_tg)


# ---------- assistant <-> group mapping ----------
@_locked
def assign_all() -> list[tuple[int, int, float]]:
    return [(r[0], r[1], r[2]) for r in
            con.execute("SELECT chat_id, assistant_id, last_used FROM assign").fetchall()]


@_locked
def assign_put(chat_id: int, assistant_id: int):
    now = time.time()
    con.execute("INSERT OR REPLACE INTO assign(chat_id,assistant_id,assigned_at,last_used) "
                "VALUES(?,?,?,?)", (chat_id, assistant_id, now, now))
    con.commit()
    _assign_touch.pop(chat_id, None)


@_locked
def assign_delete(chat_id: int):
    con.execute("DELETE FROM assign WHERE chat_id=?", (chat_id,))
    con.commit()
    _assign_touch.pop(chat_id, None)


@_locked
def assign_touch(chat_id: int):
    """A song started in this group (RAM only; written by flush_buffers)."""
    _assign_touch[chat_id] = time.time()


@_locked
def assign_idle(before_ts: float) -> list[tuple[int, int]]:
    """Groups whose assistant has not been used since before_ts."""
    flush_buffers()
    return [(r[0], r[1]) for r in
            con.execute("SELECT chat_id, assistant_id FROM assign WHERE last_used < ?",
                        (before_ts,)).fetchall()]


@_locked
def flush_buffers():
    """Write buffered hit counts and last_used times in ONE transaction."""
    if not _tg_hits and not _assign_touch:
        return
    hits, touch = dict(_tg_hits), dict(_assign_touch)
    _tg_hits.clear()
    _assign_touch.clear()
    if hits:
        con.executemany("UPDATE tgcache SET hits=hits+? WHERE key=?",
                        [(n, k) for k, n in hits.items()])
    if touch:
        con.executemany("UPDATE assign SET last_used=? WHERE chat_id=?",
                        [(ts, cid) for cid, ts in touch.items()])
    con.commit()


# ---------- per-group settings ----------
def setting_get(chat_id: int, key: str, default: str = "") -> str:
    return _settings.get((chat_id, key), default)


@_locked
def setting_set(chat_id: int, key: str, value: str):
    con.execute("INSERT OR REPLACE INTO chat_settings(chat_id,key,value) VALUES(?,?,?)",
                (chat_id, key, value))
    con.commit()
    _settings[(chat_id, key)] = value


# ---------- stats for panel ----------
_stats_cache: dict[str, tuple[float, dict]] = {}


def stats(tzname: str = "Asia/Kolkata") -> dict:
    """Cached for STATS_TTL seconds: opening the panel many times does not hammer SQLite."""
    hit = _stats_cache.get(tzname)
    if hit and time.time() - hit[0] < STATS_TTL:
        return dict(hit[1])
    res = _stats_uncached(tzname)
    _stats_cache[tzname] = (time.time(), res)
    return dict(res)


def _stats_uncached(tzname: str) -> dict:
    cleanup_errors()
    tz = ZoneInfo(tzname)
    now = datetime.now(tz)
    day0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
    month0 = day0.replace(day=1)

    def cnt(kind, since=0.0):
        return _q("SELECT COUNT(*) FROM events WHERE kind=? AND ts>=?", kind, since)[0][0]

    def fmt(ts):
        return datetime.fromtimestamp(ts, tz).strftime("%d %b, %I:%M %p")

    # daily (last 30 days) + monthly (last 12 months)
    start_day = (day0 - timedelta(days=29)).timestamp()
    daily = {}
    for i in range(30):
        d = (day0 - timedelta(days=i)).date()
        daily[d] = [0, 0]
    for ts, kind in _q("SELECT ts, kind FROM events WHERE ts>=?", start_day):
        d = datetime.fromtimestamp(ts, tz).date()
        if d in daily:
            daily[d][0 if kind == "add" else 1] += 1

    months = {}
    y, m = now.year, now.month
    for _ in range(12):
        months[(y, m)] = [0, 0]
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    first_month = min(months)
    first_ts = datetime(first_month[0], first_month[1], 1, tzinfo=tz).timestamp()
    for ts, kind in _q("SELECT ts, kind FROM events WHERE ts>=?", first_ts):
        dt = datetime.fromtimestamp(ts, tz)
        k = (dt.year, dt.month)
        if k in months and k >= first_month:
            months[k][0 if kind == "add" else 1] += 1

    # errors (last 24h)
    cutoff = time.time() - ERROR_TTL
    e_total, e_groups = _q(
        "SELECT COUNT(*), COUNT(DISTINCT chat_id) FROM errors WHERE ts>=?", cutoff)[0]
    grouped = [
        {"msg": r[0], "count": r[1], "groups": r[2], "last": fmt(r[3])}
        for r in _q("SELECT substr(msg,1,400), COUNT(*), COUNT(DISTINCT chat_id), MAX(ts) "
                    "FROM errors WHERE ts>=? GROUP BY substr(msg,1,400) "
                    "ORDER BY COUNT(*) DESC LIMIT 20", cutoff)
    ]
    recent_err = [
        {"time": fmt(r[0]), "group": r[1] or (str(r[2]) if r[2] else "-"), "msg": r[3],
         "detail": r[4] or ""}
        for r in _q("SELECT e.ts, g.title, e.chat_id, e.msg, e.detail FROM errors e "
                    "LEFT JOIN groups g ON g.chat_id=e.chat_id "
                    "WHERE e.ts>=? ORDER BY e.ts DESC LIMIT 50", cutoff)
    ]

    events = [
        {"time": fmt(r[0]), "group": r[1] or str(r[2]), "kind": r[3]}
        for r in _q("SELECT ts, title, chat_id, kind FROM events ORDER BY ts DESC LIMIT 50")
    ]
    active = [
        {"title": r[0] or str(r[1]), "id": r[1], "added": fmt(r[2]) if r[2] else "-"}
        for r in _q("SELECT title, chat_id, added_at FROM groups WHERE active=1 "
                    "ORDER BY added_at DESC LIMIT 100")
    ]

    return {
        "now": now.strftime("%d %b %Y, %I:%M %p"),
        "tz": tzname,
        "groups_now": _q("SELECT COUNT(*) FROM groups WHERE active=1")[0][0],
        "total_added": cnt("add"),
        "total_left": cnt("leave"),
        "today_added": cnt("add", day0.timestamp()),
        "today_left": cnt("leave", day0.timestamp()),
        "month_added": cnt("add", month0.timestamp()),
        "month_left": cnt("leave", month0.timestamp()),
        "problem_groups": e_groups,
        "errors_24h": e_total,
        "daily": [{"date": d.strftime("%d %b"), "added": v[0], "left": v[1]}
                  for d, v in sorted(daily.items(), reverse=True)],
        "monthly": [{"month": datetime(k[0], k[1], 1).strftime("%b %Y"), "added": v[0], "left": v[1]}
                    for k, v in sorted(months.items(), reverse=True)],
        "errors_grouped": grouped,
        "errors_recent": recent_err,
        "events": events,
        "active_groups": active,
    }
