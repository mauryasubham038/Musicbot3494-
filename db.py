"""Chhota SQLite storage, sirf admin panel ke liye.

Store hota hai:
  - groups  : bot kin groups me hai (id + naam)
  - events  : group add / leave ki date-time
  - errors  : error log (24 ghante baad apne aap delete)
Gaane, users ya queue ka koi record nahi rakha jata.
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

os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
con = sqlite3.connect(DB_PATH, check_same_thread=False)
_lock = threading.RLock()  # db is used from the event loop AND worker threads
con.execute("PRAGMA journal_mode=WAL")
con.execute("PRAGMA synchronous=NORMAL")
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
CREATE INDEX IF NOT EXISTS ix_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS ix_errors_ts ON errors(ts);
""")
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


@_locked
def mark_left(chat_id: int, title: str | None):
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
    """Group me koi command aaya. Naya group ho to chupchap list me jodo (add event nahi)."""
    row = _q("SELECT active FROM groups WHERE chat_id=?", chat_id)
    if not row:
        con.execute("INSERT INTO groups(chat_id,title,active,added_at) VALUES(?,?,1,?)",
                    (chat_id, title, time.time()))
        con.commit()
    elif row[0][0] == 0:
        mark_added(chat_id, title)


def active_group_ids() -> list[int]:
    return [r[0] for r in _q("SELECT chat_id FROM groups WHERE active=1")]


# ---------- errors ----------
@_locked
def log_error(chat_id: int | None, where: str, ex: Exception | None = None):
    msg = where if ex is None else f"{where}: {type(ex).__name__}: {ex}"
    msg = _ANSI.sub("", msg).replace("ERROR: ", "").strip()
    msg = _VIDEO_ID.sub(r"\1:", msg)[:300]  # video id hatao, taaki same error ek hi row me gine
    print("[error]", chat_id, msg)
    con.execute("INSERT INTO errors(ts,chat_id,msg) VALUES(?,?,?)", (time.time(), chat_id, msg))
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


# ---------- stats for panel ----------
def stats(tzname: str = "Asia/Kolkata") -> dict:
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
        for r in _q("SELECT substr(msg,1,150), COUNT(*), COUNT(DISTINCT chat_id), MAX(ts) "
                    "FROM errors WHERE ts>=? GROUP BY substr(msg,1,150) "
                    "ORDER BY COUNT(*) DESC LIMIT 20", cutoff)
    ]
    recent_err = [
        {"time": fmt(r[0]), "group": r[1] or (str(r[2]) if r[2] else "-"), "msg": r[3]}
        for r in _q("SELECT e.ts, g.title, e.chat_id, e.msg FROM errors e "
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
