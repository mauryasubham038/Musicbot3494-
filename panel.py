"""Admin panel website (bot ke saath hi chalta hai).
Login: browser ka username/password popup. ADMIN_PASSWORD set na ho to panel band rahega.
"""
import asyncio
import base64
import hmac
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from aiohttp import web

import db
import media

ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")
TIMEZONE = os.environ.get("TIMEZONE", "Asia/Kolkata")


def _eq(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode(), b.encode())


_fails: dict[str, list[float]] = {}  # ip -> failed login times (brute force limiter)


def _ip(request: web.Request) -> str:
    xf = request.headers.get("X-Forwarded-For", "")
    # last entry = the one added by our own proxy (first entry can be faked by the client)
    return (xf.split(",")[-1].strip() if xf else request.remote) or "?"


@web.middleware
async def auth(request: web.Request, handler):
    if request.path == "/health":
        return await handler(request)
    ip, now = _ip(request), time.time()
    recent = [t for t in _fails.get(ip, []) if now - t < 600]
    if len(recent) >= 8:
        return web.Response(status=429, text="Too many attempts, try again in 10 minutes.")
    ok = False
    h = request.headers.get("Authorization", "")
    if h.startswith("Basic "):
        try:
            user, _, pw = base64.b64decode(h[6:]).decode().partition(":")
            ok = _eq(user, ADMIN_USER) and _eq(pw, ADMIN_PASSWORD)
        except Exception:
            ok = False
    if not ok:
        if h:  # a real wrong attempt (the first browser request without credentials is free)
            recent.append(now)
            if len(_fails) > 2000:
                _fails.clear()
            _fails[ip] = recent
        await asyncio.sleep(1)
        return web.Response(status=401, text="Unauthorized",
                            headers={"WWW-Authenticate": 'Basic realm="Bot Admin"'})
    _fails.pop(ip, None)
    return await handler(request)


async def index(_):
    return web.Response(text=PAGE, content_type="text/html")


async def api_stats(_):
    d = await asyncio.to_thread(db.stats, TIMEZONE)  # heavy queries off the event loop
    d["library"] = library
    tz = ZoneInfo(TIMEZONE)
    d["cookies"] = [dict(c, last_ok=(datetime.fromtimestamp(c["last_ok"], tz).strftime("%d %b, %I:%M %p")
                                     if c["last_ok"] else "-")) for c in media.cookie_status()]
    return web.json_response(d)


# ---------- Broadcast (sab groups me ek saath message) ----------
library = {"total_songs": 0, "total_singers": 0, "languages": []}  # main.py set karta hai (sirf admin ko dikhta hai)
broadcast_fn = None  # main.py set karta hai: async fn(text, state)
bstate = {"running": False, "done": False, "total": 0, "sent": 0, "failed": 0, "removed": 0}
_task = None


async def _run_broadcast(text: str):
    bstate.update(running=True, done=False, total=0, sent=0, failed=0, removed=0)
    try:
        await broadcast_fn(text, bstate)
    except Exception as ex:
        print("broadcast error:", ex)
    finally:
        bstate.update(running=False, done=True)


async def api_broadcast_status(_):
    return web.json_response(bstate)


async def api_broadcast_send(request: web.Request):
    global _task
    # CSRF se bachav: sirf JSON + custom header wali request chalegi
    if request.content_type != "application/json" or request.headers.get("X-Admin") != "1":
        return web.json_response({"error": "Bad request"}, status=400)
    if broadcast_fn is None:
        return web.json_response({"error": "Broadcast not available"}, status=500)
    if bstate["running"]:
        return web.json_response({"error": "A broadcast is already running"}, status=409)
    try:
        text = ((await request.json()).get("text") or "").strip()
    except Exception:
        text = ""
    if not text:
        return web.json_response({"error": "Message is empty"}, status=400)
    if len(text) > 4000:
        return web.json_response({"error": "Message too long (max 4000 characters)"}, status=400)
    _task = asyncio.create_task(_run_broadcast(text))
    return web.json_response({"ok": True})


async def api_cookie_retry(request: web.Request):
    if request.content_type != "application/json" or request.headers.get("X-Admin") != "1":
        return web.json_response({"error": "Bad request"}, status=400)
    try:
        name = ((await request.json()).get("name") or "").strip()
    except Exception:
        name = ""
    if not media.cookie_reset(name):
        return web.json_response({"error": "Cookie not found"}, status=404)
    return web.json_response({"ok": True})


async def health(_):
    return web.Response(text="ok")


async def start():
    if not ADMIN_PASSWORD:
        print("ADMIN_PASSWORD not set -> admin panel disabled")
        return
    app = web.Application(middlewares=[auth])
    app.add_routes([web.get("/", index), web.get("/api/stats", api_stats),
                    web.get("/api/broadcast", api_broadcast_status),
                    web.post("/api/broadcast", api_broadcast_send),
                    web.post("/api/cookie/retry", api_cookie_retry),
                    web.get("/health", health)])
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    print(f"Admin panel running on port {port}")


async def cleanup_loop():
    """Har 10 minute me 24 ghante purane error logs delete."""
    while True:
        try:
            db.cleanup_errors()
        except Exception as ex:
            print("cleanup error:", ex)
        await asyncio.sleep(600)


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Music Bot Admin</title>
<style>
:root{--bg:#f5f6f8;--card:#fff;--text:#14171c;--muted:#6b7280;--line:#e5e7eb;--green:#16a34a;--red:#dc2626;--blue:#2563eb;--amber:#d97706}
@media (prefers-color-scheme:dark){:root{--bg:#0f1115;--card:#181b21;--text:#e8eaee;--muted:#8b93a1;--line:#2a2f38;--green:#4ade80;--red:#f87171;--blue:#60a5fa;--amber:#fbbf24}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
header{padding:16px;display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap}
h1{font-size:18px;margin:0}
.sub{color:var(--muted);font-size:12px}
main{padding:0 12px 40px;max-width:900px;margin:0 auto}
.grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}
@media(min-width:640px){.grid{grid-template-columns:repeat(4,1fr)}}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px}
.card .n{font-size:26px;font-weight:700}
.card .l{color:var(--muted);font-size:12px}
.green{color:var(--green)}.red{color:var(--red)}.blue{color:var(--blue)}.amber{color:var(--amber)}
h2{font-size:15px;margin:22px 4px 8px}
.box{background:var(--card);border:1px solid var(--line);border-radius:12px;overflow-x:auto}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{padding:8px 10px;text-align:left;border-bottom:1px solid var(--line);white-space:nowrap}
td.wrap{white-space:normal;word-break:break-word;min-width:200px}
th{color:var(--muted);font-weight:600;font-size:12px}
tr:last-child td{border-bottom:0}
.empty{padding:14px;color:var(--muted);font-size:13px}
textarea{width:100%;min-height:110px;padding:10px;border:1px solid var(--line);border-radius:10px;background:var(--bg);color:var(--text);font:inherit;resize:vertical}
button{margin-top:8px;padding:10px 16px;border:0;border-radius:10px;background:var(--blue);color:#fff;font:inherit;font-weight:600}
button:disabled{opacity:.5}
.bpad{padding:12px}
.tag{padding:2px 8px;border-radius:99px;font-size:11px;font-weight:600}
.tag.add{background:rgba(22,163,74,.15);color:var(--green)}
.tag.ok{background:rgba(22,163,74,.15);color:var(--green)}
.tag.blocked{background:rgba(217,119,6,.18);color:var(--amber)}
.tag.dead{background:rgba(220,38,38,.15);color:var(--red)}
.tag.leave{background:rgba(220,38,38,.15);color:var(--red)}
</style>
</head>
<body>
<header>
  <div><h1>🎧 Music Bot Admin</h1><div class="sub" id="now">loading...</div></div>
  <div class="sub">auto refresh: 30s</div>
</header>
<main>
  <div class="grid">
    <div class="card"><div class="n blue" id="groups_now">-</div><div class="l">Groups now (bot is in)</div></div>
    <div class="card"><div class="n amber" id="problem_groups">-</div><div class="l">Groups with problems (24h)</div></div>
    <div class="card"><div class="n green" id="total_added">-</div><div class="l">Total added</div></div>
    <div class="card"><div class="n red" id="total_left">-</div><div class="l">Total left / removed</div></div>
    <div class="card"><div class="n green" id="today_added">-</div><div class="l">Added today</div></div>
    <div class="card"><div class="n red" id="today_left">-</div><div class="l">Left today</div></div>
    <div class="card"><div class="n green" id="month_added">-</div><div class="l">Added this month</div></div>
    <div class="card"><div class="n red" id="month_left">-</div><div class="l">Left this month</div></div>
  </div>

  <h2>📢 Broadcast to all groups</h2>
  <div class="box bpad">
    <textarea id="btext" maxlength="4000" placeholder="Type your promotion message here..."></textarea>
    <button id="bsend" onclick="sendB()">Send to all groups</button>
    <div class="sub" id="bstatus" style="margin-top:8px"></div>
  </div>

  <h2>🎵 Song library <span class="sub">(only you can see this)</span></h2>
  <div class="box" id="library"></div>

  <h2>🍪 YouTube cookies <span class="sub">(status resets on restart)</span></h2>
  <div class="box" id="cookies"></div>

  <h2>Errors <span class="sub">(last 24h: <span id="errors_24h">0</span>, auto-deleted after 1 day)</span></h2>
  <div class="box" id="errors_grouped"></div>
  <h2>Recent error log</h2>
  <div class="box" id="errors_recent"></div>

  <h2>Daily (last 30 days)</h2>
  <div class="box" id="daily"></div>
  <h2>Monthly (last 12 months)</h2>
  <div class="box" id="monthly"></div>
  <h2>Recent add / leave</h2>
  <div class="box" id="events"></div>
  <h2>Groups where bot is present</h2>
  <div class="box" id="active_groups"></div>
</main>
<script>
const $ = id => document.getElementById(id);
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

function table(el, head, rows, empty) {
  if (!rows.length) { el.innerHTML = '<div class="empty">' + esc(empty) + '</div>'; return; }
  el.innerHTML = '<table><thead><tr>' + head.map(h => '<th>' + esc(h) + '</th>').join('') +
    '</tr></thead><tbody>' + rows.map(r => '<tr>' + r.join('') + '</tr>').join('') + '</tbody></table>';
}
const td = (v, cls) => '<td' + (cls ? ' class="' + cls + '"' : '') + '>' + esc(v) + '</td>';

async function load() {
  try {
    const r = await fetch('/api/stats');
    if (r.status === 401) { location.reload(); return; }
    const d = await r.json();
    $('now').textContent = d.now + ' (' + d.tz + ')';
    ['groups_now','problem_groups','total_added','total_left','today_added','today_left','month_added','month_left','errors_24h']
      .forEach(k => $(k).textContent = d[k]);

    const lib = d.library || {languages: []};
    table($('library'), ['Language', 'Songs', 'Singers'],
      lib.languages.map(x => [td(x.language), td(x.songs), td(x.singers)])
        .concat([['<td><b>Total</b></td>', '<td><b>' + esc(lib.total_songs) + '</b></td>', '<td><b>' + esc(lib.total_singers) + '</b></td>']]),
      'No songs loaded (songs.csv missing?).');
    table($('cookies'), ['Cookie', 'Status', 'Used', 'Blocked', 'Last OK', 'Last error', ''],
      (d.cookies || []).map(c => [td(c.name),
        '<td><span class="tag ' + esc(c.status) + '">' + (c.status === 'ok' ? 'OK' : c.status === 'blocked' ? 'BLOCKED (' + esc(c.rest_min) + ' min)' : 'DEAD') + '</span></td>',
        td(c.uses), td(c.blocks), td(c.last_ok), td(c.last_error, 'wrap'),
        c.status === 'ok' ? '<td></td>' : '<td><button style="margin:0;padding:4px 10px" onclick="retryC(\'' + esc(c.name) + '\')">Retry now</button></td>']),
      'No cookies loaded (set COOKIES_B64, COOKIES_B64_1, ...).');
    table($('errors_grouped'), ['Error', 'Times', 'Groups', 'Last'],
      d.errors_grouped.map(e => [td(e.msg, 'wrap'), td(e.count), td(e.groups), td(e.last)]), 'No errors in the last 24 hours 🎉');
    table($('errors_recent'), ['Time', 'Group', 'Error'],
      d.errors_recent.map(e => [td(e.time), td(e.group), td(e.msg, 'wrap')]), 'No errors.');
    table($('daily'), ['Date', 'Added', 'Left'],
      d.daily.map(x => [td(x.date), td(x.added), td(x.left)]), 'No data.');
    table($('monthly'), ['Month', 'Added', 'Left'],
      d.monthly.map(x => [td(x.month), td(x.added), td(x.left)]), 'No data.');
    table($('events'), ['Time', 'Group', 'Event'],
      d.events.map(e => [td(e.time), td(e.group),
        '<td><span class="tag ' + e.kind + '">' + (e.kind === 'add' ? 'ADDED' : 'LEFT') + '</span></td>']),
      'No add/leave events yet.');
    table($('active_groups'), ['Group', 'ID', 'Since'],
      d.active_groups.map(g => [td(g.title), td(g.id), td(g.added)]), 'No groups yet.');
  } catch (e) {
    $('now').textContent = 'Failed to load, retrying...';
  }
}
async function retryC(name) {
  await fetch('/api/cookie/retry', {method: 'POST',
    headers: {'Content-Type': 'application/json', 'X-Admin': '1'}, body: JSON.stringify({name})});
  load();
}
function renderB(s) {
  let t = '';
  if (s.running) t = 'Sending... ' + (s.sent + s.failed) + '/' + s.total + ' (sent ' + s.sent + ', failed ' + s.failed + ')';
  else if (s.done) t = 'Done: sent ' + s.sent + ', failed ' + s.failed + (s.removed ? ' (' + s.removed + ' groups removed from list)' : '');
  $('bstatus').textContent = t;
  $('bsend').disabled = s.running;
}
async function pollB() {
  try {
    const s = await (await fetch('/api/broadcast')).json();
    renderB(s);
    if (s.running) setTimeout(pollB, 2000);
  } catch (e) {}
}
async function sendB() {
  const text = $('btext').value.trim();
  if (!text) return;
  if (!confirm('Send this message to ' + $('groups_now').textContent + ' groups?')) return;
  const r = await fetch('/api/broadcast', {method: 'POST',
    headers: {'Content-Type': 'application/json', 'X-Admin': '1'}, body: JSON.stringify({text})});
  const d = await r.json();
  if (!r.ok) { $('bstatus').textContent = d.error || 'Failed'; return; }
  pollB();
}
load();
pollB();
setInterval(load, 30000);
</script>
</body>
</html>
"""
