# Is update me kya badla

## Assistant / group join
- /setup : bot ke rights, assistant, voice chat check karke batata hai kya kami hai (sirf admins).
- Assistant add na ho paye to gaana yaad rakha jata hai. Assistant add hote hi (ya admin right milte hi) gaana khud shuru. Wait: RESUME_WAIT (default 300 sec).
- Group -> assistant mapping DB me save (restart ke baad duplicate assistant nahi aata).
- FloodWait / "too many groups" / dead session: dusra assistant use hota hai. Panel me "Assistants" table. OWNER_ID set karo to dead session par private alert.
- Assistant ban ho to group me message: "assistant BANNED, unban karo". Unban par bhi message. /setup me bhi dikhta hai.
- IDLE_LEAVE_DAYS (default 30): jis group me itne din gaana nahi baja, assistant nikal jata hai (agli /play par wapas aata hai). 0 = band.

## Notices
- Voice chat shuru / band hone par group me time ke saath message (TIMEZONE ke hisaab se). /vcnotice on|off
- Naya member aaye to private welcome (naam + group ka naam). Telegram ke niyam se DM sirf unko jaata hai jinhone bot ko /start kiya ho; baaki ko group me chhota welcome (60 sec baad delete). /welcome on|off, WELCOME_TEXT se text badal sakte ho.

## Speed
- tgcache lookups RAM me, hit count 1 minute me ek baar likha jata hai, seen() RAM me, SQLite PRAGMA, panel stats 20 sec cache.
- Storage channel upload: ek time par ek, beech me UPLOAD_GAP (3 sec), FloodWait par ek retry, backlog limit.

## Naya script
- rebuild_cache.py : DB kho jaye to storage channel se gaano ki list wapas banata hai (usi BOT_TOKEN se).

## Nayi env variables
OWNER_ID, IDLE_LEAVE_DAYS, RESUME_WAIT, VC_NOTICE_DEFAULT, WELCOME_DEFAULT, WELCOME_TEXT, UPLOAD_GAP, MAX_UPLOAD_BACKLOG (.env.example dekho)
