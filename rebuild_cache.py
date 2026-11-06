"""Rebuild the song cache (table `tgcache` in bot.db) from the storage channel.

Use it when the database was lost (for example DB_PATH was not on a Railway Volume): the songs are
still in the Telegram channel, only the bot's list of them is gone.

How: python rebuild_cache.py
  Needs the same variables as the bot: API_ID, API_HASH, BOT_TOKEN, STORAGE_CHANNEL and DB_PATH.
  Run it with the SAME BOT_TOKEN that uploaded the songs (a file_id only works for its own bot).
  Then restart the bot: it loads the list at startup.

A bot cannot read the history of a channel, but it can read messages by id, so this walks
message ids 1, 2, 3 ... and stops after STOP_AFTER_EMPTY ids in a row that do not exist. Each song
message has the cache key as the first caption line (the bot writes it when it uploads).
Existing rows are never overwritten.
"""
import asyncio
import os

from dotenv import load_dotenv

load_dotenv()

from pyrogram import Client  # noqa: E402

import db  # noqa: E402

STORAGE_CHANNEL = int(os.environ.get("STORAGE_CHANNEL", "0") or 0)
BATCH = 100
STOP_AFTER_EMPTY = int(os.environ.get("STOP_AFTER_EMPTY", "400"))


async def main():
    if not STORAGE_CHANNEL:
        raise SystemExit("Set STORAGE_CHANNEL (-100...) first.")
    app = Client("rebuild", api_id=int(os.environ["API_ID"]), api_hash=os.environ["API_HASH"],
                 bot_token=os.environ["BOT_TOKEN"], in_memory=True)
    await app.start()
    added = skipped = other = 0
    empty_streak, next_id = 0, 1
    try:
        while empty_streak < STOP_AFTER_EMPTY:
            ids = list(range(next_id, next_id + BATCH))
            next_id += BATCH
            msgs = await app.get_messages(STORAGE_CHANNEL, ids)
            for m in msgs:
                if m is None or getattr(m, "empty", False):
                    empty_streak += 1
                    continue
                empty_streak = 0
                media = m.audio or m.document
                lines = (m.caption or "").split("\n")
                key = lines[0].strip() if lines else ""
                if not media or not (key.startswith("q:") or key.startswith("u:")):
                    other += 1  # not a song written by the bot
                    continue
                page = lines[1].strip() if len(lines) > 1 and lines[1].strip() else None
                if page is None and key.startswith("u:"):
                    page = key[2:]  # for link keys the page is the key itself
                ext = os.path.splitext(getattr(media, "file_name", None) or "")[1] or ".m4a"
                title = getattr(media, "title", None)
                dur = getattr(media, "duration", None)
                if db.tgcache_put_if_missing(key, media.file_id, title, dur, page, ext):
                    added += 1
                else:
                    skipped += 1
            print(f"checked up to message {next_id - 1}: added {added}, already known {skipped}")
            await asyncio.sleep(1)  # be gentle with Telegram
    finally:
        await app.stop()
    print(f"\nDone. Added {added} songs, {skipped} were already in the database, "
          f"{other} messages were not songs. Songs in cache now: {db.tgcache_count()}")
    print("Restart the bot so it loads the list.")


if __name__ == "__main__":
    asyncio.run(main())
