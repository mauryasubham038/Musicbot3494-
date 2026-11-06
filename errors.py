"""Turns any exception into: a short CODE, a safe message for the group, and a FIX hint for the owner.

diagnose(ex, context) -> Diag(code, user, hint, fatal)
  code  : stable label shown in the admin panel (errors with the same cause are counted together)
  user  : what the group sees (no technical text, no secrets)
  hint  : what YOU must do to fix it (shown in the admin panel)
  fatal : True = retrying the next song of the queue is pointless (stop instead of skipping all)
"""
import re
from dataclasses import dataclass

_ANSI = re.compile(r"\x1b\[[0-9;]*m")


@dataclass
class Diag:
    code: str
    user: str
    hint: str
    fatal: bool = False


def _text(ex: Exception) -> str:
    return _ANSI.sub("", f"{type(ex).__name__}: {ex}")


def diagnose(ex: Exception) -> Diag:
    name = type(ex).__name__
    t = _text(ex)
    low = t.lower()

    # ---- Telegram: voice chat / admin rights ----
    if "chatadminrequired" in low or "chat_admin_required" in low:
        if "creategroupcall" in low:
            return Diag("NO_VOICE_CHAT",
                        "No active voice chat. Start a voice chat in this group and try again.",
                        "Group has no voice chat and the assistant may not create one. Start a voice "
                        "chat, or make the assistant admin with 'Manage video chats'.", True)
        if "exportchatinvite" in low or "createchatinvite" in low or "importchatinvite" in low:
            return Diag("BOT_NEEDS_INVITE_RIGHT",
                        "I need the admin right 'Invite users' to bring my assistant in. "
                        "Please give it to me and try again.",
                        "Bot is not admin (or lacks 'Invite users') in this group.", True)
        return Diag("ADMIN_REQUIRED", "I need admin rights in this group to do that.",
                    "Bot/assistant lacks an admin right in this group.", True)
    if any(k in name for k in ("NoActiveGroupCall", "GroupCallNotFound")) or "no active group call" in low:
        return Diag("NO_VOICE_CHAT",
                    "No active voice chat. Start a voice chat in this group and try again.",
                    "No voice chat is running in this group.", True)
    if name.startswith("InviteHash"):
        return Diag("INVITE_LINK_DEAD",
                    "My assistant could not join with the invite link. Please add it to the group "
                    "manually and try again.",
                    "Invite link expired/revoked. Bot must be admin with 'Invite users'; otherwise "
                    "add the assistant account to the group by hand.", True)
    if "Banned" in name or name == "UserKicked":
        return Diag("ASSISTANT_BANNED", "My assistant is banned in this group. Please unban it.",
                    "Assistant account is banned/removed in this group: unban it.", True)
    if "ChannelsTooMuch" in name:
        return Diag("ASSISTANT_FULL",
                    "My assistant account is in too many groups. Please try again later.",
                    "Assistant account hit Telegram's group limit: add another SESSION_STRING "
                    "(SESSION_STRINGS) or lower IDLE_LEAVE_DAYS so idle groups are left.", True)
    if any(k in name for k in ("AuthKeyUnregistered", "SessionRevoked", "SessionExpired",
                               "UserDeactivated", "AuthKeyDuplicated")):
        return Diag("ASSISTANT_DEAD",
                    "My assistant account is not working right now. The owner has been notified.",
                    "Assistant session is dead (revoked / account banned): make a new SESSION_STRING "
                    "with gen_session.py and redeploy.", True)
    if name == "InviteRequestSent":
        return Diag("JOIN_REQUEST_PENDING", "Approve the join request of my assistant and try again.",
                    "Group needs admin approval for new members.", True)
    if name == "FloodWait" or "flood_wait" in low:
        return Diag("TELEGRAM_FLOOD", "Telegram is limiting me for a moment. Try again in a minute.",
                    "Telegram rate limit (FloodWait). Too many joins/messages; spread across more "
                    "assistant accounts (SESSION_STRINGS).")
    if any(k in name for k in ("UserNotParticipant", "ChatForbidden", "ChannelPrivate")):
        return Diag("ASSISTANT_REMOVED", "My assistant was removed from the group. Send /play again.",
                    "Assistant left/was removed; it rejoins on the next /play.")
    if name in ("PeerIdInvalid", "ChannelInvalid") or "peer id invalid" in low:
        return Diag("PEER_UNKNOWN", "I can't reach this group right now. Send /play again.",
                    "Assistant has not seen this group yet (after restart). Usually fixes itself "
                    "after the assistant joins/gets a message.")
    if name == "MessageNotModified":
        return Diag("IGNORE_NOT_MODIFIED", "", "Harmless: same message edited twice.")

    # ---- YouTube / download ----
    if re.search(r"sign in to confirm|not a bot|cookies? (are|is) no longer valid|please sign in", low):
        return Diag("YT_BLOCKED", "YouTube is busy right now. Please try again in a minute.",
                    "YouTube blocked the server/cookie. Add more cookies (COOKIES_B64_1, _2 ...) from "
                    "fresh dummy accounts, or set PROXY to a residential proxy.")
    if "429" in low or "too many requests" in low:
        return Diag("YT_RATE_LIMIT", "YouTube is busy right now. Please try again in a minute.",
                    "YouTube rate limit (HTTP 429). Add cookies / proxy.")
    if re.search(r"video unavailable|private video|has been removed|not available in your country|"
                 r"blocked it|copyright|members-only|age.restricted|confirm your age", low):
        return Diag("VIDEO_UNAVAILABLE", "That video can't be played (private/removed/age-restricted). "
                                         "Try another version of the song.",
                    "Video unavailable. Only a problem for this one song.")
    if re.search(r"requested format is not available|only images are available|nsig|signature", low):
        return Diag("YT_FORMAT", "YouTube changed something, please try again in a minute.",
                    "yt-dlp cannot read formats: update yt-dlp (redeploy updates it) and check that "
                    "deno is installed in the Dockerfile.")
    if name == "MediaError":
        return Diag("MEDIA_REJECTED", str(ex) or "This track can't be played.",
                    "Song rejected (not found / too long / too big / live).")
    if re.search(r"timed out|timeout|connection (reset|aborted|refused)|temporary failure|"
                 r"name or service not known|network is unreachable", low) or name in (
            "TimeoutError", "ConnectionError", "ServerDisconnectedError"):
        return Diag("NETWORK", "Network problem, please try again.",
                    "Server could not reach YouTube/Telegram in time. Check Railway status/proxy.")
    if "ffmpeg" in low:
        return Diag("FFMPEG", "Audio processing failed. Try another song.",
                    "ffmpeg problem: make sure the Docker image has ffmpeg (apt-get install ffmpeg).")
    if "no space left" in low:
        return Diag("DISK_FULL", "Server storage is full, please try again later.",
                    "Disk full: lower MAX_CACHE_MB or enlarge the volume.", True)
    if isinstance(ex, FileNotFoundError):
        return Diag("FILE_MISSING", "Audio file vanished, please try again.",
                    "Cached audio file was deleted before use (cache cleanup / restart).")
    return Diag("UNKNOWN", "Something went wrong with this song, skipping.",
                f"Unexpected error {name}. Check the full traceback in the Railway logs.")
