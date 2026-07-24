"""
Promo Code Watcher — Pyrogram + aiohttp

Listens to @patrickstarsfarm for promo codes in real-time.
Pushes new codes to worker instances immediately.
Exposes HTTP endpoints for cron keepalive and health checks.

Run:  python monitor.py
Docker: pip install -r requirements.txt && python monitor.py
"""

import os
import json
import logging
import asyncio
from datetime import datetime, timezone

from pyrogram import Client, filters, idle
from pyrogram.enums import ChatType
from pyrogram.types import Message
from aiohttp import web
from supabase import create_client, Client as SupabaseClient

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
API_ID       = int(os.environ["API_ID"])
API_HASH     = os.environ["API_HASH"]
SESSION      = os.environ["MONITOR_SESSION"]
CHANNEL      = "patrickstarsfarm"
PORT         = int(os.environ.get("PORT", 10001))

INSTANCE_URLS = [
    u.strip()
    for u in os.environ.get("INSTANCE_URLS", "").split(",")
    if u.strip()
]

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ["SUPABASE_ANON_KEY"]

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("monitor")

supabase: SupabaseClient = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─────────────────────────────────────────────
# PUSH — wake every instance to redeem NOW
# ─────────────────────────────────────────────
async def push_to_instances(code: str, code_id: int | None = None):
    if not INSTANCE_URLS:
        log.warning("[PUSH] No INSTANCE_URLS configured — skipping")
        return

    ok = 0
    for base in INSTANCE_URLS:
        url = f"{base.rstrip('/')}/promo"
        try:
            async with asyncio.timeout(8):
                async with __import__("aiohttp").ClientSession() as sess:
                    async with sess.get(url) as resp:
                        log.info(f"[PUSH] {url} → {resp.status}")
                        if resp.ok:
                            ok += 1
        except Exception as e:
            log.warning(f"[PUSH] {url} failed: {e}")

    log.info(f"[PUSH] Notified {len(INSTANCE_URLS)} instance(s) for \"{code}\" — {ok} answered OK")

    if code_id:
        try:
            supabase.table("promo_codes").update({
                "pushed_count": len(INSTANCE_URLS),
                "push_ok_count": ok,
            }).eq("id", code_id).execute()
        except Exception as e:
            log.error(f"[PUSH] Outcome record failed: {e}")


# ─────────────────────────────────────────────
# PROMO EXTRACTION
# ─────────────────────────────────────────────
def extract_promos(text: str) -> list[dict]:
    results = []
    lines = [l.strip() for l in text.split("\n")]

    for i, line in enumerate(lines):
        if not _trigger.match(line):
            continue

        stars_m     = _stars.search(line)
        activates_m = _activs.search(line)

        code_line = ""
        colon_pos = line.rfind(":")
        if colon_pos != -1:
            after = line[colon_pos + 1:].strip()
            if after:
                code_line = after
            else:
                for j in range(i + 1, len(lines)):
                    if lines[j]:
                        code_line = lines[j]
                        break

        if not code_line:
            continue

        code = _clean.sub("", code_line).strip()
        if (
            not code
            or "http" in code
            or "t.me" in code
            or "telegram.me" in code
            or len(code) < 2
            or len(code) > 60
        ):
            continue

        results.append({
            "code": code,
            "stars_amount": int(stars_m.group(1)) if stars_m else None,
            "max_activations": int(activates_m.group(1)) if activates_m else None,
        })

    return results


import re
_trigger  = re.compile(r"Ловите\s+дейли\s+про", re.IGNORECASE)
_stars    = re.compile(r"на\s+(\d+)\s*(?:⭐️?|🌟|звёзд|star)", re.IGNORECASE)
_activs   = re.compile(r"(\d+)\s+активаци")
_clean    = re.compile(r"[\|*_]")


# ─────────────────────────────────────────────
# HANDLE NEW CODE
# ─────────────────────────────────────────────
async def handle_new_code(
    code: str,
    meta: dict,
    raw_message: str,
    push: bool = True,
    message_at: datetime | None = None,
    source: str = "event",
):
    detected_at = datetime.now(timezone.utc)
    msg_ts = message_at.isoformat() if message_at else None
    lag = ""
    if message_at:
        delta = (detected_at - message_at).total_seconds()
        lag = f"  |  {delta:.1f}s since posted"

    log.info(f"Code: \"{code}\"  |  {meta.get('stars_amount') or '?'}⭐  |  {meta.get('max_activations') or '?'} activations  |  {source}{lag}")

    try:
        res = supabase.table("promo_codes").insert({
            "code": code,
            "raw_message": raw_message[:500],
            "stars_amount": meta.get("stars_amount"),
            "max_activations": meta.get("max_activations"),
            "message_at": msg_ts,
            "source": source,
        }).execute()

        if res.data:
            code_id = res.data[0]["id"]
            if push:
                log.info(f"Saved to DB (id={code_id}) — pushing to instances")
                asyncio.create_task(push_to_instances(code, code_id))
            else:
                log.info(f"Saved to DB (id={code_id}) — startup backfill, not pushing")
    except Exception as e:
        if "23505" in str(e):
            log.info(f"\"{code}\" already in DB — skipping")
        else:
            log.error(f"DB insert error: {e}")


# ─────────────────────────────────────────────
# MONITOR STATE — last_seen_msg_id
# ─────────────────────────────────────────────
_last_seen_cache: int = 0


def get_last_seen() -> int:
    global _last_seen_cache
    if _last_seen_cache:
        return _last_seen_cache
    try:
        res = supabase.table("monitor_state").select("value").eq("key", "last_seen_msg_id").execute()
        if res.data:
            _last_seen_cache = int(res.data[0]["value"].get("msg_id", 0))
    except Exception:
        pass
    return _last_seen_cache


def set_last_seen(msg_id: int):
    global _last_seen_cache
    _last_seen_cache = msg_id
    try:
        supabase.table("monitor_state").upsert({
            "key": "last_seen_msg_id",
            "value": {"msg_id": msg_id},
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }, on_conflict="key").execute()
    except Exception as e:
        log.error(f"Failed to update last_seen_msg_id: {e}")


# ─────────────────────────────────────────────
# HEARTBEAT — liveness sample every 60s
# ─────────────────────────────────────────────
_heartbeat_count = 0
_channel_reachable = False


def write_heartbeat():
    global _heartbeat_count
    try:
        supabase.table("monitor_heartbeat").insert({
            "connected": True,
            "channel_ok": _channel_reachable,
            "last_seen_msg_id": _last_seen_cache or None,
        }).execute()

        _heartbeat_count += 1
        if _heartbeat_count % 60 == 0:
            cutoff = datetime.now(timezone.utc).timestamp() - 7 * 86400
            cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
            supabase.table("monitor_heartbeat").delete().lt("ts", cutoff_iso).execute()
    except Exception as e:
        log.error(f"Heartbeat failed: {e}")


# ─────────────────────────────────────────────
# HISTORY CHECK — startup backfill (20 messages)
# ─────────────────────────────────────────────
async def check_history(app: Client):
    log.info("Scanning recent channel history...")
    global _channel_reachable
    try:
        messages = []
        async for msg in app.get_chat_history(CHANNEL, limit=20):
            messages.append(msg)
        _channel_reachable = True

        found = 0
        max_id = 0

        for msg in messages:
            if not msg.text:
                if msg.id > max_id:
                    max_id = msg.id
                continue

            promos = extract_promos(msg.text)
            for promo in promos:
                existing = supabase.table("promo_codes").select("id").eq("code", promo["code"]).execute()
                if not existing.data:
                    log.info(f"Untracked historical code: \"{promo['code']}\"")
                    await handle_new_code(
                        promo["code"], promo, msg.text,
                        push=False, message_at=msg.date, source="history",
                    )
                    found += 1
                    await asyncio.sleep(0.5)
                else:
                    log.info(f"Historical \"{promo['code']}\" already tracked")

            if msg.id > max_id:
                max_id = msg.id

        if max_id > 0:
            set_last_seen(max_id)

        log.info(f"History scan complete — {found} new code(s), last_seen_msg_id = {max_id}")
    except Exception as e:
        log.error(f"History scan failed: {e}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
async def main():
    log.info("Starting promo code watcher (Pyrogram)...")

    app = Client(
        "monitor_session",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=SESSION,
    )

    # ── Event handler — real-time detection ──
    @app.on_message(filters.channel & filters.incoming)
    async def on_new_message(client: Client, message: Message):
        global _channel_reachable
        try:
            chat = await client.get_chat(message.chat.id)
            if chat.username and chat.username.lower() != CHANNEL.lower():
                return
        except Exception:
            return

        _channel_reachable = True

        if not message.text:
            return

        log.info(f"[EVENT] msg_id={message.id} chat={message.chat.id}")
        promos = extract_promos(message.text)
        for promo in promos:
            await handle_new_code(
                promo["code"], promo, message.text,
                push=True, message_at=message.date, source="event",
            )

        if message.id:
            set_last_seen(message.id)

    await app.start()

    who = await app.get_me()
    log.info(f"Connected as @{who.username or who.id}")

    # Verify channel reachability
    try:
        async for msg in app.get_chat_history(CHANNEL, limit=1):
            log.info(f"Channel @{CHANNEL} reachable (latest msg id: {msg.id})")
            _channel_reachable = True
    except Exception as e:
        log.error(f"Channel reachability check failed: {e}")

    # Recover cursor
    last_seen = get_last_seen()
    log.info(f"Resumed from last_seen_msg_id = {last_seen}")

    # Startup history scan
    await check_history(app)

    log.info("Event listener active — waiting for new messages")

    # ── HTTP server (aiohttp) — runs alongside Pyrogram ──
    async def handle_root(request):
        log.info("Trigger (cron keepalive)")
        # Poll: check for new messages since last seen
        try:
            async for msg in app.get_chat_history(CHANNEL, limit=1):
                if msg.id and msg.id > get_last_seen():
                    log.info(f"[POLL] New message detected (id={msg.id})")
                    if msg.text:
                        promos = extract_promos(msg.text)
                        for promo in promos:
                            await handle_new_code(
                                promo["code"], promo, msg.text,
                                push=True, message_at=msg.date, source="poll",
                            )
                    set_last_seen(msg.id)
        except Exception as e:
            log.error(f"[POLL] Error: {e}")

        return web.Response(text="Promo monitor (Pyrogram) ✅")

    async def handle_health(request):
        last_seen = get_last_seen()
        status = {
            "status": "ok" if _channel_reachable else "degraded",
            "channel": CHANNEL,
            "connected": True,
            "channel_reachable": _channel_reachable,
            "last_seen_msg_id": last_seen,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        return web.json_response(status)

    async def handle_status(request):
        return web.json_response({
            "status": "running",
            "channel": CHANNEL,
            "connected": True,
            "channel_reachable": _channel_reachable,
            "last_seen_msg_id": get_last_seen(),
            "instances": len(INSTANCE_URLS),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    app_http = web.Application()
    app_http.router.add_get("/", handle_root)
    app_http.router.add_get("/health", handle_health)
    app_http.router.add_get("/status", handle_status)

    runner = web.AppRunner(app_http)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info(f"HTTP server listening on :{PORT}")

    # ── Background tasks ──

    # Heartbeat every 60s
    async def heartbeat_loop():
        while True:
            await asyncio.sleep(60)
            await asyncio.get_event_loop().run_in_executor(None, write_heartbeat)

    # Keepalive — call a lightweight RPC every 30s to keep Telegram pushing updates
    async def keepalive_loop():
        count = 0
        while True:
            await asyncio.sleep(30)
            try:
                before = asyncio.get_event_loop().time()
                # get_dialogs is a cheap RPC that resets Telegram's update push timer
                async for _ in app.get_dialogs(limit=1):
                    pass
                rtt = int((asyncio.get_event_loop().time() - before) * 1000)
                count += 1
                log.info(f"[KEEPALIVE] #{count} ok — {rtt}ms RTT")
            except Exception as e:
                log.error(f"[KEEPALIVE] FAILED: {e}")

    asyncio.create_task(heartbeat_loop())
    asyncio.create_task(keepalive_loop())
    write_heartbeat()  # immediate first heartbeat
    log.info("💓 Heartbeat every 60s + keepalive every 30s")

    # ── Startup summary ──
    log.info("── Startup summary ──")
    log.info(f"  Channel: @{CHANNEL}")
    log.info(f"  Last seen msg: {last_seen}")
    log.info(f"  Instances: {len(INSTANCE_URLS)}")
    log.info(f"  HTTP: every GET / hit (cron keepalive)")
    log.info(f"  Keepalive: every 30s")
    log.info(f"  Heartbeat: every 60s")
    log.info("────────────────────")

    # Block until disconnect
    await idle()

    # Cleanup
    await runner.cleanup()
    await app.stop()
    log.info("Shutting down...")


if __name__ == "__main__":
    asyncio.run(main())
