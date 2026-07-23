require("dotenv").config();
const { TelegramClient } = require("telegram");
const { StringSession } = require("telegram/sessions");
const { NewMessage } = require("telegram/events");
const { createClient } = require("@supabase/supabase-js");
const express = require("express");

// ============================================
// CONFIG
// ============================================
const API_ID     = parseInt(process.env.API_ID);
const API_HASH   = process.env.API_HASH;
const SESSION    = process.env.MONITOR_SESSION;
const PORT       = process.env.PORT || 10001;
const CHANNEL    = "patrickstarsfarm";

// Instance base URLs to push new codes to (comma-separated).
// e.g. INSTANCE_URLS=https://inst1.onrender.com,https://inst2.onrender.com
const INSTANCE_URLS = (process.env.INSTANCE_URLS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// Prefer the service_role key so writes (heartbeat/promo inserts) bypass RLS.
// Falls back to the anon key when service_role isn't set. A silent RLS denial on
// monitor_heartbeat is what makes the dashboard show ONLINE (live /health) while
// the uptime bar goes red (no heartbeat rows) — see writeHeartbeat().
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY,
);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ============================================
// PUSH — wake every instance to redeem NOW
// Promos expire in ~1–2 min, so we can't wait for the 5-min
// UptimeRobot sweep. On a genuinely new code we ping each
// instance's /promo endpoint immediately (best-effort).
// ============================================
async function pushToInstances(code, codeId) {
  if (!INSTANCE_URLS.length) {
    console.log("[MONITOR] [PUSH] No INSTANCE_URLS configured — skipping push");
    return;
  }

  const results = await Promise.allSettled(
    INSTANCE_URLS.map(async (base) => {
      const url = `${base.replace(/\/$/, "")}/promo`;
      const controller = new AbortController();
      const t = setTimeout(() => controller.abort(), 8000);
      try {
        const res = await fetch(url, { signal: controller.signal });
        console.log(`[MONITOR] [PUSH] ${url} → ${res.status}`);
        return res.ok;
      } catch (e) {
        console.log(`[MONITOR] [PUSH] ${url} failed: ${e.message}`);
        return false;
      } finally {
        clearTimeout(t);
      }
    }),
  );

  const ok = results.filter((r) => r.status === "fulfilled" && r.value).length;
  console.log(`[MONITOR] [PUSH] Notified ${INSTANCE_URLS.length} instance(s) for "${code}" — ${ok} answered OK`);

  // Record fan-out outcome on the code row (best-effort — never blocks push).
  if (codeId) {
    try {
      await supabase
        .from("promo_codes")
        .update({ pushed_count: INSTANCE_URLS.length, push_ok_count: ok })
        .eq("id", codeId);
    } catch (e) {
      console.error(`[MONITOR] [PUSH] Outcome record failed: ${e.message}`);
    }
  }
}

// ============================================
// CLIENT — module scope so poll/reconnect can access
// ============================================
let client;
let lastPollAt = 0;
const POLL_DEBOUNCE_MS = 200;

// Resolved numeric id of @patrickstarsfarm. The event handler matches on THIS,
// never on the username: GramJS's `chats` filter resolves usernames lazily and
// silently drops every event if that resolution fails (events/common.js
// filter() returns nothing while !resolved) — which is why detections were
// always src=poll. A channel's numeric id never changes.
let channelId = null;

// FLOOD_WAIT backoff for the fast poll (500ms cadence needs a brake, just in case)
let pollBackoffUntil = 0;

// Lightweight state the 60s heartbeat samples into monitor_heartbeat (the
// dashboard reads it as uptime). Updated opportunistically by existing flows —
// no extra Telegram RPCs are made just for the heartbeat.
let lastSeenMsgIdCache = 0;
let channelReachable   = false;
let heartbeatCount     = 0;

// ============================================
// PROMO EXTRACTION
// ============================================
function extractPromos(text) {
  const results = [];
  const lines   = text.split("\n").map((l) => l.trim());

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    // Trigger phrase varies: "промо", "промик", "промокод", occasional typos.
    // Match the stable stem "Ловите дейли про" so diminutives don't slip past.
    if (!/Ловите\s+дейли\s+про/i.test(line)) continue;

    // Stars emoji varies across posts: ⭐ / ⭐️ / 🌟, and "и" may hug the emoji
    // with no space ("2 🌟и 1200"). Anchor on the number after "на" instead.
    const starsMatch       = line.match(/на\s+(\d+)\s*(?:⭐️?|🌟|звёзд|star)/i);
    const activationsMatch = line.match(/(\d+)\s+активаци/);

    let codeLine = "";
    const colonPos = line.lastIndexOf(":");
    if (colonPos !== -1) {
      const afterColon = line.slice(colonPos + 1).trim();
      if (afterColon) {
        codeLine = afterColon;
      } else {
        for (let j = i + 1; j < lines.length; j++) {
          if (lines[j]) { codeLine = lines[j]; break; }
        }
      }
    }

    if (!codeLine) continue;

    const code = codeLine
      .replace(/\|\|/g, "")
      .replace(/\*\*/g, "")
      .replace(/\*/g, "")
      .replace(/_/g, "")
      .trim();

    if (
      !code ||
      code.includes("http") ||
      code.includes("t.me") ||
      code.includes("telegram.me") ||
      code.length < 2 ||
      code.length > 60
    ) continue;

    results.push({
      code,
      stars_amount:    starsMatch       ? parseInt(starsMatch[1])       : null,
      max_activations: activationsMatch ? parseInt(activationsMatch[1]) : null,
    });
  }

  return results;
}

// ============================================
// HANDLE NEW CODE
// ============================================
async function handleNewCode(code, meta, rawMessage, push = true, messageAt = null, source = "event") {
  const detectedAt = Date.now();
  const msgTs = messageAt ? new Date(messageAt).getTime() : null;
  const reactionLag = msgTs ? `${((detectedAt - msgTs) / 1000).toFixed(1)}s since posted` : "unknown lag";

  console.log(
    `\n[MONITOR] 🎟️  Code: "${code}"  |  ${meta.stars_amount ?? "?"}⭐  |  ${meta.max_activations ?? "?"} activations  |  ${source}  |  ${reactionLag}`,
  );

  const { data, error } = await supabase
    .from("promo_codes")
    .insert({
      code,
      raw_message:     rawMessage.substring(0, 500),
      stars_amount:    meta.stars_amount,
      max_activations: meta.max_activations,
      message_at:      messageAt, // channel timestamp → detected_at − message_at = reaction lag
      source,                     // 'event' (real-time) | 'poll' (backup) | 'history' (backfill)
    })
    .select("id")
    .single();

  if (error) {
    if (error.code === "23505") {
      console.log(`[MONITOR] "${code}" already in DB — skipping`);
      return;
    }
    console.error(`[MONITOR] DB insert error: ${error.message}`);
  } else {
    const savedMs = Date.now() - detectedAt;
    if (push) {
      console.log(`[MONITOR] Saved to DB (id=${data.id}) in ${savedMs}ms — pushing to instances`);
      // Fire-and-forget: wake instances immediately so they redeem before expiry.
      pushToInstances(code, data.id).catch((e) =>
        console.error(`[MONITOR] [PUSH] Error: ${e.message}`),
      );
    } else {
      console.log(`[MONITOR] Saved to DB (id=${data.id}) in ${savedMs}ms — startup backfill, not pushing`);
    }
  }
}

// ============================================
// MONITOR STATE — last_seen_msg_id
// ============================================
async function getLastSeenMsgId() {
  try {
    const { data, error } = await supabase
      .from("monitor_state")
      .select("value")
      .eq("key", "last_seen_msg_id")
      .maybeSingle();

    if (error || !data) return 0;
    return parseInt(data.value?.msg_id) || 0;
  } catch {
    return 0;
  }
}

async function setLastSeenMsgId(msgId) {
  lastSeenMsgIdCache = msgId;
  try {
    await supabase
      .from("monitor_state")
      .upsert(
        { key: "last_seen_msg_id", value: { msg_id: msgId }, updated_at: new Date().toISOString() },
        { onConflict: "key" },
      );
  } catch (e) {
    console.error(`[MONITOR] Failed to update last_seen_msg_id: ${e.message}`);
  }
}

// ============================================
// CONNECTION HEALTH — auto-reconnect
// ============================================
async function ensureConnected() {
  if (client && client.connected) return true;

  console.log("[MONITOR] Connection lost — reconnecting...");
  try {
    if (client) {
      try { await client.disconnect(); } catch (_) {}
    }
    client = new TelegramClient(
      new StringSession(SESSION),
      API_ID,
      API_HASH,
      { connectionRetries: 10, autoReconnect: true, receiveUpdates: true },
    );
    await client.connect();
    await joinChannel(client);
    await resolveChannelId();
    registerEventHandler();
    console.log("[MONITOR] ✅ Reconnected");
    return true;
  } catch (e) {
    console.error(`[MONITOR] Reconnect failed: ${e.message}`);
    return false;
  }
}

// ============================================
// JOIN CHANNEL
// ============================================
async function joinChannel(c) {
  try {
    await c.invoke(
      // eslint-disable-next-line new-cap
      new (require("telegram").Api.channels.JoinChannel)({ channel: CHANNEL }),
    );
    console.log(`[MONITOR] Joined @${CHANNEL}`);
  } catch (e) {
    if (!e.message?.includes("USER_ALREADY_PARTICIPANT")) {
      console.log(`[MONITOR] Join note: ${e.message}`);
    }
  }
}

// ============================================
// EVENT HANDLER — primary real-time detection
// Registered WITHOUT a `chats` filter: GramJS's chats-filter drops all events
// silently if its lazy username resolution fails. We match the resolved
// numeric channelId ourselves — reliable and visible.
// ============================================
let eventHandlerRef = null;

function registerEventHandler() {
  // Never stack duplicate handlers across reconnects
  if (eventHandlerRef) {
    try { client.removeEventHandler(eventHandlerRef.cb, eventHandlerRef.ev); } catch (_) {}
    eventHandlerRef = null;
  }

  const cb = async (event) => {
    try {
      const msg = event.message;
      if (!msg?.text) return;

      // Match our channel by resolved numeric id (fall back to accepting all
      // if resolution failed — extractPromos is strict enough to not misfire).
      if (channelId && event.chatId && event.chatId.toString() !== channelId) return;

      console.log(`[MONITOR] [EVENT] msg_id=${msg.id} chat=${event.chatId}`);
      const msgAt = msg.date ? new Date(msg.date * 1000).toISOString() : null;
      const promos = extractPromos(msg.text);
      for (const promo of promos) {
        await handleNewCode(promo.code, promo, msg.text, true, msgAt, "event");
      }

      if (msg.id) await setLastSeenMsgId(msg.id);
    } catch (e) {
      console.error(`[MONITOR] Handler error: ${e.message}`);
    }
  };
  const ev = new NewMessage({});
  client.addEventHandler(cb, ev);
  eventHandlerRef = { cb, ev };
}

// Resolve the channel's numeric id once per connection (cheap, from cache).
async function resolveChannelId() {
  try {
    const entity = await client.getEntity(CHANNEL);
    channelId = entity.id.toString();
    console.log(`[MONITOR] Resolved @${CHANNEL} → id ${channelId}`);
  } catch (e) {
    console.error(`[MONITOR] ⚠️  Could not resolve @${CHANNEL} id: ${e.message} — handler will accept all chats`);
    channelId = null;
  }
}

// ============================================
// POLL CHANNEL — backup detection on every / hit
// ============================================
async function pollChannel() {
  const now = Date.now();
  if (now - lastPollAt < POLL_DEBOUNCE_MS) return;
  if (now < pollBackoffUntil) return; // FLOOD_WAIT brake
  lastPollAt = now;

  try {
    if (!(await ensureConnected())) {
      console.error("[MONITOR] [POLL] Cannot poll — connection unavailable");
      return;
    }

    const lastSeen = lastSeenMsgIdCache || (await getLastSeenMsgId());

    // limit 5, not 20 — this runs every 5s; the channel never posts 5 messages
    // in one window, and the startup history scan covers deeper backfill.
    const messages = await client.getMessages(CHANNEL, { limit: 5 });
    channelReachable = true;

    const newMsgs = messages.filter((m) => m && m.id > lastSeen);

    if (!newMsgs.length) return; // silent — logging "no new" every 5s is noise

    console.log(`[MONITOR] [POLL] ${newMsgs.length} new message(s) since last seen (id=${lastSeen})`);

    let maxId = lastSeen;
    for (const msg of newMsgs) {
      if (!msg.text) {
        if (msg.id > maxId) maxId = msg.id;
        continue;
      }

      const msgAt = msg.date ? new Date(msg.date * 1000).toISOString() : null;
      const promos = extractPromos(msg.text);
      for (const promo of promos) {
        await handleNewCode(promo.code, promo, msg.text, true, msgAt, "poll");
      }

      if (msg.id > maxId) maxId = msg.id;
    }

    await setLastSeenMsgId(maxId);
    console.log(`[MONITOR] [POLL] Updated last_seen_msg_id → ${maxId}`);
  } catch (e) {
    channelReachable = false;
    // Respect FLOOD_WAIT_N: back off exactly as long as Telegram asks
    const flood = (e.message || "").match(/FLOOD_WAIT_(\d+)/);
    if (flood) {
      pollBackoffUntil = Date.now() + (parseInt(flood[1]) + 1) * 1000;
      console.error(`[MONITOR] [POLL] FLOOD_WAIT — backing off ${flood[1]}s`);
    } else {
      console.error(`[MONITOR] [POLL] Error: ${e.message}`);
    }
  }
}

// ============================================
// STARTUP HISTORY CHECK
// ============================================
async function checkHistory() {
  console.log("[MONITOR] Scanning recent channel history...");
  try {
    const messages = await client.getMessages(CHANNEL, { limit: 20 });
    let found = 0;
    let maxId = 0;

    for (const msg of messages) {
      if (!msg?.text) {
        if (msg?.id > maxId) maxId = msg.id;
        continue;
      }

      const promos = extractPromos(msg.text);

      for (const promo of promos) {
        const { data } = await supabase
          .from("promo_codes")
          .select("id")
          .eq("code", promo.code)
          .maybeSingle();

        if (!data) {
          console.log(`[MONITOR] Untracked historical code: "${promo.code}"`);
          const msgAt = msg.date ? new Date(msg.date * 1000).toISOString() : null;
          await handleNewCode(promo.code, promo, msg.text, false, msgAt, "history");
          found++;
          await sleep(500);
        } else {
          console.log(`[MONITOR] Historical "${promo.code}" already tracked`);
        }
      }

      if (msg.id > maxId) maxId = msg.id;
    }

    if (maxId > 0) await setLastSeenMsgId(maxId);

    console.log(`[MONITOR] History scan complete — ${found} new code(s), last_seen_msg_id = ${maxId}`);
  } catch (e) {
    console.error(`[MONITOR] History scan failed: ${e.message}`);
  }
}

// ============================================
// HEARTBEAT — liveness sample every 60s (dashboard reads it as uptime).
// Downtime is inferred from gaps in this series. Best-effort: a failed write
// can never crash the monitor or affect promo detection.
// ============================================
async function writeHeartbeat() {
  try {
    await supabase.from("monitor_heartbeat").insert({
      connected:        client?.connected ?? false,
      channel_ok:       channelReachable,
      last_seen_msg_id: lastSeenMsgIdCache || null,
    });

    // Prune ~hourly (every 60 beats) — keep a rolling 7 days.
    if (++heartbeatCount % 60 === 0) {
      const cutoff = new Date(Date.now() - 7 * 86400_000).toISOString();
      await supabase.from("monitor_heartbeat").delete().lt("ts", cutoff);
    }
  } catch (e) {
    console.error(`[MONITOR] Heartbeat failed: ${e.message}`);
  }
}

// ============================================
// MAIN
// ============================================
async function main() {
  console.log("[MONITOR] Starting promo code watcher...");
  console.log("[MONITOR] Mode: event listener + HTTP/interval polling (bulletproof)");

  client = new TelegramClient(
    new StringSession(SESSION),
    API_ID,
    API_HASH,
    { connectionRetries: 10, autoReconnect: true, receiveUpdates: true },
  );

  await client.connect();
  const who = await client.getMe();
  console.log(`[MONITOR] ✅ Connected as @${who.username || who.id}`);

  await joinChannel(client);

  // Verify channel reachability
  try {
    const probe = await client.getMessages(CHANNEL, { limit: 1 });
    console.log(`[MONITOR] ✅ Channel @${CHANNEL} reachable (latest msg id: ${probe[0]?.id ?? "none"})`);
  } catch (e) {
    console.error(`[MONITOR] ⚠️  Channel reachability check failed: ${e.message}`);
  }

  // Recover last_seen_msg_id before history scan
  const lastSeen = await getLastSeenMsgId();
  console.log(`[MONITOR] Resumed from last_seen_msg_id = ${lastSeen}`);

  // Catch-up scan on startup (20 messages)
  await checkHistory();

  // Resolve channel numeric id, then register real-time event handler
  await resolveChannelId();
  registerEventHandler();
  console.log(`[MONITOR] 👂 Event listener active on @${CHANNEL}`);

  // Express server
  const app = express();

  // GET / — cron hits every 1 min → poll channel + keep-alive + log status
  app.get("/", (req, res) => {
    const ts = new Date().toLocaleString();
    const isConn = client?.connected ?? false;
    console.log(`\n[MONITOR] 🔄 Trigger @ ${ts}  |  connected: ${isConn}`);

    pollChannel().catch((e) => console.error(`[MONITOR] [POLL] Error: ${e.message}`));
    res.send("Promo monitor ✅");
  });

  // GET /health — UptimeRobot hits every 5 min → full health check
  app.get("/health", async (_, res) => {
    const ts = new Date().toLocaleString();
    const isConn = client?.connected ?? false;
    const currentLastSeen = await getLastSeenMsgId();

    // Try a lightweight channel reachability check
    let channelReachable = false;
    let channelError = null;
    if (isConn) {
      try {
        await client.getMessages(CHANNEL, { limit: 1 });
        channelReachable = true;
      } catch (e) {
        channelError = e.message;
      }
    }

    const status = {
      status: channelReachable ? "ok" : "degraded",
      channel: CHANNEL,
      connected: isConn,
      channel_reachable: channelReachable,
      channel_error: channelError,
      last_seen_msg_id: currentLastSeen,
      timestamp: ts,
    };

    console.log(`[MONITOR] 🩺 Health @ ${ts}  |  connected: ${isConn}  |  channel_reachable: ${channelReachable}${channelError ? `  |  error: ${channelError}` : ""}`);

    res.json(status);
  });

  app.listen(PORT, () => console.log(`[MONITOR] Keep-alive on :${PORT}`));

  // Interval polling — every 500ms. This is the WORKHORSE detection path:
  // the event layer silently starves on idle sessions (see registerEventHandler),
  // so worst-case detection must come from here. 500ms cadence + limit 5 is one
  // tiny getMessages call — negligible on CPU, FLOOD-guarded above. Promos expire
  // in ~1-2 min so every second of detection lag is lost activations.
  setInterval(() => {
    pollChannel().catch((e) => console.error(`[MONITOR] [INTERVAL] Error: ${e.message}`));
  }, 500);

  console.log(`[MONITOR] ⏱️  Interval polling every 500ms`);
  console.log(`[MONITOR] 🌐 HTTP polling on every GET / hit`);

  // Heartbeat — liveness sample every 60s for the dashboard uptime view.
  writeHeartbeat();
  setInterval(() => {
    writeHeartbeat().catch((e) => console.error(`[MONITOR] Heartbeat error: ${e.message}`));
  }, 60_000);
  console.log(`[MONITOR] 💓 Heartbeat every 60s`);

  // Graceful shutdown
  process.on("SIGINT", async () => {
    console.log("[MONITOR] Shutting down...");
    await client.disconnect();
    process.exit(0);
  });

  // Prevent crashes from unhandled Telegram RPC errors
  process.on("unhandledRejection", (e) => {
    console.error(`[MONITOR] Unhandled rejection: ${e?.message || e}`);
  });
}

main().catch((e) => {
  console.error("[MONITOR] Fatal:", e.message);
  process.exit(1);
});
