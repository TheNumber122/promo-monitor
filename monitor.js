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

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY,
);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ============================================
// PUSH — wake every instance to redeem NOW
// Promos expire in ~1–2 min, so we can't wait for the 5-min
// UptimeRobot sweep. On a genuinely new code we ping each
// instance's /promo endpoint immediately (best-effort).
// ============================================
async function pushToInstances(code) {
  if (!INSTANCE_URLS.length) {
    console.log("[MONITOR] [PUSH] No INSTANCE_URLS configured — skipping push");
    return;
  }

  await Promise.allSettled(
    INSTANCE_URLS.map(async (base) => {
      const url = `${base.replace(/\/$/, "")}/promo`;
      const controller = new AbortController();
      const t = setTimeout(() => controller.abort(), 8000);
      try {
        const res = await fetch(url, { signal: controller.signal });
        console.log(`[MONITOR] [PUSH] ${url} → ${res.status}`);
      } catch (e) {
        console.log(`[MONITOR] [PUSH] ${url} failed: ${e.message}`);
      } finally {
        clearTimeout(t);
      }
    }),
  );
  console.log(`[MONITOR] [PUSH] Notified ${INSTANCE_URLS.length} instance(s) for "${code}"`);
}

// ============================================
// CLIENT — module scope so poll/reconnect can access
// ============================================
let client;
let lastPollAt = 0;
const POLL_DEBOUNCE_MS = 5000;

// ============================================
// PROMO EXTRACTION
// ============================================
function extractPromos(text) {
  const results = [];
  const lines   = text.split("\n").map((l) => l.trim());

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!line.includes("Ловите дейли промо")) continue;

    const starsMatch       = line.match(/на\s+(\d+)\s*⭐/);
    const activationsMatch = line.match(/(\d+)\s+активаций/);

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
async function handleNewCode(code, meta, rawMessage, push = true) {
  console.log(
    `\n[MONITOR] 🎟️  Code: "${code}"  |  ${meta.stars_amount ?? "?"}⭐  |  ${meta.max_activations ?? "?"} activations`,
  );

  const { data, error } = await supabase
    .from("promo_codes")
    .insert({
      code,
      raw_message:     rawMessage.substring(0, 500),
      stars_amount:    meta.stars_amount,
      max_activations: meta.max_activations,
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
    if (push) {
      console.log(`[MONITOR] Saved to DB (id=${data.id}) — pushing to instances`);
      // Fire-and-forget: wake instances immediately so they redeem before expiry.
      pushToInstances(code).catch((e) =>
        console.error(`[MONITOR] [PUSH] Error: ${e.message}`),
      );
    } else {
      console.log(`[MONITOR] Saved to DB (id=${data.id}) — startup backfill, not pushing`);
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
// ============================================
function registerEventHandler() {
  client.addEventHandler(
    async (event) => {
      try {
        const msg = event.message;
        if (!msg?.text) return;

        const promos = extractPromos(msg.text);
        for (const promo of promos) {
          await handleNewCode(promo.code, promo, msg.text);
        }

        if (msg.id) await setLastSeenMsgId(msg.id);
      } catch (e) {
        console.error(`[MONITOR] Handler error: ${e.message}`);
      }
    },
    new NewMessage({ chats: [CHANNEL] }),
  );
}

// ============================================
// POLL CHANNEL — backup detection on every / hit
// ============================================
async function pollChannel() {
  const now = Date.now();
  if (now - lastPollAt < POLL_DEBOUNCE_MS) return;
  lastPollAt = now;

  try {
    if (!(await ensureConnected())) {
      console.error("[MONITOR] [POLL] Cannot poll — connection unavailable");
      return;
    }

    const lastSeen = await getLastSeenMsgId();

    const messages = await client.getMessages(CHANNEL, { limit: 20 });

    const newMsgs = messages.filter((m) => m && m.id > lastSeen);

    if (!newMsgs.length) {
      console.log(`[MONITOR] [POLL] No new messages (last_seen=${lastSeen}, checked ${messages.length})`);
      return;
    }

    console.log(`[MONITOR] [POLL] ${newMsgs.length} new message(s) since last seen (id=${lastSeen})`);

    let maxId = lastSeen;
    for (const msg of newMsgs) {
      if (!msg.text) {
        if (msg.id > maxId) maxId = msg.id;
        continue;
      }

      const promos = extractPromos(msg.text);
      for (const promo of promos) {
        await handleNewCode(promo.code, promo, msg.text);
      }

      if (msg.id > maxId) maxId = msg.id;
    }

    await setLastSeenMsgId(maxId);
    console.log(`[MONITOR] [POLL] Updated last_seen_msg_id → ${maxId}`);
  } catch (e) {
    console.error(`[MONITOR] [POLL] Error: ${e.message}`);
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
          await handleNewCode(promo.code, promo, msg.text, false);
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

  // Register real-time event handler (primary detection)
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

  // Interval polling — every 30s as safety net
  setInterval(() => {
    pollChannel().catch((e) => console.error(`[MONITOR] [INTERVAL] Error: ${e.message}`));
  }, 30_000);

  console.log(`[MONITOR] ⏱️  Interval polling every 30s`);
  console.log(`[MONITOR] 🌐 HTTP polling on every GET / hit`);

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
