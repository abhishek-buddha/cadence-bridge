import express from "express";
import http from "http";
import { WebSocketServer, WebSocket } from "ws";
import { URL } from "url";

// ---------------------------------------------------------------------------
// Process-level safety net.
//
// This process relays MANY concurrent, independent calls (one per Twilio
// media-stream connection). Node's default behavior for an uncaught exception
// or unhandled promise rejection is to crash the ENTIRE process — which would
// kill every other in-progress call, not just the one that hit a bug. Each
// call's state lives in its own Map entries, so surviving one call's bug and
// continuing to serve the rest is the right tradeoff here. Log loudly instead
// of exiting; per-connection try/catch (see elevenLabsWs.on("message")) should
// catch most of these before they ever reach here — this is the last resort.
// ---------------------------------------------------------------------------
process.on("uncaughtException", (err) => {
  console.error("[fatal] Uncaught exception (process kept alive):", err && err.stack ? err.stack : err);
});
process.on("unhandledRejection", (reason) => {
  console.error("[fatal] Unhandled promise rejection (process kept alive):", reason);
});

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------
const PORT = parseInt(process.env.PORT || "3001", 10);
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY;
const ELEVENLABS_AGENT_ID = process.env.ELEVENLABS_AGENT_ID;
const CONVEX_SITE_URL = process.env.CONVEX_SITE_URL; // e.g. https://groovy-wren-932.convex.site
const HANDOFF_ON_TRANSFER_CUE = process.env.HANDOFF_ON_TRANSFER_CUE !== "false";
const DETACH_ELEVENLABS_ON_HANDOFF = process.env.DETACH_ELEVENLABS_ON_HANDOFF !== "false";
const HOLD_CUE_HANDOFF_DELAY_MS = parseInt(process.env.HOLD_CUE_HANDOFF_DELAY_MS || "8000", 10);

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

// callId -> { twilioWs, elevenLabsWs, streamSid }
const activeCalls = new Map();

// callId -> Set<WebSocket>  (browser listeners)
const browserListeners = new Map();

// callId -> Set<WebSocket>  (monitor sources — Twilio unidirectional streams)
const monitorStreams = new Map();

// callId -> true once we've fired the human-handoff broadcast for this call
// (one-shot guard so we don't re-notify on every subsequent IVR line).
const handoffFired = new Map();

// callId -> true after the IVR says it is transferring/holding for a rep.
// Once armed, the next non-IVR human-like utterance triggers handoff.
const handoffArmed = new Map();

// callId -> timeout handle for delayed hold/music handoff.
const holdCueTimers = new Map();

// callId -> insuranceContacts.callConnectionType for this call, populated from
// /call-metadata's dynamic_variables.call_connection_type. The automatic,
// audio-pattern-based handoff detection below (handoffSignalFromText,
// scheduleHoldCueHandoff) was built only for the original "ivr_human_handoff"
// flow (Option 1: Cadence owns the call, bridge detects a human and hands off
// to a Cadence operator). For "ivr_only_cut_at_handoff" and "direct_to_agent"
// payers, that behavior is wrong — it silently mutes the AI and requests an
// operator handoff nobody is there to accept, dropping the call. Those two
// types are driven entirely by the LLM's own prompt instructions instead (it
// calls end_call itself, or just keeps talking), so the bridge's own
// detection must stay out of the way entirely.
const callConnectionTypes = new Map();

// Whether the bridge's own audio-based auto-handoff detection should run for
// this call. Defaults to enabled (true) when the type is unset/unknown —
// matches every payer's behavior before callConnectionType existed.
function autoHandoffEnabled(callId) {
  const type = callId ? callConnectionTypes.get(callId) : undefined;
  return !type || type === "ivr_human_handoff";
}

// All WebSocket connections for counting
let totalWsConnections = 0;

// ---------------------------------------------------------------------------
// Human-handoff detection (Option 1: Cadence owns the call).
//
// The bridge must fire only after a real insurance representative speaks.
// Queue/hold/transfer prompts are still IVR audio; broadcasting on those lines
// offers the call before any human has answered.
// ---------------------------------------------------------------------------
const NON_HUMAN_HANDOFF_PHRASES = [
  "please hold",
  "transferring you",
  "transfer you",
  "connecting you",
  "next available representative",
  "next available agent",
  "next available claims",
  "one moment while",
  "let me get someone",
  "let me connect you",
  "connect you to a representative",
  "connect you with a representative",
  "connecting you to the next",
  "hold while we transfer",
  "please stay on the line",
  "your call is important",
  "estimated wait time",
  "wait time",
  "hold music",
  "at the tone",
  "record your message",
  "continued patience",
  "unable to reach",
  "try again later",
  "voicemail",
];

const IVR_MENU_PHRASES = [
  "press ",
  "press one",
  "press two",
  "press three",
  "press four",
  "say claims",
  "say eligibility",
  "say member services",
  "to repeat this menu",
  "for claims",
  "for eligibility",
  "for member services",
  "for provider relations",
  "to file a new claim",
  "to speak with",
];

const LIVE_HUMAN_PATTERNS = [
  /\bhi\b.*\bhello\b/i,
  /\bhello\b/i,
  /\brecord your name\b/i,
  /\breason for calling\b/i,
  /\bthis is [a-z][a-z .'-]{1,40}\b/i,
  /\bhow (can|may) i help\b/i,
  /\bhow can i assist\b/i,
  /\bwho am i speaking with\b/i,
  /\bmay i (have|get|verify)\b/i,
  /\bcan i (have|get|verify)\b/i,
  /\bclaim(s)? department\b.*\b(help|assist|speaking)\b/i,
  /\brepresentative\b.*\b(help|assist|speaking)\b/i,
  /\bthanks? (so much )?for holding\b/i,
];
function isTransferOrHoldCue(t) {
  return [
    "please hold",
    "hold music",
    "transferring you",
    "transfer you",
    "connecting you",
    "next available representative",
    "next available agent",
    "estimated wait time",
    "wait time",
    "hold while we transfer",
    "please stay on the line",
    "your call is important",
    "continued patience",
  ].some((p) => t.includes(p));
}

function isRepresentativeTransferCue(t) {
  return (
    [
      "transferring you",
      "transfer you",
      "connecting you",
      "next available representative",
      "next available agent",
      "next available claims",
      "let me get someone",
      "let me connect you",
      "connect you to a representative",
      "connect you with a representative",
      "connecting you to the next",
      "hold while we transfer",
    ].some((p) => t.includes(p)) ||
    /\b(connect|connecting|transfer|transferring)\b.*\b(representative|agent|specialist|claims department|operator)\b/i.test(t) ||
    /\b(representative|agent|specialist|operator)\b.*\bwill be with you\b/i.test(t)
  );
}

function isSilenceTranscript(t) {
  return !t || t === "..." || t === "." || t === "[silence]";
}

function isActionableIvrPrompt(t) {
  return IVR_MENU_PHRASES.some((p) => t.includes(p));
}

function handoffSignalFromText(callId, text) {
  const raw = (text || "").trim();
  const t = raw.toLowerCase();
  if (isSilenceTranscript(t)) return null;

  if (isTransferOrHoldCue(t)) {
    if (callId) handoffArmed.set(callId, true);
    return HANDOFF_ON_TRANSFER_CUE && isRepresentativeTransferCue(t)
      ? {
          reason: "ivr_transfer_hold_detected",
          detachElevenLabs: true,
        }
      : null;
  }

  if (NON_HUMAN_HANDOFF_PHRASES.some((p) => t.includes(p))) return null;
  if (isActionableIvrPrompt(t)) return null;

  if (LIVE_HUMAN_PATTERNS.some((p) => p.test(raw))) {
    return {
      reason: "ivr_human_handoff_detected",
      detachElevenLabs: true,
    };
  }

  // After the IVR has announced a transfer, the first remaining non-IVR speech
  // is treated as the answered party. This catches call-screening greetings and
  // short human openings before the AI starts the claim conversation.
  if (callId && handoffArmed.get(callId)) {
    if (/\b(hi|hello|yes|yeah|speaking|available)\b/i.test(raw)) {
      return {
        reason: "ivr_human_handoff_detected",
        detachElevenLabs: true,
      };
    }
  }

  return null;
}
async function fireHandoff(callId, convexSiteUrl, reasonText) {
  if (!callId || !convexSiteUrl) return false;
  if (handoffFired.get(callId)) return true; // one-shot
  handoffFired.set(callId, true);
  const reason = reasonText || "ivr_human_handoff_detected";
  const url = `${convexSiteUrl}/twilio-request-handoff?callId=${encodeURIComponent(
    callId
  )}&reason=${encodeURIComponent(reason)}`;
  console.log(`[handoff] Detected handoff signal for callId=${callId} reason=${reason} — firing ${url}`);
  try {
    const res = await fetch(url, { method: "POST" });
    console.log(`[handoff] /twilio-request-handoff → ${res.status}`);
    if (!res.ok) {
      throw new Error(`/twilio-request-handoff returned ${res.status}`);
    }
    return true;
  } catch (err) {
    console.error(`[handoff] Failed to fire handoff:`, err.message);
    handoffFired.delete(callId);
    handoffArmed.delete(callId); // allow a retry on the next matching line
    return false;
  }
}

function detachElevenLabsForHandoff(callId, elevenLabsWs, cause) {
  if (!DETACH_ELEVENLABS_ON_HANDOFF) return;
  if (!callId || !elevenLabsWs || elevenLabsWs.readyState !== WebSocket.OPEN) return;
  console.log(
    `[handoff] Detaching ElevenLabs for callId=${callId}; Twilio payer leg stays open (${cause})`
  );
  elevenLabsWs.close(1000, cause || "handoff_detach");
}

function clearHoldCueTimer(callId) {
  const timer = holdCueTimers.get(callId);
  if (timer) {
    clearTimeout(timer);
    holdCueTimers.delete(callId);
  }
}

function scheduleHoldCueHandoff(callId, elevenLabsWs, reason) {
  if (!HANDOFF_ON_TRANSFER_CUE) return;
  if (!callId || handoffFired.get(callId) || holdCueTimers.has(callId)) return;
  const delayMs = Number.isFinite(HOLD_CUE_HANDOFF_DELAY_MS)
    ? Math.max(0, HOLD_CUE_HANDOFF_DELAY_MS)
    : 8000;
  console.log(`[handoff] Hold cue armed for callId=${callId}; scheduling guard in ${delayMs}ms`);
  const timer = setTimeout(async () => {
    holdCueTimers.delete(callId);
    if (!handoffArmed.get(callId) || handoffFired.get(callId)) return;
    const handoffStarted = await fireHandoff(callId, CONVEX_SITE_URL, reason);
    if (handoffStarted) {
      detachElevenLabsForHandoff(callId, elevenLabsWs, reason);
    }
  }, delayMs);
  holdCueTimers.set(callId, timer);
}

function isHumanHandoffActive(callId) {
  return !!callId && handoffFired.get(callId) === true;
}

function isPotentialHumanHandoff(callId) {
  return !!callId && (handoffFired.get(callId) === true || handoffArmed.get(callId) === true);
}

async function preserveTwilioForPossibleHandoff(callId, cause) {
  if (!callId || !isPotentialHumanHandoff(callId)) return false;
  if (!handoffFired.get(callId)) {
    console.log(`[handoff] ${cause}; firing fallback web handoff for callId=${callId}`);
    await fireHandoff(callId, CONVEX_SITE_URL, cause);
  }
  return true;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Fetch call metadata from Convex HTTP endpoint.
 */
async function fetchCallMetadata(callId) {
  if (!CONVEX_SITE_URL) {
    console.warn("[metadata] CONVEX_SITE_URL not set, skipping metadata fetch");
    return {};
  }
  try {
    const url = `${CONVEX_SITE_URL}/call-metadata?callId=${encodeURIComponent(callId)}`;
    console.log(`[metadata] Fetching: ${url}`);
    const res = await fetch(url);
    if (!res.ok) {
      console.error(`[metadata] HTTP ${res.status}: ${await res.text()}`);
      return {};
    }
    const data = await res.json();
    console.log(`[metadata] Got metadata for callId=${callId}:`, JSON.stringify(data).slice(0, 200));
    return data;
  } catch (err) {
    console.error(`[metadata] Fetch failed:`, err.message);
    return {};
  }
}

/**
 * Get a signed URL from ElevenLabs for the conversational AI WebSocket.
 */
async function getElevenLabsSignedUrl() {
  const url = `https://api.elevenlabs.io/v1/convai/conversation/get_signed_url?agent_id=${ELEVENLABS_AGENT_ID}`;
  const res = await fetch(url, {
    method: "GET",
    headers: { "xi-api-key": ELEVENLABS_API_KEY },
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`ElevenLabs signed URL failed (${res.status}): ${body}`);
  }
  const data = await res.json();
  return data.signed_url;
}

/**
 * Forward audio chunk to all browser listeners for a given callId.
 */
let audioChunkCount = 0;
function forwardToListeners(callId, payload, track, meta = {}) {
  const listeners = browserListeners.get(callId);
  if (!listeners || listeners.size === 0) return;
  audioChunkCount++;
  if (audioChunkCount % 100 === 1) {
    console.log(`[audio] Forwarding chunk #${audioChunkCount} to ${listeners.size} listener(s) for callId=${callId} track=${track} codec=${meta.codec || "mulaw_8000"} source=${meta.source || "twilio"} payload_len=${payload?.length || 0}`);
  }
  const msg = JSON.stringify({
    event: "audio",
    media: { payload, track, ...meta },
  });
  for (const client of listeners) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  }
}

/**
 * Broadcast any JSON message to all browser listeners for a given callId.
 * Used for transcript events, phase changes, etc.
 */
function broadcastToListeners(callId, message) {
  const listeners = browserListeners.get(callId);
  if (!listeners || listeners.size === 0) return;
  const msg = JSON.stringify(message);
  for (const client of listeners) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  }
}

/**
 * Clean up all resources for a given callId.
 */
function cleanupCall(callId) {
  clearHoldCueTimer(callId);
  const call = activeCalls.get(callId);
  if (call) {
    if (call.twilioWs && call.twilioWs.readyState === WebSocket.OPEN) {
      call.twilioWs.close();
    }
    if (call.elevenLabsWs && call.elevenLabsWs.readyState === WebSocket.OPEN) {
      call.elevenLabsWs.close();
    }
    // Read the handoff flag BEFORE clearing it.
    const wasHandoff = handoffFired.get(callId) ? true : false;
    activeCalls.delete(callId);
    handoffFired.delete(callId);
    handoffArmed.delete(callId);
    callConnectionTypes.delete(callId);
    console.log(`[cleanup] Call ${callId} cleaned up (wasHandoff=${wasHandoff})`);

    // Notify Convex that the call ended so it doesn't stay stuck as in_progress.
    // NOTE: when a handoff was fired for this call, the media stream closing is
    // EXPECTED (Cadence redirected the payer leg into the conference to drop the
    // AI) — it does NOT mean the whole call ended. We pass wasHandoff so Convex
    // can distinguish "AI stream closed for handoff" from "call actually over".
    if (CONVEX_SITE_URL && callId) {
      notifyCallEnded(callId, wasHandoff).catch((err) =>
        console.error(`[cleanup] Failed to notify Convex:`, err.message)
      );
    }
  }
}

/**
 * Notify Convex that a call has ended by calling the call-ended HTTP endpoint.
 */
async function notifyCallEnded(callId, wasHandoff = false) {
  // wasHandoff=true → the AI media stream closed because Cadence redirected the
  // payer into the conference (handoff in progress), NOT because the call ended.
  // Convex uses this to avoid marking the call completed prematurely.
  const url = `${CONVEX_SITE_URL}/call-ended?callId=${encodeURIComponent(callId)}${
    wasHandoff ? "&handoff=1" : ""
  }`;
  console.log(`[cleanup] Notifying Convex call ended: ${callId} (handoff=${wasHandoff})`);
  const res = await fetch(url, { method: "POST" });
  if (!res.ok) {
    console.error(`[cleanup] Convex call-ended returned ${res.status}`);
  }
}

// ---------------------------------------------------------------------------
// Express app + HTTP server
// ---------------------------------------------------------------------------
const app = express();
const server = http.createServer(app);

app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    connections: totalWsConnections,
    activeCalls: activeCalls.size,
    monitorStreams: monitorStreams.size,
    browserListeners: browserListeners.size,
  });
});

// ---------------------------------------------------------------------------
// POST /start-monitor — Connect to ElevenLabs conversation monitor WebSocket
// and forward real-time events (user_transcript, agent_response, tool_call)
// to Convex via the /call-events HTTP endpoint.
// ---------------------------------------------------------------------------
app.post("/start-monitor", express.json(), async (req, res) => {
  const { conversationId, callId, convexSiteUrl } = req.body;
  if (!conversationId || !callId) {
    return res.status(400).json({ error: "Missing conversationId or callId" });
  }

  const targetConvexUrl = convexSiteUrl || CONVEX_SITE_URL;
  let agentOutputAudioFormat = null;
  console.log(`[rt-monitor] Starting monitor for conv=${conversationId} call=${callId} convex=${targetConvexUrl}`);

  try {
    const monitorUrl = `wss://api.elevenlabs.io/v1/convai/conversations/${conversationId}/monitor`;
    const monitorWs = new WebSocket(monitorUrl, {
      headers: { "xi-api-key": ELEVENLABS_API_KEY },
    });

    monitorWs.on("open", () => {
      console.log(`[rt-monitor] Connected to ElevenLabs monitor for conv=${conversationId}`);
    });

    monitorWs.on("message", async (data) => {
      try {
        const msg = JSON.parse(data.toString());
        let type = null;
        let message = null;

        // Forward audio events to browser listeners only when Twilio is not
        // already supplying call media for this call. ElevenLabs monitor audio
        // is PCM in the agent output format, while Twilio monitor/media-stream
        // audio is mu-law 8k. Mixing both sources into /listen causes doubled
        // and distorted live audio.
        if (msg.type === "audio" || msg.type === "audio_event") {
          receivedAnyEvent = true;
          const audioData = msg.audio?.chunk || msg.audio_event?.audio_base_64 || msg.audio?.data || msg.data;
          if (audioData && !monitorStreams.has(callId) && !activeCalls.has(callId)) {
            forwardToListeners(callId, audioData, "outbound", {
              codec: agentOutputAudioFormat || "pcm_16000",
              source: "elevenlabs_monitor",
            });
          }
          return; // Don't forward audio as a call event
        }

        if (msg.type === "user_transcript" || msg.type === "user_transcription_event") {
          type = "user_transcript";
          message = msg.user_transcription_event?.user_transcript || msg.user_transcript || "";
        } else if (msg.type === "agent_response" || msg.type === "agent_response_event") {
          type = "agent_response";
          message = msg.agent_response_event?.agent_response || msg.agent_response || "";
        } else if (msg.type === "tool_call" || msg.type === "client_tool_call") {
          type = "tool_call";
          message = msg.tool_name || msg.client_tool_call?.tool_name || "DTMF";
        } else if (msg.type === "conversation_initiation_metadata") {
          type = "status";
          message = "Call connected";
          agentOutputAudioFormat =
            msg.conversation_initiation_metadata_event?.agent_output_audio_format ||
            msg.conversation_initiation_metadata?.agent_output_audio_format ||
            msg.agent_output_audio_format ||
            agentOutputAudioFormat;
        } else {
          // Log unhandled event types for debugging
          console.log(`[rt-monitor] Unhandled event type: ${msg.type} keys: ${Object.keys(msg).join(",")}`);
        }

        if (type) receivedAnyEvent = true;
        if (type && targetConvexUrl) {
          fetch(`${targetConvexUrl}/call-events`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ callId, type, message }),
          }).catch((err) => {
            console.error(`[rt-monitor] Failed to POST event to Convex:`, err.message);
          });
        }
      } catch (parseErr) {
        // Log binary/unparseable messages (could be raw audio)
        if (data instanceof Buffer && data.length > 100 && !monitorStreams.has(callId) && !activeCalls.has(callId)) {
          // Likely raw audio from the ElevenLabs monitor; label it with the
          // monitor's output format so the browser does not decode it as mu-law.
          forwardToListeners(callId, data.toString("base64"), "outbound", {
            codec: agentOutputAudioFormat || "pcm_16000",
            source: "elevenlabs_monitor",
          });
          receivedAnyEvent = true;
        }
      }
    });

    let receivedAnyEvent = false;

    monitorWs.on("close", (code, reason) => {
      const reasonStr = reason?.toString() || '';
      console.log(`[rt-monitor] Monitor closed for conv=${conversationId}: ${code} ${reasonStr}`);
      // Only send "Call ended" if we actually received events (monitoring was working)
      // Don't send it if monitoring was rejected (1008) — that's not a real call end
      if (targetConvexUrl && receivedAnyEvent && code !== 1008) {
        fetch(`${targetConvexUrl}/call-events`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ callId, type: "status", message: "Call ended" }),
        }).catch(() => {});
      }
    });

    monitorWs.on("error", (err) => {
      console.error(`[rt-monitor] Monitor error for conv=${conversationId}:`, err.message);
    });

    res.json({ success: true, monitoring: conversationId });
  } catch (err) {
    console.error(`[rt-monitor] Failed to start:`, err.message);
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// WebSocket server
// ---------------------------------------------------------------------------
const wss = new WebSocketServer({ server });

wss.on("connection", (ws, req) => {
  const pathname = new URL(req.url, `http://${req.headers.host}`).pathname;
  totalWsConnections++;

  if (pathname === "/media-stream") {
    handleMediaStream(ws);
  } else if (pathname === "/monitor") {
    handleMonitor(ws);
  } else if (pathname.startsWith("/listen/")) {
    const callId = pathname.replace("/listen/", "");
    handleListen(ws, callId);
  } else {
    console.warn(`[ws] Unknown path: ${pathname}`);
    ws.close(4000, "Unknown path");
    totalWsConnections--;
  }

  ws.on("close", () => {
    totalWsConnections--;
  });
});

// ---------------------------------------------------------------------------
// /media-stream — Bidirectional Twilio <-> ElevenLabs relay
//
// Follows the nibodev/elevenlabs-twilio-i-o reference pattern:
//   1. ElevenLabs connection starts IMMEDIATELY on Twilio WS open
//   2. Audio pass-through with NO conversion, NO batching
//   3. Init message sent after metadata arrives from Convex
// ---------------------------------------------------------------------------
function handleMediaStream(ws) {
  console.log("[media-stream] New Twilio connection");

  let streamSid = null;
  let callId = null;
  let elevenLabsWs = null;
  let elevenLabsConnected = false; // true once ElevenLabs WS is OPEN
  let pendingInitMessage = null;   // queued init message if metadata arrives before EL connects

  // ------------------------------------------------------------------
  // Set up ElevenLabs connection IMMEDIATELY (don't wait for "start")
  // ------------------------------------------------------------------
  const setupElevenLabs = async () => {
    try {
      console.log("[media-stream] Getting ElevenLabs signed URL...");
      const signedUrl = await getElevenLabsSignedUrl();
      console.log("[media-stream] Got signed URL, connecting to ElevenLabs...");

      elevenLabsWs = new WebSocket(signedUrl);

      elevenLabsWs.on("open", () => {
        console.log("[media-stream] ElevenLabs WebSocket connected");
        elevenLabsConnected = true;

        // If we already have the init message queued (metadata fetched before EL connected), send it now
        if (pendingInitMessage) {
          console.log("[media-stream] Sending queued init message to ElevenLabs");
          elevenLabsWs.send(JSON.stringify(pendingInitMessage));
          pendingInitMessage = null;
        }

        // Update the active call reference if callId is already known
        if (callId) {
          const callEntry = activeCalls.get(callId);
          if (callEntry) {
            callEntry.elevenLabsWs = elevenLabsWs;
          }
        }
      });

      elevenLabsWs.on("message", async (data) => {
        let message;
        try {
          message = JSON.parse(data.toString());
        } catch {
          return;
        }

        // Isolate per-message handling: this handler is async and runs per
        // ElevenLabs event for the lifetime of a call, so an uncaught throw
        // or rejected await anywhere in the switch below would otherwise
        // become an unhandled promise rejection — which crashes the ENTIRE
        // Node process (killing every other in-progress call), not just this
        // one connection. Catch and log instead.
        try {
        switch (message.type) {
          case "conversation_initiation_metadata":
            console.log(`[ElevenLabs] Received conversation initiation metadata`);
            break;

          case "audio":
            if (isHumanHandoffActive(callId)) {
              // After a real payer-side human answers, ElevenLabs is isolated:
              // it should neither talk over nor influence the human-human call.
              break;
            }
            // Check BOTH audio.chunk AND audio_event.audio_base_64 (reference pattern)
            if (message.audio?.chunk) {
              // Send directly to Twilio — NO conversion
              if (ws.readyState === WebSocket.OPEN && streamSid) {
                ws.send(JSON.stringify({
                  event: "media",
                  streamSid,
                  media: { payload: message.audio.chunk },
                }));
              }
              // Forward to browser listeners
              if (callId) {
                forwardToListeners(callId, message.audio.chunk, "outbound", {
                  codec: "mulaw_8000",
                  source: "media_stream",
                });
              }
            } else if (message.audio_event?.audio_base_64) {
              // Send directly to Twilio — NO conversion
              if (ws.readyState === WebSocket.OPEN && streamSid) {
                ws.send(JSON.stringify({
                  event: "media",
                  streamSid,
                  media: { payload: message.audio_event.audio_base_64 },
                }));
              }
              // Forward to browser listeners
              if (callId) {
                forwardToListeners(callId, message.audio_event.audio_base_64, "outbound", {
                  codec: "mulaw_8000",
                  source: "media_stream",
                });
              }
            } else {
              console.log("[ElevenLabs] Received audio but no StreamSid yet");
            }
            break;

          case "interruption":
            if (isHumanHandoffActive(callId)) {
              break;
            }
            if (ws.readyState === WebSocket.OPEN && streamSid) {
              ws.send(JSON.stringify({ event: "clear", streamSid }));
            }
            break;

          case "ping":
            // Reference uses message.ping_event?.event_id
            if (message.ping_event?.event_id) {
              if (elevenLabsWs.readyState === WebSocket.OPEN) {
                elevenLabsWs.send(JSON.stringify({
                  type: "pong",
                  event_id: message.ping_event.event_id,
                }));
              }
            }
            break;

          case "user_transcript": {
            // Reference uses message.user_transcription_event?.user_transcript
            const text = (message.user_transcription_event?.user_transcript || "").trim();
            if (text) {
              console.log(`[User] ${text}`);
              if (callId) {
                broadcastToListeners(callId, {
                  event: "transcript",
                  role: "user",
                  text,
                });
                // The payer IVR/rep speech comes through as "user". If it
                // signals a live-rep handoff, notify Convex to broadcast the
                // call to our agent pool (the AI stays silent per its prompt).
                // Only for callConnectionType "ivr_human_handoff" (default) —
                // the other types are driven entirely by the LLM's own prompt
                // instructions, so this automatic detection must stay out of
                // their way (see autoHandoffEnabled()).
                if (autoHandoffEnabled(callId)) {
                  const normalizedText = text.toLowerCase();
                  if (isActionableIvrPrompt(normalizedText) && !isTransferOrHoldCue(normalizedText)) {
                    clearHoldCueTimer(callId);
                  }
                  const handoffSignal = handoffSignalFromText(callId, text);
                  if (handoffSignal) {
                    clearHoldCueTimer(callId);
                    const handoffStarted = await fireHandoff(callId, CONVEX_SITE_URL, handoffSignal.reason);
                    if (handoffStarted && handoffSignal.detachElevenLabs) {
                      detachElevenLabsForHandoff(callId, elevenLabsWs, handoffSignal.reason);
                    }
                  } else if (handoffArmed.get(callId) && isTransferOrHoldCue(normalizedText)) {
                    scheduleHoldCueHandoff(callId, elevenLabsWs, "ivr_hold_queue_detected");
                  }
                }
              }
            }
            break;
          }

          case "agent_response":
          case "agent_response_correction": {
            if (isHumanHandoffActive(callId)) {
              break;
            }
            // Reference uses message.agent_response_event?.agent_response
            const text = (message.agent_response_event?.agent_response || "").trim();
            if (text) {
              console.log(`[Agent] ${text}`);
              if (callId) {
                broadcastToListeners(callId, {
                  event: "transcript",
                  role: "agent",
                  text,
                });
              }
            }
            break;
          }

          default:
            // DIAGNOSTIC: dump the full message for any unhandled type so we can
            // see whether ElevenLabs relays the agent's DTMF (play_keypad_touch_tone)
            // intent over this WebSocket — the deciding factor for how we inject DTMF.
            console.log(
              `[ElevenLabs] Unhandled message type: ${message.type} | full: ${JSON.stringify(message).slice(0, 800)}`
            );
            break;
        }
        } catch (err) {
          console.error(
            `[ElevenLabs] Error handling message type=${message.type} callId=${callId}:`,
            err && err.stack ? err.stack : err
          );
        }
      });

      elevenLabsWs.on("error", (err) => {
        console.error(`[ElevenLabs] WebSocket error:`, err.message);
        if (callId && handoffArmed.get(callId) && !handoffFired.get(callId)) {
          fireHandoff(callId, CONVEX_SITE_URL, "elevenlabs_error_after_transfer_hold").catch((handoffErr) => {
            console.error(`[handoff] Fallback after ElevenLabs error failed:`, handoffErr.message);
          });
        }
      });

      elevenLabsWs.on("close", async (code, reason) => {
        console.log(`[ElevenLabs] Disconnected: ${code} ${reason}`);
        elevenLabsConnected = false;
        if (await preserveTwilioForPossibleHandoff(callId, "elevenlabs_closed_after_transfer_hold")) {
          console.log(
            `[ElevenLabs] Closed after possible handoff for callId=${callId}; leaving Twilio payer leg open for UI accept.`
          );
          return;
        }
        // Close Twilio side too
        if (ws.readyState === WebSocket.OPEN) {
          ws.close();
        }
        if (callId) {
          cleanupCall(callId);
        }
      });
    } catch (err) {
      console.error(`[media-stream] Failed to set up ElevenLabs:`, err.message);
      ws.close();
    }
  };

  // Start ElevenLabs connection IMMEDIATELY — matches reference pattern
  setupElevenLabs();

  // ------------------------------------------------------------------
  // Handle messages from Twilio
  // ------------------------------------------------------------------
  ws.on("message", async (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }

    switch (msg.event) {
      case "start":
        streamSid = msg.start.streamSid;
        const customParams = msg.start.customParameters || {};
        callId = customParams.callId || `unknown-${Date.now()}`;
        const claimId = customParams.claimId || null;
        console.log(`[Twilio] Stream started — streamSid=${streamSid}, callId=${callId}, claimId=${claimId}`);

        // Store the active call
        activeCalls.set(callId, { twilioWs: ws, elevenLabsWs, streamSid });

        // Fetch metadata from Convex then send init to ElevenLabs
        try {
          const metadata = await fetchCallMetadata(callId);
          callConnectionTypes.set(
            callId,
            metadata.dynamic_variables?.call_connection_type || "ivr_human_handoff"
          );
          const initMessage = {
            type: "conversation_initiation_client_data",
            dynamic_variables: metadata.dynamic_variables || {},
          };
          if (metadata.conversation_config_override) {
            initMessage.conversation_config_override = metadata.conversation_config_override;
          }

          if (elevenLabsConnected && elevenLabsWs && elevenLabsWs.readyState === WebSocket.OPEN) {
            // ElevenLabs already connected — send init now
            console.log("[media-stream] Sending init message to ElevenLabs");
            elevenLabsWs.send(JSON.stringify(initMessage));
          } else {
            // ElevenLabs not connected yet — queue it
            console.log("[media-stream] ElevenLabs not ready yet, queuing init message");
            pendingInitMessage = initMessage;
          }
        } catch (err) {
          console.error(`[media-stream] Metadata fetch failed:`, err.message);
          // Send init with empty dynamic_variables anyway so ElevenLabs starts
          const initMessage = {
            type: "conversation_initiation_client_data",
            dynamic_variables: {},
          };
          if (elevenLabsConnected && elevenLabsWs && elevenLabsWs.readyState === WebSocket.OPEN) {
            elevenLabsWs.send(JSON.stringify(initMessage));
          } else {
            pendingInitMessage = initMessage;
          }
        }
        break;

      case "media":
        // Pass audio straight through to ElevenLabs — NO conversion, NO batching
        // Exactly matches reference: Buffer.from(payload, "base64").toString("base64")
        if (elevenLabsWs?.readyState === WebSocket.OPEN && !isHumanHandoffActive(callId)) {
          const audioMessage = {
            user_audio_chunk: Buffer.from(msg.media.payload, "base64").toString("base64"),
          };
          elevenLabsWs.send(JSON.stringify(audioMessage));
        }

        // Forward to browser listeners (inbound track)
        if (callId) {
          forwardToListeners(callId, msg.media.payload, "inbound", {
            codec: "mulaw_8000",
            source: "media_stream",
          });
        }
        break;

      case "stop":
        console.log(`[Twilio] Stream stopped for callId=${callId}`);
        if (elevenLabsWs?.readyState === WebSocket.OPEN) {
          elevenLabsWs.close();
        }
        if (callId) {
          cleanupCall(callId);
        }
        break;

      default:
        break;
    }
  });

  ws.on("close", () => {
    console.log(`[media-stream] Twilio WS closed for callId=${callId}`);
    if (elevenLabsWs?.readyState === WebSocket.OPEN) {
      elevenLabsWs.close();
    }
    if (callId) {
      cleanupCall(callId);
    }
  });

  ws.on("error", (err) => {
    console.error(`[media-stream] Twilio WS error:`, err.message);
  });
}

// ---------------------------------------------------------------------------
// /monitor — Unidirectional Twilio stream for browser monitoring
// ---------------------------------------------------------------------------
function handleMonitor(ws) {
  console.log("[monitor] New monitor connection");

  let callId = null;

  ws.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }

    switch (msg.event) {
      case "connected":
        console.log("[monitor] Twilio monitor connected");
        break;

      case "start":
        const customParams = msg.start.customParameters || {};
        callId = customParams.callId || `monitor-${Date.now()}`;
        console.log(`[monitor] Stream started for callId=${callId}`);

        // Register this monitor stream
        if (!monitorStreams.has(callId)) {
          monitorStreams.set(callId, new Set());
        }
        monitorStreams.get(callId).add(ws);
        break;

      case "media":
        if (callId && msg.media && msg.media.payload) {
          // Forward to all browser listeners for this callId
          forwardToListeners(callId, msg.media.payload, msg.media.track || "both", {
            codec: "mulaw_8000",
            source: "twilio_monitor",
          });
        }
        break;

      case "stop":
        console.log(`[monitor] Stream stopped for callId=${callId}`);
        break;

      default:
        break;
    }
  });

  ws.on("close", () => {
    console.log(`[monitor] WS closed for callId=${callId}`);
    if (callId) {
      const streams = monitorStreams.get(callId);
      if (streams) {
        streams.delete(ws);
        if (streams.size === 0) {
          monitorStreams.delete(callId);
        }
      }
    }
  });

  ws.on("error", (err) => {
    console.error(`[monitor] WS error:`, err.message);
  });
}

// ---------------------------------------------------------------------------
// /listen/:callId — Browser connects to hear live audio
// ---------------------------------------------------------------------------
function handleListen(ws, callId) {
  console.log(`[listen] Browser connected for callId=${callId}`);

  // Register this browser listener
  if (!browserListeners.has(callId)) {
    browserListeners.set(callId, new Set());
  }
  browserListeners.get(callId).add(ws);

  // Send a welcome message
  ws.send(
    JSON.stringify({
      event: "connected",
      callId,
      message: "Listening for audio on this call",
    })
  );

  ws.on("close", () => {
    console.log(`[listen] Browser disconnected for callId=${callId}`);
    const listeners = browserListeners.get(callId);
    if (listeners) {
      listeners.delete(ws);
      if (listeners.size === 0) {
        browserListeners.delete(callId);
      }
    }
  });

  ws.on("error", (err) => {
    console.error(`[listen] WS error for callId=${callId}:`, err.message);
  });
}

// ---------------------------------------------------------------------------
// Start server
// ---------------------------------------------------------------------------
server.listen(PORT, () => {
  console.log(`[cadence-bridge] Server listening on port ${PORT}`);
  console.log(`[cadence-bridge] Endpoints:`);
  console.log(`  ws://localhost:${PORT}/media-stream       — Twilio <-> ElevenLabs relay`);
  console.log(`  ws://localhost:${PORT}/monitor            — Twilio monitor stream`);
  console.log(`  ws://localhost:${PORT}/listen/:callId     — Browser audio listener`);
  console.log(`  http://localhost:${PORT}/health            — Health check`);
  console.log(`  POST http://localhost:${PORT}/start-monitor — Start ElevenLabs conversation monitor`);
  console.log(`[cadence-bridge] ElevenLabs Agent ID: ${ELEVENLABS_AGENT_ID || "(not set)"}`);
  console.log(`[cadence-bridge] Convex Site URL: ${CONVEX_SITE_URL || "(not set)"}`);
  console.log(`[cadence-bridge] HANDOFF_ON_TRANSFER_CUE=${HANDOFF_ON_TRANSFER_CUE}`);
  console.log(`[cadence-bridge] DETACH_ELEVENLABS_ON_HANDOFF=${DETACH_ELEVENLABS_ON_HANDOFF}`);
  console.log(`[cadence-bridge] HOLD_CUE_HANDOFF_DELAY_MS=${HOLD_CUE_HANDOFF_DELAY_MS}`);
});
