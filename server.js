import express from "express";
import http from "http";
import { WebSocketServer, WebSocket } from "ws";
import { URL } from "url";

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------
const PORT = parseInt(process.env.PORT || "3001", 10);
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY;
const ELEVENLABS_AGENT_ID = process.env.ELEVENLABS_AGENT_ID;
const CONVEX_SITE_URL = process.env.CONVEX_SITE_URL; // e.g. https://groovy-wren-932.convex.site

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

// callId -> { twilioWs, elevenLabsWs, streamSid }
const activeCalls = new Map();

// callId -> Set<WebSocket>  (browser listeners)
const browserListeners = new Map();

// callId -> Set<WebSocket>  (monitor sources — Twilio unidirectional streams)
const monitorStreams = new Map();

// All WebSocket connections for counting
let totalWsConnections = 0;

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
function forwardToListeners(callId, payload, track) {
  const listeners = browserListeners.get(callId);
  if (!listeners || listeners.size === 0) return;
  const msg = JSON.stringify({
    event: "audio",
    media: { payload, track },
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
  const call = activeCalls.get(callId);
  if (call) {
    if (call.twilioWs && call.twilioWs.readyState === WebSocket.OPEN) {
      call.twilioWs.close();
    }
    if (call.elevenLabsWs && call.elevenLabsWs.readyState === WebSocket.OPEN) {
      call.elevenLabsWs.close();
    }
    activeCalls.delete(callId);
    console.log(`[cleanup] Call ${callId} cleaned up`);

    // Notify Convex that the call ended so it doesn't stay stuck as in_progress
    if (CONVEX_SITE_URL && callId) {
      notifyCallEnded(callId).catch((err) =>
        console.error(`[cleanup] Failed to notify Convex:`, err.message)
      );
    }
  }
}

/**
 * Notify Convex that a call has ended by calling the call-ended HTTP endpoint.
 */
async function notifyCallEnded(callId) {
  const url = `${CONVEX_SITE_URL}/call-ended?callId=${encodeURIComponent(callId)}`;
  console.log(`[cleanup] Notifying Convex call ended: ${callId}`);
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

        // Forward audio events to browser listeners
        if (msg.type === "audio" || msg.type === "audio_event") {
          receivedAnyEvent = true;
          const audioData = msg.audio?.chunk || msg.audio_event?.audio_base_64 || msg.audio?.data || msg.data;
          if (audioData) {
            forwardToListeners(callId, audioData, "outbound");
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
        if (data instanceof Buffer && data.length > 100) {
          // Likely raw audio — forward as base64
          forwardToListeners(callId, data.toString("base64"), "outbound");
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

      elevenLabsWs.on("message", (data) => {
        let message;
        try {
          message = JSON.parse(data.toString());
        } catch {
          return;
        }

        switch (message.type) {
          case "conversation_initiation_metadata":
            console.log(`[ElevenLabs] Received conversation initiation metadata`);
            break;

          case "audio":
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
                forwardToListeners(callId, message.audio.chunk, "outbound");
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
                forwardToListeners(callId, message.audio_event.audio_base_64, "outbound");
              }
            } else {
              console.log("[ElevenLabs] Received audio but no StreamSid yet");
            }
            break;

          case "interruption":
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
              }
            }
            break;
          }

          case "agent_response":
          case "agent_response_correction": {
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
            console.log(`[ElevenLabs] Unhandled message type: ${message.type}`);
            break;
        }
      });

      elevenLabsWs.on("error", (err) => {
        console.error(`[ElevenLabs] WebSocket error:`, err.message);
      });

      elevenLabsWs.on("close", (code, reason) => {
        console.log(`[ElevenLabs] Disconnected: ${code} ${reason}`);
        elevenLabsConnected = false;
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
          const initMessage = {
            type: "conversation_initiation_client_data",
            dynamic_variables: metadata.dynamic_variables || {},
          };

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
        if (elevenLabsWs?.readyState === WebSocket.OPEN) {
          const audioMessage = {
            user_audio_chunk: Buffer.from(msg.media.payload, "base64").toString("base64"),
          };
          elevenLabsWs.send(JSON.stringify(audioMessage));
        }

        // Forward to browser listeners (inbound track)
        if (callId) {
          forwardToListeners(callId, msg.media.payload, "inbound");
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
          forwardToListeners(callId, msg.media.payload, msg.media.track || "both");
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
});
