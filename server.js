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
// Mu-law <-> PCM16 conversion + resampling
// ---------------------------------------------------------------------------

// Standard mu-law decoding table (256 entries, index = mu-law byte)
const MULAW_DECODE_TABLE = new Int16Array([
  -32124, -31100, -30076, -29052, -28028, -27004, -25980, -24956,
  -23932, -22908, -21884, -20860, -19836, -18812, -17788, -16764,
  -15996, -15484, -14972, -14460, -13948, -13436, -12924, -12412,
  -11900, -11388, -10876, -10364, -9852, -9340, -8828, -8316,
  -7932, -7676, -7420, -7164, -6908, -6652, -6396, -6140,
  -5884, -5628, -5372, -5116, -4860, -4604, -4348, -4092,
  -3900, -3772, -3644, -3516, -3388, -3260, -3132, -3004,
  -2876, -2748, -2620, -2492, -2364, -2236, -2108, -1980,
  -1884, -1820, -1756, -1692, -1628, -1564, -1500, -1436,
  -1372, -1308, -1244, -1180, -1116, -1052, -988, -924,
  -876, -844, -812, -780, -748, -716, -684, -652,
  -620, -588, -556, -524, -492, -460, -428, -396,
  -372, -356, -340, -324, -308, -292, -276, -260,
  -244, -228, -212, -196, -180, -164, -148, -132,
  -120, -112, -104, -96, -88, -80, -72, -64,
  -56, -48, -40, -32, -24, -16, -8, 0,
  32124, 31100, 30076, 29052, 28028, 27004, 25980, 24956,
  23932, 22908, 21884, 20860, 19836, 18812, 17788, 16764,
  15996, 15484, 14972, 14460, 13948, 13436, 12924, 12412,
  11900, 11388, 10876, 10364, 9852, 9340, 8828, 8316,
  7932, 7676, 7420, 7164, 6908, 6652, 6396, 6140,
  5884, 5628, 5372, 5116, 4860, 4604, 4348, 4092,
  3900, 3772, 3644, 3516, 3388, 3260, 3132, 3004,
  2876, 2748, 2620, 2492, 2364, 2236, 2108, 1980,
  1884, 1820, 1756, 1692, 1628, 1564, 1500, 1436,
  1372, 1308, 1244, 1180, 1116, 1052, 988, 924,
  876, 844, 812, 780, 748, 716, 684, 652,
  620, 588, 556, 524, 492, 460, 428, 396,
  372, 356, 340, 324, 308, 292, 276, 260,
  244, 228, 212, 196, 180, 164, 148, 132,
  120, 112, 104, 96, 88, 80, 72, 64,
  56, 48, 40, 32, 24, 16, 8, 0,
]);

/**
 * Encode a single PCM16 sample to mu-law byte.
 */
function pcm16ToMulaw(sample) {
  const MULAW_MAX = 0x1fff;
  const MULAW_BIAS = 33;
  let sign = 0;
  if (sample < 0) {
    sign = 0x80;
    sample = -sample;
  }
  if (sample > MULAW_MAX) sample = MULAW_MAX;
  sample += MULAW_BIAS;

  let exponent = 7;
  let mask = 0x4000;
  while (exponent > 0 && !(sample & mask)) {
    exponent--;
    mask >>= 1;
  }
  const mantissa = (sample >> (exponent + 3)) & 0x0f;
  const mulawByte = ~(sign | (exponent << 4) | mantissa) & 0xff;
  return mulawByte;
}

/**
 * Convert mu-law 8kHz base64 audio → PCM16 16kHz base64 audio.
 * Steps: decode mu-law → PCM16 8kHz → upsample 2x to 16kHz.
 */
function mulawToPcm16(base64Mulaw) {
  const mulawBuf = Buffer.from(base64Mulaw, "base64");
  const sampleCount = mulawBuf.length;
  // Upsample 2x: each input sample becomes 2 output samples (linear interpolation)
  const pcm16Buf = Buffer.alloc(sampleCount * 2 * 2); // 2x samples, 2 bytes each

  for (let i = 0; i < sampleCount; i++) {
    const currentSample = MULAW_DECODE_TABLE[mulawBuf[i]];
    const nextSample =
      i + 1 < sampleCount
        ? MULAW_DECODE_TABLE[mulawBuf[i + 1]]
        : currentSample;
    const midSample = (currentSample + nextSample) >> 1;

    // Write two samples (little-endian 16-bit)
    pcm16Buf.writeInt16LE(currentSample, i * 4);
    pcm16Buf.writeInt16LE(midSample, i * 4 + 2);
  }

  return pcm16Buf.toString("base64");
}

/**
 * Convert PCM16 16kHz base64 audio → mu-law 8kHz base64 audio.
 * Steps: downsample 2x (take every other sample) → encode mu-law.
 */
function pcm16ToMulawBuffer(base64Pcm16) {
  const pcmBuf = Buffer.from(base64Pcm16, "base64");
  const totalSamples = pcmBuf.length / 2;
  // Downsample 2x
  const outputSamples = Math.floor(totalSamples / 2);
  const mulawBuf = Buffer.alloc(outputSamples);

  for (let i = 0; i < outputSamples; i++) {
    const sample = pcmBuf.readInt16LE(i * 4); // skip every other sample
    mulawBuf[i] = pcm16ToMulaw(sample);
  }

  return mulawBuf.toString("base64");
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
  const url = `https://api.elevenlabs.io/v1/convai/conversation/get-signed-url?agent_id=${ELEVENLABS_AGENT_ID}`;
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
// ---------------------------------------------------------------------------
function handleMediaStream(ws) {
  console.log("[media-stream] New Twilio connection");

  let streamSid = null;
  let callId = null;
  let claimId = null;
  let elevenLabsWs = null;
  let elevenLabsReady = false;
  let pendingAudioChunks = []; // Buffer audio while ElevenLabs connects

  ws.on("message", async (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }

    switch (msg.event) {
      case "connected":
        console.log("[media-stream] Twilio connected");
        break;

      case "start":
        streamSid = msg.start.streamSid;
        const customParams = msg.start.customParameters || {};
        callId = customParams.callId || `unknown-${Date.now()}`;
        claimId = customParams.claimId || null;
        console.log(`[media-stream] Stream started — streamSid=${streamSid}, callId=${callId}, claimId=${claimId}`);

        // Store the active call
        activeCalls.set(callId, { twilioWs: ws, elevenLabsWs: null, streamSid });

        // Fetch metadata and connect to ElevenLabs
        try {
          const metadata = await fetchCallMetadata(callId);

          // Get signed URL from ElevenLabs
          console.log("[media-stream] Getting ElevenLabs signed URL...");
          const signedUrl = await getElevenLabsSignedUrl();
          console.log("[media-stream] Got signed URL, connecting to ElevenLabs...");

          elevenLabsWs = new WebSocket(signedUrl);

          elevenLabsWs.on("open", () => {
            console.log(`[media-stream] ElevenLabs WebSocket connected for callId=${callId}`);

            // Send initial configuration — only dynamic variables, no prompt overrides
            const initMessage = {
              type: "conversation_initiation_client_data",
              dynamic_variables: metadata.dynamic_variables || {},
            };
            elevenLabsWs.send(JSON.stringify(initMessage));
            console.log("[media-stream] Sent conversation init to ElevenLabs — waiting for metadata response before sending audio");

            // Update the active call reference
            const callEntry = activeCalls.get(callId);
            if (callEntry) {
              callEntry.elevenLabsWs = elevenLabsWs;
            }
          });

          elevenLabsWs.on("message", (elData) => {
            let elMsg;
            try {
              elMsg = JSON.parse(elData.toString());
            } catch {
              return;
            }

            // ElevenLabs ready signal — now we can start sending audio
            if (elMsg.type === "conversation_initiation_metadata") {
              console.log(`[media-stream] ElevenLabs ready for callId=${callId} — dropping ${pendingAudioChunks.length} pre-ready chunks`);
              elevenLabsReady = true;
              pendingAudioChunks = []; // Drop pre-ready audio, only forward new audio from here
            } else if (elMsg.type === "audio" && elMsg.audio && elMsg.audio.chunk) {
              // ElevenLabs sends PCM16 16kHz, convert to mu-law 8kHz for Twilio
              const mulawAudio = pcm16ToMulawBuffer(elMsg.audio.chunk);

              // Send to Twilio
              if (ws.readyState === WebSocket.OPEN && streamSid) {
                ws.send(
                  JSON.stringify({
                    event: "media",
                    streamSid,
                    media: { payload: mulawAudio },
                  })
                );
              }

              // Forward to browser listeners
              forwardToListeners(callId, mulawAudio, "outbound");
            } else if (elMsg.type === "agent_response" || elMsg.type === "agent_response_correction") {
              // Forward agent transcript to browser listeners
              const text = (elMsg.agent_response || "").trim();
              if (text && callId) {
                broadcastToListeners(callId, {
                  event: "transcript",
                  role: "agent",
                  text,
                });
              }
            } else if (elMsg.type === "user_transcript") {
              // Forward user transcript to browser listeners
              const text = (elMsg.user_transcript || "").trim();
              if (text && callId) {
                broadcastToListeners(callId, {
                  event: "transcript",
                  role: "user",
                  text,
                });
              }
            } else if (elMsg.type === "interruption") {
              // Send clear event to Twilio to stop playing audio
              if (ws.readyState === WebSocket.OPEN && streamSid) {
                ws.send(JSON.stringify({ event: "clear", streamSid }));
              }
            } else if (elMsg.type === "ping") {
              // Respond to ElevenLabs ping with pong
              if (elevenLabsWs.readyState === WebSocket.OPEN) {
                elevenLabsWs.send(
                  JSON.stringify({ type: "pong", event_id: elMsg.event_id })
                );
              }
            }
          });

          elevenLabsWs.on("error", (err) => {
            console.error(`[media-stream] ElevenLabs WS error for callId=${callId}:`, err.message);
          });

          elevenLabsWs.on("close", (code, reason) => {
            console.log(`[media-stream] ElevenLabs WS closed for callId=${callId}: ${code} ${reason}`);
            elevenLabsReady = false;
            // Close Twilio side too
            if (ws.readyState === WebSocket.OPEN) {
              ws.close();
            }
            cleanupCall(callId);
          });
        } catch (err) {
          console.error(`[media-stream] Failed to connect to ElevenLabs for callId=${callId}:`, err.message);
          ws.close();
          cleanupCall(callId);
        }
        break;

      case "media":
        if (msg.media && msg.media.payload) {
          // Convert Twilio mu-law 8kHz to PCM16 16kHz for ElevenLabs
          const pcm16Audio = mulawToPcm16(msg.media.payload);

          if (elevenLabsReady && elevenLabsWs && elevenLabsWs.readyState === WebSocket.OPEN) {
            elevenLabsWs.send(JSON.stringify({ user_audio_chunk: pcm16Audio }));
          } else {
            // Buffer while waiting for ElevenLabs connection
            pendingAudioChunks.push(pcm16Audio);
            // Cap buffer at ~5 seconds of audio (8000 samples/s * 5s / 160 samples per chunk ~ 250 chunks)
            if (pendingAudioChunks.length > 250) {
              pendingAudioChunks.shift();
            }
          }

          // Forward to browser listeners (inbound track)
          if (callId) {
            forwardToListeners(callId, msg.media.payload, "inbound");
          }
        }
        break;

      case "stop":
        console.log(`[media-stream] Twilio stream stopped for callId=${callId}`);
        if (elevenLabsWs && elevenLabsWs.readyState === WebSocket.OPEN) {
          elevenLabsWs.close();
        }
        cleanupCall(callId);
        break;

      default:
        break;
    }
  });

  ws.on("close", () => {
    console.log(`[media-stream] Twilio WS closed for callId=${callId}`);
    if (elevenLabsWs && elevenLabsWs.readyState === WebSocket.OPEN) {
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
  console.log(`  ws://localhost:${PORT}/media-stream  — Twilio <-> ElevenLabs relay`);
  console.log(`  ws://localhost:${PORT}/monitor       — Twilio monitor stream`);
  console.log(`  ws://localhost:${PORT}/listen/:callId — Browser audio listener`);
  console.log(`  http://localhost:${PORT}/health       — Health check`);
  console.log(`[cadence-bridge] ElevenLabs Agent ID: ${ELEVENLABS_AGENT_ID || "(not set)"}`);
  console.log(`[cadence-bridge] Convex Site URL: ${CONVEX_SITE_URL || "(not set)"}`);
});
