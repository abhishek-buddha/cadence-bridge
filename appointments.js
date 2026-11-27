import { WebSocket } from 'ws';

// Separate transport entrypoint: claims retain their existing relay and detection.
export function handleAppointmentStream(twilio, { forwardToListeners, broadcastToListeners }) {
  const base = process.env.CADENCE_API_BASE_URL;
  const secret = process.env.APPOINTMENT_BRIDGE_SECRET;
  let callId, token, streamSid, agent, conversationId;
  let failureCode;
  let closed = false;
  let patientTurn = 0;
  const transcript = [];
  const pendingAudio = [];
  const completedTools = new Map();
  let tools = Promise.resolve();

  async function api(path, body) {
    const response = await fetch(`${base}/appointment-voice/${path}`, {
      method: body ? 'POST' : 'GET',
      headers: { Authorization: `Bearer ${secret}`, 'Content-Type': 'application/json' },
      body: body ? JSON.stringify({ ...body, callId, token }) : undefined,
      signal: AbortSignal.timeout(15000),
    });
    if (!response.ok) throw new Error(`Appointment service returned ${response.status}`);
    return response.json();
  }

  async function persist() {
    if (!callId || !token) return;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        await api('artifacts', { conversation_id: conversationId, transcript: transcript.join('\n'), failure_code: failureCode });
        return;
      } catch (error) {
        if (attempt === 2) console.error('[appointments] Artifact persistence failed:', error.message);
      }
    }
  }

  function finish() {
    if (closed) return;
    closed = true;
    clearInterval(saveTimer);
    if (callId) broadcastToListeners(callId, { event: 'clear', track: 'outbound' });
    if (agent && agent.readyState < WebSocket.CLOSING) agent.close();
    persist();
  }
  const saveTimer = setInterval(persist, 10000);

  async function runTool(event, patientTurnAtRequest) {
    const { tool_call_id: id, tool_name: name, parameters } = event;
    let result = completedTools.get(id);
    if (!result) {
      try {
        const response = await api('tool', { name, arguments: parameters || {}, patient_turn: patientTurnAtRequest });
        result = { type: 'client_tool_result', tool_call_id: id, result: JSON.stringify(response), is_error: false };
      } catch {
        result = { type: 'client_tool_result', tool_call_id: id, result: 'The request could not be completed. Do not claim it succeeded. Offer assistance from the clinic.', is_error: true };
      }
      completedTools.set(id, result);
    }
    if (agent?.readyState === WebSocket.OPEN) agent.send(JSON.stringify(result));
  }

  async function start(message) {
    if (callId) throw new Error('Duplicate stream start');
    ({ callId, token } = message.start.customParameters || {});
    streamSid = message.start.streamSid;
    if (!callId || !token || !secret || !base) throw new Error('Appointment stream is not configured');
    const metadata = await api(`metadata?callId=${encodeURIComponent(callId)}&token=${encodeURIComponent(token)}`);
    const response = await fetch(`https://api.elevenlabs.io/v1/convai/conversation/get_signed_url?agent_id=${encodeURIComponent(metadata.agent_id)}`, {
      headers: { 'xi-api-key': process.env.ELEVENLABS_API_KEY }, signal: AbortSignal.timeout(15000),
    });
    if (!response.ok) {
      failureCode = 'agent_unavailable';
      throw new Error('Appointment agent is unavailable');
    }
    const { signed_url: signedUrl } = await response.json();
    if (closed) return;
    agent = new WebSocket(signedUrl);
    agent.on('open', () => {
      agent.send(JSON.stringify({ type: 'conversation_initiation_client_data', dynamic_variables: metadata.dynamic_variables }));
      for (const payload of pendingAudio.splice(0)) agent.send(JSON.stringify({ user_audio_chunk: payload }));
    });
    agent.on('message', raw => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch { return; }
      if (msg.type === 'ping') {
        agent.send(JSON.stringify({ type: 'pong', event_id: msg.ping_event?.event_id }));
      } else if (msg.type === 'conversation_initiation_metadata') {
        conversationId = msg.conversation_initiation_metadata_event?.conversation_id;
        persist();
      } else if (msg.type === 'audio') {
        const payload = msg.audio_event?.audio_base_64 || msg.audio?.chunk;
        if (payload && twilio.readyState === WebSocket.OPEN) {
          twilio.send(JSON.stringify({ event: 'media', streamSid, media: { payload } }));
          forwardToListeners(callId, payload, 'outbound');
        }
      } else if (msg.type === 'interruption' && twilio.readyState === WebSocket.OPEN) {
        twilio.send(JSON.stringify({ event: 'clear', streamSid }));
        broadcastToListeners(callId, { event: 'clear', track: 'outbound' });
      } else if (msg.type === 'user_transcript' || msg.type === 'agent_response') {
        const role = msg.type === 'user_transcript' ? 'user' : 'agent';
        const text = role === 'user' ? msg.user_transcription_event?.user_transcript : msg.agent_response_event?.agent_response;
        if (text) {
          if (role === 'user') patientTurn++;
          transcript.push(`${role === 'user' ? 'Patient' : 'Agent'}: ${text}`);
          broadcastToListeners(callId, { event: 'transcript', role, text });
        }
        // No IVR/human-speech detection: the patient is human from the start.
      } else if (msg.type === 'client_tool_call') {
        const turn = patientTurn;
        tools = tools.then(() => runTool(msg.client_tool_call, turn)).catch(error => console.error('[appointments] Tool failed:', error.message));
      }
    });
    agent.on('error', () => {
      failureCode = 'agent_connection_failed';
      console.error('[appointments] Agent connection failed');
      twilio.close();
    });
    agent.on('close', () => { if (twilio.readyState === WebSocket.OPEN) twilio.close(); });
  }

  twilio.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (msg.event === 'start') start(msg).catch(error => {
      failureCode ||= 'stream_start_failed';
      console.error('[appointments] Start failed:', error.message);
      twilio.close();
    });
    else if (msg.event === 'media' && msg.media?.payload) {
      if (agent?.readyState === WebSocket.OPEN) agent.send(JSON.stringify({ user_audio_chunk: msg.media.payload }));
      else if (pendingAudio.length < 100) pendingAudio.push(msg.media.payload);
      if (callId) forwardToListeners(callId, msg.media.payload, 'inbound');
    } else if (msg.event === 'stop') finish();
  });
  twilio.on('close', finish);
  twilio.on('error', finish);
}
