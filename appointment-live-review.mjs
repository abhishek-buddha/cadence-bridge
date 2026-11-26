// Optional real-service test. Requires a synthetic active call in cadence_review.
// Speaks generated test audio over the Twilio protocol; does not place a PSTN call.
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { readFileSync } from 'node:fs';
import { WebSocket, WebSocketServer } from 'ws';
import { handleAppointmentStream } from './appointments.js';

assert.equal(process.env.CADENCE_API_BASE_URL, 'http://127.0.0.1:18080');
const callId = process.env.REVIEW_CALL_ID;
const appointmentId = process.env.REVIEW_APPOINTMENT_ID;
assert(callId && appointmentId && process.env.REVIEW_CALL_TOKEN && process.env.REVIEW_AUDIO_PATH);
function readSpeech(path) {
const wav = readFileSync(path);
assert.equal(wav.toString('ascii', 0, 4), 'RIFF');
let speech;
for (let offset = 12; offset + 8 <= wav.length;) {
  const type = wav.toString('ascii', offset, offset + 4), size = wav.readUInt32LE(offset + 4);
  if (type === 'fmt ') {
    assert.equal(wav.readUInt16LE(offset + 8), 7, 'Test WAV must be mu-law');
    assert.equal(wav.readUInt32LE(offset + 12), 8000, 'Test audio must be 8 kHz');
  }
  if (type === 'data') speech = wav.subarray(offset + 8, offset + 8 + size);
  offset += 8 + size + size % 2;
}
assert(speech?.length);
return Buffer.concat([speech, Buffer.alloc(16000, 255)]);
}
const turns = [];
let inbound = 0, outbound = 0, receivedAudio = 0;
const server = new WebSocketServer({ port: 0, host: '127.0.0.1' });
await once(server, 'listening');
server.on('connection', ws => handleAppointmentStream(ws, {
  forwardToListeners: (_id, _payload, track) => { if (track === 'inbound') inbound++; else outbound++; },
  broadcastToListeners: (_id, turn) => { turns.push(turn); console.log(turn.role + ': ' + turn.text); },
}));
const socket = new WebSocket(`ws://127.0.0.1:${server.address().port}/appointment-media-stream`);
await once(socket, 'open');
socket.on('message', raw => { if (JSON.parse(raw).event === 'media') receivedAudio++; });
socket.send(JSON.stringify({ event: 'start', start: { streamSid: 'review-stream', customParameters: { callId, token: process.env.REVIEW_CALL_TOKEN } } }));
async function speak(path) {
  const audio = readSpeech(path);
  for (let i = 0; i < audio.length; i += 160) {
    socket.send(JSON.stringify({ event: 'media', media: { payload: audio.subarray(i, i + 160).toString('base64') } }));
    await delay(20);
  }
}
const getAppointment = () => fetch(`http://127.0.0.1:18080/api/appointments/${appointmentId}`, { headers: { Authorization: 'Bearer review-session' } }).then(r => r.json());
try {
  for (let i = 0; i < 20 && !receivedAudio; i++) await delay(1000);
  assert(receivedAudio > 0, 'Agent audio must reach the caller');
  await delay(10000);
  await speak(process.env.REVIEW_AUDIO_PATH);
  for (let i = 0; i < 35 && !turns.some(t => t.role === 'agent' && /scheduled|appointment.*January/i.test(t.text)); i++) await delay(1000);
  assert(turns.some(t => t.role === 'user'), 'Patient audio must be transcribed');
  assert(turns.some(t => t.role === 'agent' && /scheduled|appointment.*January/i.test(t.text)), 'Real verification tool must unlock appointment details');
  assert(inbound > 0 && outbound > 0, 'Live monitor must receive both audio tracks');
  await delay(15000);
  const beforeRequest = turns.length;
  await speak(process.env.REVIEW_RESCHEDULE_AUDIO_PATH);
  const asked = () => turns.slice(beforeRequest).some(t => t.role === 'agent' && /February/i.test(t.text) && /\?/.test(t.text));
  for (let i = 0; i < 30 && !asked(); i++) await delay(1000);
  assert(asked(), 'Agent must read back the new slot');
  assert.equal((await getAppointment()).status, 'scheduled', 'No booking change before confirmation');
  await delay(15000);
  await speak(process.env.REVIEW_CONFIRM_AUDIO_PATH);
  let appointment;
  for (let i = 0; i < 30; i++) {
    appointment = await getAppointment();
    if (appointment.status === 'rescheduled') break;
    await delay(1000);
  }
  assert.equal(appointment.status, 'rescheduled', 'Real ASR patient-turn counting must unlock the confirmed change');
  assert(appointment.scheduled_at.startsWith('2027-02-20T15:45'));
  await delay(10000);
  const handoff = await fetch(`http://127.0.0.1:18080/api/handoff/${callId}`, { headers: { Authorization: 'Bearer review-session' } }).then(r => r.json());
  assert(!handoff.events.some(e => e.type === 'request_human' || e.type === 'handoff_requested'), 'Booking-only speech, including split/repeated confirmation, must not ask for an operator');
} finally {
  socket.close();
  await once(socket, 'close');
  await new Promise(resolve => server.close(resolve));
}
await delay(2000);
const history = await fetch(`http://127.0.0.1:18080/api/appointments/${appointmentId}/calls`, { headers: { Authorization: 'Bearer review-session' } }).then(r => r.json());
const call = history.find(c => String(c.id) === callId);
assert(call.eleven_labs_conversation_id && call.transcript.includes('Patient:'), 'Bridge must persist conversation and transcript');
assert.equal(call.status, 'in_progress', 'AI disconnect must not close the patient phone call');
console.log('PASS: real ElevenLabs audio, identity, prepare/confirm booking tools with real patient-turn counting, two-way monitor, persisted artifacts and stream-close lifecycle');
