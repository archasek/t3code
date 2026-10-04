const fixtureMarkers = [
  "ASK_SOCKET",
  "ALLOW",
  "REJECT",
  "ASK",
  "PLAN",
  "INTERRUPT",
  "RESUME",
  "CONCURRENT_A",
  "CONCURRENT_B",
] as const;

export function createFetchWrapper(cliPath: string): string {
  return `#!/usr/bin/env node
import { syncBuiltinESMExports } from 'node:module';
import { existsSync, watch, writeFileSync } from 'node:fs';
import { access, writeFile } from 'node:fs/promises';
import http from 'node:http';
import https from 'node:https';
import net from 'node:net';
import tls from 'node:tls';
import { basename, dirname, join } from 'node:path';

const cliPath = ${JSON.stringify(cliPath)};
const fixtureRoot = process.env.T3_MASTRA_CODE_FIXTURE_DIR;
const codexEndpoint = 'https://chatgpt.com/backend-api/codex/responses';
if (!fixtureRoot || !process.env.MASTRA_APP_DATA_DIR || !process.env.MASTRA_DB_PATH) {
  throw new Error('Mastra Code test isolation paths are missing.');
}

const writeMarker = (name, value = 'started') =>
  writeFile(join(fixtureRoot, name), value, { mode: 0o600 });
const blockedNetwork = () => {
  const markerPath = join(fixtureRoot, 'unexpected-network');
  if (!existsSync(markerPath)) {
    writeFileSync(markerPath, new Error('Blocked fixture network call').stack?.slice(-4_096) ?? 'blocked', { mode: 0o600 });
  }
  throw new Error('Network is disabled in the Mastra Code integration fixture.');
};
http.request = blockedNetwork;
http.get = blockedNetwork;
https.request = blockedNetwork;
https.get = blockedNetwork;
net.connect = blockedNetwork;
net.createConnection = blockedNetwork;
tls.connect = blockedNetwork;
globalThis.WebSocket = class { constructor() { blockedNetwork(); } };
syncBuiltinESMExports();

async function exists(filePath) {
  try { await access(filePath); return true; } catch { return false; }
}

async function waitForControl(filePath, signal) {
  if (await exists(filePath)) return;
  if (signal?.aborted) throw new DOMException('Fixture request cancelled.', 'AbortError');
  await new Promise((resolve, reject) => {
    let settled = false;
    const finish = (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      watcher.close();
      signal?.removeEventListener('abort', abort);
      if (error) reject(error); else resolve();
    };
    const abort = () => finish(new DOMException('Fixture request cancelled.', 'AbortError'));
    const watcher = watch(dirname(filePath), (_event, name) => {
      if (name === null || name.toString() === basename(filePath)) {
        void exists(filePath).then(found => { if (found) finish(); });
      }
    });
    watcher.on('error', finish);
    signal?.addEventListener('abort', abort, { once: true });
    const timer = setTimeout(() => finish(new Error('Fixture control event timed out: ' + basename(filePath))), 20_000);
    void exists(filePath).then(found => { if (found) finish(); });
  });
}

const calls = new Map();
let socketAskCallId;
let responseSequence = 0;
function responseEnvelope(sequence, status, output, completed = false) {
  return {
    id: 'resp_' + sequence,
    object: 'response',
    created_at: 1,
    model: 't3-mc-fixture',
    status,
    output,
    error: null,
    incomplete_details: null,
    ...(completed ? {
      usage: {
        input_tokens: 1,
        output_tokens: 1,
        total_tokens: 2,
        input_tokens_details: { cached_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 0 },
      },
    } : {}),
  };
}

function responseStream(events) {
  const frames = events.map((event, index) => {
    const sequencedEvent = { ...event, sequence_number: index + 1 };
    return 'event: ' + event.type + '\\ndata: ' + JSON.stringify(sequencedEvent) + '\\n\\n';
  }).join('');
  return new Response(frames, { headers: { 'content-type': 'text/event-stream' } });
}

function responseForText(text) {
  const sequence = ++responseSequence;
  const item = {
    id: 'msg_' + sequence,
    type: 'message',
    role: 'assistant',
    status: 'in_progress',
    content: [],
  };
  const completedItem = {
    ...item,
    status: 'completed',
    content: [{ type: 'output_text', annotations: [], text }],
  };
  const events = [
    { type: 'response.created', response: responseEnvelope(sequence, 'in_progress', []) },
    { type: 'response.output_item.added', output_index: 0, item },
    { type: 'response.output_text.delta', item_id: item.id, output_index: 0, content_index: 0, delta: text },
    { type: 'response.output_text.done', item_id: item.id, output_index: 0, content_index: 0, text },
    { type: 'response.output_item.done', output_index: 0, item: completedItem },
    { type: 'response.completed', response: responseEnvelope(sequence, 'completed', [completedItem], true) },
  ];
  return responseStream(events);
}

function responseForTool(marker) {
  const sequence = ++responseSequence;
  const toolCall = marker === 'ASK' || marker === 'ASK_SOCKET'
    ? { name: 'ask_user', arguments: { question: 'Choose a test color.', options: [{ label: 'Blue' }, { label: 'Green' }], selectionMode: 'single_select' } }
    : marker === 'PLAN'
    ? { name: 'submit_plan', arguments: { path: join(process.env.MASTRA_PLANS_DIR, 'fixture-plan.md') } }
    : { name: 'request_access', arguments: { path: join(fixtureRoot, marker === 'ALLOW' ? 'allowed-test-path' : 'rejected-test-path'), reason: 'Verify the T3 permission handoff using a temporary fixture path.' } };
  const argumentsJson = JSON.stringify(toolCall.arguments);
  const item = { id: 'fc_' + sequence, type: 'function_call', call_id: 'call_' + sequence, name: toolCall.name, arguments: '', status: 'in_progress' };
  const completedItem = { ...item, arguments: argumentsJson, status: 'completed' };
  if (marker === 'ASK_SOCKET') socketAskCallId = item.call_id;
  const events = [
    { type: 'response.created', response: responseEnvelope(sequence, 'in_progress', []) },
    { type: 'response.output_item.added', output_index: 0, item },
    { type: 'response.function_call_arguments.delta', item_id: item.id, output_index: 0, delta: argumentsJson },
    { type: 'response.function_call_arguments.done', item_id: item.id, output_index: 0, name: toolCall.name, arguments: argumentsJson },
    { type: 'response.output_item.done', output_index: 0, item: completedItem },
    { type: 'response.completed', response: responseEnvelope(sequence, 'completed', [completedItem], true) },
  ];
  return responseStream(events);
}

globalThis.fetch = async (input, init) => {
  const url = new URL(input instanceof Request ? input.url : String(input));
  // The native model catalog reads public gateway metadata even without gateway credentials.
  // Return an empty catalog locally; do not permit a real gateway request or model route.
  if (url.href === 'https://api.netlify.com/api/v1/ai-gateway/providers' || url.href === 'https://models.dev/api.json') {
    const method = init?.method ?? (input instanceof Request ? input.method : 'GET');
    const headers = new Headers(input instanceof Request ? input.headers : undefined);
    new Headers(init?.headers).forEach((value, name) => headers.set(name, value));
    if (method !== 'GET' || headers.has('authorization') || init?.body) return blockedNetwork();
    const netlifyCatalog = url.hostname === 'api.netlify.com';
    await writeMarker(netlifyCatalog ? 'metadata-netlify-catalog' : 'metadata-models-dev-catalog');
    return Response.json(netlifyCatalog ? { providers: {} } : {});
  }
  if (url.href !== codexEndpoint) return blockedNetwork();
  const signal = init?.signal ?? (input instanceof Request ? input.signal : undefined);
  const headers = new Headers(input instanceof Request ? input.headers : undefined);
  new Headers(init?.headers).forEach((value, name) => headers.set(name, value));
  if (headers.get('authorization') !== 'Bearer test-only-synthetic-access-token') {
    throw new Error('The isolated synthetic Codex OAuth credential was not used.');
  }
  const rawBody = input instanceof Request ? await input.clone().text() : String(init?.body ?? '');
  const payload = JSON.parse(rawBody);
  const lastUserMessage = Array.isArray(payload.input)
    ? [...payload.input].reverse().find(item => item?.role === 'user')
    : undefined;
  const searchText = JSON.stringify(lastUserMessage?.content ?? payload.input);
  const marker = ${JSON.stringify(fixtureMarkers)}.find(value => searchText.includes('T3-MC-FIXTURE-' + value));
  if (!marker) throw new Error('Fixture prompt marker was not found.');

  const metadata = {
    appDataDirectory: process.env.MASTRA_APP_DATA_DIR,
    databasePath: process.env.MASTRA_DB_PATH,
    vectorDatabasePath: process.env.MASTRA_VECTOR_DB_PATH,
    observabilityDatabasePath: process.env.MASTRA_OBSERVABILITY_DB_PATH,
    storageBackend: process.env.MASTRA_STORAGE_BACKEND,
    databaseUrl: process.env.MASTRA_DB_URL,
    endpoint: url.pathname,
  };
  await writeFile(join(fixtureRoot, 'paths-' + marker + '.json'), JSON.stringify(metadata), { mode: 0o600 });
  await writeMarker('request-' + marker + '.started');

  if (marker === 'INTERRUPT') {
    if (!signal) throw new Error('The Codex fixture request did not receive an abort signal.');
    await new Promise((resolve, reject) => {
      const abort = () => {
        void writeMarker('interrupt-aborted');
        reject(new DOMException('Fixture request cancelled.', 'AbortError'));
      };
      if (signal.aborted) abort();
      else signal.addEventListener('abort', abort, { once: true });
    });
  }
  if (marker === 'CONCURRENT_A' || marker === 'CONCURRENT_B') {
    await waitForControl(join(fixtureRoot, 'release-concurrent'), signal);
    return responseForText('Concurrent fixture completed.');
  }
  if (marker === 'RESUME') return responseForText('Resumed fixture completed.');
  const count = (calls.get(marker) ?? 0) + 1;
  calls.set(marker, count);
  if (marker === 'ASK_SOCKET' && count > 1) {
    if (count > 3) throw new Error('Unexpected repeated native ASK continuation');
    const outputs = payload.input.filter(item => item.type === 'function_call_output' && item.call_id === socketAskCallId);
    if (!socketAskCallId || outputs.length !== 1) throw new Error('Expected one matching native ASK tool result');
    await writeFile(join(fixtureRoot, 'socket-ask-continuation-' + count + '.json'), JSON.stringify({ callId: socketAskCallId, outputs }), { mode: 0o600, flag: 'wx' });
  }
  if ((marker === 'ALLOW' || marker === 'REJECT' || marker === 'ASK' || marker === 'ASK_SOCKET' || marker === 'PLAN') && count === 1) {
    if (marker === 'PLAN') {
      await writeFile(join(process.env.MASTRA_PLANS_DIR, 'fixture-plan.md'), '# Native fixture plan\\n\\nPreserve the existing conversation.\\n', { mode: 0o600 });
    }
    return responseForTool(marker);
  }
  return responseForText('Fixture completed: ' + marker + '.');
};

await import(${JSON.stringify(`file://${cliPath}`)});
`;
}
