import { watch } from "node:fs";
import { access, readFile, writeFile } from "node:fs/promises";
import { basename, delimiter, dirname, join } from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ApprovalRequestId,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { ServerConfig } from "../../config.ts";
import { buildMastraCodeEnvironment } from "../MastraCodeEnvironment.ts";
import { makeMastraCodeAdapter } from "./MastraCodeAdapter.ts";
import { makeMastraCodeAcpRuntime } from "../acp/MastraCodeAcpSupport.ts";

const fixtureMarkers = [
  "ALLOW",
  "REJECT",
  "ASK",
  "INTERRUPT",
  "RESUME",
  "CONCURRENT_A",
  "CONCURRENT_B",
] as const;

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function waitForFile(filePath: string): Promise<void> {
  if (await fileExists(filePath)) return;
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    let watcher: ReturnType<typeof watch> | undefined;
    let timer: NodeJS.Timeout | undefined;
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      watcher?.close();
      if (error) reject(error);
      else resolve();
    };
    watcher = watch(dirname(filePath), (_event, name) => {
      if (name === null || name.toString() === basename(filePath)) {
        void fileExists(filePath).then((exists) => {
          if (exists) finish();
        });
      }
    });
    watcher.on("error", (error) => finish(error));
    timer = setTimeout(
      () =>
        finish(new Error(`Mastra Code fixture did not reach file event '${basename(filePath)}'.`)),
      20_000,
    );
    void fileExists(filePath).then((exists) => {
      if (exists) finish();
    });
  });
}

function createFetchWrapper(cliPath: string): string {
  return `#!/usr/bin/env node
import { syncBuiltinESMExports } from 'node:module';
import { watch, writeFileSync } from 'node:fs';
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
  writeFileSync(join(fixtureRoot, 'unexpected-network'), 'blocked', { mode: 0o600 });
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
  const toolCall = marker === 'ASK'
    ? { name: 'ask_user', arguments: { question: 'Choose a test color.', options: [{ label: 'Blue' }, { label: 'Green' }], selectionMode: 'single_select' } }
    : { name: 'request_access', arguments: { path: join(fixtureRoot, marker === 'ALLOW' ? 'allowed-test-path' : 'rejected-test-path'), reason: 'Verify the T3 permission handoff using a temporary fixture path.' } };
  const argumentsJson = JSON.stringify(toolCall.arguments);
  const item = { id: 'fc_' + sequence, type: 'function_call', call_id: 'call_' + sequence, name: toolCall.name, arguments: '', status: 'in_progress' };
  const completedItem = { ...item, arguments: argumentsJson, status: 'completed' };
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
  if ((marker === 'ALLOW' || marker === 'REJECT' || marker === 'ASK') && count === 1) {
    return responseForTool(marker);
  }
  return responseForText('Fixture completed: ' + marker + '.');
};

await import(${JSON.stringify(`file://${cliPath}`)});
`;
}

const integrationLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-mastracode-integration-config-",
}).pipe(Layer.provideMerge(NodeServices.layer));

describe("Mastra Code real-process adapter integration", () => {
  it.layer(integrationLayer)("runs the pinned CLI through T3 ACP lifecycle offline", (it) => {
    it.effect(
      "covers permission allow/reject, elicitation, cancel, resume, and concurrent thread storage",
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const testStartedAt = Date.now();
            const reportPhase = (phase: string) => {
              console.info(`[t3-mc-real-process +${Date.now() - testStartedAt}ms] ${phase}`);
            };
            const cliPath = process.env.T3_MASTRA_CODE_CLI;
            if (!cliPath) return yield* Effect.fail(new Error("T3_MASTRA_CODE_CLI is required."));

            reportPhase("create isolated fixture");
            const fileSystem = yield* FileSystem.FileSystem;
            const path = yield* Path.Path;
            const crypto = yield* Crypto.Crypto;
            const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
            const testRoot = yield* fileSystem.makeTempDirectoryScoped({
              prefix: "t3-mastracode-real-acp-",
            });
            const appDataDirectory = path.join(testRoot, "mastra-app-data");
            const homeDirectory = path.join(testRoot, "home");
            const codexHomeDirectory = path.join(homeDirectory, ".codex");
            const fixtureDirectory = path.join(testRoot, "fixture");
            const executableDirectory = path.join(testRoot, "trusted-bin");
            const workspace = path.join(testRoot, "workspace");
            const wrapperPath = path.join(testRoot, "mastracode-acp-fixture.mjs");
            const trustedExecutablePath = path.join(executableDirectory, "mastracode");
            const shadowedExecutablePath = path.join(workspace, "mastracode");
            const shadowedExecutableMarker = path.join(
              fixtureDirectory,
              "relative-path-executable-ran",
            );
            for (const candidate of [
              appDataDirectory,
              homeDirectory,
              codexHomeDirectory,
              fixtureDirectory,
              executableDirectory,
              workspace,
              wrapperPath,
              trustedExecutablePath,
              shadowedExecutablePath,
              shadowedExecutableMarker,
            ]) {
              const relative = path.relative(testRoot, candidate);
              if (relative === "" || relative.startsWith("..") || path.isAbsolute(relative)) {
                return yield* Effect.fail(
                  new Error("Mastra Code fixture path escaped its owned temporary root."),
                );
              }
            }
            for (const directory of [
              appDataDirectory,
              homeDirectory,
              codexHomeDirectory,
              fixtureDirectory,
              executableDirectory,
              workspace,
            ]) {
              yield* fileSystem.makeDirectory(directory, { recursive: true, mode: 0o700 });
            }
            const authFile = path.join(appDataDirectory, "auth.json");
            yield* fileSystem.writeFileString(
              authFile,
              JSON.stringify({
                "openai-codex": {
                  type: "oauth",
                  access: "test-only-synthetic-access-token",
                  refresh: "test-only-synthetic-refresh-token",
                  expires: 4_102_444_800_000,
                  accountId: "test-only-account",
                },
              }),
            );
            yield* fileSystem.chmod(authFile, 0o600);
            const settingsFile = path.join(appDataDirectory, "settings.json");
            yield* fileSystem.writeFileString(
              settingsFile,
              JSON.stringify({
                onboarding: { quietModePreferenceSelected: true },
                observability: { resources: {}, localTracing: true },
              }),
            );
            yield* fileSystem.chmod(settingsFile, 0o600);
            yield* fileSystem.makeDirectory(path.join(workspace, ".mastracode"), {
              recursive: true,
              mode: 0o700,
            });
            yield* fileSystem.writeFileString(
              path.join(workspace, ".mastracode", "database.json"),
              JSON.stringify({
                url: "libsql://must-not-be-used.invalid",
                authToken: "synthetic-fixture-only",
              }),
            );
            yield* fileSystem.writeFileString(wrapperPath, createFetchWrapper(cliPath));
            yield* fileSystem.chmod(wrapperPath, 0o700);
            const trustedLauncher = `#!${process.execPath}
require("node:child_process").execFileSync(
  process.execPath,
  [${JSON.stringify(wrapperPath)}, ...process.argv.slice(2)],
  { stdio: "inherit" },
);
`;
            yield* fileSystem.writeFileString(trustedExecutablePath, trustedLauncher);
            yield* fileSystem.chmod(trustedExecutablePath, 0o700);
            const shadowedLauncher = `#!${process.execPath}
require("node:fs").writeFileSync(
  ${JSON.stringify(shadowedExecutableMarker)},
  "executed",
  { mode: 0o600 },
);
require("node:child_process").execFileSync(
  process.execPath,
  [${JSON.stringify(wrapperPath)}, ...process.argv.slice(2)],
  { stdio: "inherit" },
);
`;
            yield* fileSystem.writeFileString(shadowedExecutablePath, shadowedLauncher);
            yield* fileSystem.chmod(shadowedExecutablePath, 0o700);

            const baseEnvironment = buildMastraCodeEnvironment(
              {
                appDataDirectory,
                homeDirectory,
                codexHomeDirectory,
                databasePath: path.join(appDataDirectory, "unused-main.db"),
                vectorDatabasePath: path.join(appDataDirectory, "unused-vectors.db"),
                observabilityDatabasePath: path.join(
                  appDataDirectory,
                  "unused-observability.duckdb",
                ),
                plansDirectory: path.join(appDataDirectory, "plans"),
                configDirectory: path.join(homeDirectory, ".config"),
                dataDirectory: path.join(homeDirectory, ".local", "share"),
                cacheDirectory: path.join(homeDirectory, ".cache"),
                roamingAppDataDirectory: path.join(homeDirectory, "AppData", "Roaming"),
                localAppDataDirectory: path.join(homeDirectory, "AppData", "Local"),
              },
              undefined,
              {
                PATH: [".", executableDirectory, process.env.PATH].filter(Boolean).join(delimiter),
                TMPDIR: testRoot,
                LANG: "C.UTF-8",
              },
            );
            const environment = {
              ...baseEnvironment,
              T3_MASTRA_CODE_FIXTURE_DIR: fixtureDirectory,
            };
            const eventStreamSubscribed = yield* Queue.unbounded<void>();
            const adapter = yield* makeMastraCodeAdapter(
              { binaryPath: "mastracode" },
              {
                instanceId: ProviderInstanceId.make("mastra-real-process-test"),
                appDataDirectory,
                environment,
                onEventStreamSubscribed: () =>
                  Queue.offer(eventStreamSubscribed, undefined).pipe(Effect.asVoid),
                makeRuntime: (runtimeInput) =>
                  makeMastraCodeAcpRuntime({
                    ...runtimeInput,
                    settings: { binaryPath: "mastracode" },
                    childProcessSpawner,
                  }).pipe(
                    Effect.provideService(Crypto.Crypto, crypto),
                    Effect.provideService(FileSystem.FileSystem, fileSystem),
                    Effect.provideService(Path.Path, path),
                  ),
              },
            );
            const eventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();
            const observedEvents: ProviderRuntimeEvent[] = [];
            const eventListenerFiber = yield* adapter.streamEvents.pipe(
              Stream.runForEach((event) =>
                Effect.sync(() => observedEvents.push(event)).pipe(
                  Effect.andThen(Queue.offer(eventQueue, event)),
                ),
              ),
              Effect.tapCause(() =>
                Effect.logError(
                  "Mastra Code test event listener stopped unexpectedly (cause details redacted).",
                ),
              ),
              Effect.forkScoped({ startImmediately: true }),
            );
            yield* Effect.addFinalizer(() => {
              reportPhase("stop all test adapter sessions");
              return adapter.stopAll().pipe(Effect.ignore);
            });
            const getEventListenerState = () =>
              Fiber.await(eventListenerFiber).pipe(
                Effect.timeoutOption(Duration.millis(1)),
                Effect.map((listenerExit) =>
                  Option.isSome(listenerExit)
                    ? Exit.match(listenerExit.value, {
                        onSuccess: () => "completed successfully",
                        onFailure: () => "failed (cause details redacted)",
                      })
                    : "still running",
                ),
              );
            const subscriptionReady = yield* Queue.take(eventStreamSubscribed).pipe(
              Effect.timeoutOption(Duration.millis(5_000)),
            );
            if (Option.isNone(subscriptionReady)) {
              const listenerState = yield* getEventListenerState();
              return yield* Effect.fail(
                new Error(
                  `Mastra Code event subscription did not become active; listener ${listenerState}.`,
                ),
              );
            }

            const waitForEvent = Effect.fnUntraced(function* <T extends ProviderRuntimeEvent>(
              description: string,
              predicate: (event: ProviderRuntimeEvent) => event is T,
            ) {
              reportPhase(`wait for ${description}`);
              const deadline = performance.now() + 20_000;
              const timeoutError = (listenerState: string) => {
                const observedTypes = [...new Set(observedEvents.map((event) => event.type))];
                const terminalEventTypes = observedEvents
                  .filter(
                    (event) => event.type === "turn.completed" || event.type === "session.exited",
                  )
                  .map((event) => event.type);
                return new Error(
                  `Timed out waiting for ${description}; observed provider event types: ${observedTypes.join(", ") || "none"}; terminal event types: ${terminalEventTypes.join(", ") || "none"}; event listener: ${listenerState}.`,
                );
              };
              while (true) {
                const remainingMs = deadline - performance.now();
                if (remainingMs <= 0) {
                  const listenerState = yield* getEventListenerState();
                  return yield* Effect.fail(timeoutError(listenerState));
                }
                const next = yield* Queue.take(eventQueue).pipe(
                  Effect.timeoutOption(Duration.millis(remainingMs)),
                );
                if (Option.isNone(next)) {
                  const listenerState = yield* getEventListenerState();
                  return yield* Effect.fail(timeoutError(listenerState));
                }
                if (predicate(next.value)) return next.value;
              }
            });
            const startSession = (threadId: string, resumeCursor?: unknown) => {
              reportPhase(
                `start session ${threadId}${resumeCursor === undefined ? "" : " (resume)"}`,
              );
              return adapter.startSession({
                threadId: ThreadId.make(threadId),
                cwd: workspace,
                runtimeMode: "approval-required",
                ...(resumeCursor === undefined ? {} : { resumeCursor }),
              });
            };
            const runTurn = (threadId: string, marker: (typeof fixtureMarkers)[number]) => {
              reportPhase(`send turn ${marker} on ${threadId}`);
              return adapter
                .sendTurn({
                  threadId: ThreadId.make(threadId),
                  input: `T3-MC-FIXTURE-${marker}`,
                })
                .pipe(Effect.forkChild);
            };
            const joinTurn = <A, E>(turn: Fiber.Fiber<A, E>, marker: string) => {
              reportPhase(`wait for turn ${marker} to finish`);
              return Fiber.join(turn);
            };
            const expectAssistantText = (threadId: string, expected: string) => {
              expect(
                observedEvents.some(
                  (event) =>
                    event.type === "content.delta" &&
                    event.threadId === threadId &&
                    event.payload.streamKind === "assistant_text" &&
                    event.payload.delta.includes(expected),
                ),
              ).toBe(true);
            };

            const lifecycleThreadId = "mastra-real-lifecycle";
            const initialSession = yield* startSession(lifecycleThreadId);
            yield* waitForEvent(
              "lifecycle session.started",
              (event): event is Extract<ProviderRuntimeEvent, { type: "session.started" }> =>
                event.type === "session.started" && event.threadId === lifecycleThreadId,
            );

            const allowTurn = yield* runTurn(lifecycleThreadId, "ALLOW");
            const allowRequest = yield* waitForEvent(
              "ALLOW request.opened",
              (event): event is Extract<ProviderRuntimeEvent, { type: "request.opened" }> =>
                event.type === "request.opened" && event.threadId === lifecycleThreadId,
            );
            if (allowRequest.requestId === undefined)
              return yield* Effect.fail(new Error("Allow fixture request had no ID."));
            reportPhase("respond to ALLOW permission");
            yield* adapter.respondToRequest(
              ThreadId.make(lifecycleThreadId),
              ApprovalRequestId.make(allowRequest.requestId),
              "accept",
            );
            const allowResolved = yield* waitForEvent(
              "ALLOW request.resolved",
              (event): event is Extract<ProviderRuntimeEvent, { type: "request.resolved" }> =>
                event.type === "request.resolved" && event.requestId === allowRequest.requestId,
            );
            expect(allowResolved.payload.decision).toBe("accept");
            yield* joinTurn(allowTurn, "ALLOW");
            expectAssistantText(lifecycleThreadId, "Fixture completed: ALLOW.");

            const rejectTurn = yield* runTurn(lifecycleThreadId, "REJECT");
            const rejectRequest = yield* waitForEvent(
              "REJECT request.opened",
              (event): event is Extract<ProviderRuntimeEvent, { type: "request.opened" }> =>
                event.type === "request.opened" && event.threadId === lifecycleThreadId,
            );
            if (rejectRequest.requestId === undefined)
              return yield* Effect.fail(new Error("Reject fixture request had no ID."));
            reportPhase("respond to REJECT permission");
            yield* adapter.respondToRequest(
              ThreadId.make(lifecycleThreadId),
              ApprovalRequestId.make(rejectRequest.requestId),
              "decline",
            );
            const rejectResolved = yield* waitForEvent(
              "REJECT request.resolved",
              (event): event is Extract<ProviderRuntimeEvent, { type: "request.resolved" }> =>
                event.type === "request.resolved" && event.requestId === rejectRequest.requestId,
            );
            expect(rejectResolved.payload.decision).toBe("decline");
            yield* joinTurn(rejectTurn, "REJECT");
            expectAssistantText(lifecycleThreadId, "Fixture completed: REJECT.");

            const askTurn = yield* runTurn(lifecycleThreadId, "ASK");
            const question = yield* waitForEvent(
              "ASK user-input.requested",
              (event): event is Extract<ProviderRuntimeEvent, { type: "user-input.requested" }> =>
                event.type === "user-input.requested" && event.threadId === lifecycleThreadId,
            );
            if (question.requestId === undefined)
              return yield* Effect.fail(new Error("Question fixture request had no ID."));
            reportPhase("respond to ASK user input");
            yield* adapter.respondToUserInput(
              ThreadId.make(lifecycleThreadId),
              ApprovalRequestId.make(question.requestId),
              { answer: "Blue" },
            );
            const answerResolved = yield* waitForEvent(
              "ASK user-input.resolved",
              (event): event is Extract<ProviderRuntimeEvent, { type: "user-input.resolved" }> =>
                event.type === "user-input.resolved" && event.requestId === question.requestId,
            );
            expect(answerResolved.payload.answers).toEqual({ answer: "Blue" });
            yield* joinTurn(askTurn, "ASK");
            expectAssistantText(lifecycleThreadId, "Fixture completed: ASK.");

            const interruptTurn = yield* runTurn(lifecycleThreadId, "INTERRUPT");
            reportPhase("wait for INTERRUPT request fixture");
            yield* Effect.promise(() =>
              waitForFile(join(fixtureDirectory, "request-INTERRUPT.started")),
            );
            reportPhase("interrupt active turn");
            yield* adapter.interruptTurn(ThreadId.make(lifecycleThreadId));
            const interrupted = yield* waitForEvent(
              "INTERRUPT cancelled turn.completed",
              (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
                event.type === "turn.completed" &&
                event.threadId === lifecycleThreadId &&
                event.payload.state === "cancelled",
            );
            expect(interrupted.payload.state).toBe("cancelled");
            reportPhase("wait for aborted Codex fixture request");
            yield* Effect.promise(() => waitForFile(join(fixtureDirectory, "interrupt-aborted")));
            yield* joinTurn(interruptTurn, "INTERRUPT");

            reportPhase("stop lifecycle session");
            yield* adapter.stopSession(ThreadId.make(lifecycleThreadId));
            yield* waitForEvent(
              "lifecycle session.exited",
              (event): event is Extract<ProviderRuntimeEvent, { type: "session.exited" }> =>
                event.type === "session.exited" && event.threadId === lifecycleThreadId,
            );
            const resumedSession = yield* startSession(
              lifecycleThreadId,
              initialSession.resumeCursor,
            );
            expect(resumedSession.resumeCursor).toEqual(initialSession.resumeCursor);
            const resumeTurn = yield* runTurn(lifecycleThreadId, "RESUME");
            yield* joinTurn(resumeTurn, "RESUME");
            expectAssistantText(lifecycleThreadId, "Resumed fixture completed.");

            const threadA = "mastra-concurrent-a";
            const threadB = "mastra-concurrent-b";
            yield* startSession(threadA);
            yield* startSession(threadB);
            const turnA = yield* runTurn(threadA, "CONCURRENT_A");
            const turnB = yield* runTurn(threadB, "CONCURRENT_B");
            reportPhase("wait for both concurrent request fixtures");
            yield* Effect.all([
              Effect.promise(() =>
                waitForFile(join(fixtureDirectory, "request-CONCURRENT_A.started")),
              ),
              Effect.promise(() =>
                waitForFile(join(fixtureDirectory, "request-CONCURRENT_B.started")),
              ),
            ]);
            const metadataA = JSON.parse(
              yield* Effect.promise(() =>
                readFile(join(fixtureDirectory, "paths-CONCURRENT_A.json"), "utf8"),
              ),
            ) as Record<string, string>;
            const metadataB = JSON.parse(
              yield* Effect.promise(() =>
                readFile(join(fixtureDirectory, "paths-CONCURRENT_B.json"), "utf8"),
              ),
            ) as Record<string, string>;
            expect(metadataA.appDataDirectory).toBe(appDataDirectory);
            expect(metadataB.appDataDirectory).toBe(appDataDirectory);
            expect(metadataA.endpoint).toBe("/backend-api/codex/responses");
            expect(metadataB.endpoint).toBe("/backend-api/codex/responses");
            expect(metadataA.databasePath).not.toBe(metadataB.databasePath);
            expect(metadataA.vectorDatabasePath).not.toBe(metadataB.vectorDatabasePath);
            expect(metadataA.observabilityDatabasePath).not.toBe(
              metadataB.observabilityDatabasePath,
            );
            for (const value of [metadataA, metadataB]) {
              expect(value.storageBackend).toBe("libsql");
              expect(value.databaseUrl).toBe(`file:${value.databasePath}`);
              expect(value.databaseUrl).not.toContain("must-not-be-used.invalid");
              for (const key of [
                "databasePath",
                "vectorDatabasePath",
                "observabilityDatabasePath",
              ] as const) {
                expect(path.relative(testRoot, value[key]!).startsWith("..")).toBe(false);
              }
              expect(yield* Effect.promise(() => fileExists(value.databasePath!))).toBe(true);
              expect(
                yield* Effect.promise(() => fileExists(value.observabilityDatabasePath!)),
              ).toBe(true);
            }
            reportPhase("release both concurrent requests");
            yield* Effect.promise(() =>
              writeFile(join(fixtureDirectory, "release-concurrent"), "release", { mode: 0o600 }),
            );
            yield* Effect.all([joinTurn(turnA, "CONCURRENT_A"), joinTurn(turnB, "CONCURRENT_B")]);
            expectAssistantText(threadA, "Concurrent fixture completed.");
            expectAssistantText(threadB, "Concurrent fixture completed.");
            reportPhase("stop concurrent sessions");
            yield* adapter.stopSession(ThreadId.make(threadA));
            yield* adapter.stopSession(ThreadId.make(threadB));

            reportPhase("check no unexpected network and confirm cancellation fixture");
            expect(
              yield* Effect.promise(() => fileExists(join(fixtureDirectory, "unexpected-network"))),
            ).toBe(false);
            expect(
              yield* Effect.promise(() => fileExists(join(fixtureDirectory, "interrupt-aborted"))),
            ).toBe(true);
            expect(yield* Effect.promise(() => fileExists(shadowedExecutableMarker))).toBe(false);
          }),
        ).pipe(TestClock.withLive),
      { timeout: 180_000 },
    );
  });
});
