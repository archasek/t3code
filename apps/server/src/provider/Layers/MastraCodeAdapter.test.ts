import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  ApprovalRequestId,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import { ServerConfig } from "../../config.ts";
import {
  type MastraCodeAdapterOptions,
  mapMastraCodeAcpError,
  makeMastraCodeAdapter,
  mastraCodeAcpSessionMatches,
  mastraCodeSessionCanStartTurn,
} from "./MastraCodeAdapter.ts";

type Runtime = Effect.Success<ReturnType<MastraCodeAdapterOptions["makeRuntime"]>>;

const makeHarness = Effect.fn("makeMastraCodeAdapterTestHarness")(function* (
  options: {
    failCancel?: boolean;
    holdModeState?: boolean;
    holdPromptDispatch?: boolean;
    holdCancel?: boolean;
    holdSecondPrompt?: boolean;
    modeState?: {
      currentModeId: string;
      availableModes: ReadonlyArray<{ id: string; name: string; description?: string }>;
    };
    availableCommands?: ReadonlyArray<EffectAcpSchema.AvailableCommand>;
    onAvailableCommands?: (
      commands: ReadonlyArray<EffectAcpSchema.AvailableCommand>,
      cwd: string,
    ) => Effect.Effect<void>;
  } = {},
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const appDataDirectory = yield* fileSystem.makeTempDirectoryScoped({
    prefix: "t3-mastracode-adapter-test-",
  });
  const promptStarted = yield* Deferred.make<void>();
  const promptResponse = yield* Deferred.make<EffectAcpSchema.PromptResponse>();
  const secondPromptStarted = yield* Deferred.make<void>();
  const secondPromptResponse = yield* Deferred.make<EffectAcpSchema.PromptResponse>();
  const cancelCalled = yield* Deferred.make<void>();
  const releaseCancel = yield* Deferred.make<void>();
  const modeStateStarted = yield* Deferred.make<void>();
  const releaseModeState = yield* Deferred.make<void>();
  const promptDispatchStarted = yield* Deferred.make<void>();
  const releasePromptDispatch = yield* Deferred.make<void>();
  const permissionOpened = yield* Deferred.make<void>();
  const elicitationRequested = yield* Deferred.make<void>();
  const secondTurnStarted = yield* Deferred.make<void>();
  const sessionExited = yield* Deferred.make<void>();
  const runtimeScopeClosed = yield* Deferred.make<void>();
  const events: ProviderRuntimeEvent[] = [];
  const modeCalls: string[] = [];
  let turnStartedCount = 0;
  let promptCallCount = 0;
  let cancelCallCount = 0;
  let runtimeFactoryCount = 0;
  let permissionHandler: Parameters<Runtime["handleRequestPermission"]>[0] | undefined;
  let elicitationHandler: Parameters<Runtime["handleElicitation"]>[0] | undefined;

  const runtime: Runtime = {
    handleRequestPermission: (handler) =>
      Effect.sync(() => {
        permissionHandler = handler;
      }),
    handleElicitation: (handler) =>
      Effect.sync(() => {
        elicitationHandler = handler;
      }),
    handleSessionUpdate: () => Effect.void,
    start: () =>
      Effect.succeed({
        sessionId: "mastra-acp-session",
        initializeResult: {
          protocolVersion: 1,
          agentCapabilities: { sessionCapabilities: { resume: {} } },
        },
        sessionSetupResult: { sessionId: "mastra-acp-session" },
        modelConfigId: undefined,
      } as never),
    getModeState: options.holdModeState
      ? Effect.gen(function* () {
          yield* Deferred.succeed(modeStateStarted, undefined);
          yield* Deferred.await(releaseModeState);
          return undefined;
        })
      : Effect.succeed(options.modeState),
    getEvents: () =>
      Stream.fromIterable(
        options.availableCommands
          ? [
              {
                _tag: "AvailableCommandsUpdated",
                availableCommands: options.availableCommands,
              } as never,
            ]
          : [],
      ),
    drainEvents: Effect.void,
    prompt: (_input, promptOptions) =>
      Effect.gen(function* () {
        promptCallCount += 1;
        if (promptOptions?.dispatched) {
          yield* Deferred.succeed(promptDispatchStarted, undefined);
          if (options.holdPromptDispatch) {
            yield* Deferred.await(releasePromptDispatch);
          }
          yield* Deferred.succeed(promptOptions.dispatched, undefined);
        }
        if (promptCallCount === 2) {
          yield* Deferred.succeed(secondPromptStarted, undefined);
          if (options.holdSecondPrompt) {
            return yield* Deferred.await(secondPromptResponse);
          }
        }
        yield* Deferred.succeed(promptStarted, undefined);
        return yield* Deferred.await(promptResponse);
      }),
    cancel: Effect.gen(function* () {
      cancelCallCount += 1;
      yield* Deferred.succeed(cancelCalled, undefined);
      if (options.holdCancel) yield* Deferred.await(releaseCancel);
      if (options.failCancel) {
        return yield* Effect.fail(
          new EffectAcpErrors.AcpProcessExitedError({ code: 1, stderr: "token=must-not-leak" }),
        );
      }
      yield* Deferred.succeed(promptResponse, { stopReason: "cancelled" });
    }),
    setMode: (modeId) =>
      Effect.sync(() => {
        modeCalls.push(modeId);
        return {} as never;
      }),
    setSessionModel: () => Effect.succeed({} as never),
  };

  const adapter = yield* makeMastraCodeAdapter(
    { binaryPath: "mastracode" },
    {
      instanceId: ProviderInstanceId.make("mastra-adapter-test"),
      appDataDirectory,
      environment: { PATH: process.env.PATH },
      ...(options.onAvailableCommands ? { onAvailableCommands: options.onAvailableCommands } : {}),
      makeRuntime: () =>
        Effect.gen(function* () {
          runtimeFactoryCount += 1;
          yield* Effect.addFinalizer(() => Deferred.succeed(runtimeScopeClosed, undefined));
          return runtime;
        }),
    },
  );
  yield* adapter.streamEvents.pipe(
    Stream.runForEach((event) =>
      Effect.gen(function* () {
        events.push(event);
        if (event.type === "turn.started") {
          turnStartedCount += 1;
          if (turnStartedCount === 2) {
            yield* Deferred.succeed(secondTurnStarted, undefined);
          }
        }
        if (event.type === "request.opened") {
          yield* Deferred.succeed(permissionOpened, undefined);
        }
        if (event.type === "user-input.requested") {
          yield* Deferred.succeed(elicitationRequested, undefined);
        }
        if (event.type === "session.exited") {
          yield* Deferred.succeed(sessionExited, undefined);
        }
      }),
    ),
    Effect.forkScoped({ startImmediately: true }),
  );

  return {
    adapter,
    events,
    promptResponse,
    promptStarted,
    secondPromptStarted,
    secondPromptResponse,
    cancelCalled,
    modeStateStarted,
    releaseModeState,
    promptDispatchStarted,
    releasePromptDispatch,
    releaseCancel,
    secondTurnStarted,
    permissionOpened,
    elicitationRequested,
    sessionExited,
    runtimeScopeClosed,
    promptCallCount: () => promptCallCount,
    cancelCallCount: () => cancelCallCount,
    permissionHandler: () => permissionHandler,
    elicitationHandler: () => elicitationHandler,
    runtimeFactoryCount: () => runtimeFactoryCount,
    modeCalls: () => modeCalls,
  };
});

const adapterTestLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-mastracode-adapter-test-config-",
}).pipe(Layer.provideMerge(NodeServices.layer));

it.layer(adapterTestLayer)("MastraCodeAdapter", (it) => {
  it.effect("cancels permission requests from a different ACP session", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const threadId = ThreadId.make("mastra-stale-permission");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });
        const handler = harness.permissionHandler();
        expect(handler).toBeDefined();
        const response = yield* handler!({
          sessionId: "stale-acp-session",
          toolCall: { toolCallId: "tool-1", kind: "execute", title: "run command" },
          options: [
            { optionId: "allow", name: "Allow", kind: "allow_once" },
            { optionId: "reject", name: "Reject", kind: "reject_once" },
          ],
        });

        expect(response).toEqual({ outcome: { outcome: "cancelled" } });
        yield* harness.adapter.stopSession(threadId);
      }),
    ),
  );

  it.effect("requires an explicit decision to submit a plan in full-access mode", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const threadId = ThreadId.make("mastra-plan-approval");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });
        const handler = harness.permissionHandler();
        expect(handler).toBeDefined();

        const permission = yield* handler!({
          sessionId: "mastra-acp-session",
          toolCall: {
            toolCallId: "plan-1",
            kind: "execute",
            title: "submit_plan",
            rawInput: {},
          },
          options: [
            { optionId: "allow", name: "Allow", kind: "allow_once" },
            { optionId: "reject", name: "Reject", kind: "reject_once" },
          ],
        }).pipe(Effect.forkChild({ startImmediately: true }));
        yield* Effect.yieldNow;
        expect(permission.pollUnsafe()).toBeUndefined();
        yield* Deferred.await(harness.permissionOpened);

        yield* harness.adapter.stopSession(threadId);
        const response = yield* Fiber.join(permission);
        expect(response).toEqual({ outcome: { outcome: "cancelled" } });
        yield* Deferred.await(harness.sessionExited);
      }),
    ),
  );

  it.effect("does not publish plan contents through a symlinked project plan root", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "mastra-plan-root-" });
        const project = path.join(root, "project");
        const outside = path.join(root, "outside");
        yield* fileSystem.makeDirectory(path.join(project, ".mastracode"), { recursive: true });
        yield* fileSystem.makeDirectory(outside, { recursive: true });
        yield* fileSystem.writeFileString(
          path.join(outside, "plan.md"),
          "# secret external plan\n",
        );
        yield* fileSystem.symlink(outside, path.join(project, ".mastracode", "plans"));

        {
          const harness = yield* makeHarness();
          const threadId = ThreadId.make("mastra-plan-symlink-root");
          yield* harness.adapter.startSession({
            threadId,
            cwd: project,
            runtimeMode: "approval-required",
          });
          const handler = harness.permissionHandler();
          expect(handler).toBeDefined();
          if (!handler) return;

          const permission = yield* handler({
            sessionId: "mastra-acp-session",
            toolCall: {
              toolCallId: "external-plan",
              kind: "execute",
              title: "submit_plan",
              rawInput: { path: path.join(project, ".mastracode", "plans", "plan.md") },
            },
            options: [
              { optionId: "allow", name: "Allow", kind: "allow_once" },
              { optionId: "reject", name: "Reject", kind: "reject_once" },
            ],
          }).pipe(Effect.forkChild({ startImmediately: true }));
          yield* Deferred.await(harness.permissionOpened);
          expect(harness.events.some((event) => event.type === "turn.proposed.completed")).toBe(
            false,
          );

          yield* harness.adapter.stopSession(threadId);
          expect((yield* Fiber.join(permission)).outcome.outcome).toBe("cancelled");
        }
      }),
    ),
  );

  it.effect("fails closed when plan mode is unavailable", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const threadId = ThreadId.make("mastra-plan-mode-unavailable");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });

        const result = yield* harness.adapter
          .sendTurn({ threadId, input: "plan this change", interactionMode: "plan" })
          .pipe(Effect.exit);

        expect(Exit.isFailure(result)).toBe(true);
        expect(harness.promptCallCount()).toBe(0);
        expect(harness.events.find((event) => event.type === "turn.completed")?.payload.state).toBe(
          "failed",
        );
        yield* harness.adapter.stopSession(threadId);
      }),
    ),
  );

  it.effect("rejects a Plan label when the ACP mode ID is not plan", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          modeState: {
            currentModeId: "build",
            availableModes: [{ id: "architect", name: "Plan / Architect" }],
          },
        });
        const threadId = ThreadId.make("mastra-plan-mode-canonical-id");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });

        const result = yield* harness.adapter
          .sendTurn({ threadId, input: "plan this change", interactionMode: "plan" })
          .pipe(Effect.exit);

        expect(Exit.isFailure(result)).toBe(true);
        expect(harness.modeCalls()).toEqual([]);
        expect(harness.promptCallCount()).toBe(0);
        yield* harness.adapter.stopSession(threadId);
      }),
    ),
  );

  it.effect("selects the canonical plan mode ID", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          modeState: {
            currentModeId: "build",
            availableModes: [{ id: "plan", name: "Plan" }],
          },
        });
        const threadId = ThreadId.make("mastra-plan-mode-canonical-select");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });
        const turn = yield* harness.adapter
          .sendTurn({ threadId, input: "plan this change", interactionMode: "plan" })
          .pipe(Effect.forkChild({ startImmediately: true }));

        yield* Deferred.await(harness.promptStarted);
        expect(harness.modeCalls()).toEqual(["plan"]);
        yield* Deferred.succeed(harness.promptResponse, { stopReason: "end_turn" });
        expect(Exit.isSuccess(yield* Fiber.join(turn).pipe(Effect.exit))).toBe(true);
        yield* harness.adapter.stopSession(threadId);
      }),
    ),
  );

  it.effect("forwards ACP command catalogs with the active workspace", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const received = yield* Deferred.make<{
          readonly commands: ReadonlyArray<EffectAcpSchema.AvailableCommand>;
          readonly cwd: string;
        }>();
        const commands = [{ name: "review", description: "Review changes" }];
        const harness = yield* makeHarness({
          availableCommands: commands,
          onAvailableCommands: (nextCommands, cwd) =>
            Deferred.succeed(received, { commands: nextCommands, cwd }),
        });
        const threadId = ThreadId.make("mastra-command-catalog");
        yield* harness.adapter.startSession({
          threadId,
          cwd: "/workspace/catalog",
          runtimeMode: "approval-required",
        });

        expect(yield* Deferred.await(received)).toEqual({
          commands,
          cwd: "/workspace/catalog",
        });
        yield* harness.adapter.stopSession(threadId);
      }),
    ),
  );

  it.effect("does not widen a one-time approval to allow-always", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const threadId = ThreadId.make("mastra-approval-scope");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
        });
        const handler = harness.permissionHandler();
        expect(handler).toBeDefined();

        const permission = yield* handler!({
          sessionId: "mastra-acp-session",
          toolCall: { toolCallId: "tool-scope", kind: "execute", title: "run command" },
          options: [
            { optionId: "always", name: "Allow always", kind: "allow_always" },
            { optionId: "reject", name: "Reject", kind: "reject_once" },
          ],
        }).pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(harness.permissionOpened);

        const opened = harness.events.find(
          (event): event is Extract<ProviderRuntimeEvent, { type: "request.opened" }> =>
            event.type === "request.opened" && event.threadId === threadId,
        );
        expect(opened).toBeDefined();
        if (!opened) return;
        expect(opened.payload.options).toEqual([
          { decision: "acceptForSession", label: "Always allow this session" },
          { decision: "decline", label: "Deny" },
          { decision: "cancel", label: "Cancel" },
        ]);

        yield* harness.adapter.respondToRequest(
          threadId,
          ApprovalRequestId.make(String(opened.requestId)),
          "accept",
        );
        expect(yield* Fiber.join(permission)).toEqual({
          outcome: { outcome: "cancelled" },
        });
        yield* harness.adapter.stopSession(threadId);
      }),
    ),
  );

  it.effect("does not dispatch or cancel a prompt when stop wins before prompt dispatch", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness({ holdModeState: true });
        const threadId = ThreadId.make("mastra-stop-before-prompt");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });

        const turn = yield* harness.adapter
          .sendTurn({ threadId, input: "must not be sent after stop" })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(harness.modeStateStarted);
        const stop = yield* harness.adapter
          .stopSession(threadId)
          .pipe(Effect.forkChild({ startImmediately: true }));
        expect(yield* harness.adapter.hasSession(threadId)).toBe(false);

        yield* Deferred.succeed(harness.releaseModeState, undefined);
        const turnResult = yield* Fiber.join(turn).pipe(Effect.exit);
        yield* Fiber.join(stop);
        yield* Deferred.await(harness.sessionExited);

        expect(Exit.isSuccess(turnResult)).toBe(true);
        expect(harness.promptCallCount()).toBe(0);
        expect(harness.cancelCallCount()).toBe(0);
        const completed = harness.events.find((event) => event.type === "turn.completed");
        expect(completed?.type === "turn.completed" ? completed.payload.state : undefined).toBe(
          "cancelled",
        );
        const turnCompletedIndex = harness.events.findIndex(
          (event) => event.type === "turn.completed",
        );
        const sessionExitedIndex = harness.events.findIndex(
          (event) => event.type === "session.exited",
        );
        expect(turnCompletedIndex).toBeGreaterThanOrEqual(0);
        expect(sessionExitedIndex).toBeGreaterThan(turnCompletedIndex);
      }),
    ),
  );

  it.effect("serializes stop with ACP prompt registration and cancels it exactly once", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness({ holdPromptDispatch: true });
        const threadId = ThreadId.make("mastra-stop-during-prompt-dispatch");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });

        const turn = yield* harness.adapter
          .sendTurn({ threadId, input: "stop while ACP registers the prompt" })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(harness.promptDispatchStarted);
        const stop = yield* harness.adapter
          .stopSession(threadId)
          .pipe(Effect.forkChild({ startImmediately: true }));
        expect(stop.pollUnsafe()).toBeUndefined();
        expect(yield* harness.adapter.hasSession(threadId)).toBe(true);

        yield* Deferred.succeed(harness.releasePromptDispatch, undefined);
        yield* Deferred.await(harness.cancelCalled);
        const turnResult = yield* Fiber.join(turn).pipe(Effect.exit);
        yield* Fiber.join(stop);
        yield* Deferred.await(harness.sessionExited);

        expect(Exit.isSuccess(turnResult)).toBe(true);
        expect(harness.promptCallCount()).toBe(1);
        expect(harness.cancelCallCount()).toBe(1);
        const turnCompletedIndex = harness.events.findIndex(
          (event) => event.type === "turn.completed",
        );
        const sessionExitedIndex = harness.events.findIndex(
          (event) => event.type === "session.exited",
        );
        expect(turnCompletedIndex).toBeGreaterThanOrEqual(0);
        expect(sessionExitedIndex).toBeGreaterThan(turnCompletedIndex);
      }),
    ),
  );

  it.effect("does not let a delayed interrupt cancel the next turn", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness({ holdCancel: true });
        const threadId = ThreadId.make("mastra-interrupt-next-turn-race");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });

        const firstTurn = yield* harness.adapter
          .sendTurn({ threadId, input: "first prompt ends naturally" })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(harness.promptStarted);
        const secondTurn = yield* harness.adapter
          .sendTurn({ threadId, input: "next prompt must not be cancelled" })
          .pipe(Effect.forkChild({ startImmediately: true }));
        const interrupt = yield* harness.adapter
          .interruptTurn(threadId)
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(harness.cancelCalled);

        yield* Deferred.succeed(harness.promptResponse, { stopReason: "end_turn" });
        const firstResult = yield* Fiber.join(firstTurn).pipe(Effect.exit);
        yield* Deferred.await(harness.secondTurnStarted);
        yield* Effect.yieldNow;
        expect(Exit.isSuccess(firstResult)).toBe(true);
        expect(harness.promptCallCount()).toBe(1);

        yield* Deferred.succeed(harness.releaseCancel, undefined);
        yield* Fiber.join(interrupt);
        const secondResult = yield* Fiber.join(secondTurn).pipe(Effect.exit);
        yield* harness.adapter.stopSession(threadId);
        yield* Deferred.await(harness.sessionExited);

        expect(Exit.isSuccess(secondResult)).toBe(true);
        expect(harness.promptCallCount()).toBe(2);
        expect(harness.cancelCallCount()).toBe(1);
      }),
    ),
  );

  it.effect("ignores a delayed interrupt targeting a completed earlier turn", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness({ holdSecondPrompt: true });
        const threadId = ThreadId.make("mastra-stale-turn-interrupt");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });

        const firstTurn = yield* harness.adapter
          .sendTurn({ threadId, input: "first turn completes" })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(harness.promptStarted);
        const firstStarted = harness.events.find((event) => event.type === "turn.started");
        if (firstStarted?.type !== "turn.started") {
          throw new Error("first Mastra Code turn did not emit turn.started");
        }
        yield* Deferred.succeed(harness.promptResponse, { stopReason: "end_turn" });
        const firstResult = yield* Fiber.join(firstTurn).pipe(Effect.exit);
        expect(Exit.isSuccess(firstResult)).toBe(true);

        const secondTurn = yield* harness.adapter
          .sendTurn({ threadId, input: "second turn stays active" })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(harness.secondPromptStarted);
        yield* harness.adapter.interruptTurn(threadId, firstStarted.turnId);
        expect(harness.cancelCallCount()).toBe(0);

        yield* Deferred.succeed(harness.secondPromptResponse, { stopReason: "end_turn" });
        const secondResult = yield* Fiber.join(secondTurn).pipe(Effect.exit);
        yield* harness.adapter.stopSession(threadId);
        yield* Deferred.await(harness.sessionExited);

        expect(Exit.isSuccess(secondResult)).toBe(true);
        expect(harness.promptCallCount()).toBe(2);
        expect(harness.cancelCallCount()).toBe(0);
      }),
    ),
  );

  it.effect("surfaces a safe cancel failure and still completes session cleanup", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness({ failCancel: true });
        const threadId = ThreadId.make("mastra-cancel-failure");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });

        const turn = yield* harness.adapter
          .sendTurn({ threadId, input: "active prompt with a failing cancel" })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(harness.promptStarted);
        const stop = yield* harness.adapter
          .stopSession(threadId)
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(harness.cancelCalled);

        const turnResult = yield* Fiber.join(turn).pipe(Effect.exit);
        yield* Fiber.join(stop);
        yield* Deferred.await(harness.sessionExited);

        expect(Exit.isFailure(turnResult)).toBe(true);
        expect(harness.cancelCallCount()).toBe(1);
        const terminalEvents = harness.events.filter((event) => event.type === "turn.completed");
        expect(terminalEvents).toHaveLength(1);
        if (terminalEvents[0]?.type === "turn.completed") {
          expect(terminalEvents[0].payload.state).toBe("failed");
          expect(terminalEvents[0].payload.errorMessage ?? "").not.toContain("must-not-leak");
        }
        expect(harness.events.some((event) => event.type === "session.exited")).toBe(true);
      }),
    ),
  );

  it.effect("does not reuse an ACP session after cancellation fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness({ failCancel: true });
        const threadId = ThreadId.make("mastra-cancel-failure-no-reuse");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });

        const firstTurn = yield* harness.adapter
          .sendTurn({ threadId, input: "cancel this active prompt" })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(harness.promptStarted);
        const queuedTurn = yield* harness.adapter
          .sendTurn({ threadId, input: "must not overlap the unresolved prompt" })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Effect.yieldNow;
        const interrupt = yield* harness.adapter
          .interruptTurn(threadId)
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(harness.cancelCalled);

        const interruptResult = yield* Fiber.join(interrupt).pipe(Effect.exit);
        yield* Deferred.await(harness.runtimeScopeClosed);
        const firstResult = yield* Fiber.join(firstTurn).pipe(Effect.exit);
        const secondResult = yield* Fiber.join(queuedTurn).pipe(Effect.exit);
        yield* Deferred.await(harness.sessionExited);
        const firstStarted = harness.events.find((event) => event.type === "turn.started");
        const firstTurnTerminalEvents = harness.events.filter(
          (event) =>
            event.type === "turn.completed" &&
            firstStarted?.type === "turn.started" &&
            event.turnId === firstStarted.turnId,
        );

        expect(Exit.isFailure(interruptResult)).toBe(true);
        expect(Exit.isFailure(firstResult)).toBe(true);
        expect(Exit.isFailure(secondResult)).toBe(true);
        expect(harness.promptCallCount()).toBe(1);
        expect(harness.events.filter((event) => event.type === "turn.started")).toHaveLength(1);
        expect(firstTurnTerminalEvents).toHaveLength(1);
        expect(yield* harness.adapter.hasSession(threadId)).toBe(false);
        const exitedEvents = harness.events.filter((event) => event.type === "session.exited");
        expect(exitedEvents).toHaveLength(1);
        if (exitedEvents[0]?.type === "session.exited") {
          expect(exitedEvents[0].payload.exitKind).toBe("error");
        }
      }),
    ),
  );

  it.effect("reuses a matching live ACP session without restarting it", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const threadId = ThreadId.make("mastra-resume-live-session");
        const original = yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });

        const resumed = yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
          resumeCursor: { schemaVersion: 1, sessionId: "mastra-acp-session" },
        });

        expect(resumed).toEqual(original);
        expect(harness.runtimeFactoryCount()).toBe(1);
        expect(harness.events.some((event) => event.type === "session.exited")).toBe(false);

        yield* harness.adapter.stopSession(threadId);
        yield* Deferred.await(harness.sessionExited);
      }),
    ),
  );

  it.effect("rejects a live resume when runtime mode changes and keeps the active session", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const threadId = ThreadId.make("mastra-resume-live-session-mode-mismatch");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });

        const rejectedResume = yield* harness.adapter
          .startSession({
            threadId,
            cwd: process.cwd(),
            runtimeMode: "approval-required",
            resumeCursor: { schemaVersion: 1, sessionId: "mastra-acp-session" },
          })
          .pipe(Effect.exit);

        expect(Exit.isFailure(rejectedResume)).toBe(true);
        expect(yield* harness.adapter.hasSession(threadId)).toBe(true);
        expect(harness.runtimeFactoryCount()).toBe(1);
        expect(harness.events.some((event) => event.type === "session.exited")).toBe(false);

        yield* harness.adapter.stopSession(threadId);
        yield* Deferred.await(harness.sessionExited);
      }),
    ),
  );

  it.effect("rejects a missing resume target without replacing the live session", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const threadId = ThreadId.make("mastra-invalid-resume-cursor");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });

        const malformedResume = yield* harness.adapter
          .startSession({
            threadId,
            cwd: process.cwd(),
            runtimeMode: "full-access",
            resumeCursor: { schemaVersion: 2, sessionId: "wrong-version" },
          })
          .pipe(Effect.exit);
        const rejectedResume = yield* harness.adapter
          .startSession({
            threadId,
            cwd: process.cwd(),
            runtimeMode: "full-access",
            resumeCursor: { schemaVersion: 1, sessionId: "missing-acp-session" },
          })
          .pipe(Effect.exit);

        expect(Exit.isFailure(malformedResume)).toBe(true);
        expect(Exit.isFailure(rejectedResume)).toBe(true);
        expect(yield* harness.adapter.hasSession(threadId)).toBe(true);
        expect(harness.runtimeFactoryCount()).toBe(1);
        expect(harness.events.some((event) => event.type === "session.exited")).toBe(false);

        yield* harness.adapter.stopSession(threadId);
        yield* Deferred.await(harness.sessionExited);
      }),
    ),
  );

  it.effect("cancels elicitation requests from a different ACP session", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const threadId = ThreadId.make("mastra-stale-elicitation");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });
        const handler = harness.elicitationHandler();
        expect(handler).toBeDefined();

        const response = yield* handler!({
          mode: "form",
          sessionId: "stale-acp-session",
          message: "This question belongs to a different session.",
          requestedSchema: {
            type: "object",
            properties: { approved: { type: "boolean" } },
          },
        });

        expect(response).toEqual({ action: { action: "cancel" } });
        expect(harness.events.some((event) => event.type === "user-input.requested")).toBe(false);
        expect(yield* harness.adapter.hasSession(threadId)).toBe(true);

        yield* harness.adapter.stopSession(threadId);
        yield* Deferred.await(harness.sessionExited);
      }),
    ),
  );

  it.effect("answers a cancelled ACP elicitation with the protocol cancel action", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const threadId = ThreadId.make("mastra-elicitation-cancel");
        yield* harness.adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });

        const turn = yield* harness.adapter
          .sendTurn({ threadId, input: "ask before finishing" })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(harness.promptStarted);
        const handler = harness.elicitationHandler();
        expect(handler).toBeDefined();
        const elicitation = yield* handler!({
          mode: "form",
          sessionId: "mastra-acp-session",
          message: "Should the agent continue?",
          requestedSchema: {
            type: "object",
            properties: { approved: { type: "boolean" } },
          },
        }).pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(harness.elicitationRequested);
        const interrupt = yield* harness.adapter
          .interruptTurn(threadId)
          .pipe(Effect.forkChild({ startImmediately: true }));

        const response = yield* Fiber.join(elicitation);
        const turnResult = yield* Fiber.join(turn).pipe(Effect.exit);
        yield* Fiber.join(interrupt);

        expect(response).toEqual({ action: { action: "cancel" } });
        expect(Exit.isSuccess(turnResult)).toBe(true);
        expect(harness.events.some((event) => event.type === "user-input.resolved")).toBe(false);

        yield* harness.adapter.stopSession(threadId);
        yield* Deferred.await(harness.sessionExited);
      }),
    ),
  );

  it.effect(
    "does not emit a queued turn after stop and emits exit after the active turn ends",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          const threadId = ThreadId.make("mastra-stop-turn-race");
          yield* harness.adapter.startSession({
            threadId,
            cwd: process.cwd(),
            runtimeMode: "full-access",
          });

          const activeTurn = yield* harness.adapter
            .sendTurn({ threadId, input: "active turn" })
            .pipe(Effect.forkChild);
          yield* Deferred.await(harness.promptStarted);
          const queuedTurn = yield* harness.adapter
            .sendTurn({ threadId, input: "queued turn" })
            .pipe(Effect.forkChild);
          yield* Effect.yieldNow;
          const stop = yield* harness.adapter.stopSession(threadId).pipe(Effect.forkChild);
          yield* Deferred.await(harness.cancelCalled);

          const activeResult = yield* Fiber.join(activeTurn).pipe(Effect.exit);
          const queuedResult = yield* Fiber.join(queuedTurn).pipe(Effect.exit);
          yield* Fiber.join(stop);
          yield* Deferred.await(harness.sessionExited);

          expect(Exit.isSuccess(activeResult)).toBe(true);
          expect(Exit.isFailure(queuedResult)).toBe(true);
          expect(harness.cancelCallCount()).toBe(1);
          const turnStarts = harness.events.filter((event) => event.type === "turn.started");
          const turnCompletedIndex = harness.events.findIndex(
            (event) => event.type === "turn.completed",
          );
          const sessionExitedIndex = harness.events.findIndex(
            (event) => event.type === "session.exited",
          );
          expect(turnStarts).toHaveLength(1);
          expect(turnCompletedIndex).toBeGreaterThanOrEqual(0);
          expect(sessionExitedIndex).toBeGreaterThan(turnCompletedIndex);
        }),
      ),
  );
});

describe("Mastra Code ACP boundaries", () => {
  it("accepts ACP requests only from the active ACP session", () => {
    expect(mastraCodeAcpSessionMatches("session-a", "session-a")).toBe(true);
    expect(mastraCodeAcpSessionMatches("session-a", "session-b")).toBe(false);
    expect(mastraCodeAcpSessionMatches("session-a", undefined)).toBe(false);
  });

  it("does not begin queued turns after a session stops, disconnects, or is replaced", () => {
    expect(
      mastraCodeSessionCanStartTurn({ isCurrent: true, stopped: false, disconnected: false }),
    ).toBe(true);
    expect(
      mastraCodeSessionCanStartTurn({ isCurrent: true, stopped: true, disconnected: false }),
    ).toBe(false);
    expect(
      mastraCodeSessionCanStartTurn({ isCurrent: true, stopped: false, disconnected: true }),
    ).toBe(false);
    expect(
      mastraCodeSessionCanStartTurn({ isCurrent: false, stopped: false, disconnected: false }),
    ).toBe(false);
  });

  it("does not expose ACP stderr or provider errors to clients", () => {
    const secret = "refresh_token=must-not-leak";
    const error = mapMastraCodeAcpError(
      "thread-1" as never,
      "session/start",
      new EffectAcpErrors.AcpProcessExitedError({ code: 1, stderr: secret }),
    );

    expect(error._tag).toBe("ProviderAdapterProcessError");
    expect(error.message).not.toContain(secret);
    if (error._tag === "ProviderAdapterProcessError") {
      expect(error.detail).not.toContain(secret);
      expect(error.cause).toBeUndefined();
    }
  });
});
