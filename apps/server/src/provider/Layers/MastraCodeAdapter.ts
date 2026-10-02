import {
  ApprovalRequestId,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeRequestId,
  TurnId,
  type MastraCodeSettings,
  ProviderSetupError,
  type ProviderApprovalDecision,
  type ProviderApprovalOption,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ProviderUserInputAnswers,
  type RuntimeMode,
  type ThreadId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Encoding from "effect/Encoding";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { ServerConfig } from "../../config.ts";
import { withMastraCodeThreadStorage } from "../MastraCodeEnvironment.ts";
import { buildRuntimeInstructions } from "../RuntimeInstructions.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpPlanUpdatedEvent,
  makeAcpRequestOpenedEvent,
  makeAcpRequestResolvedEvent,
  makeAcpToolCallEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";
import { parsePermissionRequest } from "../acp/AcpRuntimeModel.ts";
import type * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import type { MastraCodeAcpRuntimeInput } from "../acp/MastraCodeAcpSupport.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";

const PROVIDER = ProviderDriverKind.make("mastraCode");
const RESUME_CURSOR = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  sessionId: Schema.NonEmptyString,
});
const decodeResumeCursor = Schema.decodeUnknownOption(RESUME_CURSOR);
const PLAN_MAX_BYTES = 256 * 1024;
const PLAN_TOOL_NAMES = new Set(["submit_plan"]);

type Runtime = Pick<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  | "handleRequestPermission"
  | "handleElicitation"
  | "handleSessionUpdate"
  | "start"
  | "getModeState"
  | "getEvents"
  | "drainEvents"
  | "prompt"
  | "cancel"
  | "setMode"
  | "setSessionModel"
>;
type Adapter = ProviderAdapterShape<ProviderAdapterError>;
type PermissionRequest = EffectAcpSchema.RequestPermissionRequest;
type ElicitationRequest = EffectAcpSchema.ElicitationRequest;
type ElicitationResponse = EffectAcpSchema.ElicitationResponse;
type RuntimeFactoryInput = Omit<MastraCodeAcpRuntimeInput, "settings" | "childProcessSpawner">;

export interface MastraCodeAdapterOptions {
  readonly instanceId: ProviderInstanceId;
  readonly appDataDirectory: string;
  readonly environment: NodeJS.ProcessEnv;
  /** Signals deterministic tests after the PubSub subscription is active. */
  readonly onEventStreamSubscribed?: () => Effect.Effect<void>;
  readonly onAvailableCommands?: (
    commands: ReadonlyArray<EffectAcpSchema.AvailableCommand>,
    cwd: string,
  ) => Effect.Effect<void>;
  readonly makeRuntime: (
    input: RuntimeFactoryInput,
  ) => Effect.Effect<Runtime, EffectAcpErrors.AcpError | ProviderSetupError, Scope.Scope>;
}

interface PendingApproval {
  readonly request: PermissionRequest;
  readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
  readonly turnId: TurnId | undefined;
}

interface PendingElicitation {
  readonly request: ElicitationRequest;
  readonly result: Deferred.Deferred<
    | { readonly _tag: "answers"; readonly answers: ProviderUserInputAnswers }
    | { readonly _tag: "cancelled" }
  >;
  readonly turnId: TurnId | undefined;
}

interface MastraCodeSessionContext {
  readonly threadId: ThreadId;
  readonly cwd: string;
  readonly scope: Scope.Closeable;
  readonly runtime: Runtime;
  readonly approvals: Map<ApprovalRequestId, PendingApproval>;
  readonly elicitations: Map<ApprovalRequestId, PendingElicitation>;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  readonly turnLock: Semaphore.Semaphore;
  readonly promptDispatchLock: Semaphore.Semaphore;
  notificationFiber: Fiber.Fiber<void, ProviderAdapterRequestError> | undefined;
  session: ProviderSession;
  activeTurnId: TurnId | undefined;
  activePromptDispatched: Deferred.Deferred<void> | undefined;
  cancelRequestedTurnId: TurnId | undefined;
  cancelIssuedTurnId: TurnId | undefined;
  cancelResult: Deferred.Deferred<Exit.Exit<void, EffectAcpErrors.AcpError>> | undefined;
  stopped: boolean;
  disconnected: boolean;
  cancelFailed: boolean;
  cleanupStarted: boolean;
  exitEmitted: boolean;
}

const isAcpProcessExitedError = Schema.is(EffectAcpErrors.AcpProcessExitedError);

export function mapMastraCodeAcpError(
  threadId: ThreadId,
  method: string,
  error: EffectAcpErrors.AcpError,
): ProviderAdapterError {
  if (isAcpProcessExitedError(error)) {
    return new ProviderAdapterProcessError({
      provider: PROVIDER,
      threadId,
      detail: "Mastra Code ACP process exited unexpectedly.",
    });
  }
  return new ProviderAdapterRequestError({
    provider: PROVIDER,
    method,
    detail: `Mastra Code ACP request failed (${method}).`,
  });
}

export function mastraCodeAcpSessionMatches(
  requestSessionId: string,
  activeSessionId: string | undefined,
): boolean {
  return activeSessionId !== undefined && requestSessionId === activeSessionId;
}

export function mastraCodeSessionCanStartTurn(input: {
  readonly isCurrent: boolean;
  readonly stopped: boolean;
  readonly disconnected: boolean;
}): boolean {
  return input.isCurrent && !input.stopped && !input.disconnected;
}

function record(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function text(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function rawInputRecord(request: PermissionRequest): Record<string, unknown> | undefined {
  const rawInput = request.toolCall.rawInput;
  if (typeof rawInput === "string") {
    try {
      return record(JSON.parse(rawInput));
    } catch {
      return undefined;
    }
  }
  return record(rawInput);
}

function isInside(path: Path.Path, root: string, candidate: string): boolean {
  const relative = path.relative(root, candidate);
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

type ElicitationValue = EffectAcpSchema.ElicitationContentValue;

export function coerceMastraCodeElicitationAnswer(
  property: EffectAcpSchema.ElicitationPropertySchema,
  answer: unknown,
): ElicitationValue | undefined {
  switch (property.type) {
    case "boolean":
      if (typeof answer === "boolean") return answer;
      if (typeof answer === "string" && /^(true|false)$/i.test(answer.trim()))
        return answer.trim().toLowerCase() === "true";
      return undefined;
    case "number":
    case "integer": {
      const value = typeof answer === "string" && answer.trim() !== "" ? Number(answer) : answer;
      if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
      if (property.type === "integer" && !Number.isInteger(value)) return undefined;
      if (property.minimum != null && value < property.minimum) return undefined;
      if (property.maximum != null && value > property.maximum) return undefined;
      return value;
    }
    case "string": {
      if (typeof answer !== "string") return undefined;
      if (property.minLength != null && answer.length < property.minLength) return undefined;
      if (property.maxLength != null && answer.length > property.maxLength) return undefined;
      const allowed = property.enum ?? property.oneOf?.map((entry) => entry.const);
      return allowed && !allowed.includes(answer) ? undefined : answer;
    }
    case "array": {
      if (!Array.isArray(answer) || !answer.every((item) => typeof item === "string"))
        return undefined;
      if (property.minItems != null && answer.length < property.minItems) return undefined;
      if (property.maxItems != null && answer.length > property.maxItems) return undefined;
      const allowed =
        "enum" in property.items
          ? property.items.enum
          : property.items.anyOf.map((entry) => entry.const);
      return answer.every((item) => allowed.includes(item)) ? answer : undefined;
    }
  }
}

function elicitationQuestions(request: Extract<ElicitationRequest, { mode: "form" }>) {
  const properties = request.requestedSchema.properties ?? {};
  const required = new Set(request.requestedSchema.required ?? []);
  return Object.entries(properties).map(([id, property]) => {
    const enumValues =
      property.type === "string"
        ? (property.enum ?? property.oneOf?.map((entry) => entry.const))
        : property.type === "array"
          ? "enum" in property.items
            ? property.items.enum
            : property.items.anyOf.map((entry) => entry.const)
          : undefined;
    const options =
      enumValues?.map((value) => ({
        label: value,
        description: property.description ?? "",
        value,
      })) ?? [];
    return {
      id,
      header: text(property.title) ?? (required.has(id) ? "Required" : "Optional"),
      question: [request.message, text(property.description)].filter(Boolean).join("\n\n") || id,
      options,
      allowCustomAnswer: options.length === 0,
      multiSelect: property.type === "array",
    };
  });
}

function permissionOption(
  request: PermissionRequest,
  decision: ProviderApprovalDecision,
): string | undefined {
  const byKind = (kind: string) => request.options.find((option) => option.kind === kind)?.optionId;
  switch (decision) {
    case "acceptForSession":
      return byKind("allow_always");
    case "accept":
      // A one-time approval must never be widened into persistent session
      // approval just because the agent omitted its one-time option.
      return byKind("allow_once");
    case "decline":
      return byKind("reject_once");
    case "cancel":
      return undefined;
  }
}

function providerApprovalOptions(
  request: PermissionRequest,
): ReadonlyArray<ProviderApprovalOption> {
  const options: ProviderApprovalOption[] = [];
  if (request.options.some((option) => option.kind === "allow_once")) {
    options.push({ decision: "accept", label: "Allow once" });
  }
  if (request.options.some((option) => option.kind === "allow_always")) {
    options.push({ decision: "acceptForSession", label: "Always allow this session" });
  }
  if (request.options.some((option) => option.kind === "reject_once")) {
    options.push({ decision: "decline", label: "Deny" });
  }
  options.push({ decision: "cancel", label: "Cancel" });
  return options;
}

function resolvePlanPath(rawPath: string, cwd: string, path: Path.Path): string {
  return path.isAbsolute(rawPath) ? path.resolve(rawPath) : path.resolve(cwd, rawPath);
}

export const makeMastraCodeAdapter = Effect.fn("makeMastraCodeAdapter")(function* (
  _settings: Pick<MastraCodeSettings, "binaryPath">,
  options: MastraCodeAdapterOptions,
) {
  const crypto = yield* Crypto.Crypto;
  const platform = yield* HostProcessPlatform;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const serverConfig = yield* ServerConfig;
  const sessions = new Map<ThreadId, MastraCodeSessionContext>();
  const locks = yield* SynchronizedRef.make(new Map<ThreadId, Semaphore.Semaphore>());
  const events = yield* PubSub.unbounded<ProviderRuntimeEvent>();
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const newEventId = crypto.randomUUIDv4.pipe(
    Effect.mapError(
      (cause) =>
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "crypto/randomUUIDv4",
          detail: "Could not create a Mastra Code event ID.",
          cause,
        }),
    ),
  );
  const stamp = Effect.all({ eventId: Effect.map(newEventId, EventId.make), createdAt: nowIso });
  const emit = (event: ProviderRuntimeEvent) => PubSub.publish(events, event).pipe(Effect.asVoid);

  const withThreadLock = <A, E, R>(threadId: ThreadId, task: Effect.Effect<A, E, R>) =>
    SynchronizedRef.modifyEffect(locks, (current) => {
      const existing = current.get(threadId);
      if (existing) return Effect.succeed([existing, current] as const);
      return Semaphore.make(1).pipe(
        Effect.map((lock) => [lock, new Map(current).set(threadId, lock)] as const),
      );
    }).pipe(Effect.flatMap((lock) => lock.withPermit(task)));

  const requireSession = (threadId: ThreadId) => {
    const session = sessions.get(threadId);
    return session && !session.stopped
      ? Effect.succeed(session)
      : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
  };

  const requireCurrentSession = (threadId: ThreadId, expected: MastraCodeSessionContext) =>
    sessions.get(threadId) === expected && !expected.stopped && !expected.disconnected
      ? Effect.void
      : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));

  const emitSessionExited = (
    context: MastraCodeSessionContext,
    exitKind: "error" | "graceful",
    reason?: string,
  ) =>
    Effect.gen(function* () {
      const shouldEmit = yield* Effect.sync(() => {
        if (context.exitEmitted) return false;
        context.exitEmitted = true;
        return true;
      });
      if (!shouldEmit) return;
      yield* emit({
        type: "session.exited",
        ...(yield* stamp),
        provider: PROVIDER,
        threadId: context.threadId,
        payload: { exitKind, ...(reason ? { reason } : {}) },
      });
    });

  const readPlan = (request: PermissionRequest, cwd: string, plansDirectory: string) =>
    Effect.gen(function* () {
      const rawPath = text(rawInputRecord(request)?.path);
      if (!rawPath) return undefined;
      const realCwd = yield* fileSystem.realPath(cwd).pipe(Effect.orElseSucceed(() => undefined));
      if (!realCwd) return undefined;
      const candidate = resolvePlanPath(rawPath, cwd, path);
      const actual = yield* fileSystem
        .realPath(candidate)
        .pipe(Effect.orElseSucceed(() => undefined));
      if (!actual || !actual.toLowerCase().endsWith(".md")) return undefined;
      const allowedRoots = [
        { root: path.join(cwd, ".mastracode", "plans"), trustRoot: realCwd },
        { root: path.join(cwd, ".artifacts", "plans"), trustRoot: realCwd },
        {
          root: plansDirectory,
          trustRoot: yield* fileSystem
            .realPath(options.appDataDirectory)
            .pipe(Effect.orElseSucceed(() => undefined)),
        },
      ];
      let allowed = false;
      for (const allowedRoot of allowedRoots) {
        const realRoot = yield* fileSystem
          .realPath(allowedRoot.root)
          .pipe(Effect.orElseSucceed(() => undefined));
        // A project-controlled symlink at `.mastracode/plans` or
        // `.artifacts/plans` must not redefine the trust root to an external
        // directory. Check the resolved root itself before checking the file.
        if (
          !realRoot ||
          !allowedRoot.trustRoot ||
          realRoot === allowedRoot.trustRoot ||
          !isInside(path, allowedRoot.trustRoot, realRoot)
        )
          continue;
        if (!isInside(path, realRoot, actual)) continue;
        const relative = path.relative(realRoot, actual);
        if (relative !== "" && path.basename(relative) === relative) {
          allowed = true;
          break;
        }
      }
      if (!allowed) return undefined;
      const stat = yield* fileSystem.stat(actual).pipe(Effect.orElseSucceed(() => undefined));
      if (!stat || stat.size > PLAN_MAX_BYTES || stat.type !== "File") return undefined;
      return yield* fileSystem.readFileString(actual).pipe(Effect.orElseSucceed(() => undefined));
    });

  const permissionResponse = (
    request: PermissionRequest,
    decision: ProviderApprovalDecision,
  ): EffectAcpSchema.RequestPermissionResponse => {
    if (decision === "cancel") return { outcome: { outcome: "cancelled" } };
    const optionId = permissionOption(request, decision);
    return optionId
      ? { outcome: { outcome: "selected", optionId } }
      : { outcome: { outcome: "cancelled" } };
  };

  const makePermissionHandler =
    (
      threadId: ThreadId,
      runtimeMode: RuntimeMode,
      cwd: string,
      plansDirectory: string,
      pending: Map<ApprovalRequestId, PendingApproval>,
      getTurnId: () => TurnId | undefined,
      getActiveSessionId: () => string | undefined,
    ) =>
    (request: PermissionRequest) =>
      Effect.gen(function* () {
        const session = sessions.get(threadId);
        if (
          !mastraCodeAcpSessionMatches(request.sessionId, getActiveSessionId()) ||
          !session ||
          session.stopped ||
          session.disconnected
        ) {
          return { outcome: { outcome: "cancelled" } } as const;
        }
        const turnId = getTurnId();
        const approval = parsePermissionRequest(request);
        const toolName = text(request.toolCall.title);
        const isPlanRequest = toolName !== undefined && PLAN_TOOL_NAMES.has(toolName);
        if (isPlanRequest) {
          const plan = yield* readPlan(request, cwd, plansDirectory);
          if (plan?.trim()) {
            yield* emit({
              type: "turn.proposed.completed",
              ...(yield* stamp),
              provider: PROVIDER,
              threadId,
              turnId,
              payload: { planMarkdown: plan.trim() },
              raw: {
                source: "acp.jsonrpc",
                method: "session/request_permission",
                payload: { tool: "submit_plan", planPath: text(rawInputRecord(request)?.path) },
              },
            });
          }
        }

        if (runtimeMode === "full-access" && !isPlanRequest) {
          const allow =
            request.options.find((option) => option.kind === "allow_once") ??
            request.options.find((option) => option.kind === "allow_always");
          if (allow) return { outcome: { outcome: "selected" as const, optionId: allow.optionId } };
        }

        const requestId = ApprovalRequestId.make(yield* newEventId);
        const runtimeRequestId = RuntimeRequestId.make(requestId);
        const decision = yield* Deferred.make<ProviderApprovalDecision>();
        pending.set(requestId, { request, decision, turnId });
        const detail =
          approval.detail ?? toolName ?? "Mastra Code requests permission to use a tool.";
        return yield* Effect.gen(function* () {
          yield* emit(
            makeAcpRequestOpenedEvent({
              stamp: yield* stamp,
              provider: PROVIDER,
              threadId,
              turnId,
              requestId: runtimeRequestId,
              permissionRequest: approval,
              approvalOptions: providerApprovalOptions(request),
              detail,
              args: request.toolCall.rawInput ?? request.toolCall,
              source: "acp.jsonrpc",
              method: "session/request_permission",
              rawPayload: request,
            }),
          );
          const result = yield* Deferred.await(decision);
          yield* emit(
            makeAcpRequestResolvedEvent({
              stamp: yield* stamp,
              provider: PROVIDER,
              threadId,
              turnId,
              requestId: runtimeRequestId,
              permissionRequest: approval,
              decision: result,
            }),
          );
          return permissionResponse(request, result);
        }).pipe(Effect.ensuring(Effect.sync(() => pending.delete(requestId))));
      });

  const startSession: Adapter["startSession"] = (input) =>
    withThreadLock(
      input.threadId,
      Effect.gen(function* () {
        if (input.provider !== undefined && input.provider !== PROVIDER) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
          });
        }
        if (!input.cwd?.trim()) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "cwd is required and must be non-empty.",
          });
        }
        const cwd = path.resolve(input.cwd.trim());
        const resumeCursor = Option.getOrUndefined(decodeResumeCursor(input.resumeCursor));
        if (input.resumeCursor !== undefined && resumeCursor === undefined) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "resumeCursor is not a valid Mastra Code session cursor.",
          });
        }
        const existing = sessions.get(input.threadId);
        if (existing && !existing.stopped && !existing.disconnected && resumeCursor) {
          const activeResumeCursor = Option.getOrUndefined(
            decodeResumeCursor(existing.session.resumeCursor),
          );
          const requestedModel =
            input.modelSelection?.instanceId === options.instanceId
              ? input.modelSelection.model
              : undefined;
          if (
            activeResumeCursor?.sessionId !== resumeCursor.sessionId ||
            existing.cwd !== cwd ||
            existing.session.runtimeMode !== input.runtimeMode ||
            (requestedModel !== undefined && requestedModel !== existing.session.model)
          ) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "resumeCursor does not match the active Mastra Code session.",
            });
          }
          return existing.session;
        }
        if (existing) yield* stopSessionInternal(existing);

        const pendingApprovals = new Map<ApprovalRequestId, PendingApproval>();
        const pendingElicitations = new Map<ApprovalRequestId, PendingElicitation>();
        const sessionScope = yield* Scope.make("sequential");
        let sessionScopeTransferred = false;
        yield* Effect.addFinalizer(() =>
          sessionScopeTransferred ? Effect.void : Scope.close(sessionScope, Exit.void),
        );
        let context: MastraCodeSessionContext | undefined;
        const threadDigest = yield* crypto
          .digest("SHA-256", new TextEncoder().encode(input.threadId))
          .pipe(
            Effect.mapError(
              () =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: "Mastra Code private thread storage identity could not be created.",
                }),
            ),
          );
        const threadStorageDirectory = path.join(
          options.appDataDirectory,
          "threads",
          Encoding.encodeHex(threadDigest),
        );
        for (const directory of [
          path.join(options.appDataDirectory, "threads"),
          threadStorageDirectory,
          path.join(threadStorageDirectory, "plans"),
        ]) {
          yield* fileSystem.makeDirectory(directory, { recursive: true, mode: 0o700 }).pipe(
            Effect.mapError(
              () =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: "Mastra Code private thread storage could not be created.",
                }),
            ),
          );
          if (platform !== "win32") {
            yield* fileSystem.chmod(directory, 0o700).pipe(
              Effect.mapError(
                () =>
                  new ProviderAdapterProcessError({
                    provider: PROVIDER,
                    threadId: input.threadId,
                    detail: "Mastra Code private thread storage permissions could not be set.",
                  }),
              ),
            );
          }
        }
        const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
        const environment = withMastraCodeThreadStorage(
          McpProviderSession.withAgentDeviceEnvironment(options.environment, mcpSession),
          {
            appDataDirectory: options.appDataDirectory,
            databasePath: path.join(threadStorageDirectory, "mastra.db"),
            vectorDatabasePath: path.join(threadStorageDirectory, "mastra-vectors.db"),
            observabilityDatabasePath: path.join(threadStorageDirectory, "observability.duckdb"),
            plansDirectory: path.join(threadStorageDirectory, "plans"),
          },
        );
        const runtime = yield* options
          .makeRuntime({
            cwd,
            environment,
            clientInfo: { name: "t3-code", version: "0.0.0" },
            resumeMethod: "load",
            sessionLoadRequireRpcResponse: true,
            ...(resumeCursor ? { resumeSessionId: resumeCursor.sessionId } : {}),
            ...(mcpSession
              ? {
                  mcpServers: [
                    {
                      type: "http" as const,
                      name: "t3-code",
                      url: mcpSession.endpoint,
                      headers: [{ name: "Authorization", value: mcpSession.authorizationHeader }],
                    },
                  ],
                }
              : {}),
          })
          .pipe(
            Effect.provideService(Scope.Scope, sessionScope),
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.mapError(
              () =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: "Could not start the Mastra Code ACP process.",
                }),
            ),
          );
        let activeAcpSessionId: string | undefined;
        const started = yield* Effect.gen(function* () {
          yield* runtime.handleRequestPermission((request) =>
            makePermissionHandler(
              input.threadId,
              input.runtimeMode,
              cwd,
              path.join(threadStorageDirectory, "plans"),
              pendingApprovals,
              () => context?.activeTurnId,
              () => activeAcpSessionId,
            )(request).pipe(
              Effect.mapError(() =>
                EffectAcpErrors.AcpRequestError.internalError("Mastra Code approval failed."),
              ),
            ),
          );
          yield* runtime.handleElicitation((request) =>
            Effect.gen(function* () {
              if (
                request.mode !== "form" ||
                !context ||
                context.stopped ||
                context.disconnected ||
                sessions.get(input.threadId) !== context ||
                !mastraCodeAcpSessionMatches(request.sessionId, activeAcpSessionId)
              ) {
                return { action: { action: "cancel" } } as const;
              }
              const turnId = context.activeTurnId;
              const requestId = ApprovalRequestId.make(yield* newEventId);
              const runtimeRequestId = RuntimeRequestId.make(requestId);
              const result = yield* Deferred.make<
                | { readonly _tag: "answers"; readonly answers: ProviderUserInputAnswers }
                | { readonly _tag: "cancelled" }
              >();
              pendingElicitations.set(requestId, { request, result, turnId });
              return yield* Effect.gen(function* () {
                yield* emit({
                  type: "user-input.requested",
                  ...(yield* stamp),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId,
                  requestId: runtimeRequestId,
                  payload: { questions: elicitationQuestions(request) },
                  raw: {
                    source: "acp.jsonrpc",
                    method: "session/elicitation",
                    payload: {
                      message: request.message,
                      mode: request.mode,
                      propertyCount: Object.keys(request.requestedSchema.properties ?? {}).length,
                    },
                  },
                });
                const resolution = yield* Deferred.await(result);
                if (resolution._tag === "cancelled") {
                  return { action: { action: "cancel" } } as const;
                }
                const content: Record<string, ElicitationValue> = {};
                for (const [key, value] of Object.entries(resolution.answers)) {
                  const properties = request.requestedSchema.properties ?? {};
                  if (!Object.hasOwn(properties, key)) continue;
                  const coerced = coerceMastraCodeElicitationAnswer(properties[key]!, value);
                  if (coerced === undefined) return { action: { action: "cancel" } } as const;
                  content[key] = coerced;
                }
                if (request.requestedSchema.required?.some((key) => !Object.hasOwn(content, key)))
                  return { action: { action: "cancel" } } as const;
                yield* emit({
                  type: "user-input.resolved",
                  ...(yield* stamp),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId,
                  requestId: runtimeRequestId,
                  payload: { answers: content },
                });
                const response: ElicitationResponse = {
                  action: { action: "accept", content },
                };
                return response;
              }).pipe(Effect.ensuring(Effect.sync(() => pendingElicitations.delete(requestId))));
            }).pipe(
              Effect.mapError(() =>
                EffectAcpErrors.AcpRequestError.internalError("Mastra Code user input failed."),
              ),
            ),
          );
          return yield* runtime.start();
        }).pipe(
          Effect.mapError((cause) => mapMastraCodeAcpError(input.threadId, "session/start", cause)),
        );
        activeAcpSessionId = started.sessionId;

        const selectedModel =
          input.modelSelection?.instanceId === options.instanceId
            ? input.modelSelection.model
            : undefined;
        if (selectedModel) {
          yield* runtime
            .setSessionModel(selectedModel)
            .pipe(
              Effect.mapError((cause) =>
                mapMastraCodeAcpError(input.threadId, "session/set_model", cause),
              ),
            );
        }

        const createdAt = yield* nowIso;
        const session: ProviderSession = {
          provider: PROVIDER,
          providerInstanceId: options.instanceId,
          status: "ready",
          runtimeMode: input.runtimeMode,
          cwd,
          ...(selectedModel ? { model: selectedModel } : {}),
          threadId: input.threadId,
          resumeCursor: { schemaVersion: 1, sessionId: started.sessionId },
          createdAt,
          updatedAt: createdAt,
        };
        const turnLock = yield* Semaphore.make(1);
        const promptDispatchLock = yield* Semaphore.make(1);
        context = {
          threadId: input.threadId,
          cwd,
          scope: sessionScope,
          runtime,
          approvals: pendingApprovals,
          elicitations: pendingElicitations,
          turns: [],
          turnLock,
          promptDispatchLock,
          notificationFiber: undefined,
          session,
          activeTurnId: undefined,
          activePromptDispatched: undefined,
          cancelRequestedTurnId: undefined,
          cancelIssuedTurnId: undefined,
          cancelResult: undefined,
          stopped: false,
          disconnected: false,
          cancelFailed: false,
          cleanupStarted: false,
          exitEmitted: false,
        };

        const sessionContext = context;
        sessionContext.notificationFiber = yield* Stream.runForEach(runtime.getEvents(), (event) =>
          Effect.gen(function* () {
            switch (event._tag) {
              case "EventStreamBarrier":
                yield* Deferred.succeed(event.acknowledge, undefined);
                return;
              case "ConnectionTerminated":
                sessionContext.disconnected = true;
                sessionContext.stopped = true;
                sessionContext.session = {
                  ...sessionContext.session,
                  status: "error",
                  lastError: "Mastra Code ACP connection ended.",
                  updatedAt: yield* nowIso,
                };
                if (sessionContext.activeTurnId === undefined) {
                  yield* emitSessionExited(
                    sessionContext,
                    "error",
                    "Mastra Code ACP connection ended.",
                  );
                }
                return;
              case "AvailableCommandsUpdated":
                yield* (
                  options.onAvailableCommands?.(event.availableCommands, sessionContext.cwd) ??
                    Effect.void
                );
                return;
              case "ModeChanged":
              case "ConfigOptionsUpdated":
                return;
              case "AssistantItemStarted":
                yield* emit(
                  makeAcpAssistantItemEvent({
                    stamp: yield* stamp,
                    provider: PROVIDER,
                    threadId: input.threadId,
                    turnId: sessionContext.activeTurnId,
                    itemId: event.itemId,
                    lifecycle: "item.started",
                  }),
                );
                return;
              case "AssistantItemCompleted":
                yield* emit(
                  makeAcpAssistantItemEvent({
                    stamp: yield* stamp,
                    provider: PROVIDER,
                    threadId: input.threadId,
                    turnId: sessionContext.activeTurnId,
                    itemId: event.itemId,
                    lifecycle: "item.completed",
                  }),
                );
                return;
              case "PlanUpdated":
                yield* emit(
                  makeAcpPlanUpdatedEvent({
                    stamp: yield* stamp,
                    provider: PROVIDER,
                    threadId: input.threadId,
                    turnId: sessionContext.activeTurnId,
                    payload: event.payload,
                    source: "acp.jsonrpc",
                    method: "session/update",
                    rawPayload: event.rawPayload,
                  }),
                );
                return;
              case "ToolCallUpdated":
                yield* emit(
                  makeAcpToolCallEvent({
                    stamp: yield* stamp,
                    provider: PROVIDER,
                    threadId: input.threadId,
                    turnId: sessionContext.activeTurnId,
                    toolCall: event.toolCall,
                    rawPayload: event.rawPayload,
                  }),
                );
                return;
              case "ThoughtDelta":
                yield* emit(
                  makeAcpContentDeltaEvent({
                    stamp: yield* stamp,
                    provider: PROVIDER,
                    threadId: input.threadId,
                    turnId: sessionContext.activeTurnId,
                    streamKind: "reasoning_text",
                    text: event.text,
                    rawPayload: event.rawPayload,
                  }),
                );
                return;
              case "ContentDelta":
                yield* emit(
                  makeAcpContentDeltaEvent({
                    stamp: yield* stamp,
                    provider: PROVIDER,
                    threadId: input.threadId,
                    turnId: sessionContext.activeTurnId,
                    ...(event.itemId ? { itemId: event.itemId } : {}),
                    text: event.text,
                    rawPayload: event.rawPayload,
                  }),
                );
                return;
            }
          }),
        ).pipe(Effect.forkIn(sessionScope));
        sessions.set(input.threadId, sessionContext);
        sessionScopeTransferred = true;

        yield* emit({
          type: "session.started",
          ...(yield* stamp),
          provider: PROVIDER,
          threadId: input.threadId,
          payload: { resume: started.initializeResult },
        });
        yield* emit({
          type: "session.state.changed",
          ...(yield* stamp),
          provider: PROVIDER,
          threadId: input.threadId,
          payload: { state: "ready", reason: "Mastra Code ACP session ready" },
        });
        yield* emit({
          type: "thread.started",
          ...(yield* stamp),
          provider: PROVIDER,
          threadId: input.threadId,
          payload: { providerThreadId: started.sessionId },
        });
        return session;
      }).pipe(Effect.scoped),
    );

  const requestPromptCancellation = (context: MastraCodeSessionContext, turnId: TurnId) =>
    context.promptDispatchLock.withPermit(
      Effect.gen(function* () {
        const claim = yield* Effect.sync(() => {
          const result = context.cancelResult;
          if (context.activeTurnId !== turnId || !result) return undefined;
          const owner = context.cancelIssuedTurnId !== turnId;
          if (owner) context.cancelIssuedTurnId = turnId;
          return { owner, result };
        });
        if (!claim) return;
        if (claim.owner) {
          const cancellation = yield* Effect.exit(context.runtime.cancel.pipe(Effect.asVoid));
          if (Exit.isFailure(cancellation)) {
            const failedAt = yield* nowIso;
            context.cancelFailed = true;
            context.stopped = true;
            context.session = {
              ...context.session,
              status: "error",
              lastError: "Mastra Code could not confirm that the turn stopped.",
              updatedAt: failedAt,
            };
          }
          yield* Deferred.succeed(claim.result, cancellation);
        }
        return yield* Deferred.await(claim.result).pipe(Effect.flatMap((result) => result));
      }),
    );

  function stopSessionInternal(context: MastraCodeSessionContext) {
    return Effect.gen(function* () {
      const startedCleanup = yield* context.promptDispatchLock.withPermit(
        Effect.sync(() => {
          if (context.cleanupStarted) return false;
          context.cleanupStarted = true;
          context.stopped = true;
          return true;
        }),
      );
      if (!startedCleanup) return;
      for (const pending of context.approvals.values()) {
        yield* Deferred.succeed(pending.decision, "cancel").pipe(Effect.ignore);
      }
      for (const pending of context.elicitations.values()) {
        yield* Deferred.succeed(pending.result, { _tag: "cancelled" }).pipe(Effect.ignore);
      }
      const activePromptDispatched = context.activePromptDispatched;
      const activeTurnId = context.activeTurnId;
      if (activePromptDispatched && activeTurnId !== undefined) {
        yield* Deferred.await(activePromptDispatched);
        yield* requestPromptCancellation(context, activeTurnId).pipe(Effect.ignore);
      }
      yield* context.turnLock.withPermit(Effect.void);
      if (context.notificationFiber) yield* Fiber.interrupt(context.notificationFiber);
      yield* Effect.ignore(Scope.close(context.scope, Exit.void));
      if (sessions.get(context.threadId) === context) sessions.delete(context.threadId);
      const exitReason = context.disconnected
        ? "Mastra Code ACP connection ended."
        : context.cancelFailed
          ? "Mastra Code session was closed because turn cancellation could not be confirmed."
          : undefined;
      yield* emitSessionExited(context, exitReason ? "error" : "graceful", exitReason);
    });
  }

  const sendTurn: Adapter["sendTurn"] = (input) =>
    Effect.gen(function* () {
      const context = yield* requireSession(input.threadId);
      let turnLifecycle:
        | { readonly turnId: TurnId; started: boolean; terminalEmitted: boolean }
        | undefined;
      const emitTurnCompleted = (
        lifecycle: NonNullable<typeof turnLifecycle>,
        payload: Extract<ProviderRuntimeEvent, { type: "turn.completed" }>["payload"],
      ) =>
        Effect.uninterruptible(
          Effect.gen(function* () {
            if (!lifecycle.started || lifecycle.terminalEmitted) return;
            yield* emit({
              type: "turn.completed",
              ...(yield* stamp),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId: lifecycle.turnId,
              payload,
            });
            lifecycle.terminalEmitted = true;
          }),
        );
      return yield* context.turnLock.withPermit(
        Effect.gen(function* () {
          yield* requireCurrentSession(input.threadId, context);
          const prompt: Array<EffectAcpSchema.ContentBlock> = [];
          const userText = input.input?.trim() ?? "";
          if (userText) prompt.push({ type: "text", text: userText });
          for (const attachment of input.attachments ?? []) {
            if (attachment.type !== "image") continue;
            const attachmentPath = resolveAttachmentPath({
              attachmentsDir: serverConfig.attachmentsDir,
              attachment,
            });
            if (!attachmentPath) {
              return yield* new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "session/prompt",
                detail: `Invalid attachment id '${attachment.id}'.`,
              });
            }
            const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: "session/prompt",
                    detail: "Could not read an image attachment.",
                    cause,
                  }),
              ),
            );
            prompt.push({
              type: "image",
              data: Buffer.from(bytes).toString("base64"),
              mimeType: attachment.mimeType,
            });
          }
          if (prompt.length === 0) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "Turn requires non-empty text or image attachments.",
            });
          }

          yield* requireCurrentSession(input.threadId, context);
          const turnId = TurnId.make(yield* newEventId);
          const lifecycle = { turnId, started: false, terminalEmitted: false };
          turnLifecycle = lifecycle;
          const modelSelection =
            input.modelSelection?.instanceId === options.instanceId
              ? input.modelSelection
              : undefined;
          const model = modelSelection?.model ?? context.session.model;
          const startedAt = yield* nowIso;
          const cancelResult = yield* Deferred.make<Exit.Exit<void, EffectAcpErrors.AcpError>>();
          const beganTurn = yield* Effect.sync(() => {
            if (
              !mastraCodeSessionCanStartTurn({
                isCurrent: sessions.get(input.threadId) === context,
                stopped: context.stopped,
                disconnected: context.disconnected,
              })
            ) {
              return false;
            }
            context.activeTurnId = turnId;
            context.cancelRequestedTurnId = undefined;
            context.cancelIssuedTurnId = undefined;
            context.cancelResult = cancelResult;
            context.session = {
              ...context.session,
              status: "running",
              activeTurnId: turnId,
              updatedAt: startedAt,
            };
            return true;
          });
          if (!beganTurn) {
            return yield* new ProviderAdapterSessionNotFoundError({
              provider: PROVIDER,
              threadId: input.threadId,
            });
          }
          yield* Effect.uninterruptible(
            Effect.gen(function* () {
              yield* emit({
                type: "turn.started",
                ...(yield* stamp),
                provider: PROVIDER,
                threadId: input.threadId,
                turnId,
                payload: { ...(model ? { model } : {}) },
              });
              lifecycle.started = true;
            }),
          );

          const modeState = yield* context.runtime.getModeState;
          const availableModes = modeState?.availableModes ?? [];
          const targetMode =
            input.interactionMode === "plan"
              ? availableModes.find((mode) => mode.id === "plan")
              : context.session.runtimeMode === "approval-required"
                ? availableModes.find((mode) =>
                    /ask|approval|default|code|build/i.test(`${mode.id} ${mode.name}`),
                  )
                : availableModes.find((mode) =>
                    /default|code|build|agent/i.test(`${mode.id} ${mode.name}`),
                  );
          if (input.interactionMode === "plan" && !targetMode) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "session/set_mode",
              detail:
                "Mastra Code does not expose a plan or architect mode; refusing to send this turn in its current mode.",
            });
          }
          if (targetMode && targetMode.id !== modeState?.currentModeId) {
            yield* context.runtime
              .setMode(targetMode.id)
              .pipe(
                Effect.mapError((cause) =>
                  mapMastraCodeAcpError(input.threadId, "session/set_mode", cause),
                ),
              );
          }
          if (model && model !== context.session.model) {
            yield* context.runtime
              .setSessionModel(model)
              .pipe(
                Effect.mapError((cause) =>
                  mapMastraCodeAcpError(input.threadId, "session/set_model", cause),
                ),
              );
          }

          const promptDispatched = yield* Deferred.make<void>();
          const isSlashCommand = prompt.some(
            (part) => part.type === "text" && /^\/[^\s/]+(?:\s|$)/.test(part.text),
          );
          const promptPayload = {
            prompt: isSlashCommand
              ? prompt
              : [
                  ...prompt,
                  {
                    type: "text" as const,
                    text: buildRuntimeInstructions({ harness: "Mastra Code", model }),
                  },
                ],
          };
          const dispatch = yield* context.promptDispatchLock.withPermit(
            Effect.gen(function* () {
              const canDispatchPrompt = yield* Effect.sync(() => {
                if (
                  !mastraCodeSessionCanStartTurn({
                    isCurrent: sessions.get(input.threadId) === context,
                    stopped: context.stopped,
                    disconnected: context.disconnected,
                  }) ||
                  context.cancelRequestedTurnId === turnId
                ) {
                  return false;
                }
                context.activePromptDispatched = promptDispatched;
                return true;
              });
              if (!canDispatchPrompt) return { _tag: "cancelled" as const };

              const promptFiber = yield* Effect.suspend(() =>
                context.runtime.prompt(promptPayload, { dispatched: promptDispatched }),
              ).pipe(
                Effect.mapError((cause) =>
                  mapMastraCodeAcpError(input.threadId, "session/prompt", cause),
                ),
                Effect.ensuring(Deferred.succeed(promptDispatched, undefined)),
                Effect.forkChild,
              );
              yield* Deferred.await(promptDispatched);
              return { _tag: "dispatched" as const, promptFiber };
            }),
          );
          if (dispatch._tag === "cancelled") {
            const cancelledAt = yield* nowIso;
            context.session = {
              ...context.session,
              status: context.disconnected ? "error" : "ready",
              activeTurnId: undefined,
              updatedAt: cancelledAt,
            };
            context.activeTurnId = undefined;
            context.cancelRequestedTurnId = undefined;
            context.cancelIssuedTurnId = undefined;
            context.cancelResult = undefined;
            yield* emitTurnCompleted(lifecycle, {
              state: "cancelled",
              stopReason: "cancelled",
            });
            return { threadId: input.threadId, turnId, resumeCursor: context.session.resumeCursor };
          }
          const promptFiber = dispatch.promptFiber;
          if (context.stopped || context.disconnected || context.cancelRequestedTurnId === turnId) {
            yield* requestPromptCancellation(context, turnId).pipe(
              Effect.mapError((cause) =>
                mapMastraCodeAcpError(input.threadId, "session/cancel", cause),
              ),
            );
          }
          yield* Effect.raceFirst(
            Fiber.join(promptFiber).pipe(Effect.as("prompt-completed" as const)),
            Deferred.await(cancelResult).pipe(
              Effect.flatMap((result) => result),
              Effect.mapError((cause) =>
                mapMastraCodeAcpError(input.threadId, "session/cancel", cause),
              ),
              Effect.as("cancel-completed" as const),
            ),
          );
          const response = yield* Fiber.join(promptFiber);
          if (context.activePromptDispatched === promptDispatched) {
            context.activePromptDispatched = undefined;
          }
          yield* context.runtime.drainEvents;
          const record = context.turns.find((turn) => turn.id === turnId);
          const entry = { prompt, response };
          if (record) record.items.push(entry);
          else context.turns.push({ id: turnId, items: [entry] });
          const updatedAt = yield* nowIso;
          context.session = {
            ...context.session,
            status: context.disconnected || context.cancelFailed ? "error" : "ready",
            activeTurnId: undefined,
            ...(model ? { model } : {}),
            updatedAt,
          };
          yield* emitTurnCompleted(lifecycle, {
            state: response.stopReason === "cancelled" ? "cancelled" : "completed",
            stopReason: response.stopReason ?? null,
          });
          context.activeTurnId = undefined;
          return { threadId: input.threadId, turnId, resumeCursor: context.session.resumeCursor };
        }).pipe(
          Effect.ensuring(
            Effect.gen(function* () {
              const lifecycle = turnLifecycle;
              if (lifecycle?.started && !lifecycle.terminalEmitted) {
                yield* emitTurnCompleted(lifecycle, {
                  state: "failed",
                  ...(context.cancelFailed
                    ? { errorMessage: "Mastra Code could not confirm that the turn stopped." }
                    : {}),
                }).pipe(Effect.ignore);
              }
              if (
                context.session.status === "running" ||
                (lifecycle !== undefined && context.session.activeTurnId === lifecycle.turnId)
              ) {
                context.session = {
                  ...context.session,
                  status: context.disconnected || context.cancelFailed ? "error" : "ready",
                  activeTurnId: undefined,
                };
              }
              context.activePromptDispatched = undefined;
              context.cancelRequestedTurnId = undefined;
              context.cancelIssuedTurnId = undefined;
              context.cancelResult = undefined;
              context.activeTurnId = undefined;
              if (context.disconnected) {
                yield* emitSessionExited(
                  context,
                  "error",
                  "Mastra Code ACP connection ended.",
                ).pipe(Effect.ignore);
              }
            }),
          ),
        ),
      );
    });

  const interruptTurn: Adapter["interruptTurn"] = (threadId, requestedTurnId) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      // Approval replies can finish this turn and let a queued turn start; pin the target first.
      const activeTurnId = context.activeTurnId;
      if (activeTurnId === undefined) return;
      if (requestedTurnId !== undefined && requestedTurnId !== activeTurnId) return;
      const cancellationRequested = yield* context.promptDispatchLock.withPermit(
        Effect.sync(() => {
          if (context.activeTurnId !== activeTurnId) return false;
          context.cancelRequestedTurnId = activeTurnId;
          return true;
        }),
      );
      for (const pending of context.approvals.values()) {
        if (pending.turnId === activeTurnId) {
          yield* Deferred.succeed(pending.decision, "cancel").pipe(Effect.ignore);
        }
      }
      for (const pending of context.elicitations.values()) {
        if (pending.turnId === activeTurnId) {
          yield* Deferred.succeed(pending.result, { _tag: "cancelled" }).pipe(Effect.ignore);
        }
      }
      if (!cancellationRequested) return;
      const activePromptDispatched = context.activePromptDispatched;
      if (activePromptDispatched) {
        yield* Deferred.await(activePromptDispatched);
        const cancellation = yield* requestPromptCancellation(context, activeTurnId).pipe(
          Effect.mapError((cause) => mapMastraCodeAcpError(threadId, "session/cancel", cause)),
          Effect.exit,
        );
        if (Exit.isFailure(cancellation)) {
          yield* stopSessionInternal(context);
          return yield* Effect.failCause(cancellation.cause);
        }
      }
    });

  const respondToRequest: Adapter["respondToRequest"] = (threadId, requestId, decision) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      const pending = context.approvals.get(requestId);
      if (!pending) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/request_permission",
          detail: `Unknown pending permission request '${requestId}'.`,
        });
      }
      yield* Deferred.succeed(pending.decision, decision);
    });

  const respondToUserInput: Adapter["respondToUserInput"] = (threadId, requestId, answers) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      const pending = context.elicitations.get(requestId);
      if (!pending) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/elicitation",
          detail: `Unknown pending Mastra Code question '${requestId}'.`,
        });
      }
      yield* Deferred.succeed(pending.result, { _tag: "answers", answers });
    });

  const stopSession: Adapter["stopSession"] = (threadId) =>
    withThreadLock(
      threadId,
      Effect.gen(function* () {
        const context = sessions.get(threadId);
        if (!context) {
          return yield* new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId });
        }
        yield* stopSessionInternal(context);
      }),
    );
  const listSessions: Adapter["listSessions"] = () =>
    Effect.sync(() => Array.from(sessions.values(), (context) => ({ ...context.session })));
  const hasSession: Adapter["hasSession"] = (threadId) =>
    Effect.sync(() => {
      const context = sessions.get(threadId);
      return context !== undefined && !context.stopped;
    });
  const readThread: Adapter["readThread"] = (threadId) =>
    Effect.gen(function* () {
      const context = yield* requireSession(threadId);
      return { threadId, turns: context.turns };
    });
  const rollbackThread: Adapter["rollbackThread"] = (threadId, numTurns) =>
    Effect.gen(function* () {
      yield* requireSession(threadId);
      if (!Number.isInteger(numTurns) || numTurns < 1) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "numTurns must be an integer greater than zero.",
        });
      }
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "thread/rollback",
        detail: "Mastra Code ACP sessions do not support provider-side rollback.",
      });
    });
  const stopAll: Adapter["stopAll"] = () =>
    Effect.forEach(sessions.values(), stopSessionInternal, { discard: true });

  yield* Effect.addFinalizer(() =>
    Effect.forEach(sessions.values(), stopSessionInternal, { discard: true }).pipe(
      Effect.catch((cause) => Effect.logError("Mastra Code session cleanup failed.", { cause })),
      Effect.tap(() => PubSub.shutdown(events)),
    ),
  );

  const streamEvents = Stream.unwrap(
    PubSub.subscribe(events).pipe(
      Effect.tap(() => options.onEventStreamSubscribed?.() ?? Effect.void),
      Effect.map(Stream.fromSubscription),
    ),
  );
  return {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "in-session", supportsConversationRollback: false },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    stopAll,
    streamEvents,
  } satisfies Adapter;
});
