import {
  ProviderSetupError,
  type ProviderAuthMethod,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as ProviderAuthFlow from "./ProviderAuthFlow.ts";
import type { ProviderAuthController } from "./Services/ProviderAuthService.ts";

const CODEX_PROVIDER = "openai-codex";
const MAX_AUTH_OUTPUT_CHARS = 64 * 1024;
const MAX_AUTH_OUTPUT_BYTES = 64 * 1024;
const MAX_AUTH_LINE_CHARS = 8 * 1024;
const LOGIN_ARGS = ["auth", "login", "--provider", CODEX_PROVIDER, "--device", "--jsonl"];
const LOGOUT_ARGS = ["auth", "logout", "--provider", CODEX_PROVIDER, "--json"];
const DeviceCodeEvent = Schema.Struct({
  type: Schema.Literal("device_code"),
  verificationUrl: Schema.String,
  userCode: Schema.String,
  expiresAt: Schema.String,
});
const ProgressEvent = Schema.Struct({
  type: Schema.Literal("progress"),
  phase: Schema.Literal("waiting_for_authorization"),
});
const SuccessEvent = Schema.Struct({
  type: Schema.Literal("success"),
  provider: Schema.Literal(CODEX_PROVIDER),
  account: Schema.optional(
    Schema.Struct({
      id: Schema.String,
      label: Schema.String,
    }),
  ),
});
const ErrorEvent = Schema.Struct({
  type: Schema.Literal("error"),
  code: Schema.Literals(["LOGIN_CANCELLED", "LOGIN_FAILED", "AUTH_STORE_UNREADABLE"]),
});
const AuthJsonlEvent = Schema.Union([DeviceCodeEvent, ProgressEvent, SuccessEvent, ErrorEvent]);
const decodeDeviceCodeEvent = Schema.decodeUnknownOption(DeviceCodeEvent);
const decodeAuthJsonlEvent = Schema.decodeUnknownOption(Schema.fromJsonString(AuthJsonlEvent), {
  onExcessProperty: "error",
});

function setupError(instanceId: ProviderInstanceId, operation: string, detail: string) {
  return new ProviderSetupError({ instanceId, operation, detail });
}

export type MastraCodeAuthJsonlLine =
  | { readonly type: "empty" }
  | { readonly type: "event"; readonly event: typeof AuthJsonlEvent.Type }
  | { readonly type: "invalid" };

export function parseMastraCodeAuthJsonlLine(line: string): MastraCodeAuthJsonlLine {
  if (line.trim().length === 0) return { type: "empty" };
  const event = Option.getOrUndefined(decodeAuthJsonlEvent(line));
  return event ? { type: "event", event } : { type: "invalid" };
}

export function isMastraCodeAuthLoginSuccessful(input: {
  readonly exitCode: number;
  readonly loginSucceeded: boolean;
  readonly loginFailed: boolean;
  readonly outputInvalid: boolean;
}): boolean {
  return input.exitCode === 0 && input.loginSucceeded && !input.loginFailed && !input.outputInvalid;
}

function safeVerificationUrl(value: unknown): string | undefined {
  if (typeof value !== "string" || value.length > 16_384) return undefined;
  try {
    const url = new URL(value);
    return url.origin === "https://auth.openai.com" &&
      url.pathname === "/codex/device" &&
      !url.search &&
      !url.hash
      ? url.toString()
      : undefined;
  } catch {
    return undefined;
  }
}

export function parseMastraCodeDeviceCodeEvent(
  value: unknown,
  now: number,
): { readonly url: string; readonly userCode: string; readonly expiresAt: number } | undefined {
  const event = Option.getOrUndefined(decodeDeviceCodeEvent(value));
  if (!event) return undefined;
  const url = safeVerificationUrl(event.verificationUrl);
  const userCode = event.userCode.trim();
  const expiresAt = Date.parse(event.expiresAt);
  if (
    !url ||
    userCode.length === 0 ||
    userCode.length > 256 ||
    !Number.isFinite(expiresAt) ||
    expiresAt <= now
  ) {
    return undefined;
  }
  return { url, userCode, expiresAt };
}

export const resolveMastraCodeAppDataDirectory = Effect.fn("resolveMastraCodeAppDataDirectory")(
  function* (stateDirectory: string, instanceId: ProviderInstanceId) {
    const crypto = yield* Crypto.Crypto;
    const path = yield* Path.Path;
    const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(instanceId));
    const key = Encoding.encodeHex(digest);
    return path.join(stateDirectory, "providers", "mastracode", key);
  },
);

function runLogin(input: {
  readonly instanceId: ProviderInstanceId;
  readonly binaryPath: string;
  readonly environment: NodeJS.ProcessEnv;
  readonly context: ProviderAuthFlow.ProviderAuthFlowContext;
}): Effect.Effect<void, ProviderSetupError, ChildProcessSpawner.ChildProcessSpawner | Scope.Scope> {
  return Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const resolved = yield* resolveSpawnCommand(input.binaryPath || "mastracode", LOGIN_ARGS, {
      env: input.environment,
    }).pipe(
      Effect.mapError(() =>
        setupError(input.instanceId, "start", "Mastra Code CLI could not be started."),
      ),
    );
    const child = yield* spawner
      .spawn(
        ChildProcess.make(resolved.command, resolved.args, {
          env: input.environment,
          extendEnv: false,
          shell: resolved.shell,
        }),
      )
      .pipe(
        Effect.mapError(() =>
          setupError(
            input.instanceId,
            "start",
            "Mastra Code sign-in process could not be started.",
          ),
        ),
      );
    yield* Effect.addFinalizer(() =>
      child.kill({ forceKillAfter: "1 second" }).pipe(Effect.ignore),
    );

    let observedChars = 0;
    let observedBytes = 0;
    let deviceCodeSeen = false;
    let loginSucceeded = false;
    let loginFailed = false;
    let loginErrorCode: string | undefined;
    let outputInvalid = false;

    const stdoutFiber = yield* child.stdout
      .pipe(
        Stream.mapEffect((chunk) =>
          Effect.gen(function* () {
            if (outputInvalid) return new Uint8Array();
            observedBytes += chunk.byteLength;
            if (observedBytes <= MAX_AUTH_OUTPUT_BYTES) return chunk;
            outputInvalid = true;
            loginErrorCode = "INVALID_OUTPUT";
            yield* child.kill({ forceKillAfter: "1 second" }).pipe(Effect.ignore);
            return new Uint8Array();
          }),
        ),
        Stream.decodeText(),
        Stream.splitLines,
        Stream.runForEach((line) =>
          Effect.gen(function* () {
            if (outputInvalid) return;
            observedChars += line.length;
            if (line.length > MAX_AUTH_LINE_CHARS || observedChars > MAX_AUTH_OUTPUT_CHARS) {
              outputInvalid = true;
              yield* child.kill({ forceKillAfter: "1 second" }).pipe(Effect.ignore);
              return;
            }
            const parsedLine = parseMastraCodeAuthJsonlLine(line);
            if (parsedLine.type === "empty") {
              return;
            }
            if (parsedLine.type === "invalid") {
              outputInvalid = true;
              loginErrorCode = "INVALID_OUTPUT";
              yield* child.kill({ forceKillAfter: "1 second" }).pipe(Effect.ignore);
              return;
            }
            if (outputInvalid || loginFailed) return;
            const event = parsedLine.event;

            if (loginSucceeded) {
              loginFailed = true;
              loginErrorCode = "LOGIN_FAILED";
              yield* child.kill({ forceKillAfter: "1 second" }).pipe(Effect.ignore);
              return;
            }

            if (event.type === "device_code") {
              if (deviceCodeSeen) {
                outputInvalid = true;
                loginErrorCode = "INVALID_OUTPUT";
                yield* child.kill({ forceKillAfter: "1 second" }).pipe(Effect.ignore);
                return;
              }
              const now = yield* Clock.currentTimeMillis;
              const deviceCode = parseMastraCodeDeviceCodeEvent(event, now);
              if (!deviceCode) {
                loginErrorCode = "INVALID_DEVICE_CODE";
                outputInvalid = true;
                yield* child.kill({ forceKillAfter: "1 second" }).pipe(Effect.ignore);
                return;
              }
              deviceCodeSeen = true;
              yield* input.context.setInteraction({
                type: "deviceCode",
                id: input.context.flowId,
                url: deviceCode.url,
                userCode: deviceCode.userCode,
              });
              return;
            }

            if (event.type === "success" && deviceCodeSeen) {
              loginSucceeded = true;
              yield* input.context.verifying;
              return;
            }
            if (event.type === "error") {
              loginFailed = true;
              loginErrorCode = event.code;
              yield* child.kill({ forceKillAfter: "1 second" }).pipe(Effect.ignore);
              return;
            }
            if (event.type === "success") {
              outputInvalid = true;
              loginErrorCode = "INVALID_OUTPUT";
              yield* child.kill({ forceKillAfter: "1 second" }).pipe(Effect.ignore);
            }
          }),
        ),
      )
      .pipe(Effect.forkScoped);
    const stderrFiber = yield* child.stderr.pipe(Stream.runDrain).pipe(Effect.forkScoped);
    const exitCode = yield* child.exitCode.pipe(
      Effect.map(Number),
      Effect.mapError(() => setupError(input.instanceId, "start", "Mastra Code sign-in failed.")),
    );
    yield* Fiber.join(stdoutFiber).pipe(
      Effect.mapError(() =>
        setupError(input.instanceId, "start", "Mastra Code sign-in output could not be read."),
      ),
    );
    yield* Fiber.join(stderrFiber).pipe(
      Effect.mapError(() =>
        setupError(input.instanceId, "start", "Mastra Code sign-in output could not be read."),
      ),
    );

    if (
      isMastraCodeAuthLoginSuccessful({
        exitCode,
        loginSucceeded,
        loginFailed,
        outputInvalid,
      })
    ) {
      return;
    }
    if (outputInvalid) {
      return yield* setupError(
        input.instanceId,
        "start",
        "Mastra Code returned invalid sign-in output.",
      );
    }
    if (loginErrorCode === "LOGIN_CANCELLED") {
      return yield* setupError(
        input.instanceId,
        "start",
        "Codex sign-in was cancelled. Start again.",
      );
    }
    if (loginErrorCode === "AUTH_STORE_UNREADABLE") {
      return yield* setupError(
        input.instanceId,
        "start",
        "Mastra Code could not read its sign-in store.",
      );
    }
    return yield* setupError(
      input.instanceId,
      "start",
      "Codex sign-in for Mastra Code failed. Try again.",
    );
  }).pipe(Effect.scoped);
}

function runLogout(input: {
  readonly instanceId: ProviderInstanceId;
  readonly binaryPath: string;
  readonly environment: NodeJS.ProcessEnv;
}): Effect.Effect<void, ProviderSetupError, ChildProcessSpawner.ChildProcessSpawner> {
  return Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const resolved = yield* resolveSpawnCommand(input.binaryPath || "mastracode", LOGOUT_ARGS, {
      env: input.environment,
    }).pipe(
      Effect.mapError(() =>
        setupError(input.instanceId, "logout", "Mastra Code CLI could not be started."),
      ),
    );
    const child = yield* spawner
      .spawn(
        ChildProcess.make(resolved.command, resolved.args, {
          env: input.environment,
          extendEnv: false,
          shell: resolved.shell,
        }),
      )
      .pipe(
        Effect.mapError(() =>
          setupError(
            input.instanceId,
            "logout",
            "Mastra Code sign-out process could not be started.",
          ),
        ),
      );
    yield* Effect.addFinalizer(() =>
      child.kill({ forceKillAfter: "1 second" }).pipe(Effect.ignore),
    );
    const [exitCode] = yield* Effect.all(
      [
        child.exitCode.pipe(Effect.map(Number)),
        child.stdout.pipe(Stream.runDrain),
        child.stderr.pipe(Stream.runDrain),
      ],
      { concurrency: "unbounded" },
    ).pipe(
      Effect.mapError(() => setupError(input.instanceId, "logout", "Mastra Code sign-out failed.")),
      Effect.timeoutOrElse({
        duration: "30 seconds",
        orElse: () =>
          Effect.fail(
            setupError(input.instanceId, "logout", "Mastra Code sign-out timed out. Try again."),
          ),
      }),
    );
    if (exitCode !== 0) {
      return yield* setupError(
        input.instanceId,
        "logout",
        "Mastra Code sign-out failed. Try again.",
      );
    }
  }).pipe(Effect.scoped);
}

export const makeMastraCodeAuth = Effect.fn("makeMastraCodeAuth")(function* (input: {
  readonly instanceId: ProviderInstanceId;
  readonly binaryPath: string;
  readonly appDataDirectory: string;
  readonly environment: NodeJS.ProcessEnv;
}): Effect.fn.Return<
  ProviderAuthController,
  never,
  Crypto.Crypto | ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
> {
  const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const methods: ReadonlyArray<ProviderAuthMethod> = [
    {
      id: "openai-codex-device",
      name: "Sign in with OpenAI Codex",
      description: "Use a device code to sign in to the Codex account stored by Mastra Code.",
      type: "agent",
    },
  ];
  const auth = yield* ProviderAuthFlow.make({
    instanceId: input.instanceId,
    credentialBinding: { owner: "provider", key: `mastracode:${input.appDataDirectory}` },
    methods: Effect.succeed(methods),
    defaultMethodId: "openai-codex-device",
    timeoutMs: 15 * 60 * 1000,
    authenticate: (_methodId, context) =>
      runLogin({
        instanceId: input.instanceId,
        binaryPath: input.binaryPath,
        environment: input.environment,
        context,
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner)),
    logout: runLogout({
      instanceId: input.instanceId,
      binaryPath: input.binaryPath,
      environment: input.environment,
    }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner)),
  });
  return auth;
});
