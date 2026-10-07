import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/compat";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { resolveCommandPath } from "@t3tools/shared/shell";

import { isFullyQualifiedMastraCodePath } from "../MastraCodeEnvironment.ts";
import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";

export interface MastraCodeAcpRuntimeInput extends Omit<
  AcpSessionRuntime.AcpSessionRuntimeOptions,
  | "authMethodId"
  | "authenticateOnAuthRequired"
  | "sessionLoadRequireRpcResponse"
  | "clientCapabilities"
  | "spawn"
> {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly settings: { readonly binaryPath?: string };
  readonly environment: NodeJS.ProcessEnv;
}

const MASTRA_CODE_CLIENT_CAPABILITIES = {
  elicitation: { form: {} },
} satisfies NonNullable<EffectAcpSchema.InitializeRequest["clientCapabilities"]>;

export const makeMastraCodeAcpRuntime = (
  input: MastraCodeAcpRuntimeInput,
): Effect.Effect<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  EffectAcpErrors.AcpError,
  Crypto.Crypto | FileSystem.FileSystem | Path.Path | Scope.Scope
> =>
  Effect.gen(function* () {
    const { settings, environment, childProcessSpawner, ...runtimeOptions } = input;
    const command = settings.binaryPath?.trim() || "mastracode";
    const platform = yield* HostProcessPlatform;
    if (
      (command.includes("/") || command.includes("\\")) &&
      !isFullyQualifiedMastraCodePath(command, platform)
    ) {
      return yield* new EffectAcpErrors.AcpSpawnError({
        command,
        cause: new Error(
          "Mastra Code binaryPath must be fully qualified for the current operating system.",
        ),
      });
    }
    const resolvedCommand = yield* resolveCommandPath(command, {
      env: environment,
      extendEnv: false,
    }).pipe(Effect.mapError((cause) => new EffectAcpErrors.AcpSpawnError({ command, cause })));
    const acpContext = yield* Layer.build(
      AcpSessionRuntime.layer({
        ...runtimeOptions,
        // Do not report cancellation as successful until the ACP agent has
        // acknowledged it and the active prompt has reached a terminal state.
        cancelBehavior: "wait-for-prompt",
        spawn: {
          command: resolvedCommand,
          args: ["--acp"],
          cwd: runtimeOptions.cwd,
          env: environment,
          extendEnv: false,
        },
        // MC reads its own OAuth store; ACP authenticate is deliberately unsupported.
        authenticateOnAuthRequired: false,
        sessionLoadRequireRpcResponse: true,
        clientCapabilities: MASTRA_CODE_CLIENT_CAPABILITIES,
      }).pipe(
        Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner)),
      ),
    );
    return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
      Effect.provide(acpContext),
    );
  });
