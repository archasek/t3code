import { ProviderDriverKind, type MastraCodeSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { ChildProcessSpawner } from "effect/unstable/process";
import * as AcpErrors from "effect-acp/errors";
import { makeMastraCodeAcpRuntime } from "../../provider/acp/MastraCodeAcpSupport.ts";
import { withMastraCodeThreadStorage } from "../../provider/MastraCodeEnvironment.ts";
import { prepareMastraCodeForm } from "../../provider/MastraCodeForm.ts";
import { acquireMastraCodeFormAdmission, validateMastraCodeStringConstraints } from "../../provider/MastraCodeElicitationValidation.ts";
import { readMastraCodePlan } from "../../provider/MastraCodePlan.ts";
import { applyMastraCodeModelSelection } from "../../provider/MastraCodeModelSelection.ts";
import { acpPermissionDisposition, unknownRecord } from "../../provider/acp/AcpClientPolicy.ts";
import { AcpProviderCapabilitiesV2, makeAcpAdapterV2, type AcpAdapterV2Flavor } from "./AcpAdapterV2.ts";

export const MASTRA_CODE_PROVIDER = ProviderDriverKind.make("mastraCode");

export interface MastraCodeAdapterV2Options extends Omit<Parameters<typeof makeAcpAdapterV2>[0], "flavor"> {
  readonly settings: MastraCodeSettings;
  readonly appDataDirectory: string;
  readonly environment: NodeJS.ProcessEnv;
  readonly path: Path.Path;
  readonly platform: NodeJS.Platform;
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly onAvailableCommands?: AcpAdapterV2Flavor["onAvailableCommandsUpdate"];
  readonly wrapRuntime?: (task: ReturnType<AcpAdapterV2Flavor["makeRuntime"]>) => ReturnType<AcpAdapterV2Flavor["makeRuntime"]>;
}

/** MC-specific behavior only; canonical ACP owns turns, requests and transport. */
export function makeMastraCodeAdapterV2(options: MastraCodeAdapterV2Options) {
  const threadDirectory = (threadId: string) => options.crypto.digest("SHA-256", new TextEncoder().encode(threadId)).pipe(
    Effect.map((digest) => options.path.join(options.appDataDirectory, "threads", Encoding.encodeHex(digest))),
    Effect.mapError((cause) => new AcpErrors.AcpTransportError({ detail: "Mastra Code thread storage identity failed", cause })),
  );
  const flavor: AcpAdapterV2Flavor = {
    acquireFormElicitation: acquireMastraCodeFormAdmission,
    requireNativeSessionRestore: true,
    driver: MASTRA_CODE_PROVIDER,
    capabilities: {
      ...AcpProviderCapabilitiesV2,
      sessions: { ...AcpProviderCapabilitiesV2.sessions, supportsModelSwitchInSession: true },
    },
    runtimeHarness: "mastracode",
    ...(options.onAvailableCommands === undefined ? {} : { onAvailableCommandsUpdate: options.onAvailableCommands }),
    resolveModelId: (selection) => selection.model === "default" ? undefined : selection.model,
    applyModelSelection: applyMastraCodeModelSelection,
    sessionModeForPolicy: (policy) => policy.interactionMode === "plan" ? "plan" : "build",
    permissionDisposition: (policy, request) => request.toolCall.title?.trim() === "submit_plan"
      ? "ask" : acpPermissionDisposition(policy, request),
    preparePermissionRequest: ({ request, threadId, runtimePolicy }) => Effect.gen(function* () {
      if (request.toolCall.title?.trim() !== "submit_plan" || runtimePolicy.cwd === null) return {};
      let rawInput: unknown = request.toolCall.rawInput;
      if (typeof rawInput === "string") {
        try { rawInput = JSON.parse(rawInput); } catch { return {}; }
      }
      const rawPath = unknownRecord(rawInput)?.path;
      if (typeof rawPath !== "string" || rawPath.trim() === "") return {};
      const directory = yield* threadDirectory(threadId);
      const markdown = yield* readMastraCodePlan({ rawPath: rawPath.trim(), cwd: runtimePolicy.cwd,
        appDataDirectory: options.appDataDirectory, plansDirectory: options.path.join(directory, "plans") }).pipe(
        Effect.provideService(Path.Path, options.path),
        Effect.provideService(FileSystem.FileSystem, options.fileSystem),
      );
      return markdown?.trim() ? { proposedPlanMarkdown: markdown } : {};
    }),
    prepareFormElicitation: (input) => prepareMastraCodeForm(input, (property, answer) =>
      validateMastraCodeStringConstraints(property, answer).pipe(
        Effect.provideService(Path.Path, options.path),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, options.childProcessSpawner),
      )),
    makeRuntime: (input) => {
      const task = Effect.gen(function* () {
      if (input.threadId === null) {
        return yield* new AcpErrors.AcpTransportError({ detail: "Mastra Code requires an application thread identity", cause: undefined });
      }
      const directory = yield* threadDirectory(input.threadId);
      for (const target of [options.path.join(options.appDataDirectory, "threads"), directory, options.path.join(directory, "plans")]) {
        yield* options.fileSystem.makeDirectory(target, { recursive: true, mode: 0o700 }).pipe(
          Effect.mapError((cause) => new AcpErrors.AcpTransportError({ detail: "Mastra Code thread storage creation failed", cause })),
        );
        if (options.platform !== "win32") yield* options.fileSystem.chmod(target, 0o700).pipe(
          Effect.mapError((cause) => new AcpErrors.AcpTransportError({ detail: "Mastra Code thread storage permissions failed", cause })),
        );
      }
      const environment = withMastraCodeThreadStorage({ ...options.environment, ...input.processEnvironment }, {
        appDataDirectory: options.appDataDirectory,
        databasePath: options.path.join(directory, "mastra.db"),
        vectorDatabasePath: options.path.join(directory, "mastra-vectors.db"),
        observabilityDatabasePath: options.path.join(directory, "observability.duckdb"),
        plansDirectory: options.path.join(directory, "plans"),
      });
      return yield* makeMastraCodeAcpRuntime({ ...input, environment, settings: options.settings, childProcessSpawner: options.childProcessSpawner }).pipe(
        Effect.provideService(Path.Path, options.path),
        Effect.provideService(FileSystem.FileSystem, options.fileSystem),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, options.childProcessSpawner),
      );
      });
      return options.wrapRuntime?.(task) ?? task;
    },
  };
  return makeAcpAdapterV2({ ...options, flavor });
}
