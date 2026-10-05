import {
  MastraCodeSettings,
  ProviderDriverKind,
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderModel,
  type ServerProviderSlashCommand,
} from "@t3tools/contracts";
import type * as EffectAcpSchema from "effect-acp/schema";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  buildServerProvider,
  isCommandMissingCause,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import type { ServerProviderShape } from "../Services/ServerProvider.ts";

const DRIVER = ProviderDriverKind.make("mastraCode");
const PRESENTATION = {
  displayName: "Mastra Code",
  showInteractionModeToggle: true,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({ optionDescriptors: [] });
const INFO_TIMEOUT_MS = 5_000;

const MastraCodeInfoSchema = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  version: Schema.String,
  acpProtocolVersion: Schema.Literal(1),
  capabilities: Schema.Struct({
    loadSession: Schema.Boolean,
    permissions: Schema.Boolean,
    elicitation: Schema.Boolean,
    images: Schema.Boolean,
  }),
  models: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      modes: Schema.optional(Schema.Array(Schema.String)),
      thinkingLevels: Schema.optional(Schema.Array(Schema.String)),
    }),
  ),
  auth: Schema.Struct({
    provider: Schema.Literal("openai-codex"),
    status: Schema.Literals(["authenticated", "unauthenticated", "unknown"]),
  }),
});
type MastraCodeInfo = typeof MastraCodeInfoSchema.Type;
const decodeInfo = Schema.decodeUnknownOption(Schema.fromJsonString(MastraCodeInfoSchema));
const MAX_WORKSPACE_SNAPSHOTS = 16;

function nativeCommands(
  commands: ReadonlyArray<EffectAcpSchema.AvailableCommand>,
): ReadonlyArray<ServerProviderSlashCommand> {
  const seen = new Set<string>();
  return commands.flatMap((command) => {
    const name = command.name.trim();
    if (!name || seen.has(name)) return [];
    seen.add(name);
    const description = command.description.trim();
    const hint = typeof command.input?.hint === "string" ? command.input.hint.trim() : undefined;
    return [
      {
        name,
        ...(description ? { description } : {}),
        ...(hint ? { input: { hint } } : {}),
      },
    ];
  });
}

/** Keep ACP slash commands scoped to the workspace where Mastra Code discovered them. */
export const makeMastraCodeCommandCatalog = Effect.fn("makeMastraCodeCommandCatalog")(function* (
  provider: ServerProviderShape,
) {
  const workspaces = yield* SubscriptionRef.make<NonNullable<ServerProvider["workspaceSnapshots"]>>(
    [],
  );
  const getSnapshot = Effect.all([provider.getSnapshot, SubscriptionRef.get(workspaces)]).pipe(
    Effect.map(([snapshot, workspaceSnapshots]) =>
      workspaceSnapshots.length > 0 ? { ...snapshot, workspaceSnapshots } : snapshot,
    ),
  );
  const snapshotForCwd = Effect.fn("MastraCodeCommandCatalog.snapshotForCwd")(function* (
    cwd: string,
  ) {
    const machineSnapshot = yield* provider.getSnapshot;
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    yield* SubscriptionRef.update(workspaces, (entries) =>
      [
        ...entries.filter((entry) => entry.cwd !== cwd),
        {
          cwd,
          checkedAt,
          slashCommands:
            entries.find((entry) => entry.cwd === cwd)?.slashCommands ??
            machineSnapshot.slashCommands,
          skills: [],
        },
      ].slice(-MAX_WORKSPACE_SNAPSHOTS),
    );
    const snapshot = yield* getSnapshot;
    const workspace = snapshot.workspaceSnapshots?.find((entry) => entry.cwd === cwd);
    return {
      ...snapshot,
      checkedAt,
      slashCommands: workspace?.slashCommands ?? snapshot.slashCommands,
    };
  });
  const onAvailableCommands = Effect.fn("MastraCodeCommandCatalog.onAvailableCommands")(function* (
    commands: ReadonlyArray<EffectAcpSchema.AvailableCommand>,
    cwd: string,
  ) {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const slashCommands = nativeCommands(commands);
    yield* SubscriptionRef.update(workspaces, (entries) =>
      [
        ...entries.filter((entry) => entry.cwd !== cwd),
        { cwd, checkedAt, slashCommands, skills: [] },
      ].slice(-MAX_WORKSPACE_SNAPSHOTS),
    );
  });
  return {
    onAvailableCommands,
    snapshotForCwd,
    snapshot: {
      ...provider,
      getSnapshot,
      refresh: provider.refresh.pipe(Effect.andThen(getSnapshot)),
      streamChanges: Stream.merge(
        provider.streamChanges.pipe(Stream.map(() => undefined)),
        SubscriptionRef.changes(workspaces).pipe(Stream.map(() => undefined)),
      ).pipe(Stream.mapEffect(() => getSnapshot)),
    } satisfies ServerProviderShape,
  };
});

export function parseMastraCodeInfo(output: string): MastraCodeInfo | undefined {
  return Option.getOrUndefined(decodeInfo(output));
}

function modelsFromInfo(info: MastraCodeInfo | undefined): ReadonlyArray<ServerProviderModel> {
  const seen = new Set<string>();
  const models = info?.models ?? [];
  const defaultBuildModel = models
    .find(({ id, modes }) => id.trim() && (modes === undefined || modes.includes("build")))
    ?.id.trim();
  return models.flatMap(({ id, modes, thinkingLevels }) => {
    const slug = id.trim();
    if (!slug || slug.length > 256 || seen.has(slug)) return [];
    seen.add(slug);
    return [
      {
        slug,
        name: slug,
        isCustom: false,
        ...(slug === defaultBuildModel ? { isDefault: true } : {}),
        ...(modes === undefined
          ? {}
          : {
              supportedInteractionModes: modes.flatMap((mode) =>
                mode === "build" || mode === "fast"
                  ? ["default" as const]
                  : mode === "plan"
                    ? ["plan" as const]
                    : [],
              ),
            }),
        capabilities: createModelCapabilities({
          optionDescriptors: thinkingLevels?.length
            ? [
                {
                  id: "thought_level",
                  label: "Reasoning effort",
                  type: "select",
                  options: [...new Set(thinkingLevels)].map((level) => ({
                    id: level,
                    label: level.charAt(0).toUpperCase() + level.slice(1),
                  })),
                },
              ]
            : [],
        }),
      },
    ];
  });
}

function snapshot(input: {
  readonly settings: MastraCodeSettings;
  readonly checkedAt: string;
  readonly info?: MastraCodeInfo;
  readonly installed: boolean;
  readonly status: "ready" | "warning" | "error";
  readonly message?: string;
}): ServerProviderDraft {
  const info = input.info;
  const authStatus = info?.auth.status ?? "unknown";
  const models = providerModelsFromSettings(
    modelsFromInfo(info),
    input.settings.customModels,
    EMPTY_CAPABILITIES,
  );
  return {
    ...buildServerProvider({
      driver: DRIVER,
      presentation: PRESENTATION,
      enabled: input.settings.enabled,
      checkedAt: input.checkedAt,
      models,
      probe: {
        installed: input.installed,
        version: info?.version.trim() || null,
        status: input.status,
        auth: { status: authStatus },
        ...(input.message ? { message: input.message } : {}),
      },
    }),
    setup: { canAuthenticate: true, canInstall: false },
    supportsTextGeneration: false,
  };
}

export function buildInitialMastraCodeProviderSnapshot(
  settings: MastraCodeSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    return snapshot({
      settings,
      checkedAt,
      installed: false,
      status: "warning",
      message: settings.enabled
        ? "Checking Mastra Code availability..."
        : "Mastra Code is disabled in T3 Code settings.",
    });
  });
}

export const checkMastraCodeProviderStatus = Effect.fn("checkMastraCodeProviderStatus")(function* (
  settings: MastraCodeSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  if (!settings.enabled) {
    return snapshot({
      settings,
      checkedAt,
      installed: false,
      status: "warning",
      message: "Mastra Code is disabled in T3 Code settings.",
    });
  }

  const command = settings.binaryPath || "mastracode";
  const result = yield* Effect.result(
    Effect.gen(function* () {
      const resolved = yield* resolveSpawnCommand(command, ["info", "--json"], {
        env: environment,
      });
      return yield* spawnAndCollect(
        command,
        ChildProcess.make(resolved.command, resolved.args, {
          env: environment,
          extendEnv: false,
          shell: resolved.shell,
        }),
      );
    }).pipe(Effect.timeoutOption(INFO_TIMEOUT_MS)),
  );

  if (Result.isFailure(result)) {
    return snapshot({
      settings,
      checkedAt,
      installed: !isCommandMissingCause(result.failure),
      status: "error",
      message: isCommandMissingCause(result.failure)
        ? `Mastra Code CLI was not found at '${command}'. Install it or set its binary path.`
        : "Could not read Mastra Code runtime information.",
    });
  }
  if (Option.isNone(result.success)) {
    return snapshot({
      settings,
      checkedAt,
      installed: true,
      status: "error",
      message: `Mastra Code info check timed out after ${INFO_TIMEOUT_MS}ms.`,
    });
  }

  const commandResult = result.success.value;
  const info = parseMastraCodeInfo(commandResult.stdout);
  if (!info) {
    return snapshot({
      settings,
      checkedAt,
      installed: true,
      status: "error",
      message: "Mastra Code returned unsupported or invalid machine-readable runtime information.",
    });
  }

  const missingCapabilities = Object.entries(info.capabilities)
    .filter(([, supported]) => !supported)
    .map(([name]) => name);
  if (missingCapabilities.length > 0) {
    return snapshot({
      settings,
      checkedAt,
      info,
      installed: true,
      status: "error",
      message: `Mastra Code ACP is missing required capabilities: ${missingCapabilities.join(", ")}.`,
    });
  }
  // `info --json` deliberately exits 1 when the auth file is absent or
  // unreadable while still returning valid metadata with auth.status=unknown.
  if (commandResult.code !== 0 && !(commandResult.code === 1 && info.auth.status === "unknown")) {
    return snapshot({
      settings,
      checkedAt,
      info,
      installed: true,
      status: "error",
      message: "Mastra Code info check exited unsuccessfully.",
    });
  }
  if (info.auth.status !== "authenticated") {
    return snapshot({
      settings,
      checkedAt,
      info,
      installed: true,
      status: "warning",
      message:
        info.auth.status === "unknown"
          ? "Mastra Code could not verify its OpenAI Codex sign-in. Check its app data directory."
          : "Sign in to OpenAI Codex for Mastra Code from provider setup.",
    });
  }
  return snapshot({ settings, checkedAt, info, installed: true, status: "ready" });
});
