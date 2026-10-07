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
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  buildServerProvider,
  isCommandMissingCause,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "./providerSnapshot.ts";
import type { ServerProviderShape } from "./ServerProvider.ts";

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
  thinkingLevelDescription: Schema.optional(Schema.String),
  catalog: Schema.optional(
    Schema.Struct({
      source: Schema.Literal("account-cache"),
      status: Schema.Literals(["ready", "unbound", "missing", "invalid", "foreign", "expired"]),
      clientVersion: Schema.String,
      fetchedAt: Schema.optional(Schema.Number),
      expiresAt: Schema.optional(Schema.Number),
    }),
  ),
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
      defaultThinkingLevel: Schema.optional(Schema.String),
    }),
  ),
  auth: Schema.Struct({
    provider: Schema.Literal("openai-codex"),
    status: Schema.Literals(["authenticated", "unauthenticated", "unknown"]),
  }),
});
type MastraCodeInfo = typeof MastraCodeInfoSchema.Type;
const decodeInfo = Schema.decodeUnknownOption(Schema.fromJsonString(MastraCodeInfoSchema));
const decodeCatalogRefresh = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({
      type: Schema.Literal("success"),
      provider: Schema.Literal("openai-codex"),
      catalog: Schema.Struct({
        source: Schema.Literal("account-cache"),
        status: Schema.Literal("ready"),
        clientVersion: Schema.Literal("0.160.0"),
        fetchedAt: Schema.Number,
        expiresAt: Schema.Number,
      }),
    }),
  ),
);
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

function hasFreshCatalog(info: MastraCodeInfo | undefined, now: number): boolean {
  const catalog = info?.catalog;
  return (
    info?.auth.status === "authenticated" &&
    catalog?.status === "ready" &&
    catalog.clientVersion === "0.160.0" &&
    typeof catalog.fetchedAt === "number" &&
    typeof catalog.expiresAt === "number" &&
    Number.isFinite(catalog.fetchedAt) &&
    Number.isFinite(catalog.expiresAt) &&
    catalog.fetchedAt <= now &&
    catalog.expiresAt > now &&
    catalog.expiresAt - catalog.fetchedAt === 3_600_000
  );
}

function collectMachineCommand(
  settings: MastraCodeSettings,
  environment: NodeJS.ProcessEnv,
  args: ReadonlyArray<string>,
  timeoutMs: number,
) {
  const command = settings.binaryPath || "mastracode";
  return Effect.result(
    Effect.gen(function* () {
      const resolved = yield* resolveSpawnCommand(command, [...args], { env: environment });
      return yield* spawnAndCollect(
        command,
        ChildProcess.make(resolved.command, resolved.args, {
          env: environment,
          extendEnv: false,
          shell: resolved.shell,
        }),
      );
    }).pipe(Effect.timeoutOption(timeoutMs)),
  );
}

/** Only explicit admission/setup/refresh calls use this writer command; probes stay offline. */
export const makeMastraCodeCatalogRefresh = Effect.fn("makeMastraCodeCatalogRefresh")(function* (
  settings: MastraCodeSettings,
  environment: NodeJS.ProcessEnv,
) {
  const semaphore = yield* Semaphore.make(1);
  const refresh = (force = false) =>
    semaphore.withPermits(1)(
      Effect.gen(function* () {
        if (!settings.enabled) return false;
        const read = yield* collectMachineCommand(
          settings,
          environment,
          ["info", "--json"],
          INFO_TIMEOUT_MS,
        );
        if (Result.isFailure(read) || Option.isNone(read.success)) return false;
        const info = parseMastraCodeInfo(read.success.value.stdout);
        if (info?.auth.status !== "authenticated") return false;
        if (
          !force &&
          read.success.value.code === 0 &&
          hasFreshCatalog(info, yield* Clock.currentTimeMillis)
        )
          return true;
        const updated = yield* collectMachineCommand(
          settings,
          environment,
          ["catalog", "refresh", "--provider", "openai-codex", "--json"],
          35_000,
        );
        if (
          Result.isFailure(updated) ||
          Option.isNone(updated.success) ||
          updated.success.value.code !== 0
        )
          return false;
        const receipt = Option.getOrUndefined(decodeCatalogRefresh(updated.success.value.stdout));
        if (!receipt) return false;
        const reread = yield* collectMachineCommand(
          settings,
          environment,
          ["info", "--json"],
          INFO_TIMEOUT_MS,
        );
        if (
          Result.isFailure(reread) ||
          Option.isNone(reread.success) ||
          reread.success.value.code !== 0
        )
          return false;
        const current = parseMastraCodeInfo(reread.success.value.stdout);
        return (
          hasFreshCatalog(current, yield* Clock.currentTimeMillis) &&
          current?.catalog?.fetchedAt === receipt.catalog.fetchedAt &&
          current.catalog.expiresAt === receipt.catalog.expiresAt
        );
      }),
    );
  return refresh;
});

function modelsFromInfo(
  info: MastraCodeInfo | undefined,
  now: number,
): ReadonlyArray<ServerProviderModel> {
  const seen = new Set<string>();
  const models = hasFreshCatalog(info, now) ? (info?.models ?? []) : [];
  return models.flatMap(({ id, thinkingLevels, defaultThinkingLevel }) => {
    const slug = id.trim();
    if (!slug || slug.length > 256 || seen.has(slug)) return [];
    seen.add(slug);
    const levels = [
      ...new Set(
        (thinkingLevels ?? []).filter(
          (level) => level.length > 0 && level.length <= 256 && level === level.trim(),
        ),
      ),
    ];
    return [
      {
        slug,
        name: slug,
        isCustom: false,
        capabilities: createModelCapabilities({
          optionDescriptors: levels.length
            ? [
                {
                  id: "thought_level",
                  label: "Reasoning",
                  description:
                    info?.thinkingLevelDescription?.trim() ||
                    "Requested MC levels; the provider may adjust or reject the selected value.",
                  type: "select",
                  options: levels.map((level) => ({
                    id: level,
                    label:
                      level === "off" ? "Default" : level.charAt(0).toUpperCase() + level.slice(1),
                    ...(level === defaultThinkingLevel ? { isDefault: true } : {}),
                  })),
                  ...(defaultThinkingLevel && levels.includes(defaultThinkingLevel)
                    ? { currentValue: defaultThinkingLevel }
                    : {}),
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
    modelsFromInfo(info, Date.parse(input.checkedAt)),
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
  const result = yield* collectMachineCommand(
    settings,
    environment,
    ["info", "--json"],
    INFO_TIMEOUT_MS,
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
  if (!hasFreshCatalog(info, yield* Clock.currentTimeMillis)) {
    return snapshot({
      settings,
      checkedAt,
      info,
      installed: true,
      status: "warning",
      message: info.catalog
        ? "Mastra Code is signed in, but its account model catalog needs refresh."
        : "Mastra Code returned an unverified model catalog. Update the paired runtime.",
    });
  }
  return snapshot({ settings, checkedAt, info, installed: true, status: "ready" });
});
