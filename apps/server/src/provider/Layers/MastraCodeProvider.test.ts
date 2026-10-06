import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { MastraCodeSettings, ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";

import {
  buildInitialMastraCodeProviderSnapshot,
  checkMastraCodeProviderStatus,
  makeMastraCodeCommandCatalog,
  parseMastraCodeInfo,
  makeMastraCodeCatalogRefresh,
} from "./MastraCodeProvider.ts";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";

const decodeSettings = Schema.decodeSync(MastraCodeSettings);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const runtimeInfo = {
  schemaVersion: 1,
  version: "0.42.3-alpha.4",
  acpProtocolVersion: 1,
  thinkingLevelDescription: "MC requested levels, not verified backend capabilities.",
  capabilities: {
    loadSession: true,
    permissions: true,
    elicitation: true,
    images: true,
  },
  models: [
    {
      id: "openai-codex/gpt-5",
      modes: ["build", "plan"],
      thinkingLevels: ["off", "low", "high"],
      defaultThinkingLevel: "off",
    },
  ],
  auth: { provider: "openai-codex", status: "authenticated" },
  catalog: {
    source: "account-cache",
    status: "ready",
    clientVersion: "0.160.0",
    fetchedAt: 0,
    expiresAt: 3_600_000,
  },
};

describe("parseMastraCodeInfo", () => {
  it("accepts the versioned machine-readable runtime contract", () => {
    expect(parseMastraCodeInfo(JSON.stringify(runtimeInfo))).toEqual(runtimeInfo);
  });

  it("rejects unsupported schemas and any auth provider other than Codex", () => {
    expect(
      parseMastraCodeInfo(JSON.stringify({ ...runtimeInfo, schemaVersion: 2 })),
    ).toBeUndefined();
    expect(
      parseMastraCodeInfo(JSON.stringify({ ...runtimeInfo, acpProtocolVersion: "1" })),
    ).toBeUndefined();
    expect(
      parseMastraCodeInfo(
        JSON.stringify({
          ...runtimeInfo,
          auth: { provider: "openai", status: "authenticated" },
        }),
      ),
    ).toBeUndefined();
    expect(parseMastraCodeInfo("not json")).toBeUndefined();
  });
});

describe("buildInitialMastraCodeProviderSnapshot", () => {
  it.effect("keeps the provider off by default", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialMastraCodeProviderSnapshot(decodeSettings({}));
      expect(snapshot.enabled).toBe(false);
      expect(snapshot.status).toBe("disabled");
      expect(snapshot.installed).toBe(false);
      expect(snapshot.models).toEqual([]);
      expect(snapshot.supportsTextGeneration).toBe(false);
    }),
  );

  it.effect("shows a pending check when explicitly enabled", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialMastraCodeProviderSnapshot(
        decodeSettings({ enabled: true }),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.status).toBe("warning");
      expect(snapshot.installed).toBe(false);
      expect(snapshot.message).toContain("Checking Mastra Code");
    }),
  );
});

describe("Mastra Code ACP command catalog", () => {
  it.effect("publishes native slash commands per workspace and retains them on refresh", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const draft = yield* buildInitialMastraCodeProviderSnapshot(
          decodeSettings({ enabled: true }),
        );
        const base = {
          ...draft,
          instanceId: ProviderInstanceId.make("mastra-command-catalog-test"),
          driver: ProviderDriverKind.make("mastraCode"),
        };
        const provider = yield* makeMastraCodeCommandCatalog({
          getSnapshot: Effect.succeed(base),
          refresh: Effect.succeed(base),
          streamChanges: Stream.empty,
          resolveMaintenance: () => Effect.die("not used"),
          applyUsageLimits: () => Effect.void,
        });
        const review = [
          {
            name: "review",
            description: "Review changes",
            input: { type: "unstructured", hint: "target" },
          },
        ];
        yield* provider.snapshotForCwd("/workspace/one");
        yield* provider.onAvailableCommands(
          [
            ...review,
            { name: "review", description: "duplicate" },
            { name: "", description: "invalid" },
          ],
          "/workspace/one",
        );
        yield* provider.onAvailableCommands(
          [{ name: "test", description: "Run tests" }],
          "/workspace/two",
        );

        const refreshed = yield* provider.snapshot.refresh;
        expect(refreshed.slashCommands).toEqual([]);
        expect(refreshed.workspaceSnapshots).toEqual([
          {
            cwd: "/workspace/one",
            checkedAt: expect.any(String),
            slashCommands: [
              { name: "review", description: "Review changes", input: { hint: "target" } },
            ],
            skills: [],
          },
          {
            cwd: "/workspace/two",
            checkedAt: expect.any(String),
            slashCommands: [{ name: "test", description: "Run tests" }],
            skills: [],
          },
        ]);
        expect((yield* provider.snapshotForCwd("/workspace/one")).slashCommands).toEqual([
          { name: "review", description: "Review changes", input: { hint: "target" } },
        ]);
      }),
    ),
  );
});

it.layer(NodeServices.layer)("checkMastraCodeProviderStatus", (it) => {
  const writeInfoCli = (info: unknown, exitCode: number) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const directory = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3code-mastracode-info-",
      });
      return writeFakeCli({
        directory,
        name: "mastracode",
        source: [
          'if (process.argv[2] !== "info" || process.argv[3] !== "--json") process.exit(2);',
          `process.stdout.write(${encodeJson(`${encodeJson(info)}\n`)});`,
          `process.exitCode = ${exitCode};`,
        ].join("\n"),
      });
    });

  const buildStatus = (binaryPath: string, now = 0) =>
    TestClock.setTime(now).pipe(
      Effect.andThen(
        checkMastraCodeProviderStatus(decodeSettings({ enabled: true, binaryPath }), {
          ...process.env,
          MASTRA_APP_DATA_DIR: "/tmp/mastracode-test-app-data",
        }),
      ),
    );

  it.effect("treats valid metadata plus unknown auth as a sign-in warning", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const binaryPath = yield* writeInfoCli(
          { ...runtimeInfo, auth: { provider: "openai-codex", status: "unknown" } },
          1,
        );
        const snapshot = yield* buildStatus(binaryPath);
        expect(snapshot.installed).toBe(true);
        expect(snapshot.status).toBe("warning");
        expect(snapshot.auth.status).toBe("unknown");
      }),
    ),
  );

  it.effect("reports ready for an authenticated CLI", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const binaryPath = yield* writeInfoCli(runtimeInfo, 0);
        const snapshot = yield* buildStatus(binaryPath);
        expect(snapshot.installed).toBe(true);
        expect(snapshot.status).toBe("ready");
        expect(snapshot.supportsTextGeneration).toBe(false);
        expect(snapshot.models).toMatchObject([{ slug: "openai-codex/gpt-5", isDefault: true }]);
        expect(snapshot.models[0]?.capabilities?.optionDescriptors).toMatchObject([
          {
            id: "thought_level",
            label: "Reasoning",
            type: "select",
            description: expect.stringContaining("not verified backend capabilities"),
            options: [
              { id: "off", label: "Default", isDefault: true },
              { id: "low" },
              { id: "high" },
            ],
            currentValue: "off",
          },
        ]);
      }),
    ),
  );

  it.effect("does not trust an older authenticated CLI's unverified registry inventory", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { catalog: _catalog, ...oldInfo } = runtimeInfo;
        const binaryPath = yield* writeInfoCli(oldInfo, 0);
        const snapshot = yield* buildStatus(binaryPath);
        expect(snapshot.auth.status).toBe("authenticated");
        expect(snapshot.status).toBe("warning");
        expect(snapshot.models).toEqual([]);
      }),
    ),
  );

  it.effect("does not advertise an expired account catalog through an offline probe", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const binaryPath = yield* writeInfoCli(runtimeInfo, 0);
        const snapshot = yield* buildStatus(binaryPath, runtimeInfo.catalog.expiresAt);
        expect(snapshot.status).toBe("warning");
        expect(snapshot.models).toEqual([]);
      }),
    ),
  );

  it.effect("refreshes through the native command once, then admits from the offline cache", () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(0);
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-mc-catalog-refresh-" });
        const log = `${directory}/commands.jsonl`;
        const marker = `${directory}/fresh`;
        const binaryPath = writeFakeCli({
          directory,
          name: "mastracode",
          source: [
            'import * as fs from "node:fs";',
            `const log = ${encodeJson(log)}; const marker = ${encodeJson(marker)};`,
            'fs.appendFileSync(log, JSON.stringify({ args: process.argv.slice(2), isolated: process.env.MASTRA_APP_DATA_DIR }) + "\\n");',
            'if (process.argv[2] === "catalog") {',
            '  if (process.argv.slice(2).join(" ") !== "catalog refresh --provider openai-codex --json") process.exit(2);',
            `  fs.writeFileSync(marker, "ready"); process.stdout.write(JSON.stringify({ type: "success", provider: "openai-codex", catalog: ${encodeJson(runtimeInfo.catalog)} }));`,
            '} else if (process.argv[2] === "info") {',
            `  const info = ${encodeJson(runtimeInfo)};`,
            '  if (!fs.existsSync(marker)) { info.catalog.status = "missing"; info.models = []; }',
            "  process.stdout.write(JSON.stringify(info));",
            "} else process.exit(2);",
          ].join("\n"),
        });
        const refresh = yield* makeMastraCodeCatalogRefresh(
          decodeSettings({ enabled: true, binaryPath }),
          {
            ...process.env,
            MASTRA_APP_DATA_DIR: directory,
          },
        );
        expect(yield* refresh()).toBe(true);
        expect(yield* refresh()).toBe(true);
        expect(yield* refresh(true)).toBe(true);
        const calls = (yield* fs.readFileString(log))
          .trim()
          .split("\n")
          .map((line) => JSON.parse(line));
        expect(calls.map((call) => call.args)).toEqual([
          ["info", "--json"],
          ["catalog", "refresh", "--provider", "openai-codex", "--json"],
          ["info", "--json"],
          ["info", "--json"],
          ["info", "--json"],
          ["catalog", "refresh", "--provider", "openai-codex", "--json"],
          ["info", "--json"],
        ]);
        expect(calls.every((call) => call.isolated === directory)).toBe(true);
      }),
    ),
  );

  it.effect("interrupts the owned catalog child without publishing a ready catalog", () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(0);
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-mc-catalog-cancel-" });
        const started = directory + "/started";
        const stopped = directory + "/stopped";
        const binaryPath = writeFakeCli({
          directory,
          name: "mastracode",
          source: [
            'import { writeFileSync } from "node:fs";',
            'if (process.argv[2] === "info") {',
            "process.stdout.write(JSON.stringify(" +
              encodeJson({
                ...runtimeInfo,
                catalog: { ...runtimeInfo.catalog, status: "missing" },
                models: [],
              }) +
              "));",
            '} else if (process.argv[2] === "catalog") {',
            'process.on("SIGTERM", () => { writeFileSync(' +
              encodeJson(stopped) +
              ', "stopped"); process.exit(0); });',
            "writeFileSync(" + encodeJson(started) + ', "started");',
            "setInterval(() => {}, 1000);",
            "} else process.exit(2);",
          ].join("\n"),
        });
        const refresh = yield* makeMastraCodeCatalogRefresh(
          decodeSettings({ enabled: true, binaryPath }),
          { ...process.env, MASTRA_APP_DATA_DIR: directory },
        );
        const running = yield* refresh().pipe(Effect.forkChild);
        while (!(yield* fs.exists(started))) {
          yield* Effect.yieldNow;
        }
        yield* Fiber.interrupt(running);
        expect(yield* fs.exists(stopped)).toBe(true);
        const snapshot = yield* checkMastraCodeProviderStatus(
          decodeSettings({ enabled: true, binaryPath }),
          { ...process.env, MASTRA_APP_DATA_DIR: directory },
        );
        expect(snapshot.status).toBe("warning");
        expect(snapshot.models).toEqual([]);
      }),
    ),
  );

  it.effect("rejects malformed thinking levels and deduplicates native option IDs", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const binaryPath = yield* writeInfoCli(
          {
            ...runtimeInfo,
            models: [
              {
                ...runtimeInfo.models[0],
                thinkingLevels: ["", " ", " high ", "off", "low", "low", "high"],
              },
            ],
          },
          0,
        );
        const snapshot = yield* buildStatus(binaryPath);
        const descriptor = snapshot.models[0]?.capabilities?.optionDescriptors?.[0];
        expect(descriptor).toMatchObject({
          id: "thought_level",
          options: [{ id: "off" }, { id: "low" }, { id: "high" }],
        });
        if (descriptor?.type !== "select") throw new Error("Expected reasoning select descriptor");
        expect(descriptor.options).toHaveLength(3);
      }),
    ),
  );

  it.effect("selects the first build-capable model from Mastra Code's live inventory", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const binaryPath = yield* writeInfoCli(
          {
            ...runtimeInfo,
            models: [
              { id: "model-plan-only", modes: ["plan"] },
              { id: "model-build-first", modes: ["build", "plan"] },
              { id: "model-build-second", modes: ["build"] },
              { id: "openai/gpt-5.4-mini", modes: ["fast"] },
              { id: "model-unconstrained" },
            ],
          },
          0,
        );
        const snapshot = yield* buildStatus(binaryPath);

        expect(snapshot.models.map((model) => model.supportedInteractionModes)).toEqual([
          ["plan"],
          ["default", "plan"],
          ["default"],
          ["default"],
          undefined,
        ]);
        expect(snapshot.models.map(({ slug, isDefault }) => [slug, isDefault ?? false])).toEqual([
          ["model-plan-only", false],
          ["model-build-first", true],
          ["model-build-second", false],
          ["openai/gpt-5.4-mini", false],
          ["model-unconstrained", false],
        ]);
      }),
    ),
  );
});
