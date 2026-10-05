import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { MastraCodeSettings, ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";

import {
  buildInitialMastraCodeProviderSnapshot,
  checkMastraCodeProviderStatus,
  makeMastraCodeCommandCatalog,
  parseMastraCodeInfo,
} from "./MastraCodeProvider.ts";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";

const decodeSettings = Schema.decodeSync(MastraCodeSettings);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const runtimeInfo = {
  schemaVersion: 1,
  version: "0.42.3-alpha.4",
  acpProtocolVersion: 1,
  capabilities: {
    loadSession: true,
    permissions: true,
    elicitation: true,
    images: true,
  },
  models: [{ id: "openai-codex/gpt-5", modes: ["build", "plan"] }],
  auth: { provider: "openai-codex", status: "authenticated" },
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

  const buildStatus = (binaryPath: string) =>
    checkMastraCodeProviderStatus(decodeSettings({ enabled: true, binaryPath }), {
      ...process.env,
      MASTRA_APP_DATA_DIR: "/tmp/mastracode-test-app-data",
    });

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
          undefined,
        ]);
        expect(snapshot.models.map(({ slug, isDefault }) => [slug, isDefault ?? false])).toEqual([
          ["model-plan-only", false],
          ["model-build-first", true],
          ["model-build-second", false],
          ["model-unconstrained", false],
        ]);
      }),
    ),
  );
});
