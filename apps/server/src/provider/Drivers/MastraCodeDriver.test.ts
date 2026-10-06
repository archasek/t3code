import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  ProviderInstanceId,
  ProviderDriverKind,
  type ProviderInstanceConfig,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/unstable/process";
import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import * as ServerConfig from "../../config.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as IdAllocator from "../../orchestration-v2/IdAllocator.ts";
import { makeProviderInstanceRegistry } from "../Layers/ProviderInstanceRegistryLive.ts";
import { MastraCodeDriver } from "./MastraCodeDriver.ts";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";

const testLayer = ServerConfig.layerTest(process.cwd(), { prefix: "t3-mc-driver-" }).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(IdAllocator.layer),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provideMerge(
    Layer.mock(BackgroundPolicy.BackgroundPolicy)({
      shouldRunScopeWork: () => Effect.succeed(false),
    }),
  ),
);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const noSpawner = ChildProcessSpawner.make(() =>
  Effect.die("Disabled MC must not start a process"),
);

it.layer(testLayer)("MastraCodeDriver", (it) => {
  it.effect(
    "refreshes existing-account bootstrap and explicit sign-in/refresh, not snapshot reads",
    () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(0);
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-mc-driver-catalog-" });
        const logPath = path.join(directory, "calls.txt");
        const binaryPath = writeFakeCli({
          directory,
          name: "mastracode",
          source: `
          import { appendFileSync, existsSync, writeFileSync } from "node:fs";
          import { join } from "node:path";
          const args = process.argv.slice(2);
          const app = process.env.MASTRA_APP_DATA_DIR;
          appendFileSync(${encodeJson(logPath)}, args.join(" ") + "\\t" + app + "\\n");
          const marker = join(app, "fixture-catalog-ready");
          const catalog = { source: "account-cache", status: existsSync(marker) ? "ready" : "missing",
            clientVersion: "0.160.0", fetchedAt: 0, expiresAt: 3600000 };
          if (args[0] === "info") {
            process.stdout.write(JSON.stringify({ schemaVersion: 1, version: "0.45.0-alpha.3",
              acpProtocolVersion: 1, capabilities: { loadSession: true, permissions: true,
                elicitation: true, images: true },
              auth: { provider: "openai-codex", status: "authenticated" }, catalog,
              models: existsSync(marker) ? [{ id: "openai/gpt-6-luna", modes: ["build"] }] : [] }));
          } else if (args.join(" ") === "catalog refresh --provider openai-codex --json") {
            writeFileSync(marker, "ready");
            process.stdout.write(JSON.stringify({ type: "success", provider: "openai-codex",
              catalog: { source: "account-cache", status: "ready", clientVersion: "0.160.0",
                fetchedAt: 0, expiresAt: 3600000 } }));
          } else if (args[0] === "auth" && args[1] === "login") {
            process.stdout.write(JSON.stringify({ type: "device_code",
              verificationUrl: "https://auth.openai.com/codex/device",
              userCode: "ABCD-EFGH", expiresAt: "2030-01-01T00:00:00.000Z" }) + "\\n");
            process.stdout.write(JSON.stringify({ type: "success", provider: "openai-codex" }) + "\\n");
          } else { process.exitCode = 23; }
        `,
        });
        const instance = yield* MastraCodeDriver.create({
          instanceId: ProviderInstanceId.make("mc-catalog-driver"),
          displayName: undefined,
          environment: [],
          enabled: true,
          config: { ...MastraCodeDriver.defaultConfig(), binaryPath },
        });
        yield* instance.snapshot.streamChanges.pipe(
          Stream.filter((snapshot) => snapshot.status === "ready"),
          Stream.runHead,
        );
        const readCalls = fs
          .readFileString(logPath)
          .pipe(Effect.map((text) => text.trim().split("\n")));
        const refreshCount = (calls: ReadonlyArray<string>) =>
          calls.filter((call) => call.startsWith("catalog refresh ")).length;
        const bootstrap = yield* readCalls;
        expect(refreshCount(bootstrap)).toBe(1);
        expect(bootstrap.some((call) => call.startsWith("auth login"))).toBe(false);
        const privateDirectory = instance.auth!.credentialBinding!.key.slice("mastracode:".length);
        expect(bootstrap.every((call) => call.endsWith(`\t${privateDirectory}`))).toBe(true);
        yield* instance.snapshot.getSnapshot;
        yield* instance.snapshot.getSnapshot;
        expect(yield* readCalls).toEqual(bootstrap);
        expect((yield* instance.snapshot.refresh).status).toBe("ready");
        expect(refreshCount(yield* readCalls)).toBe(2);
        yield* instance.auth!.start("catalog-test-owner");
        const loginResult = yield* instance.auth!.subscribe("catalog-test-owner").pipe(
          Stream.filter((state) => state.phase === "succeeded" || state.phase === "failed"),
          Stream.runHead,
        );
        expect(Option.getOrThrow(loginResult).phase).toBe("succeeded");
        expect(refreshCount(yield* readCalls)).toBe(3);
        expect((yield* instance.snapshot.getSnapshot).status).toBe("ready");
      }).pipe(Effect.scoped),
  );
  it.effect("creates isolated opt-in instances with manual-only paired maintenance", () =>
    Effect.gen(function* () {
      const create = (id: string) =>
        MastraCodeDriver.create({
          instanceId: ProviderInstanceId.make(id),
          displayName: undefined,
          environment: [],
          enabled: false,
          config: MastraCodeDriver.defaultConfig(),
        });
      const first = yield* create("mc-one");
      const second = yield* create("mc-two");
      expect(first.driverKind).toBe("mastraCode");
      expect(first.orchestrationAdapter).toBeDefined();
      expect(first.auth?.credentialBinding?.owner).toBe("provider");
      expect(first.auth?.credentialBinding?.key).not.toBe(second.auth?.credentialBinding?.key);
      expect((yield* first.snapshot.getSnapshot).enabled).toBe(false);
      expect((yield* first.snapshot.resolveMaintenance()).update).toBeNull();
      const error = yield* first.textGeneration
        .generateThreadTitle({
          cwd: "/workspace",
          message: "title",
          modelSelection: { instanceId: first.instanceId, model: "default" },
        })
        .pipe(Effect.flip);
      expect(error._tag).toBe("TextGenerationError");
    }).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, noSpawner),
      Effect.scoped,
    ),
  );
  it.effect("hydrates pre-upgrade raw MC entries while retaining explicit disables", () =>
    Effect.gen(function* () {
      const cases: ReadonlyArray<{ id: string; entry: ProviderInstanceConfig; enabled: boolean }> =
        [
          {
            id: "old",
            entry: { driver: ProviderDriverKind.make("mastraCode"), config: {} },
            enabled: true,
          },
          {
            id: "envelope-off",
            entry: { driver: ProviderDriverKind.make("mastraCode"), enabled: false, config: {} },
            enabled: false,
          },
          {
            id: "config-off",
            entry: { driver: ProviderDriverKind.make("mastraCode"), config: { enabled: false } },
            enabled: false,
          },
          {
            id: "conflict",
            entry: {
              driver: ProviderDriverKind.make("mastraCode"),
              enabled: true,
              config: { enabled: false },
            },
            enabled: false,
          },
          {
            id: "fresh",
            entry: {
              driver: ProviderDriverKind.make("mastraCode"),
              config: MastraCodeDriver.defaultConfig(),
            },
            enabled: false,
          },
        ];
      const { registry } = yield* makeProviderInstanceRegistry({
        drivers: [MastraCodeDriver],
        configMap: Object.fromEntries(
          cases.map(({ id, entry }) => [ProviderInstanceId.make(id), entry]),
        ),
      });
      for (const { id, enabled } of cases) {
        const instance = yield* registry.getInstance(ProviderInstanceId.make(id));
        expect(instance?.enabled).toBe(enabled);
        expect((yield* instance!.snapshot.getSnapshot).enabled).toBe(enabled);
      }
    }).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, noSpawner),
      Effect.scoped,
    ),
  );
});
