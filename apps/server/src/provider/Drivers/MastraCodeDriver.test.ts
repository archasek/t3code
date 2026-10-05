import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  ProviderInstanceId,
  ProviderDriverKind,
  type ProviderInstanceConfig,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ChildProcessSpawner } from "effect/unstable/process";
import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import * as ServerConfig from "../../config.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as IdAllocator from "../../orchestration-v2/IdAllocator.ts";
import { makeProviderInstanceRegistry } from "../Layers/ProviderInstanceRegistryLive.ts";
import { MastraCodeDriver } from "./MastraCodeDriver.ts";

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
const noSpawner = ChildProcessSpawner.make(() =>
  Effect.die("Disabled MC must not start a process"),
);

it.layer(testLayer)("MastraCodeDriver", (it) => {
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
