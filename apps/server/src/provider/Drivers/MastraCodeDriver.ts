import { MastraCodeSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { HostProcessEnvironment, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { makeMastraCodeTextGeneration } from "../../textGeneration/MastraCodeTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeMastraCodeAdapter } from "../Layers/MastraCodeAdapter.ts";
import {
  buildInitialMastraCodeProviderSnapshot,
  checkMastraCodeProviderStatus,
  makeMastraCodeCommandCatalog,
} from "../Layers/MastraCodeProvider.ts";
import { makeMastraCodeAuth, resolveMastraCodeAppDataDirectory } from "../MastraCodeAuth.ts";
import { buildMastraCodeEnvironment } from "../MastraCodeEnvironment.ts";
import { makeMastraCodeAcpRuntime } from "../acp/MastraCodeAcpSupport.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";
import {
  makeCachedProviderMaintenanceResolution,
  makeManualOnlyProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const decodeMastraCodeSettings = Schema.decodeSync(MastraCodeSettings);
const DRIVER_KIND = ProviderDriverKind.make("mastraCode");

export type MastraCodeDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path
  | ServerConfig
  | ServerSettingsService;

export const MastraCodeDriver: ProviderDriver<MastraCodeSettings, MastraCodeDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Mastra Code",
    supportsMultipleInstances: true,
  },
  configSchema: MastraCodeSettings,
  defaultConfig: (): MastraCodeSettings => decodeMastraCodeSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const crypto = yield* Crypto.Crypto;
      const platform = yield* HostProcessPlatform;
      const hostEnvironment = yield* HostProcessEnvironment;
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const serverSettings = yield* ServerSettingsService;
      const serverConfig = yield* ServerConfig;
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER_KIND,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies MastraCodeSettings;

      // Mastra Code owns its Codex OAuth store. Keep it separate from both
      // T3's direct Codex provider and every other Mastra Code instance.
      const appDataDirectory = yield* resolveMastraCodeAppDataDirectory(
        serverConfig.stateDir,
        instanceId,
      ).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.provideService(Path.Path, path),
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Mastra Code private storage identity could not be created.",
              cause,
            }),
        ),
      );
      const homeDirectory = path.join(appDataDirectory, "home");
      const privateDirectories = [
        appDataDirectory,
        homeDirectory,
        path.join(appDataDirectory, "codex-home"),
        path.join(homeDirectory, ".config"),
        path.join(homeDirectory, ".local", "share"),
        path.join(homeDirectory, ".cache"),
        path.join(homeDirectory, "AppData", "Roaming"),
        path.join(homeDirectory, "AppData", "Local"),
        path.join(appDataDirectory, "plans"),
      ];
      for (const directory of privateDirectories) {
        yield* fileSystem.makeDirectory(directory, { recursive: true, mode: 0o700 }).pipe(
          Effect.mapError(
            () =>
              new ProviderDriverError({
                driver: DRIVER_KIND,
                instanceId,
                detail: "Mastra Code private storage could not be created.",
              }),
          ),
        );
        if (platform !== "win32") {
          yield* fileSystem.chmod(directory, 0o700).pipe(
            Effect.mapError(
              () =>
                new ProviderDriverError({
                  driver: DRIVER_KIND,
                  instanceId,
                  detail: "Mastra Code private storage permissions could not be set.",
                }),
            ),
          );
        }
      }

      const providerEnvironment = buildMastraCodeEnvironment(
        {
          appDataDirectory,
          homeDirectory,
          codexHomeDirectory: path.join(appDataDirectory, "codex-home"),
          databasePath: path.join(appDataDirectory, "mastra.db"),
          vectorDatabasePath: path.join(appDataDirectory, "mastra-vectors.db"),
          observabilityDatabasePath: path.join(appDataDirectory, "observability.duckdb"),
          plansDirectory: path.join(appDataDirectory, "plans"),
          configDirectory: path.join(homeDirectory, ".config"),
          dataDirectory: path.join(homeDirectory, ".local", "share"),
          cacheDirectory: path.join(homeDirectory, ".cache"),
          roamingAppDataDirectory: path.join(homeDirectory, "AppData", "Roaming"),
          localAppDataDirectory: path.join(homeDirectory, "AppData", "Local"),
        },
        environment,
        hostEnvironment,
        platform,
      );

      const auth = yield* makeMastraCodeAuth({
        instanceId,
        binaryPath: effectiveConfig.binaryPath,
        appDataDirectory,
        environment: providerEnvironment,
      }).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
      );

      // Until Mastra Code has a verified, version-pinned update contract,
      // never offer T3 a one-click updater that could break the T3/MC pair.
      const resolveMaintenance = yield* makeCachedProviderMaintenanceResolution(
        Effect.succeed(
          makeManualOnlyProviderMaintenanceCapabilities({
            provider: DRIVER_KIND,
            packageName: null,
          }),
        ),
      );
      const checkProvider = checkMastraCodeProviderStatus(
        effectiveConfig,
        providerEnvironment,
      ).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
      );
      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const managedSnapshot = yield* makeManagedServerProvider<
        ProviderSnapshotSettings<MastraCodeSettings>
      >({
        resolveMaintenance,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialMastraCodeProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build Mastra Code snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      const commandCatalog = yield* makeMastraCodeCommandCatalog(managedSnapshot);

      const adapter = yield* makeMastraCodeAdapter(effectiveConfig, {
        instanceId,
        appDataDirectory,
        environment: providerEnvironment,
        onAvailableCommands: commandCatalog.onAvailableCommands,
        makeRuntime: (runtimeInput) =>
          auth.withAccess!(
            makeMastraCodeAcpRuntime({
              ...runtimeInput,
              settings: effectiveConfig,
              childProcessSpawner,
            }).pipe(
              Effect.provideService(Crypto.Crypto, crypto),
              Effect.provideService(FileSystem.FileSystem, fileSystem),
              Effect.provideService(Path.Path, path),
            ),
          ),
      });
      const textGeneration = yield* makeMastraCodeTextGeneration;

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot: commandCatalog.snapshot,
        snapshotForCwd: commandCatalog.snapshotForCwd,
        adapter,
        textGeneration,
        auth,
      } satisfies ProviderInstance;
    }),
};
