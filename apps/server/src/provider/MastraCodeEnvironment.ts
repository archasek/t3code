// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";

import type { ProviderInstanceEnvironment } from "@t3tools/contracts";

import { mergeProviderInstanceEnvironment } from "./ProviderInstanceEnvironment.ts";

const INHERITED_ENVIRONMENT_KEYS = new Set([
  "PATH",
  "PATHEXT",
  "SYSTEMROOT",
  "WINDIR",
  "COMSPEC",
  "TMP",
  "TEMP",
  "TMPDIR",
  "LANG",
  "LC_ALL",
  "LC_CTYPE",
  "LC_MESSAGES",
  "TERM",
  "COLORTERM",
  "NO_COLOR",
  "CI",
]);

const BLOCKED_ENVIRONMENT_KEY =
  /^(?:OPENAI_|CODEX_|MASTRA_|NODE_OPTIONS$|NODE_PATH$|LD_PRELOAD$|DYLD_INSERT_LIBRARIES$|BASH_ENV$|ENV$|ELECTRON_RUN_AS_NODE$|PYTHONPATH$|PYTHONHOME$)/i;

export function isFullyQualifiedMastraCodePath(value: string, platform: NodeJS.Platform): boolean {
  if (platform !== "win32") return NodePath.posix.isAbsolute(value);

  const root = NodePath.win32.parse(value.replaceAll("/", "\\")).root;
  const driveRoot = /^[A-Za-z]:\\$/.test(root);
  const isDeviceNamespaceRoot = root.startsWith("\\\\.\\") || root.startsWith("\\\\?\\");
  const uncRoot = !isDeviceNamespaceRoot && /^\\\\[^\\]+\\[^\\]+\\$/.test(root);
  return driveRoot || uncRoot;
}

export interface MastraCodeEnvironmentPaths {
  readonly appDataDirectory: string;
  readonly homeDirectory: string;
  readonly codexHomeDirectory: string;
  readonly databasePath: string;
  readonly vectorDatabasePath: string;
  readonly observabilityDatabasePath: string;
  readonly plansDirectory: string;
  readonly configDirectory: string;
  readonly dataDirectory: string;
  readonly cacheDirectory: string;
  readonly roamingAppDataDirectory: string;
  readonly localAppDataDirectory: string;
}

/**
 * Build a least-privilege environment for Mastra Code. Only explicit
 * per-instance settings are added to a small runtime allowlist; credentials
 * and config paths that could select the host's Codex account are forced to
 * this Mastra Code instance's private directories.
 */
export function buildMastraCodeEnvironment(
  paths: MastraCodeEnvironmentPaths,
  instanceEnvironment?: ProviderInstanceEnvironment,
  baseEnvironment: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
): NodeJS.ProcessEnv {
  const inheritedEnvironment: NodeJS.ProcessEnv = {};
  for (const [name, value] of Object.entries(baseEnvironment)) {
    if (value !== undefined && INHERITED_ENVIRONMENT_KEYS.has(name.toUpperCase())) {
      inheritedEnvironment[name] = value;
    }
  }

  const environment = mergeProviderInstanceEnvironment(instanceEnvironment, inheritedEnvironment);
  for (const name of Object.keys(environment)) {
    if (BLOCKED_ENVIRONMENT_KEY.test(name)) delete environment[name];
  }

  let pathValue: string | undefined;
  for (const variable of instanceEnvironment ?? []) {
    if (variable.name.toUpperCase() === "PATH") pathValue = variable.value;
  }
  if (pathValue === undefined) {
    const pathKey = Object.keys(environment).find((name) => name.toUpperCase() === "PATH");
    pathValue = pathKey ? environment[pathKey] : undefined;
  }
  for (const name of Object.keys(environment)) {
    if (name.toUpperCase() === "PATH") delete environment[name];
  }
  if (pathValue) {
    const pathDelimiter = platform === "win32" ? ";" : ":";
    const absolutePathEntries = pathValue
      .split(pathDelimiter)
      .map((entry) => entry.trim().replace(/^\"+|\"+$/g, ""))
      .filter((entry) => entry.length > 0 && isFullyQualifiedMastraCodePath(entry, platform));
    if (absolutePathEntries.length > 0) {
      environment.PATH = absolutePathEntries.join(pathDelimiter);
    }
  }

  return {
    ...environment,
    HOME: paths.homeDirectory,
    USERPROFILE: paths.homeDirectory,
    CODEX_HOME: paths.codexHomeDirectory,
    MASTRA_APP_DATA_DIR: paths.appDataDirectory,
    MASTRA_STORAGE_BACKEND: "libsql",
    MASTRA_DB_PATH: paths.databasePath,
    MASTRA_DB_URL: `file:${paths.databasePath}`,
    MASTRA_VECTOR_DB_PATH: paths.vectorDatabasePath,
    MASTRA_OBSERVABILITY_DB_PATH: paths.observabilityDatabasePath,
    MASTRA_PLANS_DIR: paths.plansDirectory,
    XDG_CONFIG_HOME: paths.configDirectory,
    XDG_DATA_HOME: paths.dataDirectory,
    XDG_CACHE_HOME: paths.cacheDirectory,
    APPDATA: paths.roamingAppDataDirectory,
    LOCALAPPDATA: paths.localAppDataDirectory,
  };
}

export function withMastraCodeThreadStorage(
  environment: NodeJS.ProcessEnv,
  input: {
    readonly appDataDirectory: string;
    readonly databasePath: string;
    readonly vectorDatabasePath: string;
    readonly observabilityDatabasePath: string;
    readonly plansDirectory: string;
  },
): NodeJS.ProcessEnv {
  return {
    ...environment,
    MASTRA_APP_DATA_DIR: input.appDataDirectory,
    MASTRA_STORAGE_BACKEND: "libsql",
    MASTRA_DB_PATH: input.databasePath,
    MASTRA_DB_URL: `file:${input.databasePath}`,
    MASTRA_VECTOR_DB_PATH: input.vectorDatabasePath,
    MASTRA_OBSERVABILITY_DB_PATH: input.observabilityDatabasePath,
    MASTRA_PLANS_DIR: input.plansDirectory,
  };
}
