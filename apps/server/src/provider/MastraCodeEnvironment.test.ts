// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";

import { describe, expect, it } from "@effect/vitest";

import {
  buildMastraCodeEnvironment,
  isFullyQualifiedMastraCodePath,
  withMastraCodeThreadStorage,
} from "./MastraCodeEnvironment.ts";

const paths = {
  appDataDirectory: "/private/mastracode/instance-a",
  homeDirectory: "/private/mastracode/instance-a/home",
  codexHomeDirectory: "/private/mastracode/instance-a/codex-home",
  databasePath: "/private/mastracode/instance-a/mastra.db",
  vectorDatabasePath: "/private/mastracode/instance-a/mastra-vectors.db",
  observabilityDatabasePath: "/private/mastracode/instance-a/observability.duckdb",
  plansDirectory: "/private/mastracode/instance-a/plans",
  configDirectory: "/private/mastracode/instance-a/home/.config",
  dataDirectory: "/private/mastracode/instance-a/home/.local/share",
  cacheDirectory: "/private/mastracode/instance-a/home/.cache",
  roamingAppDataDirectory: "/private/mastracode/instance-a/home/AppData/Roaming",
  localAppDataDirectory: "/private/mastracode/instance-a/home/AppData/Local",
} as const;

describe("buildMastraCodeEnvironment", () => {
  it("inherits only runtime essentials and isolates ambient Codex credentials and home paths", () => {
    const environment = buildMastraCodeEnvironment(
      paths,
      undefined,
      {
        PATH: "/usr/bin",
        TMPDIR: "/tmp",
        HOME: "/host/home",
        CODEX_HOME: "/host/.codex",
        OPENAI_API_KEY: "ambient-openai-secret",
        HERMES_TOKEN: "ambient-hermes-secret",
        CUSTOM_PARENT_SECRET: "ambient-custom-secret",
      },
      "linux",
    );

    expect(environment).toMatchObject({
      PATH: "/usr/bin",
      TMPDIR: "/tmp",
      HOME: paths.homeDirectory,
      USERPROFILE: paths.homeDirectory,
      CODEX_HOME: paths.codexHomeDirectory,
      MASTRA_APP_DATA_DIR: paths.appDataDirectory,
      MASTRA_STORAGE_BACKEND: "libsql",
      MASTRA_DB_URL: `file:${paths.databasePath}`,
      MASTRA_VECTOR_DB_PATH: paths.vectorDatabasePath,
    });
    expect(environment).not.toHaveProperty("OPENAI_API_KEY");
    expect(environment).not.toHaveProperty("HERMES_TOKEN");
    expect(environment).not.toHaveProperty("CUSTOM_PARENT_SECRET");
  });

  it("keeps explicit non-Codex provider settings but blocks auth overrides and process injection", () => {
    const environment = buildMastraCodeEnvironment(
      paths,
      [
        { name: "ANTHROPIC_API_KEY", value: "explicit-provider-secret", sensitive: true },
        { name: "OPENAI_API_KEY", value: "must-not-override-oauth", sensitive: true },
        { name: "CODEX_HOME", value: "/host/.codex", sensitive: false },
        { name: "NODE_OPTIONS", value: "--require /tmp/inject.js", sensitive: false },
        { name: "MASTRA_DB_PATH", value: "/host/shared.db", sensitive: false },
        { name: "MASTRA_DB_URL", value: "libsql://host-owned.example", sensitive: false },
        { name: "MASTRA_VECTOR_DB_PATH", value: "/host/shared-vectors.db", sensitive: false },
      ],
      { PATH: "/usr/bin" },
      "linux",
    );

    expect(environment.ANTHROPIC_API_KEY).toBe("explicit-provider-secret");
    expect(environment.OPENAI_API_KEY).toBeUndefined();
    expect(environment.NODE_OPTIONS).toBeUndefined();
    expect(environment.CODEX_HOME).toBe(paths.codexHomeDirectory);
    expect(environment.MASTRA_DB_PATH).toBe(paths.databasePath);
    expect(environment.MASTRA_DB_URL).toBe(`file:${paths.databasePath}`);
    expect(environment.MASTRA_STORAGE_BACKEND).toBe("libsql");
    expect(environment.MASTRA_VECTOR_DB_PATH).toBe(paths.vectorDatabasePath);
  });

  it("removes empty and relative PATH entries after merging instance settings", () => {
    const trustedBinDirectory = NodePath.resolve("mastracode-trusted-bin");
    const environment = buildMastraCodeEnvironment(
      paths,
      [
        {
          name: "PATH",
          value: ["", ".", "./bin", trustedBinDirectory, "../workspace-bin"].join(
            NodePath.delimiter,
          ),
          sensitive: false,
        },
      ],
      { PATH: "/ignored/inherited/path" },
      "linux",
    );

    expect(environment.PATH).toBe(trustedBinDirectory);
  });

  it("accepts only drive-qualified or complete UNC paths for Windows executables", () => {
    expect(isFullyQualifiedMastraCodePath("C:\\Mastra\\mastracode.exe", "win32")).toBe(true);
    expect(isFullyQualifiedMastraCodePath("\\\\server\\share\\mastracode.exe", "win32")).toBe(true);
    expect(isFullyQualifiedMastraCodePath("\\workspace\\mastracode.exe", "win32")).toBe(false);
    expect(isFullyQualifiedMastraCodePath("/workspace/mastracode.exe", "win32")).toBe(false);
    expect(isFullyQualifiedMastraCodePath("\\\\workspace", "win32")).toBe(false);
    expect(isFullyQualifiedMastraCodePath("\\\\.\\pipe\\mastracode.exe", "win32")).toBe(false);
  });

  it("removes Windows PATH entries that inherit a process drive or name no UNC share", () => {
    const environment = buildMastraCodeEnvironment(
      paths,
      [
        {
          name: "PATH",
          value: [
            "",
            "\\workspace\\bin",
            "/workspace/bin",
            "C:\\trusted\\bin",
            "\\\\server\\share\\bin",
            "\\\\workspace",
          ].join(";"),
          sensitive: false,
        },
      ],
      { PATH: "C:\\inherited\\bin" },
      "win32",
    );

    expect(environment.PATH).toBe("C:\\trusted\\bin;\\\\server\\share\\bin");
  });

  it("shares an instance OAuth store while giving each ACP thread its own databases", () => {
    const first = withMastraCodeThreadStorage(
      {},
      {
        appDataDirectory: paths.appDataDirectory,
        databasePath: `${paths.appDataDirectory}/threads/first/mastra.db`,
        vectorDatabasePath: `${paths.appDataDirectory}/threads/first/mastra-vectors.db`,
        observabilityDatabasePath: `${paths.appDataDirectory}/threads/first/observability.duckdb`,
        plansDirectory: paths.plansDirectory,
      },
    );
    const second = withMastraCodeThreadStorage(
      {},
      {
        appDataDirectory: paths.appDataDirectory,
        databasePath: `${paths.appDataDirectory}/threads/second/mastra.db`,
        vectorDatabasePath: `${paths.appDataDirectory}/threads/second/mastra-vectors.db`,
        observabilityDatabasePath: `${paths.appDataDirectory}/threads/second/observability.duckdb`,
        plansDirectory: paths.plansDirectory,
      },
    );

    expect(first.MASTRA_APP_DATA_DIR).toBe(second.MASTRA_APP_DATA_DIR);
    expect(first.MASTRA_DB_PATH).not.toBe(second.MASTRA_DB_PATH);
    expect(first.MASTRA_DB_URL).not.toBe(second.MASTRA_DB_URL);
    expect(first.MASTRA_VECTOR_DB_PATH).not.toBe(second.MASTRA_VECTOR_DB_PATH);
    expect(first.MASTRA_OBSERVABILITY_DB_PATH).not.toBe(second.MASTRA_OBSERVABILITY_DB_PATH);
  });
});
