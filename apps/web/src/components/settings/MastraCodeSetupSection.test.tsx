import {
  isValidElement,
  type EffectCallback,
  type FunctionComponent,
  type ReactElement,
} from "react";
import {
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderAuthState,
  type ServerProvider,
} from "@t3tools/contracts";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { visitElements } from "../../test/reactElementTree";
import { reactHookHarness as hooks } from "../../test/reactHookHarness";

const setup = vi.hoisted(() => ({
  auth: null as ProviderAuthState | null,
  authState: vi.fn(() => "auth"),
  startAuth: vi.fn(),
  cancelAuth: vi.fn(),
  logoutAuth: vi.fn(),
  refreshProviders: vi.fn(),
  confirm: vi.fn(),
  openExternal: vi.fn(),
  copyText: vi.fn(),
}));

vi.mock("react", async (importOriginal) => {
  const actual = await importOriginal<typeof import("react")>();
  const { reactHookHarness } = await import("../../test/reactHookHarness");
  return {
    ...actual,
    useEffect: (effect: EffectCallback, dependencies?: readonly unknown[]) => {
      const previousDependencies = reactHookHarness.useRef<readonly unknown[] | undefined>(
        undefined,
      );
      const dependenciesChanged =
        dependencies === undefined ||
        previousDependencies.current === undefined ||
        previousDependencies.current.length !== dependencies.length ||
        dependencies.some(
          (dependency, index) => !Object.is(dependency, previousDependencies.current?.[index]),
        );
      if (dependenciesChanged) {
        previousDependencies.current = dependencies;
        effect();
      }
    },
    useRef: reactHookHarness.useRef,
    useState: reactHookHarness.useState,
  };
});

vi.mock("react/compiler-runtime", async () => {
  const { reactHookHarness } = await import("../../test/reactHookHarness");
  return { c: reactHookHarness.useMemoCache };
});

vi.mock("../../state/server", () => ({
  serverEnvironment: {
    providerAuthState: setup.authState,
    startProviderAuth: setup.startAuth,
    cancelProviderAuth: setup.cancelAuth,
    logoutProviderAuth: setup.logoutAuth,
    refreshProviders: setup.refreshProviders,
  },
}));

vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: (command: unknown) => command,
}));

vi.mock("../../state/query", () => ({
  useEnvironmentQuery: (atom: string) => ({
    data: atom === "auth" ? setup.auth : null,
    error: null,
    isPending: false,
    refresh: vi.fn(),
  }),
}));

vi.mock("../../localApi", () => ({
  ensureLocalApi: () => ({
    dialogs: { confirm: setup.confirm },
    shell: { openExternal: setup.openExternal },
  }),
}));

vi.mock("../../hooks/useCopyToClipboard", () => ({
  writeTextToClipboard: setup.copyText,
}));

import { MastraCodeSetupSection } from "./MastraCodeSetupSection";

const environmentId = EnvironmentId.make("remote-hq");
const instanceId = ProviderInstanceId.make("mastracode_hq");
const deviceUrl = "https://auth.openai.com/codex/device";
const provider: ServerProvider = {
  instanceId,
  driver: ProviderDriverKind.make("mastraCode"),
  installed: true,
  enabled: true,
  version: "test-version",
  status: "warning",
  auth: { status: "unauthenticated" },
  checkedAt: "2026-09-27T00:00:00.000Z",
  models: [],
  skills: [],
  slashCommands: [],
  setup: { canAuthenticate: true, canInstall: false },
};

function authState(patch: Partial<ProviderAuthState> = {}): ProviderAuthState {
  return {
    instanceId,
    phase: "waiting",
    flowId: "flow-1",
    authorizationUrl: deviceUrl,
    expiresAt: "2026-09-27T00:15:00.000Z",
    message: "Complete sign-in to continue.",
    interaction: {
      type: "deviceCode",
      id: "flow-1",
      url: deviceUrl,
      userCode: "ABCD-EFGH",
    },
    ...patch,
  };
}

function renderSetup(options: { readOnly?: boolean; provider?: ServerProvider } = {}) {
  hooks.beginRender();
  const view = MastraCodeSetupSection({
    environmentId,
    environmentLabel: "HQ server",
    instanceId,
    provider: options.provider ?? provider,
    readOnly: options.readOnly ?? false,
  });
  const actions = visitElements(
    view,
    (element) =>
      typeof element.type === "function" &&
      element.props.environmentId === environmentId &&
      element.props.instanceId === instanceId,
  );
  if (!actions) return view;
  const Actions = actions.type as FunctionComponent<Record<string, unknown>>;
  return Actions(actions.props) as ReactElement<Record<string, unknown>>;
}

function button(view: unknown, label: string) {
  return visitElements(
    view,
    (element) =>
      (element.props.children === label || element.props["aria-label"] === label) &&
      typeof element.props.onClick === "function",
  );
}

function click(view: unknown, label: string) {
  const target = button(view, label);
  if (!target) throw new Error(`Missing button: ${label}`);
  if (target.props.disabled) throw new Error(`Button is disabled: ${label}`);
  (target.props.onClick as () => void)();
}

function hasText(node: unknown, text: string): boolean {
  if (node === text) return true;
  if (Array.isArray(node)) return node.some((child) => hasText(child, text));
  if (!isValidElement<Record<string, unknown>>(node)) return false;
  return Object.values(node.props).some((value) => hasText(value, text));
}

async function flushPromises() {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

describe("Mastra Code setup", () => {
  beforeEach(() => {
    hooks.reset();
    vi.clearAllMocks();
    setup.auth = authState();
    for (const command of [
      setup.startAuth,
      setup.cancelAuth,
      setup.logoutAuth,
      setup.refreshProviders,
    ]) {
      command.mockReset().mockResolvedValue({ _tag: "Success", value: undefined });
    }
    setup.confirm.mockReset().mockResolvedValue(false);
    setup.openExternal.mockReset().mockResolvedValue(undefined);
    setup.copyText.mockReset().mockResolvedValue(true);
  });

  it("shows and opens the device-code flow returned by Mastra Code", async () => {
    const view = renderSetup();
    expect(hasText(view, "ABCD-EFGH")).toBe(true);
    click(view, "Open Codex sign-in");
    click(view, "Copy code");
    await flushPromises();

    expect(setup.openExternal).toHaveBeenCalledWith(deviceUrl);
    expect(setup.copyText).toHaveBeenCalledWith("ABCD-EFGH", "Codex sign-in code");
  });

  it("starts and cancels sign-in through the selected HQ provider instance", async () => {
    setup.auth = authState({
      phase: "idle",
      flowId: null,
      authorizationUrl: null,
      expiresAt: null,
      message: null,
      interaction: null,
    });
    const startView = renderSetup();
    click(startView, "Sign in with Codex");
    await flushPromises();
    expect(setup.startAuth).toHaveBeenCalledWith({ environmentId, input: { instanceId } });

    setup.auth = authState();
    const waitingView = renderSetup();
    click(waitingView, "Cancel sign-in");
    await flushPromises();
    expect(setup.cancelAuth).toHaveBeenCalledWith({
      environmentId,
      input: { instanceId, flowId: "flow-1" },
    });
  });

  it("uses the completed sign-in state until the provider snapshot refreshes", async () => {
    setup.auth = authState({ phase: "succeeded", message: "Sign-in complete." });
    const view = renderSetup({
      provider: { ...provider, auth: { status: "unauthenticated" } },
    });
    await flushPromises();

    expect(button(view, "Sign in with Codex")).toBeNull();
    expect(button(view, "Sign out of Mastra Code")).not.toBeNull();
    expect(setup.refreshProviders).toHaveBeenCalledWith({
      environmentId,
      input: { instanceId },
    });
  });

  it("refreshes provider status after each sign-in completed on another device", async () => {
    setup.auth = authState({ phase: "succeeded", flowId: null, message: "Sign-in complete." });
    renderSetup({ provider: { ...provider, auth: { status: "unauthenticated" } } });
    await flushPromises();

    setup.auth = authState({
      phase: "starting",
      flowId: null,
      authorizationUrl: null,
      expiresAt: null,
      message: null,
      interaction: null,
    });
    renderSetup({ provider: { ...provider, auth: { status: "unauthenticated" } } });
    setup.auth = authState({ phase: "succeeded", flowId: null, message: "Sign-in complete." });
    renderSetup({ provider: { ...provider, auth: { status: "unauthenticated" } } });
    await flushPromises();

    expect(setup.refreshProviders).toHaveBeenCalledTimes(2);
    expect(setup.refreshProviders).toHaveBeenCalledWith({
      environmentId,
      input: { instanceId },
    });
  });

  it("offers sign-in immediately after logout even if the provider snapshot is stale", () => {
    setup.auth = authState({
      phase: "idle",
      flowId: null,
      authorizationUrl: null,
      expiresAt: null,
      message: "Signed out.",
      interaction: null,
    });
    const view = renderSetup({
      provider: { ...provider, auth: { status: "authenticated" } },
    });

    expect(button(view, "Sign in with Codex")).not.toBeNull();
  });

  it("refreshes only the selected Mastra Code instance after confirmed logout", async () => {
    setup.auth = authState({
      phase: "idle",
      flowId: null,
      authorizationUrl: null,
      expiresAt: null,
      message: null,
      interaction: null,
    });
    setup.confirm.mockResolvedValue(true);
    const view = renderSetup({
      provider: { ...provider, auth: { status: "authenticated" } },
    });
    click(view, "Sign out of Mastra Code");
    await flushPromises();

    expect(setup.logoutAuth).toHaveBeenCalledWith({ environmentId, input: { instanceId } });
    expect(setup.refreshProviders).toHaveBeenCalledWith({
      environmentId,
      input: { instanceId },
    });
  });

  it("does not expose the login code to a read-only setup view", () => {
    const view = renderSetup({ readOnly: true });
    expect(hasText(view, "ABCD-EFGH")).toBe(false);
    expect(setup.authState).not.toHaveBeenCalled();
  });
});
