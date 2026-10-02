import {
  type EnvironmentId,
  type ProviderAuthState,
  type ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
  type AtomCommandResult,
} from "@t3tools/client-runtime/state/runtime";
import { useEffect, useRef, useState } from "react";

import { writeTextToClipboard } from "../../hooks/useCopyToClipboard";
import { ensureLocalApi } from "../../localApi";
import { useEnvironmentQuery } from "../../state/query";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { SettingsRow } from "./settingsLayout";

interface MastraCodeSetupSectionProps {
  readonly environmentId: EnvironmentId;
  readonly environmentLabel: string;
  readonly instanceId: ProviderInstanceId;
  readonly provider: ServerProvider | undefined;
  readonly readOnly: boolean;
}

const AUTH_PHASE_LABELS: Record<ProviderAuthState["phase"], string> = {
  idle: "Sign in to the Codex account used by Mastra Code.",
  starting: "Starting Mastra Code sign-in.",
  waiting: "Enter the code to finish signing in to Codex.",
  verifying: "Checking the Mastra Code sign-in.",
  succeeded: "Codex sign-in for Mastra Code is complete.",
  failed: "Mastra Code sign-in failed.",
  cancelled: "Mastra Code sign-in was cancelled.",
};

export function MastraCodeSetupSection(props: MastraCodeSetupSectionProps) {
  return (
    <section
      aria-label="Mastra Code setup"
      className="@container/setup divide-y divide-border/50 text-xs"
    >
      {props.readOnly ? (
        <SettingsRow title="Sign-in unavailable" description="Provider setup is read-only." />
      ) : props.provider?.setup === undefined ? (
        <SettingsRow
          title="Update required"
          description="Update this environment to manage Mastra Code sign-in."
        />
      ) : (
        <MastraCodeAuthActions
          key={`${props.environmentId}:${props.instanceId}`}
          environmentId={props.environmentId}
          environmentLabel={props.environmentLabel}
          instanceId={props.instanceId}
          provider={props.provider}
        />
      )}
    </section>
  );
}

function MastraCodeAuthActions({
  environmentId,
  environmentLabel,
  instanceId,
  provider,
}: Pick<MastraCodeSetupSectionProps, "environmentId" | "environmentLabel" | "instanceId"> & {
  readonly provider: ServerProvider;
}) {
  const target = { environmentId, input: { instanceId } };
  const authQuery = useEnvironmentQuery(serverEnvironment.providerAuthState(target));
  const auth = authQuery.data;
  const startAuth = useAtomCommand(serverEnvironment.startProviderAuth, {
    reportFailure: false,
    reportDefect: false,
  });
  const cancelAuth = useAtomCommand(serverEnvironment.cancelProviderAuth, {
    reportFailure: false,
    reportDefect: false,
  });
  const logoutAuth = useAtomCommand(serverEnvironment.logoutProviderAuth, {
    reportFailure: false,
    reportDefect: false,
  });
  const refreshProviders = useAtomCommand(serverEnvironment.refreshProviders, {
    reportFailure: false,
    reportDefect: false,
  });
  const [pendingLabel, setPendingLabel] = useState<string | null>(null);
  const pendingRef = useRef(false);
  const refreshedAuthFlowId = useRef<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [copiedAction, setCopiedAction] = useState<string | null>(null);
  const authActive =
    auth?.phase === "starting" || auth?.phase === "waiting" || auth?.phase === "verifying";
  const signedOutInThisFlow = auth?.phase === "idle" && auth.message === "Signed out.";
  const authenticated =
    auth?.phase === "succeeded" ||
    (!signedOutInThisFlow && provider.auth.status === "authenticated");
  const completedAuthFlowId =
    auth?.phase === "succeeded" ? (auth.flowId ?? "completed-on-another-client") : null;
  const deviceCode =
    auth?.phase === "waiting" && auth.interaction?.type === "deviceCode" ? auth.interaction : null;
  const actionsDisabled = pendingLabel !== null || authQuery.error !== null;
  const authStatusMessage =
    auth === null
      ? "Reading Mastra Code sign-in status."
      : authActive || auth.phase === "failed" || auth.phase === "cancelled"
        ? (auth.message ?? AUTH_PHASE_LABELS[auth.phase])
        : authenticated
          ? "Connected to Codex in Mastra Code."
          : auth.phase === "idle" && auth.message
            ? auth.message
            : AUTH_PHASE_LABELS.idle;

  useEffect(() => {
    if (!completedAuthFlowId) {
      refreshedAuthFlowId.current = null;
      return;
    }
    if (refreshedAuthFlowId.current === completedAuthFlowId) return;
    refreshedAuthFlowId.current = completedAuthFlowId;
    void refreshProviders({ environmentId, input: { instanceId } })
      .then((result) => {
        if (result._tag === "Failure" && !isAtomCommandInterrupted(result)) {
          setError("Codex sign-in completed, but Mastra Code status could not be refreshed.");
        }
      })
      .catch(() => {
        setError("Codex sign-in completed, but Mastra Code status could not be refreshed.");
      });
  }, [auth?.phase, completedAuthFlowId, environmentId, instanceId, refreshProviders]);

  async function runCommand<A, E>(
    label: string,
    request: () => Promise<AtomCommandResult<A, E>>,
  ): Promise<boolean> {
    if (pendingRef.current) return false;
    pendingRef.current = true;
    setPendingLabel(label);
    setError(null);
    try {
      const result = await request();
      if (result._tag === "Failure") {
        if (!isAtomCommandInterrupted(result)) {
          const failure = squashAtomCommandFailure(result);
          setError(failure instanceof Error ? failure.message : "Mastra Code setup failed.");
        }
        return false;
      }
      return true;
    } catch {
      setError("Mastra Code setup failed. Try again.");
      return false;
    } finally {
      pendingRef.current = false;
      setPendingLabel(null);
    }
  }

  async function openSignInPage() {
    if (!deviceCode) return;
    try {
      await ensureLocalApi().shell.openExternal(deviceCode.url);
      setError(null);
    } catch {
      setError("Could not open the Codex sign-in page. Copy the link and open it in your browser.");
    }
  }

  async function copySignInLink() {
    if (!deviceCode) return;
    try {
      await writeTextToClipboard(deviceCode.url, "Codex sign-in link");
      setCopiedAction(`${auth?.flowId}:link`);
      setError(null);
    } catch {
      setError("Could not copy the Codex sign-in link.");
    }
  }

  async function copyUserCode() {
    if (!deviceCode) return;
    try {
      await writeTextToClipboard(deviceCode.userCode, "Codex sign-in code");
      setCopiedAction(`${auth?.flowId}:code`);
      setError(null);
    } catch {
      setError("Could not copy the Codex sign-in code.");
    }
  }

  async function signOut() {
    const confirmed = await ensureLocalApi().dialogs.confirm(
      `Sign out of the Codex account used by Mastra Code on ${environmentLabel}? This stops Mastra Code's running threads. Direct Codex and Hermes sign-ins are not changed.`,
    );
    if (confirmed) {
      const signedOut = await runCommand("Signing out of Mastra Code", () => logoutAuth(target));
      if (signedOut) {
        await runCommand("Refreshing Mastra Code status", () =>
          refreshProviders({ environmentId, input: { instanceId } }),
        );
      }
    }
  }

  return (
    <SettingsRow
      title="Mastra Code sign-in"
      description="Connect the Codex account Mastra Code uses. This is separate from direct Codex and Hermes."
      className="@max-lg/setup:[&>div:first-child]:flex @max-lg/setup:[&>div:first-child]:items-stretch @max-lg/setup:[&>div:first-child]:gap-3"
      control={
        <div className="flex min-w-0 flex-col gap-2 sm:max-w-64 sm:items-end sm:text-right">
          <p role="status" className="text-muted-foreground [overflow-wrap:anywhere]">
            {authStatusMessage}
          </p>
          {!provider.installed ? (
            <p className="text-muted-foreground">
              Mastra Code CLI is unavailable. Check its binary path or install it on this server.
            </p>
          ) : null}
          {deviceCode ? (
            <div className="flex flex-wrap gap-2 sm:justify-end">
              <Button size="sm" variant="outline" onClick={() => void openSignInPage()}>
                Open Codex sign-in
              </Button>
              <Button size="sm" variant="ghost" onClick={() => void copySignInLink()}>
                {copiedAction === `${auth?.flowId}:link` ? "Link copied" : "Copy sign-in link"}
              </Button>
            </div>
          ) : null}
          <div className="flex flex-wrap gap-2 sm:justify-end">
            {authActive && auth?.flowId ? (
              <Button
                size="sm"
                variant="ghost"
                disabled={actionsDisabled}
                onClick={() => {
                  const flowId = auth.flowId;
                  if (!flowId) return;
                  void runCommand("Cancelling Mastra Code sign-in", () =>
                    cancelAuth({ environmentId, input: { instanceId, flowId } }),
                  );
                }}
              >
                Cancel sign-in
              </Button>
            ) : !authActive && !authenticated && provider.setup?.canAuthenticate ? (
              <Button
                size="sm"
                variant="outline"
                disabled={actionsDisabled || !provider.installed || auth === null}
                onClick={() =>
                  void runCommand("Starting Mastra Code sign-in", () => startAuth(target))
                }
              >
                {auth?.phase === "failed" || auth?.phase === "cancelled"
                  ? "Retry Codex sign-in"
                  : "Sign in with Codex"}
              </Button>
            ) : null}
            {!authActive && provider.setup?.canAuthenticate ? (
              <Button
                size="sm"
                variant={authenticated ? "outline" : "ghost"}
                disabled={actionsDisabled || auth === null}
                onClick={() => void signOut()}
              >
                Sign out of Mastra Code
              </Button>
            ) : null}
          </div>
        </div>
      }
    >
      {deviceCode ? (
        <div className="space-y-2 pb-2">
          <p>Open the Codex sign-in page and enter this one-time code:</p>
          <div className="flex flex-wrap items-center gap-2">
            <code className="rounded bg-muted px-2 py-1 text-sm font-semibold tracking-wider">
              {deviceCode.userCode}
            </code>
            <Button size="sm" variant="outline" onClick={() => void copyUserCode()}>
              {copiedAction === `${auth?.flowId}:code` ? "Copied" : "Copy code"}
            </Button>
          </div>
        </div>
      ) : null}
      {auth?.phase === "waiting" && !deviceCode && !auth.flowId ? (
        <p className="pb-2 text-muted-foreground">
          Sign-in is in progress in another client. Complete or cancel it there.
        </p>
      ) : null}
      {authQuery.error || error ? (
        <div className="grid gap-2 px-3 py-3 sm:px-4">
          <p role="alert" className="text-destructive [overflow-wrap:anywhere]">
            {error ?? authQuery.error}
          </p>
          {authQuery.error ? (
            <Button size="sm" variant="outline" className="w-fit" onClick={authQuery.refresh}>
              Retry sign-in status
            </Button>
          ) : null}
        </div>
      ) : null}
      <p className="sr-only" role="status">
        {pendingLabel ? `${pendingLabel}.` : null}
      </p>
    </SettingsRow>
  );
}
