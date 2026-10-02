# Mastra Code

This fork adds Mastra Code as a provider. T3 Code controls the Mastra Code
harness, which calls OpenAI Codex using native ChatGPT authorization. This is
separate from selecting the direct Codex provider in a thread.

## Install on the server

Use a compatible runtime from the [Mastra fork](https://github.com/archasek/mastra)
with this [T3 fork](https://github.com/archasek/t3code). The upstream T3 installer
does not install this integration. An arbitrary upstream `mastracode` package
does not guarantee the machine-readable commands required by this adapter.
Use the runtime paired with your T3 fork release rather than updating one half
independently. On a managed HQ server, use its existing maintenance procedure.

Install Mastra Code on the machine running the T3 server, not on each phone or
remote computer. In **Settings > Providers**, enable Mastra Code and set its
**Binary path** if `mastracode` is not on the server's `PATH`. Refresh provider
status after changing the executable. A missing executable, incompatible
metadata, and missing authorization are different setup problems.

## Use your ChatGPT account

Choose **Sign in** in the Mastra Code provider settings. Open the displayed
verification URL on any device and enter the device code. Use the same ChatGPT
account you use for Codex. Wait for T3 Code to confirm completion; entering a
code alone does not prove that the server saved the authorization.

Mastra Code owns and refreshes its native authorization in the T3 server's
provider-instance data directory. Existing Codex or Hermes credentials do not
automatically authorize that store. Do not copy or symlink their token files.
Signing out of Mastra Code affects its provider instance, not the direct Codex
provider or Hermes.

## Start and reconnect

Select Mastra Code and an available model when starting a thread. Remote clients
use the same server-owned provider instance and thread; they do not run a second
agent locally. After reconnecting, continue the existing thread. If its native
session no longer exists, the provider reports the failure instead of silently
creating an empty replacement conversation.

Mastra Code supports permission requests and user questions. **Auto** does not
add an AI approval reviewer; use it with that limitation in mind. Background
text-generation tasks are not supported by this provider. Keep a suitable
provider selected for those tasks.
