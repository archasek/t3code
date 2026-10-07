/**
 * Thin CLI entry.
 *
 * Every ACP agent spawns `t3 acp-mcp-bridge` while opening its session, and
 * terminal-fallback agents run `t3 acp-mcp-call` per tool call, so their
 * startup sits on first-message latency. The Mastra form worker also needs
 * this path to preserve its bounded validation deadline. These dispatch before the full
 * CLI module graph (seconds of evaluation) loads; everything else defers to
 * the real CLI in ./binCli.ts.
 */
import { isEntrypoint } from "./entrypoint.ts";

if (
  isEntrypoint({
    moduleUrl: import.meta.url,
    entryPath: process.argv[1],
    runtimeMain: import.meta.main,
  })
) {
  const command = process.argv[2];
  if (command === "acp-mcp-bridge" || command === "acp-mcp-call") {
    const { runAcpMcpCliFastPath } = await import("./mcp/AcpMcpStdioBridge.ts");
    await runAcpMcpCliFastPath(command, process.argv.slice(3));
  } else if (command === "__mastra-elicitation") {
    const { runMastraElicitationWorker } = await import("./mastraElicitationWorker.ts");
    await runMastraElicitationWorker();
  } else {
    const { runCli } = await import("./binCli.ts");
    runCli();
  }
}
