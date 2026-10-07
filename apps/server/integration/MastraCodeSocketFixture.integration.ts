// @effect-diagnostics nodeBuiltinImport:off - Loopback-only test transport.
import * as NodeHttp from "node:http";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import {
  ORCHESTRATION_V2_WS_METHODS,
  OrchestrationV2RpcSchemas,
  OrchestrationV2DispatchCommandError,
  OrchestrationV2GetThreadProjectionError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { HttpRouter, HttpServer } from "effect/http";
import { Rpc, RpcGroup, RpcServer, RpcSerialization } from "effect/rpc";
import * as Orchestrator from "../src/orchestration-v2/Orchestrator.ts";
import * as ThreadMessageIntake from "../src/orchestration-v2/ThreadMessageIntake.ts";
import { userFacingDispatchErrorMessage } from "../src/orchestration-v2/UserFacingErrors.ts";
import { subscribeOrchestrationV2Thread } from "../src/ws.ts";
import { openMeasuredWsClient } from "./NetworkTransferMeasurement.integration.ts";

// Uses the canonical command intake and durable orchestrator. This disposable
// transport deliberately does not claim production startup/auth qualification.
export const openMastraCodeSocketFixture = Effect.fn("MC.socketFixture")(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const intakeContext = yield* Effect.context<
    | Effect.Services<ReturnType<typeof ThreadMessageIntake.dispatchCommand>>
    | Effect.Services<ReturnType<typeof subscribeOrchestrationV2Thread>>
  >();
  const group = RpcGroup.make(
    Rpc.make(ORCHESTRATION_V2_WS_METHODS.subscribeThread, {
      payload: OrchestrationV2RpcSchemas.subscribeThread.input,
      success: OrchestrationV2RpcSchemas.subscribeThread.output,
      error: OrchestrationV2GetThreadProjectionError,
      stream: true,
    }),
    Rpc.make(ORCHESTRATION_V2_WS_METHODS.dispatchCommand, {
      payload: OrchestrationV2RpcSchemas.dispatchCommand.input,
      success: OrchestrationV2RpcSchemas.dispatchCommand.output,
      error: OrchestrationV2DispatchCommandError,
    }),
    Rpc.make(ORCHESTRATION_V2_WS_METHODS.getThreadProjection, {
      payload: OrchestrationV2RpcSchemas.getThreadProjection.input,
      success: OrchestrationV2RpcSchemas.getThreadProjection.output,
      error: OrchestrationV2GetThreadProjectionError,
    }),
  );
  const handlers = group.toLayer({
    [ORCHESTRATION_V2_WS_METHODS.subscribeThread]: (input) =>
      Stream.unwrap(subscribeOrchestrationV2Thread(input).pipe(Effect.provide(intakeContext))),
    [ORCHESTRATION_V2_WS_METHODS.dispatchCommand]: (command) =>
      ThreadMessageIntake.dispatchCommand(command).pipe(
        Effect.provide(intakeContext),
        Effect.map((receipt) => ({ sequence: receipt.sequence })),
        Effect.mapError(
          (cause) =>
            new OrchestrationV2DispatchCommandError({
              commandId: command.commandId,
              commandType: command.type,
              message:
                userFacingDispatchErrorMessage(cause) ?? "Command rejected by canonical intake",
            }),
        ),
      ),
    [ORCHESTRATION_V2_WS_METHODS.getThreadProjection]: (input) =>
      orchestrator.getThreadProjection(input.threadId).pipe(
        Effect.mapError(
          () =>
            new OrchestrationV2GetThreadProjectionError({
              threadId: input.threadId,
              message: "Projection unavailable",
            }),
        ),
      ),
  });
  const server = yield* Layer.build(
    HttpRouter.serve(
      RpcServer.layerHttp({ group, path: "/ws", protocol: "websocket" }).pipe(
        Layer.provide(handlers),
        Layer.provide(RpcSerialization.layerJson),
      ),
      { disableListenLog: true },
    ).pipe(
      Layer.provideMerge(
        NodeHttpServer.layer(NodeHttp.createServer, { host: "127.0.0.1", port: 0 }),
      ),
    ),
  );
  const address = Context.get(server, HttpServer.HttpServer).address;
  if (!("port" in address)) return yield* Effect.die("Expected loopback TCP server");
  const connect = () =>
    openMeasuredWsClient({ url: `ws://127.0.0.1:${address.port}/ws`, cookie: "" });
  return { connect };
});
