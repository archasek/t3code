import * as Effect from "effect/Effect";
import * as AcpErrors from "effect-acp/errors";
import type { ModelSelection } from "@t3tools/contracts";
import type { AcpSessionRuntime, AcpSessionRuntimeStartResult } from "./acp/AcpSessionRuntime.ts";

// MC advertises legacy ACP models and owns their availability checks.
export const applyMastraCodeModelSelection = (input: {
  readonly runtime: Pick<AcpSessionRuntime["Service"], "setSessionModel">;
  readonly startResult: Pick<AcpSessionRuntimeStartResult, "sessionSetupResult">;
  readonly modelSelection: Pick<ModelSelection, "model">;
}) => Effect.gen(function* () {
    const { runtime, startResult, modelSelection } = input;
    const models = startResult.sessionSetupResult.models;
    const selected = modelSelection.model;
    if (selected === "default" || selected === "auto" || selected === "") {
      // Mode switching can replace the current model; setup-time metadata is
      // not evidence of the native model now running.
      return undefined;
    }
    if (!models?.availableModels.some((model) => model.modelId === selected)) {
      return yield* AcpErrors.AcpRequestError.invalidParams("Mastra Code model is unavailable in this session.");
    }
    // Explicit selection must reach the current native mode even when the
    // original session setup happened to advertise the same model.
    yield* runtime.setSessionModel(selected);
    return selected;
  });
