import * as Effect from "effect/Effect";
import * as AcpErrors from "effect-acp/errors";
import { modelSupportsInteractionMode } from "@t3tools/shared/model";
import type { ServerProviderModel, ModelSelection } from "@t3tools/contracts";
import type { AcpSessionRuntime, AcpSessionRuntimeStartResult } from "./acp/AcpSessionRuntime.ts";

// Apply effort after native mode/model switching, which can normalize its value.
export const applyMastraCodeThinkingSelection = Effect.fnUntraced(function* (input: {
  readonly runtime: Pick<AcpSessionRuntime["Service"], "getConfigOptions" | "setConfigOption">;
  readonly modelSelection: ModelSelection;
}) {
  const selection = input.modelSelection.options?.find((option) => option.id === "thought_level");
  if (selection === undefined) return;
  const options = yield* input.runtime.getConfigOptions;
  const option = options.find((option) => option.id === "thought_level");
  if (option?.type !== "select" || typeof selection.value !== "string") {
    return yield* AcpErrors.AcpRequestError.invalidParams(
      "Mastra Code reasoning effort is unavailable.",
    );
  }
  const values = option.options.flatMap((item) =>
    "value" in item ? [item.value] : item.options.map((choice) => choice.value),
  );
  if (!values.includes(selection.value)) {
    return yield* AcpErrors.AcpRequestError.invalidParams(
      "Mastra Code reasoning effort is unavailable for this model.",
    );
  }
  yield* input.runtime.setConfigOption(selection.id, selection.value);
});

// MC advertises legacy ACP models and owns their availability checks.
export const applyMastraCodeModelSelection = (input: {
  readonly runtime: Pick<AcpSessionRuntime["Service"], "setSessionModel">;
  readonly startResult: Pick<AcpSessionRuntimeStartResult, "sessionSetupResult">;
  readonly modelSelection: Pick<ModelSelection, "model">;
  readonly interactionMode: "default" | "plan";
  readonly models: ReadonlyArray<ServerProviderModel>;
}) =>
  Effect.gen(function* () {
    const { runtime, startResult, modelSelection } = input;
    const models = startResult.sessionSetupResult.models;
    const selected = modelSelection.model;
    if (selected === "default" || selected === "auto" || selected === "") {
      // Mode switching can replace the current model; setup-time metadata is
      // not evidence of the native model now running.
      return undefined;
    }
    const catalogModel = input.models.find((model) => model.slug === selected && !model.isCustom);
    if (!catalogModel || !modelSupportsInteractionMode(catalogModel, input.interactionMode)) {
      return yield* AcpErrors.AcpRequestError.invalidParams(
        "Mastra Code model is unavailable in this interaction mode. Refresh the model list.",
      );
    }
    if (!models?.availableModels.some((model) => model.modelId === selected)) {
      return yield* AcpErrors.AcpRequestError.invalidParams(
        "Mastra Code model is unavailable in this session.",
      );
    }
    // Explicit selection must reach the current native mode even when the
    // original session setup happened to advertise the same model.
    yield* runtime.setSessionModel(selected);
    return selected;
  });
