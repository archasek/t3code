import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { applyMastraCodeModelSelection } from "./MastraCodeModelSelection.ts";

it.effect("uses native model switching and reports the applied model for A-B-A", () => Effect.gen(function* () {
  const calls: string[] = [];
  let currentModelId = "model-a";
  const select = (model: string) => applyMastraCodeModelSelection({
    runtime: { setSessionModel: (id) => Effect.sync(() => { calls.push(id); currentModelId = id; return {}; }) },
    startResult: { sessionSetupResult: { sessionId: "native", models: {
      currentModelId, availableModels: ["model-a", "model-b"].map((modelId) => ({ modelId, name: modelId })),
    } } },
    modelSelection: { model },
  });
  expect(yield* select("model-a")).toBe("model-a");
  expect(yield* select("model-b")).toBe("model-b");
  expect(yield* select("model-a")).toBe("model-a");
  expect(yield* select("default")).toBeUndefined();
  expect(calls).toEqual(["model-a", "model-b", "model-a"]);
  const error = yield* select("unavailable").pipe(Effect.flip);
  expect(error._tag).toBe("AcpRequestError");
  expect(calls).toEqual(["model-a", "model-b", "model-a"]);
}));

it.effect("does not silently accept an explicit model without native discovery", () => Effect.gen(function* () {
  let called = false;
  const input = {
    runtime: { setSessionModel: () => Effect.sync(() => { called = true; return {}; }) },
    startResult: { sessionSetupResult: { sessionId: "native" } },
  };
  expect(yield* applyMastraCodeModelSelection({ ...input, modelSelection: { model: "default" } })).toBeUndefined();
  const error = yield* applyMastraCodeModelSelection({ ...input, modelSelection: { model: "model-a" } }).pipe(Effect.flip);
  expect(error._tag).toBe("AcpRequestError");
  expect(called).toBe(false);
}));
