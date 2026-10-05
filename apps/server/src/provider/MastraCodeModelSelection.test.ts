import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { ProviderInstanceId } from "@t3tools/contracts";
import {
  applyMastraCodeModelSelection,
  applyMastraCodeThinkingSelection,
} from "./MastraCodeModelSelection.ts";

it.effect("applies native effort for explicit and default models, and rejects stale choices", () =>
  Effect.gen(function* () {
    const calls: string[] = [];
    const instanceId = ProviderInstanceId.make("mc-effort-test");
    const runtime = {
      getConfigOptions: Effect.succeed([
        {
          id: "thought_level",
          name: "Reasoning effort",
          category: "thought_level",
          type: "select" as const,
          currentValue: "low",
          options: ["low", "high"].map((value) => ({ value, name: value })),
        },
      ]),
      setConfigOption: (id: string, value: string | boolean) =>
        Effect.sync(() => {
          calls.push(`${id}:${value}`);
          return { configOptions: [] };
        }),
    };
    for (const model of ["default", "openai/gpt-6.1-sol"]) {
      yield* applyMastraCodeThinkingSelection({
        runtime,
        modelSelection: {
          instanceId,
          model,
          options: [{ id: "thought_level", value: "high" }],
        },
      });
    }
    expect(calls).toEqual(["thought_level:high", "thought_level:high"]);
    const error = yield* applyMastraCodeThinkingSelection({
      runtime,
      modelSelection: {
        instanceId,
        model: "default",
        options: [{ id: "thought_level", value: "max" }],
      },
    }).pipe(Effect.flip);
    expect(error._tag).toBe("AcpRequestError");
    expect(calls).toHaveLength(2);
    yield* applyMastraCodeThinkingSelection({
      runtime,
      modelSelection: { instanceId, model: "default" },
    });
    expect(calls).toHaveLength(2);
  }),
);

it.effect("uses native model switching and reports the applied model for A-B-A", () =>
  Effect.gen(function* () {
    const calls: string[] = [];
    let currentModelId = "model-a";
    const select = (model: string) =>
      applyMastraCodeModelSelection({
        runtime: {
          setSessionModel: (id) =>
            Effect.sync(() => {
              calls.push(id);
              currentModelId = id;
              return {};
            }),
        },
        startResult: {
          sessionSetupResult: {
            sessionId: "native",
            models: {
              currentModelId,
              availableModels: ["model-a", "model-b"].map((modelId) => ({
                modelId,
                name: modelId,
              })),
            },
          },
        },
        modelSelection: { model },
        interactionMode: "default",
        models: ["model-a", "model-b"].map((slug) => ({
          slug,
          name: slug,
          isCustom: false,
          capabilities: null,
        })),
      });
    expect(yield* select("model-a")).toBe("model-a");
    expect(yield* select("model-b")).toBe("model-b");
    expect(yield* select("model-a")).toBe("model-a");
    expect(yield* select("default")).toBeUndefined();
    expect(calls).toEqual(["model-a", "model-b", "model-a"]);
    const error = yield* select("unavailable").pipe(Effect.flip);
    expect(error._tag).toBe("AcpRequestError");
    expect(calls).toEqual(["model-a", "model-b", "model-a"]);
  }),
);

it.effect("does not silently accept an explicit model without native discovery", () =>
  Effect.gen(function* () {
    let called = false;
    const input = {
      interactionMode: "default" as const,
      models: [{ slug: "model-a", name: "model-a", isCustom: false, capabilities: null }],
      runtime: {
        setSessionModel: () =>
          Effect.sync(() => {
            called = true;
            return {};
          }),
      },
      startResult: { sessionSetupResult: { sessionId: "native" } },
    };
    expect(
      yield* applyMastraCodeModelSelection({ ...input, modelSelection: { model: "default" } }),
    ).toBeUndefined();
    const error = yield* applyMastraCodeModelSelection({
      ...input,
      modelSelection: { model: "model-a" },
    }).pipe(Effect.flip);
    expect(error._tag).toBe("AcpRequestError");
    expect(called).toBe(false);
  }),
);

it.effect.each([
  { modes: ["plan"] as const, mode: "plan" as const, allowed: true },
  { modes: ["plan"] as const, mode: "default" as const, allowed: false },
  { modes: ["default"] as const, mode: "plan" as const, allowed: false },
  { modes: ["default"] as const, mode: "default" as const, allowed: true },
  { modes: ["default", "plan"] as const, mode: "plan" as const, allowed: true },
  { modes: undefined, mode: "plan" as const, allowed: true },
])("guards explicit model mode ($mode, $modes)", ({ modes, mode, allowed }) =>
  Effect.gen(function* () {
    const applied: string[] = [];
    const result = yield* applyMastraCodeModelSelection({
      runtime: {
        setSessionModel: (id) =>
          Effect.sync(() => {
            applied.push(id);
            return {};
          }),
      },
      startResult: {
        sessionSetupResult: {
          sessionId: "native",
          models: {
            currentModelId: "model",
            availableModels: [{ modelId: "model", name: "model" }],
          },
        },
      },
      modelSelection: { model: "model" },
      interactionMode: mode,
      models: [
        {
          slug: "model",
          name: "model",
          isCustom: false,
          capabilities: null,
          ...(modes ? { supportedInteractionModes: modes } : {}),
        },
      ],
    }).pipe(Effect.result);
    expect(result._tag).toBe(allowed ? "Success" : "Failure");
    expect(applied).toEqual(allowed ? ["model"] : []);
  }),
);

it.effect.each([true, false])(
  "configured custom/stale models cannot bypass the native catalog (%s)",
  (custom) =>
    Effect.gen(function* () {
      let applied = false;
      const error = yield* applyMastraCodeModelSelection({
        runtime: {
          setSessionModel: () =>
            Effect.sync(() => {
              applied = true;
              return {};
            }),
        },
        startResult: {
          sessionSetupResult: {
            sessionId: "native",
            models: {
              currentModelId: "stale",
              availableModels: [{ modelId: "stale", name: "stale" }],
            },
          },
        },
        modelSelection: { model: "stale" },
        interactionMode: "default",
        models: custom
          ? [{ slug: "stale", name: "stale", isCustom: true, capabilities: null }]
          : [],
      }).pipe(Effect.flip);
      expect(error._tag).toBe("AcpRequestError");
      expect(applied).toBe(false);
    }),
);
