import { OrchestrationV2UserInputQuestion, ThreadId, UserInputQuestion } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as AcpErrors from "effect-acp/errors";
import * as Layer from "effect/Layer";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as Path from "effect/Path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodePath from "@effect/platform-node/NodePath";
import { ChildProcessSpawner } from "effect/unstable/process";
import { describe, expect, it } from "@effect/vitest";
import { prepareMastraCodeForm } from "./MastraCodeForm.ts";
import {
  acquireMastraCodeFormAdmission,
  validateMastraCodeStringConstraints,
} from "./MastraCodeElicitationValidation.ts";

const encodeRuntimeQuestion = Schema.encodeEffect(UserInputQuestion);
const decodeRuntimeQuestion = Schema.decodeEffect(UserInputQuestion);
const encodeOrchestrationQuestion = Schema.encodeEffect(OrchestrationV2UserInputQuestion);
const decodeOrchestrationQuestion = Schema.decodeEffect(OrchestrationV2UserInputQuestion);

function form(requestedSchema: unknown) {
  return prepareMastraCodeForm(
    {
      request: { mode: "form", message: "Native MC request", requestedSchema },
      nativeRequestId: "request-1",
      threadId: ThreadId.make("thread-1"),
    },
    () => Effect.succeed(true),
  );
}

describe("MastraCodeForm", () => {
  it.effect(
    "consumes the validated native result without running the constraint worker twice",
    () =>
      Effect.gen(function* () {
        let validations = 0;
        const prepared = yield* prepareMastraCodeForm(
          {
            request: {
              mode: "form",
              message: "Code",
              requestedSchema: {
                properties: { code: { type: "string", pattern: "^ok$", minLength: 1 } },
                required: ["code"],
              },
            },
            nativeRequestId: "cached-form",
            threadId: ThreadId.make("thread-1"),
          },
          () => Effect.sync(() => ++validations === 1),
        );
        expect(yield* prepared.validateAnswers!({ code: ["ok"] })).toBe(true);
        expect(yield* prepared.respond({ code: ["ok"] })).toEqual({
          action: "accept",
          content: { code: "ok" },
        });
        expect(validations).toBe(1);
        expect(yield* prepared.respond(null)).toEqual({ action: "cancel" });
      }),
  );
  it.effect("validates a correction without settling or changing native response semantics", () =>
    Effect.gen(function* () {
      const prepared = yield* form({
        type: "object",
        properties: { count: { type: "integer", minimum: 1, maximum: 3 } },
        required: ["count"],
      });
      expect(yield* prepared.validateAnswers!({ count: ["4"] })).toBe(false);
      expect(yield* prepared.validateAnswers!({})).toBe(false);
      expect(yield* prepared.validateAnswers!({ count: ["2"] })).toBe(true);
      expect(yield* prepared.respond({ count: ["2"] })).toEqual({
        action: "accept",
        content: { count: 2 },
      });
      expect(yield* prepared.respond(null)).toEqual({ action: "cancel" });
    }),
  );
  it.effect("uses collision-safe UI IDs while preserving exact native field keys", () =>
    Effect.gen(function* () {
      const keys = ["", " padded ", "mastra-field-0", "mastra-field-1", "__proto__"];
      const prepared = yield* form({
        type: "object",
        properties: Object.fromEntries(keys.map((key) => [key, { type: "string" }])),
        required: keys,
      });
      expect(prepared.questions).toHaveLength(keys.length);
      expect(new Set(prepared.questions.map((question) => question.id)).size).toBe(keys.length);
      for (const question of prepared.questions) {
        expect(question.id.length).toBeGreaterThan(0);
        expect(question.id).toBe(question.id.trim());
        yield* encodeOrchestrationQuestion(question).pipe(
          Effect.flatMap(decodeOrchestrationQuestion),
        );
        expect(question.allowEmptyAnswer).toBe(true);
      }
      const answers = Object.fromEntries(
        prepared.questions.map((question, index) => [question.id, [`value-${index}`]]),
      );
      expect(yield* prepared.respond(answers)).toEqual({
        action: "accept",
        content: Object.fromEntries(keys.map((key, index) => [key, `value-${index}`])),
      });
      expect(yield* prepared.respond({})).toEqual({ action: "cancel" });
    }),
  );

  it.effect("rejects excess forms before answers and releases capacity idempotently", () =>
    Effect.gen(function* () {
      const leases = yield* Effect.all(
        Array.from({ length: 32 }, () => acquireMastraCodeFormAdmission()),
      );
      try {
        const overload = yield* acquireMastraCodeFormAdmission().pipe(
          Effect.match({ onFailure: (error) => error._tag, onSuccess: () => "unexpected-success" }),
        );
        expect(overload).toBe("AcpRequestError");
        yield* leases[0]!;
        yield* leases[0]!;
        const replacement = yield* acquireMastraCodeFormAdmission();
        yield* replacement;
      } finally {
        yield* Effect.all(leases);
      }
    }),
  );
  it.effect.each([false, true])(
    "queues concurrent validations and releases interrupted workers (%s)",
    (interruptFirst) =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let spawned = 0;
        const spawner = ChildProcessSpawner.make(() =>
          Effect.gen(function* () {
            spawned += 1;
            const first = spawned === 1;
            yield* Deferred.succeed(entered, undefined);
            return ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(spawned),
              exitCode: (first ? Deferred.await(release) : Effect.void).pipe(
                Effect.as(ChildProcessSpawner.ExitCode(0)),
              ),
              isRunning: Effect.succeed(true),
              kill: () => Effect.void,
              unref: Effect.succeed(Effect.void),
              stdin: Sink.drain,
              stdout: Stream.encodeText(Stream.make("true")),
              stderr: Stream.empty,
              all: Stream.empty,
              getInputFd: () => Sink.drain,
              getOutputFd: () => Stream.empty,
            });
          }),
        );
        const validate = (answer: string) =>
          validateMastraCodeStringConstraints({ type: "string", format: "email" }, answer).pipe(
            Effect.provide(
              Layer.mergeAll(
                NodePath.layer,
                Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
              ),
            ),
          );
        const first = yield* Effect.forkChild(validate("first@example.com"));
        yield* Deferred.await(entered);
        const second = yield* Effect.forkChild(validate("second@example.com"));
        yield* Effect.yieldNow;
        expect(spawned).toBe(1);
        expect(second.pollUnsafe()).toBeUndefined();
        const queued = [];
        for (let index = 0; index < 30; index += 1) {
          queued.push(yield* Effect.forkChild(validate(`queued-${index}@example.com`)));
        }
        yield* Effect.yieldNow;
        expect(spawned).toBe(1);
        if (interruptFirst) {
          yield* Fiber.interrupt(first);
        } else {
          yield* Deferred.succeed(release, undefined);
          expect(yield* Fiber.join(first)).toBe(true);
        }
        expect(yield* Fiber.join(second)).toBe(true);
        for (const pending of queued) expect(yield* Fiber.join(pending)).toBe(true);
        expect(spawned).toBe(32);
        expect(yield* validate("after-drain@example.com")).toBe(true);
        expect(spawned).toBe(33);
      }),
  );
  it.effect("waits for native string validation and cancels a rejected answer", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const verdict = yield* Deferred.make<boolean>();
      const prepared = yield* prepareMastraCodeForm(
        {
          request: {
            mode: "form",
            message: "Email",
            requestedSchema: {
              properties: { email: { type: "string", format: "email" } },
              required: ["email"],
            },
          },
          nativeRequestId: "validation-request",
          threadId: ThreadId.make("thread-1"),
        },
        (property, answer) =>
          Effect.gen(function* () {
            expect(property.type).toBe("string");
            expect(property.type === "string" ? property.format : undefined).toBe("email");
            if (answer === "") return false;
            expect(answer).toBe("not-an-email");
            yield* Deferred.succeed(entered, undefined);
            return yield* Deferred.await(verdict);
          }),
      );
      const response = yield* Effect.forkChild(prepared.respond({ email: ["not-an-email"] }));
      yield* Deferred.await(entered);
      expect(response.pollUnsafe()).toBeUndefined();
      yield* Deferred.succeed(verdict, false);
      expect(yield* Fiber.join(response)).toEqual({ action: "cancel" });
    }),
  );
  it.effect("preserves titled multi-select native values", () =>
    Effect.gen(function* () {
      const prepared = yield* form({
        properties: {
          choices: {
            type: "array",
            items: {
              anyOf: [
                { const: "a", title: "Same" },
                { const: "b", title: "Same" },
              ],
            },
          },
        },
        required: ["choices"],
      });
      expect(
        prepared.questions[0]?.options
          .filter((option) => !option.exclusive)
          .map((option) => option.value),
      ).toEqual(["a", "b"]);
      expect(yield* prepared.respond({ choices: ["b"] })).toEqual({
        action: "accept",
        content: { choices: ["b"] },
      });
    }),
  );
  it.effect.each(["", "  padded  ", "first\nsecond"])("preserves the literal string %j", (answer) =>
    Effect.gen(function* () {
      const prepared = yield* form({
        type: "object",
        properties: { text: { type: "string" } },
        required: ["text"],
      });
      expect(prepared.questions[0]?.answerFormat).toBe("raw-string");
      expect(yield* prepared.respond({ text: [answer] })).toEqual({
        action: "accept",
        content: { text: answer },
      });
      expect(yield* prepared.respond({})).toEqual({ action: "cancel" });
    }),
  );

  it.effect("coerces scalars, preserves enum values and array choices", () =>
    Effect.gen(function* () {
      const prepared = yield* form({
        type: "object",
        properties: {
          count: { type: "integer", minimum: 1, maximum: 3 },
          enabled: { type: "boolean" },
          choice: {
            type: "string",
            oneOf: [
              { const: "a", title: "Same" },
              { const: "b", title: "Same" },
            ],
          },
          tags: { type: "array", items: { type: "string", enum: ["x", "y"] }, minItems: 1 },
        },
        required: ["count", "enabled", "choice", "tags"],
      });
      expect(
        prepared.questions
          .find((question) => question.id === "choice")
          ?.options.map((option) => option.value),
      ).toEqual(["a", "b"]);
      expect(
        yield* prepared.respond({
          count: ["2"],
          enabled: ["true"],
          choice: ["b"],
          tags: ["x", "y"],
        }),
      ).toEqual({
        action: "accept",
        content: { count: 2, enabled: true, choice: "b", tags: ["x", "y"] },
      });
      expect(
        yield* prepared.respond({ count: ["4"], enabled: ["true"], choice: ["b"], tags: ["x"] }),
      ).toEqual({ action: "cancel" });
    }),
  );

  it.effect("omits an optional value but rejects mixing its sentinel with choices", () =>
    Effect.gen(function* () {
      const prepared = yield* form({
        type: "object",
        properties: { tags: { type: "array", items: { type: "string", enum: ["x"] } } },
      });
      const skip = prepared.questions[0]!.options.find(
        (option) => option.label === "Leave unset",
      )!.value!;
      expect(
        prepared.questions[0]!.options.find((option) => option.value === skip)?.exclusive,
      ).toBe(true);
      expect(yield* prepared.respond({ tags: [skip] })).toEqual({ action: "accept", content: {} });
      expect(yield* prepared.respond({ tags: [skip, "x"] })).toEqual({ action: "cancel" });
    }),
  );

  it.effect("requires confirmation for an empty form and fails closed on invalid schema", () =>
    Effect.gen(function* () {
      const prepared = yield* form({ type: "object", properties: {} });
      const id = prepared.questions[0]!.id;
      expect(yield* prepared.respond({ [id]: ["accept"] })).toEqual({
        action: "accept",
        content: {},
      });
      expect(yield* prepared.respond({ [id]: ["decline"] })).toEqual({ action: "decline" });
      expect(
        yield* (yield* form({ properties: { text: { type: "unsupported" } } })).respond({
          [id]: ["accept"],
        }),
      ).toEqual({ action: "cancel" });
    }),
  );

  it.effect.each([
    { properties: {}, required: ["token"] },
    { properties: { value: { type: "string", const: "fixed" } } },
    { properties: { value: { type: "number", multipleOf: 2 } } },
    { properties: { value: { type: "number", exclusiveMinimum: 0 } } },
    { properties: { value: { type: "number", exclusiveMaximum: 4 } } },
    { properties: { value: { type: "array", items: { enum: ["x"] }, uniqueItems: true } } },
    { properties: { value: { type: "string", oneOf: [{ const: "x", pattern: "^x$" }] } } },
    { properties: {}, allOf: [{ required: ["token"] }] },
  ])("does not accept a schema whose requirements are unsupported: %j", (schema) =>
    Effect.gen(function* () {
      const prepared = yield* form(schema);
      const id = prepared.questions[0]!.id;
      expect(yield* prepared.respond({ [id]: ["accept"], value: "3" })).toEqual({
        action: "cancel",
      });
    }),
  );
});

// Exercise eligibility through the same bounded worker used for native replies.
it.layer(NodeServices.layer)("MC empty string eligibility", (it) => {
  it.effect.each([
    { property: { type: "string", minLength: 1 }, eligible: false, answer: "value" },
    { property: { type: "string", format: "email" }, eligible: false, answer: "a@example.com" },
    { property: { type: "string", pattern: "^.+$" }, eligible: false, answer: "value" },
    { property: { type: "string" }, eligible: true, answer: "" },
    { property: { type: "string", pattern: "^$" }, eligible: true, answer: "" },
  ])("publishes truthful eligibility for $property", ({ property, eligible, answer }) =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const prepared = yield* prepareMastraCodeForm(
        {
          request: {
            mode: "form",
            message: "Exact value",
            requestedSchema: { properties: { value: property }, required: ["value"] },
          },
          nativeRequestId: "empty-eligibility",
          threadId: ThreadId.make("thread-1"),
        },
        (property, answer) =>
          validateMastraCodeStringConstraints(property, answer).pipe(
            Effect.provideService(Path.Path, path),
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          ),
      );
      expect(prepared.questions[0]).toMatchObject({
        answerFormat: "raw-string",
        allowEmptyAnswer: eligible,
      });
      expect(yield* prepared.respond({ value: "" })).toEqual(
        eligible ? { action: "accept", content: { value: "" } } : { action: "cancel" },
      );
      expect(yield* prepared.respond({ value: answer })).toEqual({
        action: "accept",
        content: { value: answer },
      });
    }),
  );
});

it.effect("awaits empty eligibility before publishing a form", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const verdict = yield* Deferred.make<boolean>();
    const preparation = yield* Effect.forkChild(
      prepareMastraCodeForm(
        {
          request: {
            mode: "form",
            message: "Value",
            requestedSchema: { properties: { value: { type: "string", pattern: "^$" } } },
          },
          nativeRequestId: "preparation",
          threadId: ThreadId.make("thread-1"),
        },
        (_, answer) =>
          Effect.gen(function* () {
            expect(answer).toBe("");
            yield* Deferred.succeed(entered, undefined);
            return yield* Deferred.await(verdict);
          }),
      ),
    );
    yield* Deferred.await(entered);
    expect(preparation.pollUnsafe()).toBeUndefined();
    yield* Deferred.succeed(verdict, true);
    expect((yield* Fiber.join(preparation)).questions[0]?.allowEmptyAnswer).toBe(true);
  }),
);

it.effect("keeps omission sentinel collision-safe and sends exactly omission", () =>
  Effect.gen(function* () {
    const prepared = yield* form({
      properties: { tags: { type: "array", items: { enum: ["request-1:omit:tags", "x"] } } },
    });
    const skip = prepared.questions[0]!.options.find((option) => option.label === "Leave unset")!;
    expect(skip.value).toBe("request-1:omit:tags:omit");
    expect(yield* prepared.respond({ tags: [skip.value!] })).toEqual({
      action: "accept",
      content: {},
    });
    expect(yield* prepared.respond({ tags: ["request-1:omit:tags"] })).toEqual({
      action: "accept",
      content: { tags: ["request-1:omit:tags"] },
    });
  }),
);

it.effect("hides unknown empty eligibility while preserving literal replies", () =>
  Effect.gen(function* () {
    const prepared = yield* prepareMastraCodeForm(
      {
        request: {
          mode: "form",
          message: "Value",
          requestedSchema: {
            properties: { value: { type: "string", pattern: "^.*$" } },
            required: ["value"],
          },
        },
        nativeRequestId: "unknown-eligibility",
        threadId: ThreadId.make("thread-1"),
      },
      (_, answer) =>
        answer === ""
          ? Effect.fail(AcpErrors.AcpRequestError.invalidParams("Validation unavailable"))
          : Effect.succeed(true),
    );
    expect(prepared.questions[0]).toMatchObject({
      answerFormat: "raw-string",
      allowCustomAnswer: true,
      allowEmptyAnswer: false,
    });
    expect(yield* prepared.respond({ value: " raw\n" })).toEqual({
      action: "accept",
      content: { value: " raw\n" },
    });
  }),
);

it.effect.each(["enum", "oneOf", "array-enum", "array-anyOf"] as const)(
  "encodes visible labels and preserves exact native choices (%s)",
  (kind) =>
    Effect.gen(function* () {
      const values = ["", " \t", "  ordinary  ", "named"];
      const titled = values.map((value) => ({
        const: value,
        title: value === "named" ? "  Display  " : " \t",
      }));
      const array = kind.startsWith("array-");
      const property =
        kind === "enum"
          ? { type: "string", enum: values }
          : kind === "oneOf"
            ? { type: "string", oneOf: titled }
            : {
                type: "array",
                items: kind === "array-enum" ? { enum: values } : { anyOf: titled },
              };
      const prepared = yield* form({ properties: { choice: property }, required: ["choice"] });
      const question = prepared.questions[0]!;
      const labels = [
        '""',
        '" \\t"',
        "ordinary",
        kind === "oneOf" || kind === "array-anyOf" ? "Display" : "named",
      ];
      const encodedRuntimeQuestion = yield* encodeRuntimeQuestion(question);
      const decodedRuntimeQuestion = yield* decodeRuntimeQuestion(encodedRuntimeQuestion);
      const encodedOrchestrationQuestion = yield* encodeOrchestrationQuestion(question);
      const decodedOrchestrationQuestion = yield* decodeOrchestrationQuestion(
        encodedOrchestrationQuestion,
      );
      for (const decoded of [decodedRuntimeQuestion, decodedOrchestrationQuestion]) {
        expect(
          decoded.options.filter((option) => !option.exclusive).map((option) => option.label),
        ).toEqual(labels);
        expect(
          decoded.options.filter((option) => !option.exclusive).map((option) => option.value),
        ).toEqual(values);
      }
      for (const value of values) {
        expect(yield* prepared.respond({ choice: [value] })).toEqual({
          action: "accept",
          content: { choice: array ? [value] : value },
        });
      }
    }),
);

it.effect.each([undefined, 0, 1])(
  "offers explicit empty arrays only when minItems permits them (%s)",
  (minItems) =>
    Effect.gen(function* () {
      const prepared = yield* form({
        properties: { tags: { type: "array", items: { enum: [""] }, minItems } },
        required: ["tags"],
      });
      const empty = prepared.questions[0]!.options.find(
        (option) => option.label === "Use empty array",
      );
      expect(yield* prepared.respond({})).toEqual({ action: "cancel" });
      expect(yield* prepared.respond({ tags: [""] })).toEqual({
        action: "accept",
        content: { tags: [""] },
      });
      if (minItems === 1) {
        expect(empty).toBeUndefined();
        expect(yield* prepared.respond({ tags: ["request-1:empty-array:tags"] })).toEqual({
          action: "cancel",
        });
      } else {
        expect(empty?.exclusive).toBe(true);
        expect(empty?.value).not.toBe("");
        yield* encodeOrchestrationQuestion(prepared.questions[0]!);
        expect(yield* prepared.respond({ tags: [empty!.value!] })).toEqual({
          action: "accept",
          content: { tags: [] },
        });
        expect(yield* prepared.respond({ tags: empty!.value! })).toEqual({
          action: "accept",
          content: { tags: [] },
        });
      }
    }),
);

it.effect("distinguishes optional omission, empty arrays and colliding native array items", () =>
  Effect.gen(function* () {
    const values = [
      "",
      " \t",
      "request-1:empty-array:tags",
      "request-1:empty-array:tags:empty-array",
      "request-1:omit:tags",
      "request-1:omit:tags:omit",
    ];
    const prepared = yield* form({
      properties: {
        tags: { type: "array", items: { anyOf: values.map((value) => ({ const: value })) } },
      },
    });
    const options = prepared.questions[0]!.options;
    const empty = options.find((option) => option.label === "Use empty array")!.value!;
    const omit = options.find((option) => option.label === "Leave unset")!.value!;
    expect(empty).toBe("request-1:empty-array:tags:empty-array:empty-array");
    expect(omit).toBe("request-1:omit:tags:omit:omit");
    expect(empty).not.toBe(omit);
    expect(yield* prepared.respond({ tags: [empty] })).toEqual({
      action: "accept",
      content: { tags: [] },
    });
    expect(yield* prepared.respond({ tags: [omit] })).toEqual({ action: "accept", content: {} });
    for (const value of values) {
      expect(yield* prepared.respond({ tags: [value] })).toEqual({
        action: "accept",
        content: { tags: [value] },
      });
    }
    for (const mixed of [
      [empty, ""],
      ["", empty],
      [empty, omit],
      [omit, empty],
      [empty, empty],
    ]) {
      expect(yield* prepared.respond({ tags: mixed })).toEqual({ action: "cancel" });
    }
  }),
);

it.effect("validates explicit empty arrays through the existing item limits", () =>
  Effect.gen(function* () {
    const prepared = yield* form({
      properties: { tags: { type: "array", items: { enum: ["x"] }, maxItems: 0 } },
      required: ["tags"],
    });
    const empty = prepared.questions[0]!.options.find(
      (option) => option.label === "Use empty array",
    )!;
    expect(yield* prepared.respond({ tags: [empty.value!] })).toEqual({
      action: "accept",
      content: { tags: [] },
    });
    expect(yield* prepared.respond({ tags: ["x"] })).toEqual({ action: "cancel" });
  }),
);
