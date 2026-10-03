import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as NodePath from "@effect/platform-node/NodePath";
import { ChildProcessSpawner } from "effect/unstable/process";
import { describe, expect, it } from "@effect/vitest";
import { prepareMastraCodeForm } from "./MastraCodeForm.ts";
import { acquireMastraCodeFormAdmission, validateMastraCodeStringConstraints } from "./MastraCodeElicitationValidation.ts";

function form(requestedSchema: unknown) {
  return prepareMastraCodeForm({
    request: { mode: "form", message: "Native MC request", requestedSchema },
    nativeRequestId: "request-1",
    threadId: ThreadId.make("thread-1"),
  }, () => Effect.succeed(true));
}

describe("MastraCodeForm", () => {
  it("rejects excess forms before answers and releases capacity idempotently", async () => {
    await Effect.runPromise(Effect.gen(function* () {
      const leases = yield* Effect.all(Array.from({ length: 32 }, () => acquireMastraCodeFormAdmission()));
      try {
        const overload = yield* acquireMastraCodeFormAdmission().pipe(Effect.flip);
        expect(overload._tag).toBe("AcpRequestError");
        yield* leases[0]!;
        yield* leases[0]!;
        const replacement = yield* acquireMastraCodeFormAdmission();
        yield* replacement;
      } finally {
        yield* Effect.all(leases);
      }
    }));
  });
  it.each([false, true])("queues concurrent validations and releases interrupted workers (%s)", async (interruptFirst) => {
    await Effect.runPromise(Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let spawned = 0;
      const spawner = ChildProcessSpawner.make(() => Effect.gen(function* () {
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
      }));
      const validate = (answer: string) => validateMastraCodeStringConstraints(
        { type: "string", format: "email" }, answer,
      ).pipe(
        Effect.provide(NodePath.layer),
        Effect.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner)),
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
    }));
  });
  it("waits for native string validation and cancels a rejected answer", async () => {
    await Effect.runPromise(Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const verdict = yield* Deferred.make<boolean>();
      const prepared = prepareMastraCodeForm({
        request: { mode: "form", message: "Email", requestedSchema: {
          properties: { email: { type: "string", format: "email" } },
          required: ["email"],
        } },
        nativeRequestId: "validation-request",
        threadId: ThreadId.make("thread-1"),
      }, (property, answer) => Effect.gen(function* () {
        expect(property.type).toBe("string");
        expect(property.type === "string" ? property.format : undefined).toBe("email");
        expect(answer).toBe("not-an-email");
        yield* Deferred.succeed(entered, undefined);
        return yield* Deferred.await(verdict);
      }));
      const response = yield* Effect.forkChild(prepared.respond({ email: ["not-an-email"] }));
      yield* Deferred.await(entered);
      expect(response.pollUnsafe()).toBeUndefined();
      yield* Deferred.succeed(verdict, false);
      expect(yield* Fiber.join(response)).toEqual({ action: "cancel" });
    }));
  });
  it("preserves titled multi-select native values", async () => {
    const prepared = form({ properties: { choices: { type: "array", items: { anyOf: [
      { const: "a", title: "Same" }, { const: "b", title: "Same" },
    ] } } }, required: ["choices"] });
    expect(prepared.questions[0]?.options.map((option) => option.value)).toEqual(["a", "b"]);
    expect(await Effect.runPromise(prepared.respond({ choices: ["b"] }))).toEqual({ action: "accept", content: { choices: ["b"] } });
  });
  it.each(["", "  padded  ", "first\nsecond"])("preserves the literal string %j", async (answer) => {
    const prepared = form({ type: "object", properties: { text: { type: "string" } }, required: ["text"] });
    expect(prepared.questions[0]?.answerFormat).toBe("raw-string");
    expect(await Effect.runPromise(prepared.respond({ text: [answer] }))).toEqual({ action: "accept", content: { text: answer } });
    expect(await Effect.runPromise(prepared.respond({}))).toEqual({ action: "cancel" });
  });

  it("coerces scalars, preserves enum values and array choices", async () => {
    const prepared = form({ type: "object", properties: {
      count: { type: "integer", minimum: 1, maximum: 3 },
      enabled: { type: "boolean" },
      choice: { type: "string", oneOf: [{ const: "a", title: "Same" }, { const: "b", title: "Same" }] },
      tags: { type: "array", items: { type: "string", enum: ["x", "y"] }, minItems: 1 },
    }, required: ["count", "enabled", "choice", "tags"] });
    expect(prepared.questions.find((question) => question.id === "choice")?.options.map((option) => option.value)).toEqual(["a", "b"]);
    expect(await Effect.runPromise(prepared.respond({ count: ["2"], enabled: ["true"], choice: ["b"], tags: ["x", "y"] }))).toEqual({ action: "accept", content: { count: 2, enabled: true, choice: "b", tags: ["x", "y"] } });
    expect(await Effect.runPromise(prepared.respond({ count: ["4"], enabled: ["true"], choice: ["b"], tags: ["x"] }))).toEqual({ action: "cancel" });
  });

  it("omits an optional value but rejects mixing its sentinel with choices", async () => {
    const prepared = form({ type: "object", properties: { tags: { type: "array", items: { type: "string", enum: ["x"] } } } });
    const skip = prepared.questions[0]!.options.find((option) => option.label === "Leave unset")!.value!;
    expect(await Effect.runPromise(prepared.respond({ tags: [skip] }))).toEqual({ action: "accept", content: {} });
    expect(await Effect.runPromise(prepared.respond({ tags: [skip, "x"] }))).toEqual({ action: "cancel" });
  });

  it("requires confirmation for an empty form and fails closed on invalid schema", async () => {
    const prepared = form({ type: "object", properties: {} });
    const id = prepared.questions[0]!.id;
    expect(await Effect.runPromise(prepared.respond({ [id]: ["accept"] }))).toEqual({ action: "accept", content: {} });
    expect(await Effect.runPromise(prepared.respond({ [id]: ["decline"] }))).toEqual({ action: "decline" });
    expect(await Effect.runPromise(form({ properties: { text: { type: "unsupported" } } }).respond({ [id]: ["accept"] }))).toEqual({ action: "cancel" });
  });
});
