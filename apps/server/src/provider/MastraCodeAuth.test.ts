import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProviderInstanceId, ProviderSetupError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Option from "effect/Option";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import {
  isMastraCodeAuthLoginSuccessful,
  parseMastraCodeAuthJsonlLine,
  parseMastraCodeDeviceCodeEvent,
  makeMastraCodeAuth,
} from "./MastraCodeAuth.ts";

const validEvent = {
  type: "device_code",
  verificationUrl: "https://auth.openai.com/codex/device",
  userCode: "ABCD-EFGH",
  expiresAt: "2030-01-01T00:00:00.000Z",
};

describe("parseMastraCodeDeviceCodeEvent", () => {
  it("accepts a future OpenAI Codex device code", () => {
    expect(parseMastraCodeDeviceCodeEvent(validEvent, 0)).toEqual({
      url: "https://auth.openai.com/codex/device",
      userCode: "ABCD-EFGH",
      expiresAt: Date.parse(validEvent.expiresAt),
    });
  });

  it.each([
    { ...validEvent, verificationUrl: "https://evil.example/codex/device" },
    { ...validEvent, verificationUrl: "https://auth.openai.com/" },
    { ...validEvent, verificationUrl: "https://auth.openai.com/codex/device?next=evil" },
    { ...validEvent, userCode: "  " },
    { ...validEvent, expiresAt: "not a date" },
  ])("rejects unsafe or incomplete device-code data", (event) => {
    expect(parseMastraCodeDeviceCodeEvent(event, 0)).toBeUndefined();
  });

  it("rejects an expired code", () => {
    expect(
      parseMastraCodeDeviceCodeEvent(
        { ...validEvent, expiresAt: "2029-12-31T23:59:59.000Z" },
        Date.parse("2030-01-01T00:00:00.000Z"),
      ),
    ).toBeUndefined();
  });
});

it.effect.each([false, true])(
  "refreshes native auth before terminal publication (failure=%s)",
  (verificationFails) =>
    Effect.gen(function* () {
      const nativeSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const changes: boolean[] = [];
      const spawner = ChildProcessSpawner.make((command) => {
        if (!ChildProcess.isStandardCommand(command)) return Effect.die("Unexpected pipeline");
        const login = command.args.includes("login");
        const events = login
          ? [
              { ...validEvent, expiresAt: "2099-01-01T00:00:00.000Z" },
              { type: "success", provider: "openai-codex" },
            ]
          : [];
        return nativeSpawner.spawn(
          ChildProcess.make(process.execPath, [
            "-e",
            `process.stdout.write(${JSON.stringify(events.map((event) => JSON.stringify(event)).join("\n") + "\n")})`,
          ]),
        );
      });
      const controller = yield* makeMastraCodeAuth({
        instanceId: ProviderInstanceId.make("mc-auth-refresh"),
        binaryPath: process.execPath,
        appDataDirectory: "/unused-test-only",
        environment: { PATH: process.env.PATH },
        onChanged: (signedIn) =>
          Effect.gen(function* () {
            changes.push(signedIn);
            if (signedIn && verificationFails)
              return yield* new ProviderSetupError({
                instanceId: ProviderInstanceId.make("mc-auth-refresh"),
                operation: "start",
                detail: "Could not verify native sign-in.",
              });
          }),
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
      yield* controller.start("owner");
      const terminal = yield* controller.subscribe("owner").pipe(
        Stream.filter((state) => state.phase === "succeeded" || state.phase === "failed"),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
        Effect.timeout("10 seconds"),
      );
      expect(changes).toEqual([true]);
      expect(terminal.phase).toBe(verificationFails ? "failed" : "succeeded");
      if (verificationFails) expect(terminal.message).toBe("Could not verify native sign-in.");
      yield* controller.logout(Effect.void);
      expect(changes).toEqual([true, false]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

describe("parseMastraCodeAuthJsonlLine", () => {
  it("preserves the unreadable auth-store diagnostic", () => {
    expect(parseMastraCodeAuthJsonlLine('{"type":"error","code":"AUTH_STORE_UNREADABLE"}')).toEqual(
      { type: "event", event: { type: "error", code: "AUTH_STORE_UNREADABLE" } },
    );
  });
  it("accepts one object event per JSONL line and skips blank lines", () => {
    expect(parseMastraCodeAuthJsonlLine("  \n")).toEqual({ type: "empty" });
    expect(
      parseMastraCodeAuthJsonlLine('{"type":"progress","phase":"waiting_for_authorization"}'),
    ).toEqual({
      type: "event",
      event: { type: "progress", phase: "waiting_for_authorization" },
    });
  });

  it.each([
    "success",
    "{not-json}",
    "[]",
    "null",
    '"event"',
    '{"type":"custom","accessToken":"secret"}',
    '{"type":"success","provider":"openai-codex","refresh":"secret"}',
    '{"type":"success","provider":"openai-codex","account":{"id":"user","label":"user@example.test","access":"secret"}}',
    '{"type":"error","code":"LOGIN_FAILED","token":"secret"}',
    '{"type":"error","code":"secret-bearing error"}',
  ])("rejects malformed, unknown, or secret-bearing event lines", (line) => {
    expect(parseMastraCodeAuthJsonlLine(line)).toEqual({ type: "invalid" });
  });

  it("does not accept success after a malformed line or an error event", () => {
    expect(
      isMastraCodeAuthLoginSuccessful({
        exitCode: 0,
        loginSucceeded: true,
        loginFailed: false,
        outputInvalid: true,
      }),
    ).toBe(false);
    expect(
      isMastraCodeAuthLoginSuccessful({
        exitCode: 0,
        loginSucceeded: true,
        loginFailed: true,
        outputInvalid: false,
      }),
    ).toBe(false);
    expect(
      isMastraCodeAuthLoginSuccessful({
        exitCode: 0,
        loginSucceeded: true,
        loginFailed: false,
        outputInvalid: false,
      }),
    ).toBe(true);
  });
});
