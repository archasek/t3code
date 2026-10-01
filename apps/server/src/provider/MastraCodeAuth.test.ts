import { describe, expect, it } from "@effect/vitest";

import {
  isMastraCodeAuthLoginSuccessful,
  parseMastraCodeAuthJsonlLine,
  parseMastraCodeDeviceCodeEvent,
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

describe("parseMastraCodeAuthJsonlLine", () => {
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
