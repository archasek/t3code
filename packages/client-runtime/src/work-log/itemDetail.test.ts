import { ThreadId, TurnItemId, type OrchestrationV2TurnItem } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import {
  turnItemHasDetail,
  turnItemNeedsDetailFetch,
  turnItemOutputImages,
  turnItemOutputText,
} from "./itemDetail.ts";

const tool = (
  fields: Partial<Extract<OrchestrationV2TurnItem, { type: "dynamic_tool" }>> = {},
): OrchestrationV2TurnItem => ({
  id: TurnItemId.make("tool"),
  threadId: ThreadId.make("thread"),
  runId: null,
  nodeId: null,
  providerThreadId: null,
  providerTurnId: null,
  nativeItemRef: null,
  parentItemId: null,
  ordinal: 1,
  status: "completed",
  title: null,
  startedAt: null,
  completedAt: null,
  updatedAt: DateTime.makeUnsafe("2026-10-02T12:00:00.000Z"),
  type: "dynamic_tool",
  toolName: "list",
  input: {},
  ...fields,
});

describe("dynamic tool detail", () => {
  it.each([
    ["Found a device", "Found a device"],
    [{ devices: ["phone"] }, '{\n  "devices": [\n    "phone"\n  ]\n}'],
    [{ content: [{ type: "text", text: "Found a device" }] }, "Found a device"],
  ])("expands empty input with formatted inline output %j", (output, expected) => {
    const item = tool({ output });
    expect(turnItemHasDetail(item)).toBe(true);
    expect(turnItemOutputText(item)).toBe(expected);
    expect(turnItemNeedsDetailFetch(item)).toBe(false);
  });

  it.each([undefined, null, "", "  \n", {}, [], { content: [] }])(
    "keeps empty input and empty output %j closed",
    (output) => {
      const item = tool({ output });
      expect(turnItemHasDetail(item)).toBe(false);
      expect(turnItemOutputText(item)).toBeNull();
      expect(turnItemNeedsDetailFetch(item)).toBe(false);
    },
  );

  it("keeps explicitly omitted output expandable and fetchable", () => {
    const item = tool({ outputOmitted: true });
    expect(turnItemHasDetail(item)).toBe(true);
    expect(turnItemNeedsDetailFetch(item)).toBe(true);
    expect(turnItemOutputText(item)).toBeNull();
  });
});

const screenshot = {
  id: TurnItemId.make("tool-screenshot"),
  type: "dynamic_tool" as const,
  threadId: ThreadId.make("thread-1"),
  runId: null,
  nodeId: null,
  providerThreadId: null,
  providerTurnId: null,
  nativeItemRef: null,
  parentItemId: null,
  ordinal: 1,
  status: "completed" as const,
  title: null,
  toolName: "mcp__t3-code__device_screenshot",
  input: { deviceId: "phone" },
  // What a detail read returns: the image's position without its bytes.
  output: {
    content: [
      { type: "text", text: "Captured the home screen." },
      { type: "image", mimeType: "image/png" },
    ],
  },
  startedAt: DateTime.makeUnsafe("2026-10-05T00:00:00.000Z"),
  completedAt: DateTime.makeUnsafe("2026-10-05T00:00:01.000Z"),
  updatedAt: DateTime.makeUnsafe("2026-10-05T00:00:01.000Z"),
};

describe("tool output images", () => {
  it("shows a screenshot as an image asset, not as text", () => {
    expect(turnItemOutputImages(screenshot)).toEqual([
      {
        _tag: "tool-output-image",
        threadId: screenshot.threadId,
        itemId: screenshot.id,
        index: 0,
      },
    ]);
    expect(turnItemOutputText(screenshot)).toBe("Captured the home screen.");
    expect(
      turnItemOutputText({ ...screenshot, output: { content: [screenshot.output.content[1]] } }),
    ).toBeNull();
  });

  it("keeps a placeholder for images it cannot show", () => {
    const output = { content: [{ type: "image", mimeType: "image/svg+xml" }] };
    expect(turnItemOutputImages({ ...screenshot, output })).toEqual([]);
    expect(turnItemOutputText({ ...screenshot, output })).toBe("[image]");
  });
});
