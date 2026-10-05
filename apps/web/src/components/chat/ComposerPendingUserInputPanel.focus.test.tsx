// @vitest-environment jsdom
import { act, cloneElement, useEffect, useState, type ReactElement, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { expect, it, vi } from "vite-plus/test";
import { RuntimeRequestId } from "@t3tools/contracts";
import { ComposerPendingUserInputPanel } from "./ComposerPendingUserInputPanel";
import {
  buildPendingUserInputAnswers,
  setPendingUserInputCustomAnswer,
  synchronizePendingUserInputCursor,
  type PendingUserInputDraftAnswer,
} from "../../pendingUserInput";

vi.mock("../ui/collapsible", () => ({
  Collapsible: ({ children }: { children: ReactNode }) => <>{children}</>,
  CollapsiblePanel: ({ children }: { children: ReactNode }) => <>{children}</>,
  CollapsibleTrigger: ({ render, children }: { render: ReactElement; children: ReactNode }) =>
    cloneElement(render, {}, children),
}));

it.each([true, false, undefined])(
  "keeps multi-character literal input focused and raw (%s)",
  async (allowEmptyAnswer) => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    const container = document.createElement("div");
    document.body.append(container);
    const root = createRoot(container);
    const tiptap = document.createElement("div");
    tiptap.tabIndex = 0;
    document.body.append(tiptap);
    const composer = {
      readSnapshot: vi.fn(() => ({ value: "stale TipTap text", cursor: 0, expandedCursor: 0 })),
      focusAt: () => tiptap.focus(),
    };
    const question = {
      id: "literal",
      header: "Value",
      question: "Exact value",
      multiSelect: false,
      answerFormat: "raw-string" as const,
      allowEmptyAnswer,
      options: [],
    };
    const recordAnswers = vi.fn();
    function Harness() {
      const [draft, setDraft] = useState<PendingUserInputDraftAnswer>();
      useEffect(() => {
        recordAnswers(buildPendingUserInputAnswers([question], draft ? { literal: draft } : {}));
      }, [draft]);
      return (
        <ComposerPendingUserInputPanel
          pendingUserInputs={[
            {
              requestId: RuntimeRequestId.make("focus-request"),
              createdAt: "2026-10-05T00:00:00Z",
              responseCapability: "live",
              dismissible: false,
              questions: [question],
            },
          ]}
          respondingRequestIds={[]}
          answers={draft ? { literal: draft } : {}}
          questionIndex={0}
          onToggleOption={() => {}}
          onChangeCustomAnswer={(_, value) => {
            setDraft((previous) => setPendingUserInputCustomAnswer(previous, value, question));
            synchronizePendingUserInputCursor(
              composer,
              value,
              value.length,
              value.length,
              "literal-input",
            );
          }}
          onAdvance={() => {}}
          onDismiss={() => {}}
        />
      );
    }
    try {
      await act(async () => root.render(<Harness />));
      const textarea = container.querySelector("textarea")!;
      textarea.focus();
      const setValue = Object.getOwnPropertyDescriptor(
        HTMLTextAreaElement.prototype,
        "value",
      )!.set!;
      let value = "";
      for (const character of "  alpha\nbeta\t ") {
        expect(document.activeElement).toBe(textarea);
        value += character;
        await act(async () => {
          setValue.call(textarea, value);
          textarea.dispatchEvent(new Event("input", { bubbles: true }));
        });
        expect(document.activeElement).toBe(textarea);
        expect(textarea.value).toBe(value);
        expect(recordAnswers).toHaveBeenLastCalledWith({ literal: value });
      }
      expect(composer.readSnapshot).not.toHaveBeenCalled();
      const empty = Array.from(container.querySelectorAll("button")).find(
        (button) => button.textContent === "Use empty value",
      );
      expect(Boolean(empty)).toBe(allowEmptyAnswer === true);
      if (empty) {
        await act(async () => empty.click());
        expect(recordAnswers).toHaveBeenLastCalledWith({ literal: "" });
        expect(document.activeElement).toBe(textarea);
      }
      // The existing TipTap edit route still repairs a stale snapshot's cursor.
      synchronizePendingUserInputCursor(composer, "TipTap edit", 11, 11);
      expect(document.activeElement).toBe(tiptap);
    } finally {
      await act(async () => root.unmount());
      container.remove();
      tiptap.remove();
      vi.unstubAllGlobals();
    }
  },
);
