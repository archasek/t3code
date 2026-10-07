import { type ReactNode, cloneElement, type ReactElement, useState } from "react";
import { act, create, type ReactTestRenderer } from "react-test-renderer";
import { expect, it, vi } from "vite-plus/test";
import { RuntimeRequestId } from "@t3tools/contracts";
import { ComposerPendingUserInputPanel } from "./ComposerPendingUserInputPanel";
import {
  setPendingUserInputCustomAnswer,
  togglePendingUserInputOptionSelection,
  type PendingUserInputDraftAnswer,
} from "../../pendingUserInput";

vi.mock("../ui/collapsible", () => ({
  Collapsible: ({ children }: { children: ReactNode }) => <>{children}</>,
  CollapsiblePanel: ({ children }: { children: ReactNode }) => <>{children}</>,
  CollapsibleTrigger: ({ render, children }: { render: ReactElement; children: ReactNode }) =>
    cloneElement(render, {}, children),
}));

it("cancels option auto-advance when a literal answer is edited or explicitly emptied", async () => {
  vi.useFakeTimers();
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal("window", globalThis);
  vi.stubGlobal("document", { addEventListener() {}, removeEventListener() {} });
  const question = {
    id: "literal",
    header: "Value",
    question: "Exact value",
    multiSelect: false,
    answerFormat: "raw-string" as const,
    allowEmptyAnswer: true,
    options: [{ label: "Preset", description: "Preset" }],
  };
  const advance = vi.fn();
  const changed = vi.fn();
  let renderer: ReactTestRenderer | undefined;
  function Harness() {
    const [draft, setDraft] = useState<PendingUserInputDraftAnswer>();
    return (
      <ComposerPendingUserInputPanel
        pendingUserInputs={[
          {
            requestId: RuntimeRequestId.make("literal-request"),
            createdAt: "2026-10-02T00:00:00Z",
            responseCapability: "live",
            dismissible: false,
            questions: [question],
          },
        ]}
        respondingRequestIds={[]}
        answers={draft ? { literal: draft } : {}}
        questionIndex={0}
        onToggleOption={(_, value) =>
          setDraft((previous) => togglePendingUserInputOptionSelection(question, previous, value))
        }
        onChangeCustomAnswer={(_, value) => {
          changed(value);
          setDraft((previous) => setPendingUserInputCustomAnswer(previous, value, question));
        }}
        onAdvance={advance}
        onDismiss={() => {}}
      />
    );
  }
  try {
    await act(() => {
      renderer = create(<Harness />);
    });
    const option = () =>
      renderer!.root
        .findAllByType("button")
        .find((button) =>
          button.findAllByType("span").some((span) => span.children.includes("Preset")),
        )!;
    await act(() => option().props.onClick());
    await act(() =>
      renderer!.root.findByType("textarea").props.onChange({ target: { value: " padded\n" } }),
    );
    await act(() => {
      vi.advanceTimersByTime(250);
    });
    expect(advance).not.toHaveBeenCalled();
    expect(changed).toHaveBeenLastCalledWith(" padded\n");
    await act(() => option().props.onClick());
    await act(() =>
      renderer!.root
        .findAllByType("button")
        .find((button) => button.children.includes("Use empty value"))!
        .props.onClick(),
    );
    await act(() => {
      vi.advanceTimersByTime(250);
    });
    expect(advance).not.toHaveBeenCalled();
    expect(changed).toHaveBeenLastCalledWith("");
    await act(() => option().props.onClick());
    await act(() => {
      vi.advanceTimersByTime(250);
    });
    expect(advance).toHaveBeenCalledTimes(1);
  } finally {
    if (renderer) await act(() => renderer!.unmount());
    vi.useRealTimers();
    vi.unstubAllGlobals();
  }
});
