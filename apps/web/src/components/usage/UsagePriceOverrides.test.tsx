// @vitest-environment jsdom
import { EnvironmentId } from "@t3tools/contracts";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

const testState = vi.hoisted(() => ({ useAtomValue: vi.fn(), updateSettings: vi.fn() }));
vi.mock("@effect/atom-react", () => ({ useAtomValue: testState.useAtomValue }));
vi.mock("../../env", () => ({ isElectron: false }));
vi.mock("../../state/presentation", () => ({ environmentPresentations: {} }));
vi.mock("../../state/server", () => ({ serverEnvironment: {} }));
vi.mock("../../state/session", () => ({ environmentSession: {} }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => testState.updateSettings }));
vi.mock("../ui/scroll-area", () => ({ ScrollArea: "div" }));

import { UsagePriceOverrides } from "./UsagePriceOverrides";

let renderer: Root;
let container: HTMLDivElement;

beforeEach(() => {
  testState.useAtomValue.mockReturnValue([
    {
      environmentId: EnvironmentId.make("test-environment"),
      label: "Test environment",
      prices: {},
      aliases: {},
      unavailable: null,
    },
  ]);
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  renderer = createRoot(container);
});

afterEach(async () => {
  await act(() => renderer.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

async function typeInto(input: HTMLInputElement, value: string) {
  await act(() => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, value);
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
}

it("shows the required model error for an alias-only draft and clears it after entering a model", async () => {
  await act(() =>
    renderer.render(
      <UsagePriceOverrides
        usage={[]}
        initialSelectedEnvironmentIds={null}
        onOpenChange={vi.fn()}
      />,
    ),
  );
  await act(() =>
    document.querySelector<HTMLButtonElement>('[aria-label="Add model price"]')!.click(),
  );
  const model = document.querySelector<HTMLInputElement>('[aria-label="New model ID"]')!;
  const alias = document.querySelector<HTMLInputElement>('[aria-label="Map new model to model"]')!;
  expect(model.getAttribute("aria-invalid")).toBeNull();
  expect(document.querySelector('[role="alert"]')).toBeNull();

  await typeInto(alias, "claude-sonnet-4-6");
  expect(document.querySelector('[role="alert"]')?.textContent).toBe("Enter a model ID.");
  expect(model.getAttribute("aria-invalid")).toBe("true");

  await typeInto(model, "custom-sonnet");
  expect(document.querySelector('[role="alert"]')).toBeNull();
  expect(model.getAttribute("aria-invalid")).toBeNull();
  const save = [...document.querySelectorAll<HTMLButtonElement>("button")].find(
    (button) => button.textContent === "Save changes",
  )!;
  expect(save.disabled).toBe(false);
});
