import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { makeMastraCodeTextGeneration } from "./MastraCodeTextGeneration.ts";

describe("Mastra Code background helpers", () => {
  it.effect("rejects every helper without starting the coding harness", () => Effect.gen(function* () {
    const service = yield* makeMastraCodeTextGeneration;
    const tasks = [
      service.generateCommitMessage(),
      service.generatePrContent(),
      service.generateBranchName(),
      service.generateThreadTitle(),
    ];
    for (const task of tasks) {
      const error = yield* task.pipe(Effect.flip);
      expect(error._tag).toBe("TextGenerationError");
      expect(error.detail).toContain("interactive coding harness");
    }
  }));
});
