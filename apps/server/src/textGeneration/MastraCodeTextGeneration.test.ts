import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { makeMastraCodeTextGeneration } from "./MastraCodeTextGeneration.ts";

describe("Mastra Code background text generation", () => {
  it.effect("returns a typed unsupported error without starting the interactive harness", () =>
    Effect.gen(function* () {
      const textGeneration = yield* makeMastraCodeTextGeneration;
      const error = yield* textGeneration.generateCommitMessage({} as never).pipe(Effect.flip);

      expect(error._tag).toBe("TextGenerationError");
      expect(error.detail).toContain("interactive coding harness");
    }),
  );
});
