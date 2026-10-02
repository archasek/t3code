import { TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as TextGeneration from "./TextGeneration.ts";

function unsupported(operation: string) {
  return new TextGenerationError({
    operation,
    detail:
      "Mastra Code is an interactive coding harness and is not available for background text generation. Select another provider.",
  });
}

/**
 * Background helpers must not start the full coding agent: doing so could
 * execute tools or change workspace files for a title/commit-message request.
 * T3 routes these helpers through another explicitly selected provider.
 */
export const makeMastraCodeTextGeneration = Effect.succeed({
  generateCommitMessage: () => Effect.fail(unsupported("generateCommitMessage")),
  generatePrContent: () => Effect.fail(unsupported("generatePrContent")),
  generateBranchName: () => Effect.fail(unsupported("generateBranchName")),
  generateThreadTitle: () => Effect.fail(unsupported("generateThreadTitle")),
} satisfies TextGeneration.TextGeneration["Service"]);
