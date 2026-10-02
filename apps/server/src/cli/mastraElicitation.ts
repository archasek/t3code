import * as Effect from "effect/Effect";
import { Command } from "effect/unstable/cli";
import { runMastraElicitationWorker } from "../mastraElicitationWorker.ts";

export const mastraElicitationCommand = Command.make("__mastra-elicitation").pipe(
  Command.unlisted,
  Command.withHandler(() => Effect.promise(runMastraElicitationWorker)),
);
