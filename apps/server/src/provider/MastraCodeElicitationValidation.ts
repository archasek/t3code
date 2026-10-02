import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess } from "effect/unstable/process";
import type * as AcpSchema from "effect-acp/schema";
import { HostProcessIsExecutable } from "@t3tools/shared/hostProcess";
import { spawnAndCollect } from "./providerSnapshot.ts";
import { MastraElicitationValidationInput } from "../mastraElicitationWorker.ts";

const encodeInput = Schema.encodeSync(Schema.fromJsonString(MastraElicitationValidationInput));

// Reject excess admission rather than queue untrusted regex evaluations without bounds.
let validationActive = false;

export const validateMastraCodeStringConstraints = Effect.fn("validateMastraCodeStringConstraints")(
  function* (property: AcpSchema.ElicitationPropertySchema, answer: unknown) {
    if (property.type !== "string" || typeof answer !== "string") return true;
    if (property.pattern == null && property.format == null) return true;
    if (validationActive) return false;
    validationActive = true;
    return yield* Effect.gen(function* () {
      const input = encodeInput({
        answer,
        pattern: property.pattern ?? undefined,
        format: property.format ?? undefined,
      });
      if (Buffer.byteLength(input) > 256 * 1024) return false;
      const path = yield* Path.Path;
      const args = (yield* HostProcessIsExecutable)
        ? ["__mastra-elicitation"]
        : [
            yield* path.fromFileUrl(
              new URL(
                import.meta.url.endsWith(".ts")
                  ? "../mastra-elicitation-worker.ts"
                  : "./mastra-elicitation-worker.mjs",
                import.meta.url,
              ),
            ),
          ];
      const result = yield* spawnAndCollect(
        process.execPath,
        ChildProcess.make(process.execPath, args, {
          stdin: Stream.make(new TextEncoder().encode(input)),
          killSignal: "SIGKILL",
          // No provider credentials, NODE_OPTIONS or preload hooks are passed to the worker.
          env: { ELECTRON_RUN_AS_NODE: "1", SystemRoot: process.env.SystemRoot },
        }),
      );
      return result.code === 0 && result.stdout === "true";
    }).pipe(
      Effect.timeout("2 seconds"),
      Effect.scoped,
      Effect.orElseSucceed(() => false),
      Effect.ensuring(
        Effect.sync(() => {
          validationActive = false;
        }),
      ),
    );
  },
);
