import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as EffectAcpErrors from "effect-acp/errors";
import { ChildProcess } from "effect/unstable/process";
import { HostProcessIsExecutable } from "@t3tools/shared/hostProcess";
import { spawnAndCollect } from "./providerSnapshot.ts";
import { MastraElicitationValidationInput } from "../mastraElicitationWorker.ts";

const encodeInput = Schema.encodeSync(Schema.fromJsonString(MastraElicitationValidationInput));

// Busy workers must not turn another conversation's valid answer into cancellation.
const validationPermit = Semaphore.makeUnsafe(1);
let admittedForms = 0;

export const acquireMastraCodeFormAdmission = Effect.fn("acquireMastraCodeFormAdmission")(
  function* () {
    if (admittedForms >= 32) {
      return yield* new EffectAcpErrors.AcpRequestError({
        code: -32000,
        errorMessage: "Mastra Code form capacity is full; no user question was published.",
      });
    }
    admittedForms += 1;
    let released = false;
    return Effect.sync(() => {
      if (released) return;
      released = true;
      admittedForms -= 1;
    });
  },
);

export const validateMastraCodeStringConstraints = Effect.fn("validateMastraCodeStringConstraints")(
  function* (property: { readonly type: string; readonly pattern?: string | null | undefined; readonly format?: "email" | "uri" | "date" | "date-time" | null | undefined }, answer: unknown) {
    if (property.type !== "string" || typeof answer !== "string") return true;
    if (property.pattern == null && property.format == null) return true;
    const input = encodeInput({
      answer,
      pattern: property.pattern ?? undefined,
      format: property.format ?? undefined,
    });
    if (Buffer.byteLength(input) > 256 * 1024) return false;
    const validate = Effect.gen(function* () {
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
      // The deadline covers execution, not time waiting behind another answer.
      validationPermit.withPermits(1),
    );
    return yield* validate;
  },
);
