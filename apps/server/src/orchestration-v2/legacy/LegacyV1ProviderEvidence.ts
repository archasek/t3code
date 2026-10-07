import {
  DEFAULT_PROVIDER_INTERACTION_MODE, DEFAULT_RUNTIME_MODE, IsoDateTime,
  MessageId, ModelSelection, ProjectId, ProviderInteractionMode, RuntimeMode,
  ThreadId, TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

// Migration-only provenance views of the V1 payloads removed with orchestrator
// V2. Keep all required identity fields; unrelated optional presentation fields
// are not reconstructed or used as evidence.
const modes = {
  runtimeMode: RuntimeMode.pipe(Schema.withDecodingDefault(Effect.succeed(DEFAULT_RUNTIME_MODE))),
  interactionMode: ProviderInteractionMode.pipe(
    Schema.withDecodingDefault(Effect.succeed(DEFAULT_PROVIDER_INTERACTION_MODE)),
  ),
};
const created = Schema.Struct({
  threadId: ThreadId, projectId: ProjectId, title: TrimmedNonEmptyString,
  modelSelection: ModelSelection, ...modes,
  branch: Schema.NullOr(TrimmedNonEmptyString),
  worktreePath: Schema.NullOr(TrimmedNonEmptyString),
  createdAt: IsoDateTime, updatedAt: IsoDateTime,
});
const metadata = Schema.Struct({
  threadId: ThreadId, modelSelection: ModelSelection, updatedAt: IsoDateTime,
});
const turn = Schema.Struct({
  threadId: ThreadId, messageId: MessageId, modelSelection: ModelSelection,
  titleSeed: Schema.optional(TrimmedNonEmptyString), ...modes, createdAt: IsoDateTime,
});
const decoders = {
  "thread.created": Schema.decodeUnknownOption(Schema.fromJsonString(created)),
  "thread.meta-updated": Schema.decodeUnknownOption(Schema.fromJsonString(metadata)),
  "thread.turn-start-requested": Schema.decodeUnknownOption(Schema.fromJsonString(turn)),
};

export function legacyV1ProviderEvidence(type: string, payload: string, threadId: ThreadId) {
  const decoded: Option.Option<{
    readonly threadId: ThreadId;
    readonly modelSelection: typeof ModelSelection.Type;
  }> = type === "thread.created" ? decoders["thread.created"](payload)
    : type === "thread.meta-updated" ? decoders["thread.meta-updated"](payload)
    : type === "thread.turn-start-requested" ? decoders["thread.turn-start-requested"](payload)
    : Option.none();
  return Option.isSome(decoded) && decoded.value.threadId === threadId
    ? Option.some(decoded.value.modelSelection.instanceId) : Option.none();
}
