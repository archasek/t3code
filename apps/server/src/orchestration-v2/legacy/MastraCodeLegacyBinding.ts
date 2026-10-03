import { ProviderDriverKind, type ProviderInstanceId, type OrchestrationV2AppThread, type OrchestrationV2ProviderThread } from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { deriveProviderThread } from "../IdAllocator.ts";

export interface MastraCodeLegacyRuntimeRow {
  readonly provider_name: string;
  readonly provider_instance_id: string | null;
  readonly adapter_key: string;
  readonly resume_cursor_json: string | null;
}
const decodeCursor = Schema.decodeUnknownOption(Schema.Struct({ schemaVersion: Schema.Literal(1), sessionId: Schema.NonEmptyString }));

export function mastraCodeLegacyBinding(thread: OrchestrationV2AppThread, row: MastraCodeLegacyRuntimeRow): OrchestrationV2ProviderThread | undefined {
  const owner: ProviderInstanceId = thread.providerInstanceId;
  if (row.provider_name !== "mastraCode" || row.adapter_key !== "mastraCode") return undefined;
  if (row.provider_instance_id === null ? owner !== "mastraCode" : row.provider_instance_id !== owner) return undefined;
  let value: unknown;
  try { value = JSON.parse(row.resume_cursor_json ?? "null"); } catch { return undefined; }
  const cursor = decodeCursor(value);
  if (Option.isNone(cursor) || cursor.value.sessionId.trim() !== cursor.value.sessionId) return undefined;
  const nativeId = cursor.value.sessionId;
  const driver = ProviderDriverKind.make("mastraCode");
  return {
    id: deriveProviderThread({ driver, providerInstanceId: owner, nativeThreadId: nativeId }),
    driver, providerInstanceId: owner, providerSessionId: null, appThreadId: thread.id, ownerNodeId: null,
    nativeThreadRef: { driver, nativeId, strength: "strong" }, nativeConversationHeadRef: null,
    status: "not_loaded", firstRunOrdinal: null, lastRunOrdinal: null, handoffIds: [], forkedFrom: null,
    pendingBackgroundTasks: [], contextUsage: null, nativeMetadata: null,
    createdAt: thread.createdAt, updatedAt: thread.updatedAt,
  };
}
