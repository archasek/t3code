import { assert, it } from "@effect/vitest";
import { EventId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Tracer from "effect/Tracer";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import { listLinkedPullRequestThreads } from "../../pullRequest/linkedThreads.ts";
import * as EventSink from "../EventSink.ts";
import * as EventStore from "../EventStore.ts";
import * as LegacyV1ThreadImporter from "./LegacyV1ThreadImporter.ts";
import * as ProjectionMaintenance from "../ProjectionMaintenance.ts";
import * as ProjectionStore from "../ProjectionStore.ts";

const encodeFixtureJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeFixtureJson = Schema.decodeSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const layerDatabase = SqlitePersistence.layerMemory;
const layerEventStoreProvided = EventStore.layer.pipe(Layer.provideMerge(layerDatabase));
const layerProjectionStoreProvided = ProjectionStore.layer.pipe(Layer.provideMerge(layerDatabase));
const layerStoresProvided = Layer.mergeAll(
  layerDatabase,
  layerEventStoreProvided,
  layerProjectionStoreProvided,
);
const layerEventSinkProvided = EventSink.layer.pipe(Layer.provide(layerStoresProvided));
const layerImporterProvided = LegacyV1ThreadImporter.layer.pipe(
  Layer.provide(Layer.mergeAll(layerStoresProvided, layerEventSinkProvided)),
);
const layerProjectionMaintenanceProvided = ProjectionMaintenance.layer.pipe(
  Layer.provide(layerStoresProvided),
);
const layerTest = Layer.mergeAll(
  layerStoresProvided,
  layerEventSinkProvided,
  layerImporterProvided,
  layerProjectionMaintenanceProvided,
);

for (const alreadyImported of [false, true]) {
  it.layer(layerTest)(`Missing MC runtime after import=${alreadyImported}`, (it) => {
    it.effect("rejects loss of native identity without changing durable history", () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        const timestamp = "2026-01-01T00:00:00.000Z";
        const threadId = ThreadId.make("missing-mc-runtime");
        yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, default_model_selection_json, scripts_json, created_at, updated_at)
        VALUES ('missing-mc-project', 'MC', '/workspace', '{"instanceId":"custom-owner","model":"default"}', '[]', ${timestamp}, ${timestamp})`;
        yield* sql`INSERT INTO projection_threads (thread_id, project_id, title, model_selection_json, runtime_mode, interaction_mode, created_at, updated_at)
        VALUES (${threadId}, 'missing-mc-project', 'Existing conversation', '{"instanceId":"custom-owner","model":"default"}', 'approval-required', 'default', ${timestamp}, ${timestamp})`;
        if (alreadyImported) yield* importer.reconcileShells;
        // The native provider identity comes from its persisted session, not
        // the custom instance ID or the currently selected model.
        yield* sql`INSERT INTO projection_thread_sessions (thread_id, status, provider_name, updated_at)
        VALUES (${threadId}, 'stopped', 'mastraCode', ${timestamp})`;
        const before = yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`;
        const error = yield* importer.reconcileShells.pipe(Effect.flip);
        assert.equal(error._tag, "LegacyV1ThreadImportError");
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`,
          before,
        );
        if (alreadyImported) {
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const sink = yield* EventSink.EventSinkV2;
          const projection = yield* projections.getThreadProjection(threadId);
          yield* sink.write({
            events: [
              {
                id: EventId.make("delete-missing-mc-v2-thread"),
                type: "thread.deleted",
                threadId,
                providerInstanceId: projection.thread.providerInstanceId,
                occurredAt: projection.thread.updatedAt,
                payload: { ...projection.thread, deletedAt: projection.thread.updatedAt },
              },
            ],
          });
          yield* importer.reconcileShells;
          assert.isNotNull((yield* projections.getThreadProjection(threadId)).thread.deletedAt);
        }
      }),
    );
  });
}

for (const scenario of [
  "valid",
  "previously-imported",
  "provider-changed",
  "pre-import-provider-changed",
  "later-selected-provider",
  "malformed-newest",
  "unrelated-payload",
  "malformed-history",
  "newer-binding",
  "wrong-owner",
  "invalid-cursor",
] as const) {
  it.layer(layerTest)(`Legacy MC binding ${scenario}`, (it) => {
    it.effect(`preserves MC native bindings or fails closed: ${scenario}`, () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const threadId = ThreadId.make(`legacy-mc-${scenario}`);
        const timestamp = "2026-01-01T00:00:00.000Z";
        yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, default_model_selection_json, scripts_json, created_at, updated_at)
        VALUES ('legacy-mc-project', 'MC', '/workspace', '{"instanceId":"mc-owner","model":"default"}', '[]', ${timestamp}, ${timestamp})`;
        yield* sql`INSERT INTO projection_threads (thread_id, project_id, title, model_selection_json, runtime_mode, interaction_mode, created_at, updated_at)
        VALUES (${threadId}, 'legacy-mc-project', 'MC conversation', '{"instanceId":"mc-owner","model":"default"}', 'approval-required', 'default', ${timestamp}, ${timestamp})`;
        const historical = [
          "pre-import-provider-changed",
          "later-selected-provider",
          "malformed-newest",
          "unrelated-payload",
          "malformed-history",
        ].includes(scenario);
        if (historical) {
          const createdPayload = {
            threadId: scenario === "unrelated-payload" ? "another-thread" : threadId,
            projectId: "legacy-mc-project",
            title: "MC conversation",
            modelSelection: {
              instanceId: scenario === "later-selected-provider" ? "codex" : "mc-owner",
              model: "default",
            },
            runtimeMode: "approval-required",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdAt: timestamp,
            updatedAt: timestamp,
          };
          yield* sql`INSERT INTO orchestration_events
          (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json)
          VALUES ('legacy-mc-created-before-switch', 'thread', ${threadId}, 1, 'thread.created', ${timestamp}, 'system', ${encodeFixtureJson(scenario === "malformed-history" ? { modelSelection: createdPayload.modelSelection } : createdPayload)}, '{}')`;
          if (scenario === "later-selected-provider") {
            yield* sql`INSERT INTO orchestration_events
            (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json)
            VALUES ('legacy-mc-selected-after-creation', 'thread', ${threadId}, 2, 'thread.meta-updated', ${timestamp}, 'system', ${encodeFixtureJson({ threadId, updatedAt: timestamp, modelSelection: { instanceId: "mc-owner", model: "default" } })}, '{}')`;
          }
          if (scenario === "malformed-newest") {
            yield* sql`INSERT INTO orchestration_events
            (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json)
            VALUES ('malformed-newest-evidence', 'thread', ${threadId}, 2, 'thread.meta-updated', ${timestamp}, 'system', '{"modelSelection":{"instanceId":"mc-owner","model":"default"}}', '{}')`;
          }
          yield* sql`UPDATE projection_threads SET model_selection_json = '{"instanceId":"codex","model":"default"}' WHERE thread_id = ${threadId}`;
        }
        if (scenario === "previously-imported" || scenario === "provider-changed") {
          yield* importer.reconcileShells;
          assert.lengthOf((yield* projections.getThreadProjection(threadId)).providerThreads, 0);
        }
        if (scenario === "provider-changed") {
          const eventSink = yield* EventSink.EventSinkV2;
          const imported = yield* projections.getThreadProjection(threadId);
          yield* eventSink.write({
            events: [
              {
                id: EventId.make("switch-imported-mc-to-codex"),
                type: "thread.metadata-updated",
                threadId,
                providerInstanceId: ProviderInstanceId.make("codex"),
                occurredAt: imported.thread.updatedAt,
                payload: {
                  ...imported.thread,
                  providerInstanceId: ProviderInstanceId.make("codex"),
                },
              },
            ],
          });
        }
        yield* sql`INSERT INTO provider_session_runtime (thread_id, provider_name, provider_instance_id, adapter_key, runtime_mode, status, last_seen_at, resume_cursor_json)
        VALUES (${threadId}, 'mastraCode', ${scenario === "wrong-owner" ? "another-owner" : "mc-owner"}, 'mastraCode', 'approval-required', 'ready', ${timestamp}, ${scenario === "invalid-cursor" ? '{"schemaVersion":2,"sessionId":"native-mc-session"}' : '{"schemaVersion":1,"sessionId":"native-mc-session"}'})`;
        if (scenario === "provider-changed") {
          const before = yield* projections.getThreadProjection(threadId);
          yield* importer.reconcileShells;
          const after = yield* projections.getThreadProjection(threadId);
          assert.equal(after.thread.providerInstanceId, "codex");
          assert.equal(after.thread.activeProviderThreadId, null);
          assert.lengthOf(after.providerThreads, 0);
          assert.deepStrictEqual(after, before);
          return;
        }
        if (
          scenario === "wrong-owner" ||
          scenario === "invalid-cursor" ||
          scenario === "unrelated-payload" ||
          scenario === "malformed-history"
        ) {
          const error = yield* importer.reconcileShells.pipe(Effect.flip);
          assert.equal(error._tag, "LegacyV1ThreadImportError");
          const events = yield* sql<{
            count: number;
          }>`SELECT COUNT(*) AS count FROM orchestration_events WHERE application_event_version = 2 AND stream_id = ${threadId}`;
          assert.equal(events[0]?.count, 0);
          return;
        }
        yield* importer.reconcileShells;
        const first = yield* projections.getThreadProjection(threadId);
        assert.lengthOf(first.providerThreads, 1);
        assert.equal(first.providerThreads[0]?.nativeThreadRef?.nativeId, "native-mc-session");
        assert.equal(first.providerThreads[0]?.providerInstanceId, "mc-owner");
        assert.equal(first.providerThreads[0]?.appThreadId, threadId);
        assert.equal(
          first.thread.activeProviderThreadId,
          historical ? null : first.providerThreads[0]?.id,
        );
        if (historical) {
          assert.equal(first.thread.providerInstanceId, "codex");
        }
        assert.lengthOf(first.runtimeRequests, 0);
        if (scenario === "valid") {
          yield* sql`INSERT INTO projection_thread_sessions (thread_id, status, provider_name, provider_instance_id, updated_at)
          VALUES (${threadId}, 'stopped', 'mastraCode', 'mc-owner', ${timestamp})`;
          yield* sql`DELETE FROM provider_session_runtime WHERE thread_id = ${threadId}`;
          // A native identity already preserved by V2 is sufficient. The legacy
          // runtime row is no longer needed and must not block later restarts.
        }
        if (scenario === "newer-binding") {
          const eventSink = yield* EventSink.EventSinkV2;
          const native = first.providerThreads[0];
          if (native === undefined || native.nativeThreadRef === null) {
            return yield* Effect.die(new Error("Expected an imported native MC binding"));
          }
          yield* eventSink.write({
            events: [
              {
                id: EventId.make("newer-mc-native-binding"),
                type: "provider-thread.updated",
                threadId,
                providerInstanceId: first.thread.providerInstanceId,
                occurredAt: first.thread.updatedAt,
                payload: {
                  ...native,
                  nativeThreadRef: {
                    ...native.nativeThreadRef,
                    nativeId: "newer-native-mc-session",
                  },
                },
              },
              {
                id: EventId.make("clear-active-pointer-with-existing-mc-binding"),
                type: "thread.metadata-updated",
                threadId,
                providerInstanceId: first.thread.providerInstanceId,
                occurredAt: first.thread.updatedAt,
                payload: { ...first.thread, activeProviderThreadId: null },
              },
            ],
          });
          const beforeRepair = yield* projections.getThreadProjection(threadId);
          assert.equal(beforeRepair.thread.activeProviderThreadId, null);
          assert.equal(
            beforeRepair.providerThreads[0]?.nativeThreadRef?.nativeId,
            "newer-native-mc-session",
          );
          yield* importer.reconcileShells;
          assert.deepStrictEqual(yield* projections.getThreadProjection(threadId), beforeRepair);
          return;
        }
        yield* importer.reconcileShells;
        const again = yield* projections.getThreadProjection(threadId);
        assert.deepStrictEqual(again.providerThreads, first.providerThreads);
        if (scenario === "valid") {
          for (const [field, value] of [
            ["ownerNodeId", "delegated-node"],
            ["providerInstanceId", "unrelated-owner"],
            ["nativeThreadRef", { driver: "mastraCode", nativeId: null, strength: "strong" }],
            ["nativeThreadRef", { driver: "mastraCode", nativeId: "   ", strength: "strong" }],
          ] as const) {
            const native = first.providerThreads[0];
            if (native === undefined) return yield* Effect.die(new Error("Missing native fixture"));
            const stored = yield* sql<{
              payload_json: string;
            }>`SELECT payload_json FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id = ${native.id}`;
            const original = stored[0]?.payload_json;
            if (original === undefined)
              return yield* Effect.die(new Error("Missing persisted fixture"));
            yield* sql`UPDATE orchestration_v2_projection_provider_threads SET payload_json = ${encodeFixtureJson({ ...decodeFixtureJson(original), [field]: value })} WHERE provider_thread_id = ${native.id}`;
            assert.equal(
              (yield* importer.reconcileShells.pipe(Effect.flip))._tag,
              "LegacyV1ThreadImportError",
            );
            yield* sql`UPDATE orchestration_v2_projection_provider_threads SET payload_json = ${original} WHERE provider_thread_id = ${native.id}`;
          }
        }
      }),
    );
  });
}

it.layer(layerTest)("LegacyV1ThreadImporter", (it) => {
  it.effect("uses the created-thread index for startup migration checks", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const statements: string[] = [];
      const tracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          const end = span.end.bind(span);
          span.end = (endTime, exit) => {
            end(endTime, exit);
            const query = span.attributes.get("db.query.text");
            if (
              typeof query === "string" &&
              query.includes("FROM projection_threads AS thread") &&
              query.includes("FROM orchestration_events AS event")
            ) {
              statements.push(query);
            }
          };
          return span;
        },
      });

      assert.equal(yield* importer.pendingThreadCount.pipe(Effect.withTracer(tracer)), 0);
      assert.deepStrictEqual(yield* importer.reconcileShells.pipe(Effect.withTracer(tracer)), {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });
      assert.lengthOf(statements, 2);
      for (const statement of statements) {
        const plan = yield* sql.unsafe<{ readonly detail: string }>(
          `EXPLAIN QUERY PLAN ${statement}`,
        );
        assert.match(
          plan.map((row) => row.detail).join("\n"),
          /SEARCH event USING INDEX orchestration_events_v2_created_threads_idx \(stream_id=\?\)/,
        );
      }
    }),
  );

  it.effect("imports lightweight shells, hydrates transcripts, and remains idempotent", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("thread:legacy-import");

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        ) VALUES (
          'project:legacy-import',
          'Legacy project',
          '/tmp/legacy-project',
          '{"instanceId":"codex","model":"gpt-5.4"}',
          '[]',
          '2026-01-01T00:00:00.000Z',
          '2026-01-04T00:00:00.000Z',
          NULL
        )
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          created_at,
          updated_at,
          archived_at,
          settled_override,
          settled_at,
          unsettled_at,
          snoozed_until,
          snoozed_at,
          pinned_at,
          pin_order_key,
          linked_pull_request_json,
          deleted_at
        ) VALUES (
          ${threadId},
          'project:legacy-import',
          'Migrated conversation',
          '{"instanceId":"codex","model":"gpt-5.4"}',
          'full-access',
          'default',
          ' main ',
          ' /tmp/legacy-project ',
          NULL,
          '2026-01-01T00:00:00.000Z',
          '2026-01-04T00:00:00.000Z',
          NULL,
          NULL,
          NULL,
          '2026-01-03T12:00:00.000Z',
          '2026-02-01T00:00:00.000Z',
          '2026-01-04T00:00:00.000Z',
          '2026-01-02T00:00:00.000Z',
          'm',
          '{"projectId":"project:legacy-import","repository":"pingdotgg/t3code","number":9000,"url":"https://github.com/pingdotgg/t3code/pull/9000"}',
          NULL
        )
      `;
      yield* sql`
        INSERT INTO projection_thread_messages (
          message_id,
          thread_id,
          turn_id,
          role,
          text,
          attachments_json,
          is_streaming,
          created_at,
          updated_at
        ) VALUES
          (
            'message:legacy:1',
            ${threadId},
            NULL,
            'user',
            'First question',
            '[]',
            0,
            '2026-01-01T01:00:00.000Z',
            '2026-01-01T01:00:00.000Z'
          ),
          (
            'message:legacy:2',
            ${threadId},
            NULL,
            'assistant',
            'First answer',
            '[]',
            0,
            '2026-01-02T01:00:00.000Z',
            '2026-01-02T01:00:00.000Z'
          ),
          (
            'message:legacy:3',
            ${threadId},
            NULL,
            'user',
            'Follow-up question',
            '[]',
            0,
            '2026-01-03T01:00:00.000Z',
            '2026-01-03T01:00:00.000Z'
          ),
          (
            'message:legacy:4',
            ${threadId},
            NULL,
            'assistant',
            'Partial answer',
            '[]',
            1,
            '2026-01-04T01:00:00.000Z',
            '2026-01-04T01:00:00.000Z'
        )
      `;

      yield* sql`
        INSERT INTO projection_thread_pull_requests (
          thread_id, host, repository, number, url, source, linked_at, snapshot_json
        ) VALUES
          (${threadId}, 'github.com', 'pingdotgg/t3code', 9002,
            'https://github.com/pingdotgg/t3code/pull/9002', 'created',
            '2026-01-03T00:00:00.000Z',
            '{"state":"open","title":"Second PR","headBranch":"feature-two","baseBranch":"main","isDraft":false,"updatedAt":"2026-01-03T00:00:00.000Z","syncedAt":"2026-01-03T00:00:00.000Z"}'),
          (${threadId}, 'github.com', 'pingdotgg/t3code', 9003,
            'https://github.com/pingdotgg/t3code/pull/9003', 'manual',
            '2026-01-04T00:00:00.000Z', NULL)
      `;

      assert.equal(yield* importer.pendingThreadCount, 1);
      const shellImport = yield* importer.reconcileShells;
      assert.equal(yield* importer.pendingThreadCount, 1);
      assert.deepStrictEqual(shellImport, {
        importedThreadCount: 1,
        importedMessageCount: 2,
      });
      const shellEventCount = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count
        FROM orchestration_events
        WHERE application_event_version = 2
          AND aggregate_kind = 'thread'
          AND stream_id = ${threadId}
      `;
      assert.equal(shellEventCount[0]?.count, 6);

      assert.isTrue((yield* maintenance.verify).valid);
      const shellProjection = yield* projections.getThreadProjection(threadId);
      assert.equal(shellProjection.thread.historyOrigin, "v1_import");
      assert.equal(shellProjection.thread.branch, "main");
      assert.equal(shellProjection.thread.worktreePath, "/tmp/legacy-project");
      assert.deepEqual(
        shellProjection.thread.pinnedAt,
        DateTime.makeUnsafe("2026-01-02T00:00:00.000Z"),
      );
      assert.equal(shellProjection.thread.pinOrderKey, "m");
      assert.deepEqual(
        shellProjection.thread.snoozedUntil,
        DateTime.makeUnsafe("2026-02-01T00:00:00.000Z"),
      );
      assert.deepEqual(
        shellProjection.thread.unsettledAt,
        DateTime.makeUnsafe("2026-01-03T12:00:00.000Z"),
      );
      assert.equal(shellProjection.thread.linkedPullRequest?.number, 9000);
      assert.deepStrictEqual(
        (shellProjection.thread.pullRequests ?? []).map((link) => link.number),
        [9002, 9003, 9000],
      );
      assert.equal(shellProjection.thread.pullRequests?.[0]?.snapshot?.title, "Second PR");
      assert.deepStrictEqual(
        (yield* listLinkedPullRequestThreads({
          host: "github.com",
          repository: "pingdotgg/t3code",
          number: 9002,
        })).threads.map((thread) => thread.id),
        [threadId],
      );
      const shellSnapshot = yield* projections.getShellSnapshot();
      assert.equal(
        shellSnapshot.threads.find((thread) => thread.id === threadId)?.historyOrigin,
        "v1_import",
      );
      assert.deepStrictEqual(
        shellProjection.messages.map((message) => message.id),
        ["message:legacy:3", "message:legacy:4"],
      );

      const renamedAt = DateTime.makeUnsafe("2026-01-05T00:00:00.000Z");
      yield* eventSink.write({
        events: [
          {
            id: EventId.make("event:legacy-import:metadata-after-shell"),
            type: "thread.metadata-updated",
            threadId,
            providerInstanceId: shellProjection.thread.providerInstanceId,
            occurredAt: renamedAt,
            payload: {
              ...shellProjection.thread,
              title: "Renamed after shell import",
              runtimeMode: "approval-required",
              interactionMode: "plan",
              archivedAt: renamedAt,
              settledOverride: "settled",
              settledAt: renamedAt,
              updatedAt: renamedAt,
            },
          },
        ],
      });

      const transcriptImport = yield* importer.ensureTranscript(threadId);
      assert.equal(yield* importer.pendingThreadCount, 0);
      assert.deepStrictEqual(transcriptImport, {
        importedThreadCount: 1,
        importedMessageCount: 2,
      });
      const projection = yield* projections.getThreadProjection(threadId);
      assert.equal(projection.thread.title, "Renamed after shell import");
      assert.equal(projection.thread.runtimeMode, "approval-required");
      assert.equal(projection.thread.interactionMode, "plan");
      assert.deepEqual(projection.thread.archivedAt, renamedAt);
      assert.equal(projection.thread.settledOverride, "settled");
      assert.deepEqual(projection.thread.settledAt, renamedAt);
      assert.deepStrictEqual(
        projection.messages.map((message) => message.id),
        ["message:legacy:1", "message:legacy:2", "message:legacy:3", "message:legacy:4"],
      );
      assert.deepStrictEqual(
        projection.turnItems
          .filter(
            (
              item,
            ): item is Extract<
              (typeof projection.turnItems)[number],
              { readonly type: "user_message" | "assistant_message" }
            > => item.type === "user_message" || item.type === "assistant_message",
          )
          .map((item) => [item.messageId, item.ordinal, item.status]),
        [
          ["message:legacy:1", 1, "completed"],
          ["message:legacy:2", 2, "completed"],
          ["message:legacy:3", 3, "completed"],
          ["message:legacy:4", 4, "interrupted"],
        ],
      );

      yield* eventSink.write({
        events: [
          {
            id: EventId.make("event:legacy-import:explicit-unpin"),
            type: "thread.unpinned",
            threadId,
            providerInstanceId: projection.thread.providerInstanceId,
            occurredAt: renamedAt,
            payload: { ...projection.thread, pinnedAt: null, pinOrderKey: null },
          },
        ],
      });
      yield* sql`
        UPDATE orchestration_v2_projection_threads
        SET payload_json = json_remove(
          payload_json,
          '$.snoozedUntil',
          '$.snoozedAt',
          '$.unsettledAt',
          '$.linkedPullRequest',
          '$.pullRequests'
        )
        WHERE thread_id = ${threadId}
      `;
      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 1,
        importedMessageCount: 0,
      });
      const repaired = yield* projections.getThreadProjection(threadId);
      assert.isNull(repaired.thread.pinnedAt);
      assert.isNull(repaired.thread.pinOrderKey);
      assert.deepEqual(
        repaired.thread.snoozedUntil,
        DateTime.makeUnsafe("2026-02-01T00:00:00.000Z"),
      );
      assert.deepEqual(
        repaired.thread.unsettledAt,
        DateTime.makeUnsafe("2026-01-03T12:00:00.000Z"),
      );
      assert.equal(repaired.thread.linkedPullRequest?.number, 9000);
      assert.deepStrictEqual(
        (repaired.thread.pullRequests ?? []).map((link) => link.number),
        [9002, 9003, 9000],
      );
      assert.equal(repaired.thread.pullRequests?.[0]?.snapshot?.title, "Second PR");
      const eventCountBeforeRetry = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count
        FROM orchestration_events
        WHERE application_event_version = 2
          AND aggregate_kind = 'thread'
          AND stream_id = ${threadId}
      `;
      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });
      assert.deepStrictEqual(yield* importer.ensureTranscript(threadId), {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });
      const eventCountAfterRetry = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count
        FROM orchestration_events
        WHERE application_event_version = 2
          AND aggregate_kind = 'thread'
          AND stream_id = ${threadId}
      `;
      assert.equal(eventCountAfterRetry[0]?.count, eventCountBeforeRetry[0]?.count);
    }),
  );

  it.effect("repairs newly added metadata after an earlier metadata repair", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("thread:legacy-metadata-upgrade");
      const previousRepairId = EventId.make(`migration:v1:thread:${threadId}:metadata-repair`);

      yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, scripts_json, created_at, updated_at
        ) VALUES (
          'project:legacy-metadata-upgrade',
          'Legacy project',
          '/tmp/legacy-metadata-upgrade',
          '[]',
          '2026-01-01T00:00:00.000Z',
          '2026-01-01T00:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id, project_id, title, model_selection_json, runtime_mode,
          interaction_mode, created_at, updated_at, pinned_at, pin_order_key,
          linked_pull_request_json, branch_pull_request_json, active_order_key
        ) VALUES (
          ${threadId},
          'project:legacy-metadata-upgrade',
          'Original v1 title',
          '{"instanceId":"codex","model":"gpt-5.4"}',
          'full-access',
          'default',
          '2026-01-01T00:00:00.000Z',
          '2026-01-01T00:00:00.000Z',
          '2026-01-02T00:00:00.000Z',
          'm',
          '{"projectId":"project:legacy-metadata-upgrade","repository":"pingdotgg/t3code","number":9000,"url":"https://github.com/pingdotgg/t3code/pull/9000"}',
          '{"projectId":"project:legacy-metadata-upgrade","repository":"pingdotgg/t3code","number":9001,"url":"https://github.com/pingdotgg/t3code/pull/9001"}',
          'az'
        )
      `;
      yield* importer.reconcileShells;
      yield* maintenance.rebuild;
      const shellProjection = yield* projections.getThreadProjection(threadId);
      assert.deepStrictEqual(
        (shellProjection.thread.pullRequests ?? []).map((link) => link.number),
        [9000],
      );
      const previousRepairThread = {
        ...shellProjection.thread,
        title: "Renamed in v2",
        pinnedAt: null,
        pinOrderKey: null,
        linkedPullRequest: null,
        pullRequests: [],
      };
      delete previousRepairThread.branchPullRequest;
      delete previousRepairThread.activeOrderKey;
      yield* eventSink.write({
        events: [
          {
            id: previousRepairId,
            type: "thread.metadata-updated",
            threadId,
            providerInstanceId: previousRepairThread.providerInstanceId,
            occurredAt: DateTime.makeUnsafe("2026-01-03T00:00:00.000Z"),
            payload: previousRepairThread,
          },
        ],
      });

      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 1,
        importedMessageCount: 0,
      });
      const repaired = yield* projections.getThreadProjection(threadId);
      assert.equal(repaired.thread.title, "Renamed in v2");
      assert.isNull(repaired.thread.pinnedAt);
      assert.isNull(repaired.thread.pinOrderKey);
      assert.isNull(repaired.thread.linkedPullRequest);
      assert.deepStrictEqual(repaired.thread.pullRequests, []);
      assert.equal(repaired.thread.branchPullRequest?.number, 9001);
      assert.equal(repaired.thread.activeOrderKey, "az");

      const eventsBeforeRetry = yield* sql<{ readonly event_id: string }>`
        SELECT event_id
        FROM orchestration_events
        WHERE application_event_version = 2 AND stream_id = ${threadId}
        ORDER BY sequence
      `;
      assert.equal(eventsBeforeRetry.length, 4);
      assert.isTrue(eventsBeforeRetry.some((event) => event.event_id === previousRepairId));
      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });

      assert.isTrue((yield* maintenance.rebuild).valid);
      const replayed = yield* projections.getThreadProjection(threadId);
      assert.deepStrictEqual(replayed.thread, repaired.thread);
      assert.deepStrictEqual(
        yield* Effect.gen(function* () {
          const restartedImporter = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
          return yield* restartedImporter.reconcileShells;
        }).pipe(Effect.provide(LegacyV1ThreadImporter.layer)),
        { importedThreadCount: 0, importedMessageCount: 0 },
      );
      const eventsAfterRestart = yield* sql<{ readonly event_id: string }>`
        SELECT event_id
        FROM orchestration_events
        WHERE application_event_version = 2 AND stream_id = ${threadId}
        ORDER BY sequence
      `;
      assert.deepStrictEqual(eventsAfterRestart, eventsBeforeRetry);
    }),
  );
});
