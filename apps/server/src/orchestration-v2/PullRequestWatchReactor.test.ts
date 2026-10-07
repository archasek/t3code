import {
  ProjectId,
  ThreadId,
  type OrchestrationV2ServerCommand,
  type PullRequestComment,
  type PullRequestDetail,
  type ThreadPullRequestLink,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as PullRequestWatchReactor from "./PullRequestWatchReactor.ts";

const STARTED = "2026-10-02T12:00:00.000Z";
const COMMENTED = "2026-10-02T12:01:00.000Z";

it.effect("refetches a replaced tail with unchanged count and wakes once for its new ID", () =>
  Effect.gen(function* () {
    const link: ThreadPullRequestLink = {
      host: "github.com",
      repository: "owner/repository",
      number: 1,
      url: "https://github.com/owner/repository/pull/1",
      source: "manual",
      linkedAt: STARTED,
      snapshot: null,
      stack: null,
      watch: {
        startedAt: STARTED,
        headSha: "head",
        failedChecks: [],
        passed: false,
        passedChecks: [],
        remarksThrough: COMMENTED,
        remarkIds: ["first", "old-tail"],
        conflicting: false,
        wakes: 0,
      },
    };
    let thread: ProjectionStore.ProjectionThreadPullRequests = {
      id: ThreadId.make("watch-thread"),
      projectId: ProjectId.make("project"),
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: ThreadId.make("watch-thread"),
      },
      settledAt: null,
      settledOverride: null,
      pullRequests: [link],
    };
    const comment = (id: string): PullRequestComment => ({
      id,
      kind: "review-comment",
      author: { login: "reviewer", name: null, avatarUrl: null },
      body: `Review ${id}`,
      createdAt: COMMENTED,
      url: `${link.url}#${id}`,
      path: "src/index.ts",
      reviewState: null,
    });
    const first = comment("first");
    let tailId = "old-tail";
    let tailReads = 0;
    const commands: Array<
      Extract<OrchestrationV2ServerCommand, { type: "thread.pull-request-watch.sync" }>
    > = [];
    const detail: PullRequestDetail = {
      provider: "github",
      projectId: thread.projectId,
      projectTitle: "Project",
      workspaceRoot: "/workspace/project",
      repository: link.repository,
      number: 1,
      title: "Pull request",
      body: "",
      url: link.url,
      author: { login: "agent", name: null, avatarUrl: null },
      viewer: "agent",
      state: "open",
      isDraft: false,
      mergeability: "mergeable",
      additions: 1,
      deletions: 0,
      changedFiles: 1,
      headBranch: "feature",
      headSha: "head",
      baseBranch: "main",
      createdAt: STARTED,
      updatedAt: STARTED,
      mergedAt: null,
      closedAt: null,
      reviewers: [],
      labels: [],
      checks: [{ name: "test", status: "pending", description: null, url: null }],
      mergeCapabilities: { merge: true, squash: true, rebase: true },
      capabilities: {
        diff: true,
        comment: true,
        actions: [],
        mergeMethods: [],
        search: true,
        review: { inlineComment: true, reply: true, resolve: true, verdicts: [] },
        reviewers: { request: true, listCandidates: true },
      },
      viewerPermissions: {
        actions: [],
        comment: true,
        resolve: true,
        verdicts: [],
        requestReviewers: true,
      },
    };
    const dependencies = Layer.mergeAll(
      Layer.mock(PullRequestService.PullRequestService)({
        watchFingerprint: () => Effect.succeed(null),
        detail: () => Effect.succeed(detail),
        activity: () =>
          Effect.succeed({
            comments: [first],
            commentCount: 2,
            commentsTruncated: true,
            reviewThreadsTruncated: false,
            commits: [],
            reviewThreads: [
              {
                id: "review-thread",
                path: "src/index.ts",
                line: 1,
                side: "right",
                isResolved: false,
                isOutdated: false,
                comments: [first],
                commentCount: 2,
                nextCommentsCursor: "tail",
              },
            ],
          }),
        threadComments: (input) =>
          Effect.sync(() => {
            assert.strictEqual(input.cursor, "tail");
            tailReads++;
            return { comments: [comment(tailId)], nextCursor: null };
          }),
      }),
      Layer.mock(ProjectionStore.ProjectionStoreV2)({
        getThreadsWithPullRequests: () => Effect.sync(() => [thread]),
      }),
      Layer.mock(Orchestrator.OrchestratorV2)({
        dispatch: (command) =>
          Effect.sync(() => {
            assert.strictEqual(command.type, "thread.pull-request-watch.sync");
            if (command.type !== "thread.pull-request-watch.sync")
              throw new Error("Unexpected command");
            commands.push(command);
            thread = {
              ...thread,
              pullRequests: [{ ...link, ...(command.watch ? { watch: command.watch } : {}) }],
            };
            return { sequence: commands.length, storedEvents: [] };
          }),
      }),
      Layer.succeed(
        Crypto.Crypto,
        Crypto.make({
          randomBytes: (size) => new Uint8Array(size).fill(1),
          digest: (_algorithm, data) => Effect.succeed(data),
        }),
      ),
    );
    yield* Effect.gen(function* () {
      const reactor = yield* PullRequestWatchReactor.PullRequestWatchReactor;
      yield* reactor.sweep;
      assert.strictEqual(tailReads, 1);
      assert.deepStrictEqual(commands, []);
      tailId = "new-tail";
      yield* reactor.sweep;
      assert.strictEqual(tailReads, 2);
      assert.strictEqual(commands.length, 1);
      assert.include(commands[0]?.wake?.text, "Review new-tail");
      assert.strictEqual(commands[0]?.watch?.wakes, 1);
      assert.strictEqual(commands[0]?.watch?.remarkIds.includes("new-tail"), true);
      yield* reactor.sweep;
      assert.strictEqual(tailReads, 3);
      assert.strictEqual(commands.length, 1);
    }).pipe(Effect.provide(PullRequestWatchReactor.layer.pipe(Layer.provide(dependencies))));
  }).pipe(Effect.scoped),
);
