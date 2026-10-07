import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { readMastraCodePlan } from "./MastraCodePlan.ts";

describe("Mastra Code trusted plan files", () => {
  it.effect("reads private plans but rejects symlink escapes, nested and oversized files", () => Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-mc-plan-" });
    const cwd = path.join(root, "workspace");
    const appDataDirectory = path.join(root, "app-data");
    const plansDirectory = path.join(appDataDirectory, "threads", "thread", "plans");
    const outside = path.join(root, "outside");
    for (const dir of [path.join(cwd, ".mastracode"), plansDirectory, outside]) yield* fs.makeDirectory(dir, { recursive: true });
    const read = (rawPath: string) => readMastraCodePlan({ rawPath, cwd, appDataDirectory, plansDirectory });
    const plan = path.join(plansDirectory, "plan.md");
    yield* fs.writeFileString(plan, "# Native plan\n");
    expect(yield* read(plan)).toBe("# Native plan\n");
    yield* fs.writeFileString(path.join(outside, "secret.md"), "private external content");
    yield* fs.symlink(outside, path.join(cwd, ".mastracode", "plans"));
    expect(yield* read(".mastracode/plans/secret.md")).toBeUndefined();
    yield* fs.symlink(path.join(outside, "secret.md"), path.join(plansDirectory, "escape.md"));
    expect(yield* read(path.join(plansDirectory, "escape.md"))).toBeUndefined();
    yield* fs.makeDirectory(path.join(plansDirectory, "nested"));
    yield* fs.writeFileString(path.join(plansDirectory, "nested", "plan.md"), "nested");
    expect(yield* read(path.join(plansDirectory, "nested", "plan.md"))).toBeUndefined();
    yield* fs.writeFileString(plan, "x".repeat(256 * 1024 + 1));
    expect(yield* read(plan)).toBeUndefined();
  }).pipe(Effect.provide(NodeServices.layer), Effect.scoped));
});
