import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

/** Only a direct markdown child of a trusted plan root may be surfaced to T3. */
export const readMastraCodePlan = Effect.fn("readMastraCodePlan")(function* (input: {
  readonly rawPath: string;
  readonly cwd: string;
  readonly appDataDirectory: string;
  readonly plansDirectory: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const real = (target: string) => fs.realPath(target).pipe(Effect.orElseSucceed(() => undefined));
  const inside = (root: string, target: string) => {
    const relative = path.relative(root, target);
    return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
  };
  const realCwd = yield* real(input.cwd);
  if (!realCwd) return undefined;
  const actual = yield* real(path.resolve(input.cwd, input.rawPath));
  if (!actual || !actual.toLowerCase().endsWith(".md")) return undefined;
  const roots = [
    { root: path.join(input.cwd, ".mastracode", "plans"), trust: realCwd },
    { root: path.join(input.cwd, ".artifacts", "plans"), trust: realCwd },
    { root: input.plansDirectory, trust: yield* real(input.appDataDirectory) },
  ];
  let allowed = false;
  for (const candidate of roots) {
    const root = yield* real(candidate.root);
    if (!root || !candidate.trust || root === candidate.trust || !inside(candidate.trust, root) || !inside(root, actual)) continue;
    const relative = path.relative(root, actual);
    if (relative !== "" && path.basename(relative) === relative) { allowed = true; break; }
  }
  if (!allowed) return undefined;
  const stat = yield* fs.stat(actual).pipe(Effect.orElseSucceed(() => undefined));
  if (!stat || stat.type !== "File" || stat.size > 256 * 1024) return undefined;
  const markdown = yield* fs.readFileString(actual).pipe(Effect.orElseSucceed(() => undefined));
  return markdown !== undefined && Buffer.byteLength(markdown) <= 256 * 1024 ? markdown : undefined;
});
