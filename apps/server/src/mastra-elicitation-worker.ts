// Standalone Node entry. SEA invokes the same implementation through the hidden CLI.
import { runMastraElicitationWorker } from "./mastraElicitationWorker.ts";

await runMastraElicitationWorker();
