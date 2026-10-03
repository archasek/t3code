// Import-safe implementation shared by the standalone entry and hidden SEA command.
import { fullFormats } from "ajv-formats/dist/formats.js";
import * as Schema from "effect/Schema";

export const MastraElicitationValidationInput = Schema.Struct({
  answer: Schema.String,
  pattern: Schema.optional(Schema.String),
  format: Schema.optional(Schema.Literals(["email", "uri", "date", "date-time"])),
});

export async function runMastraElicitationWorker() {
  let valid = false;
  try {
    const chunks: Buffer[] = [];
    let size = 0;
    for await (const chunk of process.stdin) {
      const bytes = Buffer.from(chunk);
      size += bytes.length;
      if (size > 256 * 1024) throw new Error("Validation input exceeds limit.");
      chunks.push(bytes);
    }
    const input = Schema.decodeUnknownSync(Schema.fromJsonString(MastraElicitationValidationInput))(
      Buffer.concat(chunks).toString("utf8"),
    );
    valid = input.pattern === undefined || new RegExp(input.pattern).test(input.answer);
    if (valid && input.format !== undefined) {
      const definition = fullFormats[input.format];
      const validator =
        typeof definition === "object" && !(definition instanceof RegExp)
          ? definition.validate
          : definition;
      valid =
        validator instanceof RegExp
          ? validator.test(input.answer)
          : // These four schema-allowlisted formats all have string validators.
            typeof validator === "function" &&
            (validator as (value: string) => boolean)(input.answer) === true;
    }
  } catch {
    valid = false;
  }
  process.stdout.write(valid ? "true" : "false");
}
