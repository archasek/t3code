import type { OrchestrationV2UserInputQuestion } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as EffectAcpSchema from "effect-acp/schema";
import type * as EffectAcpErrors from "effect-acp/errors";
import type { AcpAdapterV2Flavor } from "../orchestration-v2/Adapters/AcpAdapterV2.ts";

function text(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

type ElicitationValue = EffectAcpSchema.ElicitationContentValue;

// MC supports titled multi-select options that the current generated ACP
// schema marks Never. Decode that native form subset without losing fields.
const enumOption = Schema.Struct({ const: Schema.String, title: Schema.optional(Schema.String) });
const common = {
  title: Schema.optional(Schema.NullOr(Schema.String)),
  description: Schema.optional(Schema.NullOr(Schema.String)),
};
const propertySchema = Schema.Union([
  Schema.Struct({
    ...common,
    type: Schema.Literal("string"),
    enum: Schema.optional(Schema.NullOr(Schema.Array(Schema.String))),
    oneOf: Schema.optional(Schema.NullOr(Schema.Array(enumOption))),
    minLength: Schema.optional(Schema.NullOr(Schema.Number)),
    maxLength: Schema.optional(Schema.NullOr(Schema.Number)),
    pattern: Schema.optional(Schema.NullOr(Schema.String)),
    format: Schema.optional(Schema.NullOr(Schema.Literals(["email", "uri", "date", "date-time"]))),
  }),
  Schema.Struct({
    ...common,
    type: Schema.Literal("number"),
    minimum: Schema.optional(Schema.NullOr(Schema.Number)),
    maximum: Schema.optional(Schema.NullOr(Schema.Number)),
  }),
  Schema.Struct({
    ...common,
    type: Schema.Literal("integer"),
    minimum: Schema.optional(Schema.NullOr(Schema.Number)),
    maximum: Schema.optional(Schema.NullOr(Schema.Number)),
  }),
  Schema.Struct({ ...common, type: Schema.Literal("boolean") }),
  Schema.Struct({
    ...common,
    type: Schema.Literal("array"),
    minItems: Schema.optional(Schema.NullOr(Schema.Number)),
    maxItems: Schema.optional(Schema.NullOr(Schema.Number)),
    items: Schema.Union([
      Schema.Struct({ enum: Schema.Array(Schema.String) }),
      Schema.Struct({ anyOf: Schema.Array(enumOption) }),
    ]),
  }),
]);
type FormProperty = typeof propertySchema.Type;
const formSchema = Schema.Struct({
  properties: Schema.optional(Schema.Record(Schema.String, propertySchema)),
  required: Schema.optional(Schema.NullOr(Schema.Array(Schema.String))),
});

function elicitationEnumOptions(property: FormProperty) {
  if (property.type === "string")
    return (
      property.enum?.map((value) => ({ value, label: value })) ??
      property.oneOf?.map((entry) => ({ value: entry.const, label: entry.title ?? entry.const }))
    );
  if (property.type === "array")
    return "enum" in property.items
      ? property.items.enum.map((value) => ({ value, label: value }))
      : property.items.anyOf.map((entry) => ({
          value: entry.const,
          label: entry.title ?? entry.const,
        }));
  return undefined;
}

function elicitationEnumValues(property: FormProperty) {
  return elicitationEnumOptions(property)?.map((option) => option.value);
}

function optionalElicitationSkipValue(property: FormProperty, requestId: string, id: string) {
  let value = `${requestId}:omit:${id}`;
  const allowed = elicitationEnumValues(property);
  while (allowed?.includes(value)) value += ":omit";
  return value;
}

const EMPTY_FORM_CONFIRMATION_ID = "empty-form-confirmation";

function coerceMastraCodeElicitationAnswer(
  property: FormProperty,
  answer: unknown,
): ElicitationValue | undefined {
  switch (property.type) {
    case "boolean":
      if (typeof answer === "boolean") return answer;
      if (typeof answer === "string" && /^(true|false)$/i.test(answer.trim()))
        return answer.trim().toLowerCase() === "true";
      return undefined;
    case "number":
    case "integer": {
      const value = typeof answer === "string" && answer.trim() !== "" ? Number(answer) : answer;
      if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
      if (property.type === "integer" && !Number.isInteger(value)) return undefined;
      if (property.minimum != null && value < property.minimum) return undefined;
      if (property.maximum != null && value > property.maximum) return undefined;
      return value;
    }
    case "string": {
      if (typeof answer !== "string") return undefined;
      if (property.minLength != null && answer.length < property.minLength) return undefined;
      if (property.maxLength != null && answer.length > property.maxLength) return undefined;
      const allowed = elicitationEnumValues(property);
      return allowed && !allowed.includes(answer) ? undefined : answer;
    }
    case "array": {
      if (!Array.isArray(answer) || !answer.every((item) => typeof item === "string"))
        return undefined;
      if (property.minItems != null && answer.length < property.minItems) return undefined;
      if (property.maxItems != null && answer.length > property.maxItems) return undefined;
      const allowed =
        "enum" in property.items
          ? property.items.enum
          : property.items.anyOf.map((entry) => entry.const);
      return answer.every((item) => allowed.includes(item)) ? answer : undefined;
    }
  }
}

function elicitationQuestions(
  request: { readonly message: string; readonly requestedSchema: typeof formSchema.Type },
  requestId: string,
): OrchestrationV2UserInputQuestion[] {
  const properties = request.requestedSchema.properties ?? {};
  const required = new Set(request.requestedSchema.required ?? []);
  if (Object.keys(properties).length === 0) {
    return [
      {
        id: EMPTY_FORM_CONFIRMATION_ID,
        header: "Confirmation",
        question: request.message || "Continue with this request?",
        options: [
          {
            label: "Continue",
            description: "Accept this request without additional fields.",
            value: "accept",
          },
          { label: "Decline", description: "Decline this request.", value: "decline" },
        ],
        allowCustomAnswer: false,
        multiSelect: false,
      },
    ];
  }
  return Object.entries(properties).map(([id, property]) => {
    const enumOptions = elicitationEnumOptions(property);
    const options: Array<{
      label: string;
      description: string;
      value: string;
      exclusive?: boolean;
    }> =
      enumOptions?.map(({ value, label }) => ({
        label,
        description: text(property.description) ?? "Choose a value.",
        value,
      })) ?? [];
    if (!required.has(id))
      options.push({
        label: "Leave unset",
        description: "Optional field; do not send a value.",
        value: optionalElicitationSkipValue(property, requestId, id),
        exclusive: true,
      });
    return {
      id,
      header: text(property.title) ?? (required.has(id) ? "Required" : "Optional"),
      question: [request.message, text(property.description)].filter(Boolean).join("\n\n") || id,
      options,
      allowCustomAnswer: !enumOptions?.length,
      ...(property.type === "string" && !enumOptions?.length
        ? { answerFormat: "raw-string" as const }
        : {}),
      multiSelect: property.type === "array",
    };
  });
}

const decodeForm = Schema.decodeUnknownOption(formSchema);

export function prepareMastraCodeForm(
  input: Parameters<NonNullable<AcpAdapterV2Flavor["prepareFormElicitation"]>>[0],
  validateString: (
    property: FormProperty,
    answer: ElicitationValue,
  ) => Effect.Effect<boolean, EffectAcpErrors.AcpError>,
): ReturnType<NonNullable<AcpAdapterV2Flavor["prepareFormElicitation"]>> {
  return Effect.gen(function* () {
    const decoded = decodeForm(input.request.requestedSchema);
    const candidate = Option.getOrUndefined(decoded);
    const supported =
      candidate !== undefined &&
      Object.values(candidate.properties ?? {}).every((property) =>
        ["string", "number", "integer", "boolean", "array"].includes(property.type),
      );
    const schema = supported ? candidate : undefined;
    const request = { message: input.request.message, requestedSchema: schema ?? {} };
    const questions: OrchestrationV2UserInputQuestion[] = [];
    for (const question of elicitationQuestions(request, input.nativeRequestId)) {
      const property = schema?.properties?.[question.id];
      const allowEmptyAnswer =
        question.answerFormat === "raw-string" &&
        property?.type === "string" &&
        coerceMastraCodeElicitationAnswer(property, "") !== undefined &&
        (yield* validateString(property, "").pipe(Effect.orElseSucceed(() => false)));
      questions.push({
        ...question,
        ...(question.answerFormat === "raw-string" ? { allowEmptyAnswer } : {}),
      });
    }
    return {
      questions,
      respond: (answers) =>
        Effect.gen(function* () {
          if (schema === undefined || answers === null) return { action: "cancel" } as const;
          const properties = schema.properties ?? {};
          if (Object.keys(properties).length === 0) {
            const submitted = answers[EMPTY_FORM_CONFIRMATION_ID];
            const confirmation =
              Array.isArray(submitted) && submitted.length === 1 ? submitted[0] : submitted;
            return confirmation === "accept"
              ? ({ action: "accept", content: {} } as const)
              : ({ action: confirmation === "decline" ? "decline" : "cancel" } as const);
          }
          const content: Record<string, ElicitationValue> = {};
          for (const [key, submitted] of Object.entries(answers)) {
            if (!Object.hasOwn(properties, key)) continue;
            const property = properties[key]!;
            if (!schema.required?.includes(key)) {
              const skip = optionalElicitationSkipValue(property, input.nativeRequestId, key);
              if (submitted === skip || (Array.isArray(submitted) && submitted.includes(skip))) {
                if (Array.isArray(submitted) && submitted.length !== 1)
                  return { action: "cancel" } as const;
                continue;
              }
            }
            const value =
              property.type !== "array" && Array.isArray(submitted)
                ? submitted.length === 1
                  ? submitted[0]
                  : undefined
                : submitted;
            const coerced = coerceMastraCodeElicitationAnswer(property, value);
            if (coerced === undefined || !(yield* validateString(property, coerced))) {
              return { action: "cancel" } as const;
            }
            content[key] = coerced;
          }
          if (schema.required?.some((key) => !Object.hasOwn(content, key))) {
            return { action: "cancel" } as const;
          }
          return { action: "accept", content } as const;
        }),
    };
  });
}
