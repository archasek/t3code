import { type UserInputAttachmentAnswerPayload } from "@t3tools/contracts";

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

export function getQuestionAnswerText(value: unknown): string {
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map(getQuestionAnswerText).filter(Boolean).join(", ");
  const nested = record(value);
  return nested ? getQuestionAnswerText(nested.answers) : "";
}

export function getQuestionTextPreview(answer: UserInputAttachmentAnswerPayload): string {
  return Object.values(answer.questionTextById ?? {})
    .map((text) => text.replace(/\s+/g, " ").trim())
    .filter(Boolean)
    .join(" · ");
}

export function getQuestionAnswerPreview(answer: UserInputAttachmentAnswerPayload): string {
  const answers = Object.values(answer.answers).map(getQuestionAnswerText).filter(Boolean);
  const attachments = Object.values(answer.attachmentsByQuestionId)
    .flat()
    .map((attachment) => attachment.name);
  return (
    answers.length > 0
      ? answers.join(" · ")
      : attachments.length > 0
        ? attachments.join(", ")
        : Object.values(answer.questionTextById ?? {}).join(" · ")
  )
    .replace(/\s+/g, " ")
    .trim();
}

export function hasQuestionAnswer(answer: UserInputAttachmentAnswerPayload): boolean {
  return (
    Object.values(answer.answers).some(getQuestionAnswerText) ||
    Object.values(answer.attachmentsByQuestionId).some((attachments) => attachments.length > 0)
  );
}

/** Exclusive choices replace every other choice; ordinary choices remove exclusive ones. */
export function toggleUserInputOption(
  options: ReadonlyArray<{
    readonly label: string;
    readonly value?: string | undefined;
    readonly exclusive?: boolean | undefined;
  }>,
  selected: ReadonlyArray<string>,
  value: string,
): string[] {
  if (selected.includes(value)) return selected.filter((entry) => entry !== value);
  if (options.some((option) => (option.value ?? option.label) === value && option.exclusive))
    return [value];
  return [
    ...selected.filter(
      (entry) =>
        !options.some((option) => (option.value ?? option.label) === entry && option.exclusive),
    ),
    value,
  ];
}

/** Raw empty text is complete only when the server established schema eligibility. */
export function resolveRawUserInputAnswer(
  question: { readonly allowEmptyAnswer?: boolean | undefined },
  value: string | undefined,
): string | null {
  return value === undefined || (value === "" && question.allowEmptyAnswer !== true) ? null : value;
}
