const SCRIPT_UNSAFE_CHARACTERS = /[&<>\u{2028}\u{2029}]/gu;

const ESCAPED_CHARACTERS: Readonly<Record<string, string>> = Object.freeze({
  "&": String.raw`\u0026`,
  "<": String.raw`\u003c`,
  ">": String.raw`\u003e`,
  "\u{2028}": String.raw`\u2028`,
  "\u{2029}": String.raw`\u2029`,
});

export function serializeJsonForHtml(value: unknown): string {
  const serialized = JSON.stringify(value);
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- JSON.stringify returns undefined for unsupported top-level values despite its standard-library return type.
  if (serialized === undefined) {
    throw new TypeError("Value cannot be represented as JSON");
  }

  return serialized.replaceAll(
    SCRIPT_UNSAFE_CHARACTERS,
    (character) => ESCAPED_CHARACTERS[character] ?? character,
  );
}
