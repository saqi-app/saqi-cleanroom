const TITLE_LIMIT = 65;
const DESCRIPTION_LIMIT = 155;
const GRAPHEME_SEGMENTER = new Intl.Segmenter(undefined, {
  granularity: "grapheme",
});

function codePointLength(value: string): number {
  let length = 0;
  for (const _codePoint of value) length += 1;
  return length;
}

function truncate(value: string, limit: number): string {
  const normalized = value.replaceAll(/\s+/g, " ").trim();
  if (codePointLength(normalized) <= limit) return normalized;
  const prefix: string[] = [];
  let used = 0;
  for (const { segment } of GRAPHEME_SEGMENTER.segment(normalized)) {
    const length = codePointLength(segment);
    if (used + length > limit - 1) break;
    prefix.push(segment);
    used += length;
  }
  return `${prefix.join("").trimEnd()}…`;
}

export function pageTitle(value: string): string {
  const brand = " — Saqi";
  return `${truncate(value, TITLE_LIMIT - codePointLength(brand))}${brand}`;
}

export function contextualPageTitle(primary: string, context: string): string {
  const brand = " — Saqi";
  const contextLimit = 24;
  const conciseContext = truncate(context, contextLimit);
  const suffix = ` — ${conciseContext}${brand}`;
  return `${truncate(primary, TITLE_LIMIT - codePointLength(suffix))}${suffix}`;
}

export function metaDescription(value: string): string {
  return truncate(value, DESCRIPTION_LIMIT);
}
