export function normalizeSearch(value) {
  return value
    .normalize("NFKC")
    .toLocaleLowerCase("en-US")
    .replaceAll(
      /[\u{0610}-\u{061a}\u{064b}-\u{065f}\u{0670}\u{06d6}-\u{06ed}\u{0640}]/gu,
      "",
    )
    .replaceAll(/[أإآٱ]/gu, "ا")
    .replaceAll(/[ىی]/gu, "ي")
    .replaceAll("ک", "ك")
    .replaceAll("ؤ", "و")
    .replaceAll("ئ", "ي")
    .replaceAll(/\s+/gu, " ")
    .trim();
}
