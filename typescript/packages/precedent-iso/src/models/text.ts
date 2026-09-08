import { z } from "zod";

const UNSAFE_CONTROL =
  /[\u{0000}-\u{0008}\u{000b}\u{000c}\u{000e}-\u{001f}\u{007f}\u{202a}-\u{202e}\u{2066}-\u{2069}]/u;

export const IdentityTextSchema = z
  .string()
  .trim()
  .min(1)
  .max(500)
  .refine(
    (value) => !UNSAFE_CONTROL.test(value),
    "Text contains unsafe control characters",
  );

export const ContentLineSchema = z
  .string()
  .max(5_000)
  .refine(
    (value) => !UNSAFE_CONTROL.test(value),
    "Line contains unsafe control characters",
  )
  .transform((value) => value.normalize("NFKC"));

export const RouteSegmentSchema = z
  .string()
  .trim()
  .min(1)
  .max(500)
  .refine(
    (value) =>
      !UNSAFE_CONTROL.test(value) &&
      !value.includes("/") &&
      value !== "." &&
      value !== "..",
    "Route segment is unsafe",
  );
