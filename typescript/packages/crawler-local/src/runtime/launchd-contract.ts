import { z } from "zod";

const LAUNCHD_CLEANUP_MARGIN_MS = 15_000;

export const LaunchdServiceLabelSchema = z
  .string()
  .regex(/^[a-zA-Z\d][a-zA-Z\d.-]{2,127}$/)
  .default("net.saqi.crawler");

export function minimumLaunchdExitTimeoutSeconds(
  shutdownGraceMs: number,
): number {
  return Math.ceil((shutdownGraceMs + LAUNCHD_CLEANUP_MARGIN_MS) / 1_000);
}
