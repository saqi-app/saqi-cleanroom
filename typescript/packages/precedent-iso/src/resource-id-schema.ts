import { z } from "zod";

const RESOURCE_ID_MAX_LENGTH = 128;

export const ResourceIdSchema = z
  .string()
  .trim()
  .min(1)
  .max(RESOURCE_ID_MAX_LENGTH)
  .regex(/^[A-Za-z0-9_-]+$/)
  .describe("A bounded Saqi resource identifier");
