import { z } from "zod";

export const SolImportReceiptSchema = z.strictObject({
  sourceDigest: z.string().regex(/^[a-f\d]{64}$/),
  records: z.int().nonnegative(),
  sourceBytes: z.int().nonnegative(),
  importedAt: z.int().nonnegative(),
});
export type SolImportReceipt = z.infer<typeof SolImportReceiptSchema>;
