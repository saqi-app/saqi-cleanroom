import { timingSafeEqual } from "node:crypto";

import { z } from "zod";

const OpaqueCredentialGenerationSchema = z.string().regex(/^[a-f\d]{64}$/);
export const CredentialSnapshotObservationSchema = z.discriminatedUnion(
  "state",
  [
    z.strictObject({
      accountGeneration: OpaqueCredentialGenerationSchema,
      materialGeneration: OpaqueCredentialGenerationSchema,
      observedAt: z.number().int().nonnegative(),
      state: z.literal("stable"),
    }),
    z.strictObject({
      observedAt: z.number().int().nonnegative(),
      state: z.literal("transient"),
    }),
  ],
);
const CredentialObservationClassificationSchema = z.enum([
  "changed_during_invocation",
  "pending",
  "stable",
  "transient",
]);
export const SolCredentialObservationSchema = z
  .strictObject({
    after: CredentialSnapshotObservationSchema.nullable(),
    before: CredentialSnapshotObservationSchema,
    classification: CredentialObservationClassificationSchema,
    schemaId: z.literal("saqi.sol-credential-observation"),
    schemaVersion: z.literal(1),
  })
  .superRefine((observation, context) => {
    const expected = credentialObservationClassification(
      observation.before,
      observation.after,
    );
    if (observation.classification !== expected)
      context.addIssue({
        code: "custom",
        message: "Credential observation classification is inconsistent",
      });
  });
export type SolCredentialObservation = z.infer<
  typeof SolCredentialObservationSchema
>;

export function credentialObservationClassification(
  before: z.infer<typeof CredentialSnapshotObservationSchema>,
  after: null | z.infer<typeof CredentialSnapshotObservationSchema>,
): SolCredentialObservation["classification"] {
  if (after === null) return "pending";
  if (before.state === "transient" || after.state === "transient")
    return "transient";
  return credentialGenerationsEqual(
    before.accountGeneration,
    after.accountGeneration,
  ) &&
    credentialGenerationsEqual(
      before.materialGeneration,
      after.materialGeneration,
    )
    ? "stable"
    : "changed_during_invocation";
}

export function credentialGenerationsEqual(
  left: null | string,
  right: null | string,
): boolean {
  if (typeof left !== "string" || typeof right !== "string")
    return Object.is(left, right);
  return timingSafeEqual(Buffer.from(left, "hex"), Buffer.from(right, "hex"));
}
