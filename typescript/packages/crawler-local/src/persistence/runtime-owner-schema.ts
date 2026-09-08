import { z } from "zod";

export const RuntimeOwnerRecordSchema = z.strictObject({
  configDigest: z.string().regex(/^[a-f\d]{64}$/),
  pid: z.int().positive(),
  runId: z.uuid(),
  schemaVersion: z.literal(1),
  startedAt: z.iso.datetime(),
});
export type RuntimeOwnerRecord = z.infer<typeof RuntimeOwnerRecordSchema>;
export const RuntimeOwnerKindSchema = z.enum(["supervisor", "maintenance"]);
export const RuntimeOwnerRowSchema = z
  .strictObject({
    singleton: z.literal(1),
    epoch: z.int().nonnegative(),
    held: z.literal([0, 1]),
    owner_kind: RuntimeOwnerKindSchema.nullable(),
    pid: z.int().positive().nullable(),
    run_id: z.uuid().nullable(),
    config_digest: z
      .string()
      .regex(/^[a-f\d]{64}$/)
      .nullable(),
    started_at: z.iso.datetime().nullable(),
  })
  .superRefine((row, context) => {
    const fields = [
      row.owner_kind,
      row.pid,
      row.run_id,
      row.config_digest,
      row.started_at,
    ];
    if (
      row.epoch === 0
        ? row.held !== 0 || fields.some((value) => value !== null)
        : fields.some((value) => value === null)
    )
      context.addIssue({
        code: "custom",
        message: "Incomplete runtime owner authority",
      });
  });
export type RuntimeOwnerRow = z.infer<typeof RuntimeOwnerRowSchema>;
export const RUNTIME_OWNER_MIGRATION_SQL = `
CREATE TABLE runtime_owner (
  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
  epoch INTEGER NOT NULL CHECK(epoch BETWEEN 0 AND 9007199254740991),
  held INTEGER NOT NULL CHECK(held IN (0,1)),
  owner_kind TEXT CHECK(owner_kind IN ('supervisor','maintenance')),
  pid INTEGER CHECK(pid > 0),
  run_id TEXT,
  config_digest TEXT,
  started_at TEXT,
  CHECK((epoch = 0 AND held = 0 AND owner_kind IS NULL AND pid IS NULL AND run_id IS NULL AND config_digest IS NULL AND started_at IS NULL)
    OR (epoch > 0 AND owner_kind IS NOT NULL AND pid IS NOT NULL AND run_id IS NOT NULL AND config_digest IS NOT NULL AND started_at IS NOT NULL))
) STRICT;
INSERT INTO runtime_owner(singleton, epoch, held) VALUES(1,0,0) ON CONFLICT(singleton) DO NOTHING;
CREATE TRIGGER runtime_owner_no_delete BEFORE DELETE ON runtime_owner
BEGIN SELECT RAISE(ABORT, 'RUNTIME_OWNER_DELETE_FORBIDDEN'); END;
`;
