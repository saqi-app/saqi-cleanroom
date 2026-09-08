import { hash } from "node:crypto";

import { type WorkDefinition, WorkDefinitionSchema } from "./schema.js";

function canonicalJson(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "string" || typeof value === "boolean") {
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value))
      throw new Error("Canonical JSON rejects non-finite numbers");
    return Object.is(value, -0) ? "0" : JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    if (!value.every((_item, index) => index in value)) {
      throw new Error("Canonical JSON rejects sparse arrays");
    }
    return `[${value.map(canonicalJson).join(",")}]`;
  }
  if (typeof value !== "object") {
    throw new TypeError(`Canonical JSON rejects ${typeof value}`);
  }
  const prototype = Reflect.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new Error("Canonical JSON accepts only plain objects");
  }
  return `{${Object.entries(value)
    // Compare Unicode scalar sequences directly. localeCompare() is locale- and
    // ICU-dependent, so it cannot define a durable hash or work identity.
    .toSorted(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
    .map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`)
    .join(",")}}`;
}

export function sha256(value: string | Uint8Array): string {
  return hash("sha256", value, "hex");
}

export function inputHash(input: Readonly<Record<string, unknown>>): string {
  return sha256(canonicalJson(input));
}

export function workKey(definition: WorkDefinition): string {
  const parsed = WorkDefinitionSchema.parse(definition);
  return sha256(
    canonicalJson({
      implementationVersion: parsed.implementationVersion,
      inputHash: parsed.inputHash,
      kind: parsed.kind,
      schemaVersion: parsed.schemaVersion,
    }),
  );
}

export { canonicalJson };
