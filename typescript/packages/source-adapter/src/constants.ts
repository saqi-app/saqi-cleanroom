import { SourceNameSchema, SourceOriginSchema } from "@saqi/precedent-iso";

import {
  DEFAULT_SOURCE_ADAPTER_PROFILE,
  type SourceAdapterProfileV1,
  SourceAdapterProfileV1Schema,
} from "./profile.js";

export interface SourceConfiguration {
  readonly name: string;
  readonly origin: string;
  readonly profile?: SourceAdapterProfileV1;
}

const DEFAULT_SOURCE_CONFIGURATION: SourceConfiguration = Object.freeze({
  name: "source",
  origin: "https://source.invalid",
});

let sourceConfiguration = DEFAULT_SOURCE_CONFIGURATION;
let sourceAdapterProfile = DEFAULT_SOURCE_ADAPTER_PROFILE;

export function configureSource(raw: SourceConfiguration): void {
  const name = SourceNameSchema.safeParse(raw.name);
  if (!name.success) {
    throw new Error("SOURCE_NAME_INVALID");
  }
  const origin = SourceOriginSchema.safeParse(raw.origin);
  if (!origin.success) {
    throw new Error("SOURCE_ORIGIN_INVALID");
  }
  sourceConfiguration = Object.freeze({
    name: name.data,
    origin: origin.data,
    ...(raw.profile === undefined
      ? {}
      : { profile: SourceAdapterProfileV1Schema.parse(raw.profile) }),
  });
  sourceAdapterProfile =
    raw.profile === undefined
      ? DEFAULT_SOURCE_ADAPTER_PROFILE
      : SourceAdapterProfileV1Schema.parse(raw.profile);
}

export function currentSourceAdapterProfile(): SourceAdapterProfileV1 {
  return sourceAdapterProfile;
}

export function currentSource(): SourceConfiguration {
  return sourceConfiguration;
}

export const PROJECTION_SCHEMA_VERSION = 1 as const;

export const LIMITS = {
  authorName: 512,
  authorSlug: 256,
  authorsPerInventory: 10_000,
  poemTitle: 512,
  poemsPerAuthor: 20_000,
  poemLines: 4_096,
  poemLine: 4_096,
  poemTextBytes: 4 * 1024 * 1024,
  url: 2_048,
  verses: 2_048,
} as const;
