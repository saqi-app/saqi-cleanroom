import {
  canonicalPoemBindingIdBody,
  type CanonicalPoemBindingV1,
  CanonicalPoemBindingV1Schema,
  CORPUS_REVISION_SCHEMA_VERSION,
  type CorpusImportAction,
  ENRICHMENT_INPUT_SCHEMA_ID,
  ENRICHMENT_INPUT_SCHEMA_VERSION,
  type PoemEnrichmentInput,
  type PoemEnrichmentInputV2,
  PoemEnrichmentInputV2Schema,
  sourceAdmissionIdBody,
  type SourceAdmissionV2Item,
  SourceAdmissionV2ItemSchema,
  sourceLineNfcHashBody,
  sourcePromptMaterialHashBody,
} from "@saqi/precedent-iso";
import { currentSource } from "@saqi/source-adapter";
import { z } from "zod";

import { canonicalJson, sha256 } from "../persistence/work-key.js";

export const PublishedPoemStructureSchema = z.enum(["classical", "free_verse"]);

const DetailArtifactSchema = z.strictObject({
  artifactSchemaVersion: z.literal(1),
  collectedBy: z.string(),
  source: z.strictObject({
    author: z.strictObject({
      canonicalId: z.string(),
      href: z.url(),
      path: z.string(),
      slug: z.string(),
    }),
    canonicalId: z.string(),
    href: z.url(),
    lines: z.array(z.string()).min(1).max(2_000),
    numericId: z.string().regex(/^[1-9]\d*$/),
    slug: z.string(),
    structure: PublishedPoemStructureSchema,
    title: z.string().trim().min(1),
    verses: z.number().int().nonnegative().nullable(),
  }),
  sourceHash: z.string().regex(/^[\da-f]{64}$/),
  sourceContext: z
    .strictObject({
      authorNameArabic: z.string().trim().min(1).max(512),
      refreshGeneration: z.string().regex(/^[\w.-]{1,64}$/),
    })
    .optional(),
  workKey: z.string(),
});

export interface CatalogPoemMapping {
  readonly authorId: string;
  readonly authorNameArabic: string;
  readonly canonicalPoemId?: null | string;
  readonly poemId: string;
  readonly sourceAuthorSlug: string;
  readonly sourcePoemId: string;
}

export interface PreparedCollectedPoem {
  readonly enrichmentInput: PoemEnrichmentInput;
  readonly stageAction: CorpusImportAction;
}

/** Derive the stable collection identity available before production has
 * created a canonical poem binding. Author context is captured by the
 * collector from the author manifest and is therefore artifact-bound. */
export function prepareUnboundCollectedMapping(
  rawArtifact: unknown,
  fallbackAuthorNameArabic?: string,
): CatalogPoemMapping | null {
  const artifact = DetailArtifactSchema.parse(rawArtifact);
  const authorNameArabic =
    artifact.sourceContext?.authorNameArabic ?? fallbackAuthorNameArabic;
  if (!authorNameArabic) return null;
  const { numericId, author } = artifact.source;
  const { name: sourceName } = currentSource();
  return {
    authorId: sha256(`author\u{1F}${sourceName}\u{1F}${author.slug}`),
    authorNameArabic,
    canonicalPoemId: null,
    poemId: sha256(`poem\u{1F}${sourceName}\u{1F}${numericId}`),
    sourceAuthorSlug: author.slug,
    sourcePoemId: numericId,
  };
}

/** Build the content-addressed admission request solely from captured source
 * material. Production resolves the canonical author and poem identities. */
export function prepareSourceAdmission(
  rawArtifact: unknown,
): SourceAdmissionV2Item {
  const artifact = DetailArtifactSchema.parse(rawArtifact);
  const detail = artifact.source;
  const { name: sourceName } = currentSource();
  const sourceContentSha256 = sha256(
    canonicalJson({ content: detail.lines, titleArabic: detail.title }),
  );
  const body = {
    lineNfcHash: sha256(sourceLineNfcHashBody(detail.lines)),
    linesArabic: detail.lines,
    sourceAuthorId: detail.author.slug,
    sourceAuthorUrl: detail.author.href,
    sourceContentSha256,
    sourceName,
    externalPoemId: detail.numericId,
    sourcePoemUrl: detail.href,
    sourceRevisionId: canonicalSourceRevisionId(
      sourceName,
      detail.numericId,
      CORPUS_REVISION_SCHEMA_VERSION,
      sourceContentSha256,
    ),
    titleArabic: detail.title,
  };
  return SourceAdmissionV2ItemSchema.parse({
    admissionId: sha256(sourceAdmissionIdBody(body)),
    ...body,
  });
}

export function bindCollectedPoem(
  input: PoemEnrichmentInput,
  bindingInput: CanonicalPoemBindingV1,
): PoemEnrichmentInputV2 {
  const binding = CanonicalPoemBindingV1Schema.parse(bindingInput);
  const {
    admissionEvidence: _admissionEvidence,
    bindingId: _bindingId,
    ...bindingIdentity
  } = binding;
  if (sha256(canonicalPoemBindingIdBody(bindingIdentity)) !== binding.bindingId)
    throw new Error("CANONICAL_BINDING_ID_MISMATCH");
  if (sha256(sourceLineNfcHashBody(input.linesArabic)) !== binding.lineNfcHash)
    throw new Error("CANONICAL_BINDING_LINE_NFC_MISMATCH");
  if (
    sha256(
      sourcePromptMaterialHashBody({
        authorArabic: input.authorArabic,
        linesArabic: input.linesArabic,
        titleArabic: input.titleArabic,
      }),
    ) !== binding.promptMaterialHash
  )
    throw new Error("CANONICAL_BINDING_PROMPT_MATERIAL_MISMATCH");
  return PoemEnrichmentInputV2Schema.parse({
    ...input,
    canonicalBinding: binding,
    schemaVersion: 2,
    // Production owns revision identity. Prompt and line fingerprints above
    // prove that rebinding does not change any model-visible Arabic input.
    sourceRevisionId: binding.sourceRevisionId,
  });
}

export function canonicalSourceRevisionId(
  sourceName: string,
  sourcePoemId: string,
  schemaVersion: number,
  contentHash: string,
): string {
  return sha256(
    `revision\u{1F}${sourceName}\u{1F}${sourcePoemId}\u{1F}${String(schemaVersion)}\u{1F}${contentHash}`,
  );
}

export function prepareCollectedPoem(
  rawArtifact: unknown,
  mapping: CatalogPoemMapping,
  observedAt: string,
  writerEpoch: number,
): PreparedCollectedPoem {
  const artifact = DetailArtifactSchema.parse(rawArtifact);
  const detail = artifact.source;
  if (
    detail.numericId !== mapping.sourcePoemId ||
    detail.author.slug !== mapping.sourceAuthorSlug
  ) {
    throw new Error("COLLECTED_POEM_MAPPING_MISMATCH");
  }
  const sourceContentArabic = { content: detail.lines };
  const sourceContentHash = sha256(canonicalJson(sourceContentArabic));
  const contentArabic = {
    content: detail.lines,
    titleArabic: detail.title,
  };
  const contentHash = sha256(canonicalJson(contentArabic));
  const { name: sourceName } = currentSource();
  const recordBody = {
    authorNameArabic: mapping.authorNameArabic,
    canonicalAuthorId: mapping.authorId,
    canonicalPoemId:
      mapping.canonicalPoemId === undefined
        ? mapping.poemId
        : mapping.canonicalPoemId,
    contentArabic,
    contentHash,
    observedAt,
    ordinal: 0,
    sourceAuthorId: detail.author.slug,
    sourceAuthorUrl: detail.author.href,
    sourceName,
    sourcePoemId: detail.numericId,
    sourcePoemUrl: detail.href,
    titleArabic: detail.title,
  };
  const recordHash = sha256(canonicalJson(recordBody));
  const manifestHash = sha256(canonicalJson({ recordHashes: [recordHash] }));
  const bundleId = `${sourceName}:${manifestHash}`;
  const revisionId = canonicalSourceRevisionId(
    sourceName,
    detail.numericId,
    CORPUS_REVISION_SCHEMA_VERSION,
    contentHash,
  );
  return {
    enrichmentInput: {
      schemaId: ENRICHMENT_INPUT_SCHEMA_ID,
      schemaVersion: ENRICHMENT_INPUT_SCHEMA_VERSION,
      poemId: mapping.poemId,
      sourceRevisionId: revisionId,
      sourceContentSha256: sourceContentHash,
      titleArabic: detail.title,
      authorArabic: mapping.authorNameArabic,
      linesArabic: detail.lines,
    },
    stageAction: {
      action: "stage-and-plan",
      input: {
        bundle: {
          expectedRecordCount: 1,
          id: bundleId,
          manifestHash,
          schemaVersion: CORPUS_REVISION_SCHEMA_VERSION,
          writerEpoch,
        },
        records: [{ ...recordBody, bundleId, recordHash }],
        rootHash: sha256(canonicalJson([recordHash])),
      },
    },
  };
}
