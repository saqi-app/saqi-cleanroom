import { z, type ZodType } from "zod";

import { CorpusImportActionSchema } from "./corpus-import.js";
import {
  ProductionResolutionApiResponseSchema,
  ProductionResolutionRequestSchema,
} from "./production-resolution.js";
import { ResourceIdSchema } from "./resource-id-schema.js";
import {
  EnrichmentPublicationV2RequestSchema,
  EnrichmentPublicationV2ResponseSchema,
  SourceAdmissionV2RequestSchema,
  SourceAdmissionV2ResponseSchema,
} from "./source-bound-publication.js";

const EmptyHttpPartSchema = z.strictObject({});
const TextDocumentSchema = z.string().describe("Rendered response body");
const AuthorSlugSchema = z
  .string()
  .min(1)
  .max(512)
  .describe("URL-encoded author slug");
const PositiveIntegerSegmentSchema = z
  .string()
  .regex(/^[1-9]\d*$/)
  .describe("A base-10 positive integer path segment");

export const ErrorResponseSchema = z
  .strictObject({ error: z.string().min(1) })
  .describe("Non-sensitive API error response");

export const AuthorRouteParamsSchema = z.strictObject({
  authorSlug: AuthorSlugSchema,
});
export const AuthorPageRouteParamsSchema = AuthorRouteParamsSchema.extend({
  pageNumber: PositiveIntegerSegmentSchema,
});
export const PoemRouteParamsSchema = AuthorRouteParamsSchema.extend({
  poemId: ResourceIdSchema,
});
export const SitemapRouteParamsSchema = z.strictObject({
  shard: PositiveIntegerSegmentSchema,
});

type HttpMethod = "GET" | "POST";
type HttpService = "operations" | "public-site";
type HttpAudience = "authenticated" | "public";

interface HttpContract {
  audience: HttpAudience;
  body: null | ZodType;
  id: string;
  method: HttpMethod;
  params: ZodType;
  path: string;
  query: ZodType;
  responses: readonly {
    body: null | ZodType;
    contentType: string;
    status: number;
  }[];
  service: HttpService;
  summary: string;
}

const HTML_RESPONSE = {
  body: TextDocumentSchema,
  contentType: "text/html; charset=utf-8",
  status: 200,
} as const;
const NOT_FOUND_RESPONSE = {
  body: TextDocumentSchema,
  contentType: "text/html; charset=utf-8",
  status: 404,
} as const;
const EMPTY_INPUT = {
  body: null,
  params: EmptyHttpPartSchema,
  query: EmptyHttpPartSchema,
} as const;
const CorpusImportResponseSchema = z.discriminatedUnion("ok", [
  z.strictObject({ ok: z.literal(true), result: z.unknown() }),
  z.strictObject({ error: z.string().min(1), ok: z.literal(false) }),
]);
export const LegacySourceLineageAdoptionRequestSchema = z.strictObject({
  poemIds: z
    .array(z.uuid())
    .min(1)
    .max(10)
    .refine((poemIds) => new Set(poemIds).size === poemIds.length, {
      message: "Legacy source lineage poem IDs must be unique",
    }),
});
const LegacySourceLineageAdoptionResponseSchema = z.discriminatedUnion("ok", [
  z.strictObject({
    ok: z.literal(true),
    result: z.strictObject({
      adopted: z.number().int().nonnegative().max(10),
      conflicts: z
        .array(
          z.strictObject({
            code: z.string().regex(/^[A-Z][A-Z\d_]{2,99}$/),
            poemId: z.uuid(),
          }),
        )
        .max(10)
        .default([]),
      scanned: z.number().int().nonnegative().max(10),
      unchanged: z.number().int().nonnegative().max(10),
    }),
  }),
  z.strictObject({
    error: z.string().min(1),
    ok: z.literal(false),
    retryable: z.boolean(),
  }),
]);
const SourceLineageMaintenanceRequestSchema = z.strictObject({
  maxPages: z.number().int().min(1).max(2).default(2),
});
const SourceLineageMaintenanceStateSchema = z.enum([
  "active",
  "blocked",
  "complete",
  "failed",
  "idle",
]);
const SourceLineageMaintenanceStatusSchema = z.strictObject({
  adoptedTotal: z.number().int().nonnegative(),
  conflictTotal: z.number().int().nonnegative(),
  cursorPoemId: z.string().nullable(),
  lastErrorCode: z.string().nullable(),
  pass: z.number().int().nonnegative(),
  remaining: z.number().int().nonnegative(),
  scannedTotal: z.number().int().nonnegative(),
  state: SourceLineageMaintenanceStateSchema,
  updatedAt: z.number().int().nonnegative(),
});
const SourceLineageMaintenanceResponseSchema = z.discriminatedUnion("ok", [
  z.strictObject({
    ok: z.literal(true),
    result: SourceLineageMaintenanceStatusSchema,
  }),
  z.strictObject({
    error: z.string().min(1),
    ok: z.literal(false),
    retryable: z.boolean(),
  }),
]);
export const SourceFingerprintBackfillRequestSchema = z.strictObject({
  cursor: z
    .strictObject({
      createdAt: z.number().int().nonnegative(),
      sourceRevisionId: z.string().regex(/^[a-f\d]{64}$/),
    })
    .optional(),
  limit: z.number().int().min(1).max(20).default(20),
});
const SourceFingerprintBackfillResponseSchema = z.discriminatedUnion("ok", [
  z.strictObject({
    ok: z.literal(true),
    result: z.strictObject({
      complete: z.boolean(),
      existing: z.number().int().nonnegative(),
      inserted: z.number().int().nonnegative(),
      nextCursor:
        SourceFingerprintBackfillRequestSchema.shape.cursor.nullable(),
      scanned: z.number().int().nonnegative().max(20),
    }),
  }),
  z.strictObject({ error: z.string().min(1), ok: z.literal(false) }),
]);
const ProductionResolutionErrorResponseSchema = z.strictObject({
  error: z.string().min(1),
  ok: z.literal(false),
});
const SourceBoundPublicationErrorResponseSchema = z.strictObject({
  code: z.string().regex(/^[A-Z][A-Z0-9_]{1,63}$/),
  error: z.string().min(1),
  ok: z.literal(false),
  retryable: z.boolean(),
});

export const HTTP_CONTRACTS = [
  {
    ...EMPTY_INPUT,
    audience: "public",
    id: "public.author-index",
    method: "GET",
    path: "/",
    responses: [HTML_RESPONSE],
    service: "public-site",
    summary: "List and search all poets with published poems",
  },
  {
    ...EMPTY_INPUT,
    audience: "public",
    id: "public.legacy-author-page",
    method: "GET",
    params: z.strictObject({ pageNumber: PositiveIntegerSegmentSchema }),
    path: "/authors/page/{pageNumber}",
    responses: [
      {
        body: null,
        contentType: "text/plain; charset=utf-8",
        status: 308,
      },
    ],
    service: "public-site",
    summary: "Redirect a legacy paginated author index to the complete index",
  },
  {
    ...EMPTY_INPUT,
    audience: "public",
    id: "public.author",
    method: "GET",
    params: AuthorRouteParamsSchema,
    path: "/author/{authorSlug}",
    responses: [HTML_RESPONSE, NOT_FOUND_RESPONSE],
    service: "public-site",
    summary: "Render an author's first page of published poems",
  },
  {
    ...EMPTY_INPUT,
    audience: "public",
    id: "public.author-page",
    method: "GET",
    params: AuthorPageRouteParamsSchema,
    path: "/author/{authorSlug}/page/{pageNumber}",
    responses: [HTML_RESPONSE, NOT_FOUND_RESPONSE],
    service: "public-site",
    summary: "Render a numbered page of an author's published poems",
  },
  {
    ...EMPTY_INPUT,
    audience: "public",
    id: "public.poem",
    method: "GET",
    params: PoemRouteParamsSchema,
    path: "/author/{authorSlug}/poem/{poemId}",
    responses: [HTML_RESPONSE, NOT_FOUND_RESPONSE],
    service: "public-site",
    summary: "Render a published poem and its available translations",
  },
  {
    ...EMPTY_INPUT,
    audience: "public",
    id: "public.not-found",
    method: "GET",
    path: "/404",
    responses: [NOT_FOUND_RESPONSE],
    service: "public-site",
    summary: "Render the noindex public not-found document",
  },
  {
    ...EMPTY_INPUT,
    audience: "public",
    id: "public.server-error",
    method: "GET",
    path: "/500",
    responses: [
      {
        body: TextDocumentSchema,
        contentType: "text/html; charset=utf-8",
        status: 500,
      },
    ],
    service: "public-site",
    summary: "Render the noindex public server-error document",
  },
  {
    ...EMPTY_INPUT,
    audience: "public",
    id: "public.robots",
    method: "GET",
    path: "/robots.txt",
    responses: [
      {
        body: TextDocumentSchema,
        contentType: "text/plain; charset=utf-8",
        status: 200,
      },
    ],
    service: "public-site",
    summary: "Publish crawler policy and the sitemap index location",
  },
  {
    ...EMPTY_INPUT,
    audience: "public",
    id: "public.sitemap-index",
    method: "GET",
    path: "/sitemap-index.xml",
    responses: [
      {
        body: TextDocumentSchema,
        contentType: "application/xml; charset=utf-8",
        status: 200,
      },
    ],
    service: "public-site",
    summary: "List the stable sitemap shards",
  },
  {
    ...EMPTY_INPUT,
    audience: "public",
    id: "public.author-sitemap",
    method: "GET",
    path: "/sitemaps/authors-1.xml",
    responses: [
      {
        body: TextDocumentSchema,
        contentType: "application/xml; charset=utf-8",
        status: 200,
      },
    ],
    service: "public-site",
    summary: "List the public author URLs",
  },
  {
    ...EMPTY_INPUT,
    audience: "public",
    id: "public.poem-sitemap",
    method: "GET",
    params: SitemapRouteParamsSchema,
    path: "/sitemaps/poems-{shard}.xml",
    responses: [
      {
        body: TextDocumentSchema,
        contentType: "application/xml; charset=utf-8",
        status: 200,
      },
      {
        body: TextDocumentSchema,
        contentType: "text/plain; charset=utf-8",
        status: 404,
      },
    ],
    service: "public-site",
    summary: "List one bounded shard of public poem URLs",
  },
  {
    ...EMPTY_INPUT,
    audience: "authenticated",
    id: "operations.home",
    method: "GET",
    path: "/",
    responses: [HTML_RESPONSE],
    service: "operations",
    summary: "Render the authenticated operations landing page",
  },
  {
    ...EMPTY_INPUT,
    audience: "authenticated",
    id: "operations.tasks",
    method: "GET",
    path: "/tasks",
    responses: [HTML_RESPONSE],
    service: "operations",
    summary: "Report retirement of the cloud task console",
  },
  {
    ...EMPTY_INPUT,
    audience: "authenticated",
    id: "operations.not-found",
    method: "GET",
    path: "/404",
    responses: [NOT_FOUND_RESPONSE],
    service: "operations",
    summary: "Render the authenticated operations not-found page",
  },
  {
    audience: "authenticated",
    body: LegacySourceLineageAdoptionRequestSchema,
    id: "operations.legacy-source-lineage-adoption",
    method: "POST",
    params: EmptyHttpPartSchema,
    path: "/api/legacy-source-lineage-adoption",
    query: EmptyHttpPartSchema,
    responses: [200, 400, 403, 409, 415, 503].map((status) => ({
      body: LegacySourceLineageAdoptionResponseSchema,
      contentType: "application/json",
      status,
    })),
    service: "operations",
    summary: "Adopt legacy source lineage for explicit poem UUIDs",
  },
  {
    ...EMPTY_INPUT,
    audience: "authenticated",
    id: "operations.source-lineage-maintenance-status",
    method: "GET",
    path: "/api/source-lineage-maintenance-status",
    responses: [200, 503].map((status) => ({
      body: SourceLineageMaintenanceResponseSchema,
      contentType: "application/json",
      status,
    })),
    service: "operations",
    summary: "Report exact legacy source-lineage maintenance progress",
  },
  {
    audience: "authenticated",
    body: SourceLineageMaintenanceRequestSchema,
    id: "operations.source-lineage-maintenance-run",
    method: "POST",
    params: EmptyHttpPartSchema,
    path: "/api/source-lineage-maintenance",
    query: EmptyHttpPartSchema,
    responses: [200, 400, 403, 415, 503].map((status) => ({
      body: SourceLineageMaintenanceResponseSchema,
      contentType: "application/json",
      status,
    })),
    service: "operations",
    summary: "Run bounded self-healing source-lineage maintenance",
  },
  {
    audience: "authenticated",
    body: SourceFingerprintBackfillRequestSchema,
    id: "operations.corpus-fingerprint-backfill",
    method: "POST",
    params: EmptyHttpPartSchema,
    path: "/api/corpus-fingerprint-backfill",
    query: EmptyHttpPartSchema,
    responses: [200, 400, 403, 409, 415, 503].map((status) => ({
      body: SourceFingerprintBackfillResponseSchema,
      contentType: "application/json",
      status,
    })),
    service: "operations",
    summary:
      "Backfill authoritative active source fingerprints in bounded pages",
  },
  {
    audience: "authenticated",
    body: CorpusImportActionSchema,
    id: "operations.corpus-import",
    method: "POST",
    params: EmptyHttpPartSchema,
    path: "/api/corpus-import",
    query: EmptyHttpPartSchema,
    responses: [
      {
        body: CorpusImportResponseSchema,
        contentType: "application/json",
        status: 200,
      },
      {
        body: CorpusImportResponseSchema,
        contentType: "application/json",
        status: 409,
      },
      {
        body: CorpusImportResponseSchema,
        contentType: "application/json",
        status: 415,
      },
    ],
    service: "operations",
    summary: "Stage, confirm, and publish immutable corpus revisions",
  },
  {
    audience: "authenticated",
    body: ProductionResolutionRequestSchema,
    id: "operations.corpus-resolution",
    method: "POST",
    params: EmptyHttpPartSchema,
    path: "/api/corpus-resolution",
    query: EmptyHttpPartSchema,
    responses: [
      {
        body: ProductionResolutionApiResponseSchema,
        contentType: "application/json",
        status: 200,
      },
      ...[400, 403, 409, 415, 503].map((status) => ({
        body: ProductionResolutionErrorResponseSchema,
        contentType: "application/json",
        status,
      })),
    ],
    service: "operations",
    summary: "Resolve bounded source targets to production identities",
  },
  {
    audience: "authenticated",
    body: SourceAdmissionV2RequestSchema,
    id: "operations.source-admissions-v2",
    method: "POST",
    params: EmptyHttpPartSchema,
    path: "/api/v2/source-admissions",
    query: EmptyHttpPartSchema,
    responses: [
      {
        body: SourceAdmissionV2ResponseSchema,
        contentType: "application/json",
        status: 200,
      },
      ...[400, 403, 409, 413, 415, 503].map((status) => ({
        body: SourceBoundPublicationErrorResponseSchema,
        contentType: "application/json",
        status,
      })),
    ],
    service: "operations",
    summary: "Admit exact source revisions and issue canonical poem bindings",
  },
  {
    audience: "authenticated",
    body: EnrichmentPublicationV2RequestSchema,
    id: "operations.enrichment-publications-v2",
    method: "POST",
    params: EmptyHttpPartSchema,
    path: "/api/v2/enrichment-publications",
    query: EmptyHttpPartSchema,
    responses: [
      {
        body: EnrichmentPublicationV2ResponseSchema,
        contentType: "application/json",
        status: 200,
      },
      ...[400, 403, 409, 413, 415, 503].map((status) => ({
        body: SourceBoundPublicationErrorResponseSchema,
        contentType: "application/json",
        status,
      })),
    ],
    service: "operations",
    summary: "Atomically publish bound, approved enrichment artifacts",
  },
] as const satisfies readonly HttpContract[];

export const CLOUDFLARE_WORKER_CONTRACTS = [
  {
    bindings: ["ASSETS", "DB"],
    handlers: ["fetch"],
    id: "operations",
    requiredVariables: ["SAQI_ACCESS_AUDIENCE", "SAQI_ACCESS_TEAM_ORIGIN"],
  },
  {
    bindings: ["ASSETS", "DB"],
    handlers: ["fetch"],
    id: "public-site",
    requiredVariables: [],
  },
  {
    bindings: [],
    handlers: ["fetch"],
    id: "www-redirect",
    requiredVariables: [],
  },
] as const;

function jsonSchema(schema: ZodType) {
  return z.toJSONSchema(schema);
}

export function generateContractCatalog() {
  return {
    version: 1,
    http: HTTP_CONTRACTS.map((contract) => ({
      audience: contract.audience,
      id: contract.id,
      method: contract.method,
      path: contract.path,
      request: {
        body: contract.body ? jsonSchema(contract.body) : null,
        params: jsonSchema(contract.params),
        query: jsonSchema(contract.query),
      },
      responses: contract.responses.map((response) => ({
        body: response.body ? jsonSchema(response.body) : null,
        contentType: response.contentType,
        status: response.status,
      })),
      service: contract.service,
      summary: contract.summary,
    })),
    actions: [],
    workers: CLOUDFLARE_WORKER_CONTRACTS,
    queues: [],
  };
}
