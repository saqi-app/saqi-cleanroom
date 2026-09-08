import {
  type CorpusImportAction,
  CorpusImportActionSchema,
  type EnrichmentPublicationV2Request,
  EnrichmentPublicationV2RequestSchema,
  EnrichmentPublicationV2ResponseSchema,
  MAX_CORPUS_IMPORT_BYTES,
  type SourceAdmissionV2Request,
  SourceAdmissionV2RequestSchema,
  SourceAdmissionV2ResponseSchema,
} from "@saqi/precedent-iso";
import { z } from "zod";

import { canonicalJson, sha256 } from "../persistence/work-key.js";
import {
  isNetworkFailureText,
  networkProbeDelayMs,
} from "../runtime/network-resilience.js";

export const PublicationRequestTimeoutMsSchema = z
  .number()
  .int()
  .min(1)
  .max(10 * 60_000);
const HttpStatusSchema = z.number().int().min(100).max(599);
const PublicationConflictKindSchema = z.enum([
  "pointer",
  "source_revision",
  "writer_epoch",
]);

const ResponseSchema = z.discriminatedUnion("ok", [
  z.strictObject({ ok: z.literal(true), result: z.unknown() }),
  z.strictObject({
    conflict: z
      .strictObject({
        kind: PublicationConflictKindSchema,
        retryable: z.literal(true),
      })
      .optional(),
    error: z.string().min(1),
    ok: z.literal(false),
  }),
]);

const DEFAULT_REQUEST_TIMEOUT_MS = 2 * 60_000;
const SupersededPublicationResponseSchema = z.object({
  error: z.literal("LEGACY_SOL_PUBLICATION_SUPERSEDED"),
  ok: z.literal(false),
  retryable: z.literal(false),
});

export interface PublicationRequest {
  readonly body: string;
  readonly signal?: AbortSignal;
  readonly url: string;
}

export interface PublicationTransportResponse {
  readonly authFailure?: "expired" | "rejected";
  readonly body: string;
  readonly retryAfter?: string | undefined;
  readonly status: number;
}

export type PublicationTransport = (
  request: PublicationRequest,
) => Promise<PublicationTransportResponse>;

export type PublicationClientResult =
  | {
      readonly actionHash: string;
      readonly responseHash: string;
      readonly result: unknown;
      readonly state: "confirmed";
    }
  | {
      readonly errorCode: "PUBLICATION_NETWORK_UNAVAILABLE";
      readonly retryAt: number;
      readonly state: "network_wait";
    }
  | {
      readonly errorCode: string;
      readonly kind: "pointer" | "source_revision" | "writer_epoch";
      readonly state: "conflict";
    }
  | {
      readonly errorCode: string;
      readonly retryAt: number;
      readonly state: "auth_wait";
    }
  | {
      readonly errorCode: string;
      readonly retryAt: number;
      readonly state: "retry_wait";
    }
  | {
      readonly errorCode: string;
      readonly retryAt: number;
      readonly state: "service_wait";
    }
  | {
      readonly errorCode: string;
      readonly state: "rejected";
    };

export interface PublicationClientOptions {
  readonly allowInsecureLocalhost?: boolean;
  readonly endpoint: string;
  readonly now?: () => number;
  readonly random?: () => number;
  readonly requestTimeoutMs?: number;
  readonly transport?: PublicationTransport;
}

/** Auth remains outside this client: the default transport uses the caller's
 * existing same-origin credentials; service-token transports may be injected
 * by the operator without persisting secrets in work inputs or receipts. */
// eslint-disable-next-line @sarj/require-interface-for-exported-class -- This concrete protocol client consumes the injected PublicationTransport contract; another interface would duplicate the protocol implementation.
export class PublicationClient {
  readonly #boundEnrichmentEndpoint: string;
  readonly #sourceAdmissionEndpoint: string;
  readonly #endpoint: string;
  readonly #now: () => number;
  readonly #random: () => number;
  readonly #requestTimeoutMs: number;
  readonly #transport: PublicationTransport;
  #consecutiveNetworkFailures = 0;
  #consecutiveAuthFailures = 0;
  #consecutiveServiceFailures = 0;

  constructor(options: PublicationClientOptions) {
    const endpoint = new URL(options.endpoint);
    if (endpoint.username || endpoint.password || endpoint.hash) {
      throw new Error(
        "Publication endpoint cannot contain credentials or a fragment",
      );
    }
    const local =
      endpoint.hostname === "localhost" || endpoint.hostname === "127.0.0.1";
    if (
      endpoint.protocol !== "https:" &&
      !(local && options.allowInsecureLocalhost)
    ) {
      throw new Error("Publication endpoint must use HTTPS");
    }
    this.#endpoint = endpoint.toString();
    this.#boundEnrichmentEndpoint = new URL(
      "/api/v2/enrichment-publications",
      endpoint,
    ).toString();
    this.#sourceAdmissionEndpoint = new URL(
      "/api/v2/source-admissions",
      endpoint,
    ).toString();
    this.#now = options.now ?? Date.now;
    this.#random = options.random ?? Math.random;
    this.#requestTimeoutMs = PublicationRequestTimeoutMsSchema.parse(
      options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
    );
    this.#transport = options.transport ?? fetchTransport;
  }

  async send(
    rawAction: unknown,
    signal?: AbortSignal,
  ): Promise<PublicationClientResult> {
    const action = CorpusImportActionSchema.parse(rawAction);
    const body = canonicalJson(action);
    if (Buffer.byteLength(body) > MAX_CORPUS_IMPORT_BYTES) {
      throw new Error("PUBLICATION_ACTION_TOO_LARGE");
    }
    const actionHash = sha256(body);
    const timeoutSignal = AbortSignal.timeout(this.#requestTimeoutMs);
    const requestSignal = signal
      ? AbortSignal.any([signal, timeoutSignal])
      : timeoutSignal;
    let response: PublicationTransportResponse;
    try {
      response = await this.#transport({
        body,
        signal: requestSignal,
        url: this.#endpoint,
      });
    } catch (error) {
      if (timeoutSignal.aborted || isNetworkFailureText(errorText(error))) {
        this.#consecutiveNetworkFailures += 1;
        return {
          errorCode: "PUBLICATION_NETWORK_UNAVAILABLE",
          retryAt:
            this.#now() +
            networkProbeDelayMs(this.#consecutiveNetworkFailures, this.#random),
          state: "network_wait",
        };
      }
      this.#consecutiveNetworkFailures = 0;
      return {
        // A transport can fail after the server commits. Corpus publication
        // actions are content-addressed and idempotent, so a bounded replay is
        // safe; do not call an unclassified failure a proven network outage.
        errorCode: "PUBLICATION_TRANSPORT_OUTCOME_UNKNOWN",
        retryAt: this.#now() + 30_000,
        state: "retry_wait",
      };
    }
    this.#consecutiveNetworkFailures = 0;
    if (
      response.authFailure !== undefined ||
      response.status === 401 ||
      response.status === 403
    ) {
      this.#consecutiveAuthFailures += 1;
      this.#consecutiveServiceFailures = 0;
      return {
        errorCode:
          response.authFailure === "expired"
            ? "PUBLICATION_AUTH_EXPIRED"
            : response.authFailure === "rejected"
              ? "PUBLICATION_AUTH_REJECTED"
              : "PUBLICATION_AUTH_REQUIRED",
        retryAt:
          this.#now() +
          networkProbeDelayMs(
            this.#consecutiveAuthFailures,
            this.#random,
            30_000,
            30 * 60_000,
          ),
        state: "auth_wait",
      };
    }
    this.#consecutiveAuthFailures = 0;
    if (Buffer.byteLength(response.body) > MAX_CORPUS_IMPORT_BYTES) {
      this.#consecutiveServiceFailures = 0;
      return { errorCode: "PUBLICATION_RESPONSE_TOO_LARGE", state: "rejected" };
    }
    if ([408, 425, 429].includes(response.status) || response.status >= 500) {
      if (response.status !== 429) {
        this.#consecutiveServiceFailures += 1;
        return {
          errorCode: serviceUnavailableCode(response.status),
          retryAt: Math.max(
            retryAt(response.retryAfter, this.#now()),
            this.#now() +
              networkProbeDelayMs(
                this.#consecutiveServiceFailures,
                this.#random,
                30_000,
                30 * 60_000,
              ),
          ),
          state: "service_wait",
        };
      }
      this.#consecutiveServiceFailures = 0;
      return {
        errorCode: "PUBLICATION_RATE_LIMITED",
        retryAt: retryAt(response.retryAfter, this.#now()),
        state: "retry_wait",
      };
    }
    this.#consecutiveServiceFailures = 0;
    let parsed: z.infer<typeof ResponseSchema>;
    try {
      parsed = ResponseSchema.parse(JSON.parse(response.body));
    } catch {
      return { errorCode: "PUBLICATION_INVALID_RESPONSE", state: "rejected" };
    }
    if (response.status < 200 || response.status >= 300 || !parsed.ok) {
      if (!parsed.ok && parsed.conflict) {
        return {
          errorCode: parsed.error,
          kind: parsed.conflict.kind,
          state: "conflict",
        };
      }
      return {
        errorCode: parsed.ok ? "PUBLICATION_HTTP_REJECTED" : parsed.error,
        state: "rejected",
      };
    }
    const resultJson = canonicalJson(parsed.result);
    return {
      actionHash,
      responseHash: sha256(resultJson),
      result: parsed.result,
      state: "confirmed",
    };
  }

  async sendBoundEnrichment(
    rawRequest: EnrichmentPublicationV2Request,
    signal?: AbortSignal,
  ): Promise<PublicationClientResult> {
    const request = EnrichmentPublicationV2RequestSchema.parse(rawRequest);
    const body = canonicalJson(request);
    if (Buffer.byteLength(body) > MAX_CORPUS_IMPORT_BYTES) {
      throw new Error("PUBLICATION_ACTION_TOO_LARGE");
    }
    const actionHash = sha256(body);
    const timeoutSignal = AbortSignal.timeout(this.#requestTimeoutMs);
    const requestSignal = signal
      ? AbortSignal.any([signal, timeoutSignal])
      : timeoutSignal;
    let response: PublicationTransportResponse;
    try {
      response = await this.#transport({
        body,
        signal: requestSignal,
        url: this.#boundEnrichmentEndpoint,
      });
    } catch (error) {
      if (timeoutSignal.aborted || isNetworkFailureText(errorText(error))) {
        this.#consecutiveNetworkFailures += 1;
        return {
          errorCode: "PUBLICATION_NETWORK_UNAVAILABLE",
          retryAt:
            this.#now() +
            networkProbeDelayMs(this.#consecutiveNetworkFailures, this.#random),
          state: "network_wait",
        };
      }
      this.#consecutiveNetworkFailures = 0;
      return {
        errorCode: "PUBLICATION_TRANSPORT_OUTCOME_UNKNOWN",
        retryAt: this.#now() + 30_000,
        state: "retry_wait",
      };
    }
    this.#consecutiveNetworkFailures = 0;
    if (
      response.authFailure !== undefined ||
      response.status === 401 ||
      response.status === 403
    ) {
      this.#consecutiveAuthFailures += 1;
      this.#consecutiveServiceFailures = 0;
      return {
        errorCode:
          response.authFailure === "expired"
            ? "PUBLICATION_AUTH_EXPIRED"
            : response.authFailure === "rejected"
              ? "PUBLICATION_AUTH_REJECTED"
              : "PUBLICATION_AUTH_REQUIRED",
        retryAt:
          this.#now() +
          networkProbeDelayMs(
            this.#consecutiveAuthFailures,
            this.#random,
            30_000,
            30 * 60_000,
          ),
        state: "auth_wait",
      };
    }
    this.#consecutiveAuthFailures = 0;
    if (Buffer.byteLength(response.body) > MAX_CORPUS_IMPORT_BYTES) {
      this.#consecutiveServiceFailures = 0;
      return { errorCode: "PUBLICATION_RESPONSE_TOO_LARGE", state: "rejected" };
    }
    if ([408, 425, 429].includes(response.status) || response.status >= 500) {
      if (response.status !== 429) {
        this.#consecutiveServiceFailures += 1;
        return {
          errorCode: serviceUnavailableCode(response.status),
          retryAt: Math.max(
            retryAt(response.retryAfter, this.#now()),
            this.#now() +
              networkProbeDelayMs(
                this.#consecutiveServiceFailures,
                this.#random,
                30_000,
                30 * 60_000,
              ),
          ),
          state: "service_wait",
        };
      }
      this.#consecutiveServiceFailures = 0;
      return {
        errorCode: "PUBLICATION_RATE_LIMITED",
        retryAt: retryAt(response.retryAfter, this.#now()),
        state: "retry_wait",
      };
    }
    this.#consecutiveServiceFailures = 0;
    if (response.status < 200 || response.status >= 300) {
      try {
        const superseded = SupersededPublicationResponseSchema.safeParse(
          JSON.parse(response.body),
        );
        if (superseded.success)
          return { errorCode: superseded.data.error, state: "rejected" };
      } catch {
        // Non-JSON failures remain terminal HTTP rejections.
      }
      return { errorCode: "PUBLICATION_HTTP_REJECTED", state: "rejected" };
    }
    try {
      const result = EnrichmentPublicationV2ResponseSchema.parse(
        JSON.parse(response.body),
      );
      return {
        actionHash,
        responseHash: sha256(canonicalJson(result)),
        result,
        state: "confirmed",
      };
    } catch {
      return { errorCode: "PUBLICATION_INVALID_RESPONSE", state: "rejected" };
    }
  }

  async sendSourceAdmissions(
    rawRequest: SourceAdmissionV2Request,
    signal?: AbortSignal,
  ): Promise<PublicationClientResult> {
    const request = SourceAdmissionV2RequestSchema.parse(rawRequest);
    const body = canonicalJson(request);
    if (Buffer.byteLength(body) > MAX_CORPUS_IMPORT_BYTES) {
      throw new Error("PUBLICATION_ACTION_TOO_LARGE");
    }
    const actionHash = sha256(body);
    const timeoutSignal = AbortSignal.timeout(this.#requestTimeoutMs);
    const requestSignal = signal
      ? AbortSignal.any([signal, timeoutSignal])
      : timeoutSignal;
    let response: PublicationTransportResponse;
    try {
      response = await this.#transport({
        body,
        signal: requestSignal,
        url: this.#sourceAdmissionEndpoint,
      });
    } catch (error) {
      if (timeoutSignal.aborted || isNetworkFailureText(errorText(error))) {
        this.#consecutiveNetworkFailures += 1;
        return {
          errorCode: "PUBLICATION_NETWORK_UNAVAILABLE",
          retryAt:
            this.#now() +
            networkProbeDelayMs(this.#consecutiveNetworkFailures, this.#random),
          state: "network_wait",
        };
      }
      this.#consecutiveNetworkFailures = 0;
      return {
        errorCode: "PUBLICATION_TRANSPORT_OUTCOME_UNKNOWN",
        retryAt: this.#now() + 30_000,
        state: "retry_wait",
      };
    }
    this.#consecutiveNetworkFailures = 0;
    if (
      response.authFailure !== undefined ||
      response.status === 401 ||
      response.status === 403
    ) {
      this.#consecutiveAuthFailures += 1;
      this.#consecutiveServiceFailures = 0;
      return {
        errorCode:
          response.authFailure === "expired"
            ? "PUBLICATION_AUTH_EXPIRED"
            : response.authFailure === "rejected"
              ? "PUBLICATION_AUTH_REJECTED"
              : "PUBLICATION_AUTH_REQUIRED",
        retryAt:
          this.#now() +
          networkProbeDelayMs(
            this.#consecutiveAuthFailures,
            this.#random,
            30_000,
            30 * 60_000,
          ),
        state: "auth_wait",
      };
    }
    this.#consecutiveAuthFailures = 0;
    if (Buffer.byteLength(response.body) > MAX_CORPUS_IMPORT_BYTES) {
      this.#consecutiveServiceFailures = 0;
      return { errorCode: "PUBLICATION_RESPONSE_TOO_LARGE", state: "rejected" };
    }
    if ([408, 425, 429].includes(response.status) || response.status >= 500) {
      if (response.status !== 429) {
        this.#consecutiveServiceFailures += 1;
        return {
          errorCode: serviceUnavailableCode(response.status),
          retryAt: Math.max(
            retryAt(response.retryAfter, this.#now()),
            this.#now() +
              networkProbeDelayMs(
                this.#consecutiveServiceFailures,
                this.#random,
                30_000,
                30 * 60_000,
              ),
          ),
          state: "service_wait",
        };
      }
      this.#consecutiveServiceFailures = 0;
      return {
        errorCode: "PUBLICATION_RATE_LIMITED",
        retryAt: retryAt(response.retryAfter, this.#now()),
        state: "retry_wait",
      };
    }
    this.#consecutiveServiceFailures = 0;
    if (response.status < 200 || response.status >= 300) {
      return { errorCode: "PUBLICATION_HTTP_REJECTED", state: "rejected" };
    }
    try {
      const result = SourceAdmissionV2ResponseSchema.parse(
        JSON.parse(response.body),
      );
      return {
        actionHash,
        responseHash: sha256(canonicalJson(result)),
        result,
        state: "confirmed",
      };
    } catch {
      return { errorCode: "PUBLICATION_INVALID_RESPONSE", state: "rejected" };
    }
  }
}

function serviceUnavailableCode(status: number): string {
  return `PUBLICATION_SERVICE_UNAVAILABLE_HTTP_${String(
    HttpStatusSchema.parse(status),
  )}`;
}

function errorText(error: unknown): string {
  if (error instanceof Error) {
    const cause =
      "cause" in error
        ? error.cause instanceof Error
          ? `${error.cause.name}\n${error.cause.message}`
          : typeof error.cause === "string"
            ? error.cause
            : ""
        : "";
    return `${error.name}\n${error.message}\n${cause}`;
  }
  return String(error);
}

async function fetchTransport(
  request: PublicationRequest,
): Promise<PublicationTransportResponse> {
  const response = await fetch(request.url, {
    body: request.body,
    credentials: "include",
    headers: { "content-type": "application/json" },
    method: "POST",
    redirect: "error",
    ...(request.signal ? { signal: request.signal } : {}),
  });
  const retryAfter = response.headers.get("retry-after");
  return {
    body: await response.text(),
    ...(retryAfter === null ? {} : { retryAfter }),
    status: response.status,
  };
}

function retryAt(value: string | undefined, now: number): number {
  if (value) {
    const seconds = Number(value);
    if (Number.isFinite(seconds) && seconds >= 0) {
      return now + Math.min(seconds * 1_000, 24 * 60 * 60_000);
    }
    const timestamp = Date.parse(value);
    if (Number.isFinite(timestamp) && timestamp > now) {
      return Math.min(timestamp, now + 24 * 60 * 60_000);
    }
  }
  return now + 60_000;
}

export function publicationActionHash(action: CorpusImportAction): string {
  return sha256(canonicalJson(CorpusImportActionSchema.parse(action)));
}
