import { currentSource } from "@saqi/source-adapter";

export interface CollectionWorkKinds {
  readonly authorManifest: string;
  readonly poemDetail: string;
}

/** Resolve the durable v28 queue namespace after source configuration. */
export function collectionWorkKinds(): CollectionWorkKinds {
  const namespace = currentSource().name;
  return {
    authorManifest: `${namespace}_author_manifest`,
    poemDetail: `${namespace}_poem_detail`,
  };
}

interface CollectionScheduleSnapshot {
  readonly detailBurst: number;
  readonly detailsSinceManifest: number;
  readonly detailsUntilManifest: number;
  readonly preferredKind: string;
}

interface CollectionLaneSchedule {
  preferredKinds(): readonly string[];
  recordClaim(kind: string): void;
  snapshot(): CollectionScheduleSnapshot;
}

export class CollectionLaneScheduler implements CollectionLaneSchedule {
  readonly #detailBurst: number;
  #detailsSinceManifest: number;

  constructor(detailBurst = 100) {
    if (!Number.isSafeInteger(detailBurst) || detailBurst <= 0) {
      throw new Error("Detail burst must be a positive integer");
    }
    this.#detailBurst = detailBurst;
    // Prefer an already-seeded detail backlog on startup. When no details are
    // ready the claim loop still falls through to manifests, so fresh ledgers
    // continue to seed normally without making every restart hit a manifest
    // endpoint first.
    this.#detailsSinceManifest = 0;
  }

  preferredKinds(): readonly string[] {
    const kinds = collectionWorkKinds();
    return this.#detailsSinceManifest >= this.#detailBurst
      ? [kinds.authorManifest, kinds.poemDetail]
      : [kinds.poemDetail, kinds.authorManifest];
  }

  snapshot(): CollectionScheduleSnapshot {
    const kinds = collectionWorkKinds();
    const detailsUntilManifest = Math.max(
      0,
      this.#detailBurst - this.#detailsSinceManifest,
    );
    return {
      detailBurst: this.#detailBurst,
      detailsSinceManifest: this.#detailsSinceManifest,
      detailsUntilManifest,
      preferredKind:
        detailsUntilManifest === 0 ? kinds.authorManifest : kinds.poemDetail,
    };
  }

  recordClaim(kind: string): void {
    const kinds = collectionWorkKinds();
    if (kind === kinds.authorManifest) {
      this.#detailsSinceManifest = 0;
    } else if (kind === kinds.poemDetail) {
      this.#detailsSinceManifest = Math.min(
        this.#detailBurst,
        this.#detailsSinceManifest + 1,
      );
    } else {
      throw new Error("Unsupported collection lane");
    }
  }
}
