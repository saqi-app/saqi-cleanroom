export const SITEMAP_SHARD_COUNT = 16;

const HASH_MODULUS = 2_147_483_647;

/** Mirrors the SQLite backfill in migration 0021 for every newly created ID. */
export function sitemapShardForId(identifier: string): number {
  let hash = 0;
  for (const character of identifier) {
    hash = (hash * 31 + (character.codePointAt(0) ?? 0)) % HASH_MODULUS;
  }
  return hash % SITEMAP_SHARD_COUNT;
}
