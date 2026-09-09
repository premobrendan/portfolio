import type { PackMeta } from '../shared/types'
import { listPacks } from '../electron/storage'

/**
 * The tools need `draftVersion` and the saved name before deciding whether a write is safe.
 * `listPacks` already reads every meta.json and skips malformed ones, so this reuses it
 * rather than widening storage.ts's private `readMeta`. Reading every meta to find one is
 * wasteful in principle; at the scale of a personal pack library it is not worth a second
 * code path that could disagree with the listing.
 */
export async function readPackMetaForTools(packId: string): Promise<PackMeta | null> {
  return (await listPacks()).find(m => m.id === packId) ?? null
}
