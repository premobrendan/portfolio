import fs from 'node:fs/promises'
import path from 'node:path'
import type { Pack } from '../shared/types'
import { listPacks, readState, writePackVersion, writeState } from './storage'

/** On first run, copy bundled packs in and select all of them. */
export async function seedIfEmpty(resourcesDir: string): Promise<void> {
  const state = await readState()
  if (state.hasLaunchedBefore) return

  let files: string[] = []
  try {
    files = (await fs.readdir(resourcesDir)).filter(f => f.endsWith('.json'))
  } catch {
    files = []
  }

  for (const file of files) {
    try {
      const pack = JSON.parse(
        await fs.readFile(path.join(resourcesDir, file), 'utf8'),
      ) as Pack
      await writePackVersion(pack.id, pack.name, pack.cards)
    } catch {
      console.warn(`[seed] skipping unreadable bundled pack: ${file}`)
    }
  }

  // Default on first ever open: every pack selected.
  const all = await listPacks()
  await writeState({ selectedPackIds: all.map(p => p.id), hasLaunchedBefore: true })
}
