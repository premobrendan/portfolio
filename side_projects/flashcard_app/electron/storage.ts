import fs from 'node:fs/promises'
import fsSync from 'node:fs'
import path from 'node:path'
import type { AppState, Card, Pack, PackMeta, Settings } from '../shared/types'

const DEFAULT_STATE: AppState = { selectedPackIds: [], hasLaunchedBefore: false }
const DEFAULT_SETTINGS: Settings = { themeId: 'space', themeMode: 'dynamic' }

let root = ''

export function setStorageRoot(dir: string) {
  root = dir
  fsSync.mkdirSync(root, { recursive: true })
}
export function getStorageRoot() { return root }

const packsDir = () => path.join(root, 'packs')
const packDir = (id: string) => path.join(packsDir(), id)

/** Read JSON, falling back to `fallback` on missing OR malformed files. Never throws. */
async function readJson<T>(file: string, fallback: T): Promise<T> {
  try {
    return JSON.parse(await fs.readFile(file, 'utf8')) as T
  } catch {
    return fallback
  }
}

async function writeJson(file: string, data: unknown): Promise<void> {
  await fs.mkdir(path.dirname(file), { recursive: true })
  await fs.writeFile(file, JSON.stringify(data, null, 2), 'utf8')
}

export async function readState(): Promise<AppState> {
  const s = await readJson<AppState>(path.join(root, 'state.json'), DEFAULT_STATE)
  return {
    selectedPackIds: Array.isArray(s.selectedPackIds) ? s.selectedPackIds : [],
    hasLaunchedBefore: Boolean(s.hasLaunchedBefore),
  }
}

export async function writeState(patch: Partial<AppState>): Promise<void> {
  await writeJson(path.join(root, 'state.json'), { ...(await readState()), ...patch })
}

export async function readSettings(): Promise<Settings> {
  return { ...DEFAULT_SETTINGS, ...(await readJson(path.join(root, 'settings.json'), {})) }
}

export async function writeSettings(patch: Partial<Settings>): Promise<void> {
  await writeJson(path.join(root, 'settings.json'), { ...(await readSettings()), ...patch })
}

/** Lists packs by reading meta.json only — never card data, so this stays fast. */
export async function listPacks(): Promise<PackMeta[]> {
  let entries: string[]
  try {
    entries = (await fs.readdir(packsDir(), { withFileTypes: true }))
      .filter(e => e.isDirectory()).map(e => e.name)
  } catch {
    return []
  }

  const metas: PackMeta[] = []
  for (const id of entries) {
    const meta = await readJson<PackMeta | null>(path.join(packDir(id), 'meta.json'), null)
    // One malformed pack must never prevent the others from loading.
    if (meta && typeof meta.id === 'string' && typeof meta.currentVersion === 'number') {
      metas.push(meta)
    } else {
      console.warn(`[storage] skipping malformed pack: ${id}`)
    }
  }
  return metas.sort((a, b) => a.name.localeCompare(b.name))
}

export async function readPack(id: string): Promise<Pack> {
  const meta = await readJson<PackMeta | null>(path.join(packDir(id), 'meta.json'), null)
  if (!meta) throw new Error(`pack not found: ${id}`)
  const body = await readJson<{ cards: Card[] }>(
    path.join(packDir(id), `v${meta.currentVersion}.json`), { cards: [] },
  )
  return { id: meta.id, name: meta.name, schemaVersion: 1, cards: body.cards }
}

/** Appends a new numbered version and points meta.json at it. */
export async function writePackVersion(id: string, name: string, cards: Card[]): Promise<number> {
  const existing = await readJson<PackMeta | null>(path.join(packDir(id), 'meta.json'), null)
  const version = (existing?.currentVersion ?? 0) + 1
  await writeJson(path.join(packDir(id), `v${version}.json`), { cards })
  await writeJson(path.join(packDir(id), 'meta.json'), {
    id, name, cardCount: cards.length, currentVersion: version, schemaVersion: 1,
  } satisfies PackMeta)
  return version
}
