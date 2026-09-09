import fs from 'node:fs/promises'
import fsSync from 'node:fs'
import path from 'node:path'
import type { AppState, Card, Pack, PackMeta, Settings } from '../shared/types'
import { validateCards, type CardProblem } from '../shared/validate'

const DEFAULT_STATE: AppState = { selectedPackIds: [], hasLaunchedBefore: false }
const DEFAULT_SETTINGS: Settings = {
  themeId: 'space', themeMode: 'dynamic', cardDirection: 'forward', backView: 'primary',
}

let root = ''

export function setStorageRoot(dir: string) {
  root = dir
  fsSync.mkdirSync(root, { recursive: true })
}
export function getStorageRoot() { return root }

const packsDir = () => path.join(root, 'packs')

/**
 * Pack ids reach this module from disk, from IPC, and — once the MCP server lands — from
 * a model's output. `packs/` is a public interface (vision §2), so an id is treated as
 * untrusted input and may only ever name a single directory beneath it.
 */
function packDir(id: string): string {
  if (!/^[A-Za-z0-9._-]+$/.test(id) || id === '.' || id === '..') {
    throw new Error(`invalid pack id: ${id}`)
  }
  return path.join(packsDir(), id)
}

const metaPath = (id: string) => path.join(packDir(id), 'meta.json')
const versionPath = (id: string, v: number) => path.join(packDir(id), `v${v}.json`)

/**
 * A version file carries the pack name alongside its cards so that an unsaved rename can
 * live in a draft without touching `meta.name` — the name Pack Selection shows. Files
 * written before this field existed simply fall back to the meta name.
 */
type PackBody = { name?: string; cards: LegacyCard[] }

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

/**
 * `mixed` was a direction that put every card in the deck twice. It was replaced by the
 * independent Direction and Back View settings, so a profile saved while it existed is
 * mapped onto the nearest survivor rather than left holding a value nothing understands.
 */
const RETIRED_DIRECTIONS: Record<string, Settings['cardDirection']> = { mixed: 'random' }

export async function readSettings(): Promise<Settings> {
  const merged = {
    ...DEFAULT_SETTINGS,
    ...(await readJson<Partial<Settings>>(path.join(root, 'settings.json'), {})),
  }
  return {
    ...merged,
    cardDirection: RETIRED_DIRECTIONS[merged.cardDirection] ?? merged.cardDirection,
  }
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

async function readMeta(id: string): Promise<PackMeta | null> {
  return readJson<PackMeta | null>(metaPath(id), null)
}

/**
 * Cards written before `secondaries` existed carry a single `secondary` string. Folding it
 * in on READ means no migration pass over the library and no flag day: an old pack, a
 * backup version, and anything an out-of-date MCP client writes all load correctly. The
 * old key is dropped so nothing downstream has two sources of truth.
 */
type LegacyCard = Card & { secondary?: string }

function migrateCard(card: LegacyCard): Card {
  if (!card.secondary) {
    const { secondary: _drop, ...rest } = card
    return rest
  }
  const { secondary, secondaries, ...rest } = card
  return { ...rest, secondaries: [...(secondaries ?? []), secondary] }
}

async function readBody(id: string, version: number, fallbackName: string): Promise<Pack> {
  if (version === 0) return { id, name: fallbackName, schemaVersion: 1, cards: [] }
  const body = await readJson<PackBody>(versionPath(id, version), { cards: [] })
  return {
    id,
    name: body.name ?? fallbackName,
    schemaVersion: 1,
    cards: Array.isArray(body.cards) ? body.cards.map(migrateCard) : [],
  }
}

/** A never-saved pack reads as empty rather than throwing — it exists, it just has nothing yet. */
export async function readPack(id: string): Promise<Pack> {
  const meta = await readMeta(id)
  if (!meta) throw new Error(`pack not found: ${id}`)
  // Deliberately ignores draftVersion: Learn and the preview only ever see saved work.
  return { ...(await readBody(id, meta.currentVersion, meta.name)), name: meta.name }
}

/** The next free version number, accounting for drafts as well as saves. */
function nextVersion(meta: PackMeta | null): number {
  return Math.max(meta?.currentVersion ?? 0, meta?.draftVersion ?? 0) + 1
}

/** Appends a new numbered version, points meta.json at it, and drops any draft. */
export async function writePackVersion(id: string, name: string, cards: Card[]): Promise<number> {
  const existing = await readMeta(id)
  const version = nextVersion(existing)
  await writeJson(versionPath(id, version), { name, cards } satisfies PackBody)
  await writeJson(metaPath(id), {
    id, name, cardCount: cards.length, currentVersion: version, schemaVersion: 1,
  } satisfies PackMeta)
  return version
}

// ---------------------------------------------------------------------------
// Authoring
//
// Everything below is the write surface behind Make a Pack. It imports nothing from
// Electron, so the future MCP server can call `setStorageRoot` and then these functions
// directly — the same code path the UI uses, held to the same validation.
// ---------------------------------------------------------------------------

/** Slug plus a short random suffix, so two packs may share a name without colliding. */
function makePackId(name: string): string {
  const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 40)
  const suffix = Math.random().toString(36).slice(2, 8)
  return slug ? `${slug}-${suffix}` : `pack-${suffix}`
}

/**
 * Creates an empty pack on disk immediately, so drafts have a stable id to attach to.
 * With no saved cards it stays out of Pack Selection, so an abandoned one costs nothing
 * but a directory the user can delete.
 */
export async function createPack(name: string): Promise<PackMeta> {
  const meta: PackMeta = {
    id: makePackId(name), name, cardCount: 0, currentVersion: 0, schemaVersion: 1,
  }
  await writeJson(metaPath(meta.id), meta)
  return meta
}

/** What the editor resumes: the draft if there is one, otherwise the last save. */
export async function readPackForEdit(id: string): Promise<Pack> {
  const meta = await readMeta(id)
  if (!meta) throw new Error(`pack not found: ${id}`)
  return readBody(id, Math.max(meta.currentVersion, meta.draftVersion ?? 0), meta.name)
}

const sameContent = (a: Pack, name: string, cards: Card[]) =>
  a.name === name && JSON.stringify(a.cards) === JSON.stringify(cards)

/**
 * One version per unsaved exit (vision §8) — but only when something actually changed,
 * so bouncing in and out of an untouched pack cannot pile up identical versions.
 * Returns the version written, or null when the exit was a no-op.
 */
export async function writeDraft(id: string, name: string, cards: Card[]): Promise<number | null> {
  const meta = await readMeta(id)
  if (!meta) throw new Error(`pack not found: ${id}`)
  if (sameContent(await readPackForEdit(id), name, cards)) return null

  const version = nextVersion(meta)
  await writeJson(versionPath(id, version), { name, cards } satisfies PackBody)
  // currentVersion is untouched: an unsaved exit must never change what Learn studies.
  // draftName is dropped once it matches the saved name again, so the listing does not
  // keep flagging a rename the user has since undone.
  const draftName = name === meta.name ? undefined : name
  await writeJson(metaPath(id), { ...meta, draftVersion: version, draftName } satisfies PackMeta)
  return version
}

export type SaveResult = { ok: true; version: number } | { ok: false; problems: CardProblem[] }

/**
 * The one gate into a pack becoming live. Validates, writes, repoints meta, discards the
 * draft, then trims to the saved version plus two backups.
 */
export async function savePack(id: string, name: string, cards: Card[]): Promise<SaveResult> {
  const meta = await readMeta(id)
  if (!meta) throw new Error(`pack not found: ${id}`)

  const validation = validateCards(cards)
  if (!validation.ok) return { ok: false, problems: validation.problems }

  const version = nextVersion(meta)
  await writeJson(versionPath(id, version), { name, cards } satisfies PackBody)
  // Rebuilt rather than spread, so draftVersion is dropped rather than carried forward.
  await writeJson(metaPath(id), {
    id, name, cardCount: cards.length, currentVersion: version, schemaVersion: 1,
  } satisfies PackMeta)
  await pruneVersions(id, version)
  return { ok: true, version }
}

/** Keeps the newest three version files: the saved pack plus two backups (vision §8). */
async function pruneVersions(id: string, newest: number): Promise<void> {
  let files: string[]
  try {
    files = await fs.readdir(packDir(id))
  } catch {
    return
  }
  const doomed = files.filter(f => {
    const match = /^v(\d+)\.json$/.exec(f)
    return match ? Number(match[1]) <= newest - 3 : false
  })
  await Promise.all(doomed.map(f => fs.rm(path.join(packDir(id), f), { force: true })))
}

/** Removes a pack and every version it ever had. Silent if it is already gone. */
export async function deletePack(id: string): Promise<void> {
  await fs.rm(packDir(id), { recursive: true, force: true })
}
