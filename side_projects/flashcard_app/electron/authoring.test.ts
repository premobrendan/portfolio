import { describe, it, expect, beforeEach } from 'vitest'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import type { Card } from '../shared/types'
import {
  setStorageRoot, getStorageRoot, listPacks, readPack,
  createPack, readPackForEdit, writeDraft, savePack, deletePack,
} from './storage'

let root: string
beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), 'fc-authoring-'))
  setStorageRoot(root)
})

const card = (id: string, front = 'France', primary = 'Paris'): Card =>
  ({ id, front, primary })

const versionFiles = (id: string) =>
  fs.readdirSync(path.join(getStorageRoot(), 'packs', id))
    .filter(f => /^v\d+\.json$/.test(f))
    .sort()

describe('createPack', () => {
  it('creates a pack that lists with no cards and no saved version', async () => {
    const meta = await createPack('World Capitals')
    expect(meta.name).toBe('World Capitals')
    expect(meta.cardCount).toBe(0)
    expect(meta.currentVersion).toBe(0)
    expect(await listPacks()).toHaveLength(1)
  })

  it('gives distinct ids to two packs sharing a name', async () => {
    const a = await createPack('Duplicate')
    const b = await createPack('Duplicate')
    expect(a.id).not.toBe(b.id)
    expect(await listPacks()).toHaveLength(2)
  })

  it('derives a filesystem-safe id even from an awkward name', async () => {
    const meta = await createPack('  Slashes/And..Dots!  ')
    expect(meta.id).toMatch(/^[a-z0-9-]+$/)
  })
})

describe('savePack', () => {
  it('makes saved cards visible to readPack', async () => {
    const { id } = await createPack('Capitals')
    const result = await savePack(id, 'Capitals', [card('a'), card('b', 'Japan', 'Tokyo')])

    expect(result.ok).toBe(true)
    const live = await readPack(id)
    expect(live.cards).toHaveLength(2)
    expect(live.cards[1].primary).toBe('Tokyo')
  })

  it('refuses a pack containing an incomplete card and writes nothing', async () => {
    const { id } = await createPack('Capitals')
    const result = await savePack(id, 'Capitals', [card('a'), card('b', 'Japan', '  ')])

    expect(result).toEqual({
      ok: false,
      problems: [{ cardId: 'b', index: 1, missing: ['primary'] }],
    })
    expect((await readPack(id)).cards).toHaveLength(0)
    expect(versionFiles(id)).toHaveLength(0)
  })

  it('records the saved card count in the listing', async () => {
    const { id } = await createPack('Capitals')
    await savePack(id, 'Capitals', [card('a'), card('b')])
    expect((await listPacks())[0].cardCount).toBe(2)
  })

  it('keeps only the saved version plus two backups', async () => {
    const { id } = await createPack('Capitals')
    for (let i = 1; i <= 5; i++) await savePack(id, 'Capitals', [card(`c${i}`)])

    expect(versionFiles(id)).toEqual(['v3.json', 'v4.json', 'v5.json'])
    expect((await readPack(id)).cards[0].id).toBe('c5')
  })
})

describe('drafts', () => {
  it('leaves the live pack untouched', async () => {
    const { id } = await createPack('Capitals')
    await savePack(id, 'Capitals', [card('saved', 'France', 'Paris')])
    await writeDraft(id, 'Capitals', [card('saved'), card('draft', 'Peru', 'Lima')])

    expect((await readPack(id)).cards).toHaveLength(1)
    expect((await listPacks())[0].cardCount).toBe(1)
  })

  it('is what the editor resumes from', async () => {
    const { id } = await createPack('Capitals')
    await savePack(id, 'Capitals', [card('saved')])
    await writeDraft(id, 'Capitals', [card('saved'), card('draft', 'Peru', 'Lima')])

    expect((await readPackForEdit(id)).cards).toHaveLength(2)
  })

  it('holds an unsaved rename without renaming the live pack', async () => {
    const { id } = await createPack('Old Name')
    await savePack(id, 'Old Name', [card('a')])
    await writeDraft(id, 'New Name', [card('a')])

    expect((await listPacks())[0].name).toBe('Old Name')
    expect((await readPackForEdit(id)).name).toBe('New Name')
  })

  it('surfaces the unsaved name in the listing, so the author can still find the pack', async () => {
    const { id } = await createPack('Untitled pack')
    await writeDraft(id, 'Capital Cities', [])

    // `name` stays the saved name — Pack Selection must not shift under the user — but
    // Make a Pack needs to show what the author actually last called it.
    const [meta] = await listPacks()
    expect(meta.name).toBe('Untitled pack')
    expect(meta.draftName).toBe('Capital Cities')
  })

  it('drops the draft name once the pack is saved under it', async () => {
    const { id } = await createPack('Untitled pack')
    await writeDraft(id, 'Capital Cities', [])
    await savePack(id, 'Capital Cities', [card('a')])

    const [meta] = await listPacks()
    expect(meta.name).toBe('Capital Cities')
    expect(meta.draftName).toBeUndefined()
  })

  it('may hold cards too incomplete to save', async () => {
    const { id } = await createPack('Capitals')
    await writeDraft(id, 'Capitals', [card('blank', '', '')])
    expect((await readPackForEdit(id)).cards).toHaveLength(1)
  })

  it('is discarded once the pack is saved', async () => {
    const { id } = await createPack('Capitals')
    await writeDraft(id, 'Capitals', [card('a'), card('b')])
    await savePack(id, 'Capitals', [card('a')])

    expect((await readPackForEdit(id)).cards).toHaveLength(1)
  })

  it('is skipped when nothing actually changed', async () => {
    const { id } = await createPack('Capitals')
    await savePack(id, 'Capitals', [card('a')])
    const before = versionFiles(id)

    await writeDraft(id, 'Capitals', [card('a')])

    expect(versionFiles(id)).toEqual(before)
  })
})

describe('readPackForEdit', () => {
  it('returns an empty pack for one that has never been saved', async () => {
    const { id } = await createPack('Fresh')
    const pack = await readPackForEdit(id)
    expect(pack).toEqual({ id, name: 'Fresh', schemaVersion: 1, cards: [] })
  })
})

describe('deletePack', () => {
  it('removes the pack and every version it had', async () => {
    const { id } = await createPack('Doomed')
    await savePack(id, 'Doomed', [card('a')])

    await deletePack(id)

    expect(await listPacks()).toHaveLength(0)
    expect(fs.existsSync(path.join(root, 'packs', id))).toBe(false)
  })

  it('is silent about a pack that is already gone', async () => {
    await expect(deletePack('never-existed')).resolves.toBeUndefined()
  })
})
