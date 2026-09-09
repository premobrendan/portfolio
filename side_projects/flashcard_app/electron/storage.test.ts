import { describe, it, expect, beforeEach } from 'vitest'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import {
  setStorageRoot, listPacks, readPack, writePackVersion,
  readState, writeState, readSettings, writeSettings,
} from './storage'

let root: string
beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), 'fc-store-'))
  setStorageRoot(root)
})

describe('state', () => {
  it('returns defaults when state.json is absent', async () => {
    expect(await readState()).toEqual({ selectedPackIds: [], hasLaunchedBefore: false })
  })

  it('rewrites defaults when state.json is malformed', async () => {
    fs.writeFileSync(path.join(root, 'state.json'), '{ not json')
    expect(await readState()).toEqual({ selectedPackIds: [], hasLaunchedBefore: false })
  })

  it('persists a shallow patch', async () => {
    await writeState({ selectedPackIds: ['a'] })
    await writeState({ hasLaunchedBefore: true })
    expect(await readState()).toEqual({ selectedPackIds: ['a'], hasLaunchedBefore: true })
  })
})

describe('packs', () => {
  it('lists nothing when packs/ is absent', async () => {
    expect(await listPacks()).toEqual([])
  })

  it('writes v1 and lists it without reading card data', async () => {
    await writePackVersion('demo', 'Demo', [{ id: 'c1', front: 'F', primary: 'P' }])
    expect(await listPacks()).toEqual([
      { id: 'demo', name: 'Demo', cardCount: 1, currentVersion: 1, schemaVersion: 1 },
    ])
  })

  it('reads back the current version', async () => {
    await writePackVersion('demo', 'Demo', [{ id: 'c1', front: 'F', primary: 'P' }])
    const pack = await readPack('demo')
    expect(pack.cards[0].front).toBe('F')
  })

  it('increments the version and reads the newest', async () => {
    await writePackVersion('demo', 'Demo', [{ id: 'c1', front: 'one', primary: 'P' }])
    await writePackVersion('demo', 'Demo', [{ id: 'c1', front: 'two', primary: 'P' }])
    const metas = await listPacks()
    expect(metas[0].currentVersion).toBe(2)
    expect((await readPack('demo')).cards[0].front).toBe('two')
  })

  it('skips a malformed pack directory and still lists the good ones', async () => {
    await writePackVersion('good', 'Good', [{ id: 'c1', front: 'F', primary: 'P' }])
    fs.mkdirSync(path.join(root, 'packs', 'broken'), { recursive: true })
    fs.writeFileSync(path.join(root, 'packs', 'broken', 'meta.json'), '{ nope')
    const metas = await listPacks()
    expect(metas.map(m => m.id)).toEqual(['good'])
  })
})

describe('settings', () => {
  it('defaults to the dynamic space theme, studied front-first', async () => {
    expect(await readSettings()).toEqual({
      themeId: 'space', themeMode: 'dynamic', cardDirection: 'forward', backView: 'primary',
    })
  })

  it('gives a settings file written before card direction existed the default', async () => {
    // DEFAULT_SETTINGS is spread under whatever is on disk, so an old profile gains the
    // new key on read rather than needing a migration.
    await writeSettings({ themeId: 'space' })
    expect((await readSettings()).cardDirection).toBe('forward')
  })
})

describe('migrating older data', () => {
  it('folds a legacy single `secondary` into the secondaries list', async () => {
    // Cards written before secondaries existed — the presidents pack was 45 of them.
    const dir = path.join(root, 'packs', 'legacy')
    fs.mkdirSync(dir, { recursive: true })
    fs.writeFileSync(path.join(dir, 'v1.json'), JSON.stringify({
      name: 'Legacy',
      cards: [{ id: 'x', front: 'Lincoln', primary: '16th', secondary: 'Freed the slaves.' }],
    }))
    fs.writeFileSync(path.join(dir, 'meta.json'), JSON.stringify({
      id: 'legacy', name: 'Legacy', cardCount: 1, currentVersion: 1, schemaVersion: 1,
    }))

    const [card] = (await readPack('legacy')).cards
    expect(card.secondaries).toEqual(['Freed the slaves.'])
    // The old key does not survive alongside the new one.
    expect(card).not.toHaveProperty('secondary')
  })

  it('leaves a card that already uses secondaries alone', async () => {
    const dir = path.join(root, 'packs', 'modern')
    fs.mkdirSync(dir, { recursive: true })
    fs.writeFileSync(path.join(dir, 'v1.json'), JSON.stringify({
      name: 'Modern',
      cards: [{ id: 'y', front: 'a', primary: 'b', secondaries: ['c', 'd'] }],
    }))
    fs.writeFileSync(path.join(dir, 'meta.json'), JSON.stringify({
      id: 'modern', name: 'Modern', cardCount: 1, currentVersion: 1, schemaVersion: 1,
    }))

    expect((await readPack('modern')).cards[0].secondaries).toEqual(['c', 'd'])
  })

  it('maps a retired `mixed` direction onto random rather than breaking', async () => {
    await writeSettings({ cardDirection: 'mixed' as never })
    expect((await readSettings()).cardDirection).toBe('random')
  })

  it('defaults backView for a profile written before it existed', async () => {
    await writeSettings({ themeId: 'space' })
    expect((await readSettings()).backView).toBe('primary')
  })
})
