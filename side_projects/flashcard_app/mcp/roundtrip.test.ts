import { describe, it, expect, beforeEach } from 'vitest'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { setStorageRoot, listPacks, readPack } from '../electron/storage'
import { addCardsTool, createPackTool } from './tools'

let root: string
beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), 'fc-round-'))
  setStorageRoot(root)
})

describe('a pack written through the MCP tools', () => {
  it('is on disk in the layout the app reads', async () => {
    const { packId } = await createPackTool({ name: 'From Claude' })
    await addCardsTool({
      packId,
      cards: [
        { front: 'France', primary: 'Paris', moreInfo: 'Pop 2.1M.' },
        { front: 'Japan', primary: 'Tokyo' },
      ],
    })

    // The exact files the Electron app expects.
    expect(fs.existsSync(path.join(root, 'packs', packId, 'meta.json'))).toBe(true)

    const [meta] = await listPacks()
    expect(meta).toMatchObject({ id: packId, name: 'From Claude', cardCount: 2 })
    // A model's write is a SAVE, never a draft — it goes live, and leaves no draft behind.
    expect(meta.draftVersion).toBeUndefined()

    const pack = await readPack(packId)
    expect(pack.cards.map(c => c.front)).toEqual(['France', 'Japan'])
    expect(pack.cards[0].moreInfo).toBe('Pop 2.1M.')
    expect(pack.cards[1].moreInfo).toBeUndefined()
  })

  it('is immediately studyable — cardCount is what gates Pack Selection', async () => {
    const { packId } = await createPackTool({ name: 'From Claude' })
    await addCardsTool({ packId, cards: [{ front: 'a', primary: 'b' }] })
    expect((await listPacks()).filter(p => p.cardCount > 0).map(p => p.id)).toContain(packId)
  })
})
