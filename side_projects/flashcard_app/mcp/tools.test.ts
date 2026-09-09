import { describe, it, expect, beforeEach } from 'vitest'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { setStorageRoot, readPack, readPackForEdit, writeDraft } from '../electron/storage'
import {
  ToolError, listPacksTool, readPackTool, createPackTool,
  addCardsTool, replaceAllCardsTool, updateCardTool, deletePackTool,
} from './tools'

beforeEach(() => {
  setStorageRoot(fs.mkdtempSync(path.join(os.tmpdir(), 'fc-mcp-')))
})

const card = (front: string, primary: string) => ({ front, primary })

/** A two-card pack plus the ids of its cards — the shape most update tests need. */
async function seedTwo() {
  const { packId } = await createPackTool({ name: 'Capitals' })
  const { cardIds } = await addCardsTool({
    packId, cards: [card('France', 'Paris'), card('Japan', 'Tokyo')],
  })
  return { packId, cardIds }
}

describe('create_pack', () => {
  it('creates an empty pack and returns its id', async () => {
    const { packId, name } = await createPackTool({ name: 'Capitals' })
    expect(name).toBe('Capitals')
    expect((await listPacksTool()).packs).toHaveLength(1)
    expect((await readPack(packId)).cards).toEqual([])
  })

  it('rejects a blank name', async () => {
    await expect(createPackTool({ name: '   ' })).rejects.toThrow(ToolError)
  })
})

describe('add_cards', () => {
  it('appends to the saved pack and saves', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    const result = await addCardsTool({ packId, cards: [card('France', 'Paris')] })
    expect(result).toMatchObject({ packId, added: 1, cardCount: 1 })
    expect(result.cardIds).toHaveLength(1)

    await addCardsTool({ packId, cards: [card('Japan', 'Tokyo'), card('Peru', 'Lima')] })
    expect((await readPack(packId)).cards).toHaveLength(3)
  })

  it('gives every card a server-generated id', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await addCardsTool({ packId, cards: [card('France', 'Paris')] })
    const [only] = (await readPack(packId)).cards
    expect(only.id).toMatch(/[0-9a-f-]{36}/)
  })

  it('carries More Info through untouched', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await addCardsTool({
      packId,
      cards: [{ front: 'France', primary: 'Paris', moreInfo: '# Paris\n\nPop 2.1M.' }],
    })
    expect((await readPack(packId)).cards[0].moreInfo).toBe('# Paris\n\nPop 2.1M.')
  })

  it('numbers a bad card by its place in the submitted batch, not the whole pack', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await addCardsTool({ packId, cards: [card('France', 'Paris'), card('Japan', 'Tokyo')] })

    // The caller sent one card; telling them "card 3" would point at nothing they can see.
    await expect(addCardsTool({ packId, cards: [card('Peru', '')] }))
      .rejects.toThrow(/card 1 is missing primary/)
  })

  it('refuses an incomplete card and writes nothing', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await expect(addCardsTool({ packId, cards: [card('France', '  ')] }))
      .rejects.toThrow(/incomplete/i)
    expect((await readPack(packId)).cards).toHaveLength(0)
  })

  it('refuses to clobber an unsaved draft', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await writeDraft(packId, 'Capitals', [{ id: 'd', front: 'draft', primary: 'work' }])

    await expect(addCardsTool({ packId, cards: [card('France', 'Paris')] }))
      .rejects.toThrow(/unsaved/i)
    expect((await readPackForEdit(packId)).cards[0].id).toBe('d')
  })

  it('overrides the draft guard when forced', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await writeDraft(packId, 'Capitals', [{ id: 'd', front: 'draft', primary: 'work' }])

    await addCardsTool({ packId, cards: [card('France', 'Paris')], force: true })
    expect((await readPack(packId)).cards).toHaveLength(1)
  })

  it('rejects an unknown pack', async () => {
    await expect(addCardsTool({ packId: 'nope', cards: [card('a', 'b')] }))
      .rejects.toThrow(ToolError)
  })
})

describe('add_cards return value', () => {
  it('reports the ids it created, in the order supplied', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    const { cardIds } = await addCardsTool({
      packId, cards: [card('France', 'Paris'), card('Japan', 'Tokyo')],
    })

    expect(cardIds).toHaveLength(2)
    // Usable directly: no read_pack round trip to find what you just wrote.
    const saved = (await readPack(packId)).cards
    expect(saved.map(c => c.id)).toEqual(cardIds)
  })
})

describe('update_card', () => {
  it('changes one field and leaves the rest alone', async () => {
    const { packId, cardIds } = await seedTwo()
    await updateCardTool({ packId, cardId: cardIds[0], primary: 'Paris, France' })

    const [first, second] = (await readPack(packId)).cards
    expect(first).toMatchObject({ front: 'France', primary: 'Paris, France' })
    expect(second).toMatchObject({ front: 'Japan', primary: 'Tokyo' })
  })

  it('adds More Info to a card that had none', async () => {
    const { packId, cardIds } = await seedTwo()
    await updateCardTool({ packId, cardId: cardIds[1], moreInfo: '## Tokyo\n\nFormerly Edo.' })
    expect((await readPack(packId)).cards[1].moreInfo).toBe('## Tokyo\n\nFormerly Edo.')
  })

  it('clears More Info when passed an empty string', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    const { cardIds } = await addCardsTool({
      packId, cards: [{ front: 'France', primary: 'Paris', moreInfo: 'delete me' }],
    })

    await updateCardTool({ packId, cardId: cardIds[0], moreInfo: '' })
    expect((await readPack(packId)).cards[0].moreInfo).toBeUndefined()
  })

  it('refuses a change that would leave the card unstudiable', async () => {
    const { packId, cardIds } = await seedTwo()
    await expect(updateCardTool({ packId, cardId: cardIds[0], primary: '  ' }))
      .rejects.toThrow(/front and a back/i)
    expect((await readPack(packId)).cards[0].primary).toBe('Paris')
  })

  it('names read_pack when the card id is unknown', async () => {
    const { packId } = await seedTwo()
    await expect(updateCardTool({ packId, cardId: 'not-a-card', primary: 'x' }))
      .rejects.toThrow(/read_pack/)
  })

  it('respects the unsaved-draft guard', async () => {
    const { packId, cardIds } = await seedTwo()
    await writeDraft(packId, 'Capitals', [{ id: 'd', front: 'draft', primary: 'work' }])
    await expect(updateCardTool({ packId, cardId: cardIds[0], primary: 'x' }))
      .rejects.toThrow(/unsaved/i)
  })
})

describe('secondary texts', () => {
  it('accepts several, and drops blank entries', async () => {
    const { packId } = await createPackTool({ name: 'Presidents' })
    await addCardsTool({ packId, cards: [{
      front: 'Lincoln', primary: '16th', secondaries: ['Freed the slaves.', '  ', 'Gettysburg.'],
    }] })
    expect((await readPack(packId)).cards[0].secondaries)
      .toEqual(['Freed the slaves.', 'Gettysburg.'])
  })

  it('is stored when supplied', async () => {
    const { packId } = await createPackTool({ name: 'Presidents' })
    await addCardsTool({ packId, cards: [{
      front: 'Abraham Lincoln',
      primary: 'He was the 16th president.',
      secondaries: ['He issued the Emancipation Proclamation.'],
    }] })
    expect((await readPack(packId)).cards[0].secondaries)
      .toEqual(['He issued the Emancipation Proclamation.'])
  })

  it('is absent rather than empty when not supplied', async () => {
    const { packId } = await createPackTool({ name: 'Presidents' })
    await addCardsTool({ packId, cards: [card('France', 'Paris')] })
    expect((await readPack(packId)).cards[0]).not.toHaveProperty('secondaries')
  })

  it('is never required — a card with only a primary still saves', async () => {
    const { packId } = await createPackTool({ name: 'Presidents' })
    await expect(addCardsTool({ packId, cards: [card('a', 'b')] })).resolves.toBeTruthy()
  })

  it('can be added to an existing card, and cleared with an empty string', async () => {
    const { packId } = await createPackTool({ name: 'Presidents' })
    const { cardIds } = await addCardsTool({ packId, cards: [card('Lincoln', '16th')] })

    await updateCardTool({ packId, cardId: cardIds[0], secondaries: ['Freed the slaves.'] })
    expect((await readPack(packId)).cards[0].secondaries).toEqual(['Freed the slaves.'])

    await updateCardTool({ packId, cardId: cardIds[0], secondaries: [] })
    expect((await readPack(packId)).cards[0]).not.toHaveProperty('secondaries')
  })
})

describe('replace_all_cards', () => {
  it('replaces the whole card list', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await addCardsTool({ packId, cards: [card('France', 'Paris'), card('Japan', 'Tokyo')] })

    await replaceAllCardsTool({ packId, cards: [card('Peru', 'Lima')] })
    const live = await readPack(packId)
    expect(live.cards).toHaveLength(1)
    expect(live.cards[0].front).toBe('Peru')
  })

  it('renames the pack when asked', async () => {
    const { packId } = await createPackTool({ name: 'Old' })
    await replaceAllCardsTool({ packId, cards: [card('France', 'Paris')], name: 'New' })
    expect((await readPack(packId)).name).toBe('New')
  })
})

describe('list_packs', () => {
  it('returns summaries, never card bodies', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await addCardsTool({ packId, cards: [card('France', 'Paris')] })

    const [summary] = (await listPacksTool()).packs
    expect(summary).toEqual({
      id: packId, name: 'Capitals', cardCount: 1, hasUnsavedDraft: false,
    })
  })

  it('flags a pack the user has unsaved work in', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await writeDraft(packId, 'Capitals', [{ id: 'd', front: 'x', primary: 'y' }])
    expect((await listPacksTool()).packs[0].hasUnsavedDraft).toBe(true)
  })
})

describe('read_pack', () => {
  it('returns the saved cards', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await addCardsTool({ packId, cards: [card('France', 'Paris')] })
    expect((await readPackTool({ packId })).cards[0].front).toBe('France')
  })
})

describe('delete_pack', () => {
  it('requires the confirmation word', async () => {
    const { packId } = await createPackTool({ name: 'Doomed' })
    await expect(deletePackTool({ packId, confirm: 'yes' })).rejects.toThrow(/DELETE/)
    expect((await listPacksTool()).packs).toHaveLength(1)
  })

  it('deletes when confirmed', async () => {
    const { packId } = await createPackTool({ name: 'Doomed' })
    expect(await deletePackTool({ packId, confirm: 'DELETE' })).toEqual({ packId, deleted: true })
    expect((await listPacksTool()).packs).toHaveLength(0)
  })
})
