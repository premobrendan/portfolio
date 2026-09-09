import type { Card } from '../shared/types'
import { createPack, deletePack, listPacks, readPack, savePack } from '../electron/storage'
import { isCardComplete } from '../shared/validate'
import { readPackMetaForTools } from './meta'

/**
 * An expected failure the model can act on — a missing pack, an incomplete card, an unsaved
 * draft in the way. `server.ts` renders these as `isError` results with the message intact,
 * so the model reads guidance rather than a stack trace.
 */
export class ToolError extends Error {}

export type CardInput = {
  front: string
  primary: string
  /** Further facts that compete with `primary` for the back of the card. Optional. */
  secondaries?: string[]
  moreInfo?: string
}

const toCard = (input: CardInput): Card => ({
  id: crypto.randomUUID(),
  front: input.front,
  primary: input.primary,
  // Preserve "absent" rather than storing an empty string: both optional fields are read
  // as present/absent — Learn gates Tab on More Info being non-empty, and the deck builder
  // only offers a coin flip when a secondary actually exists.
  ...(input.secondaries?.some(t => t.trim()) 
    ? { secondaries: input.secondaries.filter(t => t.trim() !== '') }
    : {}),
  ...(input.moreInfo ? { moreInfo: input.moreInfo } : {}),
})

async function requireMeta(packId: string) {
  const meta = await readPackMetaForTools(packId)
  if (!meta) throw new ToolError(`No pack with id "${packId}". Call list_packs to see what exists.`)
  return meta
}

/**
 * A save clears `draftVersion`, which would throw away whatever the user has open and unsaved
 * in Make a Pack. Two processes share this library and neither can see the other's intent, so
 * refuse rather than race; `force` is the deliberate override.
 */
async function guardDraft(packId: string, force: boolean | undefined) {
  const meta = await requireMeta(packId)
  if (meta.draftVersion !== undefined && !force) {
    throw new ToolError(
      `"${meta.name}" has unsaved changes open in the Flashcards editor. Saving now would `
      + 'discard them. Ask the user to save or leave that pack first, or retry with '
      + 'force: true to overwrite their unsaved work.',
    )
  }
  return meta
}

/**
 * Turns a validation failure into something the model can fix without another round trip.
 *
 * `indexOffset` exists because `add_cards` validates the appended list, so a problem's index
 * counts from the start of the whole pack. Reporting that number to a caller who sent three
 * cards would point at a card they never submitted.
 */
function commit(result: Awaited<ReturnType<typeof savePack>>, indexOffset = 0) {
  if (result.ok) return
  const detail = result.problems
    .map(p => `card ${p.index + 1 - indexOffset} is missing ${p.missing.join(' and ')}`)
    .join('; ')
  throw new ToolError(
    `${result.problems.length} card(s) incomplete — every card needs a front and a back. ${detail}`,
  )
}

export async function listPacksTool() {
  const metas = await listPacks()
  return {
    packs: metas.map(m => ({
      id: m.id,
      name: m.name,
      cardCount: m.cardCount,
      hasUnsavedDraft: m.draftVersion !== undefined,
    })),
  }
}

export async function readPackTool({ packId }: { packId: string }) {
  await requireMeta(packId)
  const pack = await readPack(packId)
  return { id: pack.id, name: pack.name, cards: pack.cards }
}

export async function createPackTool({ name }: { name: string }) {
  if (!name.trim()) throw new ToolError('A pack needs a name.')
  const meta = await createPack(name.trim())
  return { packId: meta.id, name: meta.name }
}

export async function addCardsTool(
  { packId, cards, force }: { packId: string; cards: CardInput[]; force?: boolean },
) {
  if (cards.length === 0) throw new ToolError('No cards supplied.')
  const meta = await guardDraft(packId, force)
  const existing = (await readPack(packId)).cards
  const created = cards.map(toCard)
  const next = [...existing, ...created]

  commit(await savePack(packId, meta.name, next), existing.length)
  // Return the ids so update_card is usable on what was just written, without paying for a
  // full read_pack to find them again.
  return {
    packId,
    added: cards.length,
    cardCount: next.length,
    cardIds: created.map(c => c.id),
  }
}

/**
 * Edit one card in place. The alternative — read the pack, change one field, write it all
 * back — pulls every card into the conversation to fix a typo, which is exactly what the
 * read_pack description warns against.
 *
 * Omitted fields are left alone. `moreInfo: ''` removes it, since Learn gates the Tab key on
 * More Info being non-empty and an empty string would leave a panel that opens onto nothing.
 */
export async function updateCardTool(
  { packId, cardId, front, primary, secondaries, moreInfo, force }: {
    packId: string
    cardId: string
    front?: string
    primary?: string
    secondaries?: string[]
    moreInfo?: string
    force?: boolean
  },
) {
  const meta = await guardDraft(packId, force)
  const cards = (await readPack(packId)).cards
  const index = cards.findIndex(c => c.id === cardId)
  if (index === -1) {
    throw new ToolError(
      `No card with id "${cardId}" in "${meta.name}". Call read_pack to see its cards and `
      + 'their ids.',
    )
  }

  const current = cards[index]
  const patched: Card = {
    ...current,
    ...(front !== undefined ? { front } : {}),
    ...(primary !== undefined ? { primary } : {}),
  }
  // Both optional fields follow the same rule: omit to leave alone, empty to remove.
  if (secondaries !== undefined) {
    const kept = secondaries.filter(t => t.trim() !== '')
    if (kept.length) patched.secondaries = kept
    else delete patched.secondaries
  }
  if (moreInfo !== undefined) {
    if (moreInfo) patched.moreInfo = moreInfo
    else delete patched.moreInfo
  }

  // Checked here rather than left to `commit` so the message names the card the caller
  // actually touched, instead of its position among all the others.
  if (!isCardComplete(patched)) {
    const missing = patched.front.trim() === '' ? 'front' : 'back'
    throw new ToolError(
      `That change would leave the card with no ${missing}. Every card needs a front and a `
      + 'back; nothing was written.',
    )
  }

  const next = cards.map((c, i) => (i === index ? patched : c))
  commit(await savePack(packId, meta.name, next))
  return { packId, cardId, card: patched }
}

/** Replaces every card. Named for what it destroys, not for what it saves. */
export async function replaceAllCardsTool(
  { packId, cards, name, force }:
  { packId: string; cards: CardInput[]; name?: string; force?: boolean },
) {
  const meta = await guardDraft(packId, force)
  const finalName = name?.trim() || meta.name
  const next = cards.map(toCard)

  commit(await savePack(packId, finalName, next))
  return { packId, name: finalName, cardCount: next.length }
}

export async function deletePackTool({ packId, confirm }: { packId: string; confirm: string }) {
  const meta = await requireMeta(packId)
  // Mirrors the editor's type-DELETE gate: an irreversible action should never be one
  // malformed tool call away.
  if (confirm !== 'DELETE') {
    throw new ToolError(
      `Refusing to delete "${meta.name}". Pass confirm: "DELETE" if the user has actually `
      + 'asked for this pack and all its backups to be destroyed.',
    )
  }
  await deletePack(packId)
  return { packId, deleted: true as const }
}
