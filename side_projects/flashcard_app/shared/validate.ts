import type { Card } from './types'

/**
 * Validation is deliberately minimal (vision §8): a card is valid if it has a Front Text
 * and a Back Primary Text. More Info is never validated — it is where the depth lives and
 * the app has no opinion about it.
 *
 * This lives in `shared/` rather than beside the UI because it has two callers that must
 * agree: the editor, which marks incomplete cards live, and `savePack`, which is the
 * actual gate. A future MCP server writing packs goes through `savePack` too, so an
 * AI-authored pack is held to exactly the same standard as a hand-typed one.
 */

export type MissingField = 'front' | 'primary'

export type CardProblem = {
  cardId: string
  /** Position in the pack — what the editor needs to jump to the offending card. */
  index: number
  missing: MissingField[]
}

export type ValidationResult = { ok: true } | { ok: false; problems: CardProblem[] }

/** Whitespace is not content: a card of spaces is as empty as a card of nothing. */
export function isCardComplete(card: Card): boolean {
  return card.front.trim() !== '' && card.primary.trim() !== ''
}

export function validateCards(cards: Card[]): ValidationResult {
  const problems: CardProblem[] = []

  cards.forEach((card, index) => {
    const missing: MissingField[] = []
    if (card.front.trim() === '') missing.push('front')
    if (card.primary.trim() === '') missing.push('primary')
    if (missing.length) problems.push({ cardId: card.id, index, missing })
  })

  // An empty pack is valid — it simply stays hidden from Pack Selection until it has
  // cards, so there is nothing to protect the user from here.
  return problems.length ? { ok: false, problems } : { ok: true }
}
