import type { BackView, Card, CardDirection } from '../../shared/types'

/**
 * One appearance of a card in a session: which card, which way round, and which of its
 * backs is in play.
 *
 * A view holds a REFERENCE to the card, never a copy. Building decks by cloning cards
 * would duplicate every card's More Info — on a pack carrying images and tables that is
 * megabytes of Markdown copied to change which string faces forward.
 */
export type CardView = {
  card: Card
  direction: 'forward' | 'reverse'
  /** Index into `backTexts(card)`. 0 is always the primary. */
  textIndex: number
}

/**
 * Every back the card can show, primary first.
 *
 * Blank secondaries are dropped rather than offered: an empty entry would produce a card
 * whose answer side is nothing at all, and secondaries are never validated on save, so
 * this is the place that has to be defensive.
 */
export function backTexts(card: Card): string[] {
  const extra = (card.secondaries ?? []).filter(t => t.trim() !== '')
  return [card.primary, ...extra]
}

/**
 * A stable identity for React. Derived from the view, so the card's stored id is left
 * alone — an earlier version mangled ids to dodge a key collision, which meant changing
 * saved data to solve a rendering problem.
 */
export const viewKey = (view: CardView) =>
  `${view.card.id}:${view.direction}:${view.textIndex}`

/** The chosen back, falling back to the primary if the index no longer fits the card. */
const factOf = (view: CardView) => {
  const texts = backTexts(view.card)
  return texts[view.textIndex] ?? texts[0]
}

export const frontText = (view: CardView) =>
  view.direction === 'forward' ? view.card.front : factOf(view)

export const backText = (view: CardView) =>
  view.direction === 'forward' ? factOf(view) : view.card.front

/**
 * Expands a pool of cards into the views a session walks through — exactly one per card,
 * in every mode, so a session is always as long as the pack.
 *
 * `rng` is injectable so tests can pin the coin flips, matching `shuffle`.
 */
export function buildDeck(
  cards: Card[],
  direction: CardDirection,
  backView: BackView,
  rng: () => number = Math.random,
): CardView[] {
  return cards.map(card => {
    const texts = backTexts(card)
    return {
      card,
      direction: direction === 'random'
        ? (rng() < 0.5 ? 'forward' : 'reverse')
        : direction,
      // A choice only means something when the card actually has more than one back.
      textIndex: backView === 'random' && texts.length > 1
        ? Math.min(Math.floor(rng() * texts.length), texts.length - 1)
        : 0,
    }
  })
}
