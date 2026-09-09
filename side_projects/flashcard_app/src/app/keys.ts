import type { CardView } from '../lib/deck'
import type { UI } from './machine'
import { topLayer } from './machine'

export type KeyIntent =
  | { type: 'flip' }
  | { type: 'advance' }
  | { type: 'openMoreInfo' }
  | { type: 'closeMoreInfo' }
  | { type: 'scrollMoreInfo'; direction: 'up' | 'down' }
  | { type: 'savePack' }
  | { type: 'blurField' }
  | { type: 'requestLeave' }
  | { type: 'confirmLeave' }
  | { type: 'denyLeave' }
  | null

/**
 * What the active screen knows that the key grammar needs. `hasFlipped`/`card` answer
 * Learn's gates; `editingField` answers the editor's — Escape means "leave this text
 * field" while typing and "leave this screen" otherwise.
 */
export type KeyContext = {
  hasFlipped: boolean
  view: CardView | null
  editingField: boolean
}

/**
 * Whether holding the key down should keep firing this intent. Continuous scrolling is
 * the only place auto-repeat is the point; everywhere else leaning on a key is either
 * dangerous or meaningless — held Escape would walk confirm → leave → quit, held Space
 * would flip the card over and over, held Enter would race through the deck, and held
 * Tab would toggle More Info open and shut. Kept here beside `keyActionFor` so the
 * scroll keys stay enumerated in exactly one file.
 */
export function repeatable(intent: KeyIntent): boolean {
  return intent?.type === 'scrollMoreInfo'
}

/**
 * Maps a key to an intent for the TOP layer only. Anything not listed for the
 * active layer is swallowed, which is what stops Space flipping the card
 * underneath an open dialog.
 */
export function keyActionFor(
  ui: UI,
  key: string,
  ctx: KeyContext,
  /** Cmd on macOS, Ctrl elsewhere. Only the editor's save gesture reads it. */
  mod = false,
): KeyIntent {
  const layer = topLayer(ui)

  if (layer?.kind === 'confirm') {
    if (key === 'Escape') return { type: 'confirmLeave' }
    if (key === ' ' || key === 'Enter') return { type: 'denyLeave' }
    return null
  }

  if (layer?.kind === 'moreInfo') {
    if (key === 'Escape' || key === 'Tab') return { type: 'closeMoreInfo' }
    if (key === 'ArrowUp' || key === 'w' || key === 'W')
      return { type: 'scrollMoreInfo', direction: 'up' }
    if (key === 'ArrowDown' || key === 's' || key === 'S')
      return { type: 'scrollMoreInfo', direction: 'down' }
    return null
  }

  if (ui.screen === 'makepackEdit') {
    if (mod) return key === 's' || key === 'S' ? { type: 'savePack' } : null
    // Escape pops exactly one thing here too: the field first, the screen second.
    if (key === 'Escape') return ctx.editingField ? { type: 'blurField' } : { type: 'requestLeave' }
    // Everything else is a character the user is typing.
    return null
  }

  if (ui.screen === 'learn') {
    if (key === ' ') return { type: 'flip' }
    if (key === 'Enter') return ctx.hasFlipped ? { type: 'advance' } : null
    // Gated on BOTH a prior flip and the card actually having More Info.
    if (key === 'Tab')
      return ctx.hasFlipped && ctx.view?.card.moreInfo ? { type: 'openMoreInfo' } : null
    if (key === 'Escape') return { type: 'requestLeave' }
    return null
  }

  if (key === 'Escape') return { type: 'requestLeave' }
  return null
}
