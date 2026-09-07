import { useCallback, useEffect, useRef, useState } from 'react'
import type { Card as CardType } from '../../shared/types'
import CardView from '../components/Card'
import AdvanceArrow from '../components/AdvanceArrow'
import LeaveButton from '../components/LeaveButton'
import { shuffle } from '../lib/shuffle'
import '../styles/learn.css'

type Props = {
  packIds: string[]
  /** Bumped by the parent when Space is pressed. Forwarded to the card. */
  flipSignal: number
  /** Bumped by the parent when Enter is pressed after a flip. */
  advanceSignal: number
  /** MUST be a stable callback in the parent — it is called from an effect. */
  onCardContext: (card: CardType | null, hasFlipped: boolean) => void
  onOpenMoreInfo: () => void
  onRequestLeave: () => void
}

/**
 * Everything a session is, in one value. `deck`, `index`, and `hasFlipped` always
 * change together (on entry, on reshuffle, on advance), so they live together: one
 * commit per transition and never a render where the deck exists but the index does
 * not yet point into it. Flip state itself stays inside `Card`.
 */
type Session = { deck: CardType[]; index: number; hasFlipped: boolean }

const freshSession = (cards: CardType[]): Session =>
  ({ deck: shuffle(cards), index: 0, hasFlipped: false })

export default function LearnScreen({
  packIds, flipSignal, advanceSignal, onCardContext, onOpenMoreInfo, onRequestLeave,
}: Props) {
  const [session, setSession] = useState<Session | null>(null)

  // Load every selected pack ONCE, up front, and shuffle the merged pool. Keyed on the
  // ids' content, not the array's identity, so a parent re-render can never trigger a
  // second read: nothing touches disk mid-session. (`packIds` inside the closure is
  // the array the key was derived from, so the two cannot disagree.)
  const packKey = packIds.join('\0')
  useEffect(() => {
    let cancelled = false
    void (async () => {
      const packs = await Promise.all(packIds.map(id => window.flashcards.packs.read(id)))
      if (!cancelled) setSession(freshSession(packs.flatMap(p => p.cards)))
    })()
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- keyed on content, see above
  }, [packKey])

  const current = session?.deck[session.index] ?? null
  const next = session?.deck[session.index + 1] ?? null
  const hasFlipped = session?.hasFlipped ?? false

  // Tell the parent which card is up and whether it has been flipped, so the keyboard
  // gate for Enter/Tab is answered from the same truth the arrow uses. The parent's
  // handler bails out when nothing changed, so this can never loop.
  useEffect(() => { onCardContext(current, hasFlipped) }, [current, hasFlipped, onCardContext])

  // "Has this card EVER been flipped" — set once per card, never cleared by a flip back.
  const markFlipped = useCallback(() => {
    setSession(s => (s && !s.hasFlipped ? { ...s, hasFlipped: true } : s))
  }, [])

  // The gate is re-checked against the latest state inside the updater, so a stale
  // closure can never advance an unflipped card.
  const advance = useCallback(() => {
    setSession(s => (s && s.hasFlipped ? { ...s, index: s.index + 1, hasFlipped: false } : s))
  }, [])

  // The signal value this instance mounted with is its baseline; only a change after
  // mount is a request. The parent's counter persists across Learn sessions, so a plain
  // `> 0` check would fire once on every re-entry.
  const seenAdvance = useRef(advanceSignal)
  useEffect(() => {
    if (advanceSignal === seenAdvance.current) return
    seenAdvance.current = advanceSignal
    advance()
  }, [advanceSignal, advance])

  // Same pool, new order. Shuffled outside the updater: StrictMode runs updaters twice.
  const reshuffle = () => { if (session) setSession(freshSession(session.deck)) }

  if (!session) return <div className="learn" data-testid="learn-loading" />

  if (!current) {
    return (
      <div className="learn" data-testid="screen-learn">
        <LeaveButton onLeave={onRequestLeave} label="Return to main menu" />
        <div className="session-end" data-testid="session-end">
          <h2>You&rsquo;ve reached the end of the deck.</h2>
          <div className="session-end__actions">
            <button type="button" data-testid="reshuffle-btn" onClick={reshuffle}>Reshuffle</button>
            <button type="button" data-testid="end-leave-btn" onClick={onRequestLeave}>Main Menu</button>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="learn" data-testid="screen-learn">
      <LeaveButton onLeave={onRequestLeave} label="Return to main menu" />

      {/* `key={index}` guards the merged deck: card ids are unique only WITHIN a pack,
          so two selected packs can both contain "c1". Keying on position guarantees a
          remount (and therefore a front-face reset) on every advance regardless. */}
      <CardView
        key={session.index}
        card={current}
        flipSignal={flipSignal}
        onFirstFlip={markFlipped}
        onOpenMoreInfo={onOpenMoreInfo}
      />

      {/* Pre-mounted so advancing swaps an element rather than mounting one. `preload`
          is REQUIRED: it renames the test ids and hides the copy from assistive tech, so
          `getByTestId('card')` resolves to exactly one node. */}
      {next && (
        <div className="card-preload" aria-hidden="true">
          <CardView card={next} preload />
        </div>
      )}

      {hasFlipped && <AdvanceArrow onAdvance={advance} />}
    </div>
  )
}
