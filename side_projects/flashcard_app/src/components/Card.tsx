import { useEffect, useRef, useState, type TransitionEvent } from 'react'
import type { Card as CardType } from '../../shared/types'
import '../styles/card.css'

type Props = {
  card: CardType
  /** Bumped by the parent to request a flip from the keyboard. */
  flipSignal?: number
  onFirstFlip?: () => void
  onOpenMoreInfo?: () => void
  /**
   * Offscreen pre-mount of the next card. Identical DOM, but a different test id so
   * `getByTestId('card')` resolves to exactly one element, and hidden from assistive
   * tech. The parent still wraps it in `.card-preload` to take it out of view.
   */
  preload?: boolean
}

/**
 * Flip state is LOCAL — it never enters the app reducer, so flipping does not
 * re-render the screen tree.
 *
 * Keying the stage on the card id makes React throw the whole stage away when the
 * card changes: flip state, first-flip tracking, and the DOM node reset together.
 * A freshly created element never runs a transition, so the next card appears
 * front-up instantly. (Resetting `flipped` in an effect instead would commit the
 * new card's text first and then animate the back face — its answer — into hiding.)
 */
export default function Card(props: Props) {
  return <CardStage key={props.card.id} {...props} />
}

function CardStage({ card, flipSignal = 0, onFirstFlip, onOpenMoreInfo, preload = false }: Props) {
  const [flipped, setFlipped] = useState(false)
  const innerRef = useRef<HTMLDivElement>(null)
  const hasFlippedRef = useRef(false)
  // The signal value this instance was born with is its baseline; only a change
  // after mount is a request. Survives StrictMode's mount/unmount/mount.
  const seenSignal = useRef(flipSignal)

  const flip = () => {
    // Promote the layer in the same style pass that starts the transition. Kept
    // out of React state so the lifecycle costs no renders and cannot be clobbered
    // by a parent re-render mid-flip.
    innerRef.current?.style.setProperty('will-change', 'transform')
    setFlipped(f => !f)
    if (!hasFlippedRef.current) {
      hasFlippedRef.current = true
      onFirstFlip?.()
    }
  }

  // Keyboard-driven flips arrive as a bumped number, so flip state stays in here.
  // `flip` is deliberately not a dependency: it is recreated every render and the
  // effect must run once per signal change, not once per render.
  useEffect(() => {
    if (flipSignal === seenSignal.current) return
    seenSignal.current = flipSignal
    flip()
  }, [flipSignal])

  // `transitionend` bubbles. Only the inner's own transform transition ending may
  // drop the layer — a child's transition finishing mid-flip must not demote it.
  const onTransitionEnd = (e: TransitionEvent<HTMLDivElement>) => {
    if (e.target !== e.currentTarget || e.propertyName !== 'transform') return
    e.currentTarget.style.removeProperty('will-change')
  }

  return (
    <div
      className="card-stage"
      data-testid={preload ? 'card-preload' : 'card'}
      data-card-id={card.id}
      data-flipped={flipped ? 'true' : 'false'}
      aria-hidden={preload || undefined}
    >
      <div
        ref={innerRef}
        className="card-inner"
        data-testid={preload ? 'card-preload-inner' : 'card-inner'}
        data-flipped={flipped ? 'true' : 'false'}
        onTransitionEnd={onTransitionEnd}
        onClick={flip}
      >
        <div className="card-face">
          <div className="card-face__front-text">{card.front}</div>
        </div>

        <div className="card-face card-face--back">
          <div className="card-face__primary">{card.primary}</div>

          {/* Rendered only when there is actually something to show. */}
          {card.moreInfo ? (
            <button
              type="button"
              className="card-face__more"
              data-testid="more-info-btn"
              onClick={e => {
                // The one click on the card that does not flip it.
                e.stopPropagation()
                // Drop focus so a later Space is a flip, not a second button press.
                e.currentTarget.blur()
                onOpenMoreInfo?.()
              }}
            >
              More Info
            </button>
          ) : null}
        </div>
      </div>
    </div>
  )
}
