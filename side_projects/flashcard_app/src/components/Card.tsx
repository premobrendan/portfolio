import { useEffect, useMemo, useRef, useState, type TransitionEvent } from 'react'
import { backText, frontText, viewKey, type CardView } from '../lib/deck'
import { renderMarkdown } from '../lib/markdown'
import '../styles/card.css'

type Props = {
  /** Which card, which way round, and which of its facts. See lib/deck.ts. */
  view: CardView
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
 * Keying the stage on the VIEW makes React throw the whole stage away when the
 * card changes: flip state, first-flip tracking, and the DOM node reset together.
 * It must be the view rather than the card id, because one card can appear twice in a
 * session (mixed mode) and a shared key would reuse the instance — arriving already
 * flipped, showing its answer.
 * A freshly created element never runs a transition, so the next card appears
 * front-up instantly. (Resetting `flipped` in an effect instead would commit the
 * new card's text first and then animate the back face — its answer — into hiding.)
 */
export default function Card(props: Props) {
  return <CardStage key={viewKey(props.view)} {...props} />
}

function CardStage({ view, flipSignal = 0, onFirstFlip, onOpenMoreInfo, preload = false }: Props) {
  const card = view.card
  /**
   * Faces are Markdown, so a card can ask a question with a picture or a table — a flag
   * on the front and the country on the back. Parsed here and memoised per face: it is a
   * few microseconds for the plain-text cards that are the overwhelming majority, but it
   * must not run again mid-flip.
   *
   * Sanitised by `renderMarkdown`, the same gate More Info uses. Card content is
   * untrusted — a pack can be written by the MCP server, i.e. by a model.
   */
  const frontHtml = useMemo(() => renderMarkdown(frontText(view)), [view])
  const backHtml = useMemo(() => renderMarkdown(backText(view)), [view])

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
          <div className="card-face__front-text" dangerouslySetInnerHTML={{ __html: frontHtml }} />
        </div>

        <div className="card-face card-face--back">
          <div className="card-face__primary" dangerouslySetInnerHTML={{ __html: backHtml }} />

          {/* Rendered only when there is something to show AND somewhere to show it:
              pack preview reuses this card without a handler, and a control that
              silently does nothing is worse than no control. */}
          {card.moreInfo && onOpenMoreInfo ? (
            <button
              type="button"
              className="card-face__more"
              // The preload instance must not collide with the live card's id, and a
              // focusable button inside an aria-hidden subtree is a tab trap.
              data-testid={preload ? 'more-info-btn-preload' : 'more-info-btn'}
              tabIndex={preload ? -1 : undefined}
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
