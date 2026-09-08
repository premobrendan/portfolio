import { useEffect, useRef } from 'react'
import '../styles/moreinfo.css'

/**
 * A scroll request from the single key listener. `n` is a counter rather than a boolean
 * so that pressing the same key twice is two events; `continuous` is true only for an
 * auto-repeat, which scrolls in smaller instant steps so a held key glides instead of
 * queueing up a backlog of smooth animations.
 */
export type ScrollRequest = { n: number; dir: 'up' | 'down'; continuous: boolean }

export const NO_SCROLL: ScrollRequest = { n: 0, dir: 'down', continuous: false }

const STEP = 120
const REPEAT_STEP = 40

type Props = {
  /** Already parsed and sanitized — see the note in App about when that happens. */
  html: string
  onClose: () => void
  scroll: ScrollRequest
}

export default function MoreInfoPanel({ html, onClose, scroll }: Props) {
  const bodyRef = useRef<HTMLDivElement>(null)

  // The request this instance mounted with is its baseline; only a change after mount is
  // a scroll. The counter outlives the panel, so a plain `> 0` check would make every
  // reopen jump. (Also what makes StrictMode's double-effect a no-op.)
  const seen = useRef(scroll.n)
  useEffect(() => {
    if (scroll.n === seen.current) return
    seen.current = scroll.n
    bodyRef.current?.scrollBy({
      top: (scroll.dir === 'down' ? 1 : -1) * (scroll.continuous ? REPEAT_STEP : STEP),
      behavior: scroll.continuous ? 'auto' : 'smooth',
    })
  }, [scroll])

  return (
    <div className="moreinfo" data-testid="more-info-panel">
      <div className="moreinfo__panel" role="dialog" aria-modal="true" aria-label="More info">
        <button
          type="button"
          className="moreinfo__close"
          data-testid="more-info-close"
          onClick={onClose}
          aria-label="Close More Info"
        >
          ✕
        </button>
        {/* Sanitized in `renderMarkdown`, which is the only producer of this string. */}
        <div
          className="moreinfo__body"
          data-testid="more-info-body"
          ref={bodyRef}
          dangerouslySetInnerHTML={{ __html: html }}
        />
      </div>
    </div>
  )
}
