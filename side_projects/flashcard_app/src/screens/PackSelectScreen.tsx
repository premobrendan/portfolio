import { useEffect, useState } from 'react'
import type { Card as CardType, PackMeta } from '../../shared/types'
import CardView from '../components/Card'
import LeaveButton from '../components/LeaveButton'
import '../styles/packs.css'

type Props = {
  packs: PackMeta[]
  selectedIds: string[]
  onToggle: (id: string, selected: boolean) => void
  onPreview: (id: string) => void
  previewPackId: string | null
  onClosePreview: () => void
  onRequestLeave: () => void
}

/**
 * Flip through a pack to see what is in it. Deliberately NOT a session: the deck is walked
 * in file order (no shuffle), nothing gates on a flip, no advance arrow, and no session
 * state is touched. It reuses `Card` unchanged, so the flip here is the same flip.
 *
 * `preload` is not used: preview shows one card at a time, and a second `Card` without it
 * would emit a duplicate `data-testid="card"`.
 */
function PackPreview({ packId, onClose }: { packId: string; onClose: () => void }) {
  const [cards, setCards] = useState<CardType[] | null>(null)
  const [i, setI] = useState(0)

  useEffect(() => {
    let cancelled = false
    void window.flashcards.packs.read(packId).then(p => { if (!cancelled) setCards(p.cards) })
    return () => { cancelled = true }
  }, [packId])

  const card = cards && cards.length > 0 ? cards[i % cards.length] : null
  const step = (d: number) => setI(n => {
    const len = cards?.length ?? 1
    return (n + d + len) % len
  })

  return (
    <div className="screen" data-testid="screen-preview">
      {/* Rendered even while the read is in flight, and even for an empty pack, so Close
          is always reachable — the preview must never become a screen with no way out. */}
      {card
        ? <CardView card={card} />
        : <p className="hint">{cards ? 'This pack has no cards.' : ''}</p>}

      <div className="dialog__actions">
        <button type="button" data-testid="preview-prev" onClick={() => step(-1)}>
          Previous
        </button>
        <button type="button" data-testid="preview-next" onClick={() => step(1)}>
          Next
        </button>
        <button type="button" data-testid="preview-close" onClick={onClose}>Close</button>
      </div>
    </div>
  )
}

export default function PackSelectScreen(
  { packs, selectedIds, onToggle, onPreview, previewPackId, onClosePreview, onRequestLeave }: Props,
) {
  // The preview REPLACES the list rather than overlaying it: there is nothing to interact
  // with underneath, and a full swap keeps a single `screen-*` test id live at a time.
  if (previewPackId) {
    return <PackPreview key={previewPackId} packId={previewPackId} onClose={onClosePreview} />
  }

  return (
    <div className="screen" data-testid="screen-packs">
      <LeaveButton onLeave={onRequestLeave} label="Return to main menu" />
      <h2>Pack Selection</h2>

      {packs.length === 0 && <p className="hint">No packs found.</p>}

      <div className="pack-list">
        {packs.map(p => (
          <div className="pack-row" key={p.id}>
            {/* The Preview button is a SIBLING of the label, not a child: a <label> may
                not contain another labelable element, and nesting it would fold "Preview"
                into the checkbox's accessible name. */}
            <label className="pack-row__select">
              <input
                type="checkbox"
                data-testid={`pack-toggle-${p.id}`}
                checked={selectedIds.includes(p.id)}
                onChange={e => onToggle(p.id, e.target.checked)}
              />
              <span className="pack-row__name">{p.name}</span>
              <span className="pack-row__count">{p.cardCount} cards</span>
            </label>
            <button
              type="button"
              className="pack-row__preview"
              data-testid={`pack-preview-${p.id}`}
              onClick={() => onPreview(p.id)}
            >
              Preview
            </button>
          </div>
        ))}
      </div>
    </div>
  )
}
