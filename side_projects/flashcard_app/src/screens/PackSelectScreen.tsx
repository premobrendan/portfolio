import type { PackMeta } from '../../shared/types'
import LeaveButton from '../components/LeaveButton'
import '../styles/packs.css'

type Props = {
  packs: PackMeta[]
  selectedIds: string[]
  onToggle: (id: string, selected: boolean) => void
  onPreview: (id: string) => void
  onRequestLeave: () => void
}

export default function PackSelectScreen(
  { packs, selectedIds, onToggle, onPreview, onRequestLeave }: Props,
) {
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
