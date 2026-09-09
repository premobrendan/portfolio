import type { PackMeta } from '../../shared/types'
import LeaveButton from '../components/LeaveButton'
import '../styles/makepack.css'

type Props = {
  packs: PackMeta[]
  onOpen: (id: string) => void
  onCreate: () => void
  onRequestLeave: () => void
}

/**
 * The authoring home: every pack, plus a way to start a new one. Unlike Pack Selection
 * this lists packs with no saved cards — an unfinished pack has to be reachable from
 * somewhere, and this is the only screen that can finish it.
 *
 * Nothing here is editable, so leaving asks nothing.
 */
export default function MakePackScreen({ packs, onOpen, onCreate, onRequestLeave }: Props) {
  return (
    <div className="screen" data-testid="screen-makepack">
      <LeaveButton onLeave={onRequestLeave} label="Return to main menu" />
      <h2>Make a Pack</h2>

      <div className="pack-list">
        <button
          type="button"
          className="makepack-new"
          data-testid="makepack-new"
          onClick={onCreate}
        >
          + New pack
        </button>

        {packs.map(p => (
          <button
            type="button"
            className="makepack-row"
            key={p.id}
            data-testid={`makepack-open-${p.id}`}
            onClick={() => onOpen(p.id)}
          >
            {/* The draft name, when there is one: this is the only screen that can
                finish the pack, so it shows the pack as the author last left it. */}
            <span className="makepack-row__name">{p.draftName ?? p.name}</span>
            <span className="makepack-row__count">
              {p.cardCount === 1 ? '1 card' : `${p.cardCount} cards`}
              {/* A pack with nothing saved is invisible to Pack Selection, so say so
                  here rather than letting the user wonder where it went. */}
              {p.cardCount === 0 && <span className="makepack-row__flag"> · not yet in Learn</span>}
            </span>
          </button>
        ))}
      </div>

      {packs.length === 0 && <p className="hint">No packs yet.</p>}
    </div>
  )
}
