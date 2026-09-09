import { useState } from 'react'
import '../styles/overlay.css'

type Props = { packName: string; onCancel: () => void; onConfirm: () => void }

const PHRASE = 'DELETE'

/**
 * Deleting a pack is the only thing in the app that destroys work outright — every other
 * destructive-looking action is recoverable from a version or simply re-selectable. A
 * yes/no dialog is one mis-aimed click away from gone, so this one has to be typed. The
 * button stays disabled until the word matches exactly.
 */
export default function DeletePackDialog({ packName, onCancel, onConfirm }: Props) {
  const [typed, setTyped] = useState('')

  return (
    <div
      className="overlay"
      data-testid="delete-dialog"
      // Escape belongs to this dialog while it is open. Stopping propagation here keeps
      // it from reaching App's window listener and leaving the screen underneath.
      onKeyDown={e => {
        if (e.key === 'Escape') { e.stopPropagation(); onCancel() }
      }}
    >
      <div className="dialog dialog--danger" role="dialog" aria-modal="true">
        <p>Delete <strong>{packName}</strong>?</p>
        <p className="dialog__note">
          This removes the pack and every backup of it. It cannot be undone.
        </p>

        <label className="dialog__prompt">
          Type {PHRASE} to confirm
          <input
            data-testid="delete-input"
            aria-label={`Type ${PHRASE} to confirm`}
            value={typed}
            autoFocus
            onChange={e => setTyped(e.target.value)}
          />
        </label>

        <div className="dialog__actions">
          <button type="button" data-testid="delete-cancel" onClick={onCancel}>Cancel</button>
          <button
            type="button"
            className="is-danger"
            data-testid="delete-submit"
            disabled={typed !== PHRASE}
            onClick={onConfirm}
          >
            Delete pack
          </button>
        </div>
      </div>
    </div>
  )
}
