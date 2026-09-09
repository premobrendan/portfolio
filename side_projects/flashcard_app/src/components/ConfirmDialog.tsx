import type { ConfirmIntent } from '../../shared/types'
import '../styles/overlay.css'

export const CONFIRM_COPY: Record<ConfirmIntent['type'], string> = {
  leaveLearn: 'Return to Main Menu?',
  leaveSettings: 'Leave without saving?',
  leaveMakePack: 'Leave without saving?',
}

/**
 * The second line, where the answer is not obvious from the question alone. Make a Pack
 * needs one: "leave without saving" sounds like losing the work, and it is not — the
 * draft is kept, it just does not become the pack Learn studies until it is saved.
 */
const CONFIRM_NOTE: Partial<Record<ConfirmIntent['type'], string>> = {
  // Settings keeps no draft, so leaving really does discard — say so plainly. This
  // replaces "Save settings before leaving?", which read as a three-way save/discard/cancel
  // question while the dialog only offers two answers (vision §14 Q12).
  leaveSettings: 'Your changes to Settings will be lost.',
  leaveMakePack:
    'Your changes are kept as a draft and will be here when you come back. '
    + 'The pack itself will not change until you save.',
}

type Props = { intent: ConfirmIntent; onConfirm: () => void; onDeny: () => void }

export default function ConfirmDialog({ intent, onConfirm, onDeny }: Props) {
  return (
    <div className="overlay" data-testid="confirm-dialog">
      <div className="dialog" role="dialog" aria-modal="true">
        <p>{CONFIRM_COPY[intent.type]}</p>
        {CONFIRM_NOTE[intent.type] && (
          <p className="dialog__note">{CONFIRM_NOTE[intent.type]}</p>
        )}
        <div className="dialog__actions">
          <button onClick={onConfirm} data-testid="confirm-yes">Yes</button>
          <button onClick={onDeny} data-testid="confirm-no">No</button>
        </div>
      </div>
    </div>
  )
}
