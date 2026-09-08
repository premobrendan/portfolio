import type { ConfirmIntent } from '../../shared/types'
import '../styles/overlay.css'

export const CONFIRM_COPY: Record<ConfirmIntent['type'], string> = {
  leaveLearn: 'Return to Main Menu?',
  leaveSettings: 'Save settings before leaving?',
  leaveMakePack: 'Are you sure you want to leave?',
}

type Props = { intent: ConfirmIntent; onConfirm: () => void; onDeny: () => void }

export default function ConfirmDialog({ intent, onConfirm, onDeny }: Props) {
  return (
    <div className="overlay" data-testid="confirm-dialog">
      <div className="dialog" role="dialog" aria-modal="true">
        <p>{CONFIRM_COPY[intent.type]}</p>
        <div className="dialog__actions">
          <button onClick={onConfirm} data-testid="confirm-yes">Yes</button>
          <button onClick={onDeny} data-testid="confirm-no">No</button>
        </div>
      </div>
    </div>
  )
}
