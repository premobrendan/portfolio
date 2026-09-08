import LeaveButton from '../components/LeaveButton'

type Props = {
  onRequestLeave: () => void
  /**
   * Report an edit worth confirming on leave. Nothing on this placeholder screen can
   * edit anything yet, so it is never called — App treats Make a Pack as clean and the
   * leave button returns to the menu without asking.
   */
  onDirtyChange: (dirty: boolean) => void
}

export default function MakePackScreen({ onRequestLeave }: Props) {
  return (
    <div className="screen" data-testid="screen-makepack">
      <LeaveButton onLeave={onRequestLeave} label="Return to main menu" />
      <h2>Make a Pack</h2>
      <p className="hint">Not built yet.</p>
    </div>
  )
}
