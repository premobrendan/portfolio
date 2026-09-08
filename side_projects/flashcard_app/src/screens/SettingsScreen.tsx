import LeaveButton from '../components/LeaveButton'

type Props = {
  onRequestLeave: () => void
  /**
   * Report a change worth confirming on leave. Nothing on this placeholder screen can
   * change anything yet, so it is never called — App treats Settings as clean and the
   * leave button returns to the menu without asking.
   */
  onDirtyChange: (dirty: boolean) => void
}

export default function SettingsScreen({ onRequestLeave }: Props) {
  return (
    <div className="screen" data-testid="screen-settings">
      <LeaveButton onLeave={onRequestLeave} label="Return to main menu" />
      <h2>Settings</h2>
      <p className="hint">Nothing here yet.</p>
    </div>
  )
}
