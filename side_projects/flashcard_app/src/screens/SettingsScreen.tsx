import LeaveButton from '../components/LeaveButton'

export default function SettingsScreen({ onRequestLeave }: { onRequestLeave: () => void }) {
  return (
    <div className="screen" data-testid="screen-settings">
      <LeaveButton onLeave={onRequestLeave} label="Return to main menu" />
      <h2>Settings</h2>
      <p className="hint">Nothing here yet.</p>
    </div>
  )
}
