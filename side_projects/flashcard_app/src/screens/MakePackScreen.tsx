import LeaveButton from '../components/LeaveButton'

export default function MakePackScreen({ onRequestLeave }: { onRequestLeave: () => void }) {
  return (
    <div className="screen" data-testid="screen-makepack">
      <LeaveButton onLeave={onRequestLeave} label="Return to main menu" />
      <h2>Make a Pack</h2>
      <p className="hint">Not built yet.</p>
    </div>
  )
}
