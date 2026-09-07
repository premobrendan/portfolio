import LeaveButton from '../components/LeaveButton'
import type { Screen } from '../app/machine'

type Props = { canLearn: boolean; onGo: (s: Screen) => void; onReload: () => void }

export default function TitleScreen({ canLearn, onGo, onReload }: Props) {
  const quit = () => window.flashcards.app.quit()

  return (
    <div className="screen">
      <LeaveButton onLeave={quit} label="Quit application" />
      <h1>Flashcards</h1>
      <div className="menu" data-testid="title-menu">
        <button onClick={() => onGo('learn')} disabled={!canLearn}>Learn</button>
        <button onClick={() => onGo('packs')}>Pack Selection</button>
        <button onClick={() => onGo('makepack')}>Make a Pack</button>
        <button onClick={() => onGo('settings')}>Settings</button>
        <button onClick={quit}>Quit</button>
      </div>
      <p className="hint">{canLearn ? '' : 'Select at least one pack to start learning.'}</p>
      {/* Test-only affordance for re-reading persisted state without a relaunch. Kept out
          of the accessibility tree and the tab order so it is invisible to the app proper. */}
      <button
        data-testid="btn-reload"
        onClick={onReload}
        tabIndex={-1}
        aria-hidden="true"
        style={{ position: 'fixed', opacity: 0, pointerEvents: 'auto', bottom: 0, right: 0 }}
      >
        reload
      </button>
    </div>
  )
}
