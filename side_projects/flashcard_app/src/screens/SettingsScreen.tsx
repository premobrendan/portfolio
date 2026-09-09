import { useEffect, useState } from 'react'
import type { BackView, CardDirection, Settings } from '../../shared/types'
import LeaveButton from '../components/LeaveButton'
import '../styles/settings.css'

type Props = {
  settings: Settings
  /** Persists the change. Only called by Save — edits stay local until then. */
  onSave: (patch: Partial<Settings>) => Promise<void>
  onRequestLeave: () => void
  /** Reported up so App can ask before an edited screen is abandoned (vision §5). */
  onDirtyChange: (dirty: boolean) => void
}

const DIRECTIONS: { value: CardDirection; label: string; detail: string }[] = [
  { value: 'forward', label: 'Front first',
    detail: 'See the prompt, flip to reveal the answer.' },
  { value: 'reverse', label: 'Back first',
    detail: 'See the answer, flip to recall the prompt it belongs to.' },
  { value: 'random', label: 'Random',
    detail: 'Each card picks its own side, by chance.' },
]

const BACK_VIEWS: { value: BackView; label: string; detail: string }[] = [
  { value: 'primary', label: 'Primary only',
    detail: 'Always the Primary Text, even on cards that carry more.' },
  { value: 'random', label: 'Random',
    detail: 'Picks one of the card\'s texts at chance — Primary or any Secondary.' },
]

/**
 * Settings holds edits locally and writes only on Save, so leaving with changes really
 * does lose them — which is what earns the confirmation that Pack Selection does not get.
 */
export default function SettingsScreen(
  { settings, onSave, onRequestLeave, onDirtyChange }: Props,
) {
  const [direction, setDirection] = useState<CardDirection>(settings.cardDirection)
  const [backView, setBackView] = useState<BackView>(settings.backView)
  const [saving, setSaving] = useState(false)

  const dirty = direction !== settings.cardDirection || backView !== settings.backView

  useEffect(() => { onDirtyChange(dirty) }, [dirty, onDirtyChange])

  const save = async () => {
    setSaving(true)
    await onSave({ cardDirection: direction, backView })
    setSaving(false)
  }

  return (
    <div className="screen" data-testid="screen-settings">
      <LeaveButton onLeave={onRequestLeave} label="Return to main menu" />
      <h2>Settings</h2>

      <fieldset className="setting">
        <legend className="setting__legend">Card direction</legend>
        <p className="setting__help">
          Which side of a card you are shown first. Takes effect the next time you open Learn.
        </p>

        {DIRECTIONS.map(option => (
          <label className="setting__option" key={option.value}>
            <input
              type="radio"
              name="cardDirection"
              data-testid={`direction-${option.value}`}
              checked={direction === option.value}
              onChange={() => setDirection(option.value)}
            />
            <span className="setting__option-text">
              <span className="setting__option-label">{option.label}</span>
              <span className="setting__option-detail">{option.detail}</span>
            </span>
          </label>
        ))}
      </fieldset>

      <fieldset className="setting">
        <legend className="setting__legend">Back view</legend>
        <p className="setting__help">
          A card needs only a Primary Text, but may carry any number of Secondary Texts.
          This chooses which one a session shows.
        </p>

        {BACK_VIEWS.map(option => (
          <label className="setting__option" key={option.value}>
            <input
              type="radio"
              name="backView"
              data-testid={`backview-${option.value}`}
              checked={backView === option.value}
              onChange={() => setBackView(option.value)}
            />
            <span className="setting__option-text">
              <span className="setting__option-label">{option.label}</span>
              <span className="setting__option-detail">{option.detail}</span>
            </span>
          </label>
        ))}
      </fieldset>

      <div className="setting__actions">
        <span className="setting__status" data-testid="settings-status">
          {dirty ? 'Unsaved changes' : ''}
        </span>
        <button
          type="button"
          data-testid="settings-save"
          onClick={() => void save()}
          disabled={!dirty || saving}
        >
          Save
        </button>
      </div>
    </div>
  )
}
