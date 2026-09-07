import '../theme/space.css'

/**
 * Themed background on its own compositor layer. A dynamic background is allowed
 * to drop frames on weak hardware; `contain: strict` and the separate layer keep
 * that from ever costing the card a frame. The user's remedy is the static mode.
 */
export default function Background({ mode }: { mode: 'static' | 'dynamic' }) {
  return (
    <div className="bg" data-testid="background" aria-hidden="true">
      <div className={`bg__stars ${mode === 'dynamic' ? 'bg__stars--dynamic' : ''}`} />
    </div>
  )
}
