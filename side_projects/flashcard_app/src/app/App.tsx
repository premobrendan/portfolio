import { useCallback, useEffect, useReducer, useState } from 'react'
import type { AppState, ConfirmIntent, PackMeta, Settings } from '../../shared/types'
import Background from '../components/Background'
import ConfirmDialog from '../components/ConfirmDialog'
import MakePackScreen from '../screens/MakePackScreen'
import SettingsScreen from '../screens/SettingsScreen'
import TitleScreen from '../screens/TitleScreen'
import { applyTheme } from '../theme/themes'
import { keyActionFor } from './keys'
import { initialUI, reduce, topLayer, type Screen } from './machine'
import '../styles/screens.css'

/** What Escape raises on each screen. Title has none — its leave button quits outright. */
const LEAVE_INTENT: Record<Screen, ConfirmIntent | null> = {
  title: null,
  learn: { type: 'leaveLearn' },
  packs: { type: 'leavePacks' },
  settings: { type: 'leaveSettings' },
  makepack: { type: 'leaveMakePack' },
}

export default function App() {
  const [ui, dispatch] = useReducer(reduce, initialUI)
  const [packs, setPacks] = useState<PackMeta[]>([])
  const [state, setState] = useState<AppState | null>(null)
  const [settings, setSettings] = useState<Settings | null>(null)

  const reload = useCallback(async () => {
    const [p, s, cfg] = await Promise.all([
      window.flashcards.packs.list(),
      window.flashcards.state.get(),
      window.flashcards.settings.get(),
    ])
    // Drop selections pointing at packs that no longer exist.
    const live = new Set(p.map(x => x.id))
    const cleaned = s.selectedPackIds.filter(id => live.has(id))
    if (cleaned.length !== s.selectedPackIds.length) {
      await window.flashcards.state.set({ selectedPackIds: cleaned })
    }
    setPacks(p)
    setState({ ...s, selectedPackIds: cleaned })
    setSettings(cfg)
  }, [])

  useEffect(() => { void reload() }, [reload])

  // Task 6 built the theme registry but had nowhere to call it from; settings now exist.
  useEffect(() => { if (settings) applyTheme(settings.themeId) }, [settings])

  const requestLeave = useCallback(() => {
    const intent = LEAVE_INTENT[ui.screen]
    if (intent) dispatch({ type: 'pushLayer', layer: { kind: 'confirm', intent } })
  }, [ui.screen])

  // The ONE global key listener in the app. Everything routes through keyActionFor,
  // which only ever answers for the top of the layer stack.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const intent = keyActionFor(ui, e.key, { hasFlipped: false, card: null })
      if (!intent) return
      e.preventDefault()   // Tab must not move focus; Space must not scroll.
      switch (intent.type) {
        case 'requestLeave': requestLeave(); break
        case 'confirmLeave': dispatch({ type: 'goto', screen: 'title' }); break
        case 'denyLeave': dispatch({ type: 'popLayer' }); break
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [ui, requestLeave])

  if (!state || !settings) return <div className="app-root" data-testid="app-root" />

  const go = (screen: Screen) => dispatch({ type: 'goto', screen })

  return (
    <div className="app-root" data-testid="app-root">
      <Background mode={settings.themeMode} />
      {ui.screen === 'title' && (
        <TitleScreen
          canLearn={packs.length > 0 && state.selectedPackIds.length > 0}
          onGo={go}
          onReload={() => void reload()}
        />
      )}
      {ui.screen === 'settings' && <SettingsScreen onRequestLeave={requestLeave} />}
      {ui.screen === 'makepack' && <MakePackScreen onRequestLeave={requestLeave} />}

      {topLayer(ui)?.kind === 'confirm' && (
        <ConfirmDialog
          intent={(topLayer(ui) as { kind: 'confirm'; intent: ConfirmIntent }).intent}
          onConfirm={() => dispatch({ type: 'goto', screen: 'title' })}
          onDeny={() => dispatch({ type: 'popLayer' })}
        />
      )}
    </div>
  )
}
