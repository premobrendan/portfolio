import { useCallback, useEffect, useReducer, useState } from 'react'
import type { AppState, PackMeta, Settings } from '../../shared/types'
import Background from '../components/Background'
import TitleScreen from '../screens/TitleScreen'
import { applyTheme } from '../theme/themes'
import { initialUI, reduce, type Screen } from './machine'
import '../styles/screens.css'

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
    </div>
  )
}
