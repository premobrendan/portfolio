import { useCallback, useEffect, useReducer, useState } from 'react'
import type { AppState, Card as CardType, ConfirmIntent, PackMeta, Settings } from '../../shared/types'
import Background from '../components/Background'
import ConfirmDialog from '../components/ConfirmDialog'
import LearnScreen from '../screens/LearnScreen'
import MakePackScreen from '../screens/MakePackScreen'
import PackSelectScreen from '../screens/PackSelectScreen'
import SettingsScreen from '../screens/SettingsScreen'
import TitleScreen from '../screens/TitleScreen'
import { applyTheme } from '../theme/themes'
import { keyActionFor, type KeyContext } from './keys'
import { initialUI, reduce, topLayer, type Screen } from './machine'
import '../styles/screens.css'

/**
 * What leaving a screen costs, and therefore whether it is worth a question.
 * The rule (vision §5): never ask a question that has only one sensible answer.
 */
type LeavePolicy =
  | { kind: 'quit' }
  | { kind: 'immediate' }
  | { kind: 'always'; intent: ConfirmIntent }
  | { kind: 'ifDirty'; intent: ConfirmIntent }

const LEAVE_POLICY: Record<Screen, LeavePolicy> = {
  // TitleScreen's own leave button quits; nothing routed through here raises it.
  title: { kind: 'quit' },
  // Leaving mid-deck loses your place, so this one asks unconditionally.
  learn: { kind: 'always', intent: { type: 'leaveLearn' } },
  // Every toggle is already written to disk — a confirmation could protect nothing.
  packs: { kind: 'immediate' },
  settings: { kind: 'ifDirty', intent: { type: 'leaveSettings' } },
  makepack: { kind: 'ifDirty', intent: { type: 'leaveMakePack' } },
}

export default function App() {
  const [ui, dispatch] = useReducer(reduce, initialUI)
  const [packs, setPacks] = useState<PackMeta[]>([])
  const [state, setState] = useState<AppState | null>(null)
  const [settings, setSettings] = useState<Settings | null>(null)
  // Task 13 reads this back to open the preview; for now only the setter is needed.
  const [, setPreviewPackId] = useState<string | null>(null)

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

  /**
   * Write-through: the new selection hits disk on every toggle, not on leaving the
   * screen, so it survives a crash as well as a clean exit. The write is deliberately
   * OUTSIDE a setState updater — StrictMode invokes updaters twice and would double-fire
   * the IPC call.
   */
  const togglePack = useCallback(async (id: string, selected: boolean) => {
    if (!state) return
    const next = selected
      ? [...state.selectedPackIds, id]
      : state.selectedPackIds.filter(x => x !== id)
    setState({ ...state, selectedPackIds: next })
    await window.flashcards.state.set({ selectedPackIds: next })
  }, [state])

  /**
   * Which screens currently have unsaved work. Settings and Make a Pack are placeholders
   * with nothing to change, so nothing sets a flag here yet — the map exists so that
   * giving them real content is a screen change, not a navigation redesign.
   */
  const [dirty, setDirty] = useState<Partial<Record<Screen, boolean>>>({})

  /** Every navigation clears it: one screen is live at a time, and it arrives clean. */
  const go = useCallback((screen: Screen) => {
    setDirty({})
    dispatch({ type: 'goto', screen })
  }, [])

  /** An `ifDirty` screen reports its own state up; the policy itself stays here. */
  const setScreenDirty = useCallback((screen: Screen, isDirty: boolean) => {
    setDirty(prev => (prev[screen] === isDirty ? prev : { ...prev, [screen]: isDirty }))
  }, [])

  const confirmLeave = useCallback((intent: ConfirmIntent) => {
    dispatch({ type: 'pushLayer', layer: { kind: 'confirm', intent } })
  }, [])

  const requestLeave = useCallback(() => {
    const policy = LEAVE_POLICY[ui.screen]
    switch (policy.kind) {
      // Escape on the title screen does nothing; its leave button owns the quit.
      case 'quit': return
      case 'immediate': return go('title')
      case 'always': return confirmLeave(policy.intent)
      case 'ifDirty': return dirty[ui.screen] ? confirmLeave(policy.intent) : go('title')
    }
  }, [ui.screen, dirty, go, confirmLeave])

  // Keyboard flip/advance reach the Learn screen as bumped counters, so flip state can
  // stay inside Card and nothing here re-renders mid-flip.
  const [flipSignal, setFlipSignal] = useState(0)
  const [advanceSignal, setAdvanceSignal] = useState(0)
  // What Learn currently shows: answers the Enter/Tab gate in keyActionFor, and gives
  // the More Info layer its Markdown.
  const [cardCtx, setCardCtx] = useState<KeyContext>({ card: null, hasFlipped: false })

  // Stable identity — LearnScreen calls this from an effect, so an inline arrow would
  // re-run that effect every render and spin. Bail out when nothing changed so a repeat
  // call cannot trigger another render either.
  const handleCardContext = useCallback((card: CardType | null, hasFlipped: boolean) => {
    setCardCtx(prev =>
      prev.card === card && prev.hasFlipped === hasFlipped ? prev : { card, hasFlipped },
    )
  }, [])

  const openMoreInfo = useCallback(
    () => dispatch({ type: 'pushLayer', layer: { kind: 'moreInfo' } }),
    [],
  )

  // The ONE global key listener in the app. Everything routes through keyActionFor,
  // which only ever answers for the top of the layer stack.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const intent = keyActionFor(ui, e.key, cardCtx)
      if (!intent) return
      e.preventDefault()   // Tab must not move focus; Space must not scroll.
      switch (intent.type) {
        case 'flip': setFlipSignal(n => n + 1); break
        case 'advance': setAdvanceSignal(n => n + 1); break
        case 'openMoreInfo': openMoreInfo(); break
        case 'closeMoreInfo': dispatch({ type: 'popLayer' }); break
        case 'requestLeave': requestLeave(); break
        case 'confirmLeave': go('title'); break
        case 'denyLeave': dispatch({ type: 'popLayer' }); break
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [ui, cardCtx, requestLeave, openMoreInfo, go])

  if (!state || !settings) return <div className="app-root" data-testid="app-root" />

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
      {ui.screen === 'packs' && (
        <PackSelectScreen
          packs={packs}
          selectedIds={state.selectedPackIds}
          onToggle={(id, sel) => void togglePack(id, sel)}
          onPreview={setPreviewPackId}
          onRequestLeave={requestLeave}
        />
      )}
      {ui.screen === 'learn' && (
        <LearnScreen
          packIds={state.selectedPackIds}
          flipSignal={flipSignal}
          advanceSignal={advanceSignal}
          onCardContext={handleCardContext}
          onOpenMoreInfo={openMoreInfo}
          onRequestLeave={requestLeave}
        />
      )}
      {ui.screen === 'settings' && (
        <SettingsScreen
          onRequestLeave={requestLeave}
          onDirtyChange={d => setScreenDirty('settings', d)}
        />
      )}
      {ui.screen === 'makepack' && (
        <MakePackScreen
          onRequestLeave={requestLeave}
          onDirtyChange={d => setScreenDirty('makepack', d)}
        />
      )}

      {topLayer(ui)?.kind === 'confirm' && (
        <ConfirmDialog
          intent={(topLayer(ui) as { kind: 'confirm'; intent: ConfirmIntent }).intent}
          onConfirm={() => go('title')}
          onDeny={() => dispatch({ type: 'popLayer' })}
        />
      )}
    </div>
  )
}
