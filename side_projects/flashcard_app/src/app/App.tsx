import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from 'react'
import type { AppState, ConfirmIntent, PackMeta, Settings } from '../../shared/types'
import Background from '../components/Background'
import ConfirmDialog from '../components/ConfirmDialog'
import MoreInfoPanel, { NO_SCROLL, type ScrollRequest } from '../components/MoreInfoPanel'
import LearnScreen from '../screens/LearnScreen'
import MakePackScreen from '../screens/MakePackScreen'
import PackEditScreen, { type EditorSnapshot } from '../screens/PackEditScreen'
import PackSelectScreen from '../screens/PackSelectScreen'
import SettingsScreen from '../screens/SettingsScreen'
import TitleScreen from '../screens/TitleScreen'
import { renderMarkdown } from '../lib/markdown'
import { applyTheme } from '../theme/themes'
import type { CardView } from '../lib/deck'
import { keyActionFor, repeatable, type KeyContext } from './keys'
import { initialUI, reduce, topLayer, type Screen } from './machine'
import '../styles/screens.css'

/**
 * What leaving a screen costs, and therefore whether it is worth a question.
 * The rule (vision §5): never ask a question that has only one sensible answer.
 */
type LeavePolicy =
  | { kind: 'quit' }
  | { kind: 'immediate'; to: Screen }
  | { kind: 'always'; intent: ConfirmIntent; to: Screen }
  | { kind: 'ifDirty'; intent: ConfirmIntent; to: Screen }

const LEAVE_POLICY: Record<Screen, LeavePolicy> = {
  // Escape, the leave button and the Quit button all close the app outright.
  title: { kind: 'quit' },
  // Leaving mid-deck loses your place, so this one asks unconditionally.
  learn: { kind: 'always', intent: { type: 'leaveLearn' }, to: 'title' },
  // Every toggle is already written to disk — a confirmation could protect nothing.
  packs: { kind: 'immediate', to: 'title' },
  settings: { kind: 'ifDirty', intent: { type: 'leaveSettings' }, to: 'title' },
  // The pack list edits nothing; the editor is where work can be pending, and it steps
  // back to the list rather than all the way out — Escape pops exactly one thing.
  makepack: { kind: 'immediate', to: 'title' },
  makepackEdit: { kind: 'ifDirty', intent: { type: 'leaveMakePack' }, to: 'makepack' },
}

export default function App() {
  const [ui, dispatch] = useReducer(reduce, initialUI)
  const [packs, setPacks] = useState<PackMeta[]>([])
  const [state, setState] = useState<AppState | null>(null)
  const [settings, setSettings] = useState<Settings | null>(null)
  /** Which pack Pack Selection is previewing, or null for the list. */
  const [previewPackId, setPreviewPackId] = useState<string | null>(null)
  /** Which pack the editor has open. Set alongside the navigation to `makepackEdit`. */
  const [editingPackId, setEditingPackId] = useState<string | null>(null)
  /**
   * The editor's live working copy, kept as a ref so a keystroke never re-renders App.
   * Read only when leaving, to write the unsaved-exit draft.
   */
  const editorSnapshot = useRef<EditorSnapshot | null>(null)
  const [saveSignal, setSaveSignal] = useState(0)

  /**
   * Packs with saved cards. A pack that has never been saved, or was emptied, exists only
   * in Make a Pack — it is not something you can study, so Pack Selection never lists it.
   */
  const studyable = useMemo(() => packs.filter(p => p.cardCount > 0), [packs])

  const reload = useCallback(async () => {
    const [p, s, cfg] = await Promise.all([
      window.flashcards.packs.list(),
      window.flashcards.state.get(),
      window.flashcards.settings.get(),
    ])
    // Drop selections pointing at packs that are gone OR have nothing saved to study:
    // an emptied pack is as unstudiable as a deleted one.
    const live = new Set(p.filter(x => x.cardCount > 0).map(x => x.id))
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
    // An open preview belongs to the screen you are leaving, not the one you arrive at.
    setPreviewPackId(null)
    dispatch({ type: 'goto', screen })
  }, [])

  /** An `ifDirty` screen reports its own state up; the policy itself stays here. */
  const setScreenDirty = useCallback((screen: Screen, isDirty: boolean) => {
    setDirty(prev => (prev[screen] === isDirty ? prev : { ...prev, [screen]: isDirty }))
  }, [])

  /** Where the pending confirmation will go if it is answered yes. */
  const [leaveTarget, setLeaveTarget] = useState<Screen>('title')

  const confirmLeave = useCallback((intent: ConfirmIntent, to: Screen) => {
    setLeaveTarget(to)
    dispatch({ type: 'pushLayer', layer: { kind: 'confirm', intent } })
  }, [])

  /**
   * Leaving the editor is the one navigation with a side effect: the working copy is
   * written as a numbered draft (vision §8). `writeDraft` is a no-op when nothing changed,
   * so an untouched pack cannot pile up versions. The draft never becomes the live pack —
   * only Save does that — so walking away can neither lose work nor publish half of it.
   */
  const leaveTo = useCallback(async (screen: Screen) => {
    const snap = editorSnapshot.current
    if (snap) {
      editorSnapshot.current = null
      await window.flashcards.packs.saveDraft(snap.packId, snap.name, snap.cards)
    }
    if (screen !== 'makepackEdit') setEditingPackId(null)
    await reload()
    go(screen)
  }, [go, reload])

  const requestLeave = useCallback(() => {
    /**
     * Escape pops exactly ONE thing. A pack preview is not in the `Layer` union — it has
     * no keyboard grammar of its own and nothing renders underneath it — but it is still
     * something the user opened, so leaving must close it before it leaves the screen.
     * Without this, `packs`' `immediate` policy would jump from an open preview all the
     * way to Title, skipping the list entirely. This is a refinement of the leave policy,
     * which is why it lives here and not in `keys.ts`.
     */
    if (ui.screen === 'packs' && previewPackId !== null) {
      setPreviewPackId(null)
      return
    }
    const policy = LEAVE_POLICY[ui.screen]
    switch (policy.kind) {
      // Escape quits, matching the Quit button and the leave button beside it — three
      // routes to the same door, none of them worth a confirmation.
      case 'quit': return window.flashcards.app.quit()
      case 'immediate': return void leaveTo(policy.to)
      case 'always': return confirmLeave(policy.intent, policy.to)
      case 'ifDirty':
        return dirty[ui.screen]
          ? confirmLeave(policy.intent, policy.to)
          : void leaveTo(policy.to)
    }
  }, [ui.screen, previewPackId, dirty, leaveTo, confirmLeave])

  // Keyboard flip/advance reach the Learn screen as bumped counters, so flip state can
  // stay inside Card and nothing here re-renders mid-flip.
  const [flipSignal, setFlipSignal] = useState(0)
  const [advanceSignal, setAdvanceSignal] = useState(0)
  // What Learn currently shows: answers the Enter/Tab gate in keyActionFor, and gives
  // the More Info layer its Markdown.
  const [cardCtx, setCardCtx] = useState<Omit<KeyContext, 'editingField'>>(
    { view: null, hasFlipped: false },
  )

  // Stable identity — LearnScreen calls this from an effect, so an inline arrow would
  // re-run that effect every render and spin. Bail out when nothing changed so a repeat
  // call cannot trigger another render either.
  const handleCardContext = useCallback((view: CardView | null, hasFlipped: boolean) => {
    setCardCtx(prev =>
      prev.view === view && prev.hasFlipped === hasFlipped ? prev : { view, hasFlipped },
    )
  }, [])

  const openMoreInfo = useCallback(
    () => dispatch({ type: 'pushLayer', layer: { kind: 'moreInfo' } }),
    [],
  )

  /**
   * Parsed when the card becomes current, NOT when the panel opens: Markdown parsing plus
   * sanitizing is the one expensive thing this layer does, and paying for it on Tab would
   * put it in the middle of an interaction. This runs off the card-context change, which
   * has already committed by then, so opening the panel costs a render and nothing more.
   */
  const moreInfoHtml = useMemo(
    () => renderMarkdown(cardCtx.view?.card.moreInfo ?? ''),
    [cardCtx.view],
  )

  /** Scroll requests reach the panel as a bumped counter, the same way flips reach Card. */
  const [scroll, setScroll] = useState<ScrollRequest>(NO_SCROLL)

  // The ONE global key listener in the app. Everything routes through keyActionFor,
  // which only ever answers for the top of the layer stack.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      // Read focus from the DOM rather than mirroring it in state: it is always current,
      // and a blur that React did not cause cannot desynchronise it.
      const el = document.activeElement
      const editingField = el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement
      const intent = keyActionFor(
        ui, e.key, { ...cardCtx, editingField }, e.metaKey || e.ctrlKey,
      )
      if (!intent) return
      // Repeats included: a held Tab must not move focus, a held Space must not scroll.
      e.preventDefault()
      // Auto-repeat drives More Info scrolling and nothing else.
      if (e.repeat && !repeatable(intent)) return
      switch (intent.type) {
        case 'flip': setFlipSignal(n => n + 1); break
        case 'advance': setAdvanceSignal(n => n + 1); break
        case 'openMoreInfo': openMoreInfo(); break
        case 'closeMoreInfo': dispatch({ type: 'popLayer' }); break
        // The one intent auto-repeat is allowed to drive; `continuous` tells the panel to
        // take a smaller instant step so a held key glides instead of stuttering.
        case 'scrollMoreInfo':
          setScroll(r => ({ n: r.n + 1, dir: intent.direction, continuous: e.repeat }))
          break
        case 'savePack': setSaveSignal(n => n + 1); break
        case 'blurField': (el as HTMLElement).blur(); break
        case 'requestLeave': requestLeave(); break
        case 'confirmLeave': void leaveTo(leaveTarget); break
        case 'denyLeave': dispatch({ type: 'popLayer' }); break
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [ui, cardCtx, requestLeave, openMoreInfo, leaveTo, leaveTarget])

  if (!state || !settings) return <div className="app-root" data-testid="app-root" />

  return (
    <div className="app-root" data-testid="app-root">
      <Background mode={settings.themeMode} />
      {ui.screen === 'title' && (
        <TitleScreen
          canLearn={studyable.length > 0 && state.selectedPackIds.length > 0}
          onGo={go}
          onReload={() => void reload()}
        />
      )}
      {ui.screen === 'packs' && (
        <PackSelectScreen
          packs={studyable}
          selectedIds={state.selectedPackIds}
          onToggle={(id, sel) => void togglePack(id, sel)}
          onPreview={setPreviewPackId}
          previewPackId={previewPackId}
          onClosePreview={() => setPreviewPackId(null)}
          onRequestLeave={requestLeave}
        />
      )}
      {ui.screen === 'learn' && (
        <LearnScreen
          packIds={state.selectedPackIds}
          cardDirection={settings.cardDirection}
          backView={settings.backView}
          flipSignal={flipSignal}
          advanceSignal={advanceSignal}
          onCardContext={handleCardContext}
          onOpenMoreInfo={openMoreInfo}
          onRequestLeave={requestLeave}
        />
      )}
      {ui.screen === 'settings' && (
        <SettingsScreen
          settings={settings}
          onSave={async patch => {
            await window.flashcards.settings.set(patch)
            await reload()
          }}
          onRequestLeave={requestLeave}
          onDirtyChange={d => setScreenDirty('settings', d)}
        />
      )}
      {ui.screen === 'makepack' && (
        <MakePackScreen
          packs={packs}
          onOpen={id => { setEditingPackId(id); go('makepackEdit') }}
          onCreate={() => void (async () => {
            const meta = await window.flashcards.packs.create('Untitled pack')
            await reload()
            setEditingPackId(meta.id)
            go('makepackEdit')
          })()}
          onRequestLeave={requestLeave}
        />
      )}
      {ui.screen === 'makepackEdit' && editingPackId && (
        <PackEditScreen
          key={editingPackId}
          packId={editingPackId}
          snapshotRef={editorSnapshot}
          saveSignal={saveSignal}
          onDirtyChange={d => setScreenDirty('makepackEdit', d)}
          onSaved={() => void reload()}
          onDeleted={() => {
            // Already gone from disk — drop the snapshot so leaving cannot resurrect it
            // as a draft, then step back to the list.
            editorSnapshot.current = null
            void leaveTo('makepack')
          }}
          onRequestLeave={requestLeave}
        />
      )}

      {topLayer(ui)?.kind === 'moreInfo' && moreInfoHtml && (
        <MoreInfoPanel
          html={moreInfoHtml}
          onClose={() => dispatch({ type: 'popLayer' })}
          scroll={scroll}
        />
      )}

      {topLayer(ui)?.kind === 'confirm' && (
        <ConfirmDialog
          intent={(topLayer(ui) as { kind: 'confirm'; intent: ConfirmIntent }).intent}
          onConfirm={() => void leaveTo(leaveTarget)}
          onDeny={() => dispatch({ type: 'popLayer' })}
        />
      )}
    </div>
  )
}
