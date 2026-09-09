import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import type { Card } from '../../shared/types'
import { isCardComplete, type CardProblem } from '../../shared/validate'
import DeletePackDialog from '../components/DeletePackDialog'
import LeaveButton from '../components/LeaveButton'
import '../styles/makepack.css'

export type EditorSnapshot = { packId: string; name: string; cards: Card[] }

type Props = {
  packId: string
  /**
   * Kept current on every render so App can write a draft on the way out without owning
   * the working copy — a ref assignment costs nothing and never re-renders App, which
   * matters because this screen changes state on every keystroke.
   */
  snapshotRef: { current: EditorSnapshot | null }
  onDirtyChange: (dirty: boolean) => void
  onSaved: () => void
  onDeleted: () => void
  onRequestLeave: () => void
  /** Bumped by App when Ctrl/Cmd+S is pressed. */
  saveSignal: number
}

const blankCard = (): Card => ({ id: crypto.randomUUID(), front: '', primary: '' })

/** The comparison behind the dirty flag and the draft-skip. Field order is stable. */
const fingerprint = (name: string, cards: Card[]) => JSON.stringify({ name, cards })

export default function PackEditScreen(props: Props) {
  const { packId, snapshotRef, onDirtyChange, onSaved, onDeleted, onRequestLeave } = props

  const [name, setName] = useState('')
  const [cards, setCards] = useState<Card[]>([])
  const [selectedId, setSelectedId] = useState<string | null>(null)
  /** Fingerprint of the last state written to disk — dirty is measured against this. */
  const [baseline, setBaseline] = useState<string | null>(null)
  const [saved, setSaved] = useState(false)
  const [problems, setProblems] = useState<CardProblem[] | null>(null)
  const [deleting, setDeleting] = useState(false)

  // Load the draft if there is one, otherwise the last save.
  useEffect(() => {
    let cancelled = false
    void window.flashcards.packs.readForEdit(packId).then(pack => {
      if (cancelled) return
      setName(pack.name)
      setCards(pack.cards)
      setSelectedId(pack.cards[0]?.id ?? null)
      setBaseline(fingerprint(pack.name, pack.cards))
    })
    return () => { cancelled = true }
  }, [packId])

  // `baseline === null` means the load is still in flight; nothing is dirty yet.
  const dirty = baseline !== null && fingerprint(name, cards) !== baseline

  useEffect(() => { onDirtyChange(dirty) }, [dirty, onDirtyChange])

  useEffect(() => { snapshotRef.current = { packId, name, cards } })
  useEffect(() => () => { snapshotRef.current = null }, [snapshotRef])

  const selected = cards.find(c => c.id === selectedId) ?? null

  const patchSelected = useCallback((patch: Partial<Card>) => {
    setCards(prev => prev.map(c => (c.id === selectedId ? { ...c, ...patch } : c)))
    // Any edit invalidates both the "Saved" note and the stale problem list.
    setSaved(false)
    setProblems(null)
  }, [selectedId])

  /** The three list edits, all routed through patchSelected so dirty tracking is automatic. */
  const patchSecondary = useCallback((index: number, value: string) => {
    setCards(prev => prev.map(c => {
      if (c.id !== selectedId) return c
      const next = [...(c.secondaries ?? [])]
      next[index] = value
      return { ...c, secondaries: next }
    }))
    setSaved(false)
    setProblems(null)
  }, [selectedId])

  const addSecondary = useCallback(() => {
    setCards(prev => prev.map(c =>
      c.id === selectedId ? { ...c, secondaries: [...(c.secondaries ?? []), ''] } : c))
    setSaved(false)
  }, [selectedId])

  const removeSecondary = useCallback((index: number) => {
    setCards(prev => prev.map(c => {
      if (c.id !== selectedId) return c
      const next = (c.secondaries ?? []).filter((_, i) => i !== index)
      // Drop the key entirely at zero, so an emptied list is stored as absent rather than
      // as [] — the same present/absent rule the MCP tools use.
      if (next.length === 0) {
        const { secondaries: _gone, ...rest } = c
        return rest
      }
      return { ...c, secondaries: next }
    }))
    setSaved(false)
  }, [selectedId])

  const addCard = useCallback(() => {
    const card = blankCard()
    setCards(prev => [...prev, card])
    setSelectedId(card.id)
    setSaved(false)
    setProblems(null)
  }, [])

  const deleteSelected = useCallback(() => {
    if (!selectedId) return
    setCards(prev => {
      const i = prev.findIndex(c => c.id === selectedId)
      const next = prev.filter(c => c.id !== selectedId)
      // Select the neighbour that takes the deleted card's place, so the detail pane is
      // never left empty while cards remain.
      setSelectedId(next[Math.min(i, next.length - 1)]?.id ?? null)
      return next
    })
    setSaved(false)
    setProblems(null)
  }, [selectedId])

  const save = useCallback(async () => {
    const outcome = await window.flashcards.packs.save(packId, name, cards)
    if (outcome.ok) {
      setBaseline(fingerprint(name, cards))
      setProblems(null)
      setSaved(true)
      onSaved()
    } else {
      setProblems(outcome.problems)
      setSaved(false)
      // Put the first offending card in front of the user rather than making them hunt.
      setSelectedId(cards[outcome.problems[0].index]?.id ?? null)
    }
  }, [packId, name, cards, onSaved])

  // Ctrl/Cmd+S arrives from App's global listener as a bumped counter. The ref keeps the
  // effect from re-running (and re-saving) every time `save` is rebuilt by a keystroke.
  const saveRef = useRef(save)
  useEffect(() => { saveRef.current = save })
  useEffect(() => {
    if (props.saveSignal > 0) void saveRef.current()
  }, [props.saveSignal])

  const remove = useCallback(async () => {
    await window.flashcards.packs.remove(packId)
    onDeleted()
  }, [packId, onDeleted])

  /** Only the fields the list draws, so typing in More Info cannot rebuild every row. */
  const rows = useMemo(
    () => cards.map(c => ({ id: c.id, front: c.front, complete: isCardComplete(c) })),
    [cards],
  )

  const status = dirty ? 'Unsaved changes' : saved ? 'Saved' : ''

  return (
    <div className="screen screen--wide" data-testid="screen-makepack-edit">
      <LeaveButton onLeave={onRequestLeave} label="Back to pack list" />

      <header className="editor-head">
        <input
          className="editor-name"
          data-testid="pack-name-input"
          aria-label="Pack name"
          value={name}
          placeholder="Untitled pack"
          onChange={e => { setName(e.target.value); setSaved(false) }}
        />
        <span className="editor-status" data-testid="save-status">{status}</span>
        <button type="button" className="editor-save" data-testid="save-pack" onClick={() => void save()}>
          Save
        </button>
      </header>

      {problems && (
        <p className="editor-problems" data-testid="save-problems" role="alert">
          {problems.length === 1 ? '1 card is' : `${problems.length} cards are`} incomplete.
          Every card needs a front and a back before the pack can be saved.
        </p>
      )}

      <div className="editor">
        <aside className="editor-list" data-testid="card-list">
          <div className="editor-list__head">
            <span>Cards</span>
            <button type="button" data-testid="card-add" onClick={addCard} aria-label="Add card">+</button>
          </div>
          <div className="editor-list__scroll">
            {rows.map(row => (
              <button
                type="button"
                key={row.id}
                className={`editor-row${row.id === selectedId ? ' is-selected' : ''}`}
                onClick={() => setSelectedId(row.id)}
              >
                {!row.complete && <span className="editor-row__warn" aria-label="incomplete">⚠</span>}
                <span className="editor-row__front">{row.front.trim() || '(untitled)'}</span>
              </button>
            ))}
          </div>
          <div className="editor-list__foot">
            {cards.length === 1 ? '1 card' : `${cards.length} cards`}
          </div>
        </aside>

        <section className="editor-detail">
          {selected ? (
            <>
              <label className="editor-field">
                <span className="editor-field__label">Front Text <em>required</em></span>
                <input
                  data-testid="field-front"
                  value={selected.front}
                  onChange={e => patchSelected({ front: e.target.value })}
                />
              </label>

              <label className="editor-field">
                <span className="editor-field__label">Primary Text <em>required</em></span>
                <input
                  data-testid="field-primary"
                  value={selected.primary}
                  onChange={e => patchSelected({ primary: e.target.value })}
                />
              </label>

              <div className="editor-field">
                <span className="editor-field__label">
                  Secondary Texts <em>optional — each competes with Primary</em>
                </span>

                {(selected.secondaries ?? []).map((text, i) => (
                  // Index as key: these rows have no identity of their own, and the list
                  // is short enough that a re-render on edit costs nothing.
                  <div className="editor-secondary" key={i}>
                    <input
                      data-testid={`field-secondary-${i}`}
                      value={text}
                      onChange={e => patchSecondary(i, e.target.value)}
                    />
                    <button
                      type="button"
                      data-testid={`secondary-remove-${i}`}
                      aria-label={`Remove secondary text ${i + 1}`}
                      onClick={() => removeSecondary(i)}
                    >
                      ×
                    </button>
                  </div>
                ))}

                <button
                  type="button"
                  className="editor-secondary__add"
                  data-testid="secondary-add"
                  onClick={addSecondary}
                >
                  + Add another
                </button>
              </div>

              <label className="editor-field editor-field--grow">
                <span className="editor-field__label">More Info <em>Markdown, optional</em></span>
                <textarea
                  data-testid="field-moreinfo"
                  value={selected.moreInfo ?? ''}
                  onChange={e => patchSelected({ moreInfo: e.target.value || undefined })}
                />
              </label>

              <div className="editor-detail__foot">
                <button type="button" data-testid="card-delete" onClick={deleteSelected}>
                  Delete card
                </button>
              </div>
            </>
          ) : (
            <p className="hint">
              {cards.length === 0 ? 'No cards yet — add one to get started.' : 'Select a card.'}
            </p>
          )}
        </section>
      </div>

      {/* Deliberately the quietest control on the screen: it is the only irreversible one. */}
      <button type="button" className="editor-danger" data-testid="delete-pack" onClick={() => setDeleting(true)}>
        Delete this pack
      </button>

      {deleting && (
        <DeletePackDialog
          packName={name || 'this pack'}
          onCancel={() => setDeleting(false)}
          onConfirm={() => void remove()}
        />
      )}
    </div>
  )
}
