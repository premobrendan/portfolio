import { describe, it, expect } from 'vitest'
import { keyActionFor } from './keys'
import type { UI } from './machine'

const editor: UI = { screen: 'makepackEdit', layers: [] }
const idle = { view: null, hasFlipped: false, editingField: false }
const typing = { ...idle, editingField: true }

describe('the pack editor key grammar', () => {
  it('saves on Ctrl/Cmd+S', () => {
    expect(keyActionFor(editor, 's', idle, true)).toEqual({ type: 'savePack' })
  })

  it('does not save on a bare S — that is a character being typed', () => {
    expect(keyActionFor(editor, 's', typing, false)).toBeNull()
  })

  it('leaves the screen on Escape when no field has focus', () => {
    expect(keyActionFor(editor, 'Escape', idle, false)).toEqual({ type: 'requestLeave' })
  })

  it('leaves the FIELD on Escape while typing, and nothing else', () => {
    expect(keyActionFor(editor, 'Escape', typing, false)).toEqual({ type: 'blurField' })
  })

  it('ignores ordinary typing', () => {
    expect(keyActionFor(editor, 'a', typing, false)).toBeNull()
    expect(keyActionFor(editor, ' ', typing, false)).toBeNull()
    expect(keyActionFor(editor, 'Enter', typing, false)).toBeNull()
  })

  it('does not hijack Ctrl/Cmd+S on other screens', () => {
    const packs: UI = { screen: 'packs', layers: [] }
    expect(keyActionFor(packs, 's', idle, true)).toBeNull()
  })

  it('lets an open dialog keep swallowing keys', () => {
    const confirming: UI = {
      screen: 'makepackEdit',
      layers: [{ kind: 'confirm', intent: { type: 'leaveMakePack' } }],
    }
    expect(keyActionFor(confirming, 's', idle, true)).toBeNull()
    expect(keyActionFor(confirming, 'Escape', idle, false)).toEqual({ type: 'confirmLeave' })
  })
})
