import { describe, it, expect } from 'vitest'
import { initialUI, reduce, topLayer } from './machine'
import { keyActionFor, repeatable } from './keys'

describe('screens', () => {
  it('starts on the title screen with no layers', () => {
    expect(initialUI).toEqual({ screen: 'title', layers: [] })
  })

  it('navigates between screens', () => {
    const ui = reduce(initialUI, { type: 'goto', screen: 'learn' })
    expect(ui.screen).toBe('learn')
  })
})

describe('layer stack', () => {
  it('pushes and reports the top layer', () => {
    let ui = reduce(initialUI, { type: 'goto', screen: 'learn' })
    ui = reduce(ui, { type: 'pushLayer', layer: { kind: 'moreInfo' } })
    expect(topLayer(ui)).toEqual({ kind: 'moreInfo' })
  })

  it('pops exactly one layer', () => {
    let ui = reduce(initialUI, { type: 'goto', screen: 'learn' })
    ui = reduce(ui, { type: 'pushLayer', layer: { kind: 'moreInfo' } })
    ui = reduce(ui, {
      type: 'pushLayer', layer: { kind: 'confirm', intent: { type: 'leaveLearn' } },
    })
    ui = reduce(ui, { type: 'popLayer' })
    expect(topLayer(ui)).toEqual({ kind: 'moreInfo' })
    expect(ui.layers).toHaveLength(1)
  })

  it('reports no top layer when the stack is empty', () => {
    expect(topLayer(initialUI)).toBeNull()
  })

  it('clears layers on navigation so overlays never survive a screen change', () => {
    let ui = reduce(initialUI, { type: 'goto', screen: 'learn' })
    ui = reduce(ui, { type: 'pushLayer', layer: { kind: 'moreInfo' } })
    ui = reduce(ui, { type: 'goto', screen: 'title' })
    expect(ui.layers).toEqual([])
  })

  it('popping an empty stack is a no-op', () => {
    expect(reduce(initialUI, { type: 'popLayer' })).toEqual(initialUI)
  })
})

const learn = { screen: 'learn' as const, layers: [] }
const cardWith = { id: 'c', front: 'f', primary: 'p', moreInfo: '# hi' }
const cardWithout = { id: 'c', front: 'f', primary: 'p' }

describe('key dispatch', () => {
  it('Space flips on the card layer', () => {
    expect(keyActionFor(learn, ' ', { hasFlipped: false, card: cardWith }))
      .toEqual({ type: 'flip' })
  })

  it('Enter does nothing before a flip and advances after', () => {
    expect(keyActionFor(learn, 'Enter', { hasFlipped: false, card: cardWith })).toBeNull()
    expect(keyActionFor(learn, 'Enter', { hasFlipped: true, card: cardWith }))
      .toEqual({ type: 'advance' })
  })

  it('Tab needs both a prior flip and non-empty More Info', () => {
    expect(keyActionFor(learn, 'Tab', { hasFlipped: false, card: cardWith })).toBeNull()
    expect(keyActionFor(learn, 'Tab', { hasFlipped: true, card: cardWithout })).toBeNull()
    expect(keyActionFor(learn, 'Tab', { hasFlipped: true, card: cardWith }))
      .toEqual({ type: 'openMoreInfo' })
  })

  it('swallows Space and Enter beneath an open More Info panel', () => {
    const ui = { screen: 'learn' as const, layers: [{ kind: 'moreInfo' as const }] }
    expect(keyActionFor(ui, ' ', { hasFlipped: true, card: cardWith })).toBeNull()
    expect(keyActionFor(ui, 'Enter', { hasFlipped: true, card: cardWith })).toBeNull()
  })

  it('closes More Info on Escape without leaving the screen', () => {
    const ui = { screen: 'learn' as const, layers: [{ kind: 'moreInfo' as const }] }
    expect(keyActionFor(ui, 'Escape', { hasFlipped: true, card: cardWith }))
      .toEqual({ type: 'closeMoreInfo' })
  })

  it('confirms on a second Escape and denies on Space or Enter', () => {
    const ui = {
      screen: 'learn' as const,
      layers: [{ kind: 'confirm' as const, intent: { type: 'leaveLearn' as const } }],
    }
    const ctx = { hasFlipped: true, card: cardWith }
    expect(keyActionFor(ui, 'Escape', ctx)).toEqual({ type: 'confirmLeave' })
    expect(keyActionFor(ui, ' ', ctx)).toEqual({ type: 'denyLeave' })
    expect(keyActionFor(ui, 'Enter', ctx)).toEqual({ type: 'denyLeave' })
  })
})

describe('key auto-repeat', () => {
  it('is meaningful only for scrolling More Info', () => {
    expect(repeatable({ type: 'scrollMoreInfo', direction: 'up' })).toBe(true)
    expect(repeatable({ type: 'scrollMoreInfo', direction: 'down' })).toBe(true)
  })

  it('is refused by every intent a held key could otherwise abuse', () => {
    // Held Escape would walk confirm -> leave -> quit; the rest are merely meaningless.
    expect(repeatable({ type: 'requestLeave' })).toBe(false)
    expect(repeatable({ type: 'confirmLeave' })).toBe(false)
    expect(repeatable({ type: 'denyLeave' })).toBe(false)
    expect(repeatable({ type: 'flip' })).toBe(false)
    expect(repeatable({ type: 'advance' })).toBe(false)
    expect(repeatable({ type: 'openMoreInfo' })).toBe(false)
    expect(repeatable({ type: 'closeMoreInfo' })).toBe(false)
    expect(repeatable(null)).toBe(false)
  })
})
