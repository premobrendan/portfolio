import { describe, it, expect } from 'vitest'
import { isCardComplete, validateCards } from './validate'
import type { Card } from './types'

const card = (over: Partial<Card> = {}): Card =>
  ({ id: 'c1', front: 'France', primary: 'Paris', ...over })

describe('isCardComplete', () => {
  it('accepts a card with both required fields', () => {
    expect(isCardComplete(card())).toBe(true)
  })

  it('rejects a card whose front is only whitespace', () => {
    expect(isCardComplete(card({ front: '   ' }))).toBe(false)
  })

  it('rejects a card whose primary is empty', () => {
    expect(isCardComplete(card({ primary: '' }))).toBe(false)
  })

  it('ignores More Info entirely', () => {
    expect(isCardComplete(card({ moreInfo: undefined }))).toBe(true)
  })
})

describe('validateCards', () => {
  it('passes a pack whose cards are all complete', () => {
    expect(validateCards([card({ id: 'a' }), card({ id: 'b' })])).toEqual({ ok: true })
  })

  it('passes an empty pack', () => {
    expect(validateCards([])).toEqual({ ok: true })
  })

  it('reports which field is missing, and where', () => {
    const result = validateCards([card({ id: 'a' }), card({ id: 'b', primary: ' ' })])
    expect(result).toEqual({
      ok: false,
      problems: [{ cardId: 'b', index: 1, missing: ['primary'] }],
    })
  })

  it('reports both fields when a card is entirely blank', () => {
    const result = validateCards([card({ id: 'a', front: '', primary: '' })])
    expect(result).toEqual({
      ok: false,
      problems: [{ cardId: 'a', index: 0, missing: ['front', 'primary'] }],
    })
  })
})
