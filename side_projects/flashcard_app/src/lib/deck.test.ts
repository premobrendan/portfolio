import { describe, it, expect } from 'vitest'
import { buildDeck, viewKey, frontText, backText, backTexts } from './deck'
import type { Card } from '../../shared/types'

const plain: Card = { id: 'a', front: 'John Adams', primary: 'He was the 2nd president.' }
const many: Card = {
  id: 'b',
  front: 'Abraham Lincoln',
  primary: 'He was the 16th president.',
  secondaries: [
    'He issued the Emancipation Proclamation.',
    'He delivered the Gettysburg Address.',
  ],
  moreInfo: '# Lincoln',
}

/** Deterministic stand-in for Math.random, cycling through fixed values. */
const seeded = (...values: number[]) => {
  let i = 0
  return () => values[i++ % values.length]
}

describe('backTexts', () => {
  it('is just the primary when a card has nothing else', () => {
    expect(backTexts(plain)).toEqual(['He was the 2nd president.'])
  })

  it('puts the primary first, then every secondary in order', () => {
    expect(backTexts(many)).toEqual([
      'He was the 16th president.',
      'He issued the Emancipation Proclamation.',
      'He delivered the Gettysburg Address.',
    ])
  })

  it('ignores blank secondaries rather than offering an empty back', () => {
    expect(backTexts({ ...plain, secondaries: ['', '   ', 'Real one.'] }))
      .toEqual(['He was the 2nd president.', 'Real one.'])
  })
})

describe('one view per card, always', () => {
  it.each(['forward', 'reverse', 'random'] as const)('%s gives exactly one', mode => {
    expect(buildDeck([plain, many], mode, 'primary', seeded(0.1))).toHaveLength(2)
  })

  it('never copies the card — the view points at the original', () => {
    expect(buildDeck([many], 'forward', 'primary', seeded(0))[0].card).toBe(many)
  })
})

describe('direction', () => {
  it('forward shows the prompt first', () => {
    expect(buildDeck([plain], 'forward', 'primary', seeded(0))[0].direction).toBe('forward')
  })

  it('reverse shows the fact first', () => {
    expect(buildDeck([plain], 'reverse', 'primary', seeded(0))[0].direction).toBe('reverse')
  })

  it('random picks a side by chance', () => {
    expect(buildDeck([plain], 'random', 'primary', seeded(0.1))[0].direction).toBe('forward')
    expect(buildDeck([plain], 'random', 'primary', seeded(0.9))[0].direction).toBe('reverse')
  })
})

describe('back view', () => {
  it('primary-only always takes the first text, however many exist', () => {
    for (const r of [0.0, 0.4, 0.99]) {
      expect(buildDeck([many], 'forward', 'primary', seeded(r))[0].textIndex).toBe(0)
    }
  })

  it('random can reach a secondary', () => {
    // Three texts: an rng near 1 selects the last.
    expect(buildDeck([many], 'forward', 'random', seeded(0.99))[0].textIndex).toBe(2)
  })

  it('random on a card with only a primary still picks the primary', () => {
    expect(buildDeck([plain], 'forward', 'random', seeded(0.99))[0].textIndex).toBe(0)
  })
})

describe('the faces a view shows', () => {
  it('puts the prompt first going forward', () => {
    const v = { card: many, direction: 'forward' as const, textIndex: 0 }
    expect(frontText(v)).toBe('Abraham Lincoln')
    expect(backText(v)).toBe('He was the 16th president.')
  })

  it('puts the chosen fact first going backward', () => {
    const v = { card: many, direction: 'reverse' as const, textIndex: 2 }
    expect(frontText(v)).toBe('He delivered the Gettysburg Address.')
    expect(backText(v)).toBe('Abraham Lincoln')
  })

  it('falls back to the primary if the index is out of range', () => {
    const v = { card: plain, direction: 'forward' as const, textIndex: 7 }
    expect(backText(v)).toBe('He was the 2nd president.')
  })
})

describe('viewKey', () => {
  it('distinguishes views of the same card', () => {
    const a = viewKey({ card: many, direction: 'forward', textIndex: 0 })
    const b = viewKey({ card: many, direction: 'reverse', textIndex: 1 })
    expect(a).not.toBe(b)
  })

  it('does not alter the card id to achieve that', () => {
    expect(buildDeck([many], 'random', 'random', seeded(0.9))[0].card.id).toBe('b')
  })
})

describe('an empty pool', () => {
  it('produces an empty deck in every combination', () => {
    for (const d of ['forward', 'reverse', 'random'] as const) {
      for (const b of ['primary', 'random'] as const) {
        expect(buildDeck([], d, b, seeded(0))).toEqual([])
      }
    }
  })
})
