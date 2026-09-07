import { describe, it, expect } from 'vitest'
import { shuffle } from './shuffle'

describe('shuffle', () => {
  it('returns a new array and leaves the input untouched', () => {
    const input = [1, 2, 3, 4, 5]
    const out = shuffle(input)
    expect(out).not.toBe(input)
    expect(input).toEqual([1, 2, 3, 4, 5])
  })

  it('preserves every element', () => {
    const out = shuffle([1, 2, 3, 4, 5])
    expect([...out].sort()).toEqual([1, 2, 3, 4, 5])
  })

  it('is deterministic for a fixed rng', () => {
    const fixed = () => 0.5
    expect(shuffle([1, 2, 3, 4, 5], fixed)).toEqual(shuffle([1, 2, 3, 4, 5], fixed))
  })

  it('handles empty and single-element arrays', () => {
    expect(shuffle([])).toEqual([])
    expect(shuffle([7])).toEqual([7])
  })
})
