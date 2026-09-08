import { describe, it, expect } from 'vitest'
import { renderMarkdown } from './markdown'

describe('renderMarkdown', () => {
  it('renders basic markdown', () => {
    expect(renderMarkdown('# Title')).toContain('<h1')
    expect(renderMarkdown('**bold**')).toContain('<strong>')
  })

  it('renders tables, which packs will need later', () => {
    const md = '| a | b |\n| - | - |\n| 1 | 2 |'
    expect(renderMarkdown(md)).toContain('<table')
  })

  it('strips script tags', () => {
    expect(renderMarkdown('<script>alert(1)</script>')).not.toContain('<script')
  })

  it('strips inline event handlers', () => {
    expect(renderMarkdown('<img src=x onerror="alert(1)">')).not.toContain('onerror')
  })

  it('handles empty input', () => {
    expect(renderMarkdown('')).toBe('')
  })
})
