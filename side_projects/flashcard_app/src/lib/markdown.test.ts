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

  it('marks links to open outside the app', () => {
    // Without target/rel, clicking a source link navigates the app window itself to the
    // web — no address bar, no back button, no way home.
    const html = renderMarkdown('[Wikipedia](https://en.wikipedia.org/wiki/Q-learning)')
    expect(html).toContain('target="_blank"')
    expect(html).toContain('rel="noopener noreferrer"')
  })

  it('keeps the href of an ordinary web link', () => {
    expect(renderMarkdown('[x](https://example.org/a)')).toContain('https://example.org/a')
  })

  it('strips a javascript: link entirely', () => {
    const html = renderMarkdown('[click](javascript:alert(1))')
    expect(html).not.toContain('javascript:')
  })
})