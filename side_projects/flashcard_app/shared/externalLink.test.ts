import { describe, it, expect } from 'vitest'
import { isSafeExternalUrl } from './externalLink'

describe('isSafeExternalUrl', () => {
  it('allows the web links a source citation would use', () => {
    expect(isSafeExternalUrl('https://en.wikipedia.org/wiki/Q-learning')).toBe(true)
    expect(isSafeExternalUrl('http://example.org/paper.pdf')).toBe(true)
  })

  it('refuses schemes that would run code or read the disk', () => {
    // Pack content is untrusted — it can be written by an MCP server, i.e. by a model.
    expect(isSafeExternalUrl('javascript:alert(1)')).toBe(false)
    expect(isSafeExternalUrl('file:///etc/passwd')).toBe(false)
    expect(isSafeExternalUrl('data:text/html,<script>alert(1)</script>')).toBe(false)
    expect(isSafeExternalUrl('vbscript:msgbox(1)')).toBe(false)
  })

  it('refuses anything that is not a parseable absolute URL', () => {
    expect(isSafeExternalUrl('not a url')).toBe(false)
    expect(isSafeExternalUrl('')).toBe(false)
    expect(isSafeExternalUrl('/relative/path')).toBe(false)
  })
})
