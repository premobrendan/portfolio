import { marked } from 'marked'
import DOMPurify from 'dompurify'

marked.setOptions({ gfm: true, breaks: true })

/**
 * Every surviving link is marked to open in a new context rather than in place.
 *
 * This is what lets the main process intercept it (see electron/main.ts) and hand it to the
 * user's real browser. Without it, clicking a source link navigates the app's own window to
 * the web — and the app has no address bar and no back button, so there is no way home.
 *
 * `rel` is not optional: a `target="_blank"` link without `noopener` grants the opened page
 * a handle back to this window, and card content is untrusted.
 *
 * Registered once at module scope — DOMPurify hooks are global, so adding it per call would
 * stack a new copy on every render.
 */
DOMPurify.addHook('afterSanitizeAttributes', node => {
  if (node.tagName === 'A') {
    node.setAttribute('target', '_blank')
    node.setAttribute('rel', 'noopener noreferrer')
  }
})

/**
 * Parse Markdown and sanitize the result. Pack files will eventually be written by an
 * MCP server, so every card's More Info is untrusted input: it is parsed here and
 * scrubbed of scripts and event handlers before anything renders it.
 */
export function renderMarkdown(src: string): string {
  if (!src) return ''
  return DOMPurify.sanitize(marked.parse(src, { async: false }) as string)
}
