import { marked } from 'marked'
import DOMPurify from 'dompurify'

marked.setOptions({ gfm: true, breaks: true })

/**
 * Parse Markdown and sanitize the result. Pack files will eventually be written by an
 * MCP server, so every card's More Info is untrusted input: it is parsed here and
 * scrubbed of scripts and event handlers before anything renders it.
 */
export function renderMarkdown(src: string): string {
  if (!src) return ''
  return DOMPurify.sanitize(marked.parse(src, { async: false }) as string)
}
