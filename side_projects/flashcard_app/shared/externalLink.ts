/**
 * Whether a URL from card content may be handed to the OS browser.
 *
 * Pack content is untrusted — packs can be written by an MCP server, which means by a
 * model — so a link is opened only if it is an absolute web URL. Anything else (file:,
 * data:, javascript:, custom schemes) is refused rather than passed to the shell, where it
 * could read the disk or launch a handler.
 */
export function isSafeExternalUrl(url: string): boolean {
  let parsed: URL
  try {
    parsed = new URL(url)
  } catch {
    // Relative paths and junk land here: not absolute, so not openable.
    return false
  }
  return parsed.protocol === 'https:' || parsed.protocol === 'http:'
}
