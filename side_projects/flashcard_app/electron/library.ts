import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'

/**
 * Where the app records which directory holds the library.
 *
 * The MCP server cannot ask Electron for `app.getPath('userData')`, and it must not guess:
 * that path is derived from the app NAME, which differs between a dev run (`flashcards`,
 * from package.json) and a packaged build (`Flashcards`, which electron-builder writes in).
 * Guessing wrong would silently create a second, empty library rather than fail — the worst
 * possible outcome, because it looks like the app lost your packs. So the app writes the
 * answer to a fixed, name-independent location on every launch.
 */
export function libraryPointerPath(env: NodeJS.ProcessEnv = process.env): string {
  const home = env.HOME || env.USERPROFILE || os.homedir()
  return path.join(home, '.flashcards', 'library.json')
}

export async function writeLibraryPointer(
  root: string, env: NodeJS.ProcessEnv = process.env,
): Promise<void> {
  const file = libraryPointerPath(env)
  await fs.mkdir(path.dirname(file), { recursive: true })
  await fs.writeFile(file, JSON.stringify({ root }, null, 2), 'utf8')
}

/** env var > --root flag > pointer file > a clear error. Never a guess. */
export async function resolveLibraryRoot(
  argv: string[] = process.argv.slice(2), env: NodeJS.ProcessEnv = process.env,
): Promise<string> {
  if (env.FLASHCARDS_USER_DATA) return env.FLASHCARDS_USER_DATA

  const flag = argv.indexOf('--root')
  if (flag !== -1 && argv[flag + 1]) return argv[flag + 1]

  try {
    const pointer = JSON.parse(
      await fs.readFile(libraryPointerPath(env), 'utf8'),
    ) as { root?: string }
    if (pointer.root) return pointer.root
  } catch {
    // Missing or malformed pointer: fall through to the actionable error below.
  }

  throw new Error(
    'Could not find the flashcard library. Launch the Flashcards app once so it can record '
    + `its location (expected a pointer at ${libraryPointerPath(env)}), or start this server `
    + 'with --root /path/to/library.',
  )
}

/**
 * Records the library location for the MCP server — but ONLY when the app is running on its
 * real profile.
 *
 * `FLASHCARDS_USER_DATA` being set means somebody deliberately pointed the app at a
 * different profile, and in practice that somebody is the Playwright harness, which uses a
 * fresh temp directory per test and deletes it afterwards. Writing that path here would
 * repoint the MCP server at a profile that no longer exists, so a test run would silently
 * disconnect the server from the user's real library. Anything driving the app with an
 * override can pass the same variable to the server.
 *
 * Never throws: a read-only home directory is not a reason to fail to start.
 */
export async function recordLibraryPointer(
  root: string, env: NodeJS.ProcessEnv = process.env,
): Promise<void> {
  if (env.FLASHCARDS_USER_DATA) return
  try {
    await writeLibraryPointer(root, env)
  } catch {
    // The server can still be started with --root; losing the convenience is not fatal.
  }
}
