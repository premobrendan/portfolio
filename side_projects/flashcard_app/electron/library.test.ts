import { describe, it, expect, beforeEach } from 'vitest'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import {
  libraryPointerPath, writeLibraryPointer, recordLibraryPointer, resolveLibraryRoot,
} from './library'

let home: string
beforeEach(() => {
  home = fs.mkdtempSync(path.join(os.tmpdir(), 'fc-home-'))
})

const env = (over: Record<string, string> = {}) => ({ HOME: home, ...over })

describe('resolveLibraryRoot', () => {
  it('prefers the explicit env var', async () => {
    expect(await resolveLibraryRoot([], env({ FLASHCARDS_USER_DATA: '/tmp/explicit' })))
      .toBe('/tmp/explicit')
  })

  it('accepts --root from the command line', async () => {
    expect(await resolveLibraryRoot(['--root', '/tmp/flag'], env())).toBe('/tmp/flag')
  })

  it('falls back to the pointer the app wrote', async () => {
    await writeLibraryPointer('/tmp/from-app', env())
    expect(await resolveLibraryRoot([], env())).toBe('/tmp/from-app')
  })

  it('refuses to guess when nothing points anywhere', async () => {
    await expect(resolveLibraryRoot([], env())).rejects.toThrow(/launch the Flashcards app/i)
  })
})

describe('libraryPointerPath', () => {
  it('lives at a fixed location independent of the app name', () => {
    expect(libraryPointerPath(env())).toBe(path.join(home, '.flashcards', 'library.json'))
  })
})

describe('recordLibraryPointer', () => {
  it('records the location when the app is using its real profile', async () => {
    await recordLibraryPointer('/real/library', env())
    expect(await resolveLibraryRoot([], env())).toBe('/real/library')
  })

  it('does NOT record a profile that was overridden for a test run', async () => {
    // Every Playwright launch sets FLASHCARDS_USER_DATA to a throwaway directory. Writing
    // that to the pointer would repoint the MCP server at a temp profile that is then
    // deleted — the user's library would appear empty, or worse, wrong.
    await recordLibraryPointer('/real/library', env())
    await recordLibraryPointer('/tmp/throwaway-test-profile', env({
      FLASHCARDS_USER_DATA: '/tmp/throwaway-test-profile',
    }))

    const pointer = JSON.parse(fs.readFileSync(libraryPointerPath(env()), 'utf8'))
    expect(pointer.root).toBe('/real/library')
  })

  it('never throws, so a read-only home cannot stop the app starting', async () => {
    await expect(recordLibraryPointer('/x', { HOME: '/nonexistent/\0/bad' }))
      .resolves.toBeUndefined()
  })
})
