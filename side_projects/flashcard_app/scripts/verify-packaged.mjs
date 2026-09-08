#!/usr/bin/env node
/**
 * Launches the PACKAGED binary (not `electron .`) against an empty profile and checks
 * the one thing the test suite cannot: that a first launch of the installed app really
 * seeds its bundled packs from `process.resourcesPath/packs` (see resourcesDir() in
 * electron/main.ts). A build that silently seeds nothing is invisible until someone
 * runs the installer, so run this after `npm run dist`:
 *
 *   node scripts/verify-packaged.mjs release/mac-arm64/Flashcards.app/Contents/MacOS/Flashcards
 *
 * Also prints app.getName() / userData as the packaged build resolves them, so a dev
 * build and an installed build can be confirmed to share one profile directory.
 */
import { _electron as electron } from '@playwright/test'
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'

const exe = process.argv[2]
if (!exe || !fs.existsSync(exe)) { console.error('usage: node packaged-probe.mjs <executable>'); process.exit(2) }

const userData = fs.mkdtempSync(path.join(os.tmpdir(), 'flashcards-packaged-'))
const app = await electron.launch({
  executablePath: exe, args: [],
  env: { ...process.env, FLASHCARDS_USER_DATA: userData, FLASHCARDS_HIDDEN: '1' },
})
const win = await app.firstWindow()
await win.getByTestId('title-menu').waitFor({ timeout: 20000 })

const main = await app.evaluate(({ app }) => ({
  isPackaged: app.isPackaged,
  resourcesPath: process.resourcesPath,
  name: app.getName(),
  userData: app.getPath('userData'),
  version: app.getVersion(),
}))
console.log('main process:', main)
console.log('resourcesPath/packs on disk:', fs.readdirSync(path.join(main.resourcesPath, 'packs')))

const packs = await win.evaluate(() => window.flashcards.packs.list())
const state = await win.evaluate(() => window.flashcards.state.get())
console.log('packs.list():', packs.map(p => `${p.id} (${p.cardCount} cards, v${p.currentVersion})`))
console.log('state.get():', state)
console.log('profile on disk:', fs.readdirSync(path.join(userData, 'packs')), fs.existsSync(path.join(userData, 'state.json')) ? 'state.json present' : 'NO state.json')
console.log('Learn enabled:', await win.getByRole('button', { name: 'Learn' }).isEnabled())

await app.close()
// Electron's default userData is <appData>/<app.getName()>; report it without launching
// an un-overridden instance (which would seed the real profile as a side effect).
console.log('default userData would be: <appData>/' + main.name)
console.log(packs.length > 0 && state.selectedPackIds.length === packs.length ? 'SEED OK' : 'SEED FAILED')
