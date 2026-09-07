import { _electron as electron, type ElectronApplication } from '@playwright/test'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import type { Pack } from '../shared/types'

export type Session = { app: ElectronApplication; userData: string }

export function makeUserDataDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'flashcards-test-'))
}

/** Launch the built app against an isolated profile. Reuse `userData` to test relaunch. */
export async function launchApp(userData = makeUserDataDir()): Promise<Session> {
  const app = await electron.launch({
    args: ['.'],
    env: { ...process.env, FLASHCARDS_USER_DATA: userData, NODE_ENV: 'test' },
  })
  return { app, userData }
}

/** A pack with known shape: two cards WITH moreInfo, one deliberately without. */
export const FIXTURE: Pack = {
  id: 'fixture',
  name: 'Fixture Pack',
  schemaVersion: 1,
  cards: [
    { id: 'f1', front: 'Front One', primary: 'Primary One', moreInfo: '# Heading\n\nBody text.' },
    { id: 'f2', front: 'Front Two', primary: 'Primary Two', moreInfo: 'Second card detail.' },
    { id: 'f3', front: 'Front Three', primary: 'Primary Three' },
  ],
}

/**
 * Write a pack straight to disk and mark the profile as already launched, so
 * `seedIfEmpty` does not also install the shipped packs. Gives a test an exact deck.
 */
export function seedPack(userData: string, pack: Pack = FIXTURE): string {
  const dir = path.join(userData, 'packs', pack.id)
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(path.join(dir, 'v1.json'), JSON.stringify({ cards: pack.cards }))
  fs.writeFileSync(path.join(dir, 'meta.json'), JSON.stringify({
    id: pack.id, name: pack.name, cardCount: pack.cards.length,
    currentVersion: 1, schemaVersion: 1,
  }))
  fs.writeFileSync(path.join(userData, 'state.json'), JSON.stringify({
    selectedPackIds: [pack.id], hasLaunchedBefore: true,
  }))
  return userData
}

/** Launch against a profile preloaded with the fixture pack. */
export async function launchWithFixture(pack: Pack = FIXTURE): Promise<Session> {
  return launchApp(seedPack(makeUserDataDir(), pack))
}
