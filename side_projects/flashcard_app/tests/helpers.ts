import { _electron as electron, type ElectronApplication } from '@playwright/test'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

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
