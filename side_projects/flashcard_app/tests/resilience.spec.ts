import { test, expect } from '@playwright/test'
import fs from 'node:fs'
import path from 'node:path'
import { FIXTURE, launchApp, makeUserDataDir, seedPack } from './helpers'

/**
 * The §5.4 failure table — the storage faults a user can actually hit. Every profile here
 * is deliberately damaged before launch, and the assertion is always the same shape: the
 * app still comes up, the damage is contained, and whatever it repaired is on disk.
 */

const readStateFile = (userData: string) =>
  JSON.parse(fs.readFileSync(path.join(userData, 'state.json'), 'utf8')) as {
    selectedPackIds: string[]
    hasLaunchedBefore: boolean
  }

test('a malformed pack is skipped and the good ones still load', async () => {
  const userData = seedPack(makeUserDataDir())
  // Two distinct kinds of broken: unparseable meta.json, and a directory with no meta at all.
  const broken = path.join(userData, 'packs', 'broken')
  fs.mkdirSync(broken, { recursive: true })
  fs.writeFileSync(path.join(broken, 'meta.json'), '{ not json')
  fs.mkdirSync(path.join(userData, 'packs', 'empty'), { recursive: true })

  const { app } = await launchApp(userData)
  const win = await app.firstWindow()
  const packs = await win.evaluate(() => window.flashcards.packs.list())
  expect(packs.map(p => p.id)).toEqual([FIXTURE.id])
  // The good pack is still selected, so studying is not blocked by the bad ones.
  await expect(win.getByRole('button', { name: 'Learn' })).toBeEnabled()
  await app.close()
})

test('malformed state.json falls back to defaults instead of blocking launch', async () => {
  const userData = makeUserDataDir()
  fs.writeFileSync(path.join(userData, 'state.json'), 'garbage')

  const { app } = await launchApp(userData)
  const win = await app.firstWindow()
  await expect(win.getByTestId('title-menu')).toBeVisible()

  // Defaults means "every pack selected", and the rewrite reaches disk as valid JSON.
  const packs = await win.evaluate(() => window.flashcards.packs.list())
  const state = await win.evaluate(() => window.flashcards.state.get())
  expect(state.hasLaunchedBefore).toBe(true)
  expect(state.selectedPackIds).toEqual(packs.map(p => p.id))
  expect(readStateFile(userData)).toEqual(state)
  await app.close()
})

test('corrupting state.json on an existing profile keeps its packs and reselects them all', async () => {
  const userData = seedPack(makeUserDataDir())
  fs.writeFileSync(path.join(userData, 'state.json'), '{"selectedPackIds": 42')

  const { app } = await launchApp(userData)
  const win = await app.firstWindow()
  await expect(win.getByTestId('title-menu')).toBeVisible()

  const packs = await win.evaluate(() => window.flashcards.packs.list())
  const state = await win.evaluate(() => window.flashcards.state.get())
  expect(packs.map(p => p.id)).toContain(FIXTURE.id)
  expect(state.selectedPackIds).toEqual(packs.map(p => p.id))
  // The user's own pack is untouched by the recovery — no new version appended.
  expect(packs.find(p => p.id === FIXTURE.id)?.currentVersion).toBe(1)
  await app.close()
})

test('a selection naming a missing pack is cleaned up on load and persisted', async () => {
  const userData = seedPack(makeUserDataDir())
  fs.writeFileSync(path.join(userData, 'state.json'), JSON.stringify({
    selectedPackIds: [FIXTURE.id, 'ghost'], hasLaunchedBefore: true,
  }))

  const { app } = await launchApp(userData)
  const win = await app.firstWindow()
  // The title menu renders only after the first load has finished, and that load awaits
  // the cleanup write before it commits — so once this is visible, disk is settled.
  await expect(win.getByTestId('title-menu')).toBeVisible()
  expect((await win.evaluate(() => window.flashcards.state.get())).selectedPackIds)
    .toEqual([FIXTURE.id])
  expect(readStateFile(userData).selectedPackIds).toEqual([FIXTURE.id])
  await app.close()

  // And a real relaunch reads the cleaned list back, with nothing left to repair.
  const second = await launchApp(userData)
  const w2 = await second.app.firstWindow()
  await expect(w2.getByTestId('title-menu')).toBeVisible()
  expect((await w2.evaluate(() => window.flashcards.state.get())).selectedPackIds)
    .toEqual([FIXTURE.id])
  await second.app.close()
})
