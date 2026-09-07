import { test, expect } from '@playwright/test'
import { launchApp } from './helpers'

test('first run seeds the presidents pack and selects everything', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  const packs = await win.evaluate(() => window.flashcards.packs.list())
  expect(packs.map((p: { id: string }) => p.id)).toContain('presidents')

  const state = await win.evaluate(() => window.flashcards.state.get())
  expect(state.hasLaunchedBefore).toBe(true)
  expect(state.selectedPackIds).toContain('presidents')

  await app.close()
})

test('renderer has no Node access', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  expect(await win.evaluate(() => typeof (window as never as { require?: unknown }).require))
    .toBe('undefined')
  await app.close()
})
