import { test, expect } from '@playwright/test'
import { launchApp } from './helpers'

test('title screen shows all five buttons in order', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  // toHaveText retries; allInnerTexts() is a one-shot read that returns [] if
  // first-run seeding has not resolved and App is still rendering its empty shell.
  await expect(win.getByTestId('title-menu').getByRole('button')).toHaveText(
    ['Learn', 'Pack Selection', 'Make a Pack', 'Settings', 'Quit'],
  )
  await app.close()
})

test('Learn is disabled when no packs are selected', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  await win.evaluate(() => window.flashcards.state.set({ selectedPackIds: [] }))
  await win.getByTestId('btn-reload').click()
  await expect(win.getByRole('button', { name: 'Learn' })).toBeDisabled()
  await app.close()
})

test('the Quit button closes the app', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  // `exact` matters: the top-left leave button is labelled "Quit application",
  // and getByRole name matching is substring by default.
  await win.getByRole('button', { name: 'Quit', exact: true }).click()
  await app.waitForEvent('close')
})
