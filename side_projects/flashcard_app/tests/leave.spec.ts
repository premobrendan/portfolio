import { test, expect, type Page } from '@playwright/test'
import { launchApp, launchWithFixture } from './helpers'

/**
 * `toBeHidden` only proves the dialog is gone by the time the assertion runs — a
 * confirmation that flashed up and was dismissed would still pass. These two record
 * every appearance from the moment the watch is installed, so a screen that must never
 * ask can be held to exactly that.
 */
type Watched = Window & { __confirmSeen?: boolean }

async function watchForConfirm(win: Page): Promise<void> {
  await win.evaluate(() => {
    const w = window as Watched
    w.__confirmSeen = document.querySelector('[data-testid="confirm-dialog"]') !== null
    new MutationObserver(() => {
      if (document.querySelector('[data-testid="confirm-dialog"]')) w.__confirmSeen = true
    }).observe(document.body, { childList: true, subtree: true })
  })
}

async function confirmEverAppeared(win: Page): Promise<boolean> {
  return win.evaluate(() => (window as Watched).__confirmSeen === true)
}

// ------------------------------------------------------------------ Title: quits outright

test('Escape on the title screen quits the app', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  await expect(win.getByTestId('title-menu')).toBeVisible()

  // `press` is a key-down plus a key-up: the app is already closing when the up is sent,
  // so the down is dispatched on its own and the close event is awaited instead.
  const closed = app.waitForEvent('close')
  await win.keyboard.down('Escape')
  await closed
})

test('a third Escape out of Learn quits: confirm, land on Title, quit', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()

  await win.keyboard.press('Escape')                      // raises the confirmation
  await expect(win.getByTestId('confirm-dialog')).toBeVisible()
  await win.keyboard.press('Escape')                      // confirms — back to Title
  await expect(win.getByTestId('title-menu')).toBeVisible()
  const closed = app.waitForEvent('close')
  await win.keyboard.down('Escape')                       // Title policy: quit
  await closed
})

// ---------------------------------------------------------------- Learn: always asks

test('Learn confirms on Escape, and a second Escape returns to the menu', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()

  await win.keyboard.press('Escape')
  await expect(win.getByTestId('confirm-dialog')).toBeVisible()
  await expect(win.getByTestId('confirm-dialog')).toContainText('Return to Main Menu?')

  await win.keyboard.press('Escape')
  await expect(win.getByTestId('title-menu')).toBeVisible()

  await app.close()
})

test('Space denies the Learn confirmation and stays in the session', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()

  await win.keyboard.press('Escape')
  await expect(win.getByTestId('confirm-dialog')).toBeVisible()
  await win.keyboard.press('Space')
  await expect(win.getByTestId('confirm-dialog')).toBeHidden()
  await expect(win.getByTestId('card')).toBeVisible()

  await app.close()
})

test('Enter denies the Learn confirmation and stays in the session', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()

  await win.keyboard.press('Escape')
  await expect(win.getByTestId('confirm-dialog')).toBeVisible()
  await win.keyboard.press('Enter')
  await expect(win.getByTestId('confirm-dialog')).toBeHidden()
  await expect(win.getByTestId('card')).toBeVisible()

  await app.close()
})

test('the Learn leave button asks the same question as Escape', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()

  await win.getByTestId('leave-btn').click()
  await expect(win.getByTestId('confirm-dialog')).toContainText('Return to Main Menu?')
  await win.getByTestId('confirm-yes').click()
  await expect(win.getByTestId('title-menu')).toBeVisible()

  await app.close()
})

// ------------------------------------------- Clean screens: nothing to lose, no question

test('Settings leaves immediately, by key and by button, without ever asking', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  await watchForConfirm(win)

  await win.getByRole('button', { name: 'Settings' }).click()
  await expect(win.getByTestId('screen-settings')).toBeVisible()
  await win.keyboard.press('Escape')
  await expect(win.getByTestId('title-menu')).toBeVisible()

  await win.getByRole('button', { name: 'Settings' }).click()
  await expect(win.getByTestId('screen-settings')).toBeVisible()
  await win.getByTestId('leave-btn').click()
  await expect(win.getByTestId('title-menu')).toBeVisible()

  expect(await confirmEverAppeared(win)).toBe(false)
  await app.close()
})

test('Make a Pack leaves immediately, by key and by button, without ever asking', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  await watchForConfirm(win)

  await win.getByRole('button', { name: 'Make a Pack' }).click()
  await expect(win.getByTestId('screen-makepack')).toBeVisible()
  await win.keyboard.press('Escape')
  await expect(win.getByTestId('title-menu')).toBeVisible()

  await win.getByRole('button', { name: 'Make a Pack' }).click()
  await expect(win.getByTestId('screen-makepack')).toBeVisible()
  await win.getByTestId('leave-btn').click()
  await expect(win.getByTestId('title-menu')).toBeVisible()

  expect(await confirmEverAppeared(win)).toBe(false)
  await app.close()
})

test('Pack Selection leaves immediately, by key and by button, without ever asking', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  await watchForConfirm(win)

  await win.getByRole('button', { name: 'Pack Selection' }).click()
  await expect(win.getByTestId('screen-packs')).toBeVisible()
  await win.keyboard.press('Escape')
  await expect(win.getByTestId('title-menu')).toBeVisible()

  await win.getByRole('button', { name: 'Pack Selection' }).click()
  await expect(win.getByTestId('screen-packs')).toBeVisible()
  await win.getByTestId('leave-btn').click()
  await expect(win.getByTestId('title-menu')).toBeVisible()

  expect(await confirmEverAppeared(win)).toBe(false)
  await app.close()
})
