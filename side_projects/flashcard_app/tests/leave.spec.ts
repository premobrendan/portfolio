import { test, expect } from '@playwright/test'
import { launchApp } from './helpers'

test('Escape twice leaves; Escape then Space stays', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Settings' }).click()
  await expect(win.getByTestId('screen-settings')).toBeVisible()

  await win.keyboard.press('Escape')
  await expect(win.getByTestId('confirm-dialog')).toBeVisible()

  await win.keyboard.press('Space')
  await expect(win.getByTestId('confirm-dialog')).toBeHidden()
  await expect(win.getByTestId('screen-settings')).toBeVisible()

  await win.keyboard.press('Escape')
  await win.keyboard.press('Escape')
  await expect(win.getByTestId('title-menu')).toBeVisible()

  await app.close()
})

test('Enter also denies the confirmation', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Make a Pack' }).click()
  await win.keyboard.press('Escape')
  await win.keyboard.press('Enter')
  await expect(win.getByTestId('confirm-dialog')).toBeHidden()
  await expect(win.getByTestId('screen-makepack')).toBeVisible()
  await app.close()
})

test('each screen asks its own question', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Settings' }).click()
  await win.keyboard.press('Escape')
  await expect(win.getByTestId('confirm-dialog')).toContainText('Save settings before leaving?')
  await win.keyboard.press('Escape')

  await win.getByRole('button', { name: 'Make a Pack' }).click()
  await win.keyboard.press('Escape')
  await expect(win.getByTestId('confirm-dialog')).toContainText('Are you sure you want to leave?')

  await app.close()
})
