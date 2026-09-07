import { test, expect } from '@playwright/test'
import { launchApp } from './helpers'

test('every pack is selected on first launch', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Pack Selection' }).click()
  await expect(win.getByTestId('pack-toggle-presidents')).toBeChecked()
  await app.close()
})

test('toggling a pack updates the selection', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Pack Selection' }).click()
  await win.getByTestId('pack-toggle-presidents').uncheck()
  await expect(win.getByTestId('pack-toggle-presidents')).not.toBeChecked()
  await app.close()
})

test('selection survives a real relaunch', async () => {
  const first = await launchApp()
  const w1 = await first.app.firstWindow()
  await w1.getByRole('button', { name: 'Pack Selection' }).click()
  await w1.getByTestId('pack-toggle-presidents').uncheck()
  await w1.waitForTimeout(200)          // let the write land
  await first.app.close()

  const second = await launchApp(first.userData)   // same profile
  const w2 = await second.app.firstWindow()
  await w2.getByRole('button', { name: 'Pack Selection' }).click()
  await expect(w2.getByTestId('pack-toggle-presidents')).not.toBeChecked()
  await second.app.close()
})
