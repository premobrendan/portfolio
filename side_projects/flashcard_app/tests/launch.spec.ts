import { test, expect } from '@playwright/test'
import { launchApp } from './helpers'

test('app launches and renders a window', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  await expect(win.getByTestId('app-root')).toBeVisible()
  await app.close()
})
