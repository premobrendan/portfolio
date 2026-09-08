import { test, expect } from '@playwright/test'
import { launchWithFixture } from './helpers'

test('preview flips cards without starting a session', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Pack Selection' }).click()
  await win.getByTestId('pack-preview-fixture').click()

  await expect(win.getByTestId('card')).toBeVisible()
  await expect(win.getByTestId('advance-arrow')).toBeHidden()

  await win.getByTestId('card').click()
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'true')

  await app.close()
})

test('preview steps through cards and closes back to the list', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Pack Selection' }).click()
  await win.getByTestId('pack-preview-fixture').click()

  // Preview walks the pack in FILE order — it is not shuffled — so the exact ids are a
  // safe assertion here in a way they never are on the Learn screen.
  await expect(win.getByTestId('card')).toHaveAttribute('data-card-id', 'f1')
  await win.getByTestId('preview-next').click()
  await expect(win.getByTestId('card')).toHaveAttribute('data-card-id', 'f2')
  await win.getByTestId('preview-prev').click()
  await expect(win.getByTestId('card')).toHaveAttribute('data-card-id', 'f1')

  await win.getByTestId('preview-close').click()
  await expect(win.getByTestId('screen-packs')).toBeVisible()

  await app.close()
})

test('Escape closes the preview first, and only then leaves for Title', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Pack Selection' }).click()
  await win.getByTestId('pack-preview-fixture').click()
  await expect(win.getByTestId('card')).toBeVisible()

  // One Escape pops exactly one thing: the preview, not the screen.
  await win.keyboard.press('Escape')
  await expect(win.getByTestId('screen-packs')).toBeVisible()
  await expect(win.getByTestId('card')).toBeHidden()

  // A second Escape, with no preview open, is the screen's own immediate leave.
  await win.keyboard.press('Escape')
  await expect(win.getByTestId('title-menu')).toBeVisible()

  await app.close()
})
