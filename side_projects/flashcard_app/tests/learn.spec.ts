import { test, expect } from '@playwright/test'
import { launchWithFixture } from './helpers'

test('the advance arrow is absent before flip and present after', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()

  await expect(win.getByTestId('card')).toBeVisible()
  await expect(win.getByTestId('advance-arrow')).toBeHidden()
  await win.getByTestId('card').click()
  await expect(win.getByTestId('advance-arrow')).toBeVisible()

  await app.close()
})

test('Enter does nothing before flip and advances after', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()

  const first = await win.getByTestId('card').getAttribute('data-card-id')
  await win.keyboard.press('Enter')
  expect(await win.getByTestId('card').getAttribute('data-card-id')).toBe(first)

  await win.keyboard.press('Space')
  await win.keyboard.press('Enter')
  await expect(win.getByTestId('card')).not.toHaveAttribute('data-card-id', first!)

  await app.close()
})

test('the arrow stays visible after flipping back to the front', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()

  await win.getByTestId('card').click()
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'true')
  await win.getByTestId('card').click()
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'false')
  await expect(win.getByTestId('advance-arrow')).toBeVisible()

  await app.close()
})

test('exhausting the deck shows the end screen with no score', async () => {
  const { app } = await launchWithFixture()   // exactly 3 cards
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()

  for (let i = 0; i < 3; i++) {
    await win.keyboard.press('Space')
    await win.keyboard.press('Enter')
  }

  await expect(win.getByTestId('session-end')).toBeVisible()
  await expect(win.getByTestId('session-end')).not.toContainText('/')
  await expect(win.getByTestId('session-end')).not.toContainText(/\d/)
  await win.getByTestId('reshuffle-btn').click()
  await expect(win.getByTestId('card')).toBeVisible()
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'false')
  await expect(win.getByTestId('advance-arrow')).toBeHidden()

  await app.close()
})
