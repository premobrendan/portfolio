import { test, expect } from '@playwright/test'
import { launchApp, launchWithFixture, makeUserDataDir, seedPack } from './helpers'

// --------------------------------------------------------------- the setting itself

test('an untouched Settings screen still leaves without asking', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Settings' }).click()
  await expect(win.getByTestId('screen-settings')).toBeVisible()
  await win.keyboard.press('Escape')
  await expect(win.getByTestId('title-menu')).toBeVisible()

  await app.close()
})

test('changing a setting and leaving asks, and says what is lost', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Settings' }).click()
  await win.getByTestId('direction-reverse').check()
  await expect(win.getByTestId('settings-status')).toContainText('Unsaved')

  await win.keyboard.press('Escape')
  await expect(win.getByTestId('confirm-dialog')).toContainText('Leave without saving?')
  await expect(win.getByTestId('confirm-dialog')).toContainText('will be lost')

  // Denying keeps you on the screen with the change intact.
  await win.getByTestId('confirm-no').click()
  await expect(win.getByTestId('direction-reverse')).toBeChecked()

  await app.close()
})

test('a saved setting is clean, and survives a real relaunch', async () => {
  const userData = makeUserDataDir()
  const first = await launchApp(userData)
  let win = await first.app.firstWindow()

  await win.getByRole('button', { name: 'Settings' }).click()
  await win.getByTestId('direction-random').check()
  await win.getByTestId('settings-save').click()
  await expect(win.getByTestId('settings-status')).not.toContainText('Unsaved')

  // Clean again, so leaving asks nothing.
  await win.keyboard.press('Escape')
  await expect(win.getByTestId('title-menu')).toBeVisible()
  await first.app.close()

  const second = await launchApp(userData)
  win = await second.app.firstWindow()
  await win.getByRole('button', { name: 'Settings' }).click()
  await expect(win.getByTestId('direction-random')).toBeChecked()

  await second.app.close()
})

// ------------------------------------------------------------- what Learn actually does

test('reverse shows the answer side first', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Settings' }).click()
  await win.getByTestId('direction-reverse').check()
  await win.getByTestId('settings-save').click()
  await win.keyboard.press('Escape')

  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()

  // Both faces are in the DOM at once — that is how the CSS flip works — so this must
  // read the front face element specifically, not the card's whole text.
  const front = win.getByTestId('card').locator('.card-face__front-text')
  await expect(front).toContainText('Primary')
  await expect(front).not.toContainText('Front')

  await app.close()
})

test('back view random can surface a secondary text', async () => {
  const twoFacts = {
    id: 'twofacts',
    name: 'Two Facts',
    schemaVersion: 1 as const,
    // Three secondaries each, so a card picks its primary only 1 time in 4. Across six
    // cards the odds of never seeing a secondary are about 1 in 4096 — strong enough to
    // assert on, and short enough to stay inside the test budget.
    cards: Array.from({ length: 6 }, (_, i) => ({
      id: `c${i}`,
      front: `Card ${i}`,
      primary: `PRIMARY ${i}`,
      secondaries: [`SECONDARY ${i}a`, `SECONDARY ${i}b`, `SECONDARY ${i}c`],
    })),
  }
  const { app } = await launchApp(seedPack(makeUserDataDir(), twoFacts))
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Settings' }).click()
  await win.getByTestId('backview-random').check()
  await win.getByTestId('settings-save').click()
  await win.keyboard.press('Escape')

  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()

  let sawSecondary = false
  for (let i = 0; i < 6; i++) {
    if ((await win.getByTestId('card').innerText()).includes('SECONDARY')) sawSecondary = true
    await win.keyboard.press('Space')
    await expect(win.getByTestId('advance-arrow')).toBeVisible()
    await win.keyboard.press('Enter')
    await win.waitForTimeout(180)
  }
  expect(sawSecondary).toBe(true)

  await app.close()
})

test('primary-only never shows a secondary', async () => {
  const twoFacts = {
    id: 'twofacts2',
    name: 'Two Facts',
    schemaVersion: 1 as const,
    cards: Array.from({ length: 6 }, (_, i) => ({
      id: `d${i}`,
      front: `Card ${i}`,
      primary: `PRIMARY ${i}`,
      secondaries: [`SECONDARY ${i}`],
    })),
  }
  const { app } = await launchApp(seedPack(makeUserDataDir(), twoFacts))
  const win = await app.firstWindow()

  // 'primary' is the default, so no settings change is needed.
  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()

  for (let i = 0; i < 6; i++) {
    expect(await win.getByTestId('card').innerText()).not.toContain('SECONDARY')
    await win.keyboard.press('Space')
    await expect(win.getByTestId('advance-arrow')).toBeVisible()
    await win.keyboard.press('Enter')
    await win.waitForTimeout(180)
  }

  await app.close()
})

test('random mode keeps the deck the same length as the pack', async () => {
  // Three fixture cards in, three cards out — the point of this mode is that it does not
  // change length — every mode is one view per card.
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Settings' }).click()
  await win.getByTestId('direction-random').check()
  await win.getByTestId('settings-save').click()
  await win.keyboard.press('Escape')

  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()

  // Walk the deck with retrying assertions rather than polling visibility, which races
  // the flip transition under load.
  const advance = async () => {
    await win.keyboard.press('Space')
    await expect(win.getByTestId('advance-arrow')).toBeVisible()
    await win.keyboard.press('Enter')
  }

  await advance()
  await advance()
  // Two of three consumed: still a card to study.
  await expect(win.getByTestId('card')).toBeVisible()

  await advance()
  // Three cards in, three views out. Mixed would have left three more.
  await expect(win.getByTestId('card')).toBeHidden()

  await app.close()
})

