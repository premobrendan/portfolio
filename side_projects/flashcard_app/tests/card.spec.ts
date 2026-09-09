import { test, expect, type Page } from '@playwright/test'
import type { Pack } from '../shared/types'
import { launchApp, launchWithFixture, makeUserDataDir, seedPack } from './helpers'

async function enterLearn(win: Page) {
  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()
}

/** One-card packs so a test knows exactly which card is on screen despite the shuffle. */
const WITH_MORE_INFO: Pack = {
  id: 'with-more-info', name: 'With More Info', schemaVersion: 1,
  cards: [{ id: 'm1', front: 'Front', primary: 'Primary', moreInfo: 'Some detail.' }],
}
const WITHOUT_MORE_INFO: Pack = {
  id: 'without-more-info', name: 'Without More Info', schemaVersion: 1,
  cards: [{ id: 'n1', front: 'Front', primary: 'Primary' }],
}

test('clicking the card flips it, and clicking again flips back', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await enterLearn(win)

  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'false')
  await win.getByTestId('card').click()
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'true')
  await win.getByTestId('card').click()
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'false')

  await app.close()
})

test('Space flips the card', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await enterLearn(win)
  await win.keyboard.press('Space')
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'true')
  await app.close()
})

test('the flip animates transform only', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await enterLearn(win)
  const prop = await win.getByTestId('card-inner')
    .evaluate(el => getComputedStyle(el).transitionProperty)
  expect(prop).toBe('transform')
  await app.close()
})

test('will-change is held only while the flip animates', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await enterLearn(win)
  const inner = win.getByTestId('card-inner')
  const reducedMotion = await win.evaluate(
    () => matchMedia('(prefers-reduced-motion: reduce)').matches,
  )

  await expect(inner).toHaveCSS('will-change', 'auto')
  await inner.click()
  // With motion reduced the transition is over before we can look.
  if (!reducedMotion) await expect(inner).toHaveCSS('will-change', 'transform')
  await expect(inner).toHaveCSS('will-change', 'auto')

  await app.close()
})

test('a card without More Info renders no More Info control', async () => {
  const { app } = await launchWithFixture(WITHOUT_MORE_INFO)
  const win = await app.firstWindow()
  await enterLearn(win)
  await win.getByTestId('card').click()
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'true')
  await expect(win.getByTestId('more-info-btn')).toHaveCount(0)
  await app.close()
})

test('clicking More Info does not flip the card', async () => {
  const { app } = await launchWithFixture(WITH_MORE_INFO)
  const win = await app.firstWindow()
  await enterLearn(win)
  await win.getByTestId('card').click()
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'true')

  await win.getByTestId('more-info-btn').click()
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'true')

  await app.close()
})

test('the next card arrives front-up', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await enterLearn(win)

  const first = await win.getByTestId('card').getAttribute('data-card-id')
  await win.keyboard.press('Space')
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'true')
  await win.keyboard.press('Enter')

  await expect(win.getByTestId('card')).not.toHaveAttribute('data-card-id', first!)
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'false')

  await app.close()
})

test('a card face can be a picture, not just text', async () => {
  // The flags-of-the-world case: the question IS the image.
  const flags = {
    id: 'flags',
    name: 'Flags',
    schemaVersion: 1 as const,
    cards: [{
      id: 'usa',
      front: '![Flag of the United States](https://upload.wikimedia.org/wikipedia/en/a/a4/Flag_of_the_United_States.svg)',
      primary: 'United States of America',
    }],
  }
  const { app } = await launchApp(seedPack(makeUserDataDir(), flags))
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()

  // Rendered as a real image element, not printed as literal Markdown.
  await expect(win.getByTestId('card').locator('img')).toHaveCount(1)
  await expect(win.getByTestId('card')).not.toContainText('![Flag')

  // The answer side is ordinary text, and the flip still reveals it.
  await win.keyboard.press('Space')
  await expect(win.getByTestId('advance-arrow')).toBeVisible()
  await expect(win.getByTestId('card')).toContainText('United States of America')

  await app.close()
})

test('a card face cannot smuggle a script through', async () => {
  // Pack content is untrusted — the MCP server lets a model write these files.
  const nasty = {
    id: 'nasty',
    name: 'Nasty',
    schemaVersion: 1 as const,
    cards: [{
      id: 'x',
      front: '<script>window.__pwned = true</script>Front side',
      primary: '<img src=x onerror="window.__pwned = true">Back side',
    }],
  }
  const { app } = await launchApp(seedPack(makeUserDataDir(), nasty))
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()
  await win.keyboard.press('Space')
  await win.waitForTimeout(700)

  expect(await win.evaluate(() => (window as unknown as { __pwned?: boolean }).__pwned))
    .toBeUndefined()
  const html = await win.getByTestId('card').innerHTML()
  expect(html).not.toContain('<script')
  expect(html).not.toContain('onerror')

  await app.close()
})
