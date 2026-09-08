import { test, expect, type Page } from '@playwright/test'
import type { Pack } from '../shared/types'
import { launchWithFixture } from './helpers'

// Shipped pack content changes, so nothing here asserts against it. The three-card
// FIXTURE is used only where the deck itself is the point — f1/f2 carry moreInfo and f3
// deliberately does not. Everything else seeds a ONE-card pack, because the deck is
// shuffled and "press Space, press Tab" is otherwise a one-in-three coin flip on whether
// the current card even has More Info.

const WITH_MORE_INFO: Pack = {
  id: 'panel-card',
  name: 'Panel Card',
  schemaVersion: 1,
  cards: [{
    id: 'p1', front: 'Front', primary: 'Primary', moreInfo: '# Heading\n\nBody text.',
  }],
}

/**
 * A card whose More Info is far taller than the panel, so scrolling has somewhere to go.
 * Seeded here rather than lengthening the shared FIXTURE, whose exact shape other specs
 * depend on.
 */
const LONG_MORE_INFO: Pack = {
  id: 'long-more-info',
  name: 'Long More Info',
  schemaVersion: 1,
  cards: [{
    id: 'l1',
    front: 'Front',
    primary: 'Primary',
    moreInfo: Array.from(
      { length: 40 },
      (_, i) => `Paragraph ${i + 1}. ${'Scrollable body text. '.repeat(12)}`,
    ).join('\n\n'),
  }],
}

const scrollTop = (win: Page) =>
  win.getByTestId('more-info-body').evaluate(el => el.scrollTop)

/** One synthetic keydown per round trip: real auto-repeat arrives as separate tasks. */
async function holdKey(win: Page, key: string, times: number) {
  for (let i = 0; i < times; i++) {
    await win.evaluate(
      k => window.dispatchEvent(new KeyboardEvent('keydown', { key: k, repeat: true })),
      key,
    )
  }
}

test('Tab does nothing before flip and opens the panel after', async () => {
  const { app } = await launchWithFixture(WITH_MORE_INFO)
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()

  await win.keyboard.press('Tab')
  await expect(win.getByTestId('more-info-panel')).toBeHidden()

  await win.keyboard.press('Space')
  await win.keyboard.press('Tab')
  await expect(win.getByTestId('more-info-panel')).toBeVisible()

  await app.close()
})

test('clicking More Info opens the panel without flipping the card', async () => {
  const { app } = await launchWithFixture(WITH_MORE_INFO)
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()
  await win.getByTestId('card').click()
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'true')

  await win.getByTestId('more-info-btn').click()
  await expect(win.getByTestId('more-info-panel')).toBeVisible()
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'true')

  await app.close()
})

test('Space and Enter do not reach the card beneath an open panel', async () => {
  const { app } = await launchWithFixture(WITH_MORE_INFO)
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()
  await win.keyboard.press('Space')
  const id = await win.getByTestId('card').getAttribute('data-card-id')
  await win.keyboard.press('Tab')
  await expect(win.getByTestId('more-info-panel')).toBeVisible()

  await win.keyboard.press('Space')
  await win.keyboard.press('Enter')
  await expect(win.getByTestId('more-info-panel')).toBeVisible()
  // Still the same card, still face-up: neither key reached it.
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'true')
  expect(await win.getByTestId('card').getAttribute('data-card-id')).toBe(id)

  await app.close()
})

test('Escape closes only the panel; a second Escape raises the confirm', async () => {
  const { app } = await launchWithFixture(WITH_MORE_INFO)
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()
  await win.keyboard.press('Space')
  await win.keyboard.press('Tab')
  await expect(win.getByTestId('more-info-panel')).toBeVisible()

  await win.keyboard.press('Escape')
  await expect(win.getByTestId('more-info-panel')).toBeHidden()
  await expect(win.getByTestId('confirm-dialog')).toBeHidden()

  await win.keyboard.press('Escape')
  await expect(win.getByTestId('confirm-dialog')).toBeVisible()

  await app.close()
})

test('a card with no More Info has no control and ignores Tab', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()

  // The deck is shuffled, so walk it until card f3 — the fixture card seeded
  // WITHOUT moreInfo. The fixture has exactly 3 cards, so this always terminates.
  let found = false
  for (let i = 0; i < 3; i++) {
    const id = await win.getByTestId('card').getAttribute('data-card-id')
    await win.keyboard.press('Space')
    if (id === 'f3') {
      await expect(win.getByTestId('more-info-btn')).toHaveCount(0)
      await win.keyboard.press('Tab')
      await expect(win.getByTestId('more-info-panel')).toBeHidden()
      found = true
      break
    }
    await win.keyboard.press('Enter')
  }
  expect(found).toBe(true)

  await app.close()
})

test('X closes the panel', async () => {
  const { app } = await launchWithFixture(WITH_MORE_INFO)
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()
  await win.keyboard.press('Space')
  await win.keyboard.press('Tab')
  await win.getByTestId('more-info-close').click()
  await expect(win.getByTestId('more-info-panel')).toBeHidden()
  await app.close()
})

test('the panel renders sanitized markdown, not raw source', async () => {
  const { app } = await launchWithFixture(WITH_MORE_INFO)
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()
  await win.keyboard.press('Space')
  await win.keyboard.press('Tab')

  await expect(win.getByTestId('more-info-body').locator('h1')).toHaveText('Heading')
  await expect(win.getByTestId('more-info-body')).not.toContainText('# Heading')

  await app.close()
})

test('holding a scroll key keeps scrolling the panel', async () => {
  const { app } = await launchWithFixture(LONG_MORE_INFO)
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()
  await win.keyboard.press('Space')
  await win.keyboard.press('Tab')
  await expect(win.getByTestId('more-info-panel')).toBeVisible()
  expect(await scrollTop(win)).toBe(0)

  // One tap moves the panel.
  await win.keyboard.press('ArrowDown')
  await expect.poll(() => scrollTop(win)).toBeGreaterThan(0)
  const afterTap = await scrollTop(win)

  // Auto-repeat keeps moving it — the one intent `repeatable()` lets through.
  await holdKey(win, 'ArrowDown', 8)
  await expect.poll(() => scrollTop(win)).toBeGreaterThan(afterTap + 100)
  const afterHold = await scrollTop(win)

  // And holding W walks back up.
  await holdKey(win, 'w', 8)
  await expect.poll(() => scrollTop(win)).toBeLessThan(afterHold)

  await app.close()
})
