import { test, expect, type Page } from '@playwright/test'
import { launchApp } from './helpers'

/** Create a pack from the Make a Pack list and land in the editor. */
async function newPack(win: Page, name: string): Promise<void> {
  await win.getByRole('button', { name: 'Make a Pack' }).click()
  await expect(win.getByTestId('screen-makepack')).toBeVisible()
  await win.getByTestId('makepack-new').click()
  await expect(win.getByTestId('screen-makepack-edit')).toBeVisible()
  await win.getByTestId('pack-name-input').fill(name)
}

/** Fill the currently selected card's two required fields. */
async function fillCard(win: Page, front: string, primary: string): Promise<void> {
  await win.getByTestId('field-front').fill(front)
  await win.getByTestId('field-primary').fill(primary)
}

// ------------------------------------------------------------------ creating and saving

test('a saved pack becomes available in Pack Selection', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  await newPack(win, 'Capital Cities')
  await win.getByTestId('card-add').click()
  await fillCard(win, 'France', 'Paris')
  await win.getByTestId('save-pack').click()
  await expect(win.getByTestId('save-status')).toContainText('Saved')

  await win.keyboard.press('Escape')                  // editor -> pack list
  await expect(win.getByTestId('screen-makepack')).toBeVisible()
  await win.keyboard.press('Escape')                  // pack list -> title
  await win.getByRole('button', { name: 'Pack Selection' }).click()

  await expect(win.getByTestId('screen-packs')).toContainText('Capital Cities')
  await app.close()
})

test('a pack with no saved cards stays out of Pack Selection', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  await newPack(win, 'Empty Shell')
  // Typing the name leaves the field focused, so the first Escape only blurs it.
  await win.keyboard.press('Escape')
  // The rename itself is an unsaved change, so leaving asks — and keeps it as a draft.
  await win.keyboard.press('Escape')
  await win.getByTestId('confirm-yes').click()
  await expect(win.getByTestId('screen-makepack')).toBeVisible()
  // It is still listed here — otherwise you could never come back and finish it.
  await expect(win.getByTestId('screen-makepack')).toContainText('Empty Shell')

  await win.keyboard.press('Escape')
  await win.getByRole('button', { name: 'Pack Selection' }).click()
  await expect(win.getByTestId('screen-packs')).not.toContainText('Empty Shell')

  await app.close()
})

test('Ctrl/Cmd+S saves without reaching for the mouse', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  await newPack(win, 'Keyboard Save')
  await win.getByTestId('card-add').click()
  await fillCard(win, 'Japan', 'Tokyo')
  await win.keyboard.press('ControlOrMeta+s')

  await expect(win.getByTestId('save-status')).toContainText('Saved')
  await app.close()
})

// ------------------------------------------------------------------------- validation

test('saving an incomplete card is refused and names what is missing', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  await newPack(win, 'Half Done')
  await win.getByTestId('card-add').click()
  await win.getByTestId('field-front').fill('Peru')       // primary left blank
  await win.getByTestId('save-pack').click()

  await expect(win.getByTestId('save-problems')).toBeVisible()
  await expect(win.getByTestId('save-problems')).toContainText('1 card')
  await expect(win.getByTestId('save-status')).not.toContainText('Saved')

  await app.close()
})

// ------------------------------------------------------------------------------ drafts

test('an unsaved exit is kept as a draft without changing the live pack', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  await newPack(win, 'Draft Test')
  await win.getByTestId('card-add').click()
  await fillCard(win, 'France', 'Paris')
  await win.getByTestId('save-pack').click()
  await expect(win.getByTestId('save-status')).toContainText('Saved')

  // A second card, deliberately left unsaved.
  await win.getByTestId('card-add').click()
  await fillCard(win, 'Peru', 'Lima')
  await win.keyboard.press('Escape')                  // blurs the field it was typed into
  await win.keyboard.press('Escape')                  // now asks about leaving
  await expect(win.getByTestId('confirm-dialog')).toBeVisible()
  await win.getByTestId('confirm-yes').click()
  await expect(win.getByTestId('screen-makepack')).toBeVisible()

  // The live pack still has one card...
  await expect(win.getByTestId('screen-makepack')).toContainText('1 card')

  // ...but the editor resumes from the draft, with both.
  await win.getByText('Draft Test').click()
  await expect(win.getByTestId('screen-makepack-edit')).toBeVisible()
  await expect(win.getByTestId('card-list')).toContainText('Peru')

  await app.close()
})

// -------------------------------------------------------------------- leave behaviour

test('an untouched editor leaves without asking, a dirty one asks', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  await newPack(win, 'Dirty Check')
  await win.getByTestId('save-pack').click()          // an empty pack is valid: saves clean
  await expect(win.getByTestId('save-status')).toContainText('Saved')

  await win.keyboard.press('Escape')                  // clean: no question
  await expect(win.getByTestId('screen-makepack')).toBeVisible()

  await win.getByText('Dirty Check').click()
  await win.getByTestId('card-add').click()           // now dirty
  await win.keyboard.press('Escape')
  await expect(win.getByTestId('confirm-dialog')).toBeVisible()

  await app.close()
})

test('Escape leaves a focused field before it leaves the screen', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  await newPack(win, 'Escape Check')
  await win.getByTestId('card-add').click()
  await win.getByTestId('field-front').click()
  await expect(win.getByTestId('field-front')).toBeFocused()

  await win.keyboard.press('Escape')                  // blurs only
  await expect(win.getByTestId('field-front')).not.toBeFocused()
  await expect(win.getByTestId('screen-makepack-edit')).toBeVisible()

  await app.close()
})

// ----------------------------------------------------------------------- destruction

test('deleting a pack requires typing DELETE', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  await newPack(win, 'Doomed Pack')
  await win.getByTestId('card-add').click()
  await fillCard(win, 'France', 'Paris')
  await win.getByTestId('save-pack').click()
  await expect(win.getByTestId('save-status')).toContainText('Saved')

  await win.getByTestId('delete-pack').click()
  await expect(win.getByTestId('delete-dialog')).toBeVisible()

  // The wrong word does not arm the button.
  await win.getByTestId('delete-input').fill('delete please')
  await expect(win.getByTestId('delete-submit')).toBeDisabled()

  await win.getByTestId('delete-input').fill('DELETE')
  await expect(win.getByTestId('delete-submit')).toBeEnabled()
  await win.getByTestId('delete-submit').click()

  await expect(win.getByTestId('screen-makepack')).toBeVisible()
  await expect(win.getByTestId('screen-makepack')).not.toContainText('Doomed Pack')

  await app.close()
})

test('deleting a card removes it from the pack', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  await newPack(win, 'Card Removal')
  await win.getByTestId('card-add').click()
  await fillCard(win, 'France', 'Paris')
  await win.getByTestId('card-add').click()
  await fillCard(win, 'Japan', 'Tokyo')
  await expect(win.getByTestId('card-list')).toContainText('Japan')

  await win.getByTestId('card-delete').click()
  await expect(win.getByTestId('card-list')).not.toContainText('Japan')
  await expect(win.getByTestId('card-list')).toContainText('France')

  await app.close()
})
