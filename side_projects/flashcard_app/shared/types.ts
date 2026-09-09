export type Card = {
  id: string
  front: string        // Front Text — required
  primary: string      // Back Primary Text — required
  /**
   * Further facts about the same subject — "he issued the Emancipation Proclamation",
   * "he delivered the Gettysburg Address" — alongside "he was the 16th president". Any
   * number, all optional, never validated. Together with `primary` they are the pool of
   * backs the card can show, so one card teaches several things without becoming several
   * cards.
   *
   * Read as a superset of the older single `secondary` field, which storage folds in on
   * read so packs written before this existed keep working.
   */
  secondaries?: string[]
  moreInfo?: string    // Markdown — optional, never validated
}

export type Pack = { id: string; name: string; schemaVersion: 1; cards: Card[] }

export type PackMeta = {
  id: string
  name: string
  /** Cards in the SAVED version. 0 keeps the pack out of Pack Selection. */
  cardCount: number
  /** Last saved version. 0 means the pack has never been saved. */
  currentVersion: number
  /**
   * Newest version written by an unsaved exit, if any. Only Make a Pack reads it —
   * Learn and Pack Selection follow `currentVersion`, so unfinished work is never
   * studied. Cleared on save.
   */
  draftVersion?: number
  /**
   * The name carried by that draft, when it differs from the saved one. Make a Pack shows
   * it so a renamed-but-unsaved pack is still findable; nothing else reads it.
   */
  draftName?: string
  schemaVersion: 1
}

export type AppState = { selectedPackIds: string[]; hasLaunchedBefore: boolean }

/**
 * Which way round cards are studied.
 * - `forward` — front, then flip for the back. The original behaviour.
 * - `reverse` — the back first, flip for the front. Shown the number, recall the president.
 * - `random`  — one view per card, with the side chosen by chance.
 *
 * Every mode shows each card exactly once; a session is always as long as the pack.
 */
export type CardDirection = 'forward' | 'reverse' | 'random'

/** Which of a card's backs to show when it has more than one. */
export type BackView = 'primary' | 'random'

export type Settings = {
  themeId: string
  themeMode: 'static' | 'dynamic'
  cardDirection: CardDirection
  /** Which back a card with several shows. Independent of direction. */
  backView: BackView
}

export type ConfirmIntent =
  | { type: 'leaveLearn' }
  | { type: 'leaveSettings' }
  | { type: 'leaveMakePack' }

/** Mirrors `SaveResult` in electron/storage.ts across the bridge. */
export type SaveOutcome =
  | { ok: true; version: number }
  | { ok: false; problems: { cardId: string; index: number; missing: ('front' | 'primary')[] }[] }

export type FlashcardsApi = {
  packs: {
    list(): Promise<PackMeta[]>
    read(id: string): Promise<Pack>
    /** Draft-aware read. Only Make a Pack uses this. */
    readForEdit(id: string): Promise<Pack>
    create(name: string): Promise<PackMeta>
    saveDraft(id: string, name: string, cards: Card[]): Promise<void>
    save(id: string, name: string, cards: Card[]): Promise<SaveOutcome>
    remove(id: string): Promise<void>
  }
  state: { get(): Promise<AppState>; set(patch: Partial<AppState>): Promise<void> }
  settings: { get(): Promise<Settings>; set(patch: Partial<Settings>): Promise<void> }
  app: { quit(): void }
}

declare global {
  interface Window { flashcards: FlashcardsApi }
}
