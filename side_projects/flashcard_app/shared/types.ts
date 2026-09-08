export type Card = {
  id: string
  front: string        // Front Text — required
  primary: string      // Back Primary Text — required
  moreInfo?: string    // Markdown — optional, never validated
}

export type Pack = { id: string; name: string; schemaVersion: 1; cards: Card[] }

export type PackMeta = {
  id: string
  name: string
  cardCount: number
  currentVersion: number
  schemaVersion: 1
}

export type AppState = { selectedPackIds: string[]; hasLaunchedBefore: boolean }

export type Settings = { themeId: string; themeMode: 'static' | 'dynamic' }

export type ConfirmIntent =
  | { type: 'leaveLearn' }
  | { type: 'leaveSettings' }
  | { type: 'leaveMakePack' }

export type FlashcardsApi = {
  packs: { list(): Promise<PackMeta[]>; read(id: string): Promise<Pack> }
  state: { get(): Promise<AppState>; set(patch: Partial<AppState>): Promise<void> }
  settings: { get(): Promise<Settings>; set(patch: Partial<Settings>): Promise<void> }
  app: { quit(): void }
}

declare global {
  interface Window { flashcards: FlashcardsApi }
}
