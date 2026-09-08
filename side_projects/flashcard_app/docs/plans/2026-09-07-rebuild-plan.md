# Flashcard App Rebuild — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the core loop of a cross-platform Electron flashcard app — Title, Pack Selection, and Learn — from a clean directory, with a card flip that is smooth under all conditions.

**Architecture:** Three strictly separated Electron processes. `electron/` owns the window and is the only filesystem consumer; a narrow `contextBridge` preload is the sole path between processes; `src/` is a React renderer that never touches Node. Navigation is a screen enum plus a layer stack, with all keyboard input dispatched to the top layer only — the single rule that makes `Escape` mean "pop one layer" and prevents keys leaking to components underneath overlays.

**Tech Stack:** Electron, React 18, TypeScript, Vite (`vite-plugin-electron`), plain CSS custom properties, `marked` + `DOMPurify`, Vitest (units), Playwright `_electron` (integration), `electron-builder` (packaging).

**Spec:** [`../specs/2026-09-07-rebuild-design.md`](../specs/2026-09-07-rebuild-design.md)
**Vision:** [`../vision.md`](../vision.md)

## Global Constraints

Every task's requirements implicitly include this section.

- **The card flip is smooth, always.** It never degrades for any reason.
- Animate **`transform` and `opacity` only**. Never a property that triggers layout or paint.
- `will-change: transform` is applied on interaction and **removed on completion**.
- Flip state is **local to `Card`** — never in the app reducer. The screen tree does not re-render mid-flip.
- The **background renders on its own compositor layer**, isolated from the card. A dynamic theme may drop its own frames; it must never cost the card a frame. **No hardware detection, no automatic theme downgrade.**
- Renderer has **no Node access**: `contextIsolation: true`, `nodeIntegration: false`, `sandbox: true`.
- `electron/storage.ts` is the **only** module that touches the filesystem.
- **All keyboard input routes through `keys.ts`** to the top of the layer stack. No component registers its own global key listener.
- **No scoring, counting, ranking, or streaks anywhere.** This is a learning app.
- **Required card fields:** `front` and `primary`. `moreInfo` is optional and never validated.
- A pack is a **directory**, not a file: `packs/<id>/meta.json` + `v<n>.json`.
- **Commit messages carry no AI attribution** (per the user's global preferences).
- Storage root honors `FLASHCARDS_USER_DATA` when set, so tests get an isolated profile.
- **Tests never assert against shipped pack content.** Anything needing a specific deck or
  card shape calls `launchWithFixture()`. Only the seeding test names `presidents`.
- The shipped `presidents` pack has **no `moreInfo` on any card** — by request.

---

## File Structure

| File | Responsibility |
|---|---|
| `electron/main.ts` | Window lifecycle, single-instance lock, quit |
| `electron/storage.ts` | All filesystem I/O — the only fs consumer |
| `electron/seed.ts` | First-run copy of bundled packs into userData |
| `electron/ipc.ts` | One handler per bridge method |
| `electron/preload.ts` | `contextBridge` surface |
| `shared/types.ts` | `Card`, `Pack`, `PackMeta`, `AppState`, `Settings` — the cross-process contract |
| `src/lib/shuffle.ts` | Fisher–Yates with injectable RNG |
| `src/lib/markdown.ts` | `marked` + `DOMPurify` wrapper |
| `src/app/machine.ts` | Screen + layer-stack reducer |
| `src/app/keys.ts` | Central keyboard dispatch |
| `src/app/App.tsx` | Screen switch + layer host |
| `src/components/Card.tsx` | The flip; owns its own flip state |
| `src/components/MoreInfoPanel.tsx` | Scrollable Markdown overlay |
| `src/components/ConfirmDialog.tsx` | Confirm/deny overlay |
| `src/components/AdvanceArrow.tsx` | Textless fade-in arrow |
| `src/components/LeaveButton.tsx` | Top-left leave/quit control |
| `src/components/Background.tsx` | Themed layer, compositor-isolated |
| `src/theme/themes.ts` | Theme registry |
| `src/theme/space.css` | Default theme tokens + starfield |
| `src/screens/*.tsx` | One file per screen |

---

## Task 1: Project scaffold

**Files:**
- Create: `package.json`, `tsconfig.json`, `vite.config.ts`, `index.html`, `.gitignore`
- Create: `electron/main.ts`, `electron/preload.ts`, `src/main.tsx`, `src/app/App.tsx`
- Create: `playwright.config.ts`, `tests/launch.spec.ts`

**Interfaces:**
- Consumes: nothing
- Produces: a launchable Electron app; `tests/helpers.ts` conventions used by every later Playwright test

- [ ] **Step 1: Remove the old husk**

The old React app is being replaced, not extended. `_legacy/` is preserved.

```bash
cd /Users/premob/code/portfolio/side_projects/flashcard_app
rm -rf src node_modules package-lock.json package.json vite.config.js index.html __pycache__
ls   # expect: docs  resources
```

- [ ] **Step 2: Create `package.json`**

```json
{
  "name": "flashcards",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "main": "dist-electron/main.js",
  "scripts": {
    "dev": "vite",
    "build": "tsc --noEmit && vite build",
    "test:unit": "vitest run",
    "test:e2e": "npm run build && playwright test",
    "dist": "npm run build && electron-builder"
  },
  "dependencies": {
    "dompurify": "^3.2.3",
    "marked": "^15.0.4",
    "react": "^18.3.1",
    "react-dom": "^18.3.1"
  },
  "devDependencies": {
    "@playwright/test": "^1.49.1",
    "@types/react": "^18.3.17",
    "@types/react-dom": "^18.3.5",
    "@vitejs/plugin-react": "^4.3.4",
    "jsdom": "^25.0.1",
    "electron": "^33.2.1",
    "electron-builder": "^25.1.8",
    "typescript": "^5.7.2",
    "vite": "^6.0.3",
    "vite-plugin-electron": "^0.29.0",
    "vitest": "^2.1.8"
  }
}
```

- [ ] **Step 3: Create `tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "lib": ["ES2022", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "moduleResolution": "bundler",
    "jsx": "react-jsx",
    "strict": true,
    "noUnusedLocals": true,
    "skipLibCheck": true,
    "esModuleInterop": true,
    "resolveJsonModule": true,
    "isolatedModules": true,
    "noEmit": true,
    "types": ["vite/client"]
  },
  "include": ["src", "electron", "shared", "tests"]
}
```

- [ ] **Step 4: Create `vite.config.ts`**

```ts
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import electron from 'vite-plugin-electron/simple'

export default defineConfig({
  plugins: [
    react(),
    electron({
      main: { entry: 'electron/main.ts' },
      preload: { input: 'electron/preload.ts' },
    }),
  ],
  build: { outDir: 'dist' },
  // DOMPurify needs a DOM, so unit tests run under jsdom.
  test: { environment: 'jsdom', include: ['src/**/*.test.ts', 'electron/**/*.test.ts'] },
})
```

Add the Vitest types reference at the top of the file so `test` type-checks:

```ts
/// <reference types="vitest" />
```

- [ ] **Step 5: Create `index.html`**

```html
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Flashcards</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

- [ ] **Step 6: Create `.gitignore`**

```
node_modules/
dist/
dist-electron/
release/
test-results/
playwright-report/
```

- [ ] **Step 7: Create `electron/main.ts`**

```ts
import { app, BrowserWindow } from 'electron'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

let win: BrowserWindow | null = null

function createWindow() {
  win = new BrowserWindow({
    width: 1100,
    height: 760,
    backgroundColor: '#05060f',
    show: false,
    webPreferences: {
      preload: path.join(__dirname, 'preload.mjs'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  })

  win.once('ready-to-show', () => win?.show())

  if (process.env.VITE_DEV_SERVER_URL) {
    win.loadURL(process.env.VITE_DEV_SERVER_URL)
  } else {
    win.loadFile(path.join(__dirname, '../dist/index.html'))
  }
}

if (!app.requestSingleInstanceLock()) {
  app.quit()
} else {
  app.whenReady().then(createWindow)
}

app.on('window-all-closed', () => app.quit())
```

- [ ] **Step 8: Create `electron/preload.ts` (placeholder surface)**

```ts
import { contextBridge } from 'electron'

contextBridge.exposeInMainWorld('flashcards', {})
```

- [ ] **Step 9: Create `src/main.tsx` and `src/app/App.tsx`**

```tsx
// src/main.tsx
import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './app/App'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode><App /></React.StrictMode>,
)
```

```tsx
// src/app/App.tsx
export default function App() {
  return <div data-testid="app-root">Flashcards</div>
}
```

- [ ] **Step 10: Create `playwright.config.ts` and `tests/helpers.ts`**

```ts
// playwright.config.ts
import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: './tests',
  timeout: 30_000,
  fullyParallel: false,
  workers: 1,
  reporter: 'list',
})
```

```ts
// tests/helpers.ts
import { _electron as electron, type ElectronApplication } from '@playwright/test'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

export type Session = { app: ElectronApplication; userData: string }

export function makeUserDataDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'flashcards-test-'))
}

/** Launch the built app against an isolated profile. Reuse `userData` to test relaunch. */
export async function launchApp(userData = makeUserDataDir()): Promise<Session> {
  const app = await electron.launch({
    args: ['.'],
    env: { ...process.env, FLASHCARDS_USER_DATA: userData, NODE_ENV: 'test' },
  })
  return { app, userData }
}
```

- [ ] **Step 11: Write the failing launch test**

```ts
// tests/launch.spec.ts
import { test, expect } from '@playwright/test'
import { launchApp } from './helpers'

test('app launches and renders a window', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  await expect(win.getByTestId('app-root')).toBeVisible()
  await app.close()
})
```

- [ ] **Step 12: Install and run the test**

```bash
npm install
npm run test:e2e
```

Expected: PASS. If it fails on a missing preload path, confirm `vite-plugin-electron` emitted `dist-electron/preload.mjs` and that `main.ts` points at that exact filename.

- [ ] **Step 13: Commit**

```bash
git add -A
git commit -m "feat(flashcards): scaffold Electron + Vite + React shell with launch test"
```

---

## Task 2: Shared types and shuffle

**Files:**
- Create: `shared/types.ts`, `src/lib/shuffle.ts`
- Test: `src/lib/shuffle.test.ts`

**Interfaces:**
- Consumes: nothing
- Produces: `Card`, `Pack`, `PackMeta`, `AppState`, `Settings`, `ConfirmIntent`; `shuffle<T>(items: T[], rng?: () => number): T[]`

- [ ] **Step 1: Create `shared/types.ts`**

```ts
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
  | { type: 'leavePacks' }
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
```

- [ ] **Step 2: Write the failing shuffle test**

```ts
// src/lib/shuffle.test.ts
import { describe, it, expect } from 'vitest'
import { shuffle } from './shuffle'

describe('shuffle', () => {
  it('returns a new array and leaves the input untouched', () => {
    const input = [1, 2, 3, 4, 5]
    const out = shuffle(input)
    expect(out).not.toBe(input)
    expect(input).toEqual([1, 2, 3, 4, 5])
  })

  it('preserves every element', () => {
    const out = shuffle([1, 2, 3, 4, 5])
    expect([...out].sort()).toEqual([1, 2, 3, 4, 5])
  })

  it('is deterministic for a fixed rng', () => {
    const fixed = () => 0.5
    expect(shuffle([1, 2, 3, 4, 5], fixed)).toEqual(shuffle([1, 2, 3, 4, 5], fixed))
  })

  it('handles empty and single-element arrays', () => {
    expect(shuffle([])).toEqual([])
    expect(shuffle([7])).toEqual([7])
  })
})
```

- [ ] **Step 3: Run it and confirm it fails**

```bash
npx vitest run src/lib/shuffle.test.ts
```

Expected: FAIL — cannot resolve `./shuffle`.

- [ ] **Step 4: Implement `src/lib/shuffle.ts`**

Fisher–Yates. `sort(() => Math.random() - 0.5)` is biased and must not be used.

```ts
/** Fisher-Yates. `rng` is injectable so tests can assert a deterministic order. */
export function shuffle<T>(items: T[], rng: () => number = Math.random): T[] {
  const out = [...items]
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1))
    ;[out[i], out[j]] = [out[j], out[i]]
  }
  return out
}
```

- [ ] **Step 5: Run the tests and confirm they pass**

```bash
npx vitest run src/lib/shuffle.test.ts
```

Expected: 4 passed.

- [ ] **Step 6: Commit**

```bash
git add shared/types.ts src/lib/shuffle.ts src/lib/shuffle.test.ts
git commit -m "feat(flashcards): add shared types and unbiased shuffle"
```

---

## Task 3: Storage layer

**Files:**
- Create: `electron/storage.ts`
- Test: `electron/storage.test.ts`

**Interfaces:**
- Consumes: `shared/types.ts`
- Produces: `setStorageRoot(dir)`, `getStorageRoot()`, `listPacks()`, `readPack(id)`, `writePackVersion(id, name, cards)`, `readState()`, `writeState(patch)`, `readSettings()`, `writeSettings(patch)`

This is the only module in the codebase permitted to touch the filesystem.

- [ ] **Step 1: Write the failing tests**

```ts
// electron/storage.test.ts
import { describe, it, expect, beforeEach } from 'vitest'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import {
  setStorageRoot, listPacks, readPack, writePackVersion,
  readState, writeState, readSettings,
} from './storage'

let root: string
beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), 'fc-store-'))
  setStorageRoot(root)
})

describe('state', () => {
  it('returns defaults when state.json is absent', async () => {
    expect(await readState()).toEqual({ selectedPackIds: [], hasLaunchedBefore: false })
  })

  it('rewrites defaults when state.json is malformed', async () => {
    fs.writeFileSync(path.join(root, 'state.json'), '{ not json')
    expect(await readState()).toEqual({ selectedPackIds: [], hasLaunchedBefore: false })
  })

  it('persists a shallow patch', async () => {
    await writeState({ selectedPackIds: ['a'] })
    await writeState({ hasLaunchedBefore: true })
    expect(await readState()).toEqual({ selectedPackIds: ['a'], hasLaunchedBefore: true })
  })
})

describe('packs', () => {
  it('lists nothing when packs/ is absent', async () => {
    expect(await listPacks()).toEqual([])
  })

  it('writes v1 and lists it without reading card data', async () => {
    await writePackVersion('demo', 'Demo', [{ id: 'c1', front: 'F', primary: 'P' }])
    expect(await listPacks()).toEqual([
      { id: 'demo', name: 'Demo', cardCount: 1, currentVersion: 1, schemaVersion: 1 },
    ])
  })

  it('reads back the current version', async () => {
    await writePackVersion('demo', 'Demo', [{ id: 'c1', front: 'F', primary: 'P' }])
    const pack = await readPack('demo')
    expect(pack.cards[0].front).toBe('F')
  })

  it('increments the version and reads the newest', async () => {
    await writePackVersion('demo', 'Demo', [{ id: 'c1', front: 'one', primary: 'P' }])
    await writePackVersion('demo', 'Demo', [{ id: 'c1', front: 'two', primary: 'P' }])
    const metas = await listPacks()
    expect(metas[0].currentVersion).toBe(2)
    expect((await readPack('demo')).cards[0].front).toBe('two')
  })

  it('skips a malformed pack directory and still lists the good ones', async () => {
    await writePackVersion('good', 'Good', [{ id: 'c1', front: 'F', primary: 'P' }])
    fs.mkdirSync(path.join(root, 'packs', 'broken'), { recursive: true })
    fs.writeFileSync(path.join(root, 'packs', 'broken', 'meta.json'), '{ nope')
    const metas = await listPacks()
    expect(metas.map(m => m.id)).toEqual(['good'])
  })
})

describe('settings', () => {
  it('defaults to the dynamic space theme', async () => {
    expect(await readSettings()).toEqual({ themeId: 'space', themeMode: 'dynamic' })
  })
})
```

- [ ] **Step 2: Run and confirm failure**

```bash
npx vitest run electron/storage.test.ts
```

Expected: FAIL — cannot resolve `./storage`.

- [ ] **Step 3: Implement `electron/storage.ts`**

```ts
import fs from 'node:fs/promises'
import fsSync from 'node:fs'
import path from 'node:path'
import type { AppState, Card, Pack, PackMeta, Settings } from '../shared/types'

const DEFAULT_STATE: AppState = { selectedPackIds: [], hasLaunchedBefore: false }
const DEFAULT_SETTINGS: Settings = { themeId: 'space', themeMode: 'dynamic' }

let root = ''

export function setStorageRoot(dir: string) {
  root = dir
  fsSync.mkdirSync(root, { recursive: true })
}
export function getStorageRoot() { return root }

const packsDir = () => path.join(root, 'packs')
const packDir = (id: string) => path.join(packsDir(), id)

/** Read JSON, falling back to `fallback` on missing OR malformed files. Never throws. */
async function readJson<T>(file: string, fallback: T): Promise<T> {
  try {
    return JSON.parse(await fs.readFile(file, 'utf8')) as T
  } catch {
    return fallback
  }
}

async function writeJson(file: string, data: unknown): Promise<void> {
  await fs.mkdir(path.dirname(file), { recursive: true })
  await fs.writeFile(file, JSON.stringify(data, null, 2), 'utf8')
}

export async function readState(): Promise<AppState> {
  const s = await readJson<AppState>(path.join(root, 'state.json'), DEFAULT_STATE)
  return {
    selectedPackIds: Array.isArray(s.selectedPackIds) ? s.selectedPackIds : [],
    hasLaunchedBefore: Boolean(s.hasLaunchedBefore),
  }
}

export async function writeState(patch: Partial<AppState>): Promise<void> {
  await writeJson(path.join(root, 'state.json'), { ...(await readState()), ...patch })
}

export async function readSettings(): Promise<Settings> {
  return { ...DEFAULT_SETTINGS, ...(await readJson(path.join(root, 'settings.json'), {})) }
}

export async function writeSettings(patch: Partial<Settings>): Promise<void> {
  await writeJson(path.join(root, 'settings.json'), { ...(await readSettings()), ...patch })
}

/** Lists packs by reading meta.json only — never card data, so this stays fast. */
export async function listPacks(): Promise<PackMeta[]> {
  let entries: string[]
  try {
    entries = (await fs.readdir(packsDir(), { withFileTypes: true }))
      .filter(e => e.isDirectory()).map(e => e.name)
  } catch {
    return []
  }

  const metas: PackMeta[] = []
  for (const id of entries) {
    const meta = await readJson<PackMeta | null>(path.join(packDir(id), 'meta.json'), null)
    // One malformed pack must never prevent the others from loading.
    if (meta && typeof meta.id === 'string' && typeof meta.currentVersion === 'number') {
      metas.push(meta)
    } else {
      console.warn(`[storage] skipping malformed pack: ${id}`)
    }
  }
  return metas.sort((a, b) => a.name.localeCompare(b.name))
}

export async function readPack(id: string): Promise<Pack> {
  const meta = await readJson<PackMeta | null>(path.join(packDir(id), 'meta.json'), null)
  if (!meta) throw new Error(`pack not found: ${id}`)
  const body = await readJson<{ cards: Card[] }>(
    path.join(packDir(id), `v${meta.currentVersion}.json`), { cards: [] },
  )
  return { id: meta.id, name: meta.name, schemaVersion: 1, cards: body.cards }
}

/** Appends a new numbered version and points meta.json at it. */
export async function writePackVersion(id: string, name: string, cards: Card[]): Promise<number> {
  const existing = await readJson<PackMeta | null>(path.join(packDir(id), 'meta.json'), null)
  const version = (existing?.currentVersion ?? 0) + 1
  await writeJson(path.join(packDir(id), `v${version}.json`), { cards })
  await writeJson(path.join(packDir(id), 'meta.json'), {
    id, name, cardCount: cards.length, currentVersion: version, schemaVersion: 1,
  } satisfies PackMeta)
  return version
}
```

- [ ] **Step 4: Run the tests**

```bash
npx vitest run electron/storage.test.ts
```

Expected: 9 passed.

- [ ] **Step 5: Commit**

```bash
git add electron/storage.ts electron/storage.test.ts
git commit -m "feat(flashcards): add versioned pack storage with fault-tolerant reads"
```

---

## Task 4: Seed, IPC, and the preload bridge

**Files:**
- Create: `electron/seed.ts`, `electron/ipc.ts`
- Verify: `resources/packs/presidents.json` (already written)
- Modify: `tests/helpers.ts` (add `FIXTURE`, `seedPack`, `launchWithFixture`)
- Modify: `electron/main.ts`, `electron/preload.ts`
- Test: `tests/bridge.spec.ts`

**Interfaces:**
- Consumes: `storage.ts` exports, `shared/types.ts`
- Produces: `window.flashcards` implementing `FlashcardsApi`; `seedIfEmpty()`

- [ ] **Step 1: Confirm the bundled pack**

`resources/packs/presidents.json` already exists — 45 cards covering all 47 presidencies
(Cleveland and Trump each get one card naming both of their numbers). Front Text is the
name; Back Primary Text is "He was/is the *n*th president." **No card has `moreInfo`** —
that is intentional for now.

Verify it, do not regenerate it:

```bash
node -e "const p=require('./resources/packs/presidents.json');
console.log(p.id, p.cards.length, p.cards.some(c=>c.moreInfo))"
```

Expected: `presidents 45 false`

> **Why tests do not use this pack:** shipped content changes. Every test that needs
> specific card shapes seeds its own fixture (Step 1b) rather than asserting against
> whatever happens to ship. The only test that touches `presidents` is the seeding test.

- [ ] **Step 1b: Add fixture seeding to `tests/helpers.ts`**

```ts
import type { Pack } from '../shared/types'

/** A pack with known shape: two cards WITH moreInfo, one deliberately without. */
export const FIXTURE: Pack = {
  id: 'fixture',
  name: 'Fixture Pack',
  schemaVersion: 1,
  cards: [
    { id: 'f1', front: 'Front One', primary: 'Primary One', moreInfo: '# Heading\n\nBody text.' },
    { id: 'f2', front: 'Front Two', primary: 'Primary Two', moreInfo: 'Second card detail.' },
    { id: 'f3', front: 'Front Three', primary: 'Primary Three' },
  ],
}

/**
 * Write a pack straight to disk and mark the profile as already launched, so
 * `seedIfEmpty` does not also install the shipped packs. Gives a test an exact deck.
 */
export function seedPack(userData: string, pack: Pack = FIXTURE): string {
  const dir = path.join(userData, 'packs', pack.id)
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(path.join(dir, 'v1.json'), JSON.stringify({ cards: pack.cards }))
  fs.writeFileSync(path.join(dir, 'meta.json'), JSON.stringify({
    id: pack.id, name: pack.name, cardCount: pack.cards.length,
    currentVersion: 1, schemaVersion: 1,
  }))
  fs.writeFileSync(path.join(userData, 'state.json'), JSON.stringify({
    selectedPackIds: [pack.id], hasLaunchedBefore: true,
  }))
  return userData
}

/** Launch against a profile preloaded with the fixture pack. */
export async function launchWithFixture(pack: Pack = FIXTURE): Promise<Session> {
  return launchApp(seedPack(makeUserDataDir(), pack))
}
```

- [ ] **Step 2: Write the failing bridge test**

```ts
// tests/bridge.spec.ts
import { test, expect } from '@playwright/test'
import { launchApp } from './helpers'

test('first run seeds the presidents pack and selects everything', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  const packs = await win.evaluate(() => window.flashcards.packs.list())
  expect(packs.map((p: { id: string }) => p.id)).toContain('presidents')

  const state = await win.evaluate(() => window.flashcards.state.get())
  expect(state.hasLaunchedBefore).toBe(true)
  expect(state.selectedPackIds).toContain('presidents')

  await app.close()
})

test('renderer has no Node access', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  expect(await win.evaluate(() => typeof (window as never as { require?: unknown }).require))
    .toBe('undefined')
  await app.close()
})
```

- [ ] **Step 3: Run and confirm failure**

```bash
npm run test:e2e -- tests/bridge.spec.ts
```

Expected: FAIL — `window.flashcards.packs` is undefined.

- [ ] **Step 4: Implement `electron/seed.ts`**

```ts
import fs from 'node:fs/promises'
import path from 'node:path'
import type { Pack } from '../shared/types'
import { listPacks, readState, writePackVersion, writeState } from './storage'

/** On first run, copy bundled packs in and select all of them. */
export async function seedIfEmpty(resourcesDir: string): Promise<void> {
  const state = await readState()
  if (state.hasLaunchedBefore) return

  let files: string[] = []
  try {
    files = (await fs.readdir(resourcesDir)).filter(f => f.endsWith('.json'))
  } catch {
    files = []
  }

  for (const file of files) {
    try {
      const pack = JSON.parse(
        await fs.readFile(path.join(resourcesDir, file), 'utf8'),
      ) as Pack
      await writePackVersion(pack.id, pack.name, pack.cards)
    } catch {
      console.warn(`[seed] skipping unreadable bundled pack: ${file}`)
    }
  }

  // Default on first ever open: every pack selected.
  const all = await listPacks()
  await writeState({ selectedPackIds: all.map(p => p.id), hasLaunchedBefore: true })
}
```

- [ ] **Step 5: Implement `electron/ipc.ts`**

```ts
import { app, ipcMain } from 'electron'
import type { AppState, Settings } from '../shared/types'
import {
  listPacks, readPack, readSettings, readState, writeSettings, writeState,
} from './storage'

export function registerIpc() {
  ipcMain.handle('packs:list', () => listPacks())
  ipcMain.handle('packs:read', (_e, id: string) => readPack(id))
  ipcMain.handle('state:get', () => readState())
  ipcMain.handle('state:set', (_e, patch: Partial<AppState>) => writeState(patch))
  ipcMain.handle('settings:get', () => readSettings())
  ipcMain.handle('settings:set', (_e, patch: Partial<Settings>) => writeSettings(patch))
  ipcMain.on('app:quit', () => app.quit())
}
```

- [ ] **Step 6: Implement `electron/preload.ts`**

```ts
import { contextBridge, ipcRenderer } from 'electron'
import type { AppState, Settings } from '../shared/types'

contextBridge.exposeInMainWorld('flashcards', {
  packs: {
    list: () => ipcRenderer.invoke('packs:list'),
    read: (id: string) => ipcRenderer.invoke('packs:read', id),
  },
  state: {
    get: () => ipcRenderer.invoke('state:get'),
    set: (patch: Partial<AppState>) => ipcRenderer.invoke('state:set', patch),
  },
  settings: {
    get: () => ipcRenderer.invoke('settings:get'),
    set: (patch: Partial<Settings>) => ipcRenderer.invoke('settings:set', patch),
  },
  app: { quit: () => ipcRenderer.send('app:quit') },
})
```

- [ ] **Step 7: Wire `electron/main.ts`**

Add these imports and replace the `whenReady` block. `FLASHCARDS_USER_DATA` is what gives each test an isolated profile.

```ts
import { registerIpc } from './ipc'
import { seedIfEmpty } from './seed'
import { setStorageRoot } from './storage'

function resourcesDir() {
  return app.isPackaged
    ? path.join(process.resourcesPath, 'packs')
    : path.join(__dirname, '../resources/packs')
}

app.whenReady().then(async () => {
  setStorageRoot(process.env.FLASHCARDS_USER_DATA ?? app.getPath('userData'))
  await seedIfEmpty(resourcesDir())
  registerIpc()
  createWindow()
})
```

- [ ] **Step 8: Run the tests**

```bash
npm run test:e2e -- tests/bridge.spec.ts
```

Expected: 2 passed.

- [ ] **Step 9: Commit**

```bash
git add electron resources tests/helpers.ts tests/bridge.spec.ts
git commit -m "feat(flashcards): add first-run seeding and the contextBridge IPC surface"
```

---

## Task 5: State machine and keyboard dispatch

**Files:**
- Create: `src/app/machine.ts`, `src/app/keys.ts`
- Test: `src/app/machine.test.ts`

**Interfaces:**
- Consumes: `ConfirmIntent` from `shared/types.ts`
- Produces: `UI`, `initialUI`, `reduce(ui, action)`, `topLayer(ui)`, `Action`; `keyActionFor(ui, key, ctx)`

This is the backbone of the whole interaction spec. Input goes to the top of the layer stack and nowhere else — that is what makes `Escape` mean "pop exactly one thing" and what stops `Space` reaching the card while a dialog is open.

- [ ] **Step 1: Write the failing tests**

```ts
// src/app/machine.test.ts
import { describe, it, expect } from 'vitest'
import { initialUI, reduce, topLayer } from './machine'

describe('screens', () => {
  it('starts on the title screen with no layers', () => {
    expect(initialUI).toEqual({ screen: 'title', layers: [] })
  })

  it('navigates between screens', () => {
    const ui = reduce(initialUI, { type: 'goto', screen: 'learn' })
    expect(ui.screen).toBe('learn')
  })
})

describe('layer stack', () => {
  it('pushes and reports the top layer', () => {
    let ui = reduce(initialUI, { type: 'goto', screen: 'learn' })
    ui = reduce(ui, { type: 'pushLayer', layer: { kind: 'moreInfo' } })
    expect(topLayer(ui)).toEqual({ kind: 'moreInfo' })
  })

  it('pops exactly one layer', () => {
    let ui = reduce(initialUI, { type: 'goto', screen: 'learn' })
    ui = reduce(ui, { type: 'pushLayer', layer: { kind: 'moreInfo' } })
    ui = reduce(ui, {
      type: 'pushLayer', layer: { kind: 'confirm', intent: { type: 'leaveLearn' } },
    })
    ui = reduce(ui, { type: 'popLayer' })
    expect(topLayer(ui)).toEqual({ kind: 'moreInfo' })
    expect(ui.layers).toHaveLength(1)
  })

  it('reports no top layer when the stack is empty', () => {
    expect(topLayer(initialUI)).toBeNull()
  })

  it('clears layers on navigation so overlays never survive a screen change', () => {
    let ui = reduce(initialUI, { type: 'goto', screen: 'learn' })
    ui = reduce(ui, { type: 'pushLayer', layer: { kind: 'moreInfo' } })
    ui = reduce(ui, { type: 'goto', screen: 'title' })
    expect(ui.layers).toEqual([])
  })

  it('popping an empty stack is a no-op', () => {
    expect(reduce(initialUI, { type: 'popLayer' })).toEqual(initialUI)
  })
})
```

- [ ] **Step 2: Run and confirm failure**

```bash
npx vitest run src/app/machine.test.ts
```

Expected: FAIL — cannot resolve `./machine`.

- [ ] **Step 3: Implement `src/app/machine.ts`**

```ts
import type { ConfirmIntent } from '../../shared/types'

export type Screen = 'title' | 'packs' | 'learn' | 'settings' | 'makepack'
export type Layer =
  | { kind: 'moreInfo' }
  | { kind: 'confirm'; intent: ConfirmIntent }

export type UI = { screen: Screen; layers: Layer[] }

export type Action =
  | { type: 'goto'; screen: Screen }
  | { type: 'pushLayer'; layer: Layer }
  | { type: 'popLayer' }

export const initialUI: UI = { screen: 'title', layers: [] }

/** The layer that owns input right now, or null when the screen owns it. */
export function topLayer(ui: UI): Layer | null {
  return ui.layers.length ? ui.layers[ui.layers.length - 1] : null
}

export function reduce(ui: UI, action: Action): UI {
  switch (action.type) {
    case 'goto':
      // Overlays never survive a screen change.
      return { screen: action.screen, layers: [] }
    case 'pushLayer':
      return { ...ui, layers: [...ui.layers, action.layer] }
    case 'popLayer':
      return ui.layers.length ? { ...ui, layers: ui.layers.slice(0, -1) } : ui
  }
}
```

- [ ] **Step 4: Run and confirm the tests pass**

```bash
npx vitest run src/app/machine.test.ts
```

Expected: 7 passed.

- [ ] **Step 5: Implement `src/app/keys.ts`**

Translates a raw key into an intent, given the top layer. Pure and therefore testable; `App.tsx` binds the single listener.

```ts
import type { Card } from '../../shared/types'
import type { UI } from './machine'
import { topLayer } from './machine'

export type KeyIntent =
  | { type: 'flip' }
  | { type: 'advance' }
  | { type: 'openMoreInfo' }
  | { type: 'closeMoreInfo' }
  | { type: 'scrollMoreInfo'; direction: 'up' | 'down' }
  | { type: 'requestLeave' }
  | { type: 'confirmLeave' }
  | { type: 'denyLeave' }
  | null

export type KeyContext = { hasFlipped: boolean; card: Card | null }

/**
 * Maps a key to an intent for the TOP layer only. Anything not listed for the
 * active layer is swallowed, which is what stops Space flipping the card
 * underneath an open dialog.
 */
export function keyActionFor(ui: UI, key: string, ctx: KeyContext): KeyIntent {
  const layer = topLayer(ui)

  if (layer?.kind === 'confirm') {
    if (key === 'Escape') return { type: 'confirmLeave' }
    if (key === ' ' || key === 'Enter') return { type: 'denyLeave' }
    return null
  }

  if (layer?.kind === 'moreInfo') {
    if (key === 'Escape' || key === 'Tab') return { type: 'closeMoreInfo' }
    if (key === 'ArrowUp' || key === 'w' || key === 'W')
      return { type: 'scrollMoreInfo', direction: 'up' }
    if (key === 'ArrowDown' || key === 's' || key === 'S')
      return { type: 'scrollMoreInfo', direction: 'down' }
    return null
  }

  if (ui.screen === 'learn') {
    if (key === ' ') return { type: 'flip' }
    if (key === 'Enter') return ctx.hasFlipped ? { type: 'advance' } : null
    // Gated on BOTH a prior flip and the card actually having More Info.
    if (key === 'Tab')
      return ctx.hasFlipped && ctx.card?.moreInfo ? { type: 'openMoreInfo' } : null
    if (key === 'Escape') return { type: 'requestLeave' }
    return null
  }

  if (key === 'Escape') return { type: 'requestLeave' }
  return null
}
```

- [ ] **Step 6: Add key-dispatch tests to `src/app/machine.test.ts`**

```ts
import { keyActionFor } from './keys'

const learn = { screen: 'learn' as const, layers: [] }
const cardWith = { id: 'c', front: 'f', primary: 'p', moreInfo: '# hi' }
const cardWithout = { id: 'c', front: 'f', primary: 'p' }

describe('key dispatch', () => {
  it('Space flips on the card layer', () => {
    expect(keyActionFor(learn, ' ', { hasFlipped: false, card: cardWith }))
      .toEqual({ type: 'flip' })
  })

  it('Enter does nothing before a flip and advances after', () => {
    expect(keyActionFor(learn, 'Enter', { hasFlipped: false, card: cardWith })).toBeNull()
    expect(keyActionFor(learn, 'Enter', { hasFlipped: true, card: cardWith }))
      .toEqual({ type: 'advance' })
  })

  it('Tab needs both a prior flip and non-empty More Info', () => {
    expect(keyActionFor(learn, 'Tab', { hasFlipped: false, card: cardWith })).toBeNull()
    expect(keyActionFor(learn, 'Tab', { hasFlipped: true, card: cardWithout })).toBeNull()
    expect(keyActionFor(learn, 'Tab', { hasFlipped: true, card: cardWith }))
      .toEqual({ type: 'openMoreInfo' })
  })

  it('swallows Space and Enter beneath an open More Info panel', () => {
    const ui = { screen: 'learn' as const, layers: [{ kind: 'moreInfo' as const }] }
    expect(keyActionFor(ui, ' ', { hasFlipped: true, card: cardWith })).toBeNull()
    expect(keyActionFor(ui, 'Enter', { hasFlipped: true, card: cardWith })).toBeNull()
  })

  it('closes More Info on Escape without leaving the screen', () => {
    const ui = { screen: 'learn' as const, layers: [{ kind: 'moreInfo' as const }] }
    expect(keyActionFor(ui, 'Escape', { hasFlipped: true, card: cardWith }))
      .toEqual({ type: 'closeMoreInfo' })
  })

  it('confirms on a second Escape and denies on Space or Enter', () => {
    const ui = {
      screen: 'learn' as const,
      layers: [{ kind: 'confirm' as const, intent: { type: 'leaveLearn' as const } }],
    }
    const ctx = { hasFlipped: true, card: cardWith }
    expect(keyActionFor(ui, 'Escape', ctx)).toEqual({ type: 'confirmLeave' })
    expect(keyActionFor(ui, ' ', ctx)).toEqual({ type: 'denyLeave' })
    expect(keyActionFor(ui, 'Enter', ctx)).toEqual({ type: 'denyLeave' })
  })
})
```

- [ ] **Step 7: Run all unit tests**

```bash
npm run test:unit
```

Expected: all passed.

- [ ] **Step 8: Commit**

```bash
git add src/app
git commit -m "feat(flashcards): add screen/layer machine and central key dispatch"
```

---

## Task 6: Theme layer and isolated background

**Files:**
- Create: `src/theme/themes.ts`, `src/theme/space.css`, `src/components/Background.tsx`, `src/styles/base.css`
- Modify: `src/main.tsx`

**Interfaces:**
- Consumes: `Settings` from `shared/types.ts`
- Produces: `<Background mode="static" | "dynamic" />`, `THEMES`, `applyTheme(themeId)`

The background must be able to stutter without costing the card a frame. That isolation is built here, not retrofitted.

- [ ] **Step 1: Create `src/styles/base.css`**

```css
:root {
  --bg: #05060f;
  --fg: #e8ecf8;
  --muted: #9aa3c0;
  --card-face: #10132a;
  --card-edge: #262b4d;
  --accent: #7aa2ff;
  --radius: 16px;
  --flip-ms: 420ms;
  --ease: cubic-bezier(0.2, 0.8, 0.2, 1);
}

* { box-sizing: border-box; }

html, body, #root { height: 100%; margin: 0; }

body {
  background: var(--bg);
  color: var(--fg);
  font: 15px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
  overflow: hidden;
}

@media (prefers-reduced-motion: reduce) {
  * { animation-duration: 0.01ms !important; transition-duration: 0.01ms !important; }
}
```

- [ ] **Step 2: Create `src/theme/space.css`**

The starfield animates `transform` only and lives in its own stacking context.

```css
.bg {
  position: fixed;
  inset: 0;
  z-index: 0;
  pointer-events: none;
  /* Own stacking context + own layer: frames dropped here cannot reach the card. */
  isolation: isolate;
  contain: strict;
  background:
    radial-gradient(1200px 800px at 70% 20%, #131a3d 0%, transparent 60%),
    radial-gradient(900px 700px at 20% 80%, #1a1230 0%, transparent 55%),
    var(--bg);
}

.bg__stars {
  position: absolute;
  inset: -50%;
  background-image:
    radial-gradient(1px 1px at 20% 30%, #fff 50%, transparent 50%),
    radial-gradient(1px 1px at 70% 60%, #cfe0ff 50%, transparent 50%),
    radial-gradient(1.5px 1.5px at 45% 15%, #fff 50%, transparent 50%),
    radial-gradient(1px 1px at 85% 40%, #9fb8ff 50%, transparent 50%);
  background-size: 320px 320px, 420px 420px, 560px 560px, 300px 300px;
  opacity: 0.5;
  will-change: transform;
}

.bg__stars--dynamic { animation: drift 140s linear infinite; }

@keyframes drift {
  from { transform: translate3d(0, 0, 0); }
  to   { transform: translate3d(-160px, -120px, 0); }
}
```

- [ ] **Step 3: Create `src/theme/themes.ts`**

```ts
export type ThemeId = 'space'

export const THEMES: Record<ThemeId, { id: ThemeId; name: string }> = {
  space: { id: 'space', name: 'Space' },
}

export function applyTheme(themeId: string) {
  document.documentElement.dataset.theme = themeId
}
```

- [ ] **Step 4: Create `src/components/Background.tsx`**

```tsx
import '../theme/space.css'

/**
 * Themed background on its own compositor layer. A dynamic background is allowed
 * to drop frames on weak hardware; `contain: strict` and the separate layer keep
 * that from ever costing the card a frame. The user's remedy is the static mode.
 */
export default function Background({ mode }: { mode: 'static' | 'dynamic' }) {
  return (
    <div className="bg" data-testid="background" aria-hidden="true">
      <div className={`bg__stars ${mode === 'dynamic' ? 'bg__stars--dynamic' : ''}`} />
    </div>
  )
}
```

- [ ] **Step 5: Import the base stylesheet in `src/main.tsx`**

```tsx
import './styles/base.css'
```

And call `applyTheme` from `App.tsx` once settings load, so the theme id reaches the DOM
rather than sitting unused:

```tsx
useEffect(() => { if (settings) applyTheme(settings.themeId) }, [settings])
```

- [ ] **Step 6: Verify it builds**

```bash
npm run build
```

Expected: build succeeds with no type errors.

- [ ] **Step 7: Commit**

```bash
git add src/theme src/styles src/components/Background.tsx src/main.tsx
git commit -m "feat(flashcards): add space theme with compositor-isolated background"
```

---

## Task 7: App shell, Title screen, and quit

**Files:**
- Create: `src/screens/TitleScreen.tsx`, `src/components/LeaveButton.tsx`, `src/styles/screens.css`
- Modify: `src/app/App.tsx`
- Test: `tests/title.spec.ts`

**Interfaces:**
- Consumes: `machine.ts`, `keys.ts`, `Background`
- Produces: `AppContext` providing `{ ui, dispatch, packs, state, settings, reload }`; `<LeaveButton onLeave />`

- [ ] **Step 1: Write the failing tests**

```ts
// tests/title.spec.ts
import { test, expect } from '@playwright/test'
import { launchApp } from './helpers'

test('title screen shows all five buttons in order', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  const labels = await win.getByTestId('title-menu').getByRole('button').allInnerTexts()
  expect(labels).toEqual(['Learn', 'Pack Selection', 'Make a Pack', 'Settings', 'Quit'])
  await app.close()
})

test('Learn is disabled when no packs are selected', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  await win.evaluate(() => window.flashcards.state.set({ selectedPackIds: [] }))
  await win.getByTestId('btn-reload').click()
  await expect(win.getByRole('button', { name: 'Learn' })).toBeDisabled()
  await app.close()
})

test('the Quit button closes the app', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  // `exact` — otherwise this also matches the leave button's aria-label "Quit application".
  await win.getByRole('button', { name: 'Quit', exact: true }).click()
  await app.waitForEvent('close')
})
```

- [ ] **Step 2: Run and confirm failure**

```bash
npm run test:e2e -- tests/title.spec.ts
```

Expected: FAIL — no `title-menu` testid.

- [ ] **Step 3: Create `src/styles/screens.css`**

```css
.screen {
  position: relative;
  z-index: 1;
  height: 100%;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 2rem;
  padding: 3rem;
}

.leave-btn {
  position: fixed;
  top: 1rem;
  left: 1rem;
  z-index: 3;
  width: 36px;
  height: 36px;
  display: grid;
  place-items: center;
  border: 1px solid var(--card-edge);
  border-radius: 10px;
  background: transparent;
  color: var(--muted);
  cursor: pointer;
  transition: color 160ms var(--ease), border-color 160ms var(--ease);
}
.leave-btn:hover { color: var(--fg); border-color: var(--accent); }

.menu { display: flex; flex-direction: column; gap: 0.75rem; min-width: 260px; }

.menu button {
  padding: 0.85rem 1.25rem;
  font-size: 1rem;
  color: var(--fg);
  background: var(--card-face);
  border: 1px solid var(--card-edge);
  border-radius: var(--radius);
  cursor: pointer;
  transition: transform 160ms var(--ease), border-color 160ms var(--ease);
}
.menu button:hover:not(:disabled) { transform: translateY(-1px); border-color: var(--accent); }
.menu button:disabled { opacity: 0.4; cursor: not-allowed; }

.hint { color: var(--muted); font-size: 0.85rem; min-height: 1.2em; }
```

- [ ] **Step 4: Create `src/components/LeaveButton.tsx`**

```tsx
export default function LeaveButton({ onLeave, label }: { onLeave: () => void; label: string }) {
  return (
    <button className="leave-btn" onClick={onLeave} aria-label={label} data-testid="leave-btn">
      {/* Standard leave glyph, no text. */}
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
        <polyline points="16 17 21 12 16 7" />
        <line x1="21" y1="12" x2="9" y2="12" />
      </svg>
    </button>
  )
}
```

- [ ] **Step 5: Create `src/screens/TitleScreen.tsx`**

```tsx
import LeaveButton from '../components/LeaveButton'
import type { Screen } from '../app/machine'

type Props = { canLearn: boolean; onGo: (s: Screen) => void; onReload: () => void }

export default function TitleScreen({ canLearn, onGo, onReload }: Props) {
  const quit = () => window.flashcards.app.quit()

  return (
    <div className="screen">
      <LeaveButton onLeave={quit} label="Quit application" />
      <h1>Flashcards</h1>
      <div className="menu" data-testid="title-menu">
        <button onClick={() => onGo('learn')} disabled={!canLearn}>Learn</button>
        <button onClick={() => onGo('packs')}>Pack Selection</button>
        <button onClick={() => onGo('makepack')}>Make a Pack</button>
        <button onClick={() => onGo('settings')}>Settings</button>
        <button onClick={quit}>Quit</button>
      </div>
      <p className="hint">{canLearn ? '' : 'Select at least one pack to start learning.'}</p>
      {/* Test-only affordance for re-reading persisted state without a relaunch. */}
      <button data-testid="btn-reload" onClick={onReload} style={{ position: 'fixed', opacity: 0, pointerEvents: 'auto', bottom: 0, right: 0 }}>
        reload
      </button>
    </div>
  )
}
```

- [ ] **Step 6: Rewrite `src/app/App.tsx`**

```tsx
import { useCallback, useEffect, useReducer, useState } from 'react'
import type { AppState, PackMeta, Settings } from '../../shared/types'
import Background from '../components/Background'
import TitleScreen from '../screens/TitleScreen'
import { initialUI, reduce, type Screen } from './machine'
import '../styles/screens.css'

export default function App() {
  const [ui, dispatch] = useReducer(reduce, initialUI)
  const [packs, setPacks] = useState<PackMeta[]>([])
  const [state, setState] = useState<AppState | null>(null)
  const [settings, setSettings] = useState<Settings | null>(null)

  const reload = useCallback(async () => {
    const [p, s, cfg] = await Promise.all([
      window.flashcards.packs.list(),
      window.flashcards.state.get(),
      window.flashcards.settings.get(),
    ])
    // Drop selections pointing at packs that no longer exist.
    const live = new Set(p.map(x => x.id))
    const cleaned = s.selectedPackIds.filter(id => live.has(id))
    if (cleaned.length !== s.selectedPackIds.length) {
      await window.flashcards.state.set({ selectedPackIds: cleaned })
    }
    setPacks(p)
    setState({ ...s, selectedPackIds: cleaned })
    setSettings(cfg)
  }, [])

  useEffect(() => { void reload() }, [reload])

  if (!state || !settings) return <div data-testid="app-root" />

  const go = (screen: Screen) => dispatch({ type: 'goto', screen })

  return (
    <div data-testid="app-root">
      <Background mode={settings.themeMode} />
      {ui.screen === 'title' && (
        <TitleScreen
          canLearn={packs.length > 0 && state.selectedPackIds.length > 0}
          onGo={go}
          onReload={() => void reload()}
        />
      )}
    </div>
  )
}
```

- [ ] **Step 7: Run the tests**

```bash
npm run test:e2e -- tests/title.spec.ts
```

Expected: 3 passed.

- [ ] **Step 8: Commit**

```bash
git add src tests/title.spec.ts
git commit -m "feat(flashcards): add app shell and title screen with quit"
```

---

## Task 8: Confirm dialog and leave behavior

**Files:**
- Create: `src/components/ConfirmDialog.tsx`, `src/screens/SettingsScreen.tsx`, `src/screens/MakePackScreen.tsx`, `src/styles/overlay.css`
- Modify: `src/app/App.tsx`
- Test: `tests/leave.spec.ts`

**Interfaces:**
- Consumes: `machine.ts`, `keys.ts`, `ConfirmIntent`
- Produces: `<ConfirmDialog intent onConfirm onDeny />`; `CONFIRM_COPY: Record<ConfirmIntent['type'], string>`

Settings and Make a Pack ship as placeholders with correct leave behavior and nothing else.

- [ ] **Step 1: Write the failing tests**

```ts
// tests/leave.spec.ts
import { test, expect } from '@playwright/test'
import { launchApp } from './helpers'

test('Escape twice leaves; Escape then Space stays', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Settings' }).click()
  await expect(win.getByTestId('screen-settings')).toBeVisible()

  await win.keyboard.press('Escape')
  await expect(win.getByTestId('confirm-dialog')).toBeVisible()

  await win.keyboard.press('Space')
  await expect(win.getByTestId('confirm-dialog')).toBeHidden()
  await expect(win.getByTestId('screen-settings')).toBeVisible()

  await win.keyboard.press('Escape')
  await win.keyboard.press('Escape')
  await expect(win.getByTestId('title-menu')).toBeVisible()

  await app.close()
})

test('Enter also denies the confirmation', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Make a Pack' }).click()
  await win.keyboard.press('Escape')
  await win.keyboard.press('Enter')
  await expect(win.getByTestId('confirm-dialog')).toBeHidden()
  await expect(win.getByTestId('screen-makepack')).toBeVisible()
  await app.close()
})

test('each screen asks its own question', async () => {
  const { app } = await launchApp()
  const win = await app.firstWindow()

  await win.getByRole('button', { name: 'Settings' }).click()
  await win.keyboard.press('Escape')
  await expect(win.getByTestId('confirm-dialog')).toContainText('Save settings before leaving?')
  await win.keyboard.press('Escape')

  await win.getByRole('button', { name: 'Make a Pack' }).click()
  await win.keyboard.press('Escape')
  await expect(win.getByTestId('confirm-dialog')).toContainText('Are you sure you want to leave?')

  await app.close()
})
```

- [ ] **Step 2: Run and confirm failure**

```bash
npm run test:e2e -- tests/leave.spec.ts
```

Expected: FAIL — no settings screen.

- [ ] **Step 3: Create `src/styles/overlay.css`**

```css
.overlay {
  position: fixed;
  inset: 0;
  z-index: 10;
  display: grid;
  place-items: center;
  background: rgba(3, 4, 12, 0.72);
  animation: fade-in 140ms var(--ease);
}

@keyframes fade-in { from { opacity: 0; } to { opacity: 1; } }

.dialog {
  min-width: 320px;
  padding: 1.5rem;
  text-align: center;
  background: var(--card-face);
  border: 1px solid var(--card-edge);
  border-radius: var(--radius);
}

.dialog__actions { display: flex; gap: 0.75rem; justify-content: center; margin-top: 1.25rem; }

.dialog__actions button {
  padding: 0.6rem 1.1rem;
  color: var(--fg);
  background: transparent;
  border: 1px solid var(--card-edge);
  border-radius: 10px;
  cursor: pointer;
}
.dialog__actions button:hover { border-color: var(--accent); }
```

- [ ] **Step 4: Create `src/components/ConfirmDialog.tsx`**

```tsx
import type { ConfirmIntent } from '../../shared/types'
import '../styles/overlay.css'

export const CONFIRM_COPY: Record<ConfirmIntent['type'], string> = {
  leaveLearn: 'Return to Main Menu?',
  leavePacks: 'Return to Main Menu?',
  leaveSettings: 'Save settings before leaving?',
  leaveMakePack: 'Are you sure you want to leave?',
}

type Props = { intent: ConfirmIntent; onConfirm: () => void; onDeny: () => void }

export default function ConfirmDialog({ intent, onConfirm, onDeny }: Props) {
  return (
    <div className="overlay" data-testid="confirm-dialog">
      <div className="dialog" role="dialog" aria-modal="true">
        <p>{CONFIRM_COPY[intent.type]}</p>
        <div className="dialog__actions">
          <button onClick={onConfirm} data-testid="confirm-yes">Yes</button>
          <button onClick={onDeny} data-testid="confirm-no">No</button>
        </div>
      </div>
    </div>
  )
}
```

- [ ] **Step 5: Create the two placeholder screens**

```tsx
// src/screens/SettingsScreen.tsx
import LeaveButton from '../components/LeaveButton'

export default function SettingsScreen({ onRequestLeave }: { onRequestLeave: () => void }) {
  return (
    <div className="screen" data-testid="screen-settings">
      <LeaveButton onLeave={onRequestLeave} label="Return to main menu" />
      <h2>Settings</h2>
      <p className="hint">Nothing here yet.</p>
    </div>
  )
}
```

```tsx
// src/screens/MakePackScreen.tsx
import LeaveButton from '../components/LeaveButton'

export default function MakePackScreen({ onRequestLeave }: { onRequestLeave: () => void }) {
  return (
    <div className="screen" data-testid="screen-makepack">
      <LeaveButton onLeave={onRequestLeave} label="Return to main menu" />
      <h2>Make a Pack</h2>
      <p className="hint">Not built yet.</p>
    </div>
  )
}
```

- [ ] **Step 6: Wire the key listener and layers into `src/app/App.tsx`**

Add above the `return`:

```tsx
import { keyActionFor } from './keys'
import { topLayer } from './machine'
import ConfirmDialog from '../components/ConfirmDialog'
import SettingsScreen from '../screens/SettingsScreen'
import MakePackScreen from '../screens/MakePackScreen'
import type { ConfirmIntent } from '../../shared/types'

const LEAVE_INTENT: Record<Screen, ConfirmIntent | null> = {
  title: null,
  learn: { type: 'leaveLearn' },
  packs: { type: 'leavePacks' },
  settings: { type: 'leaveSettings' },
  makepack: { type: 'leaveMakePack' },
}
```

Inside the component, the **single** global key listener in the app:

```tsx
const requestLeave = useCallback(() => {
  const intent = LEAVE_INTENT[ui.screen]
  if (intent) dispatch({ type: 'pushLayer', layer: { kind: 'confirm', intent } })
}, [ui.screen])

useEffect(() => {
  const onKey = (e: KeyboardEvent) => {
    const intent = keyActionFor(ui, e.key, { hasFlipped: false, card: null })
    if (!intent) return
    e.preventDefault()   // Tab must not move focus; Space must not scroll.
    switch (intent.type) {
      case 'requestLeave': requestLeave(); break
      case 'confirmLeave': dispatch({ type: 'goto', screen: 'title' }); break
      case 'denyLeave': dispatch({ type: 'popLayer' }); break
    }
  }
  window.addEventListener('keydown', onKey)
  return () => window.removeEventListener('keydown', onKey)
  // `cardCtx` MUST be in the deps once Task 11 feeds real card context in — the handler
  // reads cardCtx.hasFlipped, and a stale `false` would gate Enter permanently.
}, [ui, requestLeave, cardCtx])
```

And render the screens plus the layer host:

```tsx
{ui.screen === 'settings' && <SettingsScreen onRequestLeave={requestLeave} />}
{ui.screen === 'makepack' && <MakePackScreen onRequestLeave={requestLeave} />}

{topLayer(ui)?.kind === 'confirm' && (
  <ConfirmDialog
    intent={(topLayer(ui) as { kind: 'confirm'; intent: ConfirmIntent }).intent}
    onConfirm={() => dispatch({ type: 'goto', screen: 'title' })}
    onDeny={() => dispatch({ type: 'popLayer' })}
  />
)}
```

- [ ] **Step 7: Run the tests**

```bash
npm run test:e2e -- tests/leave.spec.ts
```

Expected: 3 passed.

- [ ] **Step 8: Commit**

```bash
git add src tests/leave.spec.ts
git commit -m "feat(flashcards): add confirm dialog and per-screen leave behavior"
```

---

## Task 9: Pack Selection with persistence

**Files:**
- Create: `src/screens/PackSelectScreen.tsx`, `src/styles/packs.css`
- Modify: `src/app/App.tsx`
- Test: `tests/packs.spec.ts`

**Interfaces:**
- Consumes: `PackMeta`, `AppState`, `LeaveButton`
- Produces: `<PackSelectScreen packs selectedIds onToggle onRequestLeave />`

- [ ] **Step 1: Write the failing tests**

Test 3 is the important one — it relaunches against the *same* profile, which is the only way to prove persistence really works.

```ts
// tests/packs.spec.ts
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
```

- [ ] **Step 2: Run and confirm failure**

```bash
npm run test:e2e -- tests/packs.spec.ts
```

Expected: FAIL — no pack screen.

- [ ] **Step 3: Create `src/styles/packs.css`**

```css
.pack-list { display: flex; flex-direction: column; gap: 0.5rem; min-width: 420px; }

.pack-row {
  display: flex;
  align-items: center;
  gap: 0.9rem;
  padding: 0.9rem 1.1rem;
  background: var(--card-face);
  border: 1px solid var(--card-edge);
  border-radius: var(--radius);
}

.pack-row__select { display: flex; align-items: center; gap: 0.9rem; flex: 1; }
.pack-row__name { flex: 1; }
.pack-row__count { color: var(--muted); font-size: 0.85rem; }

.pack-row__preview {
  padding: 0.4rem 0.75rem;
  color: var(--muted);
  background: transparent;
  border: 1px solid var(--card-edge);
  border-radius: 8px;
  cursor: pointer;
}
.pack-row__preview:hover { color: var(--fg); border-color: var(--accent); }
```

- [ ] **Step 4: Create `src/screens/PackSelectScreen.tsx`**

```tsx
import type { PackMeta } from '../../shared/types'
import LeaveButton from '../components/LeaveButton'
import '../styles/packs.css'

type Props = {
  packs: PackMeta[]
  selectedIds: string[]
  onToggle: (id: string, selected: boolean) => void
  onPreview: (id: string) => void
  onRequestLeave: () => void
}

export default function PackSelectScreen(
  { packs, selectedIds, onToggle, onPreview, onRequestLeave }: Props,
) {
  return (
    <div className="screen" data-testid="screen-packs">
      <LeaveButton onLeave={onRequestLeave} label="Return to main menu" />
      <h2>Pack Selection</h2>

      {packs.length === 0 && <p className="hint">No packs found.</p>}

      <div className="pack-list">
        {packs.map(p => (
          {/* The Preview button is a SIBLING of the label, not inside it: <label>'s
              content model forbids nested labelable elements, and nesting folds
              "Preview" into the checkbox's accessible name. */}
          <div className="pack-row" key={p.id}>
            <label className="pack-row__select">
              <input
                type="checkbox"
                data-testid={`pack-toggle-${p.id}`}
                checked={selectedIds.includes(p.id)}
                onChange={e => onToggle(p.id, e.target.checked)}
              />
              <span className="pack-row__name">{p.name}</span>
              <span className="pack-row__count">{p.cardCount} cards</span>
            </label>
            <button
              type="button"
              className="pack-row__preview"
              data-testid={`pack-preview-${p.id}`}
              onClick={() => onPreview(p.id)}
            >
              Preview
            </button>
          </div>
        ))}
      </div>
    </div>
  )
}
```

- [ ] **Step 5: Wire it into `src/app/App.tsx`**

Selection writes through immediately so it survives a crash as well as a clean exit.

```tsx
// The IPC write stays OUTSIDE the setState updater — StrictMode invokes updaters
// twice, which would double-fire the disk write.
const togglePack = useCallback(async (id: string, selected: boolean) => {
  if (!state) return
  const next = selected
    ? [...state.selectedPackIds, id]
    : state.selectedPackIds.filter(x => x !== id)
  setState({ ...state, selectedPackIds: next })
  await window.flashcards.state.set({ selectedPackIds: next })
}, [state])
```

```tsx
{ui.screen === 'packs' && (
  <PackSelectScreen
    packs={packs}
    selectedIds={state.selectedPackIds}
    onToggle={(id, sel) => void togglePack(id, sel)}
    onPreview={setPreviewPackId}
    onRequestLeave={requestLeave}
  />
)}
```

Add the preview state alongside the other state. Only the setter is destructured —
`previewPackId` itself has no reader until Task 13, and `noUnusedLocals` rejects an unused
binding:

```tsx
// Task 13 puts `previewPackId` back in the hole and reads it.
const [, setPreviewPackId] = useState<string | null>(null)
```

- [ ] **Step 6: Run the tests**

```bash
npm run test:e2e -- tests/packs.spec.ts
```

Expected: 3 passed.

- [ ] **Step 7: Commit**

```bash
git add src tests/packs.spec.ts
git commit -m "feat(flashcards): add pack selection with write-through persistence"
```

---

## Task 10: The card and its flip

**Files:**
- Create: `src/components/Card.tsx`, `src/styles/card.css`
- Test: `tests/card.spec.ts`

**Interfaces:**
- Consumes: `Card` type
- Produces: `<Card card onFirstFlip onOpenMoreInfo flipSignal />` — imperative `flipSignal` lets the keyboard flip a card whose flip state is local

**This is the task the whole app is judged on.** The flip animates `transform` only, keeps its state local, and never re-renders the screen tree.

> **Ordering note:** Tasks 10 and 11 are mutually dependent — `Card` has no host until
> `LearnScreen` exists. Write `Card.tsx` and `tests/card.spec.ts` here, then implement
> Task 11, then run both suites together at Task 11 Step 7. Treat 10 and 11 as one review
> gate.

- [ ] **Step 1: Write the failing tests**

```ts
// tests/card.spec.ts
import { test, expect } from '@playwright/test'
import { launchWithFixture } from './helpers'

async function enterLearn(win: import('@playwright/test').Page) {
  await win.getByRole('button', { name: 'Learn' }).click()
  await expect(win.getByTestId('card')).toBeVisible()
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
```

- [ ] **Step 2: Run and confirm failure**

```bash
npm run test:e2e -- tests/card.spec.ts
```

Expected: FAIL — no `card` testid.

- [ ] **Step 3: Create `src/styles/card.css`**

```css
.card-stage {
  position: relative;
  z-index: 1;
  width: min(620px, 84vw);
  height: min(400px, 52vh);
  perspective: 1400px;
  /* Card work cannot invalidate the rest of the tree. */
  contain: layout paint;
}

.card-inner {
  position: relative;
  width: 100%;
  height: 100%;
  transform-style: preserve-3d;
  /* transform ONLY — never a layout or paint property. */
  transition: transform var(--flip-ms) var(--ease);
  transition-property: transform;
  cursor: pointer;
}

.card-inner[data-flipped='true'] { transform: rotateY(180deg); }

.card-face {
  position: absolute;
  inset: 0;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 1rem;
  padding: 2rem;
  text-align: center;
  background: var(--card-face);
  border: 1px solid var(--card-edge);
  border-radius: var(--radius);
  backface-visibility: hidden;
  -webkit-backface-visibility: hidden;
}

.card-face--back { transform: rotateY(180deg); }
.card-face__front-text { font-size: 1.9rem; font-weight: 600; }
.card-face__primary { font-size: 1.15rem; }

.card-face__more {
  padding: 0.5rem 0.9rem;
  color: var(--accent);
  background: transparent;
  border: 1px solid var(--card-edge);
  border-radius: 10px;
  cursor: pointer;
}
.card-face__more:hover { border-color: var(--accent); }

/* Offscreen preload of the next card — never visible, never interactive. */
.card-preload {
  position: absolute;
  opacity: 0;
  pointer-events: none;
  visibility: hidden;
}
```

- [ ] **Step 4: Create `src/components/Card.tsx`**

```tsx
import { useEffect, useRef, useState } from 'react'
import type { Card as CardType } from '../../shared/types'
import '../styles/card.css'

type Props = {
  card: CardType
  /** Bumped by the parent to request a flip from the keyboard. */
  flipSignal?: number
  onFirstFlip?: () => void
  onOpenMoreInfo?: () => void
}

/**
 * Flip state is LOCAL — it never enters the app reducer, so flipping does not
 * re-render the screen tree. `will-change` is applied only while animating.
 */
export default function Card({ card, flipSignal = 0, onFirstFlip, onOpenMoreInfo }: Props) {
  const [flipped, setFlipped] = useState(false)
  const [animating, setAnimating] = useState(false)
  const hasFlippedRef = useRef(false)
  const firstSignal = useRef(flipSignal)

  const flip = () => {
    setFlipped(f => !f)
    setAnimating(true)
    if (!hasFlippedRef.current) {
      hasFlippedRef.current = true
      onFirstFlip?.()
    }
  }

  // Reset when the card changes.
  useEffect(() => {
    setFlipped(false)
    hasFlippedRef.current = false
  }, [card.id])

  // Respond to keyboard-driven flips without lifting flip state out of here.
  useEffect(() => {
    if (flipSignal === firstSignal.current) return
    flip()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [flipSignal])

  return (
    <div
      className="card-stage"
      data-testid="card"
      data-card-id={card.id}
      data-flipped={flipped}
    >
      <div
        className="card-inner"
        data-testid="card-inner"
        data-flipped={flipped}
        style={{ willChange: animating ? 'transform' : 'auto' }}
        onTransitionEnd={() => setAnimating(false)}
        onClick={flip}
      >
        <div className="card-face">
          <div className="card-face__front-text">{card.front}</div>
        </div>

        <div className="card-face card-face--back">
          <div className="card-face__primary">{card.primary}</div>

          {/* Rendered only when there is actually something to show. */}
          {card.moreInfo && (
            <button
              type="button"
              className="card-face__more"
              data-testid="more-info-btn"
              onClick={e => { e.stopPropagation(); onOpenMoreInfo?.() }}
            >
              More Info
            </button>
          )}
        </div>
      </div>
    </div>
  )
}
```

`e.stopPropagation()` is what makes "clicking the card always flips it **unless** the click landed on More Info" true.

- [ ] **Step 5: Stage the work — do not run these tests yet**

`tests/card.spec.ts` drives the Learn screen, which does not exist until Task 11. Running
it now fails for the wrong reason. Proceed to Task 11 and run both suites at its Step 7.

```bash
git add src/components/Card.tsx src/styles/card.css tests/card.spec.ts
git commit -m "feat(flashcards): add flip card with composited transform and local state"
```

---

## Task 11: Learn screen — deck, advance gate, session end

**Files:**
- Create: `src/screens/LearnScreen.tsx`, `src/components/AdvanceArrow.tsx`, `src/styles/learn.css`
- Modify: `src/app/App.tsx`
- Test: `tests/learn.spec.ts`

**Interfaces:**
- Consumes: `Card`, `shuffle`, `Card.tsx`
- Produces: `<LearnScreen packIds onRequestLeave />`; `<AdvanceArrow onAdvance />`

- [ ] **Step 1: Write the failing tests**

```ts
// tests/learn.spec.ts
import { test, expect } from '@playwright/test'
import { launchWithFixture } from './helpers'

test('the advance arrow is absent before flip and present after', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()

  await expect(win.getByTestId('advance-arrow')).toBeHidden()
  await win.getByTestId('card').click()
  await expect(win.getByTestId('advance-arrow')).toBeVisible()

  await app.close()
})

test('Enter does nothing before flip and advances after', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()

  const first = await win.getByTestId('card').getAttribute('data-card-id')
  await win.keyboard.press('Enter')
  expect(await win.getByTestId('card').getAttribute('data-card-id')).toBe(first)

  await win.keyboard.press('Space')
  await win.keyboard.press('Enter')
  expect(await win.getByTestId('card').getAttribute('data-card-id')).not.toBe(first)

  await app.close()
})

test('the arrow stays visible after flipping back to the front', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()
  await win.getByTestId('card').click()
  await win.getByTestId('card').click()
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'false')
  await expect(win.getByTestId('advance-arrow')).toBeVisible()
  await app.close()
})

test('exhausting the deck shows the end screen with no score', async () => {
  const { app } = await launchWithFixture()   // exactly 3 cards
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()

  for (let i = 0; i < 3; i++) {
    await win.keyboard.press('Space')
    await win.keyboard.press('Enter')
  }

  await expect(win.getByTestId('session-end')).toBeVisible()
  await expect(win.getByTestId('session-end')).not.toContainText('/')
  await win.getByTestId('reshuffle-btn').click()
  await expect(win.getByTestId('card')).toBeVisible()

  await app.close()
})
```

- [ ] **Step 2: Run and confirm failure**

```bash
npm run test:e2e -- tests/learn.spec.ts
```

Expected: FAIL — no learn screen.

- [ ] **Step 3: Create `src/styles/learn.css`**

```css
.learn {
  position: relative;
  z-index: 1;
  height: 100%;
  display: grid;
  place-items: center;
}

.advance {
  position: fixed;
  right: 2rem;
  bottom: 2rem;
  z-index: 5;
  width: 56px;
  height: 56px;
  display: grid;
  place-items: center;
  color: var(--fg);
  background: var(--card-face);
  border: 1px solid var(--card-edge);
  border-radius: 50%;
  cursor: pointer;
  /* Fade in only — opacity and transform, nothing else. */
  animation: advance-in 260ms var(--ease);
}

@keyframes advance-in {
  from { opacity: 0; transform: translateX(-6px); }
  to   { opacity: 1; transform: translateX(0); }
}

.advance:hover { border-color: var(--accent); }

.session-end { display: flex; flex-direction: column; align-items: center; gap: 1.25rem; }
```

- [ ] **Step 4: Create `src/components/AdvanceArrow.tsx`**

No text — the arrow only.

```tsx
export default function AdvanceArrow({ onAdvance }: { onAdvance: () => void }) {
  return (
    <button
      className="advance"
      data-testid="advance-arrow"
      onClick={onAdvance}
      aria-label="Next card"
    >
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <line x1="5" y1="12" x2="19" y2="12" />
        <polyline points="12 5 19 12 12 19" />
      </svg>
    </button>
  )
}
```

- [ ] **Step 5: Create `src/screens/LearnScreen.tsx`**

```tsx
import { useCallback, useEffect, useState } from 'react'
import type { Card as CardType } from '../../shared/types'
import CardView from '../components/Card'
import AdvanceArrow from '../components/AdvanceArrow'
import LeaveButton from '../components/LeaveButton'
import { shuffle } from '../lib/shuffle'
import '../styles/learn.css'

type Props = {
  packIds: string[]
  flipSignal: number
  advanceSignal: number
  onCardContext: (card: CardType | null, hasFlipped: boolean) => void
  onOpenMoreInfo: () => void
  onRequestLeave: () => void
}

export default function LearnScreen({
  packIds, flipSignal, advanceSignal, onCardContext, onOpenMoreInfo, onRequestLeave,
}: Props) {
  const [allCards, setAllCards] = useState<CardType[] | null>(null)
  const [deck, setDeck] = useState<CardType[]>([])
  const [index, setIndex] = useState(0)
  const [hasFlipped, setHasFlipped] = useState(false)
  const [seed, setSeed] = useState(0)

  // Load every selected pack ONCE, up front. Nothing is read from disk mid-session.
  useEffect(() => {
    let cancelled = false
    void (async () => {
      const packs = await Promise.all(packIds.map(id => window.flashcards.packs.read(id)))
      if (!cancelled) setAllCards(packs.flatMap(p => p.cards))
    })()
    return () => { cancelled = true }
  }, [packIds])

  // Reshuffle on entry and on every explicit reshuffle.
  //
  // IMPORTANT: deck, index and hasFlipped must change in ONE commit. If they are separate
  // state updated from an effect, there is a render where allCards is set but deck is
  // still [] — and `index >= deck.length` is `0 >= 0`, which flashes the END SCREEN for a
  // frame before the first card appears. Hold them in a single session object.
  useEffect(() => {
    if (!allCards) return
    setSession({ deck: shuffle(allCards), index: 0, hasFlipped: false })
  }, [allCards, seed])

  const current = deck[index] ?? null
  const next = deck[index + 1] ?? null

  // `onCardContext` MUST be a stable useCallback in the parent. An inline arrow here
  // gives the effect a new identity every render and spins an infinite loop.
  useEffect(() => { onCardContext(current, hasFlipped) }, [current, hasFlipped, onCardContext])

  const advance = useCallback(() => {
    if (!hasFlipped) return
    setIndex(i => i + 1)
    setHasFlipped(false)
  }, [hasFlipped])

  // Parent signals drive keyboard advance without lifting flip state out of Card.
  // Baseline against the value at mount, the same pattern Card uses for flipSignal:
  // App's counter persists across Learn sessions, so a `> 0` test would fire an advance
  // on every re-entry into the screen.
  const advanceBaseline = useRef(advanceSignal)
  useEffect(() => {
    if (advanceSignal === advanceBaseline.current) return
    advance()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [advanceSignal])

  if (!allCards) return <div className="learn" data-testid="learn-loading" />

  if (index >= deck.length) {
    return (
      <div className="learn" data-testid="screen-learn">
        <LeaveButton onLeave={onRequestLeave} label="Return to main menu" />
        <div className="session-end" data-testid="session-end">
          <h2>You&rsquo;ve reached the end of the deck.</h2>
          <div className="dialog__actions">
            <button data-testid="reshuffle-btn" onClick={() => setSeed(s => s + 1)}>Reshuffle</button>
            <button data-testid="end-leave-btn" onClick={onRequestLeave}>Main Menu</button>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="learn" data-testid="screen-learn">
      <LeaveButton onLeave={onRequestLeave} label="Return to main menu" />

      {/* `key={index}` guards the merged deck: card ids are unique only WITHIN a pack,
          so two selected packs can both contain "c1". Keying on position guarantees a
          remount (and therefore a front-face reset) on every advance regardless. */}
      {current && (
        <CardView
          key={index}
          card={current}
          flipSignal={flipSignal}
          onFirstFlip={() => setHasFlipped(true)}
          onOpenMoreInfo={onOpenMoreInfo}
        />
      )}

      {/* Pre-mounted so advancing swaps an element rather than mounting one.
          `preload` is REQUIRED: it renames the test ids to card-preload/-inner and sets
          aria-hidden. Without it there are two data-testid="card" nodes in the DOM and
          every card/learn locator fails with a Playwright strict-mode violation. */}
      {next && (
        <div className="card-preload" aria-hidden="true">
          <CardView card={next} preload />
        </div>
      )}

      {hasFlipped && <AdvanceArrow onAdvance={advance} />}
    </div>
  )
}
```

- [ ] **Step 6: Wire Learn into `src/app/App.tsx`**

Add signal state and extend the key handler:

```tsx
const [flipSignal, setFlipSignal] = useState(0)
const [advanceSignal, setAdvanceSignal] = useState(0)
const [cardCtx, setCardCtx] = useState<{ card: CardType | null; hasFlipped: boolean }>(
  { card: null, hasFlipped: false },
)

// Stable identity — see the note in LearnScreen. Bail out when nothing changed so
// a repeat call cannot trigger another render.
const handleCardContext = useCallback((card: CardType | null, hasFlipped: boolean) => {
  // Reference equality, NOT id equality: two selected packs can both contain "c1",
  // and an id compare would leave cardCtx pointing at the previous pack's card
  // (and therefore the wrong moreInfo). Deck elements are stable objects.
  setCardCtx(prev =>
    prev.card === card && prev.hasFlipped === hasFlipped ? prev : { card, hasFlipped },
  )
}, [])
```

In the key handler's `switch`, add:

```tsx
case 'flip': setFlipSignal(n => n + 1); break
case 'advance': setAdvanceSignal(n => n + 1); break
case 'openMoreInfo': dispatch({ type: 'pushLayer', layer: { kind: 'moreInfo' } }); break
case 'closeMoreInfo': dispatch({ type: 'popLayer' }); break
```

and pass the real context into `keyActionFor`:

```tsx
const intent = keyActionFor(ui, e.key, {
  hasFlipped: cardCtx.hasFlipped,
  card: cardCtx.card,
})
```

Render it:

```tsx
{ui.screen === 'learn' && (
  <LearnScreen
    packIds={state.selectedPackIds}
    flipSignal={flipSignal}
    advanceSignal={advanceSignal}
    onCardContext={handleCardContext}
    onOpenMoreInfo={() => dispatch({ type: 'pushLayer', layer: { kind: 'moreInfo' } })}
    onRequestLeave={requestLeave}
  />
)}
```

- [ ] **Step 7: Run the Learn and Card suites**

```bash
npm run test:e2e -- tests/learn.spec.ts tests/card.spec.ts
```

Expected: 7 passed.

- [ ] **Step 8: Commit**

```bash
git add src tests/learn.spec.ts
git commit -m "feat(flashcards): add learn screen with shuffled deck and flip-gated advance"
```

---

## Task 12: More Info panel

**Files:**
- Create: `src/components/MoreInfoPanel.tsx`, `src/lib/markdown.ts`, `src/styles/moreinfo.css`
- Modify: `src/app/App.tsx`
- Test: `tests/moreinfo.spec.ts`, `src/lib/markdown.test.ts`

**Interfaces:**
- Consumes: `marked`, `DOMPurify`, `machine.ts`
- Produces: `renderMarkdown(src: string): string`; `<MoreInfoPanel markdown onClose scrollSignal />`

- [ ] **Step 1: Write the failing markdown unit test**

Sanitizing matters because pack files will eventually be MCP-written.

```ts
// src/lib/markdown.test.ts
import { describe, it, expect } from 'vitest'
import { renderMarkdown } from './markdown'

describe('renderMarkdown', () => {
  it('renders basic markdown', () => {
    expect(renderMarkdown('# Title')).toContain('<h1')
    expect(renderMarkdown('**bold**')).toContain('<strong>')
  })

  it('renders tables, which packs will need later', () => {
    const md = '| a | b |\n| - | - |\n| 1 | 2 |'
    expect(renderMarkdown(md)).toContain('<table')
  })

  it('strips script tags', () => {
    expect(renderMarkdown('<script>alert(1)</script>')).not.toContain('<script')
  })

  it('strips inline event handlers', () => {
    expect(renderMarkdown('<img src=x onerror="alert(1)">')).not.toContain('onerror')
  })

  it('handles empty input', () => {
    expect(renderMarkdown('')).toBe('')
  })
})
```

- [ ] **Step 2: Run and confirm failure**

```bash
npx vitest run src/lib/markdown.test.ts
```

Expected: FAIL — cannot resolve `./markdown`.

- [ ] **Step 3: Implement `src/lib/markdown.ts`**

```ts
import { marked } from 'marked'
import DOMPurify from 'dompurify'

marked.setOptions({ gfm: true, breaks: true })

/** Parse Markdown and sanitize. Pack content will eventually be MCP-written. */
export function renderMarkdown(src: string): string {
  if (!src) return ''
  return DOMPurify.sanitize(marked.parse(src, { async: false }) as string)
}
```

- [ ] **Step 4: Run and confirm the unit tests pass**

```bash
npx vitest run src/lib/markdown.test.ts
```

Expected: 5 passed.

- [ ] **Step 5: Write the failing panel tests**

Tests 4 and 5 cover exactly the gap the user flagged: no More Info means no control and no `Tab` response.

```ts
// tests/moreinfo.spec.ts
import { test, expect } from '@playwright/test'
import { launchWithFixture } from './helpers'

// The shipped presidents pack has NO moreInfo on any card, so these tests run
// against the fixture, whose f1/f2 carry moreInfo and f3 deliberately does not.

test('Tab does nothing before flip and opens the panel after', async () => {
  const { app } = await launchWithFixture()
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
  const { app } = await launchWithFixture()
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
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()
  await win.keyboard.press('Space')
  const id = await win.getByTestId('card').getAttribute('data-card-id')
  await win.keyboard.press('Tab')

  await win.keyboard.press('Space')
  await win.keyboard.press('Enter')
  await expect(win.getByTestId('card')).toHaveAttribute('data-flipped', 'true')
  expect(await win.getByTestId('card').getAttribute('data-card-id')).toBe(id)

  await app.close()
})

test('Escape closes only the panel; a second Escape raises the confirm', async () => {
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()
  await win.keyboard.press('Space')
  await win.keyboard.press('Tab')

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
  const { app } = await launchWithFixture()
  const win = await app.firstWindow()
  await win.getByRole('button', { name: 'Learn' }).click()
  await win.keyboard.press('Space')
  await win.keyboard.press('Tab')
  await win.getByTestId('more-info-close').click()
  await expect(win.getByTestId('more-info-panel')).toBeHidden()
  await app.close()
})
```

- [ ] **Step 6: Create `src/styles/moreinfo.css`**

```css
.moreinfo {
  position: fixed;
  inset: 0;
  z-index: 12;
  display: grid;
  place-items: center;
  background: rgba(3, 4, 12, 0.78);
  animation: fade-in 160ms var(--ease);
}

.moreinfo__panel {
  position: relative;
  width: min(700px, 88vw);
  max-height: 74vh;
  padding: 2rem;
  overflow-y: auto;
  background: var(--card-face);
  border: 1px solid var(--card-edge);
  border-radius: var(--radius);
  overscroll-behavior: contain;
}

.moreinfo__close {
  position: absolute;
  top: 0.75rem;
  right: 0.75rem;
  width: 30px;
  height: 30px;
  color: var(--muted);
  background: transparent;
  border: 1px solid var(--card-edge);
  border-radius: 8px;
  cursor: pointer;
}
.moreinfo__close:hover { color: var(--fg); border-color: var(--accent); }

.moreinfo__panel table { width: 100%; border-collapse: collapse; }
.moreinfo__panel th, .moreinfo__panel td {
  padding: 0.4rem 0.6rem;
  border: 1px solid var(--card-edge);
}
.moreinfo__panel img { max-width: 100%; }
.moreinfo__panel code {
  padding: 0.1rem 0.35rem;
  background: rgba(255, 255, 255, 0.06);
  border-radius: 5px;
}
```

- [ ] **Step 7: Create `src/components/MoreInfoPanel.tsx`**

```tsx
import { useEffect, useMemo, useRef } from 'react'
import { renderMarkdown } from '../lib/markdown'
import '../styles/moreinfo.css'

type Props = { markdown: string; onClose: () => void; scrollSignal: number; scrollDir: 'up' | 'down' }

const STEP = 120

export default function MoreInfoPanel({ markdown, onClose, scrollSignal, scrollDir }: Props) {
  const bodyRef = useRef<HTMLDivElement>(null)

  // Parsed once per content change, never on open, so opening costs nothing.
  const html = useMemo(() => renderMarkdown(markdown), [markdown])

  useEffect(() => {
    if (scrollSignal === 0) return
    bodyRef.current?.scrollBy({ top: scrollDir === 'down' ? STEP : -STEP, behavior: 'smooth' })
  }, [scrollSignal, scrollDir])

  return (
    <div className="moreinfo" data-testid="more-info-panel">
      <div className="moreinfo__panel" ref={bodyRef}>
        <button
          className="moreinfo__close"
          data-testid="more-info-close"
          onClick={onClose}
          aria-label="Close"
        >
          ✕
        </button>
        <div dangerouslySetInnerHTML={{ __html: html }} />
      </div>
    </div>
  )
}
```

- [ ] **Step 8: Wire it into `src/app/App.tsx`**

```tsx
const [scrollSignal, setScrollSignal] = useState(0)
const [scrollDir, setScrollDir] = useState<'up' | 'down'>('down')
```

Add to the key handler `switch`:

```tsx
case 'scrollMoreInfo':
  setScrollDir(intent.direction)
  setScrollSignal(n => n + 1)
  break
```

Render above the confirm layer:

```tsx
{topLayer(ui)?.kind === 'moreInfo' && cardCtx.card?.moreInfo && (
  <MoreInfoPanel
    markdown={cardCtx.card.moreInfo}
    onClose={() => dispatch({ type: 'popLayer' })}
    scrollSignal={scrollSignal}
    scrollDir={scrollDir}
  />
)}
```

- [ ] **Step 9: Run the tests**

```bash
npm run test:e2e -- tests/moreinfo.spec.ts
```

Expected: 6 passed.

- [ ] **Step 10: Commit**

```bash
git add src tests/moreinfo.spec.ts
git commit -m "feat(flashcards): add sanitized markdown More Info panel with layered escape"
```

---

## Task 13: Pack preview

**Files:**
- Modify: `src/screens/PackSelectScreen.tsx`, `src/app/App.tsx`
- Test: `tests/preview.spec.ts`

**Interfaces:**
- Consumes: `Card.tsx`, `packs.read`
- Produces: preview mode inside Pack Selection

Preview reuses `Card` and its flip. It does **not** advance a deck, gate on flips, or touch session state.

- [ ] **Step 1: Write the failing tests**

```ts
// tests/preview.spec.ts
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

  const first = await win.getByTestId('card').getAttribute('data-card-id')
  await win.getByTestId('preview-next').click()
  expect(await win.getByTestId('card').getAttribute('data-card-id')).not.toBe(first)

  await win.getByTestId('preview-close').click()
  await expect(win.getByTestId('screen-packs')).toBeVisible()

  await app.close()
})
```

- [ ] **Step 2: Run and confirm failure**

```bash
npm run test:e2e -- tests/preview.spec.ts
```

Expected: FAIL — no preview UI.

- [ ] **Step 3: Add the preview view to `src/screens/PackSelectScreen.tsx`**

Add to `Props`: `previewPackId: string | null`, `onClosePreview: () => void`. Render before the list when a preview is active:

```tsx
import { useEffect, useState } from 'react'
import type { Card as CardType } from '../../shared/types'
import CardView from '../components/Card'

function PackPreview({ packId, onClose }: { packId: string; onClose: () => void }) {
  const [cards, setCards] = useState<CardType[] | null>(null)
  const [i, setI] = useState(0)

  useEffect(() => {
    let cancelled = false
    void window.flashcards.packs.read(packId).then(p => { if (!cancelled) setCards(p.cards) })
    return () => { cancelled = true }
  }, [packId])

  if (!cards || cards.length === 0) return null
  const card = cards[i % cards.length]

  return (
    <div className="screen" data-testid="screen-preview">
      <CardView card={card} />
      <div className="dialog__actions">
        <button data-testid="preview-prev" onClick={() => setI(n => (n - 1 + cards.length) % cards.length)}>
          Previous
        </button>
        <button data-testid="preview-next" onClick={() => setI(n => (n + 1) % cards.length)}>
          Next
        </button>
        <button data-testid="preview-close" onClick={onClose}>Close</button>
      </div>
    </div>
  )
}
```

Then in `PackSelectScreen`, before the existing markup:

```tsx
if (previewPackId) {
  return <PackPreview packId={previewPackId} onClose={onClosePreview} />
}
```

- [ ] **Step 4: Pass the props from `src/app/App.tsx`**

First restore the state binding Task 9 left setter-only:

```tsx
const [previewPackId, setPreviewPackId] = useState<string | null>(null)
```


```tsx
previewPackId={previewPackId}
onClosePreview={() => setPreviewPackId(null)}
```

- [ ] **Step 5: Run the tests**

```bash
npm run test:e2e -- tests/preview.spec.ts
```

Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add src tests/preview.spec.ts
git commit -m "feat(flashcards): add pack preview inside pack selection"
```

---

## Task 14: Full suite, resilience, and packaging

**Files:**
- Create: `electron-builder.yml`
- Test: `tests/resilience.spec.ts`

**Interfaces:**
- Consumes: everything above
- Produces: installers for macOS, Windows, and Linux

- [ ] **Step 1: Write the failing resilience tests**

These cover the §5.4 failure table — the cases a user actually hits.

```ts
// tests/resilience.spec.ts
import { test, expect } from '@playwright/test'
import fs from 'node:fs'
import path from 'node:path'
import { launchApp, makeUserDataDir } from './helpers'

test('a malformed pack is skipped and the good ones still load', async () => {
  const userData = makeUserDataDir()
  const broken = path.join(userData, 'packs', 'broken')
  fs.mkdirSync(broken, { recursive: true })
  fs.writeFileSync(path.join(broken, 'meta.json'), '{ not json')

  const { app } = await launchApp(userData)
  const win = await app.firstWindow()
  const packs = await win.evaluate(() => window.flashcards.packs.list())
  expect(packs.map((p: { id: string }) => p.id)).not.toContain('broken')
  await app.close()
})

test('malformed state.json falls back to defaults instead of blocking launch', async () => {
  const userData = makeUserDataDir()
  fs.writeFileSync(path.join(userData, 'state.json'), 'garbage')
  const { app } = await launchApp(userData)
  const win = await app.firstWindow()
  await expect(win.getByTestId('title-menu')).toBeVisible()
  await app.close()
})

test('a selection naming a missing pack is cleaned up on load', async () => {
  const { app, userData } = await launchApp()
  const w1 = await app.firstWindow()
  await w1.evaluate(() => window.flashcards.state.set({ selectedPackIds: ['presidents', 'ghost'] }))
  await w1.waitForTimeout(200)
  await app.close()

  const second = await launchApp(userData)
  const w2 = await second.app.firstWindow()
  const state = await w2.evaluate(() => window.flashcards.state.get())
  expect(state.selectedPackIds).toEqual(['presidents'])
  await second.app.close()
})
```

- [ ] **Step 2: Run and fix any gaps**

```bash
npm run test:e2e -- tests/resilience.spec.ts
```

Expected: 3 passed. If the third fails, confirm the cleanup in `App.reload()` writes back through `state.set`.

- [ ] **Step 3: Run the entire suite**

```bash
npm run test:unit && npm run test:e2e
```

Expected: every unit and Playwright test passes. Do not proceed past a failure.

- [ ] **Step 4: Create `electron-builder.yml`**

```yaml
appId: com.premo.flashcards
productName: Flashcards
directories:
  output: release
files:
  - dist/**
  - dist-electron/**
extraResources:
  - from: resources/packs
    to: packs
mac:
  target: dmg
  category: public.app-category.education
win:
  target: nsis
linux:
  target: AppImage
  category: Education
```

- [ ] **Step 5: Build installers for the current platform**

```bash
npm run dist
ls release
```

Expected: an installer for the host OS. Cross-building all three requires CI or the respective platforms; that is expected and not a failure.

- [ ] **Step 6: Manually verify the flip**

Automated tests prove behavior, not feel — and feel is the whole point.

```bash
npm run dev
```

Check by hand:
1. The flip is smooth with no stutter or flash of the reversed face.
2. In DevTools → Rendering → **Frame Rendering Stats**, the flip holds framerate.
3. Switch `settings.json` to `"themeMode": "static"` and confirm the starfield stops while the flip is unchanged.
4. Confirm no layout shift when the advance arrow fades in.

- [ ] **Step 7: Commit**

```bash
git add electron-builder.yml tests/resilience.spec.ts
git commit -m "feat(flashcards): add resilience tests and cross-platform packaging"
```

---

## Verification

Before calling this complete, per `superpowers:verification-before-completion`:

```bash
npm run test:unit     # shuffle, storage, machine, keys, markdown
npm run test:e2e      # launch, bridge, title, leave, packs, card, learn, moreinfo, preview, resilience
npm run build         # tsc --noEmit + vite build
```

Report actual output. Do not claim passing tests without having run them.

## Spec coverage

| Spec section | Task |
|---|---|
| §2 Stack | 1 |
| §3 Project structure | 1 |
| §4 Process architecture / IPC | 4 |
| §5.1 Types | 2 |
| §5.2 Storage layout | 3 |
| §5.3 First run | 4 |
| §5.4 Failure handling | 3, 14 |
| §6 State machine and layers | 5 |
| §7.1 Title | 7 |
| §7.2 Pack Selection | 9, 13 |
| §7.3 Learn | 10, 11 |
| §7.4 Placeholders | 8 |
| §8 Input model | 5, 8, 11, 12 |
| §9 Smoothness | 6, 10, 11 |
| §10 Theming | 6 |
| §11 Testing | every task |
| §12 Packaging | 14 |

## Out of scope

MCP server, theme picker UI, study modes, accounts, the Make a Pack authoring flow, and converting the country data in `_legacy/`.
