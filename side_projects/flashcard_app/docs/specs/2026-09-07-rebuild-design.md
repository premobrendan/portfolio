# Flashcard App Rebuild — Technical Design

**Date:** 2026-09-07
**Status:** proposed, awaiting review
**Companion:** [`../vision.md`](../vision.md) — the *what* and *why*. This document is the *how*.

---

## 1. Scope

Build the core loop of a cross-platform Electron flashcard app, from a clean directory.

**In scope:** Title screen, Pack Selection (multi-select, persistence, card preview), Learn
(flip, advance, More Info, full keyboard model, leave confirmation), the space theme, the
storage layer, and a small seeded sample pack.

**Stubbed:** Settings and Make a Pack exist as reachable placeholder screens with correct
leave behavior and nothing else.

**Out of scope:** MCP server, theme picker UI, study modes, accounts, the Make a Pack
authoring flow, and converting the 197 country cards parked in `_legacy/`.

**Non-negotiable:** the card flip is smooth. Every design choice below defers to that.

---

## 2. Stack

| Concern | Choice | Reason |
|---|---|---|
| Shell | Electron | One Chromium across macOS/Windows/Linux — tune the flip once. Node built in for the future MCP server. |
| UI | React 18 | Existing familiarity; the component model fits the screen/layer split. |
| Build | Vite + `vite-plugin-electron` | Fast HMR on the renderer, single config. |
| Packaging | `electron-builder` | Three targets from one config; code signing later. |
| Language | TypeScript | The IPC boundary and pack schema are exactly where types pay for themselves. |
| Styling | Plain CSS with custom properties | Theming is a stated requirement; CSS variables are the cheapest correct mechanism. No utility framework — this UI is small and bespoke. |
| Markdown | `marked` + `DOMPurify` | More Info is Markdown today and gains tables/images later. Sanitize because pack files will eventually be MCP-written. |
| Tests | Playwright (`_electron`) | Drives the real packaged app; the spec here is mostly interaction behavior. |

**No state library.** The app has one screen stack and one deck. Context plus `useReducer`
is sufficient and avoids a dependency that would tempt over-abstraction.

---

## 3. Project structure

```
flashcard_app/
  electron/
    main.ts            window lifecycle, app.quit, single instance
    storage.ts         all filesystem reads/writes; the ONLY fs consumer
    seed.ts            first-run copy of bundled packs into userData
    ipc.ts             channel handlers, one per bridge method
    preload.ts         contextBridge surface
  src/
    app/
      App.tsx          screen switch + layer host
      machine.ts       screen/layer reducer
      keys.ts          central keyboard dispatch
    screens/
      TitleScreen.tsx
      PackSelectScreen.tsx
      LearnScreen.tsx
      SettingsScreen.tsx      placeholder
      MakePackScreen.tsx      placeholder
    components/
      Card.tsx         the flip; owns its own flip state
      MoreInfoPanel.tsx
      ConfirmDialog.tsx
      LeaveButton.tsx
      AdvanceArrow.tsx
      Background.tsx   themed layer, isolated from Card
    theme/
      themes.ts        theme registry
      space.css        default theme tokens + dynamic starfield
    lib/
      shuffle.ts       Fisher-Yates, injectable RNG for tests
      markdown.ts      marked + DOMPurify wrapper
  shared/
    types.ts           Pack, Card, AppState, Settings — imported by both sides
  tests/
    *.spec.ts          Playwright
  _legacy/             parked country data, untouched by the build
  docs/
```

`shared/types.ts` is imported by both `electron/` and `src/`. It is the contract; nothing
else crosses the boundary.

---

## 4. Process architecture

Three processes, strictly separated. **The renderer never touches Node or the filesystem.**

- **Main** (`electron/`) — owns the `BrowserWindow`, the filesystem, and quit. No app logic:
  it does not know what a "deck" or a "flip" is.
- **Preload** — a narrow `contextBridge` surface. `contextIsolation: true`,
  `nodeIntegration: false`, `sandbox: true`.
- **Renderer** (`src/`) — the React app. Talks only through `window.flashcards`.

### 4.1 IPC surface

```ts
window.flashcards = {
  packs: {
    list():        Promise<PackMeta[]>       // reads meta.json per pack dir
    read(id):      Promise<Pack>             // reads the current version
  },
  state: {
    get():         Promise<AppState>
    set(patch):    Promise<void>             // shallow merge, then write
  },
  settings: {
    get():         Promise<Settings>
    set(patch):    Promise<void>
  },
  app: {
    quit():        void
  },
}
```

Every method is async except `quit`. All are one-way request/response — no renderer-side
subscriptions in this build.

**Why this shape matters:** when the MCP server lands, it writes into the same `packs/`
directory the main process already reads. `packs.list()` and `packs.read()` do not change,
and the renderer needs no modification at all.

---

## 5. Data model and storage

### 5.1 Types

```ts
type Card = {
  id: string
  front: string          // Front Text        — required
  primary: string        // Back Primary Text — required
  moreInfo?: string      // Markdown          — optional, unvalidated
}

type Pack = { id: string; name: string; schemaVersion: 1; cards: Card[] }
type PackMeta = {
  id: string; name: string; cardCount: number
  currentVersion: number; schemaVersion: 1
}
type AppState = { selectedPackIds: string[]; hasLaunchedBefore: boolean }
type Settings = { themeId: string; themeMode: 'static' | 'dynamic' }
```

### 5.2 On-disk layout

Rooted at Electron's `app.getPath('userData')`:

```
packs/
  <packId>/
    meta.json          { id, name, cardCount, currentVersion, schemaVersion }
    v1.json            { cards: [...] }
    v2.json            later versions
state.json             { selectedPackIds, hasLaunchedBefore }
settings.json          { themeId, themeMode }
```

**A directory per pack, not a file per pack.** This is required by the versioning rule in
vision §8 — a pack accumulates numbered versions, and `meta.json` names the current one so
listing packs never reads card data. `packs.list()` reads only `meta.json` files, which
keeps the Pack Selection screen fast regardless of library size.

**Versioning** (storage support built now, authoring UI later): leaving Make a Pack without
saving writes `v<n+1>.json` and bumps `currentVersion`. A successful save trims all but the
latest three versions. Build one creates `v1.json` at seed time and exercises nothing else.

### 5.3 First run

`seed.ts` runs before the window opens. If `packs/` is absent, it copies the bundled sample
pack in and writes `state.json` with **every pack selected** and `hasLaunchedBefore: true`.

### 5.4 Failure handling

Storage is the only place this app can fail in a way the user would notice.

| Failure | Response |
|---|---|
| `state.json` missing or malformed | Rewrite defaults (all packs selected). Never block launch. |
| A pack directory is malformed | Skip that pack, log it, list the rest. One bad pack never prevents studying. |
| A selected pack id no longer exists | Drop it from the selection on load and persist the cleaned list. |
| Zero packs selected when Learn is pressed | Learn is disabled with a short inline explanation on the Title screen. Do not open an empty session. |
| Write failure | Surface a non-blocking message; keep the in-memory state so work is not lost mid-session. |

---

## 6. Application state machine

### 6.1 Screens and layers

```ts
type Screen = 'title' | 'packs' | 'learn' | 'settings' | 'makepack'
type Layer  = { kind: 'moreInfo' } | { kind: 'confirm'; intent: ConfirmIntent }

type UI = { screen: Screen; layers: Layer[] }   // layers is a stack
```

**Input is dispatched to the top of the layer stack only.** If `layers` is empty, the
screen handles it.

This single rule is the backbone of the interaction spec. It is what makes `Escape` reliably
mean *"back out of exactly one thing"*, and — critically — it is what stops `Space` from
flipping the card underneath while the "Return to Main Menu?" dialog is open. Handling keys
per-component instead would reintroduce that bug on every new overlay.

### 6.2 Confirm intents

```ts
type ConfirmIntent =
  | { type: 'leaveLearn' }      // "Return to Main Menu?"     — always
  | { type: 'leaveSettings' }   // "Save settings before leaving?" — only when dirty
  | { type: 'leaveMakePack' }   // "Are you sure you want to leave?" — only when dirty
```

**Leave policy per screen.** A confirmation is only raised when leaving could lose
something:

```ts
type LeavePolicy =
  | { kind: 'quit' }                            // title
  | { kind: 'immediate' }                       // packs — writes through on every toggle
  | { kind: 'always'; intent: ConfirmIntent }   // learn
  | { kind: 'ifDirty'; intent: ConfirmIntent }  // settings, makepack
```

`ifDirty` screens report their dirty state up to `App`; while a screen is clean, `Escape`
and the leave button return to Title directly. Both such screens are placeholders today
and are never dirty, so both currently leave immediately — the machinery exists so that
adding real content does not require rethinking navigation.

---

## 7. Screen specifications

### 7.1 Title

Five buttons in fixed order: **Learn**, **Pack Selection**, **Make a Pack**, **Settings**,
**Quit**. A top-left leave button that **quits the app**.

`Learn` is disabled when no packs are selected.

> **Carried open question (Q8):** the Title screen has both a `Quit` menu button and a
> top-left quit button. Built as specified; flagged as likely redundant.

### 7.2 Pack Selection

A flat list of packs — **no category layer**. Each row toggles selection. Selection writes
through to `state.json` immediately, so it survives a crash as well as a clean exit.

**Preview:** a row can be expanded to flip through that pack's cards without starting a
session. Preview reuses `Card.tsx` and its flip; it does not advance through a deck, gate
on flips, or touch session state.

Leave button → Title, **with no confirmation**. Every toggle is already on disk, so there
is nothing a confirmation could protect.

### 7.3 Learn

Session state:

```ts
{ deck: Card[]        // shuffled once, on entry
  index: number
  isFlipped: boolean  // current face
  hasFlipped: boolean // has this card EVER been flipped — gates arrow + Tab
}
```

**On entry the deck is reshuffled** (Fisher–Yates) across all selected packs. Every
session gets a fresh order.

**Per card**, `isFlipped` and `hasFlipped` reset on advance.

- **Clicking the card always flips it**, front↔back — *unless* the click lands on the More
  Info control, which opens the panel instead.
- The **advance arrow** fades in once `hasFlipped` is true, and stays visible even if the
  user flips back to the front. It has no text.
- **`moreInfo` is optional.** On a card without it, the More Info control is not rendered
  and `Tab` does nothing. The card back is then Primary Text alone.
- The **leave button** (top-left) and `Escape` both raise "Return to Main Menu?".

**Session end (Q7, proposed):** when the deck is exhausted, a plain end screen offering
*reshuffle* or *return to menu*. No count, no score, no stats — consistent with "no scoring,
ever."

### 7.4 Settings / Make a Pack (placeholders)

Reachable screens with the correct leave behavior wired up and no other content. Settings
confirms save-before-leave; Make a Pack confirms leave and saves regardless.

Make a Pack confirms only when the pack is dirty; leaving still saves regardless.

---

## 8. Input model

The complete matrix. Rows are the active top layer.

| Layer | Key | Action |
|---|---|---|
| **Card** | `Space` | Flip (front↔back) |
| | `Enter` | Advance — **only if `hasFlipped`** |
| | `Tab` | Open More Info — **only if `hasFlipped` and the card has `moreInfo`** |
| | `Escape` | Push confirm: "Return to Main Menu?" |
| **More Info** | `Tab` | Close panel |
| | `Escape` | Close panel — **and nothing else** |
| | `↑` / `W` | Scroll up |
| | `↓` / `S` | Scroll down |
| | `Space` / `Enter` | Ignored (do not flip or advance underneath) |
| **Confirm** | `Escape` | Confirm — leave |
| | `Space` / `Enter` | Deny — stay |

Mouse parity: every action above has a click target. More Info additionally has an **X** in
a top corner.

**Auto-repeat drives More Info scrolling and nothing else.** A held key delivers repeated
keydown events, and every other action in this matrix is dangerous or meaningless when
repeated: held `Escape` would walk confirm → leave → quit in about half a second, held
`Space` would flip the card over and over, held `Enter` would race through the deck, held
`Tab` would toggle the panel open and shut. `keys.ts` exports `repeatable(intent)`, true
only for `scrollMoreInfo`; the listener drops repeats for everything else. `preventDefault`
runs *before* that gate — a suppressed repeat is still a key the app owns, so a held `Tab`
must not walk focus and a held `Space` must not scroll the document.

**Escape is strictly "pop one layer."** With More Info open, the first Escape closes the
panel and a *second* Escape raises the leave confirmation. This falls out of §6.1 for free
rather than needing special-casing.

---

## 9. Rendering and smoothness

The flip is the one thing that never degrades. Five rules, all enforced in build one:

1. **Composited properties only.** The flip animates `transform` and `opacity`. Nothing
   that triggers layout or paint. `will-change: transform` is applied on interaction and
   removed on completion so layers are not held indefinitely.
2. **Flip state is local to `Card`.** It does not live in the app reducer. The screen tree
   does not re-render mid-flip.
3. **Next card is pre-mounted.** `deck[index + 1]` is rendered offscreen so advancing swaps
   an active element rather than mounting and parsing one. Nothing is read from disk on
   advance — the deck is fully in memory from session start.
4. **No main-thread work during transitions.** Pack loading, shuffling, and Markdown
   parsing all happen before the animation starts. More Info Markdown is parsed when the
   card becomes current, not when the panel opens.
5. **`contain: layout paint` on the card**, so its work cannot invalidate the rest of the tree.

**Reset by remount, not by effect.** The card resets to its front face by being keyed on
identity, not by an effect setting `flipped = false` after commit. An effect runs
post-commit, so the new card's text would land while the inner is still rotated 180°, and
the reset would then *animate* back — briefly showing the next card's answer before its
question. A keyed remount resets state and DOM together and never runs a transition.

**Paint containment needs a bleed.** `contain: paint` clips to the padding box, but a card
turning under 1400px perspective projects past its own edges (the near half scales by
~1.28). The stage carries padding plus an equal negative margin, so the clip box is larger
while the layout footprint stays the card's size.

**Background isolation.** `Background.tsx` renders on its own compositor layer with its own
stacking context, entirely separate from the card. A dynamic theme that cannot hold
framerate on weak hardware **drops its own frames without touching the card's** — this is
the mechanism that makes "jittery background, smooth flip" achievable rather than
contradictory. Per vision §7, jitter there is an accepted outcome and the remedy is the
user choosing a static theme. There is deliberately **no hardware detection and no
automatic theme downgrade.**

---

## 10. Theming

Themes are CSS custom property sets plus an optional animated layer.

```ts
type Theme = {
  id: string
  name: string
  tokens: Record<string, string>     // --bg, --fg, --card-face, --accent, ...
  dynamic?: () => JSX.Element        // the animated layer, if any
}
```

Applied by setting tokens on `:root`. Switching a theme changes variables, never structure.

**Default: space.** Minimal and quiet — deep background, restrained accent, high card
contrast. Ships in both forms: **static** (gradient starfield) and **dynamic** (slow
parallax drift), selected by `settings.themeMode`. The dynamic variant animates `transform`
only, in the isolated layer described in §9.

The theme *picker UI* is out of scope; `settings.json` carries the values and defaults to
the space theme.

---

## 11. Testing

Playwright drives the real Electron binary via `_electron.launch()`, with a temp `userData`
directory per test for isolation.

**The interaction spec is the test list.** These assert behavior that is easy to regress:

1. Advance arrow is **absent** before flip, **present** after.
2. `Enter` does nothing before flip; advances after.
3. `Tab` does nothing before flip; opens More Info after.
4. Clicking the card flips it; clicking More Info opens the panel **without** flipping.
5. With More Info open, `Space` neither flips nor advances the card underneath.
6. `Escape` with More Info open closes only the panel; a second `Escape` raises the confirm.
7. `Escape`→`Escape` returns to Title; `Escape`→`Space` and `Escape`→`Enter` stay.
8. Pack selection **survives a real relaunch** against the same `userData`.
9. First launch selects all packs.
10. A malformed pack directory is skipped and the remaining packs still load.
11. Entering Learn twice reshuffles. `shuffle.ts` takes an injectable RNG so the test
    asserts a deterministic order from a fixed seed rather than relying on chance.

Unit tests cover `shuffle.ts` (distribution sanity) and the `machine.ts` reducer
(layer push/pop) — pure functions worth testing directly.

---

## 12. Build and packaging

`electron-builder` produces a `.dmg` (macOS), `.exe`/NSIS (Windows), and `AppImage`
(Linux). Unsigned for now; signing and notarization are a later concern and do not affect
the code.

`npm run dev` runs Vite with HMR on the renderer. `npm run build` produces installers.

---

## 13. Carried open questions

None block this build.

- **Q3 — MCP shape.** Bundled with the app or pointed at by the user's own Claude? What
  tools does it expose? The storage layout is designed to accept either.
- **Q4 — Storage indexing.** Plain files are correct until the library is large enough that
  listing or shuffling is slow. `meta.json` defers the need; revisit with real data.
- **Q7 — Session end.** Proposed above as a plain reshuffle/return screen.
- **Q8 — Two quit affordances** on the Title screen.
- **Q9 — Make a Pack confirmation** that guards nothing.
