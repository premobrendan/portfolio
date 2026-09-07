# Flashcard App — Vision

**Status:** living document. Edit freely; this is the source of truth for intent, not a frozen spec.
**Last updated:** 2026-09-07

---

## 1. What this is

A desktop flashcard application. Not a website you visit — an app you double-click to
open. The user launches it, picks what they want to study, and studies. Everything runs
locally; selections persist between sessions.

**This is a rebuild, not a refactor.** The code currently sitting in this folder is not
the foundation. It is not being extended, migrated, or incrementally reshaped — it is being
replaced. The only thing worth carrying forward is the *content*: the 197 hand-assembled
country cards. Everything else starts from a blank slate, chosen to fit the vision below
rather than inherited from what happens to be there.

---

## 2. Distribution and platform requirements

The app ships as an **executable**. One click launches it — no terminal, no `npm run dev`,
no browser address bar.

**The stack is Electron**, chosen for cross-platform consistency: it ships its own
Chromium, so the flip and every transition can be tuned once and behave identically on
macOS, Windows, and Linux. Node being built in also makes the MCP server and eventual
accounts substantially cheaper to add later. See §15 for why the shell matters less than
the frontend here.

Requirements the build must satisfy:

- **Smoothness is the top priority.** Specifically **interaction** smoothness, not launch
  time or binary size: the card flip and every menu-to-menu navigation must feel seamless.
  No jank, no stutter, no visible transition seams. This is the criterion the stack choice
  is judged against.
- **Scales with data.** The user keeps adding packs and cards over time. The storage and
  rendering approach must not degrade as the library grows.
- **Hosts an MCP server.** A user must be able to add packs to their local `packs/` folder
  easily — via MCP, not by hand-editing JSON. This makes the on-disk pack format a public
  interface, not an implementation detail.
- **Themeable.** Backgrounds and visual treatment are customizable by both the developer
  and the end user.
- **Room for modes.** Future study modes and optional side panels (e.g. an "ADHD mode"
  with interactable elements) hang off Settings. Not built now, but the architecture
  should not preclude them.
- **Sleek.** The through-line for every one of the above.

---

## 3. Screens

### 3.1 Title screen

The entry point. Five buttons, in order:

| Button | Behavior |
|---|---|
| **Learn** | Opens the study session using whatever packs are currently selected. |
| **Pack Selection** | Opens the pack selection screen. |
| **Make a Pack** | Pack authoring. Flow not yet specified; edits save on leave. |
| **Settings** | Opens a settings screen. **Intentionally empty for now** — the screen exists as a placeholder so the slot is reserved. |
| **Quit** | Closes the application. |

The **top-left button on the title screen quits the app.**

### 3.2 Pack Selection

A flat list of packs. **There is no category layer** — packs are not grouped under
subjects. Each pack has a name the user chooses when creating it. A set of default packs
ships with the app; everything beyond that is user-named and user-defined.

**Selection rules:**

- On **first ever open**, every pack is selected by default.
- Selection state **persists across app launches**. What the user picked last time is what
  they get next time.
- "Learn" studies the union of all currently selected packs.

**Preview:** from within Pack Selection, the user can **flip through a pack's cards** to
see what is inside it, without starting a study session.

### 3.3 Learn — the main screen

This is the core of the app. It is a **linear flashcard experience** — there are no
quizzes, no typed answers, and no scoring.

**Card mechanics:**

1. A card is shown front-side up.
2. **Clicking the card always flips it** — front → back and back → front — *unless* the
   click landed on **More Info**, which opens the More Info panel instead.
3. Only **after the card has been flipped** does the option to advance appear.
4. That option is an **arrow in the bottom-right corner**. It **fades in**. It contains
   **no text** — no "Next Card" label, just the arrow.

**Keyboard:**

| Key | Action |
|---|---|
| `Space` | Flip the card |
| `Enter` | Go to the next card |
| `Tab` | Open More Info (only once the card has been flipped) |
| `↑` / `W` | Scroll More Info up |
| `↓` / `S` | Scroll More Info down |
| `Escape` | Request return to the main menu (see below) |

**Card order:** when more than one pack is selected, **all cards from all selected packs
are shuffled together** into a single pool and drawn in any order. **The deck reshuffles
every time Learn is opened** — a fresh order on every entry.

**Returning to the main menu:**

Escape does not leave immediately. It raises a confirmation: **"Return to Main Menu?"**

- Pressing `Escape` a **second time confirms** — return to the title screen.
- Pressing `Space` **or** `Enter` **denies** — stay in the session.
- The confirmation also has **clickable buttons** for the same two choices.

**On-screen controls:**

Every keyboard action has a clickable equivalent. Additionally there is a **small button in
the top-left**: a standard "leave" symbol, **no text label**. On this screen it **returns
to the main menu**, using the same confirmation as Escape.

---

## 4. Card model

A card has a distinct front and back:

- **Front Text** — the prompt. The name of a person, a state, a process, a term.
- **Back**, which holds two things:
  - **Primary Text** — what you see on flipping the card. A definition, a question, or
    whatever description the user sets for themselves.
  - **More Info** — a clickable on the card that opens a **scrollable panel** containing
    any other information the user chose to put on that card. Stored as **Markdown**.
    Today that is a short paragraph of free text. The format is Markdown specifically so
    that **tables, images, and charts** can be added later without changing how packs are
    stored or how the MCP server writes them.

**Front Text and Primary Text are the required spine of a card** — a card is only valid if
both are present. More Info is optional, fluid, and unvalidated; it is where the depth
lives.

**Opening More Info:** `Tab` opens it, but **only after the card has been flipped at least
once**. On an unflipped card, Tab does nothing.

**Scrolling More Info:** `↑`/`↓` arrow keys, or `W` (up) and `S` (down).

**Closing More Info:** an **X** in a top corner of the panel, or `Tab`, or `Escape`. While
the panel is open, `Escape` closes it and does *nothing else* — only a second `Escape`,
with the panel closed, raises "Return to Main Menu?".

---

## 5. Leaving screens

Every screen has a top-left leave button. What it does depends on the screen:

| Screen | Leave button does | Confirmation |
|---|---|---|
| Title | Quits the app | — |
| Learn | Returns to main menu | "Return to Main Menu?" (Escape confirms, Space/Enter denies) |
| Pack Selection | Returns to main menu | Confirmation |
| Settings | Returns to main menu | Asks whether to **save settings** before leaving |
| Make a Pack | Returns to main menu | Asks **"are you sure you want to leave?"** — but all changes to the pack are **saved on leave regardless** |

---

## 6. Persistence

**Anything the user makes is saved.** Packs they author, cards they write, and every
setting they have saved all survive a restart. Pack selection state persists as described
in §3.2.

---

## 7. Visual identity and theming

**Minimal, but customizable.** The interface stays quiet — the card is the subject and the
chrome gets out of its way.

**The default theme is a space theme.** It ships as the app's identity, not as a neutral
placeholder.

**Themes are either static or dynamic**, and Settings distinguishes between the two:

- **Static** — a fixed background: a colour, gradient, or still image.
- **Dynamic** — a background with motion.

> **Constraint (see §15):** the **card flip must be smooth, always.** That is
> non-negotiable and does not degrade.
>
> A dynamic background, by contrast, **is allowed to be jittery on weaker hardware.** That
> is an acceptable outcome, not a bug — the user's remedy is to switch to a static theme.
>
> The architectural consequence: **the background and the card must be independent
> compositor layers**, so that dropped background frames cannot steal frames from an
> in-flight flip. The background degrades alone. It must never be able to drag the flip
> down with it, and there is no need to auto-detect hardware or downgrade the theme
> automatically — the static/dynamic choice in Settings is the escape hatch.

---

## 8. Pack saving and versioning

Applies to authored packs (Make a Pack). The format is settled now so the storage layer is
built for it from the start, even though the authoring UI comes later.

**An "iteration" is one unsaved exit.** When the user leaves Make a Pack *without saving*,
the current state is written to disk as a new numbered version. Not per keystroke, not per
card — per abandoned editing session. Work is never lost to walking away, and the version
count stays small. The app always displays the **most recent version**.

**On save:** the pack is validated. Validation is deliberately minimal — **every card must
have a Front Text and a Back Primary Text.** More Info is not validated at all.

- If validation **fails**, the pack does not save and the user is told what is missing.
- If validation **passes**, the pack saves and **all but the last three versions are
  deleted** — the saved pack plus two backups.

**Backups are optional.** The user can delete the two retained backups to reclaim space.

---

## 9. Design principles

- **The flip is sacred.** Card flip smoothness is the one thing that never degrades for
  any reason. Everything else yields to it. Animations are composited, transitions are never blocked on work
  that could have happened earlier.
- **Keyboard-first, mouse-complete.** Every action is reachable both ways. Neither input
  method is a second-class citizen.
- **No redundant text.** The advance arrow is an arrow. The leave button is a symbol.
  Where an icon is unambiguous, it stands alone.
- **Deliberate advancement.** You cannot skip past a card you have not seen the back of.
  The gate on the arrow is a feature, not a limitation.
- **No scoring, ever.** This is a learning app, not a testing app. Nothing is graded,
  counted, ranked, or streaked.
- **The user's data is theirs.** They name their packs, write their cards, and decide what
  goes in More Info. The app does not impose a taxonomy.
- **Local now, accounts later.** Accounts are wanted eventually. Until then everything is
  local and offline — but the data model should not make accounts painful to add.
- **Built for this vision.** Structure follows from the screens described here, not from
  what the previous version happened to do.

---

## 10. Explicitly deferred

Recorded so they are not silently dropped, and not to be built yet:

- **Settings contents** — screen exists, is empty on purpose.
- **"Make a Pack"** — the button and its leave behavior are specified; the authoring flow
  is not. **Explicitly out of scope for the first build** — get the core loop running
  first.
- **Accounts** — wanted eventually, local-only for now.
- **Study modes** — "ADHD mode" and other optional interactable side panels, configured
  from Settings.
- **Theming UI** — the space default ships; the Settings interface for choosing between
  themes, and for the static/dynamic distinction, comes later.
- **Rich More Info** — tables, images, and charts inside More Info. The Markdown storage
  format accommodates them now; the rendering and authoring of them comes later.
- **Quizzes** — explicitly not wanted at this time. Not parked as "coming later"; cut.

---

## 11. What survives from the old folder

Nothing structural. The existing `src/` is a husk — a different app that happens to share a
subject. It is not a starting point, and no part of this vision should be shaped by what is
convenient to keep.

**Carried forward: the data, and only the data.**

The old pack data has been **moved to `_legacy/info_packs/`**, out of the way of the
rebuild. It is parked, not deleted, and gets converted later.

`_legacy/info_packs/countries_of_the_world/pack_config.json` holds 197 country cards, each
with `name`, `capital`, `flag`, `divisions[]`, and a `facts{}` object. That content took
real effort to assemble and is worth keeping. But it is **input to the new schema, not the
schema itself** — the country data gets converted into Front Text / Primary Text / More
Info later, once the core loop works. **It is not part of the first build.**

**Not carried forward:** the component structure, the `view`-string navigation, the
`App.css` file, the pack-loading approach, the typed-answer quiz, the card browser, the
random-card screen.

**Practical consequence:** the rebuild starts in a clean directory. The old tree stays
untouched until the new app works, then it goes.

---

## 12. Decisions made

| # | Question | Decision |
|---|---|---|
| D1 | Packaging | **Requirements set, stack not chosen.** See §2 and Q1 below. |
| D2 | Leave button | Resolved per-screen. See §5. |
| D3 | Persistence scope | Everything the user makes, plus saved settings. See §6. |
| D4 | Old modes | Quizzes and the card browser are **cut**. Linear flashcards only, plus a card preview inside Pack Selection. |
| D5 | Pack granularity | **Flat, user-named packs.** No category layer. Defaults ship with the app. |
| D6 | Card front/back | Front Text; Back = Primary Text + More Info. See §4. |
| D7 | Card order | All selected packs shuffled into one pool. |
| Q2 | Account | Wanted eventually; **local-only for now**. |
| Q6 | More Info keys | `Tab` opens (post-flip only); `↑`/`↓` or `W`/`S` scroll; X, `Tab`, or `Esc` closes. |
| Q1 | Stack | **Electron**, targeting macOS + Windows + Linux. |
| Q5 | Card back click | Click always flips, unless it lands on More Info. |
| — | Scoring | **None.** Cut entirely. |
| — | Shuffle timing | Deck reshuffles on every entry into Learn. |
| — | More Info format | **Markdown**, to allow tables/images/charts later. |
| — | Visual default | Minimal and customizable; **space theme** by default. |
| — | Theme kinds | Static or dynamic, distinguished in Settings. |
| — | Validation | Front Text + Back Primary Text required; More Info unvalidated. |
| — | Versioning | One version per **unsaved exit**; on save, trim to last three. |
| Q10 | Version granularity | Per unsaved exit from Make a Pack. |
| — | Dynamic theme jitter | Acceptable on weak hardware; static theme is the remedy. The flip never degrades. |

---

## 13. Scope of the first build

Get the core loop running. In scope:

- Title screen with all five buttons present
- Pack Selection (multi-select, persistence, card preview)
- Learn (flip, advance, More Info, all keybindings, leave confirmation)
- A small hand-written sample pack, enough to exercise the loop

Stubbed or omitted: **Make a Pack** and **Settings** are buttons that lead to placeholder
screens. **The countries pack conversion** is parked in `_legacy/` and adapted later. **MCP server**, **theming UI**, **study modes**, and **accounts** come later — but
the pack file format is designed now so MCP can write to it without a migration.

---

## 14. Open questions

- **Q3 — MCP shape.** Is the MCP server *bundled and run by the app*, or a separate thing
  the user points their own Claude at? What does it expose — create pack, add card, import
  bulk data, read existing packs?
- **Q4 — Storage.** "A lot of data" plus a `packs/` folder that MCP writes into suggests
  files on disk, but a growing library may want an index or a database for fast search and
  shuffling. Plain files, or files plus an index?
- **Q7 — Session end.** With no scoring, what does exhausting the pool look like? A plain
  end screen offering reshuffle or return to menu, or a silent reshuffle that never ends?
- **Q8 — Two quit affordances.** The title screen has both a **Quit** button in the menu
  and a **top-left quit button**. Intentional redundancy, or should one go?
- **Q11 — Pack folder shape.** Versioning means a pack is no longer one file. Proposed:
  a **directory per pack** (`packs/<id>/v3.json` + a `current` pointer) rather than
  `packs/<id>.json`. This changes what the MCP server writes into, so it should be settled
  before the storage layer is built.
- **Q9 — Make a Pack confirmation.** Leaving asks "are you sure?" but saves everything
  regardless. If nothing is lost either way, what is the confirmation protecting against?

---

## 15. Smoothness: where it actually comes from

Recorded because the stack choice is easy to overrate. Interaction smoothness is dominated
by how the frontend is built, not by which shell wraps it. Both Electron and Tauri render
through a webview and both can hold 60fps for an app this size.

**What actually determines whether the flip feels seamless:**

1. **Composited animations only.** The flip animates `transform` and `opacity` and nothing
   else. Any animation that touches layout or paint properties will stutter regardless of
   stack.
2. **Render discipline.** The card subtree must not re-render mid-animation. State that
   changes on flip is isolated from state that changes on advance.
3. **Preloading the next card.** The advance should have nothing to fetch or parse. The
   next card is resolved before the user asks for it.
4. **No main-thread work during transitions.** Pack loading, shuffling, and any indexing
   happen off the critical path, not while a screen is animating in.
5. **Fonts and images resolved up front.** No layout shift after a screen appears.

**Where the stack does matter:** Electron ships one Chromium build, so animation behavior
is identical on every platform — tune once. Tauri uses the OS webview, which is excellent
on macOS (WKWebView) and Windows (WebView2, Chromium-based) but weakest on Linux
(WebKitGTK). The more platforms in scope, the more Electron's single-engine consistency is
worth, and the less Tauri's smaller footprint buys — since footprint is not the criterion.

**Layer independence.** The themed background renders on its own compositor layer, separate
from the card. A dynamic background that cannot hold framerate on weak hardware drops its
own frames without touching the card's. This is what makes "jittery background, smooth
flip" an achievable combination rather than a contradiction.
