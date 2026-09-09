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
| **Make a Pack** | Pack authoring. Opens the pack list; see §3.4. |
| **Settings** | Opens the settings screen. See §3.5. |
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

### 3.4 Make a Pack

The authoring home. Two views:

**Pack list.** Every pack, including ones with nothing saved yet — this is the only screen
that can finish an unfinished pack, so it has to list them. Plus a **New pack** button,
which creates an "Untitled pack" on disk immediately and opens it. A pack whose name has
been changed but not saved is listed under the *draft* name, so it stays findable.

**Editor.** A card list on the left, the selected card's three fields on the right.
Adding, selecting and deleting cards happens in the list; the pack name is edited in
place in the header. Card *order* is deliberately not editable — Learn reshuffles on
every entry, so order only affects the Pack Selection preview.

| Action | Keyboard | Mouse |
|---|---|---|
| Save the pack | `Ctrl`/`Cmd`+`S` | Save button |
| Leave a text field | `Escape` | click elsewhere |
| Leave the editor | `Escape` (outside a field) | leave button |
| Delete a card | — | Delete card |
| Delete the pack | — | "Delete this pack", then type `DELETE` |

**Escape pops exactly one thing here too:** inside a text field it leaves the field;
outside one it leaves the editor for the pack list; from the pack list it returns to
Title.

**Deleting a pack is the only irreversible action in the app** — it takes the pack and
every backup. It is therefore the visually quietest control on the screen, and it
requires typing the word `DELETE`. A yes/no dialog would be one mis-aimed click from
gone.

### 3.5 Settings

**Card direction** — which side of a card you are shown first:

Two independent settings.

**Card direction** — which side is shown first:

| Mode | Session |
|---|---|
| **Front first** | The prompt, flip for the answer. The default. |
| **Back first** | The answer, flip to recall the prompt it belongs to. |
| **Random** | Each card picks its own side, by chance. |

**Back view** — which back a card with several shows:

| Mode | Session |
|---|---|
| **Primary only** | Always the Primary Text, even on cards carrying more. The default. |
| **Random** | One of the card's texts at chance — Primary or any Secondary. |

**Every mode shows each card exactly once**, so a session is always as long as the pack.
An earlier design had a "both, mixed together" direction that put each card in the deck
twice for guaranteed coverage; it was dropped in favour of these two independent axes, and
variety now comes from re-entering Learn rather than from a longer deck.

This is what makes a pack work in both directions: shown a president's name, recall his
number — and shown the number, recall the name. Reversal happens when the deck is built,
so a change takes effect the next time Learn is opened, not mid-session.

**More Info is not reversed.** It belongs to the card, not to a face, and appears on
whichever side is currently the back.

**Direction does not duplicate card data.** A session is a list of *views* — each one a
reference to a card plus which way round it is and which of its facts is in play. Nothing
is copied. An earlier version built reversed decks by cloning whole cards, which meant
duplicating every card's More Info to change which of two strings faced forward; on a pack
carrying images and tables that is megabytes of Markdown cloned for nothing. A view is
three fields and a pointer, so showing a card twice costs two descriptors rather than two
cards. It also means the stored card id is never mangled to keep React keys distinct — the
key is derived from the view.

Settings holds edits locally and writes on **Save**. Leaving with unsaved changes really
does discard them, which is why this screen asks and Pack Selection does not.

---

## 4. Card model

A card has a distinct front and back:

- **Front Text** — the prompt. The name of a person, a state, a process, a term — or a
  picture. Card faces are Markdown, so the question itself can be an image or a small
  table: a flag on the front and the country on the back is a card, not a special case.
- **Back**, which holds up to three things:
  - **Primary Text** — what you see on flipping the card. A definition, a question, or
    whatever description the user sets for themselves.
  - **Secondary Texts** — any number of *further* facts about the same subject: "he issued
    the Emancipation Proclamation", "he delivered the Gettysburg Address", alongside "he
    was the 16th president". Together with Primary they are the pool of backs the card can
    show, so one card teaches several things instead of becoming several cards. All
    optional, never validated, and blank entries are ignored rather than shown.
  - **More Info** — a clickable on the card that opens a **scrollable panel** containing
    any other information the user chose to put on that card. Stored as **Markdown**.
    Today that is a short paragraph of free text. The format is Markdown specifically so
    that **tables, images, and charts** can be added later without changing how packs are
    stored or how the MCP server writes them.

**Faces are Markdown, and sanitized.** Front, Primary and Secondary all render through the
same parser and sanitizer as More Info, because a pack can be written by the MCP server —
that is, by a model — and untrusted content must not reach the DOM unscrubbed. Images on a
face are bounded in both dimensions so that one arriving late cannot shift layout, and the
next card is pre-mounted offscreen so its images are fetched before it is ever shown; §9's
rule that the flip never degrades applies here too.

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
| Title | Quits the app | — (`Escape` quits too, matching both buttons) |
| Learn | Returns to main menu | **Always** asks "Return to Main Menu?" (Escape confirms, Space/Enter denies) |
| Pack Selection | Returns to main menu | **None.** Selection is written to disk on every toggle, so there is nothing to lose. |
| Settings | Returns to main menu | **Only if settings have changed.** Clean settings leave immediately. |
| Make a Pack (list) | Returns to main menu | **None.** Nothing on the list is editable. |
| Make a Pack (editor) | Returns to the pack list | **Only if the pack has changed.** An untouched pack leaves immediately. Changes are kept as a draft either way. |

**The rule: never ask a question that has only one sensible answer.** A confirmation on a
screen with nothing to lose is friction, not safety. Pack Selection has nothing to lose by
construction. Settings and Make a Pack have something to lose only once the user has
actually changed something, so they track a dirty state and ask only then.

Learn is the exception and asks unconditionally — leaving a study session mid-deck is
losing your place, and the Escape-Escape gesture is deliberate.

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
count stays small. An unsaved exit that changed nothing writes nothing, so bouncing in and
out of an untouched pack cannot pile up identical versions.

**A draft is private to the editor.** `meta.json` carries two pointers: `currentVersion`,
the last *saved* version, and `draftVersion`, the newest unsaved exit. Learn and Pack
Selection follow `currentVersion` only — they never see unvalidated work. Make a Pack
resumes from whichever is newer. Saving discards the draft.

This is what keeps validation meaningful: without the split, walking away from a
half-typed card would make that card live, and Learn would show a card with a blank back.

**A pack with no saved cards is not offered for study.** It is listed in Make a Pack (so
it can be finished) and hidden from Pack Selection, and any selection naming it is
dropped. `cardCount` is the test, and it counts saved cards only.

**On save:** the pack is validated. Validation is deliberately minimal — **every card must
have a Front Text and a Back Primary Text.** More Info is not validated at all.

- If validation **fails**, the pack does not save. The user is told how many cards are
  incomplete, the offending cards are marked in the list, and the editor jumps to the
  first one. Nothing is written to disk.
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

- **Accounts** — wanted eventually, local-only for now.
- **Study modes** — "ADHD mode" and other optional interactable side panels, configured
  from Settings.
- **Theming UI** — the space default ships; the Settings interface for choosing between
  themes, and for the static/dynamic distinction, comes later.
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
| — | Leave confirmations | Only when something can be lost. See §5. |
| — | Flip duration | 525ms — 80% of the original speed. |
| — | Escape on Title | Quits the app outright. |
| — | Key auto-repeat | Honoured only for More Info scrolling. |
| — | More Info format | **Markdown**, to allow tables/images/charts later. |
| — | Visual default | Minimal and customizable; **space theme** by default. |
| — | Theme kinds | Static or dynamic, distinguished in Settings. |
| — | Validation | Front Text + Back Primary Text required; Secondary Texts and More Info unvalidated. |
| — | Card direction | Front-first, back-first, or both mixed. The first real setting. See §3.5. |
| — | "Mixed" semantics | Every card appears **twice**, once each way — coverage is guaranteed, not left to the shuffle. |
| — | Reversed More Info | Not swapped. It is the card's depth, and stays on whichever side is the back. |
| — | Secondary Texts | A list of further facts joining Primary as possible backs. See §4. |
| — | Settings axes | Direction and Back View are independent settings, not one combined mode. |
| — | Deck representation | Views over cards, never copies. See §3.5. |
| — | Markdown on faces | Front and back render Markdown, sanitized, so a card's question can be a picture or a table. |
| — | Session length | Always one view per card, in every mode. |
| — | Legacy data | A single `secondary` folds into the list on read; a saved `mixed` direction maps to `random`. No migration pass. |
| — | Settings saving | Explicit **Save**, not write-through. Leaving with unsaved changes asks; a clean screen leaves silently. |
| — | Versioning | One version per **unsaved exit**; on save, trim to last three. |
| Q10 | Version granularity | Per unsaved exit from Make a Pack, and only when something changed. |
| — | Make a Pack scope | Creates **and edits** packs, including the ones that shipped. It is the authoring home, not a one-shot creator. |
| — | Editor layout | Card list plus detail pane. Scales to a 197-card pack without a huge DOM. |
| — | Draft visibility | Drafts are private to the editor; only a save changes what Learn studies. |
| — | Empty packs | Hidden from Pack Selection until they have saved cards. |
| — | Card order | Not editable. The deck reshuffles on every entry, so order buys nothing. |
| — | Deleting a pack | Requires typing `DELETE`. The control is deliberately the quietest on the screen. |
| — | Editor keys | `Ctrl`/`Cmd`+`S` saves; `Escape` leaves the field before it leaves the screen. |
| — | Dynamic theme jitter | Acceptable on weak hardware; static theme is the remedy. The flip never degrades. |

---

## 13. Scope of the first build

Get the core loop running. In scope:

- Title screen with all five buttons present
- Pack Selection (multi-select, persistence, card preview)
- Learn (flip, advance, More Info, all keybindings, leave confirmation)
- A small hand-written sample pack, enough to exercise the loop

Stubbed or omitted: **Settings** is a button that leads to a placeholder screen.
**The countries pack conversion** is parked in `_legacy/` and adapted later.
**MCP server**, **theming UI**, **study modes**, and **accounts** come later — but the
pack file format is designed now so MCP can write to it without a migration.

**Make a Pack shipped after the first build** and is described in §3.4. It was built on
the storage layer this section designed, without a migration.

---

## 14. Open questions

- **Q3 — MCP shape.** *Resolved and built.* A **standalone stdio server** (`mcp/`), not
  bundled into the app and not launched by it — the user registers it with their own Claude
  client. See `docs/mcp.md`.

  It imports `electron/storage.ts`, which has no Electron dependency, so **`savePack` is the
  single gate for hand-typed and AI-written packs alike** — a model cannot write a pack the
  app would reject. The IPC handlers are pass-throughs with no logic of their own, so there
  is no second implementation to drift.

  Seven tools: `list_packs` (summaries), `read_pack`, `create_pack`, `add_cards`,
  `update_card`, `replace_all_cards`, `delete_pack`. The three write tools are named for what
  they destroy, not what they save — a model reaching for "save" when it means "append" would
  otherwise empty the pack. `add_cards` returns the ids it created so a follow-up
  `update_card` costs no extra read. Two guards, both mirroring decisions already made for
  the UI:

  - **Draft protection.** A save clears `draftVersion`, so a write to a pack open and unsaved
    in Make a Pack would destroy that work. Such writes are refused unless `force: true`.
    This is the answer to the editor-race question this section used to leave open.
  - **`delete_pack` requires `confirm: "DELETE"`**, matching the editor's type-to-confirm gate.

  Discovery is a pointer file (`~/.flashcards/library.json`) the app writes on launch. The
  server never guesses the library location: `userData` is derived from the app name, which
  differs between dev and packaged builds, and a wrong guess would silently create a second
  empty library.

  Still open: whether to also offer an HTTP transport, and whether the app should ever host
  the server itself.

- **Q4 — Storage.** "A lot of data" plus a `packs/` folder that MCP writes into suggests
  files on disk, but a growing library may want an index or a database for fast search and
  shuffling. Plain files, or files plus an index?
- **Q7 — Session end.** With no scoring, what does exhausting the pool look like? A plain
  end screen offering reshuffle or return to menu, or a silent reshuffle that never ends?
- **Q8 — Two quit affordances.** The title screen has both a **Quit** button in the menu
  and a **top-left quit button**. Intentional redundancy, or should one go?
- **Q11 — Pack folder shape.** *Resolved:* a **directory per pack**.
  `packs/<id>/meta.json` holds the name, card count and the two version pointers;
  `packs/<id>/v<n>.json` holds `{ name, cards }`. The name lives in the version file as
  well as the meta so an unsaved rename can sit in a draft without changing the live pack.
- **Q12 — Settings' confirmation wording.** *Resolved.* Settings has real content now, and
  the dialog reads **"Leave without saving? Your changes to Settings will be lost."** —
  a two-answer question with a two-answer prompt. Unlike Make a Pack there is no draft, so
  the note says the changes are lost rather than kept.
- **Q9 — Make a Pack confirmation.** *Resolved:* it asks only when the pack has actually
  changed, and only when leaving the **editor** — the pack list has nothing to lose. The
  wording says what actually happens: "Leave without saving? Your changes are kept as a
  draft and will be here when you come back. The pack itself will not change until you
  save."

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

---

## 16. Known gaps at the end of the first build

Found by the final review. None blocks the app running; all are recorded so they are not
rediscovered later.

- **A failed disk write is silent.** Spec §5.4 calls for a non-blocking message when a
  write fails. In-memory state is kept (so nothing is lost mid-session), but the rejection
  is unhandled and the user is told nothing.
- **Seeding is gated on `hasLaunchedBefore`, not on `packs/` being absent.** If
  `state.json` is corrupted, the seed re-runs and appends an identical second version of
  each bundled pack. Harmless, but it is not what §5.3 describes.
- **No end-to-end test that re-entering Learn reshuffles.** The shuffle itself is unit
  tested with a fixed RNG; the "fresh order on every entry" promise is not covered by a
  Playwright test.
- **Packaging polish.** `react`, `marked` and `dompurify` are bundled by Vite *and*
  shipped again inside the asar — they belong in `devDependencies`. `package.json` has no
  `description`, `author`, or icon, so the build carries the default Electron icon.

- **Card faces do not scroll.** A face is a fixed box with `overflow: clip`, so Markdown
  content taller than the card is silently cut off. Images and table images are bounded to
  fit (see `card.css`), but a long table on a face will still clip. More Info, which does
  scroll, is the place for anything substantial.

- **Remote images need a network.** Every picture in the painters, presidents and flags
  packs is a Wikimedia URL, so image-bearing faces and More Info are the one part of an
  otherwise offline app that needs connectivity. Embedding them as data URIs would fix it
  and make the packs very large.

- **Two MCP writers can lose each other's work.** `add_cards` and `update_card` are
  read-modify-write over a whole pack, so two concurrent clients editing the same pack can
  clobber one another. The editor is protected by the draft guard; MCP-versus-MCP is not.
  Every bulk write in this repo was serialised by hand to avoid it.

- **290 of 840 painting rows have no image**, listed in `docs/painters-missing-images.md`.
  These are copyright gaps, not lookup failures, and need a licensed source rather than a
  better search.
- **Windows and Linux installers are built but untested.** They were cross-built from
  macOS and only checked for the presence of the bundled pack.
- ~~**The presidents pack carries test scaffolding.**~~ *Resolved.* All 45 cards were
  rewritten with a portrait, a biography, dated milestones, a Secondary Text and sources.
