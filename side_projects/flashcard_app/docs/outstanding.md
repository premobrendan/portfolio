# Outstanding work

Written 2026-09-09, at the end of the session that added Make a Pack, the MCP server, the
study-direction and Secondary Text settings, and Markdown card faces. This is the handoff
list: what is known to be unfinished, and what is a trap rather than a task.

Nothing here blocks the app running. All 135 unit tests and 64 end-to-end tests pass.

---

## 1. Setup the next session will need

**Register the MCP server.** It is built but not connected to any client:

```bash
npm run build:mcp
claude mcp add flashcards -- node /absolute/path/to/flashcard_app/dist-mcp/server.mjs
```

The library pointer at `~/.flashcards/library.json` is correct. The app rewrites it on
every launch of its real profile, and test runs no longer clobber it (see §4).

**There is no other way to drive the server.** The helper script used during development
lived in a session scratchpad and is gone. Registering it properly is the durable path.

---

## 2. Content

| Pack | State |
|---|---|
| Painters | 56 cards, audited and corrected |
| Presidents | 45 cards, rewritten and corrected |
| Machine Learning | 30 cards, audited and corrected |
| Flags of the World | **3 demo cards** — proof-of-concept only |

- **The Flags pack is scaffolding.** It exists to prove that images and tables render on
  card faces. One card carries a deliberately awkward table. Delete it or build it out.
- **290 of 840 painting rows have no image**, listed per painter in
  `painters-missing-images.md`. These are copyright gaps, not lookup failures — the works
  are in copyright and no freely-licensed reproduction exists on Wikimedia. Filling them
  needs a licensed source, not a better search.
- **British spellings remain** in the painters and ML packs (`regularisation`,
  `behaviour`, `colour` in prose). The presidents pack was normalized; the other two were
  not. See the trap in §5 before attempting a sweep.
- **The audits are not spent.** `audits/README.md` lists what was deliberately not applied:
  both painters "Questionable" sections, the wider prose lists, and the presidents' deck-wide
  template repetition (six bio formats and four bullet date styles, in nine-card blocks —
  one per authoring agent).

---

## 3. Product decisions left open

- **Card faces do not scroll.** A face is a fixed box with `overflow: clip`, so Markdown
  taller than the card is silently cut off. Images and table images are bounded to fit, but
  a long table on a face will still clip. Making faces scrollable would fight click-to-flip,
  so it needs a design decision rather than a patch.
- **Remote images need a network.** Every picture in every pack is a Wikimedia URL. This is
  the one part of an otherwise offline-first app that needs connectivity. Data URIs would
  fix it and make the packs enormous.
- **`fill-more-info.mjs` is dead.** It stripped the presidents test scaffolding, which no
  longer exists. Delete it.
- **Settings has one screen and two settings.** Theme selection, the static/dynamic
  distinction, and study modes are all still unbuilt (vision §10).

---

## 4. Known technical gaps

- **Two MCP writers can lose each other's work.** `add_cards` and `update_card` are
  read-modify-write over a whole pack, so two concurrent clients editing the same pack can
  clobber one another. The app editor is protected by the draft guard; MCP-versus-MCP is
  not. Every bulk write so far was serialised by hand. If the server ever has more than one
  concurrent caller, this needs a lock or a compare-and-swap on `currentVersion`.
- **A failed disk write is still silent** (vision §16). In-memory state survives, but the
  rejection is unhandled and the user is told nothing.
- **Packaging polish** — `react`, `marked` and `dompurify` ship twice (bundled by Vite and
  again inside the asar); they belong in `devDependencies`. No `description`, `author` or
  icon, so the build carries the default Electron icon.
- **Windows and Linux installers are cross-built and untested.**

---

## 5. Traps — read before "fixing" these

**Wikipedia's Jacob Lawrence article has the Tubman and Douglass series years swapped.**
The card originally reproduced that error and has been corrected from the *Frederick
Douglass* article and the Hampton University Museum catalogue. Re-checking it against the
Lawrence article will look like a regression. It is not one.

**`colour` appears inside Commons image filenames** in the painters pack. A British→US
spelling sweep must operate only on prose *outside* markdown link and image targets, or it
will silently break those images. The presidents normalization does this correctly and is
the pattern to copy.

**Do not write to a pack the editor has open.** `savePack` clears `draftVersion`, which
would discard unsaved work in Make a Pack. The MCP tools refuse such writes unless
`force: true`; that override exists for the caller to use deliberately, not routinely.

**Never invent an image URL.** Wikimedia paths contain an unguessable hash. Every image in
every pack was resolved from the `pageimages` API. Note `pilicense=any` — the default
returns only free-licensed images and comes back empty for most 20th-century subjects.
