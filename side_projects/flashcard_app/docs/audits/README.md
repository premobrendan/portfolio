# Content audits

Four read-only audits of the three content packs, run 2026-09-09 by separate agents that
had no hand in writing the content they reviewed.

| File | Covers | Outcome |
|---|---|---|
| `presidents.md` | Presidents, 45 cards | 23 corrections applied |
| `ml.md` | Machine Learning, 30 cards | 26 corrections applied |
| `painters-1.md` | Painters, cards 1–28 | applied with `painters-2.md` |
| `painters-2.md` | Painters, cards 29–56 | 19 cards corrected across both halves |

**These are kept because they are not fully spent.** The corrections applied were the
factual errors and the specific prose defects named in each report. Deliberately NOT
applied, and still open if you want them:

- Both painters reports' **"Questionable / needs a second look"** sections — judgement
  calls rather than errors (Salvator Mundi and Goya *Colossus* qualifiers, several
  Titian/Rubens/Gentileschi date ranges, the "Painting" → "Work" column rename).
- The **wider prose lists** — the "Active from…" template repetition, and per-card wording
  notes on roughly fifteen painters. Applying all of them would have been a wholesale
  rewrite, which the correction brief forbade.
- The presidents' **deck-wide template observations**: six different bio opening formats
  and four bullet date styles, applied in nine-card blocks (one per authoring agent), plus
  "is remembered for" closing 23 of 45 bios. Cosmetic, but it is why a run of cards can
  feel machine-assembled.
- Presidents **bullet ordering** on Tyler, Benjamin Harrison and Coolidge, where one entry
  sits out of chronological order.

Two findings worth remembering because they are traps rather than tasks:

- **Wikipedia's Jacob Lawrence article has the Tubman and Douglass series years swapped.**
  The card originally reproduced that error. It is now corrected from the *Frederick
  Douglass* article and the Hampton University Museum catalogue — so re-checking it against
  the Lawrence article will look like a regression and is not one.
- **`colour` appears inside several Commons image filenames** in the painters pack. Any
  British→US spelling sweep must operate on prose outside markdown link targets, or it will
  silently break those images.
