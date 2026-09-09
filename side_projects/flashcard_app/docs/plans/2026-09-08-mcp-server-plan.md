# Flashcards MCP Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A standalone stdio MCP server that lets a model create and edit packs in the user's local flashcard library, through the same storage and validation code the app itself uses.

**Architecture:** The server is a separate Node process the user registers with their own Claude client — not bundled into or spawned by the Electron app (vision §14 Q3). It imports `electron/storage.ts` directly, which has no Electron dependency, so `savePack` remains the single validation gate for hand-typed and AI-written packs alike. Handlers live in `mcp/tools.ts` as plain async functions so they are unit-testable without a transport; `mcp/server.ts` is thin wiring.

**Tech Stack:** TypeScript, `@modelcontextprotocol/sdk` 1.30.0, `zod` 4.x for input schemas, esbuild for the single-file bundle, vitest for tests.

**Spec:** `docs/vision.md` — §2 (packs/ is a public interface), §8 (versioning, validation), §14 Q3 (MCP shape, and the settled part of it).

## Global Constraints

- **`savePack` is the only door in.** No tool may write a version file directly. AI-written packs get the same Front+Primary validation as hand-typed ones.
- **The MCP must never silently destroy a draft.** `savePack` clears `draftVersion`. Any write to a pack with an open draft is refused unless the caller passes `force: true`.
- **Pack ids are untrusted input.** `packDir()` already rejects traversal; do not bypass it.
- **No guessing the library location.** If the root cannot be resolved, fail with an actionable message rather than silently creating a second, empty library.
- **Tool results land in conversation context.** `list_packs` returns summaries only; full card data requires an explicit `read_pack`.
- **Card ids are generated server-side.** Callers supply `front`/`primary`/`moreInfo` only.
- Node ≥ 20 (`crypto.randomUUID`, `node:fs/promises`).

---

### Task 1: Library root resolution

The MCP process has no Electron, so `app.getPath('userData')` is unavailable. The app writes a pointer file on every launch; the server reads it.

**Files:**
- Create: `electron/library.ts`
- Create: `electron/library.test.ts`
- Modify: `electron/main.ts` (call `writeLibraryPointer` after `setStorageRoot`)

**Interfaces:**
- Consumes: nothing.
- Produces: `libraryPointerPath(): string`, `writeLibraryPointer(root: string): Promise<void>`, `resolveLibraryRoot(argv?: string[], env?: NodeJS.ProcessEnv): Promise<string>` — throws `Error` with an actionable message when unresolvable.

- [ ] **Step 1: Write the failing tests**

```typescript
// electron/library.test.ts
import { describe, it, expect, beforeEach } from 'vitest'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { libraryPointerPath, writeLibraryPointer, resolveLibraryRoot } from './library'

let home: string
beforeEach(() => {
  home = fs.mkdtempSync(path.join(os.tmpdir(), 'fc-home-'))
})

const env = (over: Record<string, string> = {}) => ({ HOME: home, ...over })

describe('resolveLibraryRoot', () => {
  it('prefers the explicit env var', async () => {
    expect(await resolveLibraryRoot([], env({ FLASHCARDS_USER_DATA: '/tmp/explicit' })))
      .toBe('/tmp/explicit')
  })

  it('accepts --root from the command line', async () => {
    expect(await resolveLibraryRoot(['--root', '/tmp/flag'], env())).toBe('/tmp/flag')
  })

  it('falls back to the pointer the app wrote', async () => {
    await writeLibraryPointer('/tmp/from-app', env())
    expect(await resolveLibraryRoot([], env())).toBe('/tmp/from-app')
  })

  it('refuses to guess when nothing points anywhere', async () => {
    await expect(resolveLibraryRoot([], env())).rejects.toThrow(/launch the Flashcards app/i)
  })
})

describe('libraryPointerPath', () => {
  it('lives at a fixed location independent of the app name', () => {
    expect(libraryPointerPath(env())).toBe(path.join(home, '.flashcards', 'library.json'))
  })
})
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx vitest run electron/library.test.ts`
Expected: FAIL — `libraryPointerPath is not a function` (module does not exist).

- [ ] **Step 3: Write the implementation**

```typescript
// electron/library.ts
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'

/**
 * Where the app records which directory holds the library.
 *
 * The MCP server cannot ask Electron for `app.getPath('userData')`, and it must not guess:
 * that path is derived from the app NAME, which differs between a dev run (`flashcards`,
 * from package.json) and a packaged build (`Flashcards`, which electron-builder writes in).
 * Guessing wrong would silently create a second, empty library rather than fail. So the app
 * writes the answer to a fixed, name-independent location on every launch.
 */
export function libraryPointerPath(env: NodeJS.ProcessEnv = process.env): string {
  const home = env.HOME || env.USERPROFILE || os.homedir()
  return path.join(home, '.flashcards', 'library.json')
}

export async function writeLibraryPointer(
  root: string, env: NodeJS.ProcessEnv = process.env,
): Promise<void> {
  const file = libraryPointerPath(env)
  await fs.mkdir(path.dirname(file), { recursive: true })
  await fs.writeFile(file, JSON.stringify({ root }, null, 2), 'utf8')
}

/** env var > --root flag > pointer file > a clear error. Never a guess. */
export async function resolveLibraryRoot(
  argv: string[] = process.argv.slice(2), env: NodeJS.ProcessEnv = process.env,
): Promise<string> {
  if (env.FLASHCARDS_USER_DATA) return env.FLASHCARDS_USER_DATA

  const flag = argv.indexOf('--root')
  if (flag !== -1 && argv[flag + 1]) return argv[flag + 1]

  try {
    const pointer = JSON.parse(await fs.readFile(libraryPointerPath(env), 'utf8')) as { root?: string }
    if (pointer.root) return pointer.root
  } catch {
    // fall through to the error below
  }

  throw new Error(
    'Could not find the flashcard library. Launch the Flashcards app once so it can record '
    + `its location (expected a pointer at ${libraryPointerPath(env)}), or start this server `
    + 'with --root /path/to/library.',
  )
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `npx vitest run electron/library.test.ts`
Expected: PASS (5 tests).

- [ ] **Step 5: Have the app write the pointer**

In `electron/main.ts`, import `writeLibraryPointer` and call it inside `app.whenReady()`, immediately after `setStorageRoot`:

```typescript
    setStorageRoot(app.getPath('userData'))
    // Record where the library lives so the MCP server can find it without Electron.
    // Best-effort: a read-only home directory must not stop the app from starting.
    await writeLibraryPointer(app.getPath('userData')).catch(() => {})
```

- [ ] **Step 6: Verify nothing regressed**

Run: `npx tsc --noEmit && npx vitest run`
Expected: typecheck clean, all unit tests pass.

- [ ] **Step 7: Commit**

```bash
git add electron/library.ts electron/library.test.ts electron/main.ts
git commit -m "feat(mcp): record library location for out-of-Electron callers"
```

---

### Task 2: Tool handlers

The testable core. Plain functions over `storage.ts`, no MCP SDK types, so they can be exercised against a temp root exactly like `authoring.test.ts`.

**Files:**
- Create: `mcp/tools.ts`
- Create: `mcp/meta.ts`
- Create: `mcp/tools.test.ts`

**Interfaces:**
- Consumes: `setStorageRoot`, `listPacks`, `readPack`, `createPack`, `savePack`, `deletePack` from `electron/storage.ts`; `Card` from `shared/types.ts`.
- Produces:
  - `type CardInput = { front: string; primary: string; moreInfo?: string }`
  - `listPacksTool(): Promise<{ packs: {id,name,cardCount,hasUnsavedDraft}[] }>`
  - `readPackTool(a: {packId: string}): Promise<{id,name,cards}>`
  - `createPackTool(a: {name: string}): Promise<{packId,name}>`
  - `addCardsTool(a: {packId, cards: CardInput[], force?: boolean}): Promise<{packId,added,cardCount}>`
  - `savePackTool(a: {packId, cards: CardInput[], name?, force?}): Promise<{packId,name,cardCount}>`
  - `deletePackTool(a: {packId, confirm: string}): Promise<{packId,deleted:true}>`
  - `class ToolError extends Error` — thrown for every expected failure; `server.ts` renders it as `isError`.

- [ ] **Step 1: Write the failing tests**

```typescript
// mcp/tools.test.ts
import { describe, it, expect, beforeEach } from 'vitest'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { setStorageRoot, readPack, readPackForEdit, writeDraft } from '../electron/storage'
import {
  ToolError, listPacksTool, readPackTool, createPackTool,
  addCardsTool, savePackTool, deletePackTool,
} from './tools'

beforeEach(() => {
  setStorageRoot(fs.mkdtempSync(path.join(os.tmpdir(), 'fc-mcp-')))
})

const card = (front: string, primary: string) => ({ front, primary })

describe('create_pack', () => {
  it('creates an empty pack and returns its id', async () => {
    const { packId, name } = await createPackTool({ name: 'Capitals' })
    expect(name).toBe('Capitals')
    expect((await listPacksTool()).packs).toHaveLength(1)
    expect((await readPack(packId)).cards).toEqual([])
  })

  it('rejects a blank name', async () => {
    await expect(createPackTool({ name: '   ' })).rejects.toThrow(ToolError)
  })
})

describe('add_cards', () => {
  it('appends to the saved pack and saves', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    const result = await addCardsTool({ packId, cards: [card('France', 'Paris')] })
    expect(result).toEqual({ packId, added: 1, cardCount: 1 })

    await addCardsTool({ packId, cards: [card('Japan', 'Tokyo'), card('Peru', 'Lima')] })
    expect((await readPack(packId)).cards).toHaveLength(3)
  })

  it('gives every card a server-generated id', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await addCardsTool({ packId, cards: [card('France', 'Paris')] })
    const [only] = (await readPack(packId)).cards
    expect(only.id).toMatch(/[0-9a-f-]{36}/)
  })

  it('carries More Info through untouched', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await addCardsTool({
      packId,
      cards: [{ front: 'France', primary: 'Paris', moreInfo: '# Paris\n\nPop 2.1M.' }],
    })
    expect((await readPack(packId)).cards[0].moreInfo).toBe('# Paris\n\nPop 2.1M.')
  })

  it('refuses an incomplete card and writes nothing', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await expect(addCardsTool({ packId, cards: [card('France', '  ')] }))
      .rejects.toThrow(/incomplete/i)
    expect((await readPack(packId)).cards).toHaveLength(0)
  })

  it('refuses to clobber an unsaved draft', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await writeDraft(packId, 'Capitals', [{ id: 'd', front: 'draft', primary: 'work' }])

    await expect(addCardsTool({ packId, cards: [card('France', 'Paris')] }))
      .rejects.toThrow(/unsaved/i)
    expect((await readPackForEdit(packId)).cards[0].id).toBe('d')
  })

  it('overrides the draft guard when forced', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await writeDraft(packId, 'Capitals', [{ id: 'd', front: 'draft', primary: 'work' }])

    await addCardsTool({ packId, cards: [card('France', 'Paris')], force: true })
    expect((await readPack(packId)).cards).toHaveLength(1)
  })

  it('rejects an unknown pack', async () => {
    await expect(addCardsTool({ packId: 'nope', cards: [card('a', 'b')] }))
      .rejects.toThrow(ToolError)
  })
})

describe('save_pack', () => {
  it('replaces the whole card list', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await addCardsTool({ packId, cards: [card('France', 'Paris'), card('Japan', 'Tokyo')] })

    await savePackTool({ packId, cards: [card('Peru', 'Lima')] })
    const live = await readPack(packId)
    expect(live.cards).toHaveLength(1)
    expect(live.cards[0].front).toBe('Peru')
  })

  it('renames the pack when asked', async () => {
    const { packId } = await createPackTool({ name: 'Old' })
    await savePackTool({ packId, cards: [card('France', 'Paris')], name: 'New' })
    expect((await readPack(packId)).name).toBe('New')
  })
})

describe('list_packs', () => {
  it('returns summaries, never card bodies', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await addCardsTool({ packId, cards: [card('France', 'Paris')] })

    const [summary] = (await listPacksTool()).packs
    expect(summary).toEqual({
      id: packId, name: 'Capitals', cardCount: 1, hasUnsavedDraft: false,
    })
  })

  it('flags a pack the user has unsaved work in', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await writeDraft(packId, 'Capitals', [{ id: 'd', front: 'x', primary: 'y' }])
    expect((await listPacksTool()).packs[0].hasUnsavedDraft).toBe(true)
  })
})

describe('read_pack', () => {
  it('returns the saved cards', async () => {
    const { packId } = await createPackTool({ name: 'Capitals' })
    await addCardsTool({ packId, cards: [card('France', 'Paris')] })
    expect((await readPackTool({ packId })).cards[0].front).toBe('France')
  })
})

describe('delete_pack', () => {
  it('requires the confirmation word', async () => {
    const { packId } = await createPackTool({ name: 'Doomed' })
    await expect(deletePackTool({ packId, confirm: 'yes' })).rejects.toThrow(/DELETE/)
    expect((await listPacksTool()).packs).toHaveLength(1)
  })

  it('deletes when confirmed', async () => {
    const { packId } = await createPackTool({ name: 'Doomed' })
    expect(await deletePackTool({ packId, confirm: 'DELETE' })).toEqual({ packId, deleted: true })
    expect((await listPacksTool()).packs).toHaveLength(0)
  })
})
```

- [ ] **Step 2: Run to verify it fails**

Run: `npx vitest run mcp/tools.test.ts`
Expected: FAIL — cannot resolve `./tools`.

- [ ] **Step 3: Write the implementation**

```typescript
// mcp/tools.ts
import type { Card } from '../shared/types'
import {
  createPack, deletePack, listPacks, readPack, savePack,
} from '../electron/storage'
import { readPackMetaForTools } from './meta'

/** An expected failure the model can act on. `server.ts` renders these as `isError`. */
export class ToolError extends Error {}

export type CardInput = { front: string; primary: string; moreInfo?: string }

const toCard = (input: CardInput): Card => ({
  id: crypto.randomUUID(),
  front: input.front,
  primary: input.primary,
  // Preserve "absent" rather than storing an empty string — More Info is optional and the
  // Learn screen gates the Tab key on it being non-empty.
  ...(input.moreInfo ? { moreInfo: input.moreInfo } : {}),
})

async function requireMeta(packId: string) {
  const meta = await readPackMetaForTools(packId)
  if (!meta) throw new ToolError(`No pack with id "${packId}". Call list_packs to see what exists.`)
  return meta
}

/**
 * A save clears `draftVersion`, which would throw away whatever the user has open and
 * unsaved in Make a Pack. Refuse rather than race them; `force` is the deliberate override.
 */
async function guardDraft(packId: string, force: boolean | undefined) {
  const meta = await requireMeta(packId)
  if (meta.draftVersion !== undefined && !force) {
    throw new ToolError(
      `"${meta.name}" has unsaved changes open in the Flashcards editor. Saving now would `
      + 'discard them. Ask the user to save or leave that pack first, or retry with '
      + 'force: true to overwrite their unsaved work.',
    )
  }
  return meta
}

function commit(result: Awaited<ReturnType<typeof savePack>>) {
  if (result.ok) return
  const detail = result.problems
    .map(p => `card ${p.index + 1} is missing ${p.missing.join(' and ')}`)
    .join('; ')
  throw new ToolError(
    `${result.problems.length} card(s) incomplete — every card needs a front and a back. ${detail}`,
  )
}

export async function listPacksTool() {
  const metas = await listPacks()
  return {
    packs: metas.map(m => ({
      id: m.id,
      name: m.name,
      cardCount: m.cardCount,
      hasUnsavedDraft: m.draftVersion !== undefined,
    })),
  }
}

export async function readPackTool({ packId }: { packId: string }) {
  await requireMeta(packId)
  const pack = await readPack(packId)
  return { id: pack.id, name: pack.name, cards: pack.cards }
}

export async function createPackTool({ name }: { name: string }) {
  if (!name.trim()) throw new ToolError('A pack needs a name.')
  const meta = await createPack(name.trim())
  return { packId: meta.id, name: meta.name }
}

export async function addCardsTool(
  { packId, cards, force }: { packId: string; cards: CardInput[]; force?: boolean },
) {
  if (cards.length === 0) throw new ToolError('No cards supplied.')
  const meta = await guardDraft(packId, force)
  const existing = (await readPack(packId)).cards
  const next = [...existing, ...cards.map(toCard)]

  commit(await savePack(packId, meta.name, next))
  return { packId, added: cards.length, cardCount: next.length }
}

export async function savePackTool(
  { packId, cards, name, force }:
  { packId: string; cards: CardInput[]; name?: string; force?: boolean },
) {
  const meta = await guardDraft(packId, force)
  const finalName = name?.trim() || meta.name
  const next = cards.map(toCard)

  commit(await savePack(packId, finalName, next))
  return { packId, name: finalName, cardCount: next.length }
}

export async function deletePackTool({ packId, confirm }: { packId: string; confirm: string }) {
  const meta = await requireMeta(packId)
  // Mirrors the editor's type-DELETE gate: an irreversible action should never be one
  // malformed tool call away.
  if (confirm !== 'DELETE') {
    throw new ToolError(
      `Refusing to delete "${meta.name}". Pass confirm: "DELETE" if the user has actually `
      + 'asked for this pack and all its backups to be destroyed.',
    )
  }
  await deletePack(packId)
  return { packId, deleted: true as const }
}
```

- [ ] **Step 4: Add the meta reader the handlers need**

`storage.ts` keeps `readMeta` private. Rather than widen it, add a narrow named export used only by the tools:

```typescript
// mcp/meta.ts
import type { PackMeta } from '../shared/types'
import { listPacks } from '../electron/storage'

/**
 * The tools need `draftVersion` and the saved name before deciding whether a write is safe.
 * `listPacks` already reads every meta.json and skips malformed ones, so this reuses it
 * rather than exposing storage.ts's private `readMeta`.
 */
export async function readPackMetaForTools(packId: string): Promise<PackMeta | null> {
  return (await listPacks()).find(m => m.id === packId) ?? null
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `npx vitest run mcp/tools.test.ts`
Expected: PASS (16 tests).

- [ ] **Step 6: Verify nothing regressed**

Run: `npx tsc --noEmit && npx vitest run`
Expected: typecheck clean, all unit tests pass.

- [ ] **Step 7: Commit**

```bash
git add mcp/tools.ts mcp/meta.ts mcp/tools.test.ts
git commit -m "feat(mcp): pack authoring tool handlers with draft and validation guards"
```

---

### Task 3: Server wiring and bundle

**Files:**
- Create: `mcp/server.ts`
- Modify: `package.json` (deps + `build:mcp` script)
- Modify: `.gitignore` (`dist-mcp`)

**Interfaces:**
- Consumes: everything Task 1 and Task 2 produce.
- Produces: `dist-mcp/server.mjs`, a single self-contained executable bundle.

- [ ] **Step 1: Write the server**

```typescript
// mcp/server.ts
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { z } from 'zod'
import { resolveLibraryRoot } from '../electron/library'
import { setStorageRoot } from '../electron/storage'
import {
  ToolError, addCardsTool, createPackTool, deletePackTool,
  listPacksTool, readPackTool, savePackTool,
} from './tools'

const cardShape = z.object({
  front: z.string().describe('The prompt side of the card. Required.'),
  primary: z.string().describe('The answer shown when the card is flipped. Required.'),
  moreInfo: z.string().optional()
    .describe('Optional Markdown shown in a scrollable panel. Never validated.'),
})

/** Every handler funnels through here so an expected failure reads as guidance, not a crash. */
function wrap<A>(fn: (args: A) => Promise<unknown>) {
  return async (args: A) => {
    try {
      return { content: [{ type: 'text' as const, text: JSON.stringify(await fn(args), null, 2) }] }
    } catch (error) {
      const message = error instanceof ToolError
        ? error.message
        : `Unexpected failure: ${error instanceof Error ? error.message : String(error)}`
      return { content: [{ type: 'text' as const, text: message }], isError: true }
    }
  }
}

async function main() {
  // Resolve before serving: a server that starts without a library would answer every call
  // with the same error, which reads like a broken tool rather than a setup step.
  setStorageRoot(await resolveLibraryRoot())

  const server = new McpServer({ name: 'flashcards', version: '0.1.0' })

  server.registerTool('list_packs', {
    title: 'List packs',
    description:
      'Every pack in the library, as summaries only (id, name, card count, whether the user '
      + 'has unsaved work open in it). Cheap — start here. Use read_pack for card contents.',
    inputSchema: {},
    annotations: { readOnlyHint: true },
  }, wrap(listPacksTool))

  server.registerTool('read_pack', {
    title: 'Read a pack',
    description:
      'Every card in one pack. This can be large — a full pack lands in the conversation and '
      + 'is resent on later turns. Prefer list_packs unless the card text is actually needed.',
    inputSchema: { packId: z.string().describe('Pack id from list_packs.') },
    annotations: { readOnlyHint: true },
  }, wrap(readPackTool))

  server.registerTool('create_pack', {
    title: 'Create a pack',
    description:
      'Create an empty pack and return its id. It stays hidden from the study screen until '
      + 'it has saved cards, so follow this with add_cards.',
    inputSchema: { name: z.string().describe('User-facing pack name.') },
  }, wrap(createPackTool))

  server.registerTool('add_cards', {
    title: 'Add cards',
    description:
      'Append cards to a pack and save. Send every card in ONE call — a call per card resends '
      + 'the whole conversation each time. Every card needs a front and a back or nothing is '
      + 'written. Card ids are generated here; do not supply them.',
    inputSchema: {
      packId: z.string(),
      cards: z.array(cardShape).min(1).describe('All the cards to append, in one batch.'),
      force: z.boolean().optional()
        .describe('Overwrite the user\'s unsaved editor changes. Ask them first.'),
    },
  }, wrap(addCardsTool))

  server.registerTool('save_pack', {
    title: 'Replace a pack\'s cards',
    description:
      'Replace the ENTIRE card list, and optionally rename the pack. Cards not included are '
      + 'removed. Use add_cards to append without destroying existing cards.',
    inputSchema: {
      packId: z.string(),
      cards: z.array(cardShape).describe('The complete new card list.'),
      name: z.string().optional(),
      force: z.boolean().optional(),
    },
    annotations: { destructiveHint: true },
  }, wrap(savePackTool))

  server.registerTool('delete_pack', {
    title: 'Delete a pack',
    description:
      'Permanently delete a pack and every backup of it. Irreversible. Only call this when '
      + 'the user has explicitly asked for this specific pack to be destroyed.',
    inputSchema: {
      packId: z.string(),
      confirm: z.string().describe('Must be exactly "DELETE".'),
    },
    annotations: { destructiveHint: true },
  }, wrap(deletePackTool))

  await server.connect(new StdioServerTransport())
}

main().catch(error => {
  // stdout is the protocol channel — diagnostics must go to stderr or they corrupt it.
  console.error(error instanceof Error ? error.message : String(error))
  process.exit(1)
})
```

- [ ] **Step 2: Add dependencies and the build script**

```bash
npm install --save-dev @modelcontextprotocol/sdk@1.30.0 zod
```

Add to `package.json` scripts:

```json
"build:mcp": "esbuild mcp/server.ts --bundle --platform=node --format=esm --target=node20 --outfile=dist-mcp/server.mjs --banner:js=\"import{createRequire}from'node:module';const require=createRequire(import.meta.url);\""
```

The banner is required: the SDK's transitive CommonJS dependencies call `require` at runtime, which does not exist in an ESM bundle.

- [ ] **Step 3: Build and verify the bundle runs**

```bash
npm run build:mcp
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | FLASHCARDS_USER_DATA=/tmp/mcp-smoke node dist-mcp/server.mjs
```

Expected: a JSON-RPC response listing all six tools. An error mentioning the library location means Task 1's resolution is misbehaving.

- [ ] **Step 4: Ignore the build output**

Add `dist-mcp` to `.gitignore`.

- [ ] **Step 5: Verify nothing regressed**

Run: `npx tsc --noEmit && npx vitest run`

- [ ] **Step 6: Commit**

```bash
git add mcp/server.ts package.json package-lock.json .gitignore
git commit -m "feat(mcp): stdio server exposing the pack authoring tools"
```

---

### Task 4: End-to-end check and documentation

Proves the two processes actually share one library: write through the MCP server, read through the app.

**Files:**
- Create: `mcp/roundtrip.test.ts`
- Create: `docs/mcp.md`
- Modify: `docs/vision.md` (§14 Q3 — record what is now built)

**Interfaces:**
- Consumes: the built `dist-mcp/server.mjs` and `electron/storage.ts`.
- Produces: no new exports.

- [ ] **Step 1: Write the failing round-trip test**

```typescript
// mcp/roundtrip.test.ts
import { describe, it, expect, beforeEach } from 'vitest'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { setStorageRoot, listPacks, readPack } from '../electron/storage'
import { addCardsTool, createPackTool } from './tools'

let root: string
beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), 'fc-round-'))
  setStorageRoot(root)
})

describe('a pack written through the MCP tools', () => {
  it('is on disk in the layout the app reads', async () => {
    const { packId } = await createPackTool({ name: 'From Claude' })
    await addCardsTool({
      packId,
      cards: [
        { front: 'France', primary: 'Paris', moreInfo: 'Pop 2.1M.' },
        { front: 'Japan', primary: 'Tokyo' },
      ],
    })

    // The exact files the Electron app expects.
    expect(fs.existsSync(path.join(root, 'packs', packId, 'meta.json'))).toBe(true)

    const [meta] = await listPacks()
    expect(meta).toMatchObject({ id: packId, name: 'From Claude', cardCount: 2 })
    expect(meta.draftVersion).toBeUndefined()

    const pack = await readPack(packId)
    expect(pack.cards.map(c => c.front)).toEqual(['France', 'Japan'])
    expect(pack.cards[0].moreInfo).toBe('Pop 2.1M.')
    expect(pack.cards[1].moreInfo).toBeUndefined()
  })

  it('is immediately studyable — cardCount is what gates Pack Selection', async () => {
    const { packId } = await createPackTool({ name: 'From Claude' })
    await addCardsTool({ packId, cards: [{ front: 'a', primary: 'b' }] })
    expect((await listPacks()).filter(p => p.cardCount > 0).map(p => p.id)).toContain(packId)
  })
})
```

- [ ] **Step 2: Run it**

Run: `npx vitest run mcp/roundtrip.test.ts`
Expected: PASS.

- [ ] **Step 3: Write the setup documentation**

Create `docs/mcp.md` covering: what the server is, the one-time `npm run build:mcp`, the exact JSON block to add to the user's MCP client config (with `command: "node"` and `args: ["<abs path>/dist-mcp/server.mjs"]`), how the library is located, the six tools, the draft guard, and the note that batching cards into one `add_cards` call keeps context small.

- [ ] **Step 4: Record the resolved decision in the vision**

Update §14 Q3 to state that the shape is settled — standalone stdio, a pointer file for discovery, `savePack` as the shared gate, the draft guard — and remove the "still open" line about the editor race, which the guard now answers.

- [ ] **Step 5: Full verification**

Run: `npx tsc --noEmit && npx vitest run && npm run build && npx playwright test`
Expected: typecheck clean, all unit tests pass, all e2e tests pass.

- [ ] **Step 6: Commit**

```bash
git add mcp/roundtrip.test.ts docs/mcp.md docs/vision.md
git commit -m "docs(mcp): setup guide and round-trip coverage"
```
