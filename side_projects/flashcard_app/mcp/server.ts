import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { z } from 'zod'
import { resolveLibraryRoot } from '../electron/library'
import { setStorageRoot } from '../electron/storage'
import {
  ToolError, addCardsTool, createPackTool, deletePackTool,
  listPacksTool, readPackTool, replaceAllCardsTool, updateCardTool,
} from './tools'

const cardShape = z.object({
  front: z.string().describe('The prompt side of the card. Required.'),
  primary: z.string().describe('The answer shown when the card is flipped. Required.'),
  secondaries: z.array(z.string()).optional().describe(
    'FURTHER facts about the same subject — e.g. primary "He was the 16th president", '
    + 'secondaries ["He issued the Emancipation Proclamation", "He delivered the '
    + 'Gettysburg Address"]. Any number. They join primary as the pool of backs the card '
    + 'can show, so one card teaches several things. Optional and never validated.',
  ),
  moreInfo: z.string().optional()
    .describe('Optional Markdown shown in a scrollable panel. Never validated.'),
})

/** Every handler funnels through here so an expected failure reads as guidance, not a crash. */
function wrap<A>(fn: (args: A) => Promise<unknown>) {
  return async (args: A) => {
    try {
      return {
        content: [{ type: 'text' as const, text: JSON.stringify(await fn(args), null, 2) }],
      }
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
      'Create an empty pack and return its id. It stays hidden from the study screen until it '
      + 'has saved cards, so follow this with add_cards.',
    inputSchema: { name: z.string().describe('User-facing pack name.') },
  }, wrap(createPackTool))

  server.registerTool('add_cards', {
    title: 'Add cards',
    description:
      'Append cards to a pack and save. Send every card in ONE call — a call per card resends '
      + 'the whole conversation each time. Every card needs a front and a back or nothing is '
      + 'written. Card ids are generated here; do not supply them — they come back in the '
      + 'result, ready for update_card.',
    inputSchema: {
      packId: z.string(),
      cards: z.array(cardShape).min(1).describe('All the cards to append, in one batch.'),
      force: z.boolean().optional()
        .describe("Overwrite the user's unsaved editor changes. Ask them first."),
    },
  }, wrap(addCardsTool))

  server.registerTool('update_card', {
    title: 'Edit one card',
    description:
      'Change one card in place. Omitted fields are left alone. This is the right way to fix '
      + 'a typo — replace_all_cards would make you send the whole pack back. Card ids come '
      + 'from add_cards or read_pack. Pass moreInfo: "" to remove More Info.',
    inputSchema: {
      packId: z.string(),
      cardId: z.string().describe('From an add_cards result or read_pack.'),
      front: z.string().optional().describe('New prompt side. Omit to leave unchanged.'),
      primary: z.string().optional().describe('New answer side. Omit to leave unchanged.'),
      secondaries: z.array(z.string()).optional()
        .describe('Replaces the whole list of extra facts. Omit to leave unchanged; [] to '
          + 'remove them all.'),
      moreInfo: z.string().optional()
        .describe('New Markdown detail. Omit to leave unchanged; "" to remove it.'),
      force: z.boolean().optional(),
    },
  }, wrap(updateCardTool))

  server.registerTool('replace_all_cards', {
    title: "Replace a pack's entire card list",
    description:
      'DESTRUCTIVE: replaces every card in the pack, and optionally renames it. Any card not '
      + 'included is deleted. Use add_cards to append and update_card to edit — this tool is '
      + 'only for deliberately rewriting a pack wholesale.',
    inputSchema: {
      packId: z.string(),
      cards: z.array(cardShape)
        .describe('The complete new card list. Anything omitted is deleted.'),
      name: z.string().optional(),
      force: z.boolean().optional(),
    },
    annotations: { destructiveHint: true },
  }, wrap(replaceAllCardsTool))

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
