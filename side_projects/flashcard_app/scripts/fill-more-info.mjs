#!/usr/bin/env node
/**
 * TEST SCAFFOLDING — fills every presidents card's More Info with its own Back
 * Primary Text repeated 100 times, so the More Info panel has something long
 * enough to scroll. Run with `--clear` to strip it back out.
 *
 *   node scripts/fill-more-info.mjs           # fill
 *   node scripts/fill-more-info.mjs --clear   # revert to no moreInfo
 *
 * Also writes a new numbered version into the dev profile so a running install
 * picks the change up without deleting its packs.
 */
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

const clear = process.argv.includes('--clear')
const SRC = 'resources/packs/presidents.json'
const pack = JSON.parse(fs.readFileSync(SRC, 'utf8'))

const REPEATS = 100, PER_PARA = 10
for (const card of pack.cards) {
  if (clear) { delete card.moreInfo; continue }
  const paras = []
  for (let i = 0; i < REPEATS; i += PER_PARA) {
    paras.push(Array(Math.min(PER_PARA, REPEATS - i)).fill(card.primary).join(' '))
  }
  card.moreInfo = paras.join('\n\n')
}
fs.writeFileSync(SRC, JSON.stringify(pack, null, 2) + '\n')

// Push a new version into the dev profile so a seeded install sees it.
const profile = path.join(os.homedir(), 'Library/Application Support/flashcards')
const dir = path.join(profile, 'packs', pack.id)
if (fs.existsSync(dir)) {
  const meta = JSON.parse(fs.readFileSync(path.join(dir, 'meta.json'), 'utf8'))
  const version = meta.currentVersion + 1
  fs.writeFileSync(path.join(dir, `v${version}.json`), JSON.stringify({ cards: pack.cards }))
  fs.writeFileSync(path.join(dir, 'meta.json'), JSON.stringify(
    { ...meta, cardCount: pack.cards.length, currentVersion: version }, null, 2))
  console.log(`dev profile: wrote v${version}`)
} else {
  console.log('dev profile: none found, bundled pack updated only')
}

const c = pack.cards[0]
console.log(`${clear ? 'cleared' : 'filled'} — ${pack.cards.length} cards, ` +
  `moreInfo chars on first card: ${c.moreInfo ? c.moreInfo.length : 0}`)
