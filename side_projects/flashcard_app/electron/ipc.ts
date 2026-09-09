import { app, ipcMain } from 'electron'
import type { AppState, Card, Settings } from '../shared/types'
import {
  createPack, deletePack, listPacks, readPack, readPackForEdit, readSettings, readState,
  savePack, writeDraft, writeSettings, writeState,
} from './storage'

export function registerIpc() {
  ipcMain.handle('packs:list', () => listPacks())
  ipcMain.handle('packs:read', (_e, id: string) => readPack(id))
  // Authoring. Every handler is a pass-through: the logic, and crucially the validation,
  // lives in storage.ts so the future MCP server shares it rather than reimplementing it.
  ipcMain.handle('packs:readForEdit', (_e, id: string) => readPackForEdit(id))
  ipcMain.handle('packs:create', (_e, name: string) => createPack(name))
  ipcMain.handle('packs:saveDraft', async (_e, id: string, name: string, cards: Card[]) => {
    await writeDraft(id, name, cards)
  })
  ipcMain.handle('packs:save', (_e, id: string, name: string, cards: Card[]) =>
    savePack(id, name, cards))
  ipcMain.handle('packs:remove', (_e, id: string) => deletePack(id))
  ipcMain.handle('state:get', () => readState())
  ipcMain.handle('state:set', (_e, patch: Partial<AppState>) => writeState(patch))
  ipcMain.handle('settings:get', () => readSettings())
  ipcMain.handle('settings:set', (_e, patch: Partial<Settings>) => writeSettings(patch))
  ipcMain.on('app:quit', () => app.quit())
}
