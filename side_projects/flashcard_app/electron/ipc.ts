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
