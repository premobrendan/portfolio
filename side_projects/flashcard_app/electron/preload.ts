import { contextBridge, ipcRenderer } from 'electron'
import type { AppState, Settings } from '../shared/types'

contextBridge.exposeInMainWorld('flashcards', {
  packs: {
    list: () => ipcRenderer.invoke('packs:list'),
    read: (id: string) => ipcRenderer.invoke('packs:read', id),
  },
  state: {
    get: () => ipcRenderer.invoke('state:get'),
    set: (patch: Partial<AppState>) => ipcRenderer.invoke('state:set', patch),
  },
  settings: {
    get: () => ipcRenderer.invoke('settings:get'),
    set: (patch: Partial<Settings>) => ipcRenderer.invoke('settings:set', patch),
  },
  app: { quit: () => ipcRenderer.send('app:quit') },
})
