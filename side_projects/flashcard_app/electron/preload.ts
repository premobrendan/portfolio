import { contextBridge, ipcRenderer } from 'electron'
import type { AppState, Card, Settings } from '../shared/types'

contextBridge.exposeInMainWorld('flashcards', {
  packs: {
    list: () => ipcRenderer.invoke('packs:list'),
    read: (id: string) => ipcRenderer.invoke('packs:read', id),
    readForEdit: (id: string) => ipcRenderer.invoke('packs:readForEdit', id),
    create: (name: string) => ipcRenderer.invoke('packs:create', name),
    saveDraft: (id: string, name: string, cards: Card[]) =>
      ipcRenderer.invoke('packs:saveDraft', id, name, cards),
    save: (id: string, name: string, cards: Card[]) =>
      ipcRenderer.invoke('packs:save', id, name, cards),
    remove: (id: string) => ipcRenderer.invoke('packs:remove', id),
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
