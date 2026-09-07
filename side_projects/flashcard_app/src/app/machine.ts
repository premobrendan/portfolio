import type { ConfirmIntent } from '../../shared/types'

export type Screen = 'title' | 'packs' | 'learn' | 'settings' | 'makepack'
export type Layer =
  | { kind: 'moreInfo' }
  | { kind: 'confirm'; intent: ConfirmIntent }

export type UI = { screen: Screen; layers: Layer[] }

export type Action =
  | { type: 'goto'; screen: Screen }
  | { type: 'pushLayer'; layer: Layer }
  | { type: 'popLayer' }

export const initialUI: UI = { screen: 'title', layers: [] }

/** The layer that owns input right now, or null when the screen owns it. */
export function topLayer(ui: UI): Layer | null {
  return ui.layers.length ? ui.layers[ui.layers.length - 1] : null
}

export function reduce(ui: UI, action: Action): UI {
  switch (action.type) {
    case 'goto':
      // Overlays never survive a screen change.
      return { screen: action.screen, layers: [] }
    case 'pushLayer':
      return { ...ui, layers: [...ui.layers, action.layer] }
    case 'popLayer':
      return ui.layers.length ? { ...ui, layers: ui.layers.slice(0, -1) } : ui
  }
}
