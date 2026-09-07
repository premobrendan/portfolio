export type ThemeId = 'space'

export const THEMES: Record<ThemeId, { id: ThemeId; name: string }> = {
  space: { id: 'space', name: 'Space' },
}

export function applyTheme(themeId: string) {
  document.documentElement.dataset.theme = themeId
}
