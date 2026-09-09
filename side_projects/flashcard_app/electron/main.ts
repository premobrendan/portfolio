import { app, BrowserWindow, shell } from 'electron'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { registerIpc } from './ipc'
import { recordLibraryPointer } from './library'
import { seedIfEmpty } from './seed'
import { setStorageRoot } from './storage'
import { isSafeExternalUrl } from '../shared/externalLink'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

/** Bundled packs: alongside the app when packaged, else `<repo>/resources/packs`
 *  relative to the emitted `dist-electron/` directory. */
function resourcesDir() {
  return app.isPackaged
    ? path.join(process.resourcesPath, 'packs')
    : path.join(__dirname, '../resources/packs')
}

let win: BrowserWindow | null = null

/** Set by the Playwright harness. Keeps a test run off-screen and out of the dock. */
const HIDDEN = process.env.FLASHCARDS_HIDDEN === '1'

function createWindow() {
  win = new BrowserWindow({
    width: 1100,
    height: 760,
    backgroundColor: '#05060f',
    show: false,
    webPreferences: {
      preload: path.join(__dirname, 'preload.mjs'),
      contextIsolation: true,
      // A hidden window would otherwise have its timers throttled, which stalls
      // animations and makes test timing unreliable.
      backgroundThrottling: false,
      nodeIntegration: false,
      sandbox: true,
    },
  })

  // Tests run the app headless-ish: the window is never shown and the dock icon is
  // hidden, so a suite run cannot steal focus or flash windows over your work.
  // The renderer still loads and paints (paintWhenInitiallyHidden defaults true, and
  // backgroundThrottling is off), so Playwright drives and screenshots it normally.
  win.once('ready-to-show', () => { if (!HIDDEN) win?.show() })

  /**
   * Source links in a card's More Info belong in the user's browser, not in this window.
   * The renderer marks every link `target="_blank"`, which arrives here as a window-open
   * request; we hand it to the OS and always deny the popup.
   *
   * `isSafeExternalUrl` gates it because card content is untrusted — packs can be written
   * by the MCP server, i.e. by a model — and `shell.openExternal` will happily hand a
   * `file:` or custom-scheme URL to whatever the OS has registered for it.
   */
  win.webContents.setWindowOpenHandler(({ url }) => {
    if (isSafeExternalUrl(url)) void shell.openExternal(url)
    return { action: 'deny' }
  })

  /**
   * The belt to that braces: anything that still tries to navigate this window away from
   * the app is stopped and sent to the browser instead. Without this, one link that slipped
   * the handler above would replace the app with a web page and there is no way back —
   * no address bar, no back button.
   */
  win.webContents.on('will-navigate', (event, url) => {
    const here = process.env.VITE_DEV_SERVER_URL
    if (here && url.startsWith(here)) return   // dev server reloads are legitimate
    event.preventDefault()
    if (isSafeExternalUrl(url)) void shell.openExternal(url)
  })

  if (process.env.VITE_DEV_SERVER_URL) {
    win.loadURL(process.env.VITE_DEV_SERVER_URL)
  } else {
    win.loadFile(path.join(__dirname, '../dist/index.html'))
  }
}

// FLASHCARDS_USER_DATA is what gives each test an isolated profile. It has to move
// Electron's own userData too, not just our storage root: the single-instance lock lives
// there, so otherwise every test instance shares one lock with the dev app and with each
// other, and the suite dies the moment a second window is up.
if (process.env.FLASHCARDS_USER_DATA) app.setPath('userData', process.env.FLASHCARDS_USER_DATA)

if (!app.requestSingleInstanceLock()) {
  app.quit()
} else {
  app.whenReady().then(async () => {
    if (HIDDEN) app.dock?.hide()
    setStorageRoot(app.getPath('userData'))
    // Record where the library lives so the MCP server can find it without Electron.
    // Skipped when the profile was overridden — see recordLibraryPointer.
    await recordLibraryPointer(app.getPath('userData'))
    await seedIfEmpty(resourcesDir())
    registerIpc()
    createWindow()
  })
}

app.on('window-all-closed', () => app.quit())
