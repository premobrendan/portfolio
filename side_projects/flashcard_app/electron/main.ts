import { app, BrowserWindow } from 'electron'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { registerIpc } from './ipc'
import { seedIfEmpty } from './seed'
import { setStorageRoot } from './storage'

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
    await seedIfEmpty(resourcesDir())
    registerIpc()
    createWindow()
  })
}

app.on('window-all-closed', () => app.quit())
