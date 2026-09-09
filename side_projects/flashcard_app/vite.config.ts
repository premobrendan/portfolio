/// <reference types="vitest" />
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import electron from 'vite-plugin-electron/simple'

export default defineConfig({
  plugins: [
    react(),
    electron({
      main: { entry: 'electron/main.ts' },
      preload: { input: 'electron/preload.ts' },
    }),
  ],
  build: { outDir: 'dist' },
  // DOMPurify needs a DOM, so unit tests run under jsdom.
  test: { environment: 'jsdom', include: ['src/**/*.test.ts', 'electron/**/*.test.ts', 'shared/**/*.test.ts', 'mcp/**/*.test.ts'] },
})
