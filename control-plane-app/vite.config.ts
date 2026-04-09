import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  root: './frontend',
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './frontend/src'),
      '@dagrejs/dagre': path.resolve(__dirname, 'node_modules/@dagrejs/dagre/dist/dagre.cjs.js'),
    },
  },
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
      '/ws': {
        target: 'ws://localhost:8080',
        ws: true,
      },
    },
  },
  optimizeDeps: {
    include: ['@dagrejs/dagre'],
  },
  build: {
    outDir: '../dist',
    emptyOutDir: true,
  },
})
