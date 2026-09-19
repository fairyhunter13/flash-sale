import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// The page calls /api with no host, so the dev server and the built page use
// the same code. In development this proxy sends /api to Fastify. In a review
// Fastify serves the built page itself, and the path is already correct.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: `http://127.0.0.1:${process.env['PORT'] ?? 3000}`,
        changeOrigin: true,
      },
    },
  },
  build: { outDir: 'dist' },
})
