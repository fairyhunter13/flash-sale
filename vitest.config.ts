import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

// `projects` replaces vitest.workspace.ts, which is deprecated since Vitest 3.2.
export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: 'server',
          root: './server',
          environment: 'node',
          include: ['test/**/*.spec.ts'],
          // A container start plus a real race needs more than the 5 s default.
          testTimeout: 60_000,
          hookTimeout: 120_000,
        },
      },
      {
        plugins: [react()],
        test: {
          name: 'web',
          root: './web',
          environment: 'happy-dom',
          include: ['test/**/*.spec.tsx'],
        },
      },
    ],
  },
})
