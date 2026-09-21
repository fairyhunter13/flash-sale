import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

// `projects` replaces vitest.workspace.ts, which is deprecated since Vitest 3.2.
// One project per test kind, so `--project '*-unit'` runs the tests that need no
// container and `--project '*-integration'` runs the ones that do.
export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: 'server-unit',
          root: './server',
          environment: 'node',
          include: ['test/unit/**/*.spec.ts'],
        },
      },
      {
        test: {
          name: 'server-integration',
          root: './server',
          environment: 'node',
          include: ['test/integration/**/*.spec.ts'],
          globalSetup: ['./test/setup/containers.ts'],
          // A container start plus a real race needs more than the 5 s default.
          testTimeout: 60_000,
          hookTimeout: 120_000,
        },
      },
      {
        plugins: [react()],
        test: {
          name: 'web-unit',
          root: './web',
          environment: 'happy-dom',
          include: ['test/unit/**/*.spec.tsx'],
          setupFiles: ['./test/setup.ts'],
        },
      },
      {
        plugins: [react()],
        test: {
          name: 'web-integration',
          root: './web',
          environment: 'happy-dom',
          include: ['test/integration/**/*.spec.tsx'],
          setupFiles: ['./test/setup.ts'],
        },
      },
    ],
  },
})
