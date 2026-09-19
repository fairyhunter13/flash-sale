import { cleanup } from '@testing-library/react'
import { afterEach } from 'vitest'

// toBeDisabled and toHaveTextContent come from here. Without this file the
// matchers are missing at run time and absent from the type.
import '@testing-library/jest-dom/vitest'

// Testing Library cleans up by itself only when Vitest runs with globals on,
// and this project does not. Without this each render stays in the document,
// and the next query reads "Found multiple elements".
afterEach(cleanup)
