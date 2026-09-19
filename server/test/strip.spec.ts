import { readFileSync, readdirSync } from 'node:fs'
import { join, relative } from 'node:path'
import { stripTypeScriptTypes } from 'node:module'
import { describe, expect, it } from 'vitest'

const SRC = new URL('../src/', import.meta.url).pathname

function everySourceFile(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name)
    if (entry.isDirectory()) return everySourceFile(path)
    return entry.name.endsWith('.ts') ? [path] : []
  })
}

/**
 * `npm start` runs `node --experimental-strip-types`, which deletes types and
 * rewrites nothing. So a constructor parameter property, an enum or a
 * namespace is a SyntaxError at boot, and no other test finds it: Vitest
 * compiles the TypeScript rather than stripping it.
 */
describe('the source runs under node', () => {
  it('every server source file strips cleanly', () => {
    const broken: string[] = []
    for (const path of everySourceFile(SRC)) {
      try {
        stripTypeScriptTypes(readFileSync(path, 'utf8'), { mode: 'strip' })
      } catch (error) {
        broken.push(`${relative(SRC, path)}: ${(error as Error).message.split('\n')[0]}`)
      }
    }
    expect(broken).toEqual([])
  })
})
