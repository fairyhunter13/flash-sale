import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const ROOT = new URL('../../', import.meta.url)
const README = readFileSync(fileURLToPath(new URL('README.md', ROOT)), 'utf8')

/** npm runs these itself, so no package.json has to declare them. */
const BUILT_IN = new Set(['install', 'ci', 'test', 'start'])

function scriptsOf(manifest: string): string[] {
  const json = JSON.parse(readFileSync(fileURLToPath(new URL(manifest, ROOT)), 'utf8')) as {
    scripts?: Record<string, string>
  }
  return Object.keys(json.scripts ?? {})
}

const SCRIPTS = new Set([
  ...scriptsOf('package.json'),
  ...scriptsOf('server/package.json'),
  ...scriptsOf('web/package.json'),
  ...scriptsOf('stress/package.json'),
])

/** Every `npm ...` the README tells the reader to type, wherever it appears. */
function npmCommands(text: string): string[] {
  return [...text.matchAll(/npm (?:run )?([a-z][a-z0-9:-]*)/g)].map((it) => it[1] as string)
}

describe('the README', () => {
  it('every documented command exists', () => {
    const missing = npmCommands(README).filter(
      (name) => !BUILT_IN.has(name) && !SCRIPTS.has(name),
    )
    expect([...new Set(missing)]).toEqual([])
  })

  it('the README holds a mermaid block and a scaling section', () => {
    expect(README).toContain('```mermaid')
    expect(README).toMatch(/^## Scaling$/m)
    expect(README).toMatch(/^## Trade-offs$/m)
  })

  it('every measured number names its command', () => {
    // The Measured table is the only place a number is claimed, and each row
    // must end in the command a reader runs to see it again.
    const table = README.split('## Measured')[1]?.split('\n##')[0] ?? ''
    const rows = table
      .split('\n')
      .filter((line) => line.startsWith('|') && !line.startsWith('| ---') && !line.includes('| Measure |'))

    expect(rows.length).toBeGreaterThan(0)
    const unsourced = rows.filter((row) => {
      const command = row.split('|')[3]?.trim() ?? ''
      const name = npmCommands(command)[0]
      return name === undefined || (!BUILT_IN.has(name) && !SCRIPTS.has(name))
    })
    expect(unsourced).toEqual([])
  })
})
