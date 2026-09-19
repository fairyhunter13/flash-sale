import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const ROOT = new URL('../../', import.meta.url)

function read(path: string): string {
  return readFileSync(fileURLToPath(new URL(path, ROOT)), 'utf8')
}

const LOG = read('docs/decisions.md')

/**
 * The authored rows are read with a regex and not a YAML parser, because the
 * server ships no YAML dependency and the shape here is two fixed lines.
 */
function decisionNames(): string[] {
  const yaml = read('concepts/authored.yaml')
  return [...yaml.matchAll(/^- type: Decision\n {2}name: (.+)$/gm)].map((it) => it[1] as string)
}

const HEADINGS = [...LOG.matchAll(/^## (.+)$/gm)].map((it) => it[1] as string)

describe('the decision log', () => {
  it('every decision concept is answered', () => {
    const names = decisionNames()
    expect(names.length).toBeGreaterThan(0)
    expect(names.filter((name) => !HEADINGS.includes(name))).toEqual([])
  })

  it('every entry states the question and what it gives up', () => {
    const bodies = LOG.split(/^## .+$/gm).slice(1)
    expect(bodies).toHaveLength(HEADINGS.length)
    const thin = HEADINGS.filter((_heading, index) => {
      const body = bodies[index] ?? ''
      return !body.includes('**Q.') || !body.includes('**What it gives up.**')
    })
    expect(thin).toEqual([])
  })
})
