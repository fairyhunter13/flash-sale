import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const ROOT = new URL('../../', import.meta.url)
const YAML = readFileSync(fileURLToPath(new URL('concepts/authored.yaml', ROOT)), 'utf8')

type Row = {
  readonly type: string
  readonly name: string
  readonly edges: ReadonlyArray<{ readonly kind: string; readonly to: string }>
}

/**
 * The rows are read line by line and not by a YAML parser, because no workspace here ships
 * one and the shape is fixed: a row header, a name, then an edges block of kind and to.
 * `grounding` uses the same indent as `edges`, so the reader tracks which block it is in.
 */
function rows(): Row[] {
  const found: Row[] = []
  let row: { type: string; name: string; edges: { kind: string; to: string }[] } | null = null
  let inEdges = false
  for (const line of YAML.split('\n')) {
    const header = /^- type: (.+)$/.exec(line)
    if (header) {
      if (row) found.push(row)
      row = { type: header[1] as string, name: '', edges: [] }
      inEdges = false
      continue
    }
    if (!row) continue
    const name = /^ {2}name: (.+)$/.exec(line)
    if (name) {
      row.name = name[1] as string
      continue
    }
    if (/^ {2}[a-z]+:/.test(line)) inEdges = line.startsWith('  edges:')
    if (!inEdges) continue
    const kind = /^ {4}- kind: (.+)$/.exec(line)
    if (kind) {
      row.edges.push({ kind: kind[1] as string, to: '' })
      continue
    }
    const to = /^ {6}to: (.+)$/.exec(line)
    const last = row.edges[row.edges.length - 1]
    if (to && last) last.to = to[1] as string
  }
  if (row) found.push(row)
  return found
}

const ROWS = rows()

function named(from: string, kind: string): Set<string> {
  return new Set(
    ROWS.filter((row) => row.type === from)
      .flatMap((row) => row.edges)
      .filter((edge) => edge.kind === kind)
      .map((edge) => edge.to),
  )
}

function namesOf(...types: string[]): string[] {
  return ROWS.filter((row) => types.includes(row.type)).map((row) => row.name)
}

describe('the concept map', () => {
  it('the file reads back as rows', () => {
    expect(ROWS.length).toBeGreaterThan(40)
    expect(ROWS.filter((row) => row.name === '')).toEqual([])
  })

  it('every requirement is realized by a part', () => {
    const built = named('Mechanism', 'realizes')
    expect(namesOf('Requirement').filter((name) => !built.has(name))).toEqual([])
  })

  it('every case and every failure is covered by a check', () => {
    const covered = named('Verification', 'covers')
    expect(namesOf('Case', 'Failure').filter((name) => !covered.has(name))).toEqual([])
  })

  it('every failure is handled by a part', () => {
    const handled = named('Mechanism', 'handles')
    expect(namesOf('Failure').filter((name) => !handled.has(name))).toEqual([])
  })
})
