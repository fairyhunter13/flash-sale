/**
 * Sets the sale window and the unit count, so neither one needs a SQL edit.
 *
 *   npm run sale:window -- --start 2026-10-01T09:00:00Z --end 2026-10-01T10:00:00Z --units 1000
 *
 * The running server picks the change up within one sweep, which is 250 ms.
 */
import { Pool } from 'pg'
import { readConfig } from '../config.ts'

const USAGE =
  'npm run sale:window -- --start <ISO time> --end <ISO time> [--units <whole number>]'

type Wanted = { startAt: Date; endAt: Date; units: number | undefined }

function readFlags(argv: readonly string[]): Wanted {
  const flags = new Map<string, string>()
  for (let at = 0; at < argv.length; at += 2) {
    const name = argv[at]
    const value = argv[at + 1]
    if (name === undefined || !name.startsWith('--') || value === undefined) {
      throw new Error(`I cannot read the arguments. Use:\n  ${USAGE}`)
    }
    flags.set(name.slice(2), value)
  }

  const startAt = readTime(flags.get('start'), 'start')
  const endAt = readTime(flags.get('end'), 'end')
  if (endAt <= startAt) throw new Error('--end must come after --start.')

  const rawUnits = flags.get('units')
  if (rawUnits === undefined) return { startAt, endAt, units: undefined }
  const units = Number(rawUnits)
  if (!Number.isSafeInteger(units) || units <= 0) {
    throw new Error(`--units is ${rawUnits}. It must be a whole number above 0.`)
  }
  return { startAt, endAt, units }
}

function readTime(raw: string | undefined, name: string): Date {
  if (raw === undefined) throw new Error(`--${name} is not set. Use:\n  ${USAGE}`)
  const at = new Date(raw)
  if (Number.isNaN(at.getTime())) {
    throw new Error(`--${name} is ${raw}. It must be an ISO time, for example 2026-10-01T09:00:00Z.`)
  }
  return at
}

async function main(): Promise<void> {
  const wanted = readFlags(process.argv.slice(2))
  const pool = new Pool({ connectionString: readConfig().databaseUrl, max: 1 })
  try {
    // A change to the total resets what is left. An operator who only moves the window
    // keeps the count that the sale already reached.
    const { rows } = await pool.query<{ total_units: number; units_left: number }>(
      wanted.units === undefined
        ? `UPDATE stock SET start_at = $1, end_at = $2 WHERE id = 1
             RETURNING total_units, units_left`
        : `UPDATE stock SET start_at = $1, end_at = $2, total_units = $3, units_left = $3
             WHERE id = 1 RETURNING total_units, units_left`,
      wanted.units === undefined
        ? [wanted.startAt, wanted.endAt]
        : [wanted.startAt, wanted.endAt, wanted.units],
    )
    const row = rows[0]
    if (row === undefined) throw new Error('the stock row is missing. Run npm start first.')

    console.log(
      `The sale runs ${wanted.startAt.toISOString()} to ${wanted.endAt.toISOString()}, ` +
        `with ${row.units_left} of ${row.total_units} units left.`,
    )
    if (wanted.units !== undefined) {
      console.log('The unit count moved. The old order rows hold the last sale, so a second campaign needs: npm run reset')
    }
  } finally {
    await pool.end()
  }
}

main().catch((error: unknown) => {
  console.error((error as Error).message)
  process.exit(1)
})
