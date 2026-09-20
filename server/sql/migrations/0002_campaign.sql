-- The campaign. Change these numbers, or add a file after this one, and the
-- next boot applies it.
--
-- The window opens in the past and closes in 2036, so a fresh clone finds the
-- sale already open. DO NOTHING protects a restored dump, which can carry the
-- row without the ledger entry that says this file ran.

INSERT INTO stock (id, total_units, units_left, start_at, end_at)
VALUES (1, 1000, 1000, '2026-01-01T00:00:00Z', '2036-01-01T00:00:00Z')
ON CONFLICT (id) DO NOTHING;
