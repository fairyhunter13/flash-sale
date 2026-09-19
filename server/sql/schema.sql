-- Two tables, and the sale is decided inside them. `stock` holds the one row
-- every buyer competes for. `orders` holds one row for each buyer who won.

CREATE TABLE IF NOT EXISTS stock (
  id         int         PRIMARY KEY,
  units_left int         NOT NULL,
  start_at   timestamptz NOT NULL,
  end_at     timestamptz NOT NULL,
  -- One sale, so one row. A second row cannot be written at all.
  CONSTRAINT stock_single_row CHECK (id = 1),
  -- The third guard. The gate already refuses to go below 0, so this turns a
  -- future defect into a failed transaction and never into a sold unit.
  CONSTRAINT stock_never_negative CHECK (units_left >= 0)
);

CREATE TABLE IF NOT EXISTS orders (
  id         bigserial   PRIMARY KEY,
  user_id    text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  -- The second guard. A defect in the gate still cannot write a buyer twice.
  CONSTRAINT orders_user_id_key UNIQUE (user_id)
);

-- user_id is text, and not an integer, because the buyer types a username or an email.
