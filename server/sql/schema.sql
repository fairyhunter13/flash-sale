-- Three tables. `stock` holds the one row every buyer competes for. `orders`
-- holds one row for each buyer who won. `queue_offsets` holds how far the
-- workers have read, in the same database as the rows, so a record cannot be
-- marked read without its order row.

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
  -- The buyer's place in the queue, issued by one Redis INCR. `id` records the
  -- order the rows landed in, which is not the order the buyers arrived in as
  -- soon as more than one worker writes. ORDER BY seq reads arrival order.
  seq        bigint,
  created_at timestamptz NOT NULL DEFAULT now(),
  -- The second guard. A defect in the gate still cannot write a buyer twice.
  CONSTRAINT orders_user_id_key UNIQUE (user_id),
  -- No two buyers hold one place, so first come first serve stays readable
  -- however many workers wrote the rows.
  CONSTRAINT orders_seq_key UNIQUE (seq)
);

CREATE TABLE IF NOT EXISTS queue_offsets (
  topic       text   NOT NULL,
  partition   int    NOT NULL,
  next_offset bigint NOT NULL,
  PRIMARY KEY (topic, partition)
);

-- user_id is text, and not an integer, because the buyer types a username or an email.
