-- `queue_offsets` is here, and not in Kafka, so a worker commits how far it
-- read in the same transaction as the order row it wrote.

CREATE TABLE IF NOT EXISTS stock (
  id          int         PRIMARY KEY,
  -- The units the campaign started with. `units_left` alone cannot answer a
  -- restart, because the process needs the total to know what Redis may hand out.
  total_units int         NOT NULL,
  units_left  int         NOT NULL,
  start_at    timestamptz NOT NULL,
  end_at      timestamptz NOT NULL,
  -- One sale, so one row. A second row cannot be written at all.
  CONSTRAINT stock_single_row CHECK (id = 1),
  -- The third guard. The gate already refuses to go below 0, so this turns a
  -- future defect into a failed transaction and never into a sold unit.
  CONSTRAINT stock_never_negative CHECK (units_left >= 0),
  CONSTRAINT stock_never_over_total CHECK (units_left <= total_units),
  -- A campaign that ends before it starts is never open, and the page would
  -- report `closed` with no reason a reader can see.
  CONSTRAINT stock_window_ordered CHECK (end_at > start_at)
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
