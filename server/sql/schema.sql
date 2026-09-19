-- The unique index is the second guard. The Redis script is the first one.
-- They fail apart, so one bug cannot reach this table.

CREATE TABLE IF NOT EXISTS orders (
  id         bigserial   PRIMARY KEY,
  user_id    text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT orders_user_id_key UNIQUE (user_id)
);

-- user_id is text, and not an integer, because the buyer types a username or an email.
