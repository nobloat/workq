CREATE TABLE IF NOT EXISTS workq (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name       TEXT NOT NULL,
  param      TEXT NOT NULL,
  queue      TEXT NOT NULL DEFAULT '',
  scheduled  BOOLEAN NOT NULL DEFAULT FALSE,
  run_at     BIGINT NOT NULL,
  locked_at  BIGINT,
  version    BIGINT NOT NULL DEFAULT 0,
  error      TEXT,
  failed_at  BIGINT,
  created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS workq_due ON workq (queue, run_at, id) WHERE failed_at IS NULL;
CREATE INDEX IF NOT EXISTS workq_due_name ON workq (queue, name, run_at, id) WHERE failed_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS workq_scheduled ON workq (name) WHERE scheduled;
