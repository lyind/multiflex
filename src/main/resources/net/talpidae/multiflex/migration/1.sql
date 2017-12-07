-- Stores additional meta data
-- Pre-defined entries:
--   ID -> this stores UUID
--   VERSION -> integer which defined the format version
--   EPOCH_MICROS -> store epoch microseconds since UNIX epoch
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY NOT NULL,
  value TEXT             NOT NULL
)
  WITHOUT ROWID;