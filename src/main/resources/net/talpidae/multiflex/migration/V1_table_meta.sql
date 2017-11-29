-- Stores additional meta data
-- Pre-defined entries:
--   version -> integer which defined the format version
--   epochMillies -> store epoch microseconds since UNIX epoch
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY NOT NULL,
  value TEXT             NOT NULL
)
  WITHOUT ROWID;
