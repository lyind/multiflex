-- Store meta data
-- Pre-defined entries:
--   ID -> store UUID
--   VERSION -> format version
--   EPOCH_MICROS -> store epoch microseconds since UNIX epoch
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY NOT NULL,
  value TEXT             NOT NULL
)
  WITHOUT ROWID;

-- Stores track descriptors
CREATE TABLE IF NOT EXISTS track_descriptor (
  id         INTEGER PRIMARY KEY NOT NULL, -- alias for rowid
  descriptor BLOB UNIQUE         NOT NULL
);

-- Stores actual data as compressed int[] in BLOB column "chunk"
CREATE TABLE IF NOT EXISTS track (
  ts            INTEGER PRIMARY KEY NOT NULL, -- ts is the seconds since EPOCH_MICROS
  descriptor_id INTEGER             NOT NULL,
  chunk         BLOB                NOT NULL
)
  WITHOUT ROWID;