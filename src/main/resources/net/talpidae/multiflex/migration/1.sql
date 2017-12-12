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

-- Stores track descriptors
CREATE TABLE IF NOT EXISTS track_descriptor (
  id         INTEGER PRIMARY KEY NOT NULL, -- alias for rowid
  descriptor BLOB UNIQUE         NOT NULL
);

-- Stores actual data as compressed int[] in a blob
CREATE TABLE IF NOT EXISTS track (
  ts            INTEGER PRIMARY KEY NOT NULL, -- ts is the seconds since epochMillies
  descriptor_id INTEGER             NOT NULL,
  chunk         BLOB                NOT NULL
)
  WITHOUT ROWID;