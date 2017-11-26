-- Stores additional meta data
-- Pre-defined entries:
--   version -> integer which defined the format version
--   epochMillies -> store epoch microseconds since UNIX epoch (all timestamps are relative to this point in time)
CREATE TABLE meta (
  key   TEXT PRIMARY KEY NOT NULL,
  value TEXT             NOT NULL
)
  WITHOUT ROWID;

-- Stores data descriptors
CREATE TABLE stream_descriptor (
  id         INTEGER PRIMARY KEY NOT NULL, -- alias for rowid
  descriptor BLOB                NOT NULL
)
  WITHOUT ROWID;

-- Stores actual data as compressed int[] in a blob
CREATE TABLE stream (
  ts            INTEGER PRIMARY KEY NOT NULL, -- ts is the microseconds since this store's epoch
  descriptor_id INTEGER             NOT NULL,
  chunk         BLOB                NOT NULL
)
  WITHOUT ROWID;
