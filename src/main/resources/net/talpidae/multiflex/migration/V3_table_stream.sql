-- Stores actual data as compressed int[] in a blob
CREATE TABLE IF NOT EXISTS stream (
  ts            INTEGER PRIMARY KEY NOT NULL, -- ts is the seconds since epochMillies
  descriptor_id INTEGER             NOT NULL,
  chunk         BLOB                NOT NULL
)
  WITHOUT ROWID;
