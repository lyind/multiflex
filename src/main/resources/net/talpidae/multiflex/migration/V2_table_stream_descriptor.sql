-- Stores data descriptors
CREATE TABLE IF NOT EXISTS stream_descriptor (
  id         INTEGER PRIMARY KEY NOT NULL, -- alias for rowid
  descriptor BLOB                NOT NULL
)
  WITHOUT ROWID;
