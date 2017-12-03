-- Stores track descriptors
CREATE TABLE IF NOT EXISTS track_descriptor (
  id         INTEGER PRIMARY KEY NOT NULL, -- alias for rowid
  descriptor BLOB UNIQUE         NOT NULL
);