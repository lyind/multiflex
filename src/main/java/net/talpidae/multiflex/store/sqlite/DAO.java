package net.talpidae.multiflex.store.sqlite;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import net.talpidae.multiflex.store.StoreException;

import java.nio.ByteBuffer;
import java.util.UUID;


class DAO
{
    private final SQLiteConnection db;


    DAO(SQLiteConnection db)
    {
        this.db = db;
    }


    /**
     * Start a transaction.
     */
    void beginTransaction() throws SQLiteException
    {
        db.exec("BEGIN TRANSACTION");
    }


    /**
     * Commit a transaction.
     */
    void commit() throws SQLiteException
    {
        db.exec("COMMIT");
    }


    /**
     * Rollback a transaction.
     */
    void rollback()
    {
        try
        {
            db.exec("ROLLBACK");
        }
        catch (SQLiteException e)
        {
            // ignore
        }
    }


    /**
     * Set schema version.
     */
    void replaceVersion(int version) throws SQLiteException
    {
        final SQLiteStatement insertOrReplaceVersion = db.prepare("INSERT OR REPLACE INTO meta (\"key\", \"value\") VALUES ('version', ?)", true);
        try
        {
            insertOrReplaceVersion.bind(1, version);
            insertOrReplaceVersion.step();
        }
        finally
        {
            insertOrReplaceVersion.dispose();
        }
    }


    /**
     * Get store UUID.
     *
     * @return Store UUID or null if none has been set.
     */
    UUID selectStoreId() throws SQLiteException
    {
        final SQLiteStatement insertOrReplaceVersion = db.prepare("SELECT \"value\" FROM meta WHERE \"key\" = 'id'", false);
        try
        {
            if (insertOrReplaceVersion.step())
            {
                final String id = insertOrReplaceVersion.columnString(0);
                if (id != null)
                {
                    return UUID.fromString(id);
                }
            }

            return null;
        }
        finally
        {
            insertOrReplaceVersion.dispose();
        }
    }


    /**
     * Insert store UUID.
     */
    void insertOrReplaceStoreId(UUID storeId) throws SQLiteException
    {
        final SQLiteStatement insertOrReplaceStoreId = db.prepare("INSERT OR REPLACE INTO meta (\"key\", \"value\") VALUES ('id', ?)", true);
        try
        {
            insertOrReplaceStoreId.bind(1, storeId.toString());
            insertOrReplaceStoreId.step();
        }
        finally
        {
            insertOrReplaceStoreId.dispose();
        }
    }


    /**
     * Query the schema version. Not cached since it is rarely needed.
     */
    int selectVersion() throws SQLiteException
    {
        final SQLiteStatement selectTableMetaExists = db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'", false);
        try
        {
            if (selectTableMetaExists.step())
            {
                final SQLiteStatement selectVersion = db.prepare("SELECT \"value\" FROM meta WHERE \"key\" = 'version'", false);
                try
                {
                    return selectVersion.step() ? selectVersion.columnInt(0) : 0;
                }
                finally
                {
                    selectVersion.dispose();
                }
            }

            return 0;
        }
        finally
        {
            selectTableMetaExists.dispose();
        }
    }


    /**
     * Insert descriptor if not exists and select its ID.
     */
    long insertDescriptorAndSelectId(SQLiteDescriptor descriptor) throws SQLiteException, StoreException
    {
        final SQLiteStatement insertOrIgnoreDescriptor = db.prepare("INSERT OR IGNORE INTO track_descriptor (\"descriptor\") VALUES (?)", true);
        try
        {
            final byte[] descriptorBytes = descriptor.encode();

            final long previousLastId = db.getLastInsertId();
            insertOrIgnoreDescriptor.bind(1, descriptorBytes);
            insertOrIgnoreDescriptor.stepThrough();

            final long lastId = db.getLastInsertId();
            if (lastId != previousLastId)
                return lastId;  // already got the ID

            // get the ID the hard way (should be cached though)
            final SQLiteStatement selectDescriptorId = db.prepare("SELECT id FROM track_descriptor WHERE descriptor = (?)", true);
            try
            {
                selectDescriptorId.bind(1, descriptorBytes);
                if (selectDescriptorId.step())
                {
                    final long id = selectDescriptorId.columnLong(0);
                    if (id != 0)
                    {
                        return id;
                    }
                }

                throw new StoreException("failed to query ID of descriptor");
            }
            finally
            {
                selectDescriptorId.dispose();
            }
        }
        finally
        {
            insertOrIgnoreDescriptor.dispose();
        }
    }


    /**
     * Insert a track chunk.
     */
    void insertOrReplaceTrackChunk(long timestamp, SQLiteDescriptor descriptor, ByteBuffer chunk) throws SQLiteException
    {
        final SQLiteStatement insertOrReplaceChunk = db.prepare("INSERT OR REPLACE INTO track (\"ts\", \"descriptor_id\", \"chunk\") VALUES (?, ?, ?)", true);
        try
        {
            insertOrReplaceChunk.bind(1, timestamp);
            insertOrReplaceChunk.bind(2, descriptor.getId());
            insertOrReplaceChunk.bind(3, chunk.array());
            insertOrReplaceChunk.stepThrough();
        }
        finally
        {
            insertOrReplaceChunk.dispose();
        }
    }
}
