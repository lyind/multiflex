/*
 * Copyright (C) 2017  Jonas Zeiger <jonas.zeiger@talpidae.net>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
     * <p>
     * This also checks if the meta table already exists and returns schema version 0 otherwise.
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
     * Query the schema version. Not cached since it is rarely needed.
     */
    String selectMeta(String key) throws SQLiteException
    {
        final SQLiteStatement selectMeta = db.prepare("SELECT \"value\" FROM meta WHERE \"key\" = ?", true);
        try
        {
            selectMeta.bind(1, key);
            return selectMeta.step() ? selectMeta.columnString(0) : null;
        }
        finally
        {
            selectMeta.dispose();
        }
    }


    /**
     * Insert or replace a value for the meta field with the specified key.
     */
    void insertOrReplaceMeta(String key, String value) throws SQLiteException
    {
        final SQLiteStatement insertOrReplaceMeta = db.prepare("INSERT OR REPLACE INTO meta (\"key\", \"value\") VALUES (?, ?)", true);
        try
        {
            insertOrReplaceMeta.bind(1, key);
            insertOrReplaceMeta.bind(2, value);
            insertOrReplaceMeta.step();
        }
        finally
        {
            insertOrReplaceMeta.dispose();
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
            final ByteBuffer buffer = descriptor.encode();
            buffer.flip();

            final long previousLastId = db.getLastInsertId();
            insertOrIgnoreDescriptor.bind(1, buffer.array(), 0, buffer.remaining());
            insertOrIgnoreDescriptor.stepThrough();

            final long lastId = db.getLastInsertId();
            if (lastId != previousLastId)
                return lastId;  // already got the ID

            // get the ID the hard way (should be cached though)
            final SQLiteStatement selectDescriptorId = db.prepare("SELECT id FROM track_descriptor WHERE descriptor = (?)", true);
            try
            {
                selectDescriptorId.bind(1, buffer.array(), 0, buffer.remaining());
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
     * Get a descriptor by ID.
     */
    SQLiteDescriptor selectDescriptor(long id, UUID storeId) throws SQLiteException, StoreException
    {
        final SQLiteStatement selectDescriptor = db.prepare("SELECT \"descriptor\" FROM track_descriptor WHERE id = ?", true);
        try
        {
            selectDescriptor.bind(1, id);
            if (selectDescriptor.step())
            {
                final byte[] descriptorData = selectDescriptor.columnBlob(0);

                return SQLiteDescriptor.decode(ByteBuffer.wrap(descriptorData), id, storeId);
            }
            else
            {
                return null;
            }
        }
        finally
        {
            selectDescriptor.dispose();
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

    /**
     * Find a chunk by timestamp.
     * <p>
     * Call this within a transaction.
     */
    SQLiteChunk selectChunkByTimestamp(long timestamp, DescriptorByIdFunction descriptorById) throws SQLiteException, StoreException
    {
        final SQLiteStatement selectChunkByTimestamp = db.prepare("SELECT ts, descriptor_id, chunk FROM track WHERE ts = ?", true);
        try
        {
            selectChunkByTimestamp.bind(1, timestamp);
            if (selectChunkByTimestamp.step())
            {

                final long ts = selectChunkByTimestamp.columnLong(0);
                final long descriptorId = selectChunkByTimestamp.columnLong(1);

                // lookup descriptor by id
                final SQLiteDescriptor descriptor = descriptorById.findDescriptor(descriptorId);
                if (descriptor != null)
                {
                    return new SQLiteChunk(descriptor, ts, ByteBuffer.wrap(selectChunkByTimestamp.columnBlob(2)));
                }
            }

            return null;
        }
        finally
        {
            selectChunkByTimestamp.dispose();
        }
    }


    @FunctionalInterface
    public interface DescriptorByIdFunction
    {
        /**
         * Accept a DB query result in form of a SQLiteStatement.
         */
        SQLiteDescriptor findDescriptor(long id) throws StoreException;
    }
}
