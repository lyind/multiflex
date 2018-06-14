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
import net.talpidae.multiflex.format.Chunk;
import net.talpidae.multiflex.store.Store;
import net.talpidae.multiflex.store.StoreException;
import net.talpidae.multiflex.store.base.DAO;
import net.talpidae.multiflex.store.base.Transaction;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class AlmworksSqliteDAO implements DAO
{
    private final SQLiteConnection db;

    private final Transaction transaction;


    public AlmworksSqliteDAO(File dbFile)
    {
        this.db = new SQLiteConnection(dbFile);
        this.transaction = new AlmworksSqliteTransaction();
    }


    /**
     * Get the transaction control object.
     */
    @Override
    public Transaction getTransaction() throws StoreException
    {
        return transaction.begin();
    }

    @Override
    public void open(boolean writable) throws StoreException
    {
        // make sure binary libraries are in place
        Library.deployAndConfigure();

        try
        {
            if (writable)
            {
                db.open(true);
            }
            else
            {
                db.openReadonly();
            }
        }
        catch (SQLiteException e)
        {
            throw new StoreException("failed to open " + db.getDatabaseFile().getAbsolutePath()
                    + " in " + (writable ? "read-write" : "read-only") + " mode", e);
        }
    }


    @Override
    public void execMigration(int currentVersion, int targetVersion, String sqlMigration) throws StoreException
    {
        try
        {
            db.exec(sqlMigration);
        }
        catch (SQLiteException e)
        {
            throw new StoreException("failed to execute migration from version " + currentVersion + " -> " + targetVersion, e);
        }
    }


    /**
     * Query the schema version. Not cached since it is rarely needed.
     * <p>
     * This also checks if the meta table already exists and returns schema version 0 otherwise.
     */
    @Override
    public int selectVersion() throws StoreException
    {
        try
        {
            final SQLiteStatement selectTableMetaExists = db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'", false);
            try
            {
                if (selectTableMetaExists.step())
                {
                    final String versionValue = selectMeta(Store.ReservedMetaKey.VERSION.name());
                    if (versionValue != null)
                    {
                        try
                        {
                            return Integer.parseInt(versionValue);
                        }
                        catch (NumberFormatException e)
                        {
                            return 0;
                        }
                    }
                }

                return 0;
            }
            finally
            {
                selectTableMetaExists.dispose();
            }
        }
        catch (SQLiteException e)
        {
            throw new StoreException("failed to find store version", e);
        }
    }

    /**
     * Query the schema version. Not cached since it is rarely needed.
     */
    @Override
    public String selectMeta(String key) throws StoreException
    {
        try
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
        catch (SQLiteException e)
        {
            throw new StoreException("failed to find meta value for key: " + key, e);
        }
    }

    /**
     * Insert or replace a value for the meta field with the specified key.
     */
    @Override
    public void insertOrReplaceMeta(String key, String value) throws StoreException
    {
        try
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
        catch (SQLiteException e)
        {
            throw new StoreException("failed to write meta value for key: " + key, e);
        }
    }

    /**
     * Insert descriptor if not exists and select its ID.
     */
    @Override
    public long insertDescriptorAndSelectId(ByteBuffer descriptor) throws StoreException
    {
        try
        {
            final SQLiteStatement insertOrIgnoreDescriptor = db.prepare("INSERT OR IGNORE INTO track_descriptor (\"descriptor\") VALUES (?)", true);
            try
            {
                final long previousLastId = db.getLastInsertId();
                insertOrIgnoreDescriptor.bind(1, descriptor.array(), 0, descriptor.remaining());
                insertOrIgnoreDescriptor.stepThrough();

                final long lastId = db.getLastInsertId();
                if (lastId != previousLastId)
                    return lastId;  // already got the ID

                // get the ID the hard way (should be cached though)
                final SQLiteStatement selectDescriptorId = db.prepare("SELECT id FROM track_descriptor WHERE descriptor = (?)", true);
                try
                {
                    selectDescriptorId.bind(1, descriptor.array(), 0, descriptor.remaining());
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
        catch (SQLiteException e)
        {
            throw new StoreException("failed to write descriptor", e);
        }
    }

    /**
     * Get a descriptor by ID.
     */
    @Override
    public ByteBuffer selectDescriptor(long id) throws StoreException
    {
        try
        {
            final SQLiteStatement selectDescriptor = db.prepare("SELECT \"descriptor\" FROM track_descriptor WHERE id = ?", true);
            try
            {
                selectDescriptor.bind(1, id);
                if (selectDescriptor.step())
                {
                    final byte[] descriptorBytes = selectDescriptor.columnBlob(0);
                    if (descriptorBytes != null)
                    {
                        return ByteBuffer.wrap(descriptorBytes);
                    }
                }

                throw new StoreException("no descriptor found for id: " + id);
            }
            finally
            {
                selectDescriptor.dispose();
            }
        }
        catch (SQLiteException e)
        {
            throw new StoreException("failed to find descriptor with id: " + id, e);
        }
    }

    /**
     * Find the maximum track chunk timestamp.
     *
     * @return The maximum timestamp of any chunk contained in this store. -1 if no timestamp has been found (no chunks).
     */
    @Override
    public long selectMaxChunkTimestamp() throws StoreException
    {
        try
        {
            final SQLiteStatement selectMaxChunkTimestamp = db.prepare("SELECT ifnull(MAX(\"ts\"), -1) FROM track", true);
            try
            {
                return selectMaxChunkTimestamp.step() ? selectMaxChunkTimestamp.columnLong(0) : -1;
            }
            finally
            {
                selectMaxChunkTimestamp.dispose();
            }
        }
        catch (SQLiteException e)
        {
            throw new StoreException("failed to select max chunk timestamp", e);
        }
    }

    /**
     * Insert a track chunk.
     */
    @Override
    public void insertOrReplaceTrackChunk(long timestamp, long descriptorId, ByteBuffer data) throws StoreException
    {
        try
        {
            final SQLiteStatement insertOrReplaceChunk = db.prepare("INSERT OR REPLACE INTO track (\"ts\", \"descriptor_id\", \"chunk\") VALUES (?, ?, ?)", true);
            try
            {
                if (!data.hasArray())
                {
                    throw new IllegalStateException("can only handle HeapByteBuffer data right now");
                }

                insertOrReplaceChunk.bind(1, timestamp);
                insertOrReplaceChunk.bind(2, descriptorId);
                insertOrReplaceChunk.bind(3, data.array(), data.arrayOffset() + data.position(), data.remaining());
                insertOrReplaceChunk.stepThrough();
            }
            finally
            {
                insertOrReplaceChunk.dispose();
            }
        }
        catch (SQLiteException e)
        {
            throw new StoreException("failed to select chunk by timestamp range", e);
        }
    }

    /**
     * Find all chunks that lie within the specified timestamp (seconds since epochMillies in table meta).
     * <p>
     * Call this within a transaction.
     *
     * @param tsBegin      The begin of the range (inclusive)
     * @param tsEnd        The upper limit of the range (exclusive)
     * @param chunkFactory Function that get a descriptor by the specified descriptor ID
     * @return A list of Chunks constructed from the located DB entries, empty list if none were found
     */
    @Override
    public List<Chunk> selectChunksByTimestampRange(long tsBegin, long tsEnd, ChunkFactory chunkFactory) throws StoreException
    {
        try
        {
            final SQLiteStatement selectChunksByTimestampRange = db.prepare("SELECT ts, descriptor_id, chunk FROM track WHERE ts >= ? AND ts < ?", true);
            try
            {
                selectChunksByTimestampRange.bind(1, tsBegin);
                selectChunksByTimestampRange.bind(2, tsEnd);

                final ArrayList<Chunk> list = new ArrayList<>();
                while (selectChunksByTimestampRange.step())
                {
                    final long timestamp = selectChunksByTimestampRange.columnLong(0);
                    final long descriptorId = selectChunksByTimestampRange.columnLong(1);
                    final ByteBuffer data = ByteBuffer.wrap(selectChunksByTimestampRange.columnBlob(2));

                    list.add(chunkFactory.createChunk(timestamp, descriptorId, data));
                }

                if (list.isEmpty())
                {
                    return Collections.emptyList();
                }

                list.trimToSize();

                return list;
            }
            finally
            {
                selectChunksByTimestampRange.dispose();
            }
        }
        catch (SQLiteException e)
        {
            throw new StoreException("failed to select chunk by timestamp range", e);
        }
    }

    /**
     * Find a chunk by timestamp.
     * <p>
     * Call this within a transaction.
     */
    @Override
    public Chunk selectChunkByTimestamp(long timestamp, ChunkFactory chunkFactory) throws StoreException
    {
        try
        {
            final SQLiteStatement selectChunkByTimestamp = db.prepare("SELECT descriptor_id, chunk FROM track WHERE ts = ?", true);
            try
            {
                selectChunkByTimestamp.bind(1, timestamp);
                if (selectChunkByTimestamp.step())
                {
                    final long descriptorId = selectChunkByTimestamp.columnLong(0);
                    final ByteBuffer data = ByteBuffer.wrap(selectChunkByTimestamp.columnBlob(1));

                    return chunkFactory.createChunk(timestamp, descriptorId, data);
                }

                return null;
            }
            finally
            {
                selectChunkByTimestamp.dispose();
            }
        }
        catch (SQLiteException e)
        {
            throw new StoreException("failed to select chunk by timestamp", e);
        }
    }


    @Override
    public void close() throws StoreException
    {
        db.dispose();
    }


    private class AlmworksSqliteTransaction extends Transaction
    {
        @Override
        protected void transactionBegin() throws StoreException
        {
            try
            {
                db.exec("BEGIN TRANSACTION");
            }
            catch (SQLiteException e)
            {
                throw new StoreException("begin transaction failed", e);
            }
        }

        @Override
        protected void transactionCommit() throws StoreException
        {
            try
            {
                db.exec("COMMIT");
            }
            catch (SQLiteException e)
            {
                throw new StoreException("commit failed", e);
            }
        }

        @Override
        protected void transactionRollback() throws StoreException
        {
            try
            {
                db.exec("ROLLBACK");
            }
            catch (SQLiteException e)
            {
                throw new StoreException("rollback failed", e);
            }
        }
    }
}
