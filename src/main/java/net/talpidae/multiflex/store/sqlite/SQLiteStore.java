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
import net.talpidae.multiflex.format.Chunk;
import net.talpidae.multiflex.format.Descriptor;
import net.talpidae.multiflex.store.Store;
import net.talpidae.multiflex.store.StoreException;

import java.io.File;
import java.util.List;
import java.util.UUID;


public class SQLiteStore implements Store
{
    private final SQLiteDescriptorCache descriptorCache;

    private final SQLiteConnection db;

    private final Transaction transaction;

    private final DAO dao;

    /**
     * The opened stores unique ID.
     */
    private UUID id;

    private boolean inTransaction = false;

    private volatile State state = State.INITIAL;

    private int schemaVersion = 0;

    public SQLiteStore(File file)
    {
        db = new SQLiteConnection(file);
        descriptorCache = new SQLiteDescriptorCache(this);

        // simple DAO for our format's tables
        dao = new DAO(db);

        // implement single level transaction
        transaction = new SQLiteStoreTransaction();
    }

    @Override
    public Store open(boolean writable) throws StoreException
    {
        synchronized (this)
        {
            if (state != State.INITIAL)
                throw new StoreException("store has already been opened");

            // make sure binary libraries are in place
            Library.deployAndConfigure();

            try
            {
                if (writable)
                {
                    db.open(true);
                    state = State.OPEN_READWRITE;
                }
                else
                {
                    db.openReadonly();
                    state = State.OPEN_READONLY;
                }
            }
            catch (SQLiteException e)
            {
                state = State.CLOSED;
                throw new StoreException(e.getMessage(), e);
            }

            // DB is open now, initialize
            try
            {
                if (state == State.OPEN_READWRITE)
                {
                    schemaVersion = updateSchema();
                }
                else
                {
                    // read-only, just check if the schema is present
                    schemaVersion = validateSchema();
                }
            }
            catch (IllegalStateException | StoreException e)
            {
                final StoreException relayed = e instanceof StoreException
                        ? (StoreException) e
                        : new StoreException(e.getMessage(), e);
                try
                {
                    close();
                }
                catch (StoreException e1)
                {
                    relayed.addSuppressed(e1);
                }

                throw relayed;
            }

            return this;
        }
    }

    @Override
    public void put(long ts, Chunk chunk) throws StoreException
    {
        if (state != State.OPEN_READWRITE)
        {
            throw new StoreException("store not writable");
        }

        if (!(chunk instanceof SQLiteChunk))
        {
            throw new StoreException("unsupported chunk implementation: " + chunk.getClass().getName());
        }

        final SQLiteChunk actualChunk = (SQLiteChunk) chunk;
        transact(() ->
        {
            descriptorCache.intern(actualChunk.getDescriptor());

            actualChunk.persist(dao);

            return null;
        });
    }

    @Override
    public Chunk findByTimestamp(long ts) throws StoreException
    {
        return transact(() ->
        {
            try
            {
                return dao.selectChunkByTimestamp(ts, descriptorCache::get);
            }
            catch (SQLiteException e)
            {
                throw new StoreException("failed to read chunk for timestamp " + ts);
            }
        });
    }

    @Override
    public List<Chunk> findByTimestampRange(long tsFirst, long tsLast) throws StoreException
    {
        return transact(() ->
        {
            try
            {
                return dao.selectChunksByTimestampRange(tsFirst, tsLast, descriptorCache::get);
            }
            catch (SQLiteException e)
            {
                throw new StoreException("failed to read chunks for timestamps between " + tsFirst + " and " + tsLast
                        + ": " + e.getMessage(), e);
            }
        });
    }

    @Override
    public long getEpoch() throws StoreException
    {
        final String epochValue = transact(() ->
        {
            try
            {
                return dao.selectMeta(ReservedMetaKey.EPOCH_MICROS.name());
            }
            catch (SQLiteException e)
            {
                throw new StoreException("failed to get store epoch: " + e.getMessage(), e);
            }
        });

        if (epochValue != null)
        {
            try
            {
                return Long.parseLong(epochValue);
            }
            catch (NumberFormatException e)
            {
                throw new StoreException("invalid epoch: \"" + epochValue + "\": " + e.getMessage(), e);
            }
        }

        return -1;
    }

    @Override
    public void setEpoch(long epochMicros) throws StoreException
    {
        if (epochMicros < 0)
        {
            throw new IllegalArgumentException("negative epoch specified: " + epochMicros);
        }

        transact(() ->
        {
            try
            {
                final String existingEpoch = dao.selectMeta(ReservedMetaKey.EPOCH_MICROS.name());
                if (existingEpoch != null)
                {
                    throw new IllegalStateException("epoch has already been set and can only be set once");
                }

                dao.insertOrReplaceMeta(ReservedMetaKey.EPOCH_MICROS.name(), Long.toString(epochMicros));

                return null;
            }
            catch (SQLiteException e)
            {
                throw new StoreException("failed to set store epoch: " + e.getMessage(), e);
            }
        });
    }

    @Override
    public long findMaxTimestamp() throws StoreException
    {
        return transact(() ->
        {
            try
            {
                return dao.selectMaxChunkTimestamp();
            }
            catch (SQLiteException e)
            {
                throw new StoreException("failed find maximum chunk timestamp");
            }
        });
    }

    @Override
    public String getMeta(String key) throws StoreException
    {
        return transact(() ->
        {
            try
            {
                return dao.selectMeta(key);
            }
            catch (SQLiteException e)
            {
                throw new StoreException("failed to get meta value with key: " + key + ": " + e.getMessage(), e);
            }
        });
    }

    @Override
    public void putMeta(String key, String value) throws StoreException
    {
        if (ReservedMetaKey.names.contains(key))
        {
            throw new IllegalArgumentException("reserved keys ("
                    + String.join(", ", ReservedMetaKey.names)
                    + " can't be set, offending key: " + key);
        }

        transact(() ->
        {
            try
            {
                dao.insertOrReplaceMeta(key, value);
            }
            catch (SQLiteException e)
            {
                throw new StoreException("failed to put meta value: key=" + key + ", value: " + value + ": " + e.getMessage(), e);
            }

            return null;
        });
    }

    @Override
    public Descriptor.Builder descriptorBuilder()
    {
        return new SQLiteDescriptor.Builder(this);
    }

    @Override
    public Chunk.Builder chunkBuilder(Descriptor descriptor)
    {
        if (!(descriptor instanceof SQLiteDescriptor))
        {
            throw new IllegalArgumentException("incompatible descriptor implementation");
        }

        return new SQLiteChunk.Builder((SQLiteDescriptor) descriptor);
    }

    /**
     * Get the store's unique ID.
     */
    @Override
    public UUID getId()
    {
        return id;
    }

    @Override
    public int getVersion()
    {
        return schemaVersion;
    }

    @Override
    public void close() throws StoreException
    {
        synchronized (this)
        {
            switch (state)
            {
                case OPEN_READWRITE:
                case OPEN_READONLY:
                {
                    descriptorCache.clear();

                    db.dispose();
                    break;
                }

                default:
                    break;
            }

            state = State.CLOSED;
        }
    }

    /**
     * Get the stores DAO.
     */
    DAO getDao()
    {
        return dao;
    }

    /**
     * Run a TransactionalTask inside a new DB transaction.
     * <p>
     * The transaction is automatically rolled back on any exception thrown from the task.
     */
    private <T> T transact(TransactionalTask<T> task) throws StoreException
    {
        transaction.begin();
        try
        {
            // if task throws, the transaction is rolled back
            final T result = task.perform();

            transaction.commit();

            return result;
        }
        finally
        {
            // only rolls back if the transaction hasn't been committed before
            transaction.rollback();
        }
    }

    /**
     * Validate the schema.
     * <p>
     * Just check if the current schema version is the last available version.
     *
     * @return This store's UUID.
     */
    private int validateSchema() throws StoreException
    {
        return transact(() ->
        {
            try
            {
                final int version = dao.selectVersion();
                final int expectedVersion = Migration.getExpectedSchemaVersion();
                if (Migration.getExpectedSchemaVersion() > version)
                {
                    throw new StoreException("schema version too old: " + version + ", expected: " + expectedVersion);
                }

                UUID storeId = dao.selectStoreId();
                if (storeId == null || storeId.version() != 4)
                {
                    throw new StoreException("store id not present or invalid");
                }

                id = storeId;

                return version;
            }
            catch (SQLiteException e)
            {
                throw new StoreException(e.getMessage(), e);
            }
        });
    }

    /**
     * Create the initial schema.
     *
     * @return This stores UUID
     */
    private int updateSchema() throws StoreException
    {
        return transact(() ->
        {
            try
            {
                int version = dao.selectVersion();
                final int expectedVersion = Migration.getExpectedSchemaVersion();
                try
                {
                    if (version < expectedVersion)
                    {
                        // execute all migration necessary to reach the expected version
                        for (final String migration : Migration.getMigrations(version))
                        {
                            db.exec(migration);

                            // register schema version
                            dao.insertOrReplaceMeta(ReservedMetaKey.VERSION.name(), Integer.toString(++version));
                        }

                        final String storeIdValue = dao.selectMeta(ReservedMetaKey.ID.name());
                        final UUID storeId;
                        if (storeIdValue == null)
                        {
                            storeId = UUID.randomUUID();
                            dao.insertOrReplaceMeta(ReservedMetaKey.ID.name(), storeId.toString());
                        }
                        else
                        {
                            try
                            {
                                storeId = UUID.fromString(storeIdValue);
                                if (storeId.version() != 4)
                                {
                                    throw new StoreException("unexpected store id version");
                                }
                            }
                            catch (IllegalArgumentException e)
                            {
                                throw new StoreException("store id not present or invalid");
                            }
                        }

                        id = storeId;
                    }
                    else
                    {
                        try
                        {
                            final String storeIdValue = dao.selectMeta(ReservedMetaKey.ID.name());
                            if (storeIdValue == null)
                            {
                                throw new StoreException("store id not present");
                            }

                            final UUID storeId = UUID.fromString(storeIdValue);
                            if (storeId.version() != 4)
                            {
                                throw new StoreException("unexpected store id version");
                            }

                            id = storeId;
                        }
                        catch (IllegalArgumentException e)
                        {
                            throw new StoreException("store id not present or invalid");
                        }
                    }

                    return expectedVersion;
                }
                catch (SQLiteException e)
                {
                    throw new StoreException("failed to migrate schema from version " + version + " to " + version + 1, e);
                }
            }
            catch (SQLiteException e)
            {
                throw new StoreException("failed to ensure schema version", e);
            }
        });
    }


    /**
     * This stores state.
     */
    private enum State
    {
        INITIAL,

        OPEN_READONLY,

        OPEN_READWRITE,

        CLOSED
    }


    private class SQLiteStoreTransaction implements Transaction
    {
        @Override
        public DAO getDao()
        {
            return dao;
        }

        @Override
        public boolean isActive()
        {
            return inTransaction;
        }

        @Override
        public void begin() throws StoreException
        {
            if (!inTransaction)
            {
                try
                {
                    dao.beginTransaction();
                    inTransaction = true;
                }
                catch (SQLiteException e)
                {
                    throw new StoreException("transaction begin failed", e);
                }
            }
        }

        @Override
        public void rollback()
        {
            if (inTransaction)
            {
                dao.rollback();
                inTransaction = false;
            }
        }

        @Override
        public void commit() throws StoreException
        {
            if (inTransaction)
            {
                try
                {
                    dao.commit();
                    inTransaction = false;
                }
                catch (SQLiteException e)
                {
                    throw new StoreException("transaction commit failed", e);
                }
            }
        }
    }
}
