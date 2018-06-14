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

package net.talpidae.multiflex.store.base;


import net.talpidae.multiflex.format.Chunk;
import net.talpidae.multiflex.format.Descriptor;
import net.talpidae.multiflex.store.Store;
import net.talpidae.multiflex.store.StoreException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;


public class BaseStore implements Store
{
    private final BaseDescriptorCache descriptorCache;

    private final DAO dao;

    /**
     * The opened stores unique ID.
     */
    private UUID id;

    private volatile State state = State.INITIAL;

    private int schemaVersion = 0;

    private BaseDescriptor lastUsedDescriptor;

    public BaseStore(DAO dao)
    {
        this.descriptorCache = new BaseDescriptorCache(this);

        // simple DAO for our format's tables
        this.dao = dao;
    }

    @Override
    public Store open(boolean writable) throws StoreException
    {
        synchronized (this)
        {
            if (state != State.INITIAL)
                throw new StoreException("store has already been opened");

            try
            {
                dao.open(writable);
                state = writable ? State.OPEN_READWRITE : State.OPEN_READONLY;
            }
            catch (StoreException e)
            {
                state = State.CLOSED;
                throw e;
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
    public void put(Chunk chunk) throws StoreException
    {
        if (state != State.OPEN_READWRITE)
        {
            throw new StoreException("store not writable");
        }

        if (!(chunk instanceof BaseChunk))
        {
            throw new StoreException("cannot put unsupported chunk: " + (chunk != null ? chunk.getClass().getName() : null));
        }

        final BaseChunk actualChunk = ((BaseChunk) chunk).forStore(id);
        transact(() ->
        {
            descriptorCache.intern(actualChunk.getDescriptor());
            try
            {
                dao.insertOrReplaceTrackChunk(actualChunk.getTimestamp(), actualChunk.getDescriptor().getId(), actualChunk.getData());
            }
            catch (StoreException e)
            {
                throw new StoreException("failed to persist chunk", e);
            }

            return null;
        });
    }

    /**
     * Factory method which allows the DAO to create chunks for us.
     */
    private Chunk createChunk(long timestamp, long descriptorId, ByteBuffer data) throws StoreException
    {
        final BaseDescriptor descriptor;
        if (lastUsedDescriptor != null && lastUsedDescriptor.getId() == descriptorId)
        {
            // use last used descriptor (avoid expensive look-up in common append-with-same-descriptor case)
            descriptor = lastUsedDescriptor;
        }
        else
        {
            // lookup descriptor by id
            descriptor = descriptorCache.get(descriptorId);
        }

        return new BaseChunk(descriptor, timestamp, data);
    }


    @Override
    public Chunk findByTimestamp(long ts) throws StoreException
    {
        return transact(() ->
        {
            try
            {
                return dao.selectChunkByTimestamp(ts, this::createChunk);
            }
            catch (StoreException e)
            {
                throw new StoreException("failed to read chunk for timestamp " + ts);
            }
        });
    }

    @Override
    public List<Chunk> findByTimestampRange(long tsBegin, long tsEnd) throws StoreException
    {
        return transact(() ->
        {
            try
            {
                return dao.selectChunksByTimestampRange(tsBegin, tsEnd, this::createChunk);
            }
            catch (StoreException e)
            {
                throw new StoreException("failed to read chunks for timestamps between " + tsBegin + " and " + tsEnd + " (exclusive)"
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
            catch (StoreException e)
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
            catch (StoreException e)
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
            catch (StoreException e)
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
            catch (StoreException e)
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
            catch (StoreException e)
            {
                throw new StoreException("failed to put meta value: key=" + key + ", value: " + value + ": " + e.getMessage(), e);
            }

            return null;
        });
    }

    @Override
    public Descriptor.Builder descriptorBuilder()
    {
        return new BaseDescriptor.Builder(this);
    }

    @Override
    public Chunk.Builder chunkBuilder(Descriptor descriptor)
    {
        if (!(descriptor instanceof BaseDescriptor))
        {
            throw new IllegalArgumentException("incompatible descriptor implementation");
        }

        return new BaseChunk.Builder((BaseDescriptor) descriptor);
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
            try
            {
                switch (state)
                {
                    case OPEN_READWRITE:
                    case OPEN_READONLY:
                    {
                        lastUsedDescriptor = null;
                        descriptorCache.clear();

                        try
                        {
                            dao.close();
                        }
                        catch (Exception e)
                        {
                            throw new StoreException("failed to close store", e);
                        }
                        break;
                    }

                    default:
                        break;
                }
            }
            finally
            {
                state = State.CLOSED;
            }
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
        try (final Transaction transaction = dao.getTransaction())
        {
            // if task throws, the transaction is rolled back
            final T result = task.perform();

            transaction.commit();

            return result;
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
            final int version = dao.selectVersion();
            final int expectedVersion = Migration.getExpectedSchemaVersion();
            if (Migration.getExpectedSchemaVersion() > version)
            {
                throw new StoreException("schema version too old: " + version + ", expected: " + expectedVersion);
            }

            final String storeIdValue = dao.selectMeta(ReservedMetaKey.ID.name());
            if (storeIdValue == null)
            {
                throw new StoreException("store id is not set");
            }

            final UUID storeId = UUID.fromString(storeIdValue);
            if (storeId.version() != 4)
            {
                throw new StoreException("store id is not a valid UUIDv4");
            }

            id = storeId;

            return version;
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
                            dao.execMigration(version, version + 1, migration);

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
                catch (StoreException e)
                {
                    throw new StoreException("failed to migrate schema from version " + version + " to " + version + 1, e);
                }
            }
            catch (StoreException e)
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
}
