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

package net.talpidae.multiflex.store;


import net.talpidae.multiflex.format.Chunk;
import net.talpidae.multiflex.format.Descriptor;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;


/**
 * Operations on a single container file.
 */
public interface Store extends AutoCloseable
{

    /**
     * Open this store.
     * <p>
     * The container file is created and initialized if it doesn't exist already.
     *
     * @param writable Specifies if the store shall be opened in writable or read-only mode.
     * @return this
     */
    Store open(boolean writable) throws StoreException;

    /**
     * Insert new or replace existing chunk.
     */
    void put(Chunk chunk) throws StoreException;

    /**
     * Find a single chunk by exact timestamp (in seconds since epoch).
     */
    Chunk findByTimestamp(long ts) throws StoreException;

    /**
     * Find chunks by timestamp (in seconds since epoch) range.
     *
     * @param tsFirst The begin of the range (inclusive)
     * @param tsLast  The upper limit of the range (inclusive)
     * @return List of chunks within the specified range
     */
    List<Chunk> findByTimestampRange(long tsFirst, long tsLast) throws StoreException;

    /**
     * Find this store's epoch (in microseconds since the UNIX epoch).
     *
     * @return This store's epoch in microseconds since the UNIX epoch or -1 in case no epoch has been set
     */
    long getEpoch() throws StoreException;

    /**
     * Initialize this store's epoch (in microseconds since the UNIX epoch).
     *
     * @throws IllegalStateException    in case the epoch for this store has already been set
     * @throws IllegalArgumentException in case the epoch is negative
     */
    void setEpoch(long epochMicros) throws StoreException;

    /**
     * Find the maximum timestamp of any chunk contained in this store.
     */
    long findMaxTimestamp() throws StoreException;

    /**
     * Retrieve the meta-data value associated with the specified key.
     */
    String getMeta(String key) throws StoreException;

    /**
     * Store the specified meta-data value uniquely identified by the key.
     */
    void putMeta(String key, String value) throws StoreException;

    /**
     * Return a Descriptor.Builder instance.
     */
    Descriptor.Builder descriptorBuilder();

    /**
     * Return a Chunk.Builder instance configured for the specified descriptor.
     */
    Chunk.Builder chunkBuilder(Descriptor descriptor);

    /**
     * Return this store's unique ID.
     */
    UUID getId();

    /**
     * Return this store's schema or format version.
     */
    int getVersion();

    /**
     * Close this store. This instance can't be used afterwards.
     */
    @Override
    void close() throws StoreException;


    /**
     * Represents meta keys reserved for store internal purposes, these can not be set using setMeta().
     */
    enum ReservedMetaKey
    {
        ID,
        EPOCH_MICROS,
        VERSION;

        public static final List<ReservedMetaKey> values = Arrays.asList(values());

        public static final List<String> names;

        static
        {
            final String[] allNames = new String[values.size()];

            int i = 0;
            for (final ReservedMetaKey value : values)
            {
                allNames[i] = value.name();
                ++i;
            }

            names = Arrays.asList(allNames);
        }
    }
}
