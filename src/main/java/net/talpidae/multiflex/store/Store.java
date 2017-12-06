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

import java.util.List;


/**
 * Operations on a single container file.
 */
public interface Store extends AutoCloseable
{
    /**
     * Meta-data key that denotes the store version.
     */
    String KEY_VERSION = "version";


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
    void put(long ts, Chunk chunk) throws StoreException;


    /**
     * Find a single chunk by exact timestamp.
     */
    Chunk findByTimestamp(long ts) throws StoreException;


    /**
     * Find chunks by timestamp range.
     */
    List<Chunk> findByTimestampRange(long tsBegin, long tsEnd);


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
     * Close this store. This instance can't be used afterwards.
     */
    @Override
    void close() throws StoreException;
}
