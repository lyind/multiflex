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

import net.talpidae.multiflex.store.StoreException;
import net.talpidae.multiflex.store.util.LongSparseArray;

import java.nio.ByteBuffer;


class BaseDescriptorCache
{
    /**
     * Lookup IDs to Descriptors.
     * <p>
     * We need to use type long here because SQLite ID type is long.
     */
    private final LongSparseArray<BaseDescriptor> cache;

    private final BaseStore store;


    BaseDescriptorCache(BaseStore store)
    {
        this.store = store;
        this.cache = new LongSparseArray<>();
    }


    /**
     * Get a descriptor by its ID.
     */
    BaseDescriptor get(long id) throws StoreException
    {
        BaseDescriptor descriptor = cache.get(id);
        if (descriptor == null)
        {
            descriptor = BaseDescriptor.decode(store.getDao().selectDescriptor(id), id, store.getId());
            cache.put(id, descriptor);
        }

        return descriptor;
    }


    /**
     * Ensure the descriptor is registered with the associated store.
     * <p>
     * Sets the Descriptor's id to the ID in the store.
     * <p>
     * Call only from within a running database transaction.
     */
    void intern(BaseDescriptor descriptor) throws StoreException
    {
        if (descriptor.getId() == 0)
        {
            // persist possibly new descriptor and remember ID
            final ByteBuffer buffer = descriptor.encode();
            buffer.flip();

            final long id = store.getDao().insertDescriptorAndSelectId(buffer);
            descriptor.setId(id);

            cache.append(id, descriptor);
        }
    }


    /**
     * Clear the cache.
     */
    void clear()
    {
        cache.clear();
    }
}
