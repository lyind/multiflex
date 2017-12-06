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

import com.almworks.sqlite4java.SQLiteException;
import net.talpidae.multiflex.store.StoreException;
import net.talpidae.multiflex.store.util.LongSparseArray;


class SQLiteDescriptorCache
{
    /**
     * Lookup IDs to Descriptors.
     * <p>
     * We need to use type long here because SQLite ID type is long.
     */
    private final LongSparseArray<SQLiteDescriptor> cache;

    private final SQLiteStore store;


    SQLiteDescriptorCache(SQLiteStore store)
    {
        this.store = store;
        this.cache = new LongSparseArray<>();
    }


    /**
     * Get a descriptor by its ID.
     */
    public SQLiteDescriptor get(long id) throws StoreException
    {
        final SQLiteDescriptor descriptor = cache.get(id);
        if (descriptor == null)
        {
            try
            {
                final SQLiteDescriptor uncachedDescriptor = store.getDao().selectDescriptor(id, store.getId());
                if (uncachedDescriptor != null)
                {
                    cache.put(id, uncachedDescriptor);
                }

                return uncachedDescriptor;
            }
            catch (SQLiteException e)
            {
                throw new StoreException("failed to get descriptor: " + e.getMessage(), e);
            }
        }

        return descriptor;
    }


    /**
     * Ensure the descriptor is registered with the associated store.
     * <p>
     * Sets the Descriptor's id to the ID in the store.
     * <p>
     * Call only from within a running database transaction.
     *
     * @return Existing or new Descriptor instance registered with this store.
     */
    public SQLiteDescriptor intern(SQLiteDescriptor descriptor) throws StoreException
    {
        final SQLiteDescriptor localDescriptor = descriptor.forStore(store.getId());

        if (localDescriptor.getId() == 0)
        {
            // persist possibly new descriptor and remember ID
            try
            {
                final long id = store.getDao().insertDescriptorAndSelectId(localDescriptor);
                localDescriptor.setId(id);

                cache.append(id, localDescriptor);

                return localDescriptor;
            }
            catch (SQLiteException e)
            {
                throw new StoreException("failed to insert descriptor");
            }
        }
        else
        {
            return localDescriptor;
        }
    }


    /**
     * Clear the cache.
     */
    public void clear()
    {
        cache.clear();
    }
}
