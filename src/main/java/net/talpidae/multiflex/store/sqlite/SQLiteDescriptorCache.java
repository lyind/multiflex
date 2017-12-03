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
    public SQLiteDescriptor get(long id)
    {
        return null;
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
    public SQLiteDescriptor intern(SQLiteDescriptor descriptor, Transaction transaction) throws StoreException
    {
        final SQLiteDescriptor localDescriptor = descriptor.forStore(store.getId());

        if (localDescriptor.getId() == 0)
        {
            // persist possibly new descriptor and remember ID
            try
            {
                final long id = transaction.getDao().insertDescriptorAndSelectId(localDescriptor);
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
