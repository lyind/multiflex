package net.talpidae.multiflex.store.sqlite;

import net.talpidae.multiflex.store.StoreException;


@FunctionalInterface
interface TransactionalTask<T>
{
    /**
     * The function to be performed in the context of a transaction.
     */
    T perform(Transaction transaction) throws StoreException;
}
