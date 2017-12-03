package net.talpidae.multiflex.store.sqlite;

import net.talpidae.multiflex.store.StoreException;


/**
 * Track transaction state (for automatic rollback).
 */
interface Transaction
{
    DAO getDao();

    boolean isActive();

    void begin() throws StoreException;

    void rollback();

    void commit() throws StoreException;
}
