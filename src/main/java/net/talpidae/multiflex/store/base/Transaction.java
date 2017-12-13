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


/**
 * Track transaction state (for automatic rollback).
 */
public abstract class Transaction implements AutoCloseable
{
    // track if we are currently inside a transaction
    private boolean inTransaction;

    /**
     * Begin a new transaction.
     */
    public final Transaction begin() throws StoreException
    {
        if (!inTransaction)
        {
            try
            {
                transactionBegin();
                inTransaction = true;
            }
            catch (StoreException e)
            {
                throw new StoreException("transaction begin failed", e);
            }
        }

        return this;
    }

    /**
     * Rollback a transaction.
     */
    public final void rollback() throws StoreException
    {
        if (inTransaction)
        {
            transactionRollback();
            inTransaction = false;
        }
    }

    /**
     * Start a transaction.
     */
    public final void commit() throws StoreException
    {
        if (inTransaction)
        {
            try
            {
                transactionCommit();
                inTransaction = false;
            }
            catch (StoreException e)
            {
                try
                {
                    rollback();
                    throw new StoreException("transaction commit failed, rolled back", e);
                }
                catch (StoreException e2)
                {
                    final StoreException e3 = new StoreException("transaction commit failed, rollback unsuccessful", e);

                    e3.addSuppressed(e2);

                    throw e2;
                }
            }
        }
    }


    @Override
    public void close() throws StoreException
    {
        // only rolls back if the transaction has not been committed or rolled back before
        rollback();
    }


    /**
     * Implement how to actually begin a transaction.
     */
    protected abstract void transactionBegin() throws StoreException;

    /**
     * Implement how to commit.
     */
    protected abstract void transactionCommit() throws StoreException;

    /**
     * Implement what is necessary to do a rollback of the current transaction.
     */
    protected abstract void transactionRollback() throws StoreException;
}
