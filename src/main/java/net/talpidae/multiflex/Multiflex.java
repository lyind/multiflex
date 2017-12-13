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

package net.talpidae.multiflex;


import net.talpidae.multiflex.store.Store;
import net.talpidae.multiflex.store.StoreException;
import net.talpidae.multiflex.store.sqlite.AlmworksSqliteDAO;
import net.talpidae.multiflex.store.base.BaseStore;

import java.io.File;


public class Multiflex
{

    /**
     * Open the store at the specified location read-write or read-only using the regular SQLite wrapper.
     *
     * @param dbFile   The store file
     * @param writable Open the store in writable mode or not
     * @return An open store instance
     */
    public static Store openSqlite(File dbFile, boolean writable) throws StoreException
    {
        final Store store = new BaseStore(new AlmworksSqliteDAO(dbFile));
        try
        {
            return store.open(writable);
        }
        catch (StoreException e)
        {
            try
            {
                store.close();
            }
            catch (Exception e1)
            {
                final StoreException nextException = new StoreException("failed to close after error", e1);
                nextException.addSuppressed(e);
                throw nextException;
            }

            throw e;
        }
    }
}