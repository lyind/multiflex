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


import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import net.talpidae.multiflex.format.Chunk;
import net.talpidae.multiflex.format.Descriptor;
import net.talpidae.multiflex.store.Store;
import net.talpidae.multiflex.store.StoreException;
import net.talpidae.multiflex.store.util.Literal;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SQLiteStore implements Store
{
    private static final String[] SCHEMA_PATHS = {
            "net/talpidae/multiflex/migration/V1_table_meta.sql",
            "net/talpidae/multiflex/migration/V2_table_stream_descriptor.sql",
            "net/talpidae/multiflex/migration/V3_table_stream.sql"
    };

    private final File dbFile;

    private final Map<Integer, Descriptor> descriptorCache;

    private final SQLiteConnection db;

    private final SQLiteQueue writeQueue;


    public SQLiteStore(File file) throws StoreException
    {
        dbFile = file;
        descriptorCache = new HashMap<>();
        writeQueue = new SQLiteQueue(dbFile);
        db = new SQLiteConnection(dbFile);
    }


    @Override
    public Store open() throws StoreException
    {
        try
        {
            descriptorCache.clear();

            db.open(true);
        }
        catch (SQLiteException e)
        {

        }

        try
        {
            createSchema();

            return this;
        }
        catch (SQLiteException e)
        {
            throw new StoreException(e.getMessage(), e);
        }
    }

    @Override
    public synchronized void register(Descriptor descriptor)
    {

    }

    @Override
    public void put(long ts, Chunk chunk)
    {

    }

    @Override
    public Chunk findByTimestamp(long ts)
    {
        return null;
    }

    @Override
    public List<Chunk> findByTimestampRange(long tsBegin, long tsEnd)
    {
        return Collections.emptyList();
    }

    @Override
    public String getMeta(String key)
    {
        return null;
    }

    @Override
    public void putMeta(String key, String value)
    {

    }

    @Override
    public void close() throws Exception
    {
        writeQueue.stop(true);
        writeQueue.join();  // TODO Override and join with timeout?

        db.dispose();
    }


    /**
     * Create the initial schema.
     */
    private void createSchema() throws StoreException
    {
        writeQueue.schedule(connection ->
        {
            try
            {
                for (final String path : SCHEMA_PATHS)
                {
                    final String sql = Literal.from(path);

                    connection.exec(sql);
                }

                connection.exec("COMMIT");

                return null;
            }
            catch (SQLiteException e)
            {
                throw new StoreException(e.getMessage(), e);
            }
        }).getCatching();
    }
}
