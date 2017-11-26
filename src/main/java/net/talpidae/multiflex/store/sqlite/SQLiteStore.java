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

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class SQLiteStore implements Store
{
    private final File dbFile;

    private final Map<Integer, Descriptor> descriptorCache;

    private final SQLiteConnection db;

    private final ExecutorService executorService;


    private SQLiteStore(String path) throws StoreException
    {
        dbFile = new File(path);
        descriptorCache = new HashMap<>();
        executorService = Executors.newSingleThreadExecutor();

        db = new SQLiteConnection(new File(path));
        try
        {
            db.open(true);
        }
        catch (SQLiteException e)
        {
            throw new StoreException(e.getMessage(), e);
        }
    }

    @Override
    public Store open(String path) throws StoreException
    {
        return new SQLiteStore(path);
    }

    @Override
    public void register(Descriptor descriptor)
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
        executorService.shutdown();

        try
        {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
        finally
        {
            executorService.shutdownNow();

            db.dispose();
        }
    }
}
