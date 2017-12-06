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

import net.talpidae.multiflex.store.util.Literal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Simple format migration handling.
 * <p>
 * Valid version numbers start at 1 and have no gaps.
 */
public final class Migration
{
    private static final List<String> MIGRATIONS;

    private static final String SCHEMA_BASE_PATH = "net/talpidae/multiflex/migration/";

    private static final String SCHEMA_POSTFIX = ".sql";

    /* Load all migrations (else each thread would have to do it again). */
    static
    {
        final List<String> migrations = new ArrayList<>();

        int version = 0;
        String migration;
        while ((migration = getSchemaMigration(++version)) != null)
        {
            migrations.add(migration);
        }

        MIGRATIONS = Arrays.asList(migrations.toArray(new String[migrations.size()]));
    }

    private Migration()
    {

    }

    private static String getSchemaMigration(int version)
    {
        try
        {
            return Literal.from(SCHEMA_BASE_PATH + version + SCHEMA_POSTFIX);
        }
        catch (IOException e)
        {
            return null;
        }
    }

    public static int getExpectedSchemaVersion()
    {
        return MIGRATIONS.size();
    }

    /**
     * Return all migration SQL statements necessary to reach the expected schema version from the specified start version.
     */
    public static List<String> getMigrations(int startVersion)
    {
        return MIGRATIONS.subList(startVersion, MIGRATIONS.size());
    }
}
