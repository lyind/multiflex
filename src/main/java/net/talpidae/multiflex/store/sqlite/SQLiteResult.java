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
import com.almworks.sqlite4java.SQLiteStatement;

import java.io.InputStream;


/**
 * Simple wrapper for a stepped SQLiteStatement.
 */
public class SQLiteResult
{
    private final SQLiteStatement statement;

    private SQLiteResult(SQLiteStatement statement)
    {
        this.statement = statement;
    }

    public static SQLiteResult fromSteppedStatement(SQLiteStatement statement)
    {
        return new SQLiteResult(statement);
    }

    public String columnString(int column) throws SQLiteException
    {
        return statement.columnString(column);
    }

    public int columnInt(int column) throws SQLiteException
    {
        return statement.columnInt(column);
    }

    public double columnDouble(int column) throws SQLiteException
    {
        return statement.columnDouble(column);
    }

    public long columnLong(int column) throws SQLiteException
    {
        return statement.columnLong(column);
    }

    public byte[] columnBlob(int column) throws SQLiteException
    {
        return statement.columnBlob(column);
    }

    public InputStream columnStream(int column) throws SQLiteException
    {
        return statement.columnStream(column);
    }

    public boolean columnNull(int column) throws SQLiteException
    {
        return statement.columnNull(column);
    }

    public int columnCount() throws SQLiteException
    {
        return statement.columnCount();
    }

    public Object columnValue(int column) throws SQLiteException
    {
        return statement.columnValue(column);
    }

    public int columnType(int column) throws SQLiteException
    {
        return statement.columnType(column);
    }

    public String getColumnName(int column) throws SQLiteException
    {
        return statement.getColumnName(column);
    }

    public String getColumnTableName(int column) throws SQLiteException
    {
        return statement.getColumnTableName(column);
    }

    public String getColumnDatabaseName(int column) throws SQLiteException
    {
        return statement.getColumnDatabaseName(column);
    }

    public String getColumnOriginName(int column) throws SQLiteException
    {
        return statement.getColumnOriginName(column);
    }
}
