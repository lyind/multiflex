package net.talpidae.multiflex.store.sqlite;

import com.almworks.sqlite4java.SQLiteConnection;


@FunctionalInterface
public interface FunctionalSQLiteJob<T>
{
    T job(SQLiteConnection connection) throws Throwable;
}
