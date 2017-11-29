package net.talpidae.multiflex.store.sqlite;


import java.io.File;


/**
 * Allow lambda use with SQLiteQueue.
 */
class SQLiteQueue extends com.almworks.sqlite4java.SQLiteQueue
{
    public SQLiteQueue(File dbFile)
    {
        super(dbFile);
    }

    /**
     * Like the original, but accepts lambdas.
     */
    public <T> SQLiteJob<T> schedule(FunctionalSQLiteJob<T> job)
    {
        return super.execute(new SQLiteJob<>(job));
    }
}
