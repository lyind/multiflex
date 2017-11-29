package net.talpidae.multiflex.store.sqlite;

import com.almworks.sqlite4java.SQLiteConnection;
import net.talpidae.multiflex.store.StoreException;

import java.util.concurrent.ExecutionException;


class SQLiteJob<T> extends com.almworks.sqlite4java.SQLiteJob<T>
{
    private final FunctionalSQLiteJob<T> job;


    SQLiteJob(FunctionalSQLiteJob<T> job)
    {
        this.job = job;
    }


    public T getCatching() throws StoreException
    {
        try
        {
            return super.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new StoreException(e.getMessage(), e);
        }
    }

    @Override
    protected T job(SQLiteConnection connection) throws Throwable
    {
        return job.job(connection);
    }
}
