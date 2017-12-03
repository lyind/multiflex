package net.talpidae.multiflex;

import net.talpidae.multiflex.store.Store;
import net.talpidae.multiflex.store.StoreException;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;


public class MultiflexTest
{
    @Test
    public void testOpenWritable() throws IOException, StoreException
    {
        final File dbFile = File.createTempFile(MultiflexTest.class.getSimpleName(), ".db");

        try(Store store = Multiflex.open(dbFile, true))
        {
            assertThat("store is null", store, CoreMatchers.notNullValue());
        }
    }


    @Test
    public void testPutChunk() throws IOException, StoreException
    {
        final File dbFile = File.createTempFile(MultiflexTest.class.getSimpleName(), ".db");

        try(Store store = Multiflex.open(dbFile, true))
        {
            assertThat("store is null", store, CoreMatchers.notNullValue());

            store.descriptorBuilder();

        }

    }
}
