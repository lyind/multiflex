package net.talpidae.multiflex;

import org.junit.Test;

import java.io.File;
import java.io.IOException;


public class MultiflexTest
{
    @Test
    public void testOpenNew() throws IOException
    {
        final File dbFile = File.createTempFile(MultiflexTest.class.getSimpleName(), ".db");

        Multiflex.open();
    }
}
