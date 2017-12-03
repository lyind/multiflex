package net.talpidae.multiflex.store.sqlite;

import com.almworks.sqlite4java.SQLite;
import net.talpidae.multiflex.store.StoreException;
import net.talpidae.multiflex.store.util.Blob;
import net.talpidae.multiflex.store.util.Literal;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


class Library
{
    private static final String LIB_PATH = "natives";

    private static final String LIB_INDEX_NAME = "natives.index";

    private static final String[] LIB_NAMES = {
            "sqlite4java-win32-x64-1.0.392.dll"
    };

    private static boolean isDeployed = false;


    /**
     * Deploy binary libraries and configure SQLite to load them from the lib directory.
     */
    static synchronized void deployAndConfigure() throws StoreException
    {
        if (!isDeployed)
        {

            final File libDir = new File(LIB_PATH);

            if (!libDir.mkdir() && !libDir.isDirectory())
            {
                throw new StoreException("failed to create library directory: " + libDir.getAbsolutePath());
            }

            final List<String> index;
            try
            {
                final String indexFile = Literal.from(LIB_INDEX_NAME);

                index = indexFile != null
                        ? Arrays.asList(indexFile.split("\n"))
                        : Collections.emptyList();
            }
            catch (IOException e1)
            {
                throw new StoreException("failed to read index of native libs", e1);
            }

            for (final String libName : index)
            {
                final String libTargetPath = libDir.getAbsolutePath() + File.separatorChar + libName;
                try
                {
                    try (final OutputStream libTarget = new FileOutputStream(libTargetPath))
                    {
                        //Blob.transfer("com/almworks/sqlite4java/" + libName, libTarget);
                        Blob.transfer(libName, libTarget);
                    }
                }
                catch (IOException e)
                {
                    throw new StoreException("failed to transfer library " + libName + " to: " + libTargetPath, e);
                }
            }
        }

        SQLite.setLibraryPath(LIB_PATH);

        isDeployed = true;
    }
}
