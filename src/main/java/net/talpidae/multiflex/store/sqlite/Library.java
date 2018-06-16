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
                final String libTargetPath = libDir.getAbsolutePath() + File.separatorChar + libName.replaceFirst("-\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}","");
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
