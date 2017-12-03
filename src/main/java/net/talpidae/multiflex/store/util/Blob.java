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


package net.talpidae.multiflex.store.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


public final class Blob
{
    private Blob()
    {

    }

    /**
     * Transfer resource binary blobs to an OutputStream.
     *
     * @param path         The path of the String literal resource to use ("/blabla/resource.txt" for "/src/main/resources/blabla/resource.txt").
     * @param outputStream The destination output stream
     * @return The String content of the specified resource or null if the resources wasn't found
     */
    public static boolean transfer(String path, OutputStream outputStream) throws IOException
    {
        try (final InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(path))
        {
            if (inputStream != null)
            {
                final byte[] buffer = new byte[8192];

                int readCount;
                while ((readCount = inputStream.read(buffer)) > 0)
                {
                    outputStream.write(buffer, 0, readCount);
                }

                return true;
            }
        }

        return false;
    }
}
